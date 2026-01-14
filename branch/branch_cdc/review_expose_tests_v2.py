#!/usr/bin/env python3
import os
import sys
from branch_cdc import (
    DBConnection,
    perform_sync,
    verify_consistency,
    get_task_id,
    ensure_meta_table,
    META_DB,
    META_TABLE,
)

HOST = "127.0.0.1"
PORT = 6001
ADMIN_CFG = {"host": HOST, "port": PORT, "user": "root", "password": "111", "db": "mo_catalog"}
CDC_USER = {"user": "dump", "password": "111"}
STAGE_BASE = os.path.abspath(os.path.join("/tmp", "mo_branch_cdc_review_stage_v2"))


def admin_conn():
    conn = DBConnection(ADMIN_CFG, "Admin")
    if not conn.connect():
        raise RuntimeError("Cannot connect to MatrixOne; ensure mo-service is running.")
    return conn


def reset_db(conn, db_name):
    conn.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
    conn.execute(f"CREATE DATABASE `{db_name}`")
    conn.commit()


def create_stage(conn, stage_name, stage_dir):
    conn.execute(f"DROP STAGE IF EXISTS `{stage_name}`")
    conn.commit()
    os.makedirs(stage_dir, exist_ok=True)
    conn.execute(f"CREATE STAGE `{stage_name}` URL='file://{stage_dir}'")
    conn.commit()


def setup_basic_env(prefix):
    up_db = f"{prefix}_up"
    ds_db = f"{prefix}_ds"
    table = "t"
    stage = f"{prefix}_stage"
    stage_dir = os.path.join(STAGE_BASE, prefix)

    admin = admin_conn()
    reset_db(admin, up_db)
    reset_db(admin, ds_db)
    admin.execute(f"CREATE TABLE `{up_db}`.`{table}` (id INT PRIMARY KEY, val INT)")
    admin.execute(f"INSERT INTO `{up_db}`.`{table}` VALUES (1, 10)")
    admin.commit()
    create_stage(admin, stage, stage_dir)
    admin.close()

    cfg = {
        "upstream": {"host": HOST, "port": PORT, "db": up_db, "table": table, **CDC_USER},
        "downstream": {"host": HOST, "port": PORT, "db": ds_db, "table": table, **CDC_USER},
        "stage": {"name": stage},
    }
    return cfg


def test_lock_contention_should_not_crash():
    cfg = setup_basic_env("cdc_rev_lock")
    tid = get_task_id(cfg)

    ds = DBConnection(cfg["downstream"], "Ds")
    if not ds.connect(db_override=""):
        raise RuntimeError("Downstream connect failed for lock contention test.")
    ensure_meta_table(ds)
    # Pre-create a lock owned by another instance so acquire_lock should fail.
    ds.execute(f"DELETE FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s", (tid,))
    ds.execute(
        f"INSERT INTO `{META_DB}`.`{META_TABLE}` (task_id, lock_owner, lock_time) VALUES (%s, %s, NOW())",
        (tid, "OTHER_INSTANCE"),
    )
    ds.commit()
    ds.close()

    try:
        ok = perform_sync(cfg)
    except Exception as e:
        raise AssertionError(f"perform_sync raised when lock was held: {e}")
    assert ok is False, "Expected sync to skip when lock is held, but it did not."


def test_verify_missing_table_should_fail():
    up_db = "cdc_rev_missing_up"
    ds_db = "cdc_rev_missing_ds"

    admin = admin_conn()
    reset_db(admin, up_db)
    reset_db(admin, ds_db)
    admin.close()

    cfg = {
        "upstream": {"host": HOST, "port": PORT, "db": up_db, "table": "missing_tbl", **CDC_USER},
        "downstream": {"host": HOST, "port": PORT, "db": ds_db, "table": "missing_tbl", **CDC_USER},
    }

    up = DBConnection(cfg["upstream"], "Up")
    ds = DBConnection(cfg["downstream"], "Ds")
    if not up.connect() or not ds.connect():
        raise RuntimeError("Connect failed for missing-table verification test.")
    ok = verify_consistency(up, ds, cfg)
    up.close()
    ds.close()
    assert ok is False, "Expected verify_consistency to fail for missing tables, but it passed."


def test_full_sync_load_failure_should_keep_data():
    cfg = setup_basic_env("cdc_rev_full_txn")
    up_db = cfg["upstream"]["db"]
    ds_db = cfg["downstream"]["db"]
    table = cfg["upstream"]["table"]

    admin = admin_conn()
    # Recreate downstream table with an extra NOT NULL column to force LOAD DATA failure.
    admin.execute(f"DROP TABLE IF EXISTS `{ds_db}`.`{table}`")
    admin.execute(
        f"CREATE TABLE `{ds_db}`.`{table}` (id INT PRIMARY KEY, val INT, extra INT NOT NULL)"
    )
    admin.execute(f"INSERT INTO `{ds_db}`.`{table}` VALUES (1, 10, 999)")
    admin.commit()
    admin.close()

    ds = DBConnection(cfg["downstream"], "Ds")
    if not ds.connect():
        raise RuntimeError("Downstream connect failed for FULL rollback test.")
    before = ds.fetch_one(f"SELECT COUNT(*) AS c FROM `{ds_db}`.`{table}`")["c"]
    ds.close()

    ok = perform_sync(cfg)
    assert ok is False, "Expected FULL sync to fail due to LOAD DATA mismatch, but it succeeded."

    ds = DBConnection(cfg["downstream"], "Ds")
    ds.connect()
    after = ds.fetch_one(f"SELECT COUNT(*) AS c FROM `{ds_db}`.`{table}`")["c"]
    ds.close()
    assert after == before, "Downstream data changed after failed FULL sync."


def run_test(name, fn):
    try:
        fn()
        print(f"[PASS] {name}")
        return True
    except AssertionError as e:
        print(f"[FAIL] {name}: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] {name}: {e}")
        return False


def main():
    tests = [
        ("lock_contention_should_not_crash", test_lock_contention_should_not_crash),
        ("verify_missing_table_should_fail", test_verify_missing_table_should_fail),
        ("full_sync_load_failure_should_keep_data", test_full_sync_load_failure_should_keep_data),
    ]
    failures = 0
    for name, fn in tests:
        if not run_test(name, fn):
            failures += 1
    if failures:
        print(f"{failures} test(s) failed. Issues reproduced.")
        sys.exit(1)


if __name__ == "__main__":
    main()
