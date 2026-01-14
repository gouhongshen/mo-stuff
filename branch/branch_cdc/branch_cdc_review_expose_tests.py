#!/usr/bin/env python3
import os
import sys
from branch_cdc import DBConnection, perform_sync, verify_consistency, get_task_id, get_watermarks, META_DB

HOST = "127.0.0.1"
PORT = 6001
ADMIN_CFG = {"host": HOST, "port": PORT, "user": "root", "password": "111", "db": "mo_catalog"}
CDC_USER = {"user": "dump", "password": "111"}
STAGE_BASE = os.path.abspath(os.path.join("/tmp", "mo_branch_cdc_review_stage"))


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


def count_rows(conn, db, table):
    res = conn.fetch_one(f"SELECT COUNT(*) AS c FROM `{db}`.`{table}`")
    return res["c"] if res else 0


def test_auto_create_downstream_db():
    up_db, ds_db, table, stage = "cdc_rev_up_auto_db", "cdc_rev_ds_missing", "t_auto", "stage_auto_db"
    stage_dir = os.path.join(STAGE_BASE, "auto_db")

    admin = admin_conn()
    reset_db(admin, up_db)
    admin.execute(f"CREATE TABLE `{up_db}`.`{table}` (id INT PRIMARY KEY, val INT)")
    admin.execute(f"INSERT INTO `{up_db}`.`{table}` VALUES (1, 10)")
    admin.commit()
    admin.execute(f"DROP DATABASE IF EXISTS `{ds_db}`")
    admin.commit()
    create_stage(admin, stage, stage_dir)
    admin.close()

    cfg = {
        "upstream": {"host": HOST, "port": PORT, "db": up_db, "table": table, **CDC_USER},
        "downstream": {"host": HOST, "port": PORT, "db": ds_db, "table": table, **CDC_USER},
        "stage": {"name": stage},
    }
    ok = perform_sync(cfg)
    assert ok, "Expected downstream DB auto-create to succeed, but sync failed."


def test_full_sync_truncate_data_loss_on_diff_failure():
    up_db, ds_db, table, stage = "cdc_rev_up_trunc", "cdc_rev_ds_trunc", "t_trunc", "stage_missing"

    admin = admin_conn()
    reset_db(admin, up_db)
    reset_db(admin, ds_db)
    admin.execute(f"CREATE TABLE `{up_db}`.`{table}` (id INT PRIMARY KEY, val INT)")
    admin.execute(f"CREATE TABLE `{ds_db}`.`{table}` (id INT PRIMARY KEY, val INT)")
    admin.execute(f"INSERT INTO `{ds_db}`.`{table}` VALUES (1, 100), (2, 200)")
    admin.commit()
    admin.execute(f"DROP STAGE IF EXISTS `{stage}`")
    admin.commit()
    admin.close()

    cfg = {
        "upstream": {"host": HOST, "port": PORT, "db": up_db, "table": table, **CDC_USER},
        "downstream": {"host": HOST, "port": PORT, "db": ds_db, "table": table, **CDC_USER},
        "stage": {"name": stage},
    }

    ds = DBConnection(cfg["downstream"], "Ds")
    ds.connect()
    before = count_rows(ds, ds_db, table)
    ds.close()

    ok = perform_sync(cfg)
    assert not ok, "Expected sync to fail due to missing stage, but it succeeded."

    ds = DBConnection(cfg["downstream"], "Ds")
    ds.connect()
    after = count_rows(ds, ds_db, table)
    ds.close()
    assert after == before, "Downstream data was truncated despite failed FULL sync."


def test_missing_snapshot_no_fallback():
    up_db, ds_db, table, stage = "cdc_rev_up_snap", "cdc_rev_ds_snap", "t_snap", "stage_snap"
    stage_dir = os.path.join(STAGE_BASE, "snap")

    admin = admin_conn()
    reset_db(admin, up_db)
    reset_db(admin, ds_db)
    admin.execute(f"CREATE TABLE `{up_db}`.`{table}` (id INT PRIMARY KEY, val INT)")
    admin.execute(f"INSERT INTO `{up_db}`.`{table}` VALUES (1, 10), (2, 20)")
    admin.commit()
    create_stage(admin, stage, stage_dir)
    admin.close()

    cfg = {
        "upstream": {"host": HOST, "port": PORT, "db": up_db, "table": table, **CDC_USER},
        "downstream": {"host": HOST, "port": PORT, "db": ds_db, "table": table, **CDC_USER},
        "stage": {"name": stage},
    }

    assert perform_sync(cfg), "Initial sync failed unexpectedly."

    ds = DBConnection(cfg["downstream"], "Ds")
    ds.connect()
    tid = get_task_id(cfg)
    w = get_watermarks(ds, tid)[0]
    ds.close()

    up = DBConnection(cfg["upstream"], "Up")
    up.connect()
    up.execute(f"DROP SNAPSHOT IF EXISTS `{w}`")
    up.commit()
    up.close()

    ok = perform_sync(cfg)
    assert ok, "Expected recovery when watermark snapshot is missing, but sync failed."


def test_fast_check_without_pk():
    up_db, ds_db, table, stage = "cdc_rev_up_nopk", "cdc_rev_ds_nopk", "t_nopk", "stage_nopk"
    stage_dir = os.path.join(STAGE_BASE, "nopk")

    admin = admin_conn()
    reset_db(admin, up_db)
    reset_db(admin, ds_db)
    admin.execute(f"CREATE TABLE `{up_db}`.`{table}` (id INT, val INT)")
    admin.execute(f"INSERT INTO `{up_db}`.`{table}` VALUES (1, 10), (2, 20)")
    admin.commit()
    create_stage(admin, stage, stage_dir)
    admin.close()

    cfg = {
        "upstream": {"host": HOST, "port": PORT, "db": up_db, "table": table, **CDC_USER},
        "downstream": {"host": HOST, "port": PORT, "db": ds_db, "table": table, **CDC_USER},
        "stage": {"name": stage},
    }
    assert perform_sync(cfg), "Initial sync failed unexpectedly."

    up = DBConnection(cfg["upstream"], "Up")
    ds = DBConnection(cfg["downstream"], "Ds")
    up.connect()
    ds.connect()
    ok = verify_consistency(up, ds, cfg, mode="fast")
    up.close()
    ds.close()
    assert ok, "Expected fast check to work without PK, but it failed."


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
        ("auto_create_downstream_db", test_auto_create_downstream_db),
        ("full_sync_truncate_data_loss", test_full_sync_truncate_data_loss_on_diff_failure),
        ("missing_snapshot_no_fallback", test_missing_snapshot_no_fallback),
        ("fast_check_without_pk", test_fast_check_without_pk),
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
