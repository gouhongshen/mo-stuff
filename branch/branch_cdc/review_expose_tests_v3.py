#!/usr/bin/env python3
import os
import sys
import time
from branch_cdc import (
    DBConnection,
    LockKeeper,
    perform_sync,
    verify_consistency,
    get_task_id,
    ensure_meta_table,
    META_DB,
    META_TABLE,
    META_LOCK_TABLE,
    INSTANCE_ID,
)

HOST = "127.0.0.1"
PORT = 6001
ADMIN_CFG = {"host": HOST, "port": PORT, "user": "root", "password": "111", "db": "mo_catalog"}
CDC_USER = {"user": "dump", "password": "111"}
STAGE_BASE = os.path.abspath(os.path.join("/tmp", "mo_branch_cdc_review_stage_v3"))


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
    admin.execute(f"INSERT INTO `{up_db}`.`{table}` VALUES (1, 10), (2, 20)")
    admin.commit()
    create_stage(admin, stage, stage_dir)
    admin.close()

    cfg = {
        "upstream": {"host": HOST, "port": PORT, "db": up_db, "table": table, **CDC_USER},
        "downstream": {"host": HOST, "port": PORT, "db": ds_db, "table": table, **CDC_USER},
        "stage": {"name": stage},
    }
    return cfg


def test_snapshot_missing_fallback_full():
    cfg = setup_basic_env("cdc_rev_snapfull")
    assert perform_sync(cfg), "Initial sync failed unexpectedly."

    up = DBConnection(cfg["upstream"], "Up")
    if not up.connect():
        raise RuntimeError("Upstream connect failed for snapshot fallback test.")
    up.execute(f"INSERT INTO `{cfg['upstream']['db']}`.`{cfg['upstream']['table']}` VALUES (3, 30)")
    up.commit()
    up.close()

    assert perform_sync(cfg), "Second sync failed unexpectedly."

    ds = DBConnection(cfg["downstream"], "Ds")
    if not ds.connect():
        raise RuntimeError("Downstream connect failed for snapshot fallback test.")
    tid = get_task_id(cfg)
    ws = ds.query(f"SELECT watermark FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s ORDER BY created_at DESC", (tid,))
    if not ws:
        raise AssertionError("No watermark found after second sync.")
    lost = ws[0]["watermark"]
    ds.close()

    up = DBConnection(cfg["upstream"], "Up")
    up.connect()
    up.execute(f"DROP SNAPSHOT IF EXISTS `{lost}`")
    up.commit()
    up.close()

    ok = perform_sync(cfg)
    assert ok, "Expected fallback to FULL sync after missing snapshot, but it failed."

    up = DBConnection(cfg["upstream"], "Up")
    ds = DBConnection(cfg["downstream"], "Ds")
    up.connect()
    ds.connect()
    tid = get_task_id(cfg)
    ws = ds.query(f"SELECT watermark FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s ORDER BY created_at DESC", (tid,))
    snap = ws[0]["watermark"] if ws else None
    assert verify_consistency(up, ds, cfg, snap, mode="full"), "Data mismatch after missing snapshot fallback."
    up.close()
    ds.close()


def test_lockkeeper_heartbeat_advances_lock_time():
    cfg = setup_basic_env("cdc_rev_heartbeat")
    tid = get_task_id(cfg)
    ds_cfg = {"host": HOST, "port": PORT, "user": "root", "password": "111", "db": cfg["downstream"]["db"]}

    ds = DBConnection(ds_cfg, "Ds")
    if not ds.connect(db_override=""):
        raise RuntimeError("Downstream connect failed for heartbeat test.")
    ensure_meta_table(ds)
    ds.execute(f"DELETE FROM `{META_DB}`.`{META_LOCK_TABLE}` WHERE task_id=%s", (tid,))
    ds.execute(
        f"INSERT INTO `{META_DB}`.`{META_LOCK_TABLE}` (task_id, lock_owner, lock_time) VALUES (%s, %s, NOW())",
        (tid, INSTANCE_ID),
    )
    ds.commit()
    before = ds.fetch_one(f"SELECT lock_time FROM `{META_DB}`.`{META_LOCK_TABLE}` WHERE task_id=%s", (tid,))["lock_time"]

    keeper = LockKeeper(ds_cfg, tid)
    keeper.start()
    time.sleep(12)
    after = ds.fetch_one(f"SELECT lock_time FROM `{META_DB}`.`{META_LOCK_TABLE}` WHERE task_id=%s", (tid,))["lock_time"]
    keeper.stop()
    keeper.join()
    ds.close()

    assert after > before, "LockKeeper did not advance lock_time."


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
        ("snapshot_missing_fallback_full", test_snapshot_missing_fallback_full),
        ("lockkeeper_heartbeat_advances_lock_time", test_lockkeeper_heartbeat_advances_lock_time),
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
