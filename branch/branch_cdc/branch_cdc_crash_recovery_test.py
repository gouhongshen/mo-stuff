#!/usr/bin/env python3
import os, sys, time, subprocess, json, signal
from branch_cdc import DBConnection, verify_consistency

HOST, PORT = "127.0.0.1", 6001
ADMIN_CFG = {"host": HOST, "port": PORT, "user": "root", "password": "111", "db": "mo_catalog"}

def setup_env():
    up_db, ds_db = "cdc_crash_up", "cdc_crash_ds"
    admin = DBConnection(ADMIN_CFG, "Admin")
    admin.connect()
    for db in [up_db, ds_db]:
        admin.execute(f"DROP DATABASE IF EXISTS `{db}`"); admin.commit()
        admin.execute(f"CREATE DATABASE `{db}`"); admin.commit()
    admin.execute(f"CREATE TABLE `{up_db}`.crash_tbl (id INT PRIMARY KEY, val TEXT)")
    admin.commit()
    vals = [f"({i}, 'data_{i}')" for i in range(1000)]
    admin.execute(f"INSERT INTO `{up_db}`.crash_tbl VALUES " + ",".join(vals))
    admin.commit()
    # Use direct file URL
    stage_path = os.path.abspath("/tmp/mo_crash_v5")
    os.makedirs(stage_path, exist_ok=True)
    admin.close()
    return up_db, ds_db, stage_path

def test_crash_and_recovery():
    up_db, ds_db, stage_path = setup_env()
    cfg = {
        "upstream": {"host": HOST, "port": PORT, "user": "dump", "password": "111", "db": up_db, "table": "crash_tbl"},
        "downstream": {"host": HOST, "port": PORT, "user": "dump", "password": "111", "db": ds_db, "table": "crash_tbl"},
        "stage": {"url": f"file://{stage_path}"}
    }
    with open("config_crash.json", "w") as f: json.dump(cfg, f)

    print("[CRASH] Starting sync and killing it mid-way...")
    proc = subprocess.Popen(["python3", "branch_cdc.py", "--config", "config_crash.json", "--once"], 
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    time.sleep(1.5)
    proc.send_signal(signal.SIGKILL)
    print("[CRASH] Process SIGKILLed.")
    time.sleep(1)
    
    # Force clean meta lock
    admin = DBConnection(ADMIN_CFG, "Admin")
    admin.connect()
    admin.execute("UPDATE cdc_by_data_branch_db.meta_lock SET lock_owner=NULL, lock_time=NULL")
    admin.commit(); admin.close()
    
    print("[CRASH] Restarting CDC for recovery...")
    res = subprocess.run(["python3", "branch_cdc.py", "--config", "config_crash.json", "--once"], 
                         capture_output=True, text=True)
    
    print(f"DEBUG: Recovery STDOUT:\n{res.stdout}")
    print(f"DEBUG: Recovery STDERR:\n{res.stderr}")

    if res.returncode == 0:
        print("[CRASH] Recovery sync finished. Verifying consistency...")
        up = DBConnection(cfg["upstream"], "Up")
        ds = DBConnection(cfg["downstream"], "Ds")
        up.connect(); ds.connect()
        
        # Debugging Output
        u_cnt = up.fetch_one(f"SELECT count(*) as c FROM {up_db}.crash_tbl")['c']
        d_cnt = ds.fetch_one(f"SELECT count(*) as c FROM {ds_db}.crash_tbl")['c']
        print(f"DEBUG: Upstream Count: {u_cnt}, Downstream Count: {d_cnt}")
        
        if verify_consistency(up, ds, cfg):
            print("[PASS] Consistency reached after crash recovery.")
        else:
            print("[FAIL] Consistency NOT reached!")
            sys.exit(1)
    else:
        print(f"[FAIL] Recovery sync failed: {res.stderr}")
        sys.exit(1)

if __name__ == "__main__":
    test_crash_and_recovery()
