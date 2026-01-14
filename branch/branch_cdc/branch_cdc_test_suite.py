#!/usr/bin/env python3
import os, sys, time, shutil, pymysql
from rich.console import Console
from rich.panel import Panel

# Import the updated functions
from branch_cdc import (
    DBConnection, perform_sync, verify_consistency, load_config, save_config, 
    get_task_id, get_watermarks, META_DB, META_TABLE
)

console = Console()

# Test Config
TEST_UP_DB = "cdc_torture_up"
TEST_DS_DB1 = "cdc_torture_ds1"
TEST_DS_DB2 = "cdc_torture_ds2"
TEST_TABLE = "torture_tbl"
TEST_STAGE = "s1"

UP_CFG = {"host": "127.0.0.1", "port": 6001, "user": "dump", "password": "111", "db": TEST_UP_DB, "table": TEST_TABLE}
DS1_CFG = {"host": "127.0.0.1", "port": 6001, "user": "dump", "password": "111", "db": TEST_DS_DB1, "table": TEST_TABLE}
DS2_CFG = {"host": "127.0.0.1", "port": 6001, "user": "dump", "password": "111", "db": TEST_DS_DB2, "table": TEST_TABLE}

def setup_env():
    up = DBConnection(UP_CFG, "Up")
    if not up.connect(): sys.exit(1)
    for db in [TEST_UP_DB, TEST_DS_DB1, TEST_DS_DB2]:
        up.execute(f"DROP DATABASE IF EXISTS {db}"); up.commit()
        up.execute(f"CREATE DATABASE {db}"); up.commit()
    up.execute(f"CREATE TABLE {TEST_UP_DB}.{TEST_TABLE} (id INT PRIMARY KEY, val JSON, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"); up.commit()
    for i in range(100): # More rows for sampling
        up.execute(f"INSERT INTO {TEST_UP_DB}.{TEST_TABLE} (id, val) VALUES ({i}, '{{\"k\": {i}}}')")
    up.commit()
    up.execute(f"DROP STAGE IF EXISTS {TEST_STAGE}"); up.commit()
    stage_path = os.path.abspath(os.path.join(os.getcwd(), "..", "stage"))
    os.makedirs(stage_path, exist_ok=True)
    up.execute(f"CREATE STAGE {TEST_STAGE} URL='file://{stage_path}'"); up.commit()
    up.close()

def run_sync(ds_cfg):
    config = {"upstream": UP_CFG, "downstream": ds_cfg, "stage": {"name": TEST_STAGE}}
    save_config(config)
    return perform_sync(config)

def main():
    console.print(Panel.fit("MatrixOne BRANCH CDC Torture Test v3.2 (Review Verified)", style="bold magenta"))
    setup_env()

    # Phase 1: Initial Sync
    console.print("[bold yellow]PHASE 1: Initial Sync[/bold yellow]")
    if not run_sync(DS1_CFG): raise Exception("Initial sync failed")
    
    # Phase 2: Incremental
    console.print("[bold yellow]PHASE 2: Incremental Sync[/bold yellow]")
    up = DBConnection(UP_CFG, "Up"); up.connect()
    up.execute(f"INSERT INTO {TEST_UP_DB}.{TEST_TABLE} (id, val) VALUES (999, '{{\"k\": 999}}')"); up.commit(); up.close()
    if not run_sync(DS1_CFG): raise Exception("Incremental sync failed")

    # Phase 3: Archeology Recovery Test
    console.print("[bold yellow]PHASE 3: Archeology Meta Loss Recovery[/bold yellow]")
    ds1 = DBConnection(DS1_CFG, "Ds1"); ds1.connect()
    ds1.execute(f"DROP DATABASE IF EXISTS {META_DB}"); ds1.commit()
    
    # Review Fix: Capture log to verify Archeology path was taken
    log_path = os.path.join(os.getcwd(), "cdc_sync.log")
    with open(log_path, "w") as f: f.write("") # Clear log
    
    if not run_sync(DS1_CFG): raise Exception("Archeology recovery failed")
    
    with open(log_path, "r") as f:
        log_content = f.read()
        if "Watermark RECOVERED" not in log_content:
            raise Exception("Archeology logic was NOT triggered correctly!")
        if "FULL Check PASSED" not in log_content:
            raise Exception("Full verification after archeology was skipped!")
    console.print("[green]Archeology recovery and Full Re-verification PASSED![/green]")

    # Phase 4: Fast Check with Verify Columns
    console.print("[bold yellow]PHASE 4: Fast Check with Verify Columns[/bold yellow]")
    tamper_id = 42
    console.print(f"Tampering with ID {tamper_id} (verify_columns includes val)...")
    
    ds1.execute(f"UPDATE {TEST_DS_DB1}.{TEST_TABLE} SET val = '{{\"hacked\": true}}' WHERE id = {tamper_id}"); ds1.commit()
    
    up = DBConnection(UP_CFG, "Up"); up.connect()
    config = {"upstream": UP_CFG, "downstream": DS1_CFG, "verify_columns": ["val"]}
    tid = get_task_id(config); ws = get_watermarks(ds1, tid)
    if verify_consistency(up, ds1, config, ws[0], mode="fast"):
        raise Exception("Fast Check FAILED to catch data tampering with verify columns!")
    
    console.print("[green]Fast Check successfully caught corruption![/green]")
    console.print(Panel.fit("V3.2 REVIEWS FIXED & VERIFIED! [LEGENDARY HERO]", style="bold green"))

if __name__ == "__main__":
    main()
