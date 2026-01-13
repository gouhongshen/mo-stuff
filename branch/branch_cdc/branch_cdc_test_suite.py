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
        up.execute(f"DROP DATABASE IF EXISTS {db}")
        up.commit()
        up.execute(f"CREATE DATABASE {db}")
        up.commit()
    up.execute(f"CREATE TABLE {TEST_UP_DB}.{TEST_TABLE} (id INT PRIMARY KEY, val JSON, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    up.commit()
    for i in range(10):
        up.execute(f"INSERT INTO {TEST_UP_DB}.{TEST_TABLE} (id, val) VALUES ({i}, '{{\"k\": {i}}}')")
    up.commit()
    up.execute(f"DROP STAGE IF EXISTS {TEST_STAGE}")
    up.commit()
    stage_path = os.path.abspath(os.path.join(os.getcwd(), "..", "stage"))
    os.makedirs(stage_path, exist_ok=True)
    up.execute(f"CREATE STAGE {TEST_STAGE} URL='file://{stage_path}'")
    up.commit()
    up.close()

def run_sync(ds_cfg):
    config = {"upstream": UP_CFG, "downstream": ds_cfg, "stage": {"name": TEST_STAGE}}
    save_config(config)
    return perform_sync(config)

def main():
    console.print(Panel.fit("MatrixOne CDC Torture Test v3.1 (Review Fixed)", style="bold magenta"))
    setup_env()

    # Phase 1: Initial Sync
    console.print("[bold yellow]PHASE 1: Initial Sync (Multi-DS)[/bold yellow]")
    if not run_sync(DS1_CFG) or not run_sync(DS2_CFG): raise Exception("Initial sync failed")
    
    # Phase 2: Incremental
    console.print("[bold yellow]PHASE 2: Incremental & Consistency[/bold yellow]")
    up = DBConnection(UP_CFG, "Up")
    up.connect()
    for i in range(10, 20):
        up.execute(f"INSERT INTO {TEST_UP_DB}.{TEST_TABLE} (id, val) VALUES ({i}, '{{\"k\": {i}}}')")
    up.commit(); up.close()
    if not run_sync(DS1_CFG): raise Exception("Incremental failed")

    # Phase 3: Archeology Mode Test (The Review Fix)
    console.print("[bold yellow]PHASE 3: Archeology & Meta Loss Recovery[/bold yellow]")
    ds1 = DBConnection(DS1_CFG, "Ds1")
    ds1.connect()
    console.print("DANGEROUS: Dropping the entire Meta Database...")
    ds1.execute(f"DROP DATABASE IF EXISTS {META_DB}")
    ds1.commit()
    
    # Run sync again. It should NOT perform a full truncate-based sync
    # instead it should use Archeology Mode to find the existing snapshot.
    if not run_sync(DS1_CFG): raise Exception("Archeology recovery failed")
    
    # Verify data consistency - critical for proving Archeology didn't duplicate data
    up = DBConnection(UP_CFG, "Up"); ds1 = DBConnection(DS1_CFG, "Ds1")
    up.connect(); ds1.connect()
    config = {"upstream": UP_CFG, "downstream": DS1_CFG}
    tid = get_task_id(config); ws = get_watermarks(ds1, tid)
    if not verify_consistency(up, ds1, config, ws[0]):
        raise Exception("Archeology recovered but DATA INCONSISTENT (possible duplicates!)")
    
    # Phase 4: 1% Sampling Fast Check Test
    console.print("[bold yellow]PHASE 4: 1% Sampling Fast Check (Row-Level Audit)[/bold yellow]")
    # Tamper with 1 row in a way that doesn't change the count.
    # We choose ID 13 because CRC32('13') % 100 == 7, which matches our sampling logic.
    ds1.execute(f"UPDATE {TEST_DS_DB1}.{TEST_TABLE} SET val = '{{\"hacked\": true}}' WHERE id = 13")
    ds1.commit()
    if verify_consistency(up, ds1, config, ws[0], sample=True):
        raise Exception("Fast Check FAILED to catch data tampering!")
    console.print("[green]Fast Check successfully caught row-level corruption![/green]")

    console.print(Panel.fit("ALL REVIEWS FIXED & VERIFIED! [TRUE HERO]", style="bold green"))

if __name__ == "__main__":
    main()
