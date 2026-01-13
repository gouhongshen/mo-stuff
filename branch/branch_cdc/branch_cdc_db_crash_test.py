#!/usr/bin/env python3
import os
import sys
import time
import subprocess
import signal
import threading
import random
from datetime import datetime
from rich.console import Console
from rich.panel import Panel
from branch_cdc import DBConnection, verify_consistency, load_config, get_task_id, get_watermarks, save_config

console = Console()

# --- Configuration ---
MO_ROOT = "/Users/ghs-mo/MOWorkSpace/matrixone-1st"
LAUNCH_CMD = ["./mo-service", "-debug-http=:11235", "-launch", "etc/launch/launch.toml"]
TEST_UP_DB = "cdc_crash_up"
TEST_TABLE = "crash_tbl"
TEST_STAGE = "cdc_crash_stage"
STAGE_DIR = "/tmp/mo_crash_test"
DS_DB = "cdc_crash_ds"

def start_mo():
    console.print("[bold green]Starting MatrixOne mo-service...[/bold green]")
    with open(os.path.join(MO_ROOT, "crash_test_mo.log"), "a") as log_file:
        subprocess.Popen(LAUNCH_CMD, cwd=MO_ROOT, stdout=log_file, stderr=log_file)
    
    for i in range(60):
        try:
            conn = DBConnection({"host": "127.0.0.1", "port": 6001, "user": "dump", "password": "111", "db": "mo_catalog"}, "Probe")
            if conn.connect():
                conn.close()
                return True
        except: pass
        time.sleep(1)
    return False

def kill_mo():
    console.print("[bold red]>>> KILLING mo-service...[/bold red]")
    subprocess.run(["pkill", "-9", "mo-service"])
    time.sleep(2)

class UpstreamUpdater(threading.Thread):
    def __init__(self, cfg):
        super().__init__()
        self._stop_event = threading.Event()
        self.cfg = cfg

    def stop(self):
        self._stop_event.set()

    def run(self):
        # We need a retry loop here because MO might be down during the test
        while not self._stop_event.is_set():
            conn = DBConnection(self.cfg, "WorkloadGen")
            if conn.connect():
                console.print("[blue]Upstream Workload Generator CONNECTED.[/blue]")
                while not self._stop_event.is_set():
                    try:
                        sql = f"UPDATE {TEST_TABLE} SET val = concat('v_', rand()), ts = NOW() WHERE id % 10 = floor(rand()*10)"
                        conn.execute(sql)
                        time.sleep(0.5) # High frequency update
                    except:
                        break # Connection lost, try reconnecting
                conn.close()
            time.sleep(1)

def setup_test_env():
    if not start_mo():
        console.print("[red]Failed to start MO initially.[/red]"); sys.exit(1)
    
    conn = DBConnection({"host": "127.0.0.1", "port": 6001, "user": "dump", "password": "111", "db": "mo_catalog"}, "Setup")
    conn.connect()
    conn.execute(f"DROP DATABASE IF EXISTS {TEST_UP_DB}")
    conn.execute(f"DROP DATABASE IF EXISTS {DS_DB}")
    conn.execute(f"DROP DATABASE IF EXISTS cdc_by_data_branch_db")
    conn.execute(f"CREATE DATABASE {TEST_UP_DB}")
    conn.execute(f"CREATE DATABASE {DS_DB}")
    conn.execute(f"CREATE STAGE IF NOT EXISTS {TEST_STAGE} URL='file://{STAGE_DIR}'")
    os.makedirs(STAGE_DIR, exist_ok=True)
    conn.execute(f"CREATE TABLE {TEST_UP_DB}.{TEST_TABLE} (id INT PRIMARY KEY, val TEXT, ts TIMESTAMP)")
    
    data = [(i, f"init_{i}", datetime.now()) for i in range(1, 1001)]
    with conn.conn.cursor() as cursor:
        cursor.executemany(f"INSERT INTO {TEST_UP_DB}.{TEST_TABLE} VALUES (%s, %s, %s)", data)
    
    config = {
        "upstream": {"host": "127.0.0.1", "port": 6001, "user": "dump", "password": "111", "db": TEST_UP_DB, "table": TEST_TABLE},
        "downstream": {"host": "127.0.0.1", "port": 6001, "user": "dump", "password": "111", "db": DS_DB, "table": TEST_TABLE}, # Note: use DS_DB
        "stage": {"name": TEST_STAGE},
        "sync_interval": 5
    }
    # Fix config downstream db
    config["downstream"]["db"] = DS_DB
    save_config(config)
    conn.close()
    return config

def run_db_crash_test(duration_sec=300):
    console.print(Panel.fit("STARTING REAL-TIME DATABASE CRASH TEST", style="bold red"))
    cfg = load_config()
    
    updater = UpstreamUpdater(cfg["upstream"])
    updater.start()
    
    console.print("[green]Starting branch_cdc.py in background...[/green]")
    sync_proc = subprocess.Popen([sys.executable, "branch_cdc.py", "--mode", "auto", "--interval", "5"])
    
    start_time = time.time()
    try:
        while time.time() - start_time < duration_sec:
            time.sleep(45)
            kill_mo()
            time.sleep(15)
            if not start_mo(): break
            time.sleep(15)
        console.print("[bold yellow]Test duration reached.[/bold yellow]")
    finally:
        updater.stop()
        updater.join()
        sync_proc.terminate()
        start_mo()

    console.print("[bold cyan]Performing final catch-up and verification...[/bold cyan]")
    # Stop workload, run one last sync, then verify
    subprocess.run([sys.executable, "branch_cdc.py", "--once"])
    
    up = DBConnection(cfg["upstream"], "Up")
    ds = DBConnection(cfg["downstream"], "Ds")
    if up.connect() and ds.connect():
        task_id = get_task_id(cfg)
        w = get_watermarks(ds, task_id)
        if verify_consistency(up, ds, cfg, w[0]):
            console.print(Panel("ULTIMATE DATABASE CRASH TEST PASSED! [UNSTOPPABLE HERO]", style="bold green"))
        else:
            console.print(Panel("ULTIMATE DATABASE CRASH TEST FAILED!", style="bold red"))
            sys.exit(1)

if __name__ == "__main__":
    config = setup_test_env()
    run_db_crash_test()