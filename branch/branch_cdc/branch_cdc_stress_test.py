#!/usr/bin/env python3
import os
import sys
import time
import json
import random
import subprocess
import threading
import signal
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from branch_cdc import DBConnection, verify_consistency, load_config, save_config, get_task_id, get_watermarks

console = Console()

# --- Test Configuration ---
TEST_UP_DB = "cdc_stress_up"
TEST_TABLE = "stress_tbl"
TEST_STAGE = "cdc_stress_stage"
STAGE_DIR = "/tmp/mo_stress_test"
DS_DB = "cdc_stress_ds"

def setup_test_env():
    # Primary config for tools to reference
    config = {
        "upstream": {"host": "127.0.0.1", "port": 6001, "user": "dump", "password": "111", "db": TEST_UP_DB, "table": TEST_TABLE},
        "downstream": {"host": "127.0.0.1", "port": 6001, "user": "dump", "password": "111", "db": DS_DB, "table": TEST_TABLE},
        "stage": {"name": TEST_STAGE},
        "sync_interval": 5
    }
    # We are already inside branch_cdc folder
    save_config(config)
    
    conn = DBConnection({"host": "127.0.0.1", "port": 6001, "user": "dump", "password": "111", "db": "mo_catalog"}, "Setup")
    conn.connect()
    
    console.print("[yellow]Factory resetting stress test environment...[/yellow]")
    conn.execute(f"DROP DATABASE IF EXISTS {TEST_UP_DB}")
    conn.execute(f"DROP DATABASE IF EXISTS {DS_DB}")
    conn.execute(f"DROP DATABASE IF EXISTS cdc_by_data_branch_db")
    
    conn.execute(f"CREATE DATABASE {TEST_UP_DB}")
    conn.execute(f"CREATE DATABASE {DS_DB}")
    conn.execute(f"CREATE STAGE IF NOT EXISTS {TEST_STAGE} URL='file://{STAGE_DIR}'")
    os.makedirs(STAGE_DIR, exist_ok=True)
    
    # Large-ish table for stress (1000 rows is enough for logical stress)
    conn.execute(f"CREATE TABLE {TEST_UP_DB}.{TEST_TABLE} (id INT PRIMARY KEY, val VARCHAR(255), updated_at TIMESTAMP)")
    
    # Initial data
    data = [(i, f"initial_{i}", datetime.now()) for i in range(1, 1001)]
    with conn.conn.cursor() as cursor:
        cursor.executemany(f"INSERT INTO {TEST_UP_DB}.{TEST_TABLE} VALUES (%s, %s, %s)", data)
    
    conn.close()
    return config

class UpstreamUpdater(threading.Thread):
    def __init__(self):
        super().__init__()
        self._stop_event = threading.Event()
        self.cfg = load_config()["upstream"]

    def stop(self):
        self._stop_event.set()

    def run(self):
        conn = DBConnection(self.cfg, "WorkloadGen")
        if not conn.connect(): return
        console.print("[blue]Upstream Workload Generator STARTED.[/blue]")
        while not self._stop_event.is_set():
            try:
                # Randomly update 10% of data
                # Using floor(rand()*10) to select rows
                sql = f"UPDATE {TEST_TABLE} SET val = concat('v_', rand()), updated_at = NOW() WHERE id % 10 = floor(rand()*10)"
                conn.execute(sql)
                time.sleep(1) # Every second
            except Exception as e:
                console.print(f"[red]Workload error: {e}[/red]")
        conn.close()
        console.print("[blue]Upstream Workload Generator STOPPED.[/blue]")

def run_resilience_test(duration_sec=300, kill_interval=30):
    console.print(Panel.fit(f"STARTING RESILIENCE TEST ({duration_sec}s duration, kill every {kill_interval}s)", style="bold red"))
    
    updater = UpstreamUpdater()
    updater.start()
    
    start_time = time.time()
    sync_proc = None
    
    try:
        while time.time() - start_time < duration_sec:
            # 1. Start/Restart sync process
            if sync_proc is None or sync_proc.poll() is not None:
                console.print("[bold green]>>> Starting branch_cdc.py in Auto Mode...[/bold green]")
                sync_proc = subprocess.Popen([sys.executable, "branch_cdc.py", "--mode", "auto", "--interval", "5"])
            
            # 2. Wait for kill interval
            time.sleep(kill_interval)
            
            # 3. Kill the process
            console.print(f"[bold red]>>> KILLING sync process (PID: {sync_proc.pid})...[/bold red]")
            # Before killing, we must clear the lock in DB so the new process can start immediately
            # Otherwise it waits for 5 min timeout.
            # In a real crash, we wait. In this test, we want speed.
            sync_proc.send_signal(signal.SIGKILL)
            sync_proc.wait()
            
            # Helper: Clear lock to allow immediate restart
            conn = DBConnection(load_config()["downstream"], "LockCleaner")
            if conn.connect():
                conn.execute("UPDATE cdc_by_data_branch_db.meta SET lock_owner = NULL, lock_time = NULL")
                conn.close()

        console.print("[bold yellow]Duration reached. Stopping test...[/bold yellow]")
    
    finally:
        updater.stop()
        updater.join()
        if sync_proc:
            sync_proc.terminate()
            
    # Final catch-up sync
    console.print("[bold cyan]Performing final catch-up sync...[/bold cyan]")
    subprocess.run([sys.executable, "branch_cdc.py", "--once"])
    
    # Verify
    cfg = load_config()
    up = DBConnection(cfg["upstream"], "Up")
    ds = DBConnection(cfg["downstream"], "Ds")
    if up.connect() and ds.connect():
        task_id = get_task_id(cfg)
        w = get_watermarks(ds, task_id)
        if verify_consistency(up, ds, cfg, w[0]):
            console.print(Panel("STRESS & RESILIENCE TEST PASSED! [LEGENDARY HERO]", style="bold green"))
        else:
            console.print(Panel("STRESS TEST FAILED: INCONSISTENCY DETECTED", style="bold red"))
            sys.exit(1)

if __name__ == "__main__":
    config = setup_test_env()
    run_resilience_test()
