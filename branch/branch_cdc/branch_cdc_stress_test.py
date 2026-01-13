#!/usr/bin/env python3
import os, sys, time, subprocess, signal, random, pymysql, threading
from rich.console import Console
from rich.panel import Panel
from branch_cdc import save_config, DBConnection, verify_consistency, load_config

console = Console()
TEST_UP_DB, DS_DB, TEST_TABLE, TEST_STAGE = "cdc_stress_up", "cdc_stress_ds", "stress_tbl", "s1"

def setup_test_env():
    config = {
        "upstream": {"host": "127.0.0.1", "port": 6001, "user": "dump", "password": "111", "db": TEST_UP_DB, "table": TEST_TABLE},
        "downstream": {"host": "127.0.0.1", "port": 6001, "user": "dump", "password": "111", "db": DS_DB, "table": TEST_TABLE},
        "stage": {"name": TEST_STAGE},
        "sync_interval": 5
    }
    save_config(config)
    
    conn = pymysql.connect(host='127.0.0.1', port=6001, user='root', password='111', autocommit=True)
    with conn.cursor() as cur:
        for db in [TEST_UP_DB, DS_DB]:
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
            cur.execute(f"CREATE DATABASE {db}")
        cur.execute(f"CREATE TABLE {TEST_UP_DB}.{TEST_TABLE} (id INT PRIMARY KEY, val INT)")
        for i in range(1000): cur.execute(f"INSERT INTO {TEST_UP_DB}.{TEST_TABLE} VALUES ({i}, 0)")
        cur.execute(f"DROP STAGE IF EXISTS {TEST_STAGE}")
        # Ensure the stage directory exists relative to current work dir
        os.makedirs("../stage", exist_ok=True)
        cur.execute(f"CREATE STAGE {TEST_STAGE} URL='file://{os.getcwd()}/../stage'")
    conn.close()
    return config

def workload_gen(stop_event):
    while not stop_event.is_set():
        try:
            conn = pymysql.connect(host='127.0.0.1', port=6001, user='root', password='111', database=TEST_UP_DB, autocommit=True)
            with conn.cursor() as cur:
                # Update 100 rows randomly
                ids = random.sample(range(1000), 100)
                for i in ids: cur.execute(f"UPDATE {TEST_TABLE} SET val = val + 1 WHERE id = {i}")
            conn.close()
            time.sleep(1)
        except: time.sleep(1)

def main():
    console.print("Factory resetting stress test environment...")
    config = setup_test_env()
    
    stop_event = threading.Event()
    wl_thread = threading.Thread(target=workload_gen, args=(stop_event,))
    wl_thread.start()
    console.print("[green]Upstream Workload Generator STARTED.[/green]")

    console.print(Panel.fit("STARTING RESILIENCE TEST (300s duration, kill every 30s)", style="bold yellow"))
    
    start_time = time.time()
    try:
        while time.time() - start_time < 300:
            console.print(">>> Starting branch_cdc.py in Auto Mode...")
            # Run in auto mode. Review Fix: NOT clearing locks manually. 
            # It relies on acquire_lock's 30s timeout and owner check.
            cdc_proc = subprocess.Popen(["./branch_cdc.py", "--mode", "auto", "--interval", "5"])
            
            # Wait 35s and kill it (to allow it to at least try one sync and own the lock)
            time.sleep(35)
            console.print(f">>> KILLING sync process (PID: {cdc_proc.pid})...")
            cdc_proc.send_signal(signal.SIGKILL)
            cdc_proc.wait()
            
    finally:
        stop_event.set()
        wl_thread.join()
        console.print("Duration reached. Stopping test...")
        
    # Final check
    console.print("Performing final catch-up sync...")
    subprocess.run(["./branch_cdc.py", "--once"])
    
    up_conn = DBConnection(config['upstream'], "Up")
    ds_conn = DBConnection(config['downstream'], "Ds")
    if up_conn.connect() and ds_conn.connect():
        if verify_consistency(up_conn, ds_conn, config):
            console.print(Panel.fit("STRESS & RESILIENCE TEST PASSED! [LEGENDARY HERO]", style="bold green"))
        else:
            console.print(Panel.fit("STRESS TEST FAILED: Data Inconsistent!", style="bold red"))
        up_conn.close(); ds_conn.close()

if __name__ == "__main__":
    main()
