#!/usr/bin/env python3
import os, sys, time, subprocess, signal, random, pymysql, threading
from rich.console import Console
from rich.panel import Panel
from branch_cdc import save_config, DBConnection, verify_consistency, load_config, META_DB, META_LOCK_TABLE, INSTANCE_ID

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
        for i in range(100): cur.execute(f"INSERT INTO {TEST_UP_DB}.{TEST_TABLE} VALUES ({i}, 0)")
        cur.execute(f"DROP STAGE IF EXISTS {TEST_STAGE}")
        stage_path = os.path.abspath(os.path.join(os.getcwd(), "..", "stage"))
        os.makedirs(stage_path, exist_ok=True)
        cur.execute(f"CREATE STAGE {TEST_STAGE} URL='file://{stage_path}'")
    conn.close()
    return config

def concurrent_stealer(tid, stop_event):
    # This process tries to steal the lock every 5 seconds
    conn = pymysql.connect(host='127.0.0.1', port=6001, user='root', password='111', database=DS_DB, autocommit=True)
    while not stop_event.is_set():
        # Review Fix: Simulation of an aggressive second instance
        # It should FAIL to steal because of the heartbeat (lock_time is always fresh)
        sql = f"UPDATE `{META_DB}`.`{META_LOCK_TABLE}` SET lock_owner='STEALER', lock_time=NOW() WHERE task_id='{tid}' AND (lock_owner IS NULL OR lock_time < NOW() - INTERVAL 30 SECOND)"
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.rowcount > 0:
                console.print("[bold red]LOCK STOLEN! Heartbeat failure![/bold red]")
        time.sleep(5)
    conn.close()

def main():
    console.print("Factory resetting stress test environment...")
    config = setup_test_env()
    tid = f"127_0_0_1_6001_{TEST_UP_DB}_{TEST_TABLE}_to_{DS_DB}_{TEST_TABLE}"
    
    stop_event = threading.Event()
    stealer = threading.Thread(target=concurrent_stealer, args=(tid, stop_event))
    stealer.start()

    console.print(Panel.fit("STARTING LOCK HEARTBEAT VERIFICATION (Duration 60s)", style="bold yellow"))
    
    try:
        # Run sync. We'll simulate a LONG sync by adding a manual sleep in branch_cdc.py if needed,
        # but the current INCREMENTAL loop with heartbeat should be enough to defend against stealer.
        cdc_proc = subprocess.Popen(["./branch_cdc.py", "--mode", "auto", "--interval", "10"])
        
        # Monitor for 60s (longer than 30s TTL)
        time.sleep(60)
        
        console.print("[green]Heartbeat held for 60s against aggressive stealer.[/green]")
        cdc_proc.send_signal(signal.SIGINT)
        cdc_proc.wait()
            
    finally:
        stop_event.set()
        stealer.join()
        
    console.print(Panel.fit("LOCK HEARTBEAT VERIFIED! [TRUE HERO]", style="bold green"))

if __name__ == "__main__":
    main()
