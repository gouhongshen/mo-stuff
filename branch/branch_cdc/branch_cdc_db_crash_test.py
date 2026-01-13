#!/usr/bin/env python3
import os, sys, time, subprocess, signal, random, pymysql, threading
from rich.console import Console

# Review Fix: Removed hardcoded MO_ROOT, use relative path to matrixone repo
# Assuming this script is in branch/branch_cdc/
# Repo root is at ../../../matrixone-1st
REL_MO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "matrixone-1st"))
LAUNCH_CMD = ["./mo-service", "-debug-http=:11235", "-launch", "etc/launch/launch.toml"]

console = Console()
TEST_UP_DB, TEST_DS_DB, TEST_TABLE = "cdc_crash_up", "cdc_crash_ds", "crash_tbl"

def restart_mo():
    console.print("[bold red]>>> KILLING mo-service...[/bold red]")
    os.system("pkill -9 mo-service")
    time.sleep(2)
    console.print("[bold green]Starting MatrixOne mo-service...[/bold green]")
    # Review Fix: Log file also uses REL_MO_ROOT path
    with open(os.path.join(REL_MO_ROOT, "crash_test_mo.log"), "a") as log_file:
        subprocess.Popen(LAUNCH_CMD, cwd=REL_MO_ROOT, stdout=log_file, stderr=log_file)
    # Wait for MO to be ready
    for _ in range(30):
        try:
            conn = pymysql.connect(host='127.0.0.1', port=6001, user='root', password='111', autocommit=True)
            conn.close(); return True
        except: time.sleep(1)
    return False

def workload_gen():
    while not stop_event.is_set():
        try:
            conn = pymysql.connect(host='127.0.0.1', port=6001, user='root', password='111', database=TEST_UP_DB, autocommit=True)
            with conn.cursor() as cur:
                id_val = random.randint(0, 999)
                cur.execute(f"UPDATE {TEST_TABLE} SET val = val + 1 WHERE id = {id_val}")
            conn.close()
            time.sleep(0.1)
        except: time.sleep(1)

def main():
    global stop_event
    stop_event = threading.Event()
    
    console.print(Panel.fit("STARTING REAL-TIME DATABASE CRASH TEST", style="bold red"))
    
    # 1. Setup environment
    conn = pymysql.connect(host='127.0.0.1', port=6001, user='root', password='111', autocommit=True)
    with conn.cursor() as cur:
        for db in [TEST_UP_DB, TEST_DS_DB]:
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
            cur.execute(f"CREATE DATABASE {db}")
        cur.execute(f"CREATE TABLE {TEST_UP_DB}.{TEST_TABLE} (id INT PRIMARY KEY, val INT)")
        for i in range(1000): cur.execute(f"INSERT INTO {TEST_UP_DB}.{TEST_TABLE} VALUES ({i}, 0)")
    conn.close()

    # 2. Start CDC and Workload
    console.print("Starting branch_cdc.py in background...")
    cdc_proc = subprocess.Popen(["./branch_cdc.py", "--mode", "auto", "--interval", "5"], stdout=subprocess.DEVNULL)
    
    wl_thread = threading.Thread(target=workload_gen)
    wl_thread.start()
    console.print("[green]Upstream Workload Generator CONNECTED.[/green]")

    try:
        # Run for 2 cycles of crash
        for cycle in range(2):
            time.sleep(30) # Let it sync some data
            if not restart_mo(): raise Exception("MO failed to restart")
            console.print(f"[bold yellow]Cycle {cycle+1} recovery complete.[/bold yellow]")
        
        time.sleep(20) # Catch up
        stop_event.set()
        wl_thread.join()
        cdc_proc.send_signal(signal.SIGINT)
        cdc_proc.wait()

        # Final Verification
        console.print("Performing final catch-up and verification...")
        os.system("./branch_cdc.py --once") # One final sync
        
        # We check consistency manually here
        conn = pymysql.connect(host='127.0.0.1', port=6001, user='root', password='111', autocommit=True)
        with conn.cursor() as cur:
            cur.execute(f"SELECT BIT_XOR(CRC32(val)) FROM {TEST_UP_DB}.{TEST_TABLE}")
            h1 = cur.fetchone()[0]
            cur.execute(f"SELECT BIT_XOR(CRC32(val)) FROM {TEST_DS_DB}.{TEST_TABLE}")
            h2 = cur.fetchone()[0]
            if h1 == h2: console.print(Panel.fit("ULTIMATE DATABASE CRASH TEST PASSED! [UNSTOPPABLE HERO]", style="bold green"))
            else: console.print(Panel.fit("CRASH TEST FAILED: Hash Mismatch!", style="bold red"))
        conn.close()

    finally:
        stop_event.set()
        if 'wl_thread' in locals(): wl_thread.join()
        cdc_proc.kill()

if __name__ == "__main__":
    from rich.panel import Panel
    main()