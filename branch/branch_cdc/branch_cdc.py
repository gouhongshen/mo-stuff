#!/usr/bin/env python3
import sys, os, json, time, re, logging, argparse, uuid, socket, hashlib, threading
from datetime import datetime
from typing import Optional, Dict, Any, List
import pymysql
import questionary
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn

# --- Setup ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.makedirs(BASE_DIR, exist_ok=True)

parser = argparse.ArgumentParser(description="MatrixOne CDC Tool v3.3")
parser.add_argument("--log-file", default=os.path.join(BASE_DIR, "cdc_sync.log"))
parser.add_argument("--mode", choices=["manual", "auto"])
parser.add_argument("--interval", type=int)
parser.add_argument("--verify-interval", type=int)
parser.add_argument("--once", action="store_true")
cli_args, _ = parser.parse_known_args()

CONFIG_FILE = os.path.join(BASE_DIR, "config.json")
META_DB, META_TABLE = "cdc_by_data_branch_db", "meta"
MAX_WATERMARKS, LOCK_TIMEOUT_SEC = 4, 30
INSTANCE_ID = f"{socket.gethostname()}_{os.getpid()}_{uuid.uuid4().hex[:6]}"

console = Console()
logging.basicConfig(level="INFO", format="%(message)s", handlers=[RichHandler(console=console, markup=True), logging.FileHandler(cli_args.log_file)])
log = logging.getLogger("rich")

# --- DB Helper ---
class DBConnection:
    def __init__(self, config, name): self.config, self.name, self.conn = config, name, None
    def connect(self, use_db=True):
        try:
            db_name = self.config.get("db") if use_db else None
            self.conn = pymysql.connect(
                host=self.config["host"], port=int(self.config["port"]),
                user=self.config["user"], password=self.config["password"],
                database=db_name, charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor, local_infile=True, autocommit=False
            )
            self.conn.commit(); return True
        except pymysql.err.OperationalError as e:
            if e.args[0] == 1049 and use_db: return self.connect(use_db=False)
            log.error(f"Connect {self.name} fail: {e}"); return False
        except Exception as e: log.error(f"Connect {self.name} fail: {e}"); return False
    def close(self):
        if self.conn: self.conn.close()
    def query(self, sql, args=None):
        with self.conn.cursor() as cursor: cursor.execute(sql, args); return cursor.fetchall()
    def execute(self, sql, args=None):
        with self.conn.cursor() as cursor: cursor.execute(sql, args)
    def commit(self): self.conn.commit()
    def rollback(self): self.conn.rollback()
    def fetch_one(self, sql, args=None):
        res = self.query(sql, args); return res[0] if res else None

# --- Meta & Lock Logic ---
def load_config():
    if not os.path.exists(CONFIG_FILE): return {}
    with open(CONFIG_FILE, "r") as f: return json.load(f)
def save_config(config):
    with open(CONFIG_FILE, "w") as f: json.dump(config, f, indent=4)
def get_task_id(config):
    u, d = config["upstream"], config["downstream"]
    return f"{u['host']}_{u['port']}_{u['db']}_{u['table']}_to_{d['db']}_{d['table']}".replace(".", "_")

def ensure_meta_table(ds_conn):
    ds_conn.execute(f"CREATE DATABASE IF NOT EXISTS `{META_DB}`"); ds_conn.commit()
    ds_conn.execute(f"USE `{META_DB}`")
    ds_conn.execute(f"CREATE TABLE IF NOT EXISTS `{META_TABLE}` (task_id VARCHAR(255), watermark VARCHAR(255), lock_owner VARCHAR(255), lock_time TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"); ds_conn.commit()

def acquire_lock(ds_conn, tid):
    if not ds_conn.query(f"SELECT 1 FROM `{META_DB}`.`{META_TABLE}` WHERE task_id = %s", (tid,)):
        ds_conn.execute(f"INSERT INTO `{META_DB}`.`{META_TABLE}` (task_id) VALUES (%s)", (tid,)); ds_conn.commit()
    sql = f"UPDATE `{META_DB}`.`{META_TABLE}` SET lock_owner=%s, lock_time=NOW() WHERE task_id=%s AND (lock_owner IS NULL OR lock_owner=%s OR lock_time < NOW() - INTERVAL {LOCK_TIMEOUT_SEC} SECOND)"
    ds_conn.execute(sql, (INSTANCE_ID, tid, INSTANCE_ID)); ds_conn.commit()
    r = ds_conn.fetch_one(f"SELECT lock_owner FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s", (tid,))
    return r and r['lock_owner'] == INSTANCE_ID

class LockKeeper(threading.Thread):
    def __init__(self, ds_config, tid):
        super().__init__()
        self.ds_config, self.tid, self.stop_event = ds_config, tid, threading.Event()
    def run(self):
        conn = DBConnection(self.ds_config, "LockKeeper")
        if not conn.connect(): return
        while not self.stop_event.wait(10):
            try:
                conn.execute(f"UPDATE `{META_DB}`.`{META_TABLE}` SET lock_time=NOW() WHERE task_id=%s AND lock_owner=%s", (self.tid, INSTANCE_ID)); conn.commit()
            except: pass
        conn.close()
    def stop(self): self.stop_event.set()

def release_lock(ds_conn, tid):
    try:
        ds_conn.execute(f"UPDATE `{META_DB}`.`{META_TABLE}` SET lock_owner=NULL WHERE task_id=%s AND lock_owner=%s", (tid, INSTANCE_ID)); ds_conn.commit()
    except: pass

def get_watermarks(ds_conn, tid):
    return [r['watermark'] for r in ds_conn.query(f"SELECT watermark FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s AND watermark IS NOT NULL ORDER BY created_at DESC", (tid,))]

# --- Verification ---
def get_check_sql(conn, db, table, snap=None, sample=False):
    sc = f"{{snapshot = '{snap}'}}" if snap else ""
    try: cols = conn.query(f"SHOW COLUMNS FROM `{db}`.`{table}`")
    except: return "SELECT 0 as c, 0 as h"
    pk_col = next((c['Field'] for c in cols if c['Key'] == 'PRI'), "__mo_fake_pk_col")
    p_cols = [f"IFNULL(HEX(`{c['Field']}`), 'NULL')" if "vec" in c['Type'].lower() else f"IFNULL(CAST(`{c['Field']}` AS VARCHAR), 'NULL')" for c in cols]
    where = f" WHERE ABS(CRC32(CAST(`{pk_col}` AS VARCHAR))) % 100 = 7" if sample else ""
    return f"SELECT COUNT(*) as c, BIT_XOR(CRC32(CONCAT_WS(',', {', '.join(p_cols)}))) as h FROM `{db}`.`{table}`{sc}{where}"

def verify_consistency(up_conn, ds_conn, config, snap=None, sample=False):
    u, d = config["upstream"], config["downstream"]
    try:
        ur, dr = up_conn.fetch_one(get_check_sql(up_conn, u['db'], u['table'], snap, sample)), ds_conn.fetch_one(get_check_sql(ds_conn, d['db'], d['table'], None, sample))
        if ur['c'] == dr['c'] and (ur['h'] == dr['h'] or (ur['h'] is None and dr['h'] is None)):
            log.info(f"[green]{'FAST' if sample else 'FULL'} Check PASSED | Rows: {ur['c']} | Hash: {ur['h']}[/green]"); return True
        log.error(f"Verify FAILED | Up({ur['c']},{ur['h']}) vs Ds({dr['c']},{dr['h']})"); return False
    except Exception as e: log.error(f"Verify error: {e}"); return False

def generate_snapshot_name(tid):
    sid = hashlib.md5(tid.encode()).hexdigest()[:12]
    return f"cdc_{sid}_{datetime.now().strftime('%y%m%d%H%M%S%f')[:-3]}"

def archeology_recovery(up_conn, ds_conn, config, tid):
    log.info("[dim]Archeology Mode: Searching for historical snapshots...[/dim]")
    sid_prefix = hashlib.md5(tid.encode()).hexdigest()[:12]
    try: snaps = up_conn.query(f"SELECT sname FROM mo_catalog.mo_snapshots WHERE sname LIKE 'cdc_{sid_prefix}_%' ORDER BY ts DESC LIMIT 10")
    except: return None
    for s in snaps:
        snap = s['sname']
        if verify_consistency(up_conn, ds_conn, config, snap, sample=True):
            log.info(f"[cyan]Fast match found: {snap}. Verifying with FULL check...[/cyan]")
            if verify_consistency(up_conn, ds_conn, config, snap, sample=False):
                log.info(f"[green]Watermark RECOVERED: {snap}[/green]")
                ds_conn.execute(f"INSERT INTO `{META_DB}`.`{META_TABLE}` (task_id, watermark) VALUES (%s, %s)", (tid, snap)); ds_conn.commit(); return snap
    return None

def perform_sync(config, is_auto=False):
    u_cfg, d_cfg = config["upstream"], config["downstream"]
    up_conn, ds_conn = DBConnection(u_cfg, "Up"), DBConnection(d_cfg, "Ds")
    if not up_conn.connect() or not ds_conn.connect(): return False
    tid, newsnap, sync_success = get_task_id(config), None, False
    keeper = LockKeeper(d_cfg, tid)
    try:
        ensure_meta_table(ds_conn)
        if not acquire_lock(ds_conn, tid): return False
        keeper.start()
        ds_conn.execute(f"CREATE DATABASE IF NOT EXISTS `{d_cfg['db']}`"); ds_conn.commit()
        ds_conn.execute(f"USE `{d_cfg['db']}`")
        if not ds_conn.query(f"SHOW TABLES LIKE '{d_cfg['table']}'"):
            ddl = up_conn.fetch_one(f"SHOW CREATE TABLE `{u_cfg['db']}`.`{u_cfg['table']}`")['Create Table'].replace(f"`{u_cfg['table']}`", f"`{d_cfg['table']}`", 1)
            ds_conn.execute(ddl); ds_conn.commit()
        ws = get_watermarks(ds_conn, tid)
        is_archeology, lastgood = False, (ws[0] if ws else archeology_recovery(up_conn, ds_conn, config, tid))
        if not ws and lastgood: is_archeology = True
        up_conn.commit(); ds_conn.commit(); newsnap = generate_snapshot_name(tid)
        if not lastgood:
            log.info(f"[blue]FULL Sync | Task: {tid}[/blue]")
            t_zero = f"{u_cfg['table']}_zero"
            up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_zero}`"); up_conn.commit()
            up_conn.execute(f"CREATE TABLE `{u_cfg['db']}`.`{t_zero}` LIKE `{u_cfg['db']}`.`{u_cfg['table']}`"); up_conn.commit()
            up_conn.execute(f"CREATE SNAPSHOT `{newsnap}` FOR TABLE `{u_cfg['db']}` `{u_cfg['table']}`"); up_conn.commit()
            try: dfiles = up_conn.query(f"data branch diff `{u_cfg['db']}`.`{u_cfg['table']}`{{snapshot='{newsnap}'}} against `{u_cfg['db']}`.`{t_zero}` output file 'stage://{config['stage']['name']}'")
            finally: up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_zero}`"); up_conn.commit()
            ds_conn.conn.begin()
            try:
                ds_conn.execute(f"TRUNCATE TABLE `{d_cfg['db']}`.`{d_cfg['table']}`")
                for r in dfiles:
                    f, slash, quote = list(r.values())[0], chr(92), chr(34)
                    ds_conn.execute(f"LOAD DATA INFILE '{f}' INTO TABLE `{d_cfg['db']}`.`{d_cfg['table']}` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '{quote}' ESCAPED BY '{slash}{slash}' LINES TERMINATED BY '{slash}n' PARALLEL 'TRUE'")
                ds_conn.execute(f"INSERT INTO `{META_DB}`.`{META_TABLE}` (task_id, watermark) VALUES (%s, %s)", (tid, newsnap)); ds_conn.commit(); sync_success = True
            except Exception as e: ds_conn.rollback(); raise e
        else:
            log.info(f"[blue]INCREMENTAL Sync | Task: {tid} | From {lastgood}{' (via archeology)' if is_archeology else ''}[/blue]")
            # Review Fix: Robust check if snapshot exists BEFORE any other operation
            try:
                snap_exists = up_conn.fetch_one(f"SELECT sname FROM mo_catalog.mo_snapshots WHERE sname = '{lastgood}'")
                if not snap_exists:
                    raise RuntimeError("SNAPSHOT_LOST_FALLBACK")
            except Exception as e:
                if "SNAPSHOT_LOST_FALLBACK" in str(e): raise e
                log.warning(f"Error checking snapshot: {e}")
            
            t_cp = f"{u_cfg['table']}_copy_prev"
            up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_cp}`"); up_conn.commit()
            up_conn.execute(f"data branch create table `{u_cfg['db']}`.`{t_cp}` from `{u_cfg['db']}`.`{u_cfg['table']}`{{snapshot='{lastgood}'}}"); up_conn.commit()
            up_conn.execute(f"CREATE SNAPSHOT `{newsnap}` FOR TABLE `{u_cfg['db']}` `{u_cfg['table']}`"); up_conn.commit()
            try: dfiles = up_conn.query(f"data branch diff `{u_cfg['db']}`.`{u_cfg['table']}`{{snapshot = '{newsnap}'}} against `{u_cfg['db']}`.`{t_cp}` output file 'stage://{config['stage']['name']}'")
            except Exception as e:
                if any(k in str(e).lower() for k in ["find 0 snapshot", "not exist", "20101"]):
                    log.warning(f"Snapshot {lastgood} missing. Forcing FULL sync fallback. Error: {e}")
                    ds_conn.execute(f"DELETE FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s", (tid,)); ds_conn.commit()
                    raise RuntimeError("SNAPSHOT_LOST_FALLBACK")
                raise e
            finally: up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_cp}`"); up_conn.commit()
            ds_conn.conn.begin()
            try:
                for r in dfiles:
                    raw = up_conn.fetch_one(f"select load_file(cast('{list(r.values())[0]}' as datalink)) as c")['c']
                    stmt_str = raw.decode('utf-8') if isinstance(raw, bytes) else raw
                    stmt_str = re.sub(r"^\s*(BEGIN|COMMIT|ROLLBACK|START\s+TRANSACTION)\s*;?\s*", "", stmt_str, flags=re.I|re.M)
                    stmt_str = re.sub(r";\s*(COMMIT|ROLLBACK|END)\s*;?\s*$", ";", stmt_str, flags=re.I|re.M)
                    target = f"`{d_cfg['db']}`.`{d_cfg['table']}`"
                    sql = re.compile(r'(delete\s+from\s+|replace\s+into\s+|insert\s+into\s+|update\s+)(/\*.*?\*/\s+)?([`\w]+\.[`\w]+|[`\w]+)', re.I|re.S).sub(lambda m: f"{m.group(1)}{m.group(2) or ''} {target} ", stmt_str)
                    for s in [st.strip() for st in sql.split(';') if st.strip()]:
                        if not s or s.upper().startswith(("BEGIN", "COMMIT", "ROLLBACK")): continue
                        ds_conn.execute(s)
                ds_conn.execute(f"INSERT INTO `{META_DB}`.`{META_TABLE}` (task_id, watermark) VALUES (%s, %s)", (tid, newsnap)); ds_conn.commit(); sync_success = True
            except Exception as e: ds_conn.rollback(); raise e
        if sync_success:
            for r in dfiles:
                try: up_conn.execute(f"REMOVE '{list(r.values())[0]}'"); up_conn.commit()
                except: pass
            allw = get_watermarks(ds_conn, tid)
            if len(allw) > MAX_WATERMARKS:
                for w in allw[MAX_WATERMARKS:]:
                    ds_conn.execute(f"DELETE FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s AND watermark=%s", (tid, w)); ds_conn.commit()
                    try: up_conn.execute(f"DROP SNAPSHOT IF EXISTS `{w}`"); up_conn.commit()
                    except: pass
            return True
    except Exception as e:
        if str(e) == "SNAPSHOT_LOST_FALLBACK":
            keeper.stop(); keeper.join(); release_lock(ds_conn, tid); up_conn.close(); ds_conn.close()
            log.info("[yellow]Retrying with FULL sync now...[/yellow]"); return perform_sync(config, is_auto)
        log.error(f"Sync FAILED: {e}"); return False
    finally:
        keeper.stop(); keeper.join(); release_lock(ds_conn, tid); up_conn.close(); ds_conn.close()

def sync_loop(config, interval):
    sc = 0
    while True:
        v_int = config.get("verify_interval", 50)
        if perform_sync(config, is_auto=True):
            sc += 1
            if sc % 5 == 0 or (v_int > 0 and sc % v_int == 0):
                up, ds = DBConnection(config["upstream"], "Up"), DBConnection(config["downstream"], "Ds")
                if up.connect() and ds.connect():
                    ws = get_watermarks(ds, get_task_id(config))
                    if ws: verify_consistency(up, ds, config, ws[0], sample=not (v_int > 0 and sc % v_int == 0))
                    up.close(); ds.close()
        time.sleep(interval)

def main():
    console.print(Panel.fit("MatrixOne branch_cdc v3.3", style="bold magenta"))
    config = load_config()
    if not config: config = setup_config()
    if cli_args.once: perform_sync(config); sys.exit(0)
    if cli_args.mode == "auto": sync_loop(config, cli_args.interval or config.get("sync_interval", 60)); sys.exit(0)
    while True:
        c = questionary.select("Mode:", choices=["Manual Sync Now", "Verify Consistency", "Automatic Mode", "Edit Configuration", "Exit"]).ask()
        if c == "Exit": sys.exit(0)
        elif c == "Edit Configuration": config = setup_config(config)
        elif c == "Verify Consistency":
            up, ds = DBConnection(config["upstream"], "Up"), DBConnection(config["downstream"], "Ds")
            if up.connect() and ds.connect():
                tid = get_task_id(config); ensure_meta_table(ds); ws = get_watermarks(ds, tid)
                verify_consistency(up, ds, config, ws[0] if ws else None)
                up.close(); ds.close()
        elif c == "Manual Sync Now": perform_sync(config)
        elif c == "Automatic Mode":
            interval = int(questionary.text("Interval (sec):", default=str(config.get("sync_interval", 60))).ask())
            config["sync_interval"] = interval; save_config(config); sync_loop(config, interval)

def setup_config(existing=None):
    w = existing if existing else {"upstream": {"host": "127.0.0.1", "port": "6001", "user": "dump", "password": "111", "db": "db1", "table": "t1"}, "downstream": {"host": "127.0.0.1", "port": "6001", "user": "dump", "password": "111", "db": "db2", "table": "t2"}, "stage": {"name": "s1"}, "sync_interval": 60, "verify_interval": 50}
    while True:
        table = Table(title="Config"); table.add_row("Up", f"{w['upstream']['db']}.{w['upstream']['table']}"); table.add_row("Ds", f"{w['downstream']['db']}.{w['downstream']['table']}"); table.add_row("Verify", str(w.get('verify_interval'))); console.print(table)
        c = questionary.select("Action:", choices=["Edit Upstream", "Edit Downstream", "Edit Stage", "Edit Verify Interval", "Save", "Discard"]).ask()
        if c == "Save": save_config(w); return w
        if c == "Edit Upstream": w["upstream"] = {"host": questionary.text("Host:", default=w["upstream"]["host"]).ask(), "port": questionary.text("Port:", default=w["upstream"]["port"]).ask(), "user": questionary.text("User:", default=w["upstream"]["user"]).ask(), "password": questionary.password("Pass:", default=w["upstream"]["password"]).ask(), "db": questionary.text("DB:", default=w["upstream"]["db"]).ask(), "table": questionary.text("Table:", default=w["upstream"]["table"]).ask()}
        if c == "Edit Downstream": w["downstream"] = {"host": questionary.text("Host:", default=w["downstream"]["host"]).ask(), "port": questionary.text("Port:", default=w["downstream"]["port"]).ask(), "user": questionary.text("User:", default=w["downstream"]["user"]).ask(), "password": questionary.password("Pass:", default=w["downstream"]["password"]).ask(), "db": questionary.text("DB:", default=w["downstream"]["db"]).ask(), "table": questionary.text("Table:", default=w["downstream"]["table"]).ask()}
        if c == "Edit Stage": w["stage"] = {"name": questionary.text("Stage Name:", default=w["stage"]["name"]).ask()}
        if c == "Edit Verify Interval": w["verify_interval"] = int(questionary.text("Interval:", default=str(w.get('verify_interval', 50))).ask())
        if c == "Discard" or c is None: return existing

if __name__ == "__main__":
    main()