#!/usr/bin/env python3
import sys, os, json, time, re, logging, argparse, uuid, socket, hashlib
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

parser = argparse.ArgumentParser(description="MatrixOne CDC Tool v3.0")
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
    def connect(self):
        try:
            self.conn = pymysql.connect(host=self.config["host"], port=int(self.config["port"]),
                                        user=self.config["user"], password=self.config["password"],
                                        database=self.config.get("db"), charset='utf8mb4',
                                        cursorclass=pymysql.cursors.DictCursor, local_infile=True, autocommit=True)
            return True
        except Exception as e: log.error(f"Connect {self.name} fail: {e}"); return False
    def close(self):
        if self.conn: self.conn.close()
    def query(self, sql, args=None):
        with self.conn.cursor() as cursor: cursor.execute(sql, args); return cursor.fetchall()
    def execute(self, sql, args=None):
        with self.conn.cursor() as cursor: cursor.execute(sql, args)
    def fetch_one(self, sql, args=None):
        res = self.query(sql, args); return res[0] if res else None

# --- Meta Logic ---
def load_config():
    if not os.path.exists(CONFIG_FILE): return {}
    with open(CONFIG_FILE, "r") as f: return json.load(f)
def save_config(config):
    with open(CONFIG_FILE, "w") as f: json.dump(config, f, indent=4)
def get_task_id(config):
    u, d = config["upstream"], config["downstream"]
    return f"{u['host']}_{u['port']}_{u['db']}_{u['table']}_to_{d['db']}_{d['table']}".replace(".", "_")
def ensure_meta_table(ds_conn):
    ds_conn.execute(f"CREATE DATABASE IF NOT EXISTS {META_DB}")
    ds_conn.execute(f"CREATE TABLE IF NOT EXISTS {META_DB}.{META_TABLE} (task_id VARCHAR(255), watermark VARCHAR(255), lock_owner VARCHAR(255), lock_time TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
def acquire_lock(ds_conn, tid):
    if not ds_conn.query(f"SELECT 1 FROM {META_DB}.{META_TABLE} WHERE task_id = %s", (tid,)):
        ds_conn.execute(f"INSERT INTO {META_DB}.{META_TABLE} (task_id) VALUES (%s)", (tid,))
    sql = f"UPDATE {META_DB}.{META_TABLE} SET lock_owner=%s, lock_time=NOW() WHERE task_id=%s AND (lock_owner IS NULL OR lock_owner=%s OR lock_time < NOW() - INTERVAL {LOCK_TIMEOUT_SEC} SECOND)"
    ds_conn.execute(sql, (INSTANCE_ID, tid, INSTANCE_ID))
    r = ds_conn.fetch_one(f"SELECT lock_owner FROM {META_DB}.{META_TABLE} WHERE task_id=%s", (tid,))
    return r and r['lock_owner'] == INSTANCE_ID
def release_lock(ds_conn, tid):
    try: ds_conn.execute(f"UPDATE {META_DB}.{META_TABLE} SET lock_owner=NULL WHERE task_id=%s AND lock_owner=%s", (tid, INSTANCE_ID))
    except: pass
def get_watermarks(ds_conn, tid):
    return [r['watermark'] for r in ds_conn.query(f"SELECT watermark FROM {META_DB}.{META_TABLE} WHERE task_id=%s AND watermark IS NOT NULL ORDER BY created_at DESC", (tid,))]

# --- Core Logic ---
def generate_snapshot_name(tid):
    sid = hashlib.md5(tid.encode()).hexdigest()[:12]
    return f"cdc_{sid}_{datetime.now().strftime('%y%m%d%H%M%S%f')[:-3]}"

def verify_consistency(up_conn, ds_conn, config, snap=None):
    u_cfg, d_cfg = config["upstream"], config["downstream"]
    sc = f"{{snapshot = '{snap}'}}" if snap else ""
    try:
        cols_res = up_conn.query(f"SHOW COLUMNS FROM `{u_cfg['db']}`.`{u_cfg['table']}`")
        processed = [f"IFNULL(HEX(`{c['Field']}`), 'NULL')" if "vec" in c['Type'].lower() else f"IFNULL(CAST(`{c['Field']}` AS VARCHAR), 'NULL')" for c in cols_res]
        sql = f"SELECT COUNT(*) as c, BIT_XOR(CRC32(CONCAT_WS(',', {', '.join(processed)}))) as h FROM "
        ur, dr = up_conn.fetch_one(sql + f"`{u_cfg['db']}`.`{u_cfg['table']}`{sc}"), ds_conn.fetch_one(sql + f"`{d_cfg['db']}`.`{d_cfg['table']}`")
        if ur['c'] == dr['c'] and ur['h'] == dr['h']:
            log.info(f"[green]Verify PASSED | Rows: {ur['c']} | Hash: {ur['h']}[/green]"); return True
        log.error(f"[red]Verify FAILED | Up({ur['c']},{ur['h']}) vs Ds({dr['c']},{dr['h']})[/red]"); return False
    except Exception as e: log.error(f"Verify error: {e}"); return False

def fast_verify_count(up_conn, ds_conn, config, snap=None):
    u_cfg, d_cfg = config["upstream"], config["downstream"]
    sc = f"{{snapshot = '{snap}'}}" if snap else ""
    try:
        ur = up_conn.fetch_one(f"SELECT COUNT(*) as c FROM `{u_cfg['db']}`.`{u_cfg['table']}`{sc}")
        dr = ds_conn.fetch_one(f"SELECT COUNT(*) as c FROM `{d_cfg['db']}`.`{d_cfg['table']}`")
        if ur['c'] == dr['c']: log.info(f"[green]Fast Check PASSED | Rows: {ur['c']}[/green]"); return True
        log.error(f"Fast Check FAILED: {ur['c']} vs {dr['c']}"); return False
    except: return False

def perform_sync(config, is_auto=False):
    u_cfg, d_cfg = config["upstream"], config["downstream"]
    up_conn, ds_conn = DBConnection(u_cfg, "Up"), DBConnection(d_cfg, "Ds")
    if not up_conn.connect() or not ds_conn.connect(): return False
    tid, newsnap, sync_success = get_task_id(config), None, False
    try:
        ensure_meta_table(ds_conn)
        if not acquire_lock(ds_conn, tid): return False
        if not ds_conn.query(f"SHOW TABLES FROM `{d_cfg['db']}` LIKE '{d_cfg['table']}'"):
            ddl = up_conn.fetch_one(f"SHOW CREATE TABLE `{u_cfg['db']}`.`{u_cfg['table']}`")['Create Table'].replace(f"`{u_cfg['table']}`", f"`{d_cfg['table']}`", 1)
            ds_conn.execute(f"USE `{d_cfg['db']}`"); ds_conn.execute(ddl)
        ws = get_watermarks(ds_conn, tid)
        lastgood, newsnap = (ws[0] if ws else None), generate_snapshot_name(tid)
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as prg:
            if not lastgood:
                log.info(f"[blue]FULL Sync | Task: {tid}[/blue]")
                t_zero = f"{u_cfg['table']}_zero"
                up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_zero}`"); up_conn.execute(f"CREATE TABLE `{u_cfg['db']}`.`{t_zero}` LIKE `{u_cfg['db']}`.`{u_cfg['table']}`")
                up_conn.execute(f"CREATE SNAPSHOT `{newsnap}` FOR TABLE `{u_cfg['db']}` `{u_cfg['table']}`")
                dfiles = up_conn.query(f"data branch diff `{u_cfg['db']}`.`{u_cfg['table']}`{{snapshot='{newsnap}'}} against `{u_cfg['db']}`.`{t_zero}` output file 'stage://{config['stage']['name']}'")
                up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_zero}`")
                time.sleep(1); ds_conn.execute("BEGIN")
                try:
                    for r in dfiles:
                        f = list(r.values())[0]
                        # NO-QUOTE Construction to avoid SyntaxError
                        # MatrixOne requires double backslash: \\
                        slash = chr(92)
                        quote = chr(34)
                        sql = "LOAD DATA INFILE " + chr(39) + f + chr(39) + " INTO TABLE `" + d_cfg['db'] + "`.`" + d_cfg['table'] + "` "
                        sql += "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY " + chr(39) + quote + chr(39) + " "
                        sql += "ESCAPED BY " + chr(39) + slash + slash + chr(39) + " "
                        sql += "LINES TERMINATED BY " + chr(39) + slash + "n" + chr(39) + " PARALLEL 'TRUE'"
                        ds_conn.execute(sql)
                    ds_conn.execute(f"INSERT INTO {META_DB}.{META_TABLE} (task_id, watermark) VALUES (%s, %s)", (tid, newsnap))
                    ds_conn.execute("COMMIT"); sync_success = True
                except Exception as e: ds_conn.execute("ROLLBACK"); raise e
            else:
                log.info(f"[blue]INCREMENTAL Sync | Task: {tid} | From {lastgood}[/blue]")
                if not up_conn.query(f"SELECT sname FROM mo_catalog.mo_snapshots WHERE sname = '{lastgood}'"): return False
                t_cp = f"{u_cfg['table']}_copy_prev"
                up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_cp}`"); up_conn.execute(f"data branch create table `{u_cfg['db']}`.`{t_cp}` from `{u_cfg['db']}`.`{u_cfg['table']}`{{snapshot='{lastgood}'}}")
                up_conn.execute(f"CREATE SNAPSHOT `{newsnap}` FOR TABLE `{u_cfg['db']}` `{u_cfg['table']}`")
                dfiles = up_conn.query(f"data branch diff `{u_cfg['db']}`.`{u_cfg['table']}`{{snapshot = '{newsnap}'}} against `{u_cfg['db']}`.`{t_cp}` output file 'stage://{config['stage']['name']}'")
                up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_cp}`")
                ds_conn.execute("BEGIN")
                try:
                    for r in dfiles:
                        raw = up_conn.fetch_one(f"select load_file(cast('{list(r.values())[0]}' as datalink)) as c")['c']
                        stmt_str = raw.decode('utf-8') if isinstance(raw, bytes) else raw
                        stmt_str = re.sub(r"^(BEGIN|COMMIT|ROLLBACK);\s*", "", stmt_str, flags=re.I|re.M)
                        stmt_str = re.sub(r";\s*(BEGIN|COMMIT|ROLLBACK);\s*$", ";", stmt_str, flags=re.I|re.M)
                        target = f"`{d_cfg['db']}`.`{d_cfg['table']}`"
                        sql = re.compile(r'(delete\s+from\s+|replace\s+into\s+)([`\w]+\.[`\w]+)', re.I).sub(lambda m: f"{m.group(1)} {target} ", stmt_str)
                        for s in [st.strip() for st in sql.split(';\n') if st.strip()]:
                            if s.upper().startswith(("BEGIN", "COMMIT", "ROLLBACK")): continue
                            ds_conn.execute(s)
                    ds_conn.execute(f"INSERT INTO {META_DB}.{META_TABLE} (task_id, watermark) VALUES (%s, %s)", (tid, newsnap))
                    ds_conn.execute("COMMIT"); sync_success = True
                except Exception as e: ds_conn.execute("ROLLBACK"); raise e
        if sync_success:
            log.info(f"[green]Sync SUCCESS | {newsnap}[/green]")
            for r in dfiles: 
                try: up_conn.execute(f"REMOVE '{list(r.values())[0]}' a")
                except: pass
            allw = get_watermarks(ds_conn, tid)
            if len(allw) > MAX_WATERMARKS:
                for w in allw[MAX_WATERMARKS:]: 
                    ds_conn.execute(f"DELETE FROM {META_DB}.{META_TABLE} WHERE task_id=%s AND watermark=%s", (tid, w))
                    try: up_conn.execute(f"DROP SNAPSHOT IF EXISTS `{w}`")
                    except: pass
            return True
    except Exception as e:
        log.error(f"Sync FAILED: {e}"); 
        if newsnap: 
            try: up_conn.execute(f"DROP SNAPSHOT IF EXISTS `{newsnap}`")
            except: pass
        return False
    finally: release_lock(ds_conn, tid); up_conn.close(); ds_conn.close()

# --- App ---
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
                    if ws:
                        if v_int > 0 and sc % v_int == 0: log.info("FULL Verify..."); verify_consistency(up, ds, config, ws[0])
                        else: log.info("FAST Verify..."); fast_verify_count(up, ds, config, ws[0])
                    up.close(); ds.close()
        time.sleep(interval)

def setup_config(existing=None):
    w = existing if existing else {"upstream": {"host": "127.0.0.1", "port": "6001", "user": "dump", "password": "111", "db": "db1", "table": "t1"}, "downstream": {"host": "127.0.0.1", "port": "6001", "user": "dump", "password": "111", "db": "db2", "table": "t2"}, "stage": {"name": "s1"}, "sync_interval": 60, "verify_interval": 50}
    while True:
        table = Table(title="Config"); table.add_row("Up", f"{w['upstream']['db']}.{w['upstream']['table']}"); table.add_row("Ds", f"{w['downstream']['db']}.{w['downstream']['table']}"); table.add_row("Verify", str(w.get('verify_interval'))); console.print(table)
        c = questionary.select("Action:", choices=["Edit Upstream", "Edit Downstream", "Edit Stage", "Edit Verify Interval", "Save", "Discard"]).ask()
        if c == "Save":
            save_config(w)
            return w
        if c == "Edit Upstream": w["upstream"] = {"host": questionary.text("Host:", default=w["upstream"]["host"]).ask(), "port": questionary.text("Port:", default=w["upstream"]["port"]).ask(), "user": questionary.text("User:", default=w["upstream"]["user"]).ask(), "password": questionary.password("Pass:", default=w["upstream"]["password"]).ask(), "db": questionary.text("DB:", default=w["upstream"]["db"]).ask(), "table": questionary.text("Table:", default=w["upstream"]["table"]).ask()}
        if c == "Edit Downstream": w["downstream"] = {"host": questionary.text("Host:", default=w["downstream"]["host"]).ask(), "port": questionary.text("Port:", default=w["downstream"]["port"]).ask(), "user": questionary.text("User:", default=w["downstream"]["user"]).ask(), "password": questionary.password("Pass:", default=w["downstream"]["password"]).ask(), "db": questionary.text("DB:", default=w["downstream"]["db"]).ask(), "table": questionary.text("Table:", default=w["downstream"]["table"]).ask()}
        if c == "Edit Stage": w["stage"] = {"name": questionary.text("Stage Name:", default=w["stage"]["name"]).ask()}
        if c == "Edit Verify Interval": w["verify_interval"] = int(questionary.text("Interval:", default=str(w.get('verify_interval', 50))).ask())
        if c == "Discard" or c is None: return existing

def main():
    console.print(Panel.fit("MatrixOne branch_cdc v3.0", style="bold magenta"))
    config = load_config()
    if not config: config = setup_config()
    if cli_args.once: perform_sync(config); sys.exit(0)
    if cli_args.mode == "auto": sync_loop(config, cli_args.interval or config.get("sync_interval", 60)); sys.exit(0)
    while True:
        c = questionary.select("Mode:", choices=["Manual Sync", "Verify", "Auto Loop", "Edit Config", "Exit"]).ask()
        if c == "Exit": sys.exit(0)
        if c == "Edit Config": config = setup_config(config)
        if c == "Manual Sync": perform_sync(config)
        if c == "Verify":
            up, ds = DBConnection(config["upstream"], "Up"), DBConnection(config["downstream"], "Ds")
            if up.connect() and ds.connect():
                tid = get_task_id(config); ensure_meta_table(ds); ws = get_watermarks(ds, tid)
                verify_consistency(up, ds, config, ws[0] if ws else None)
                up.close(); ds.close()
        if c == "Auto Loop": sync_loop(config, config.get("sync_interval", 60))

if __name__ == "__main__":
    main()
