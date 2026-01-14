#!/usr/bin/env python3
import sys, os, json, time, re, logging, argparse, uuid, socket, hashlib, threading, traceback
from datetime import datetime
from typing import Optional, Dict, Any, List
import pymysql
import questionary
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn

# --- Global Setup ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.makedirs(BASE_DIR, exist_ok=True)

parser = argparse.ArgumentParser(description="MatrixOne CDC Tool v4.1")
parser.add_argument("--log-file", default=os.path.join(BASE_DIR, "cdc_sync.log"))
parser.add_argument("--config", default=os.path.join(BASE_DIR, "config.json"))
parser.add_argument("--mode", choices=["manual", "auto"])
parser.add_argument("--interval", type=int)
parser.add_argument("--verify-interval", type=int)
parser.add_argument("--once", action="store_true")
cli_args, _ = parser.parse_known_args()

CONFIG_FILE = os.path.abspath(cli_args.config)
META_DB, META_TABLE, META_LOCK_TABLE = "cdc_by_data_branch_db", "meta", "meta_lock"
MAX_WATERMARKS, LOCK_TIMEOUT_SEC = 4, 30
INSTANCE_ID = f"{socket.gethostname()}_{os.getpid()}_{uuid.uuid4().hex[:6]}"

console = Console()
logging.basicConfig(level="INFO", format="%(message)s", handlers=[RichHandler(console=console, markup=True), logging.FileHandler(cli_args.log_file)])
log = logging.getLogger("rich")

class DBConnection:
    def __init__(self, config, name, autocommit=False):
        self.config, self.name, self.conn = config, name, None
        self.autocommit = autocommit
    def connect(self, db_override=None):
        try:
            db_to_use = db_override if db_override is not None else self.config.get("db")
            self.conn = pymysql.connect(
                host=self.config["host"], port=int(self.config["port"]),
                user=self.config["user"], password=self.config["password"],
                database=db_to_use, charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor, local_infile=True, autocommit=self.autocommit
            )
            self.conn.commit(); return True
        except pymysql.err.OperationalError as e:
            if e.args[0] == 1049 and db_override is None: return self.connect(db_override="")
            return False
        except: return False
    def close(self):
        if self.conn:
            try: self.conn.close()
            except: pass
    def query(self, sql, args=None):
        with self.conn.cursor() as cursor: cursor.execute(sql, args); return cursor.fetchall()
    def execute(self, sql, args=None):
        with self.conn.cursor() as cursor: cursor.execute(sql, args)
    def commit(self): self.conn.commit()
    def rollback(self):
        try: self.conn.rollback()
        except: pass
    def fetch_one(self, sql, args=None):
        res = self.query(sql, args); return res[0] if res else None

def load_config(path=None):
    cfg_path = path or CONFIG_FILE
    if not os.path.exists(cfg_path): return {}
    with open(cfg_path, "r") as f: return json.load(f)
def save_config(config, path=None):
    cfg_path = path or CONFIG_FILE
    with open(cfg_path, "w") as f: json.dump(config, f, indent=4)
def get_task_id(config):
    u, d = config["upstream"], config["downstream"]
    return f"{u['host']}_{u['port']}_{u['db']}_{u['table']}_to_{d['db']}_{d['table']}".replace(".", "_")

def ensure_meta_table(ds_conn):
    ds_conn.execute(f"CREATE DATABASE IF NOT EXISTS `{META_DB}`"); ds_conn.commit()
    ds_conn.execute(f"CREATE TABLE IF NOT EXISTS `{META_DB}`.`{META_TABLE}` (task_id VARCHAR(255), watermark VARCHAR(255), lock_owner VARCHAR(255), lock_time TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"); ds_conn.commit()
    ds_conn.execute(f"CREATE TABLE IF NOT EXISTS `{META_DB}`.`{META_LOCK_TABLE}` (task_id VARCHAR(255) PRIMARY KEY, lock_owner VARCHAR(255), lock_time TIMESTAMP)"); ds_conn.commit()

def acquire_lock(ds_conn, tid):
    if not ds_conn.fetch_one(f"SELECT 1 FROM `{META_DB}`.`{META_LOCK_TABLE}` WHERE task_id=%s", (tid,)):
        try:
            ds_conn.execute(f"INSERT INTO `{META_DB}`.`{META_LOCK_TABLE}` (task_id) VALUES (%s)", (tid,)); ds_conn.commit()
        except pymysql.err.IntegrityError:
            pass
    sql = f"UPDATE `{META_DB}`.`{META_LOCK_TABLE}` SET lock_owner=%s, lock_time=NOW() WHERE task_id=%s AND (lock_owner IS NULL OR lock_owner=%s OR lock_time < NOW() - INTERVAL {LOCK_TIMEOUT_SEC} SECOND)"
    ds_conn.execute(sql, (INSTANCE_ID, tid, INSTANCE_ID)); ds_conn.commit()
    r = ds_conn.fetch_one(f"SELECT lock_owner FROM `{META_DB}`.`{META_LOCK_TABLE}` WHERE task_id=%s", (tid,))
    return r and r['lock_owner'] == INSTANCE_ID

class LockKeeper(threading.Thread):
    def __init__(self, ds_config, tid):
        super().__init__()
        self.ds_config, self.tid, self.stop_event = ds_config, tid, threading.Event()
    def run(self):
        conn = DBConnection(self.ds_config, "LockKeeper", autocommit=True)
        if not conn.connect(db_override=""): return
        while not self.stop_event.wait(10):
            try:
                conn.execute(f"UPDATE `{META_DB}`.`{META_LOCK_TABLE}` SET lock_time=NOW() WHERE task_id=%s AND lock_owner=%s", (self.tid, INSTANCE_ID)); conn.commit()
            except:
                conn.close()
                time.sleep(1)
                conn.connect(db_override="")
        conn.close()
    def stop(self): self.stop_event.set()

def release_lock(ds_conn, tid):
    try:
        ds_conn.execute(f"UPDATE `{META_DB}`.`{META_LOCK_TABLE}` SET lock_owner=NULL, lock_time=NULL WHERE task_id=%s AND lock_owner=%s", (tid, INSTANCE_ID)); ds_conn.commit()
    except: pass

def get_watermarks(ds_conn, tid):
    try: return [r['watermark'] for r in ds_conn.query(f"SELECT watermark FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s AND watermark IS NOT NULL ORDER BY created_at DESC", (tid,))]
    except: return []

def lock_is_held(ds_conn, tid):
    try:
        r = ds_conn.fetch_one(f"SELECT lock_owner FROM `{META_DB}`.`{META_LOCK_TABLE}` WHERE task_id=%s", (tid,))
        return r and r['lock_owner'] == INSTANCE_ID
    except:
        return False

def get_stage_config(config):
    stage = config.get("stage") or {}
    name, url = stage.get("name"), stage.get("url")
    if not name and url:
        name = f"cdc_stage_{hashlib.md5(url.encode()).hexdigest()[:8]}"
    return name, url

def ensure_stage(up_conn, stage_name, stage_url):
    if not stage_name:
        return False
    if stage_url:
        safe_url = stage_url.replace("'", "''")
        up_conn.execute(f"CREATE STAGE IF NOT EXISTS `{stage_name}` URL='{safe_url}'"); up_conn.commit()
    return True

def get_diff_file_path(row):
    if not row:
        return None
    for key in ("FILE SAVED TO", "file saved to"):
        if key in row:
            return row[key]
    return next(iter(row.values()))

def split_sql_statements(sql_text):
    stmts, buf = [], []
    in_single = in_double = in_backtick = False
    in_line_comment = in_block_comment = False
    i, n = 0, len(sql_text)
    while i < n:
        ch = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < n else ""
        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            buf.append(ch)
            if ch == "*" and nxt == "/":
                buf.append(nxt)
                i += 2
                in_block_comment = False
                continue
            i += 1
            continue
        if in_single:
            buf.append(ch)
            if ch == "\\" and nxt:
                buf.append(nxt); i += 2; continue
            if ch == "'":
                if nxt == "'":
                    buf.append(nxt); i += 2; continue
                in_single = False
            i += 1
            continue
        if in_double:
            buf.append(ch)
            if ch == "\\" and nxt:
                buf.append(nxt); i += 2; continue
            if ch == "\"":
                if nxt == "\"":
                    buf.append(nxt); i += 2; continue
                in_double = False
            i += 1
            continue
        if in_backtick:
            buf.append(ch)
            if ch == "`":
                if nxt == "`":
                    buf.append(nxt); i += 2; continue
                in_backtick = False
            i += 1
            continue
        if ch == "-" and nxt == "-":
            in_line_comment = True
            buf.append(ch); buf.append(nxt); i += 2; continue
        if ch == "/" and nxt == "*":
            in_block_comment = True
            buf.append(ch); buf.append(nxt); i += 2; continue
        if ch == "'":
            in_single = True; buf.append(ch); i += 1; continue
        if ch == "\"":
            in_double = True; buf.append(ch); i += 1; continue
        if ch == "`":
            in_backtick = True; buf.append(ch); i += 1; continue
        if ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts

def get_check_sql(conn, db, table, snap=None, sample=False):
    sc = f"{{snapshot = '{snap}'}}" if snap else ""
    try: cols = conn.query(f"SHOW COLUMNS FROM `{db}`.`{table}`")
    except: return None # Review Fix: Explicit return None when table missing
    pk_col = next((c['Field'] for c in cols if c['Key'] == 'PRI'), "__mo_fake_pk_col")
    p_cols = [f"IFNULL(HEX(`{c['Field']}`), 'NULL')" if "vec" in c['Type'].lower() else f"IFNULL(CAST(`{c['Field']}` AS VARCHAR), 'NULL')" for c in cols]
    where = f" WHERE ABS(CRC32(CAST(`{pk_col}` AS VARCHAR))) % 100 = 7" if sample else ""
    return f"SELECT COUNT(*) as c, BIT_XOR(CRC32(CONCAT_WS(',', {', '.join(p_cols)}))) as h FROM `{db}`.`{table}`{sc}{where}"

def verify_consistency(up_conn, ds_conn, config, snap=None, sample=False):
    u, d = config["upstream"], config["downstream"]
    try:
        u_sql, d_sql = get_check_sql(up_conn, u['db'], u['table'], snap, sample), get_check_sql(ds_conn, d['db'], d['table'], None, sample)
        if u_sql is None or d_sql is None: return False # Review Fix: Fail if table missing
        ur, dr = up_conn.fetch_one(u_sql), ds_conn.fetch_one(d_sql)
        if ur['c'] == dr['c'] and (ur['h'] == dr['h'] or (ur['h'] is None and dr['h'] is None)):
            return True
        return False
    except: return False

def generate_snapshot_name(tid):
    sid = hashlib.md5(tid.encode()).hexdigest()[:12]
    return f"cdc_{sid}_{datetime.now().strftime('%y%m%d%H%M%S%f')[:-3]}"

def archeology_recovery(up_conn, ds_conn, config, tid):
    sid_prefix = hashlib.md5(tid.encode()).hexdigest()[:12]
    try:
        pattern = f"cdc_{sid_prefix}_%"
        snaps = up_conn.query(
            "SELECT sname FROM mo_catalog.mo_snapshots WHERE sname LIKE %s AND database_name=%s AND table_name=%s ORDER BY ts DESC LIMIT 10",
            (pattern, config['upstream']['db'], config['upstream']['table']),
        )
    except: return None
    for s in snaps:
        snap = s['sname']
        if verify_consistency(up_conn, ds_conn, config, snap, sample=True):
            if verify_consistency(up_conn, ds_conn, config, snap, sample=False):
                log.info("[green]FULL Check PASSED[/green]")
                log.info(f"[green]Watermark RECOVERED: {snap}[/green]")
                ds_conn.execute(f"INSERT INTO `{META_DB}`.`{META_TABLE}` (task_id, watermark) VALUES (%s, %s)", (tid, snap)); ds_conn.commit(); return snap
    return None

def perform_sync(config, is_auto=False, lock_held=False):
    u_cfg, d_cfg = config["upstream"], config["downstream"]
    tid = get_task_id(config)
    up_conn, ds_conn = DBConnection(u_cfg, "Up", autocommit=True), DBConnection(d_cfg, "Ds")
    if not up_conn.connect() or not ds_conn.connect(db_override=""): return False
    
    keeper = LockKeeper(d_cfg, tid)
    dfiles = []
    snapshot_created = False
    try:
        ds_conn.execute(f"CREATE DATABASE IF NOT EXISTS `{d_cfg['db']}`"); ds_conn.commit()
        ensure_meta_table(ds_conn)
        if lock_held:
            if not lock_is_held(ds_conn, tid):
                log.warning("Lock lost before sync, skipping this cycle.")
                return False
        else:
            if not acquire_lock(ds_conn, tid): return False
            keeper.start()
        
        ds_conn.execute(f"USE `{d_cfg['db']}`"); ds_conn.commit()
        target_exists = ds_conn.query(f"SHOW TABLES LIKE '{d_cfg['table']}'")
        if not target_exists:
            ddl = up_conn.fetch_one(f"SHOW CREATE TABLE `{u_cfg['db']}`.`{u_cfg['table']}`")['Create Table'].replace(f"`{u_cfg['table']}`", f"`{d_cfg['table']}`", 1)
            ds_conn.execute(ddl); ds_conn.commit()

        ws = get_watermarks(ds_conn, tid)
        lastgood = ws[0] if ws else None
        if not lastgood and target_exists:
            ds_cnt = ds_conn.fetch_one(f"SELECT COUNT(*) as c FROM `{d_cfg['table']}`")['c']
            if ds_cnt > 0: lastgood = archeology_recovery(up_conn, ds_conn, config, tid)
        
        if lastgood and not up_conn.fetch_one(f"SELECT sname FROM mo_catalog.mo_snapshots WHERE sname = %s", (lastgood,)):
            log.warning(f"Snapshot {lastgood} lost. Resetting to FULL sync.")
            ds_conn.execute(f"DELETE FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s", (tid,)); ds_conn.commit()
            lastgood = None
        if lastgood and not verify_consistency(up_conn, ds_conn, config, lastgood, sample=True):
            log.warning("Watermark inconsistent with downstream; resetting to FULL sync.")
            ds_conn.execute(f"DELETE FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s", (tid,)); ds_conn.commit()
            lastgood = None

        stage_name, stage_url = get_stage_config(config)
        if not stage_name:
            log.error("Stage name is missing; check config.stage.name or config.stage.url.")
            return False
        if not ensure_stage(up_conn, stage_name, stage_url):
            log.error("Stage setup failed; aborting sync.")
            return False

        newsnap = generate_snapshot_name(tid)
        sync_success = False
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as prg:
            if not lastgood:
                log.info(f"[blue]FULL Sync | Task: {tid}[/blue]")
                t_zero = f"{u_cfg['table']}_zero"
                up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_zero}`"); up_conn.commit()
                up_conn.execute(f"CREATE TABLE `{u_cfg['db']}`.`{t_zero}` LIKE `{u_cfg['db']}`.`{u_cfg['table']}`"); up_conn.commit()
                up_conn.execute(f"CREATE SNAPSHOT `{newsnap}` FOR TABLE `{u_cfg['db']}` `{u_cfg['table']}`"); up_conn.commit(); snapshot_created = True
                try: dfiles = up_conn.query(f"data branch diff `{u_cfg['db']}`.`{u_cfg['table']}`{{snapshot='{newsnap}'}} against `{u_cfg['db']}`.`{t_zero}` output file 'stage://{stage_name}'")
                finally: up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_zero}`"); up_conn.commit()
                
                ds_conn.conn.begin()
                try:
                    ds_conn.execute(f"TRUNCATE TABLE `{d_cfg['db']}`.`{d_cfg['table']}`")
                    for r in dfiles:
                        f, sl, qt = get_diff_file_path(r), chr(92), chr(34)
                        if not f: continue
                        ds_conn.execute(f"LOAD DATA INFILE '{f}' INTO TABLE `{d_cfg['db']}`.`{d_cfg['table']}` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '{qt}' ESCAPED BY '{sl}{sl}' LINES TERMINATED BY '{sl}n' PARALLEL 'TRUE'")
                    ds_conn.execute(f"INSERT INTO `{META_DB}`.`{META_TABLE}` (task_id, watermark) VALUES (%s, %s)", (tid, newsnap)); ds_conn.commit(); sync_success = True
                except Exception as e: ds_conn.rollback(); raise e
            else:
                log.info(f"[blue]INCREMENTAL Sync | Task: {tid} | From {lastgood}[/blue]")
                t_cp = f"{u_cfg['table']}_copy_prev"
                up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_cp}`"); up_conn.commit()
                up_conn.execute(f"data branch create table `{u_cfg['db']}`.`{t_cp}` from `{u_cfg['db']}`.`{u_cfg['table']}`{{snapshot='{lastgood}'}}"); up_conn.commit()
                up_conn.execute(f"CREATE SNAPSHOT `{newsnap}` FOR TABLE `{u_cfg['db']}` `{u_cfg['table']}`"); up_conn.commit(); snapshot_created = True
                try: dfiles = up_conn.query(f"data branch diff `{u_cfg['db']}`.`{u_cfg['table']}`{{snapshot = '{newsnap}'}} against `{u_cfg['db']}`.`{t_cp}` output file 'stage://{stage_name}'")
                finally: up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_cp}`"); up_conn.commit()
                
                ds_conn.conn.begin()
                try:
                    for r in dfiles:
                        f = get_diff_file_path(r)
                        if not f: continue
                        raw = up_conn.fetch_one("select load_file(cast(%s as datalink)) as c", (f,))['c']
                        if raw is None: continue
                        stmt_str = raw.decode('utf-8') if isinstance(raw, bytes) else raw
                        target = f"`{d_cfg['db']}`.`{d_cfg['table']}`"
                        sql = re.compile(r'(delete\s+from\s+|replace\s+into\s+|insert\s+into\s+|update\s+)(/\*.*?\*/\s+)?([`\w]+\.[`\w]+|[`\w]+)', re.I|re.S).sub(lambda m: f"{m.group(1)}{m.group(2) or ''} {target} ", stmt_str)
                        for s in split_sql_statements(sql):
                            if re.match(r"^(BEGIN|COMMIT|ROLLBACK|START\s+TRANSACTION)\b", s, re.I): continue
                            ds_conn.execute(s)
                    ds_conn.execute(f"INSERT INTO `{META_DB}`.`{META_TABLE}` (task_id, watermark) VALUES (%s, %s)", (tid, newsnap)); ds_conn.commit(); sync_success = True
                except Exception as e: ds_conn.rollback(); raise e
        
        if sync_success:
            log.info(f"[green]Sync SUCCESS | {newsnap}[/green]")
            for r in dfiles:
                try:
                    f = get_diff_file_path(r)
                    if f: up_conn.execute(f"REMOVE '{f}'"); up_conn.commit()
                except: pass
            allw = get_watermarks(ds_conn, tid)
            if len(allw) > MAX_WATERMARKS:
                for w in allw[MAX_WATERMARKS:]:
                    ds_conn.execute(f"DELETE FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s AND watermark=%s", (tid, w)); ds_conn.commit()
                    try: up_conn.execute(f"DROP SNAPSHOT IF EXISTS `{w}`"); up_conn.commit()
                    except: pass
            return True
        return False
    except Exception as e:
        log.error(f"Sync FAILED: {e}")
        if snapshot_created:
            try: up_conn.execute(f"DROP SNAPSHOT IF EXISTS `{newsnap}`"); up_conn.commit()
            except: pass
        return False
    finally:
        if keeper.is_alive(): # Review Fix: Only join if started
            keeper.stop(); keeper.join()
        if not lock_held:
            release_lock(ds_conn, tid)
        up_conn.close(); ds_conn.close()

def sync_loop(config, interval):
    sc = 0
    tid = get_task_id(config)
    ds_conn = None
    keeper = None
    try:
        while True:
            v_int = config.get("verify_interval", 50)
            try:
                if ds_conn is None:
                    ds_conn = DBConnection(config["downstream"], "Ds", autocommit=True)
                    if not ds_conn.connect(db_override=""):
                        log.error("Downstream connection failed; retrying...")
                        time.sleep(2); continue
                    ensure_meta_table(ds_conn)
                if not acquire_lock(ds_conn, tid):
                    time.sleep(interval); continue
                if keeper is None or not keeper.is_alive():
                    keeper = LockKeeper(config["downstream"], tid)
                    keeper.start()
                if perform_sync(config, is_auto=True, lock_held=True):
                    sc += 1
                    if sc % 5 == 0 or (v_int > 0 and sc % v_int == 0):
                        up, ds = DBConnection(config["upstream"], "Up", autocommit=True), DBConnection(config["downstream"], "Ds", autocommit=True)
                        if up.connect() and ds.connect():
                            ws = get_watermarks(ds, get_task_id(config))
                            if ws:
                                ok = verify_consistency(up, ds, config, ws[0], sample=not (v_int > 0 and sc % v_int == 0))
                                if not ok:
                                    log.warning("Consistency check FAILED; please investigate.")
                            up.close(); ds.close()
                time.sleep(interval)
            except Exception as e:
                log.warning(f"Auto loop error: {e}. Reconnecting...")
                if keeper and keeper.is_alive():
                    keeper.stop(); keeper.join()
                keeper = None
                if ds_conn:
                    ds_conn.close()
                ds_conn = None
                time.sleep(2)
    finally:
        if keeper and keeper.is_alive():
            keeper.stop(); keeper.join()
        if ds_conn:
            release_lock(ds_conn, tid)
            ds_conn.close()

def main():
    console.print(Panel.fit("MatrixOne branch_cdc v4.1", style="bold magenta"))
    config = load_config()
    if not config: config = setup_config()
    if cli_args.once: perform_sync(config); sys.exit(0)
    if cli_args.mode == "auto": sync_loop(config, cli_args.interval or config.get("sync_interval", 60)); sys.exit(0)
    while True:
        c = questionary.select("Mode:", choices=["Manual Sync Now", "Verify Consistency", "Automatic Mode", "Edit Configuration", "Exit"]).ask()
        if c == "Exit": sys.exit(0)
        elif c == "Edit Configuration": config = setup_config(config)
        elif c == "Verify Consistency":
            up, ds = DBConnection(config["upstream"], "Up", autocommit=True), DBConnection(config["downstream"], "Ds", autocommit=True)
            if up.connect() and ds.connect():
                tid = get_task_id(config); ensure_meta_table(ds); ws = get_watermarks(ds, tid)
                if ws: verify_consistency(up, ds, config, ws[0])
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
