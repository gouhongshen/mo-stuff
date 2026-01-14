#!/usr/bin/env python3
import sys, os, json, time, re, logging, argparse, uuid, socket, hashlib, threading, traceback
from datetime import datetime
from typing import Optional, Dict, Any, List
import pymysql
import questionary
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.box import ROUNDED
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.live import Live

# --- Global Setup ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.makedirs(BASE_DIR, exist_ok=True)

parser = argparse.ArgumentParser(description="MatrixOne BRANCH CDC Tool v4.1")
parser.add_argument("--log-file", default=os.path.join(BASE_DIR, "cdc_sync.log"))
parser.add_argument("--config", default=os.path.join(BASE_DIR, "config.json"))
parser.add_argument("--mode", choices=["manual", "auto"])
parser.add_argument("--interval", type=int)
parser.add_argument("--verify-interval", type=int)
parser.add_argument("--once", action="store_true")
cli_args, _ = parser.parse_known_args()

CONFIG_FILE = os.path.abspath(cli_args.config)
META_DB, META_TABLE, META_LOCK_TABLE = "branch_cdc_db", "meta", "meta_lock"
SNAPSHOT_PREFIX = "branch_cdc_"
MAX_WATERMARKS, LOCK_TIMEOUT_SEC = 4, 30
FULL_VERIFY_MAX_BYTES = 1024 * 1024 * 1024
FULL_VERIFY_MAX_ROWS = 100000
FAST_VERIFY_BUCKET = 7
FAST_VERIFY_MOD = 100
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
    return f"{u['host']}_{u['port']}_{u['db']}_{u['table']}_to_{d['host']}_{d['port']}_{d['db']}_{d['table']}".replace(".", "_")

def ensure_meta_table(ds_conn):
    ds_conn.execute(f"CREATE DATABASE IF NOT EXISTS `{META_DB}`"); ds_conn.commit()
    ds_conn.execute(f"CREATE TABLE IF NOT EXISTS `{META_DB}`.`{META_TABLE}` (task_id VARCHAR(512), watermark VARCHAR(255), lock_owner VARCHAR(255), lock_time TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"); ds_conn.commit()
    ds_conn.execute(f"CREATE TABLE IF NOT EXISTS `{META_DB}`.`{META_LOCK_TABLE}` (task_id VARCHAR(512) PRIMARY KEY, lock_owner VARCHAR(255), lock_time TIMESTAMP)"); ds_conn.commit()
    try:
        ds_conn.execute(f"ALTER TABLE `{META_DB}`.`{META_TABLE}` MODIFY COLUMN task_id VARCHAR(512)"); ds_conn.commit()
    except:
        pass
    try:
        ds_conn.execute(f"ALTER TABLE `{META_DB}`.`{META_LOCK_TABLE}` MODIFY COLUMN task_id VARCHAR(512)"); ds_conn.commit()
    except:
        pass

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

def list_upstream_snapshots(up_conn, config, tid):
    sid_prefix = hashlib.md5(tid.encode()).hexdigest()[:12]
    pattern = f"{SNAPSHOT_PREFIX}{sid_prefix}_%"
    try:
        snaps = up_conn.query(
            "SELECT sname FROM mo_catalog.mo_snapshots WHERE sname LIKE %s AND database_name=%s AND table_name=%s ORDER BY ts DESC",
            (pattern, config["upstream"]["db"], config["upstream"]["table"]),
        )
        return [s["sname"] for s in snaps]
    except:
        return []

def cleanup_snapshots_after_verify(up_conn, ds_conn, config, verified_snapshot=None):
    tid = get_task_id(config)
    snaps = list_upstream_snapshots(up_conn, config, tid)
    if len(snaps) <= MAX_WATERMARKS:
        return
    last_success = verified_snapshot
    if not last_success:
        ws = get_watermarks(ds_conn, tid)
        last_success = ws[0] if ws else None
    if not last_success:
        log.warning("Snapshot cleanup skipped: no last_success watermark.")
        return
    if last_success not in snaps:
        log.warning(f"Snapshot cleanup skipped: last_success {last_success} not found upstream.")
        return
    anchor_idx = snaps.index(last_success)
    keep = snaps[anchor_idx:anchor_idx + MAX_WATERMARKS]
    keep_set = set(keep)
    drop = [s for s in snaps if s not in keep_set]
    if not drop:
        return
    log.info(f"Snapshot cleanup: keep {len(keep)}, drop {len(drop)}")
    for s in drop:
        try:
            up_conn.execute(f"DROP SNAPSHOT IF EXISTS `{s}`"); up_conn.commit()
        except Exception as e:
            log.warning(f"Snapshot cleanup drop failed: {s}: {e}")
    try:
        placeholders = ",".join(["%s"] * len(keep))
        ds_conn.execute(
            f"DELETE FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s AND watermark IS NOT NULL AND watermark NOT IN ({placeholders})",
            (tid, *keep),
        )
        ds_conn.commit()
    except Exception as e:
        log.warning(f"Snapshot cleanup meta prune failed: {e}")

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

def get_table_columns(conn, db, table):
    try: return conn.query(f"SHOW COLUMNS FROM `{db}`.`{table}`")
    except: return None

def build_check_sql(db, table, cols, snap=None, sample=False):
    if not cols:
        return None
    sc = f"{{snapshot = '{snap}'}}" if snap else ""
    pk_col = next((c['Field'] for c in cols if c['Key'] == 'PRI'), None)
    if sample and not pk_col:
        return None
    p_cols = [f"IFNULL(HEX(`{c['Field']}`), 'NULL')" if "vec" in c['Type'].lower() else f"IFNULL(CAST(`{c['Field']}` AS VARCHAR), 'NULL')" for c in cols]
    where = f" WHERE ABS(CRC32(CAST(`{pk_col}` AS VARCHAR))) % {FAST_VERIFY_MOD} = {FAST_VERIFY_BUCKET}" if sample else ""
    return f"SELECT COUNT(*) as c, BIT_XOR(CRC32(CONCAT_WS(',', {', '.join(p_cols)}))) as h FROM `{db}`.`{table}`{sc}{where}"

def get_table_size_bytes(conn, db, table):
    try:
        res = conn.fetch_one("SELECT mo_table_size(%s, %s) AS s", (db, table))
    except:
        return None
    if not res:
        return None
    try:
        return int(res["s"])
    except (TypeError, ValueError, KeyError):
        return None

def get_table_count(conn, db, table, snap=None):
    sc = f"{{snapshot = '{snap}'}}" if snap else ""
    try:
        res = conn.fetch_one(f"SELECT COUNT(*) as c FROM `{db}`.`{table}`{sc}")
    except:
        return None
    return res["c"] if res else None

def decide_verify_sample(up_conn, u_cols, ds_cols, config, snap, requested_sample):
    if not requested_sample:
        return False
    u_pk = any(c.get("Key") == "PRI" for c in u_cols)
    d_pk = any(c.get("Key") == "PRI" for c in ds_cols)
    if not (u_pk and d_pk):
        return False
    size_bytes = get_table_size_bytes(up_conn, config["upstream"]["db"], config["upstream"]["table"])
    if size_bytes is not None and size_bytes > 0:
        return size_bytes >= FULL_VERIFY_MAX_BYTES
    row_count = get_table_count(up_conn, config["upstream"]["db"], config["upstream"]["table"], snap=snap)
    if row_count is not None and row_count < FULL_VERIFY_MAX_ROWS:
        return False
    return True

def verify_consistency(up_conn, ds_conn, config, snap=None, sample=False, return_detail=False):
    u, d = config["upstream"], config["downstream"]
    err = None
    u_time = None
    d_time = None
    try:
        u_cols = get_table_columns(up_conn, u["db"], u["table"])
        d_cols = get_table_columns(ds_conn, d["db"], d["table"])
        if not u_cols or not d_cols:
            if return_detail:
                err = "failed to load table columns"
                return False, None, None, False, err, u_time, d_time
            return False
        use_sample = decide_verify_sample(up_conn, u_cols, d_cols, config, snap, sample)
        u_sql = build_check_sql(u["db"], u["table"], u_cols, snap, use_sample)
        d_sql = build_check_sql(d["db"], d["table"], d_cols, None, use_sample)
        if u_sql is None or d_sql is None:
            if return_detail:
                err = "failed to build verify SQL"
                return False, None, None, use_sample, err, u_time, d_time
            return False
        u_start = time.time()
        ur = up_conn.fetch_one(u_sql)
        u_time = time.time() - u_start
        d_start = time.time()
        dr = ds_conn.fetch_one(d_sql)
        d_time = time.time() - d_start
        if ur is None or dr is None:
            err = "verify query returned empty result"
            ok = False
        else:
            ok = ur['c'] == dr['c'] and (ur['h'] == dr['h'] or (ur['h'] is None and dr['h'] is None))
        if return_detail:
            return ok, ur, dr, use_sample, err, u_time, d_time
        return ok
    except Exception as e:
        err = str(e)
        if return_detail:
            return False, None, None, False, err, u_time, d_time
        return False

def verify_watermark_consistency(up_conn, ds_conn, config, snap, retries=3, wait_sec=5):
    for attempt in range(1, retries + 1):
        ok, ur, dr, used_sample, err, _, _ = verify_consistency(up_conn, ds_conn, config, snap, sample=True, return_detail=True)
        if ok:
            return True
        if err or ur is None or dr is None:
            msg = err or "verify returned empty result"
            log.warning(f"Watermark check error (attempt {attempt}/{retries}): {msg}")
            if attempt < retries:
                time.sleep(wait_sec)
                continue
            return None
        log_verify_result(False, ur, dr, sample=used_sample, snapshot_label=snap)
        return False
    return None

def format_check_value(val):
    if val is None:
        return "NULL"
    return str(val)

def log_verify_result(ok, ur, dr, sample=False, snapshot_label=None):
    mode = "FAST" if sample else "FULL"
    snap_info = f" snapshot={snapshot_label}" if snapshot_label else ""
    if ok and ur:
        log.info(f"[green]{mode} VERIFY PASSED[/green] upstream rows={ur['c']} hash={format_check_value(ur['h'])}{snap_info}")
        return
    u_cnt = ur['c'] if ur else "N/A"
    u_hash = format_check_value(ur['h']) if ur else "N/A"
    d_cnt = dr['c'] if dr else "N/A"
    d_hash = format_check_value(dr['h']) if dr else "N/A"
    log.error(f"[red]{mode} VERIFY FAILED[/red] upstream rows={u_cnt} hash={u_hash} downstream rows={d_cnt} hash={d_hash}{snap_info}")

def build_status_panel(config, last_sync=None, last_verify=None, last_error=None):
    up = config.get("upstream", {})
    ds = config.get("downstream", {})
    tbl = Table.grid(padding=(0, 1))
    tbl.add_row("Up", f"{up.get('db')}.{up.get('table')}")
    tbl.add_row("Ds", f"{ds.get('db')}.{ds.get('table')}")
    if last_sync:
        tbl.add_row("Last Sync", last_sync)
    if last_verify:
        tbl.add_row("Last Verify", last_verify)
    if last_error:
        tbl.add_row("Last Error", last_error)
    return Panel(tbl, title="MatrixOne BRANCH CDC", box=ROUNDED, border_style="cyan")

def run_with_activity_indicator(title, action_fn, config=None, last_sync=None, last_verify=None, last_error=None):
    result = {"value": None, "error": None}
    done = threading.Event()

    def runner():
        try:
            result["value"] = action_fn()
        except Exception as e:
            result["error"] = e
        finally:
            done.set()

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()

    width = 10
    frames = []
    for i in range(width):
        bar = ["-"] * width
        bar[i] = "#"
        frames.append("[" + "".join(bar) + "]")
    for i in range(width - 2, 0, -1):
        bar = ["-"] * width
        bar[i] = "#"
        frames.append("[" + "".join(bar) + "]")

    idx = 0
    with Live(console=console, refresh_per_second=8) as live:
        while not done.is_set():
            frame = frames[idx % len(frames)]
            tbl = Table.grid(padding=(0, 1))
            tbl.add_row("Action", title)
            tbl.add_row("Status", "RUNNING")
            tbl.add_row("Pulse", frame)
            if config:
                panel = Panel(tbl, title="Working", box=ROUNDED, border_style="yellow")
                layout = Table.grid(padding=(1, 2))
                layout.add_row(build_status_panel(config, last_sync, last_verify, last_error))
                layout.add_row(panel)
                live.update(layout)
            else:
                live.update(Panel(tbl, title="Working", box=ROUNDED, border_style="yellow"))
            time.sleep(0.12)
            idx += 1

    thread.join()
    if result["error"]:
        raise result["error"]
    return result["value"]

def generate_snapshot_name(tid):
    sid = hashlib.md5(tid.encode()).hexdigest()[:12]
    return f"{SNAPSHOT_PREFIX}{sid}_{datetime.now().strftime('%y%m%d%H%M%S%f')[:-3]}"

def archeology_recovery(up_conn, ds_conn, config, tid):
    sid_prefix = hashlib.md5(tid.encode()).hexdigest()[:12]
    try:
        pattern = f"{SNAPSHOT_PREFIX}{sid_prefix}_%"
        snaps = up_conn.query(
            "SELECT sname FROM mo_catalog.mo_snapshots WHERE sname LIKE %s AND database_name=%s AND table_name=%s ORDER BY ts DESC LIMIT 10",
            (pattern, config['upstream']['db'], config['upstream']['table']),
        )
    except: return None
    for s in snaps:
        snap = s['sname']
        ok, _, _, used_sample, _, _, _ = verify_consistency(up_conn, ds_conn, config, snap, sample=True, return_detail=True)
        if ok:
            if used_sample or verify_consistency(up_conn, ds_conn, config, snap, sample=False):
                log.info("[green]FULL Check PASSED[/green]")
                log.info(f"[green]Watermark RECOVERED: {snap}[/green]")
                ds_conn.execute(f"INSERT INTO `{META_DB}`.`{META_TABLE}` (task_id, watermark) VALUES (%s, %s)", (tid, snap)); ds_conn.commit(); return snap
    return None

def perform_sync(config, is_auto=False, lock_held=False, force_full=False):
    start_ts = time.time()
    diff_elapsed = 0.0
    apply_elapsed = 0.0
    sync_success = False
    sync_status = "FAILED"
    sync_noop = False
    u_cfg, d_cfg = config["upstream"], config["downstream"]
    tid = get_task_id(config)
    up_conn, ds_conn = DBConnection(u_cfg, "Up", autocommit=True), DBConnection(d_cfg, "Ds")
    keeper = LockKeeper(d_cfg, tid)
    dfiles = []
    snapshot_created = False
    try:
        if not up_conn.connect() or not ds_conn.connect(db_override=""):
            log.error("Sync FAILED: connection failed")
            return False
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
        if force_full:
            lastgood = None
        if not lastgood and target_exists and not force_full:
            ds_cnt = ds_conn.fetch_one(f"SELECT COUNT(*) as c FROM `{d_cfg['table']}`")['c']
            if ds_cnt > 0: lastgood = archeology_recovery(up_conn, ds_conn, config, tid)
        
        if lastgood and not up_conn.fetch_one(f"SELECT sname FROM mo_catalog.mo_snapshots WHERE sname = %s", (lastgood,)):
            log.warning(f"Snapshot {lastgood} lost. Resetting to FULL sync.")
            ds_conn.execute(f"DELETE FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s", (tid,)); ds_conn.commit()
            lastgood = None
        if lastgood:
            check = verify_watermark_consistency(up_conn, ds_conn, config, lastgood)
            if check is False:
                log.warning("Watermark inconsistent with downstream; resetting to FULL sync.")
                ds_conn.execute(f"DELETE FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s", (tid,)); ds_conn.commit()
                lastgood = None
            elif check is None:
                log.error("Watermark check failed after retries; skipping this sync.")
                return False

        stage_name, stage_url = get_stage_config(config)
        if not stage_name:
            log.error("Stage name is missing; check config.stage.name or config.stage.url.")
            return False
        if not ensure_stage(up_conn, stage_name, stage_url):
            log.error("Stage setup failed; aborting sync.")
            return False

        newsnap = generate_snapshot_name(tid)
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as prg:
            if not lastgood:
                log.info(f"[blue]FULL Sync | Task: {tid}[/blue]")
                t_zero = f"{u_cfg['table']}_zero"
                up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_zero}`"); up_conn.commit()
                up_conn.execute(f"CREATE TABLE `{u_cfg['db']}`.`{t_zero}` LIKE `{u_cfg['db']}`.`{u_cfg['table']}`"); up_conn.commit()
                diff_start = time.time()
                up_conn.execute(f"CREATE SNAPSHOT `{newsnap}` FOR TABLE `{u_cfg['db']}` `{u_cfg['table']}`"); up_conn.commit(); snapshot_created = True
                try: dfiles = up_conn.query(f"data branch diff `{u_cfg['db']}`.`{u_cfg['table']}`{{snapshot='{newsnap}'}} against `{u_cfg['db']}`.`{t_zero}` output file 'stage://{stage_name}'")
                finally: up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_zero}`"); up_conn.commit()
                diff_elapsed += time.time() - diff_start
                
                apply_start = time.time()
                ds_conn.conn.begin()
                try:
                    ds_conn.execute(f"TRUNCATE TABLE `{d_cfg['db']}`.`{d_cfg['table']}`")
                    for r in dfiles:
                        f, sl, qt = get_diff_file_path(r), chr(92), chr(34)
                        if not f: continue
                        ds_conn.execute(f"LOAD DATA INFILE '{f}' INTO TABLE `{d_cfg['db']}`.`{d_cfg['table']}` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '{qt}' ESCAPED BY '{sl}{sl}' LINES TERMINATED BY '{sl}n' PARALLEL 'TRUE'")
                    ds_conn.execute(f"INSERT INTO `{META_DB}`.`{META_TABLE}` (task_id, watermark) VALUES (%s, %s)", (tid, newsnap)); ds_conn.commit(); sync_success = True
                except Exception as e:
                    ds_conn.rollback(); raise e
                finally:
                    apply_elapsed += time.time() - apply_start
            else:
                log.info(f"[blue]INCREMENTAL Sync | Task: {tid} | From {lastgood}[/blue]")
                t_cp = f"{u_cfg['table']}_copy_prev"
                up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_cp}`"); up_conn.commit()
                up_conn.execute(f"data branch create table `{u_cfg['db']}`.`{t_cp}` from `{u_cfg['db']}`.`{u_cfg['table']}`{{snapshot='{lastgood}'}}"); up_conn.commit()
                diff_start = time.time()
                up_conn.execute(f"CREATE SNAPSHOT `{newsnap}` FOR TABLE `{u_cfg['db']}` `{u_cfg['table']}`"); up_conn.commit(); snapshot_created = True
                try: dfiles = up_conn.query(f"data branch diff `{u_cfg['db']}`.`{u_cfg['table']}`{{snapshot = '{newsnap}'}} against `{u_cfg['db']}`.`{t_cp}` output file 'stage://{stage_name}'")
                finally: up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_cp}`"); up_conn.commit()
                diff_elapsed += time.time() - diff_start
                
                apply_start = time.time()
                ds_conn.conn.begin()
                try:
                    applied_stmt_count = 0
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
                            applied_stmt_count += 1
                    if applied_stmt_count == 0:
                        ds_conn.rollback()
                        sync_noop = True
                    else:
                        ds_conn.execute(f"INSERT INTO `{META_DB}`.`{META_TABLE}` (task_id, watermark) VALUES (%s, %s)", (tid, newsnap)); ds_conn.commit(); sync_success = True
                except Exception as e:
                    ds_conn.rollback(); raise e
                finally:
                    apply_elapsed += time.time() - apply_start
        
        if sync_noop:
            sync_status = "NOOP"
            log.info(f"[yellow]Sync NOOP | {newsnap}[/yellow]")
            for r in dfiles:
                try:
                    f = get_diff_file_path(r)
                    if f: up_conn.execute(f"REMOVE '{f}'"); up_conn.commit()
                except: pass
            if snapshot_created:
                try: up_conn.execute(f"DROP SNAPSHOT IF EXISTS `{newsnap}`"); up_conn.commit()
                except: pass
            return True
        if sync_success:
            sync_status = "SUCCESS"
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
        elapsed = time.time() - start_ts
        log.info(f"Sync duration={diff_elapsed:.3f}/{apply_elapsed:.3f}/{elapsed:.3f}s status={sync_status}")
        log.info("-" * 72)
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
    last_sync = None
    last_verify = None
    last_error = None
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
                    last_sync = time.strftime("%Y-%m-%d %H:%M:%S")
                    if sc % 5 == 0 or (v_int > 0 and sc % v_int == 0):
                        up, ds = DBConnection(config["upstream"], "Up", autocommit=True), DBConnection(config["downstream"], "Ds", autocommit=True)
                        if up.connect() and ds.connect():
                            ws = get_watermarks(ds, get_task_id(config))
                            if ws:
                                sample = not (v_int > 0 and sc % v_int == 0)
                                verify_start = time.time()
                                ok, ur, dr, used_sample, _, u_time, d_time = verify_consistency(up, ds, config, ws[0], sample=sample, return_detail=True)
                                log_verify_result(ok, ur, dr, sample=used_sample, snapshot_label=ws[0])
                                verify_elapsed = time.time() - verify_start
                                u_dur = u_time or 0.0
                                d_dur = d_time or 0.0
                                log.info(f"Verify duration={u_dur:.3f}/{d_dur:.3f}/{verify_elapsed:.3f}s mode={'FAST' if used_sample else 'FULL'} snapshot={ws[0]}")
                                if not ok:
                                    log.warning("Consistency check FAILED; please investigate.")
                                else:
                                    cleanup_snapshots_after_verify(up, ds, config, ws[0])
                                last_verify = f"{time.strftime('%Y-%m-%d %H:%M:%S')} ({'FAST' if used_sample else 'FULL'})"
                            up.close(); ds.close()
                time.sleep(interval)
            except Exception as e:
                last_error = str(e)
                log.warning(f"Auto loop error: {e}. Reconnecting...")
                if keeper and keeper.is_alive():
                    keeper.stop(); keeper.join()
                keeper = None
                if ds_conn:
                    ds_conn.close()
                ds_conn = None
                time.sleep(2)
    except KeyboardInterrupt:
        log.info("Auto mode interrupted by user.")
    finally:
        if keeper and keeper.is_alive():
            keeper.stop(); keeper.join()
        if ds_conn:
            release_lock(ds_conn, tid)
            ds_conn.close()

def main():
    console.print(Panel.fit("MatrixOne BRANCH CDC v4.1", style="bold magenta", box=ROUNDED))
    config = load_config()
    if not config: config = setup_config()
    last_sync = None
    last_verify = None
    last_error = None
    if cli_args.once: perform_sync(config); sys.exit(0)
    if cli_args.mode == "auto": sync_loop(config, cli_args.interval or config.get("sync_interval", 60)); sys.exit(0)
    while True:
        console.print(build_status_panel(config, last_sync, last_verify, last_error))
        c = questionary.select("Mode:", choices=["Manual Sync Now", "Verify Consistency", "Automatic Mode", "Edit Configuration", "Exit"]).ask()
        if c == "Exit": sys.exit(0)
        elif c == "Edit Configuration": config = setup_config(config)
        elif c == "Verify Consistency":
            ds_meta = DBConnection(config["downstream"], "DsMeta", autocommit=True)
            if not ds_meta.connect(db_override=""):
                log.error("Downstream connection failed; cannot verify.")
                continue
            tid = get_task_id(config)
            ensure_meta_table(ds_meta)
            snaps = get_watermarks(ds_meta, tid)[:MAX_WATERMARKS]
            ds_meta.close()
            latest_label = "Latest upstream (no snapshot)"
            choices = snaps + [latest_label]
            chosen = questionary.select("Verify snapshot:", choices=choices).ask()
            if chosen is None:
                continue
            snap = None
            snap_label = "LATEST"
            if chosen != latest_label:
                snap = chosen
                snap_label = chosen
            def do_verify():
                up, ds = DBConnection(config["upstream"], "Up", autocommit=True), DBConnection(config["downstream"], "Ds", autocommit=True)
                ok, ur, dr, used_sample = False, None, None, False
                if up.connect() and ds.connect():
                    ok, ur, dr, used_sample, _, u_time, d_time = verify_consistency(up, ds, config, snap, return_detail=True)
                    if ok:
                        cleanup_snapshots_after_verify(up, ds, config, snap)
                up.close(); ds.close()
                return ok, ur, dr, used_sample, u_time, d_time
            verify_start = time.time()
            ok, ur, dr, used_sample, u_time, d_time = run_with_activity_indicator("Verify Consistency", do_verify, config, last_sync, last_verify, last_error)
            log_verify_result(ok, ur, dr, sample=used_sample, snapshot_label=snap_label)
            verify_elapsed = time.time() - verify_start
            u_dur = u_time or 0.0
            d_dur = d_time or 0.0
            log.info(f"Verify duration={u_dur:.3f}/{d_dur:.3f}/{verify_elapsed:.3f}s mode={'FAST' if used_sample else 'FULL'} snapshot={snap_label}")
            last_verify = f"{time.strftime('%Y-%m-%d %H:%M:%S')} ({'FAST' if used_sample else 'FULL'})"
            if not ok:
                last_error = "Verify failed"
        elif c == "Manual Sync Now":
            mode = questionary.select("Manual Sync Mode:", choices=["Incremental Sync", "Full Sync"]).ask()
            if mode is None:
                continue
            force_full = mode == "Full Sync"
            ok = run_with_activity_indicator(
                "Manual Sync",
                lambda: perform_sync(config, force_full=force_full),
                config,
                last_sync,
                last_verify,
                last_error,
            )
            last_sync = time.strftime("%Y-%m-%d %H:%M:%S")
            if not ok:
                last_error = "Sync failed"
        elif c == "Automatic Mode":
            val = questionary.text("Interval (sec):", default=str(config.get("sync_interval", 60))).ask()
            if val is None:
                continue
            try:
                interval = int(val)
            except ValueError:
                log.warning(f"Invalid interval: {val}")
                continue
            config["sync_interval"] = interval; save_config(config); sync_loop(config, interval)

def setup_config(existing=None):
    w = existing if existing else {"upstream": {"host": "127.0.0.1", "port": "6001", "user": "dump", "password": "111", "db": "db1", "table": "t1"}, "downstream": {"host": "127.0.0.1", "port": "6001", "user": "dump", "password": "111", "db": "db2", "table": "t2"}, "stage": {"name": "s1"}, "sync_interval": 60, "verify_interval": 50}
    def text_default(value):
        return "" if value is None else str(value)
    def ask_text(prompt, default, secret=False):
        if secret:
            res = questionary.password(prompt, default=default).ask()
        else:
            res = questionary.text(prompt, default=default).ask()
        return default if res is None else res
    while True:
        table = Table(title="Config"); table.add_row("Up", f"{w['upstream']['db']}.{w['upstream']['table']}"); table.add_row("Ds", f"{w['downstream']['db']}.{w['downstream']['table']}"); table.add_row("Verify", str(w.get('verify_interval'))); console.print(table)
        c = questionary.select("Action:", choices=["Edit Upstream", "Edit Downstream", "Edit Stage", "Edit Verify Interval", "Save", "Discard"]).ask()
        if c == "Save": save_config(w); return w
        if c == "Edit Upstream": w["upstream"] = {"host": ask_text("Host:", text_default(w["upstream"].get("host"))), "port": ask_text("Port:", text_default(w["upstream"].get("port"))), "user": ask_text("User:", text_default(w["upstream"].get("user"))), "password": ask_text("Pass:", text_default(w["upstream"].get("password")), secret=True), "db": ask_text("DB:", text_default(w["upstream"].get("db"))), "table": ask_text("Table:", text_default(w["upstream"].get("table")))}
        if c == "Edit Downstream": w["downstream"] = {"host": ask_text("Host:", text_default(w["downstream"].get("host"))), "port": ask_text("Port:", text_default(w["downstream"].get("port"))), "user": ask_text("User:", text_default(w["downstream"].get("user"))), "password": ask_text("Pass:", text_default(w["downstream"].get("password")), secret=True), "db": ask_text("DB:", text_default(w["downstream"].get("db"))), "table": ask_text("Table:", text_default(w["downstream"].get("table")))}
        if c == "Edit Stage": w["stage"] = {"name": ask_text("Stage Name:", text_default(w.get("stage", {}).get("name")))}
        if c == "Edit Verify Interval":
            val = questionary.text("Interval:", default=text_default(w.get("verify_interval", 50))).ask()
            if val is None:
                continue
            try:
                w["verify_interval"] = int(val)
            except ValueError:
                log.warning(f"Invalid verify interval: {val}")
        if c == "Discard" or c is None: return existing

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
