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
MAX_WATERMARKS, LOCK_TIMEOUT_SEC = 4, 30
DEFAULT_PITR_RANGE_DAYS = 7
FULL_VERIFY_MAX_BYTES = 1024 * 1024 * 1024
FULL_VERIFY_MAX_ROWS = 100000
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
def get_sync_scope(config):
    scope = (config.get("sync_scope") or "table").lower()
    return "database" if scope == "database" else "table"

def get_task_id(config):
    u, d = config["upstream"], config["downstream"]
    if get_sync_scope(config) == "database":
        return f"{u['host']}_{u['port']}_{u['db']}_to_{d['host']}_{d['port']}_{d['db']}".replace(".", "_")
    return f"{u['host']}_{u['port']}_{u['db']}_{u['table']}_to_{d['host']}_{d['port']}_{d['db']}_{d['table']}".replace(".", "_")

def with_table_config(config, table):
    cfg = dict(config)
    cfg["upstream"] = dict(config["upstream"])
    cfg["downstream"] = dict(config["downstream"])
    cfg["upstream"]["table"] = table
    cfg["downstream"]["table"] = table
    return cfg

def ensure_meta_table(ds_conn):
    ds_conn.execute(f"CREATE DATABASE IF NOT EXISTS `{META_DB}`"); ds_conn.commit()
    ds_conn.execute(f"CREATE TABLE IF NOT EXISTS `{META_DB}`.`{META_TABLE}` (task_id VARCHAR(512), watermark BIGINT UNSIGNED, lock_owner VARCHAR(255), lock_time TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"); ds_conn.commit()
    ds_conn.execute(f"CREATE TABLE IF NOT EXISTS `{META_DB}`.`{META_LOCK_TABLE}` (task_id VARCHAR(512) PRIMARY KEY, lock_owner VARCHAR(255), lock_time TIMESTAMP)"); ds_conn.commit()
    try:
        ds_conn.execute(f"ALTER TABLE `{META_DB}`.`{META_TABLE}` MODIFY COLUMN task_id VARCHAR(512)"); ds_conn.commit()
    except:
        pass
    try:
        ds_conn.execute(f"ALTER TABLE `{META_DB}`.`{META_TABLE}` MODIFY COLUMN watermark BIGINT UNSIGNED"); ds_conn.commit()
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
    try:
        rows = ds_conn.query(f"SELECT watermark FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s AND watermark IS NOT NULL ORDER BY created_at DESC", (tid,))
    except:
        return []
    out = []
    for r in rows:
        w = r.get("watermark")
        if w is None:
            continue
        try:
            out.append(int(w))
        except (TypeError, ValueError):
            continue
    return out

def prune_watermarks(ds_conn, tid, max_keep=MAX_WATERMARKS):
    allw = get_watermarks(ds_conn, tid)
    if len(allw) <= max_keep:
        return
    for w in allw[max_keep:]:
        ds_conn.execute(f"DELETE FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s AND watermark=%s", (tid, w)); ds_conn.commit()

def get_table_id(up_conn, db, table):
    queries = [
        ("SELECT rel_id FROM mo_catalog.mo_tables WHERE relname=%s AND reldatabase=%s AND account_id = current_account_id()", (table, db)),
        ("SELECT rel_id FROM mo_catalog.mo_tables WHERE relname=%s AND reldatabase=%s", (table, db)),
    ]
    for sql, args in queries:
        try:
            r = up_conn.fetch_one(sql, args)
        except:
            r = None
        if r and r.get("rel_id") is not None:
            return int(r["rel_id"])
    return None

def get_database_id(up_conn, db):
    queries = [
        ("SELECT dat_id FROM mo_catalog.mo_database WHERE datname=%s AND account_id = current_account_id()", (db,)),
        ("SELECT dat_id FROM mo_catalog.mo_database WHERE datname=%s", (db,)),
    ]
    for sql, args in queries:
        try:
            r = up_conn.fetch_one(sql, args)
        except:
            r = None
        if r and r.get("dat_id") is not None:
            return int(r["dat_id"])
    return None

def resolve_pitr_target(up_conn, config):
    scope = get_sync_scope(config)
    u_cfg = config["upstream"]
    if scope == "database":
        return "database", get_database_id(up_conn, u_cfg["db"])
    return "table", get_table_id(up_conn, u_cfg["db"], u_cfg["table"])

def list_active_pitr(up_conn, level, obj_id):
    if not level or obj_id is None:
        return []
    sql = (
        "SELECT pitr_name, level, obj_id, pitr_length, pitr_unit "
        "FROM mo_catalog.mo_pitr "
        "WHERE pitr_status = 1 AND create_account = current_account_id() AND level = %s AND obj_id = %s "
        "ORDER BY modified_time DESC"
    )
    try:
        return up_conn.query(sql, (level, obj_id))
    except:
        return []

def get_pitr_record(up_conn, pitr_name):
    queries = [
        ("SELECT pitr_name, level, obj_id, pitr_status, pitr_length, pitr_unit FROM mo_catalog.mo_pitr WHERE pitr_name=%s AND create_account = current_account_id()", (pitr_name,)),
        ("SELECT pitr_name, level, obj_id, pitr_status, pitr_length, pitr_unit FROM mo_catalog.mo_pitr WHERE pitr_name=%s", (pitr_name,)),
    ]
    for sql, args in queries:
        try:
            r = up_conn.fetch_one(sql, args)
        except:
            r = None
        if r:
            return r
    return None

def format_pitr_label(pitr):
    name = pitr.get("pitr_name", "")
    level = pitr.get("level", "")
    length = pitr.get("pitr_length")
    unit = pitr.get("pitr_unit")
    range_str = ""
    if length is not None and unit:
        range_str = f", range={length}{unit}"
    return f"{name} (level={level}{range_str})"

def generate_pitr_name(db, table=None):
    suffix = table if table else "db"
    base = f"pitr_{db}_{suffix}_{datetime.now().strftime('%y%m%d%H%M%S')}"
    return re.sub(r"[^a-zA-Z0-9_]", "_", base)

def lock_is_held(ds_conn, tid):
    try:
        r = ds_conn.fetch_one(f"SELECT lock_owner FROM `{META_DB}`.`{META_LOCK_TABLE}` WHERE task_id=%s", (tid,))
        return r and r['lock_owner'] == INSTANCE_ID
    except:
        return False

def record_timing(timings, name, start_ts):
    timings.append((name, time.time() - start_ts))

def summarize_top_timings(timings, topn=3):
    totals = {}
    for name, dur in timings:
        totals[name] = totals.get(name, 0.0) + dur
    top = sorted(totals.items(), key=lambda x: x[1], reverse=True)[:topn]
    return ", ".join([f"{n}:{d:.3f}s" for n, d in top])

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

class IncrementalFallback(Exception):
    pass

def remove_stage_files(up_conn, dfiles):
    for r in dfiles:
        f = None
        try:
            f = get_diff_file_path(r)
            if f:
                up_conn.execute(f"REMOVE FILES FROM STAGE IF EXISTS '{f}'"); up_conn.commit()
        except Exception as e:
            log.warning(f"Remove stage file failed: {f}: {e}")

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

def find_matching_paren(text, start_idx):
    depth = 0
    in_single = in_double = in_backtick = False
    i, n = start_idx, len(text)
    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if in_single:
            if ch == "\\" and nxt:
                i += 2; continue
            if ch == "'" and nxt != "'":
                in_single = False
            elif ch == "'" and nxt == "'":
                i += 2; continue
            i += 1; continue
        if in_double:
            if ch == "\\" and nxt:
                i += 2; continue
            if ch == '"' and nxt != '"':
                in_double = False
            elif ch == '"' and nxt == '"':
                i += 2; continue
            i += 1; continue
        if in_backtick:
            if ch == "`":
                in_backtick = False
            i += 1; continue
        if ch == "'":
            in_single = True; i += 1; continue
        if ch == '"':
            in_double = True; i += 1; continue
        if ch == "`":
            in_backtick = True; i += 1; continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return None

def count_top_level_items(text):
    depth = 0
    in_single = in_double = in_backtick = False
    count = 0
    has_token = False
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if in_single:
            has_token = True
            if ch == "\\" and nxt:
                i += 2; continue
            if ch == "'" and nxt != "'":
                in_single = False
            elif ch == "'" and nxt == "'":
                i += 2; continue
            i += 1; continue
        if in_double:
            has_token = True
            if ch == "\\" and nxt:
                i += 2; continue
            if ch == '"' and nxt != '"':
                in_double = False
            elif ch == '"' and nxt == '"':
                i += 2; continue
            i += 1; continue
        if in_backtick:
            has_token = True
            if ch == "`":
                in_backtick = False
            i += 1; continue
        if ch == "'":
            in_single = True; has_token = True; i += 1; continue
        if ch == '"':
            in_double = True; has_token = True; i += 1; continue
        if ch == "`":
            in_backtick = True; has_token = True; i += 1; continue
        if ch == "(":
            depth += 1
            has_token = True
        elif ch == ")":
            if depth > 0:
                depth -= 1
        elif ch == "," and depth == 0:
            if has_token:
                count += 1
            has_token = False
        elif depth == 0 and not ch.isspace():
            has_token = True
        i += 1
    if has_token:
        count += 1
    return count

def count_values_tuples(sql_tail):
    depth = 0
    in_single = in_double = in_backtick = False
    count = 0
    i, n = 0, len(sql_tail)
    while i < n:
        ch = sql_tail[i]
        nxt = sql_tail[i + 1] if i + 1 < n else ""
        if in_single:
            if ch == "\\" and nxt:
                i += 2; continue
            if ch == "'" and nxt != "'":
                in_single = False
            elif ch == "'" and nxt == "'":
                i += 2; continue
            i += 1; continue
        if in_double:
            if ch == "\\" and nxt:
                i += 2; continue
            if ch == '"' and nxt != '"':
                in_double = False
            elif ch == '"' and nxt == '"':
                i += 2; continue
            i += 1; continue
        if in_backtick:
            if ch == "`":
                in_backtick = False
            i += 1; continue
        if ch == "'":
            in_single = True; i += 1; continue
        if ch == '"':
            in_double = True; i += 1; continue
        if ch == "`":
            in_backtick = True; i += 1; continue
        if ch == "(":
            if depth == 0:
                count += 1
            depth += 1
        elif ch == ")":
            if depth > 0:
                depth -= 1
        i += 1
    return count if count > 0 else None

def count_insert_values(stmt):
    m = re.search(r"\bvalues\b", stmt, re.I)
    if not m:
        return None
    tail = stmt[m.end():]
    stop = re.search(r"\bon\s+(duplicate|conflict)\b", tail, re.I)
    if stop:
        tail = tail[:stop.start()]
    return count_values_tuples(tail)

def count_delete_in_values(stmt):
    m = re.search(r"\bin\s*\(", stmt, re.I)
    if not m:
        return None
    start = stmt.find("(", m.end() - 1)
    if start == -1:
        return None
    end = find_matching_paren(stmt, start)
    if end is None or end <= start + 1:
        return None
    content = stmt[start + 1:end].strip()
    if not content:
        return None
    if re.match(r"^select\b", content, re.I):
        return None
    return count_top_level_items(content)

def count_apply_stats(sql_text):
    insert_stmt_count = 0
    insert_value_count = 0
    delete_stmt_count = 0
    delete_value_count = 0
    unknown_value_count = 0
    for s in split_sql_statements(sql_text):
        s_strip = s.strip()
        if not s_strip:
            continue
        if re.match(r"^(BEGIN|COMMIT|ROLLBACK|START\s+TRANSACTION)\b", s_strip, re.I):
            continue
        if re.match(r"^(INSERT|REPLACE)\s+INTO\b", s_strip, re.I):
            insert_stmt_count += 1
            vcnt = count_insert_values(s_strip)
            if vcnt is None:
                unknown_value_count += 1
            else:
                insert_value_count += vcnt
        elif re.match(r"^DELETE\s+FROM\b", s_strip, re.I):
            delete_stmt_count += 1
            if re.search(r"\blimit\s+1\b", s_strip, re.I):
                delete_value_count += 1
            else:
                vcnt = count_delete_in_values(s_strip)
                if vcnt is None:
                    unknown_value_count += 1
                else:
                    delete_value_count += vcnt
    return insert_stmt_count, insert_value_count, delete_stmt_count, delete_value_count, unknown_value_count

def get_table_columns(conn, db, table):
    try: return conn.query(f"SHOW COLUMNS FROM `{db}`.`{table}`")
    except: return None

def table_has_primary_key(conn, db, table):
    try:
        rows = conn.query(f"SHOW INDEX FROM `{db}`.`{table}`")
    except:
        return False
    return any(r.get("Key_name") == "PRIMARY" for r in rows)

def table_has_secondary_index(conn, db, table):
    try:
        rows = conn.query(f"SHOW INDEX FROM `{db}`.`{table}`")
    except:
        return False
    for r in rows:
        name = r.get("Key_name")
        if name and name != "PRIMARY":
            return True
    return False

def is_integer_type(col):
    t = str(col.get("Type") or "").lower()
    return t.startswith(("int", "integer", "bigint", "smallint", "tinyint"))

def count_distinct_single(conn, db, table, col):
    sql = f"SELECT COUNT(DISTINCT `{col}`) as c FROM `{db}`.`{table}`"
    r = conn.fetch_one(sql)
    return r["c"] if r and "c" in r else None

def make_index_name(cols):
    base = "idx_cdc_" + "_".join(cols)
    if len(base) <= 60:
        return base
    h = hashlib.md5(base.encode()).hexdigest()[:8]
    return f"{base[:50]}_{h}"

def ensure_aux_index_for_no_pk(conn, db, table):
    if table_has_primary_key(conn, db, table):
        return
    if table_has_secondary_index(conn, db, table):
        log.info(f"Skip auto index: secondary index exists table={db}.{table}")
        return
    cols = get_table_columns(conn, db, table) or []
    int_cols = [c["Field"] for c in cols if c.get("Field") and is_integer_type(c)]
    if len(int_cols) < 2:
        if len(int_cols) == 1:
            best_pair = (int_cols[0],)
            idx_name = make_index_name(best_pair)
            try:
                conn.execute(f"CREATE INDEX `{idx_name}` ON `{db}`.`{table}` (`{best_pair[0]}`)")
                conn.commit()
                log.info(f"Auto index created table={db}.{table} index={idx_name} cols=({best_pair[0]})")
            except Exception as e:
                log.warning(f"Auto index create failed table={db}.{table} index={idx_name}: {e}")
        else:
            log.warning(f"No suitable integer columns for auto index table={db}.{table}")
        return
    log.info(f"Auto index eval cols table={db}.{table} cols={int_cols}")
    total = get_table_count(conn, db, table)
    if total is None or total == 0:
        log.warning(f"Skip auto index: empty table or count failed table={db}.{table}")
        return
    scores = []
    for c in int_cols:
        try:
            log.info(f"Auto index single sel start col={c}")
            t0 = time.time()
            distinct = count_distinct_single(conn, db, table, c)
            elapsed = time.time() - t0
            if distinct is None:
                log.warning(f"Auto index single selectivity failed col={c} table={db}.{table}")
                continue
            sel = float(distinct) / float(total)
            log.info(f"Auto index single sel col={c} distinct={distinct} total={total} sel={sel:.6f} time={elapsed:.3f}s")
            scores.append((sel, c))
        except Exception as e:
            log.warning(f"Auto index single selectivity error col={c} table={db}.{table}: {e}")
    if not scores:
        log.warning(f"Auto index skipped: no usable columns table={db}.{table}")
        return
    scores.sort(reverse=True)
    if len(scores) < 2:
        log.warning(f"Auto index skipped: not enough integer columns table={db}.{table}")
        return
    best_pair = (scores[0][1], scores[1][1])
    best_sel = min(scores[0][0], scores[1][0])
    idx_name = make_index_name(best_pair)
    try:
        conn.execute(f"CREATE INDEX `{idx_name}` ON `{db}`.`{table}` (`{best_pair[0]}`, `{best_pair[1]}`)")
        conn.commit()
        log.info(f"Auto index created table={db}.{table} index={idx_name} cols=({best_pair[0]},{best_pair[1]}) sel={best_sel:.6f} total={total}")
    except Exception as e:
        log.warning(f"Auto index create failed table={db}.{table} index={idx_name}: {e}")

def build_check_sql(db, table, cols, mo_ts=None):
    sc = f"{{MO_TS = {mo_ts}}}" if mo_ts else ""
    if not cols:
        return f"SELECT COUNT(*) as c, NULL as h FROM `{db}`.`{table}`{sc}"
    p_cols = [f"IFNULL(HEX(`{c['Field']}`), 'NULL')" if "vec" in c['Type'].lower() else f"IFNULL(CAST(`{c['Field']}` AS VARCHAR), 'NULL')" for c in cols]
    return f"SELECT COUNT(*) as c, BIT_XOR(CRC32(CONCAT_WS(',', {', '.join(p_cols)}))) as h FROM `{db}`.`{table}`{sc}"

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

def get_table_count(conn, db, table, mo_ts=None):
    sc = f"{{MO_TS = {mo_ts}}}" if mo_ts else ""
    try:
        res = conn.fetch_one(f"SELECT COUNT(*) as c FROM `{db}`.`{table}`{sc}")
    except:
        return None
    return res["c"] if res else None

def normalize_verify_columns(config):
    cols = config.get("verify_columns")
    if cols is None:
        cols = (config.get("verify") or {}).get("columns")
    if not cols:
        return []
    if isinstance(cols, str):
        return [c.strip() for c in cols.split(",") if c.strip()]
    if isinstance(cols, list):
        return [str(c).strip() for c in cols if str(c).strip()]
    return []

def resolve_fast_check_columns(u_cols, d_cols, config):
    u_map = {c["Field"]: c for c in u_cols}
    d_map = {c["Field"]: c for c in d_cols}
    req = [c for c in normalize_verify_columns(config) if c in u_map and c in d_map]
    u_pks = [c['Field'] for c in u_cols if c.get('Key') == 'PRI']
    d_pks = [c['Field'] for c in d_cols if c.get('Key') == 'PRI']
    pk_cols = [c for c in u_pks if c in d_pks]
    use_cpkey = False
    if len(pk_cols) > 1 and "__mo_cpkey_col" in u_map and "__mo_cpkey_col" in d_map:
        pk_cols = ["__mo_cpkey_col"]
        use_cpkey = True
    cols = []
    if pk_cols and req:
        cols = pk_cols + [c for c in req if c not in pk_cols]
        detail = "pk+cols_cpkey" if use_cpkey else "pk+cols"
    elif pk_cols:
        cols = pk_cols
        detail = "pk_cpkey" if use_cpkey else "pk"
    elif req:
        cols = req
        detail = "cols"
    else:
        detail = "count"
    u_sel = [u_map[c] for c in cols] if cols else []
    d_sel = [d_map[c] for c in cols] if cols else []
    return u_sel, d_sel, detail

def is_small_table(up_conn, config, mo_ts=None):
    size_bytes = get_table_size_bytes(up_conn, config["upstream"]["db"], config["upstream"]["table"])
    if size_bytes is not None and size_bytes > 0:
        return size_bytes < FULL_VERIFY_MAX_BYTES
    row_count = get_table_count(up_conn, config["upstream"]["db"], config["upstream"]["table"], mo_ts=mo_ts)
    if row_count is not None:
        return row_count < FULL_VERIFY_MAX_ROWS
    return False

def verify_consistency(up_conn, ds_conn, config, mo_ts=None, mode="fast", return_detail=False):
    u, d = config["upstream"], config["downstream"]
    err = None
    u_time = None
    d_time = None
    detail = None
    try:
        u_cols = get_table_columns(up_conn, u["db"], u["table"])
        d_cols = get_table_columns(ds_conn, d["db"], d["table"])
        if not u_cols or not d_cols:
            if return_detail:
                err = "failed to load table columns"
                return False, None, None, detail, err, u_time, d_time
            return False
        if mode == "full":
            u_sel, d_sel = u_cols, d_cols
            detail = "all"
        else:
            u_sel, d_sel, detail = resolve_fast_check_columns(u_cols, d_cols, config)
        u_sql = build_check_sql(u["db"], u["table"], u_sel, mo_ts)
        d_sql = build_check_sql(d["db"], d["table"], d_sel, None)
        if u_sql is None or d_sql is None:
            if return_detail:
                err = "failed to build verify SQL"
                return False, None, None, detail, err, u_time, d_time
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
            return ok, ur, dr, detail, err, u_time, d_time
        return ok
    except Exception as e:
        err = str(e)
        if return_detail:
            return False, None, None, detail, err, u_time, d_time
        return False

def verify_watermark_consistency(up_conn, ds_conn, config, mo_ts, retries=3, wait_sec=5):
    for attempt in range(1, retries + 1):
        ok, ur, dr, detail, err, _, _ = verify_consistency(up_conn, ds_conn, config, mo_ts, mode="fast", return_detail=True)
        if ok:
            if detail:
                log.info(f"FAST CHECK PASSED type={detail}")
            return True
        if err or ur is None or dr is None:
            msg = err or "verify returned empty result"
            log.warning(f"Watermark check error (attempt {attempt}/{retries}): {msg}")
            if attempt < retries:
                time.sleep(wait_sec)
                continue
            return None
        log_verify_result(False, ur, dr, mode="FAST", detail=detail, mo_ts_label=mo_ts)
        return False
    return None

def format_check_value(val):
    if val is None:
        return "NULL"
    return str(val)

def format_mo_ts_utc(mo_ts):
    if mo_ts is None:
        return None
    try:
        ts = int(mo_ts)
    except (TypeError, ValueError):
        return None
    secs = ts // 1_000_000_000
    frac = ts % 1_000_000_000
    dt = datetime.utcfromtimestamp(secs)
    frac_str = f"{frac:09d}"[:7]
    return f"{dt.strftime('%Y-%m-%d %H:%M:%S')}.{frac_str} UTC"

def log_verify_result(ok, ur, dr, mode="FAST", detail=None, mo_ts_label=None, table_label=None):
    table_info = f" table={table_label}" if table_label else ""
    snap_info = f" mo_ts={mo_ts_label}" if mo_ts_label else ""
    detail_info = f" type={detail}" if detail and mode == "FAST" else ""
    if ok and ur:
        log.info(f"[green]{mode} VERIFY PASSED[/green]{detail_info} upstream rows={ur['c']} hash={format_check_value(ur['h'])}{table_info}{snap_info}")
        return
    u_cnt = ur['c'] if ur else "N/A"
    u_hash = format_check_value(ur['h']) if ur else "N/A"
    d_cnt = dr['c'] if dr else "N/A"
    d_hash = format_check_value(dr['h']) if dr else "N/A"
    log.error(f"[red]{mode} VERIFY FAILED[/red]{detail_info} upstream rows={u_cnt} hash={u_hash} downstream rows={d_cnt} hash={d_hash}{table_info}{snap_info}")

def build_status_panel(config, last_sync=None, last_verify=None, last_error=None):
    up = config.get("upstream", {})
    ds = config.get("downstream", {})
    scope = get_sync_scope(config)
    up_table = "*" if scope == "database" else up.get("table")
    ds_table = "*" if scope == "database" else ds.get("table")
    tbl = Table.grid(padding=(0, 1))
    tbl.add_row("Scope", config.get("sync_scope", "table"))
    tbl.add_row("Up", f"{up.get('db')}.{up_table}")
    tbl.add_row("Ds", f"{ds.get('db')}.{ds_table}")
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

def check_mo_ts_available(up_conn, config, mo_ts):
    u_cfg = config["upstream"]
    try:
        up_conn.fetch_one(f"SELECT 1 FROM `{u_cfg['db']}`.`{u_cfg['table']}`{{MO_TS = {mo_ts}}} LIMIT 1")
        return True
    except:
        return False

def validate_pitr_config(up_conn, config):
    pitr_cfg = config.get("pitr") or {}
    name = pitr_cfg.get("name")
    if not name:
        return False, "PITR not configured"
    expected_level, expected_obj = resolve_pitr_target(up_conn, config)
    if expected_obj is None:
        return False, "PITR target not found"
    rec = get_pitr_record(up_conn, name)
    if not rec:
        return False, f"PITR {name} not found"
    status = rec.get("pitr_status")
    if status is not None and str(status) != "1":
        return False, f"PITR {name} inactive"
    rec_obj = rec.get("obj_id")
    rec_level = rec.get("level")
    if rec_level and rec_level != expected_level:
        return False, f"PITR {name} level mismatch"
    if rec_obj is not None and int(rec_obj) != int(expected_obj):
        return False, f"PITR {name} obj_id mismatch"
    return True, None

def configure_pitr(config):
    u_cfg = config["upstream"]
    up_conn = DBConnection(u_cfg, "UpPITR", autocommit=True)
    if not up_conn.connect(db_override=""):
        log.error("Upstream connection failed; cannot configure PITR.")
        return config.get("pitr")
    level, obj_id = resolve_pitr_target(up_conn, config)
    if obj_id is None:
        log.error("Resolve object id failed; cannot configure PITR.")
        up_conn.close()
        return config.get("pitr")
    pitr_list = list_active_pitr(up_conn, level, obj_id)
    choices = ["Create new PITR"]
    choice_map = {}
    for p in pitr_list:
        label = format_pitr_label(p)
        choices.append(label)
        choice_map[label] = p
    chosen = questionary.select("Select PITR:", choices=choices).ask()
    if chosen is None:
        up_conn.close()
        return config.get("pitr")

    def ask_range_value(default_val):
        while True:
            val = questionary.text("PITR range value:", default=str(default_val)).ask()
            if val is None:
                return None
            try:
                num = int(val)
            except ValueError:
                log.warning(f"Invalid PITR range value: {val}")
                continue
            if num <= 0 or num > 100:
                log.warning(f"PITR range out of range: {num}")
                continue
            return num

    def ask_range_unit(default_unit):
        return questionary.select("PITR range unit:", choices=["h", "d", "mo", "y"], default=default_unit).ask()

    if chosen == "Create new PITR":
        name_default = generate_pitr_name(u_cfg["db"], u_cfg["table"] if level == "table" else None)
        name = questionary.text("PITR name:", default=name_default).ask()
        if name is None:
            up_conn.close()
            return config.get("pitr")
        length = ask_range_value(DEFAULT_PITR_RANGE_DAYS)
        if length is None:
            up_conn.close()
            return config.get("pitr")
        unit = ask_range_unit("d")
        if unit is None:
            up_conn.close()
            return config.get("pitr")
        try:
            if level == "database":
                up_conn.execute(f"CREATE PITR `{name}` FOR DATABASE `{u_cfg['db']}` RANGE {length} '{unit}'"); up_conn.commit()
            else:
                up_conn.execute(f"CREATE PITR `{name}` FOR TABLE `{u_cfg['db']}` `{u_cfg['table']}` RANGE {length} '{unit}'"); up_conn.commit()
        except Exception as e:
            log.error(f"Create PITR failed: {e}")
            up_conn.close()
            return config.get("pitr")
        pitr_cfg = {"name": name, "level": level, "obj_id": obj_id, "length": length, "unit": unit}
    else:
        p = choice_map.get(chosen) or {}
        name = p.get("pitr_name")
        length_default = p.get("pitr_length") or DEFAULT_PITR_RANGE_DAYS
        unit_default = p.get("pitr_unit") or "d"
        length = ask_range_value(length_default)
        if length is None:
            up_conn.close()
            return config.get("pitr")
        unit = ask_range_unit(unit_default)
        if unit is None:
            up_conn.close()
            return config.get("pitr")
        try:
            up_conn.execute(f"ALTER PITR `{name}` RANGE {length} '{unit}'"); up_conn.commit()
        except Exception as e:
            log.error(f"Alter PITR failed: {e}")
            up_conn.close()
            return config.get("pitr")
        pitr_cfg = {"name": name, "level": p.get("level") or level, "obj_id": p.get("obj_id") or obj_id, "length": length, "unit": unit}

    up_conn.close()
    return pitr_cfg

def list_database_tables(up_conn, db):
    try:
        rows = up_conn.query(f"SHOW TABLES FROM `{db}`")
    except Exception as e:
        log.error(f"Show tables failed: {db}: {e}")
        return []
    out = []
    tmp_suffixes = ("_copy_now", "_copy_prev", "_zero")
    for r in rows:
        if not r:
            continue
        name = next(iter(r.values()))
        if not name:
            continue
        name = str(name)
        if any(name.endswith(suf) for suf in tmp_suffixes):
            continue
        out.append(name)
    return out

def ensure_downstream_table(ds_conn, up_conn, u_db, u_table, d_db, d_table):
    target_exists = ds_conn.query(f"SHOW TABLES LIKE '{d_table}'")
    if target_exists:
        return
    ddl = up_conn.fetch_one(f"SHOW CREATE TABLE `{u_db}`.`{u_table}`")['Create Table']
    ddl = ddl.replace(f"`{u_table}`", f"`{d_table}`", 1)
    ds_conn.execute(ddl); ds_conn.commit()

def build_full_diff_files(up_conn, u_db, u_table, stage_name, new_mo_ts):
    t_zero = f"{u_table}_zero"
    t_cn = f"{u_table}_copy_now"
    up_conn.execute(f"DROP TABLE IF EXISTS `{u_db}`.`{t_zero}`"); up_conn.commit()
    up_conn.execute(f"DROP TABLE IF EXISTS `{u_db}`.`{t_cn}`"); up_conn.commit()
    up_conn.execute(f"CREATE TABLE `{u_db}`.`{t_zero}` LIKE `{u_db}`.`{u_table}`"); up_conn.commit()
    try:
        up_conn.execute(f"data branch create table `{u_db}`.`{t_cn}` from `{u_db}`.`{u_table}`{{MO_TS = {new_mo_ts}}}"); up_conn.commit()
        return up_conn.query(f"data branch diff `{u_db}`.`{t_cn}` against `{u_db}`.`{t_zero}` output file 'stage://{stage_name}'")
    finally:
        up_conn.execute(f"DROP TABLE IF EXISTS `{u_db}`.`{t_cn}`"); up_conn.commit()
        up_conn.execute(f"DROP TABLE IF EXISTS `{u_db}`.`{t_zero}`"); up_conn.commit()

def build_incremental_diff_files(up_conn, u_db, u_table, stage_name, lastgood, new_mo_ts):
    t_cp = f"{u_table}_copy_prev"
    t_cn = f"{u_table}_copy_now"
    up_conn.execute(f"DROP TABLE IF EXISTS `{u_db}`.`{t_cp}`"); up_conn.commit()
    up_conn.execute(f"DROP TABLE IF EXISTS `{u_db}`.`{t_cn}`"); up_conn.commit()
    try:
        up_conn.execute(f"data branch create table `{u_db}`.`{t_cp}` from `{u_db}`.`{u_table}`{{MO_TS = {lastgood}}}"); up_conn.commit()
        up_conn.execute(f"data branch create table `{u_db}`.`{t_cn}` from `{u_db}`.`{u_table}`{{MO_TS = {new_mo_ts}}}"); up_conn.commit()
        return up_conn.query(f"data branch diff `{u_db}`.`{t_cn}` against `{u_db}`.`{t_cp}` output file 'stage://{stage_name}'")
    except Exception as e:
        raise IncrementalFallback(str(e))
    finally:
        up_conn.execute(f"DROP TABLE IF EXISTS `{u_db}`.`{t_cn}`"); up_conn.commit()
        up_conn.execute(f"DROP TABLE IF EXISTS `{u_db}`.`{t_cp}`"); up_conn.commit()

def apply_full_diff(ds_conn, d_db, d_table, dfiles):
    ds_conn.execute(f"TRUNCATE TABLE `{d_db}`.`{d_table}`")
    for r in dfiles:
        f, sl, qt = get_diff_file_path(r), chr(92), chr(34)
        if not f:
            continue
        ds_conn.execute(f"LOAD DATA INFILE '{f}' INTO TABLE `{d_db}`.`{d_table}` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '{qt}' ESCAPED BY '{sl}{sl}' LINES TERMINATED BY '{sl}n' PARALLEL 'TRUE'")

def apply_incremental_diff(up_conn, ds_conn, d_db, d_table, dfiles):
    applied_stmt_count = 0
    target = f"`{d_db}`.`{d_table}`"
    rewrite = re.compile(r'(delete\s+from\s+|replace\s+into\s+|insert\s+into\s+|update\s+)(/\*.*?\*/\s+)?([`\w]+\.[`\w]+|[`\w]+)', re.I|re.S)
    total_insert_stmts = 0
    total_insert_values = 0
    total_delete_stmts = 0
    total_delete_values = 0
    total_unknown_values = 0

    for r in dfiles:
        f = get_diff_file_path(r)
        if not f:
            continue
        raw = up_conn.fetch_one("select load_file(cast(%s as datalink)) as c", (f,))['c']
        if raw is None:
            log.error(f"Load diff file failed: {f}")
            raise RuntimeError(f"load_file returned NULL: {f}")
        stmt_str = raw.decode('utf-8') if isinstance(raw, bytes) else raw
        c_ins, c_ins_v, c_del, c_del_v, c_unk = count_apply_stats(stmt_str)
        total_insert_stmts += c_ins
        total_insert_values += c_ins_v
        total_delete_stmts += c_del
        total_delete_values += c_del_v
        total_unknown_values += c_unk

    if total_insert_stmts or total_delete_stmts:
        stats = (
            f"Apply stats pre table={d_db}.{d_table} "
            f"insert_stmts={total_insert_stmts} insert_values={total_insert_values} "
            f"delete_stmts={total_delete_stmts} delete_values={total_delete_values}"
        )
        if total_unknown_values:
            stats += f" unknown_values={total_unknown_values}"
        log.info(stats)

    for r in dfiles:
        f = get_diff_file_path(r)
        if not f:
            continue
        raw = up_conn.fetch_one("select load_file(cast(%s as datalink)) as c", (f,))['c']
        if raw is None:
            log.error(f"Load diff file failed: {f}")
            raise RuntimeError(f"load_file returned NULL: {f}")
        stmt_str = raw.decode('utf-8') if isinstance(raw, bytes) else raw
        sql = rewrite.sub(lambda m: f"{m.group(1)}{m.group(2) or ''} {target} ", stmt_str)
        stmt_count = 0
        for s in split_sql_statements(sql):
            s_strip = s.strip()
            if not s_strip:
                continue
            if re.match(r"^(BEGIN|COMMIT|ROLLBACK|START\s+TRANSACTION)\b", s_strip, re.I):
                continue
            ds_conn.execute(s_strip)
            applied_stmt_count += 1
            stmt_count += 1
    return applied_stmt_count

def sync_database_table(up_conn, ds_conn, u_db, u_table, d_db, stage_name, lastgood, new_mo_ts):
    d_table = u_table
    diff_start = time.time()
    if lastgood:
        try:
            dfiles = build_incremental_diff_files(up_conn, u_db, u_table, stage_name, lastgood, new_mo_ts)
            mode = "INCREMENTAL"
        except IncrementalFallback as e:
            log.warning(f"Incremental failed {u_db}.{u_table}: {e}; fallback to FULL.")
            dfiles = build_full_diff_files(up_conn, u_db, u_table, stage_name, new_mo_ts)
            mode = "FULL"
    else:
        dfiles = build_full_diff_files(up_conn, u_db, u_table, stage_name, new_mo_ts)
        mode = "FULL"
    diff_elapsed = time.time() - diff_start

    apply_start = time.time()
    if mode == "FULL":
        apply_full_diff(ds_conn, d_db, d_table, dfiles)
        applied_stmt_count = None
    else:
        applied_stmt_count = apply_incremental_diff(up_conn, ds_conn, d_db, d_table, dfiles)
    apply_elapsed = time.time() - apply_start
    return mode, dfiles, applied_stmt_count, diff_elapsed, apply_elapsed

def perform_db_sync(config, is_auto=False, lock_held=False, force_full=False, return_detail=False):
    start_ts = time.time()
    diff_elapsed = 0.0
    apply_elapsed = 0.0
    timings = []
    sync_success = False
    sync_status = "FAILED"
    sync_kind = "UNKNOWN"
    u_cfg, d_cfg = config["upstream"], config["downstream"]
    tid = get_task_id(config)
    up_conn, ds_conn = DBConnection(u_cfg, "Up", autocommit=True), DBConnection(d_cfg, "Ds")
    keeper = LockKeeper(d_cfg, tid)
    new_mo_ts = None

    def sync_return(ok):
        if return_detail:
            return ok, sync_kind, sync_status
        return ok

    try:
        precheck_start = time.time()
        if not up_conn.connect() or not ds_conn.connect(db_override=""):
            log.error("Sync FAILED: connection failed")
            return sync_return(False)
        ds_conn.execute(f"CREATE DATABASE IF NOT EXISTS `{d_cfg['db']}`"); ds_conn.commit()
        ensure_meta_table(ds_conn)
        ds_conn.execute(f"USE `{d_cfg['db']}`"); ds_conn.commit()
        ok, err = validate_pitr_config(up_conn, config)
        if not ok:
            log.error(f"Sync FAILED: {err}")
            return sync_return(False)
        if lock_held:
            if not lock_is_held(ds_conn, tid):
                log.warning("Lock lost before sync, skipping this cycle.")
                return sync_return(False)
        else:
            if not acquire_lock(ds_conn, tid):
                return sync_return(False)
            keeper.start()

        tables = list_database_tables(up_conn, u_cfg["db"])
        if not tables:
            log.warning("No tables found for database sync.")
            return sync_return(False)
        for t in tables:
            ensure_downstream_table(ds_conn, up_conn, u_cfg["db"], t, d_cfg["db"], t)
            ensure_aux_index_for_no_pk(ds_conn, d_cfg["db"], t)
        record_timing(timings, "precheck", precheck_start)

        watermark_start = time.time()
        ws = get_watermarks(ds_conn, tid)
        lastgood = ws[0] if ws else None
        if force_full:
            lastgood = None
        if lastgood is not None:
            try:
                lastgood = int(lastgood)
            except (TypeError, ValueError):
                log.warning(f"Invalid watermark {lastgood}. Resetting to FULL sync.")
                ds_conn.execute(f"DELETE FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s", (tid,)); ds_conn.commit()
                lastgood = None
        record_timing(timings, "watermark", watermark_start)

        stage_start = time.time()
        stage_name, stage_url = get_stage_config(config)
        if not stage_name:
            log.error("Stage name is missing; check config.stage.name or config.stage.url.")
            return sync_return(False)
        if not ensure_stage(up_conn, stage_name, stage_url):
            log.error("Stage setup failed; aborting sync.")
            return sync_return(False)
        record_timing(timings, "stage", stage_start)

        sync_kind = "FULL" if not lastgood else "INCREMENTAL"
        # Force a non-zero tail even if system clock resolution is coarse.
        new_mo_ts = max(0, time.time_ns() - 1)
        ds_conn.conn.begin()
        try:
            for t in tables:
                try:
                    mode, dfiles, applied_stmt_count, diff_cost, apply_cost = sync_database_table(
                        up_conn, ds_conn, u_cfg["db"], t, d_cfg["db"], stage_name, lastgood, new_mo_ts
                    )
                except Exception as e:
                    raise RuntimeError(f"Table sync failed: {u_cfg['db']}.{t}: {e}") from e
                diff_elapsed += diff_cost
                apply_elapsed += apply_cost
                if mode == "INCREMENTAL":
                    if applied_stmt_count == 0:
                        log.info(f"[yellow]Apply diff done table={u_cfg['db']}.{t} mode=INCREMENTAL noop mo_ts={new_mo_ts}[/yellow]")
                    else:
                        log.info(f"[green]Apply diff done table={u_cfg['db']}.{t} mode=INCREMENTAL applied={applied_stmt_count} mo_ts={new_mo_ts}[/green]")
                else:
                    log.info(f"[green]Apply diff done table={u_cfg['db']}.{t} mode=FULL mo_ts={new_mo_ts}[/green]")
                cleanup_start = time.time()
                remove_stage_files(up_conn, dfiles)
                record_timing(timings, "cleanup", cleanup_start)
            apply_start = time.time()
            ds_conn.execute(f"INSERT INTO `{META_DB}`.`{META_TABLE}` (task_id, watermark) VALUES (%s, %s)", (tid, new_mo_ts))
            ds_conn.commit()
            apply_elapsed += time.time() - apply_start
            sync_success = True
        except Exception as e:
            ds_conn.rollback()
            raise e

        if sync_success:
            sync_status = "SUCCESS"
            log.info(f"[green]Sync SUCCESS | {new_mo_ts}[/green]")
            prune_watermarks(ds_conn, tid)
            return sync_return(True)
        return sync_return(False)
    except KeyboardInterrupt:
        try:
            ds_conn.rollback()
        except:
            pass
        log.info("Sync interrupted by user.")
        raise
    except KeyboardInterrupt:
        try:
            ds_conn.rollback()
        except:
            pass
        log.info("Sync interrupted by user.")
        raise
    except Exception as e:
        log.error(f"Sync FAILED: {e}")
        return sync_return(False)
    finally:
        elapsed = time.time() - start_ts
        log.info(f"Sync duration={diff_elapsed:.3f}/{apply_elapsed:.3f}/{elapsed:.3f}s status={sync_status}")
        if diff_elapsed > 0:
            timings.append(("diff", diff_elapsed))
        if apply_elapsed > 0:
            timings.append(("apply", apply_elapsed))
        top3 = summarize_top_timings(timings)
        if top3:
            log.info(f"Sync timing top3={top3}")
        log.info("-" * 72)
        if keeper.is_alive():
            keeper.stop(); keeper.join()
        if not lock_held:
            release_lock(ds_conn, tid)
        up_conn.close(); ds_conn.close()

def perform_sync(config, is_auto=False, lock_held=False, force_full=False, return_detail=False):
    if get_sync_scope(config) == "database":
        return perform_db_sync(config, is_auto=is_auto, lock_held=lock_held, force_full=force_full, return_detail=return_detail)
    return perform_table_sync(config, is_auto=is_auto, lock_held=lock_held, force_full=force_full, return_detail=return_detail)

def perform_table_sync(config, is_auto=False, lock_held=False, force_full=False, return_detail=False):
    start_ts = time.time()
    diff_elapsed = 0.0
    apply_elapsed = 0.0
    timings = []
    sync_success = False
    sync_status = "FAILED"
    sync_noop = False
    sync_kind = "UNKNOWN"
    u_cfg, d_cfg = config["upstream"], config["downstream"]
    tid = get_task_id(config)
    up_conn, ds_conn = DBConnection(u_cfg, "Up", autocommit=True), DBConnection(d_cfg, "Ds")
    keeper = LockKeeper(d_cfg, tid)
    dfiles = []
    new_mo_ts = None
    def sync_return(ok):
        if return_detail:
            return ok, sync_kind, sync_status
        return ok

    try:
        precheck_start = time.time()
        if not up_conn.connect() or not ds_conn.connect(db_override=""):
            log.error("Sync FAILED: connection failed")
            return sync_return(False)
        ds_conn.execute(f"CREATE DATABASE IF NOT EXISTS `{d_cfg['db']}`"); ds_conn.commit()
        ensure_meta_table(ds_conn)
        ok, err = validate_pitr_config(up_conn, config)
        if not ok:
            log.error(f"Sync FAILED: {err}")
            return sync_return(False)
        if lock_held:
            if not lock_is_held(ds_conn, tid):
                log.warning("Lock lost before sync, skipping this cycle.")
                return sync_return(False)
        else:
            if not acquire_lock(ds_conn, tid): return sync_return(False)
            keeper.start()
        
        ds_conn.execute(f"USE `{d_cfg['db']}`"); ds_conn.commit()
        target_exists = ds_conn.query(f"SHOW TABLES LIKE '{d_cfg['table']}'")
        if not target_exists:
            ddl = up_conn.fetch_one(f"SHOW CREATE TABLE `{u_cfg['db']}`.`{u_cfg['table']}`")['Create Table'].replace(f"`{u_cfg['table']}`", f"`{d_cfg['table']}`", 1)
            ds_conn.execute(ddl); ds_conn.commit()
        ensure_aux_index_for_no_pk(ds_conn, d_cfg["db"], d_cfg["table"])
        record_timing(timings, "precheck", precheck_start)

        watermark_start = time.time()
        ws = get_watermarks(ds_conn, tid)
        lastgood = ws[0] if ws else None
        if force_full:
            lastgood = None
        if lastgood is not None:
            try:
                lastgood = int(lastgood)
            except (TypeError, ValueError):
                log.warning(f"Invalid watermark {lastgood}. Resetting to FULL sync.")
                ds_conn.execute(f"DELETE FROM `{META_DB}`.`{META_TABLE}` WHERE task_id=%s", (tid,)); ds_conn.commit()
                lastgood = None

        if lastgood and not check_mo_ts_available(up_conn, config, lastgood):
            log.warning(f"MO_TS {lastgood} unavailable. Resetting to FULL sync.")
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
                return sync_return(False)
        record_timing(timings, "watermark", watermark_start)

        stage_start = time.time()
        stage_name, stage_url = get_stage_config(config)
        if not stage_name:
            log.error("Stage name is missing; check config.stage.name or config.stage.url.")
            return sync_return(False)
        if not ensure_stage(up_conn, stage_name, stage_url):
            log.error("Stage setup failed; aborting sync.")
            return sync_return(False)
        record_timing(timings, "stage", stage_start)

        sync_kind = "FULL" if not lastgood else "INCREMENTAL"
        # Force a non-zero tail even if system clock resolution is coarse.
        new_mo_ts = max(0, time.time_ns() - 1)
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as prg:
            if not lastgood:
                log.info(f"[blue]FULL Sync | Task: {tid}[/blue]")
                t_zero = f"{u_cfg['table']}_zero"
                t_cn = f"{u_cfg['table']}_copy_now"
                up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_zero}`"); up_conn.commit()
                up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_cn}`"); up_conn.commit()
                up_conn.execute(f"CREATE TABLE `{u_cfg['db']}`.`{t_zero}` LIKE `{u_cfg['db']}`.`{u_cfg['table']}`"); up_conn.commit()
                diff_start = time.time()
                try:
                    up_conn.execute(f"data branch create table `{u_cfg['db']}`.`{t_cn}` from `{u_cfg['db']}`.`{u_cfg['table']}`{{MO_TS = {new_mo_ts}}}"); up_conn.commit()
                    dfiles = up_conn.query(f"data branch diff `{u_cfg['db']}`.`{t_cn}` against `{u_cfg['db']}`.`{t_zero}` output file 'stage://{stage_name}'")
                finally:
                    up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_cn}`"); up_conn.commit()
                    up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_zero}`"); up_conn.commit()
                diff_elapsed += time.time() - diff_start
                
                apply_start = time.time()
                ds_conn.conn.begin()
                try:
                    ds_conn.execute(f"TRUNCATE TABLE `{d_cfg['db']}`.`{d_cfg['table']}`")
                    for r in dfiles:
                        f, sl, qt = get_diff_file_path(r), chr(92), chr(34)
                        if not f: continue
                        ds_conn.execute(f"LOAD DATA INFILE '{f}' INTO TABLE `{d_cfg['db']}`.`{d_cfg['table']}` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '{qt}' ESCAPED BY '{sl}{sl}' LINES TERMINATED BY '{sl}n' PARALLEL 'TRUE'")
                    ds_conn.execute(f"INSERT INTO `{META_DB}`.`{META_TABLE}` (task_id, watermark) VALUES (%s, %s)", (tid, new_mo_ts)); ds_conn.commit(); sync_success = True
                except Exception as e:
                    ds_conn.rollback(); raise e
                finally:
                    apply_elapsed += time.time() - apply_start
            else:
                log.info(f"[blue]INCREMENTAL Sync | Task: {tid} | From {lastgood}[/blue]")
                t_cp = f"{u_cfg['table']}_copy_prev"
                t_cn = f"{u_cfg['table']}_copy_now"
                up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_cp}`"); up_conn.commit()
                up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_cn}`"); up_conn.commit()
                up_conn.execute(f"data branch create table `{u_cfg['db']}`.`{t_cp}` from `{u_cfg['db']}`.`{u_cfg['table']}`{{MO_TS = {lastgood}}}"); up_conn.commit()
                diff_start = time.time()
                try:
                    up_conn.execute(f"data branch create table `{u_cfg['db']}`.`{t_cn}` from `{u_cfg['db']}`.`{u_cfg['table']}`{{MO_TS = {new_mo_ts}}}"); up_conn.commit()
                    dfiles = up_conn.query(f"data branch diff `{u_cfg['db']}`.`{t_cn}` against `{u_cfg['db']}`.`{t_cp}` output file 'stage://{stage_name}'")
                finally:
                    up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_cn}`"); up_conn.commit()
                    up_conn.execute(f"DROP TABLE IF EXISTS `{u_cfg['db']}`.`{t_cp}`"); up_conn.commit()
                diff_elapsed += time.time() - diff_start
                
                apply_start = time.time()
                ds_conn.conn.begin()
                try:
                    applied_stmt_count = apply_incremental_diff(up_conn, ds_conn, d_cfg["db"], d_cfg["table"], dfiles)
                    if applied_stmt_count == 0:
                        ds_conn.rollback()
                        sync_noop = True
                    else:
                        ds_conn.execute(f"INSERT INTO `{META_DB}`.`{META_TABLE}` (task_id, watermark) VALUES (%s, %s)", (tid, new_mo_ts)); ds_conn.commit(); sync_success = True
                except Exception as e:
                    ds_conn.rollback(); raise e
                finally:
                    apply_elapsed += time.time() - apply_start
        
        if sync_noop:
            sync_status = "NOOP"
            log.info(f"[yellow]Sync NOOP | {new_mo_ts}[/yellow]")
            cleanup_start = time.time()
            remove_stage_files(up_conn, dfiles)
            record_timing(timings, "cleanup", cleanup_start)
            return sync_return(True)
        if sync_success:
            sync_status = "SUCCESS"
            log.info(f"[green]Sync SUCCESS | {new_mo_ts}[/green]")
            cleanup_start = time.time()
            remove_stage_files(up_conn, dfiles)
            prune_watermarks(ds_conn, tid)
            record_timing(timings, "cleanup", cleanup_start)
            return sync_return(True)
        return sync_return(False)
    except Exception as e:
        log.error(f"Sync FAILED: {e}")
        return sync_return(False)
    finally:
        elapsed = time.time() - start_ts
        log.info(f"Sync duration={diff_elapsed:.3f}/{apply_elapsed:.3f}/{elapsed:.3f}s status={sync_status}")
        if diff_elapsed > 0:
            timings.append(("diff", diff_elapsed))
        if apply_elapsed > 0:
            timings.append(("apply", apply_elapsed))
        top3 = summarize_top_timings(timings)
        if top3:
            log.info(f"Sync timing top3={top3}")
        log.info("-" * 72)
        if keeper.is_alive(): # Review Fix: Only join if started
            keeper.stop(); keeper.join()
        if not lock_held:
            release_lock(ds_conn, tid)
        up_conn.close(); ds_conn.close()

def sync_loop(config, interval):
    inc_sc = 0
    total_sc = 0
    tid = get_task_id(config)
    ds_conn = None
    keeper = None
    last_sync = None
    last_verify = None
    last_error = None
    try:
        while True:
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
                ok, sync_kind, _ = perform_sync(config, is_auto=True, lock_held=True, return_detail=True)
                if ok:
                    total_sc += 1
                    last_sync = time.strftime("%Y-%m-%d %H:%M:%S")
                    if sync_kind == "INCREMENTAL":
                        inc_sc += 1
                    v_int = config.get("verify_interval", 0)
                    force_full = v_int and total_sc % v_int == 0
                    periodic_full = sync_kind == "INCREMENTAL" and inc_sc % 3 == 0
                    if force_full or periodic_full:
                        up, ds = DBConnection(config["upstream"], "Up", autocommit=True), DBConnection(config["downstream"], "Ds", autocommit=True)
                        if up.connect() and ds.connect():
                            ws = get_watermarks(ds, get_task_id(config))
                            if ws:
                                if force_full:
                                    log.info(f"Verify forced by interval={v_int}")
                                elif not is_small_table(up, config, mo_ts=ws[0]):
                                    log.info("Skip verify check due to table is too big.")
                                    up.close(); ds.close()
                                    time.sleep(interval)
                                    continue
                                verify_start = time.time()
                                ok, ur, dr, _, _, u_time, d_time = verify_consistency(up, ds, config, ws[0], mode="full", return_detail=True)
                                table_label = f"{config['upstream']['db']}.{config['upstream']['table']}"
                                log_verify_result(ok, ur, dr, mode="FULL", mo_ts_label=ws[0], table_label=table_label)
                                verify_elapsed = time.time() - verify_start
                                u_dur = u_time or 0.0
                                d_dur = d_time or 0.0
                                log.info(f"Verify duration={u_dur:.3f}/{d_dur:.3f}/{verify_elapsed:.3f}s mode=FULL table={table_label} mo_ts={ws[0]}")
                                if not ok:
                                    log.warning("Consistency check FAILED; please investigate.")
                                last_verify = f"{time.strftime('%Y-%m-%d %H:%M:%S')} (FULL)"
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
            scope = get_sync_scope(config)
            ds_meta = DBConnection(config["downstream"], "DsMeta", autocommit=True)
            if not ds_meta.connect(db_override=""):
                log.error("Downstream connection failed; cannot verify.")
                continue
            tid = get_task_id(config)
            ensure_meta_table(ds_meta)
            raw_snaps = get_watermarks(ds_meta, tid)[:MAX_WATERMARKS]
            ds_meta.close()
            labels = []
            label_map = {}
            for w in raw_snaps:
                utc = format_mo_ts_utc(w) or "Invalid UTC"
                label = f"{w} ({utc})"
                labels.append(label)
                label_map[label] = int(w)
            latest_label = "Latest upstream (no MO_TS)"
            verify_tables = [config["upstream"]["table"]]
            if scope == "database":
                up_list = DBConnection(config["upstream"], "UpList", autocommit=True)
                if not up_list.connect(db_override=""):
                    log.error("Upstream connection failed; cannot list tables.")
                    continue
                tables = list_database_tables(up_list, config["upstream"]["db"])
                up_list.close()
                if not tables:
                    log.error("No tables found; cannot verify.")
                    continue
                while True:
                    table_choice = questionary.select("Verify table:", choices=["Back", "All tables"] + tables).ask()
                    if table_choice is None or table_choice == "Back":
                        break
                    verify_tables = tables if table_choice == "All tables" else [table_choice]
                    choices = ["Back"] + labels + [latest_label]
                    chosen = questionary.select("Verify MO_TS:", choices=choices).ask()
                    if chosen is None or chosen == "Back":
                        continue
                    mo_ts = None
                    mo_ts_label = "LATEST"
                    if chosen != latest_label:
                        if chosen not in label_map:
                            log.warning(f"Invalid MO_TS: {chosen}")
                            continue
                        mo_ts = label_map[chosen]
                        mo_ts_label = str(mo_ts)
                    break
                else:
                    continue
                if table_choice is None or table_choice == "Back":
                    continue
            else:
                choices = ["Back"] + labels + [latest_label]
                chosen = questionary.select("Verify MO_TS:", choices=choices).ask()
                if chosen is None or chosen == "Back":
                    continue
                mo_ts = None
                mo_ts_label = "LATEST"
                if chosen != latest_label:
                    if chosen not in label_map:
                        log.warning(f"Invalid MO_TS: {chosen}")
                        continue
                    mo_ts = label_map[chosen]
                    mo_ts_label = str(mo_ts)
            def do_verify():
                up = DBConnection(config["upstream"], "Up", autocommit=True)
                ds = DBConnection(config["downstream"], "Ds", autocommit=True)
                ok_all = True
                if up.connect() and ds.connect():
                    for t in verify_tables:
                        t_cfg = with_table_config(config, t)
                        v_start = time.time()
                        ok, ur, dr, detail, _, u_time, d_time = verify_consistency(up, ds, t_cfg, mo_ts, mode="full", return_detail=True)
                        log_verify_result(ok, ur, dr, mode="FULL", mo_ts_label=mo_ts_label, table_label=f"{t_cfg['upstream']['db']}.{t}")
                        v_elapsed = time.time() - v_start
                        u_dur = u_time or 0.0
                        d_dur = d_time or 0.0
                        log.info(f"Verify duration={u_dur:.3f}/{d_dur:.3f}/{v_elapsed:.3f}s mode=FULL table={t_cfg['upstream']['db']}.{t} mo_ts={mo_ts_label}")
                        if not ok:
                            ok_all = False
                up.close(); ds.close()
                return ok_all
            ok_all = run_with_activity_indicator("Verify Consistency", do_verify, config, last_sync, last_verify, last_error)
            last_verify = f"{time.strftime('%Y-%m-%d %H:%M:%S')} (FULL)"
            if not ok_all:
                last_error = "Verify failed"
        elif c == "Manual Sync Now":
            mode = questionary.select("Manual Sync Mode:", choices=["Back", "Incremental Sync", "Full Sync"]).ask()
            if mode is None or mode == "Back":
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
            val = questionary.text("Interval (sec) (or 'back'):", default=str(config.get("sync_interval", 60))).ask()
            if val is None:
                continue
            if str(val).strip().lower() in ("back", "b"):
                continue
            try:
                interval = int(val)
            except ValueError:
                log.warning(f"Invalid interval: {val}")
                continue
            config["sync_interval"] = interval; save_config(config); sync_loop(config, interval)

def setup_config(existing=None):
    w = existing if existing else {"upstream": {"host": "127.0.0.1", "port": "6001", "user": "dump", "password": "111", "db": "db1", "table": "t1"}, "downstream": {"host": "127.0.0.1", "port": "6001", "user": "dump", "password": "111", "db": "db2", "table": "t2"}, "stage": {"name": "s1"}, "sync_interval": 60, "verify_interval": 50, "verify_columns": [], "pitr": {}, "sync_scope": "table"}
    if "pitr" not in w:
        w["pitr"] = {}
    if "sync_scope" not in w:
        w["sync_scope"] = "table"
    def text_default(value):
        return "" if value is None else str(value)
    def ask_text(prompt, default, secret=False):
        if secret:
            res = questionary.password(prompt, default=default).ask()
        else:
            res = questionary.text(prompt, default=default).ask()
        return default if res is None else res
    while True:
        verify_cols = w.get("verify_columns", [])
        verify_cols_text = ",".join(verify_cols) if verify_cols else ""
        scope = get_sync_scope(w)
        pitr_cfg = w.get("pitr") or {}
        if pitr_cfg.get("name"):
            length = pitr_cfg.get("length")
            unit = pitr_cfg.get("unit")
            range_label = f"{length}{unit}" if length is not None and unit else ""
            if range_label:
                pitr_label = f"{pitr_cfg.get('name')} ({pitr_cfg.get('level')}, {range_label})"
            else:
                pitr_label = f"{pitr_cfg.get('name')} ({pitr_cfg.get('level')})"
        else:
            pitr_label = "Not set"
        scope_label = w.get("sync_scope", "table")
        up_table = "*" if scope == "database" else w["upstream"]["table"]
        ds_table = "*" if scope == "database" else w["downstream"]["table"]
        table = Table(title="Config"); table.add_row("Scope", scope_label); table.add_row("Up", f"{w['upstream']['db']}.{up_table}"); table.add_row("Ds", f"{w['downstream']['db']}.{ds_table}"); table.add_row("PITR", pitr_label); table.add_row("Verify", str(w.get('verify_interval'))); table.add_row("Fast Columns", verify_cols_text); console.print(table)
        c = questionary.select("Action:", choices=["Edit Sync Scope", "Edit Upstream", "Edit Downstream", "Edit Stage", "Edit PITR", "Edit Verify Interval", "Edit Fast Verify Columns", "Save", "Discard"]).ask()
        if c == "Save": save_config(w); return w
        if c == "Edit Sync Scope":
            val = questionary.select("Sync Scope:", choices=["table", "database"], default=w.get("sync_scope", "table")).ask()
            if val is None:
                continue
            if val != w.get("sync_scope"):
                w["sync_scope"] = val
                w["pitr"] = {}
        if c == "Edit Upstream":
            if scope == "database":
                w["upstream"] = {"host": ask_text("Host:", text_default(w["upstream"].get("host"))), "port": ask_text("Port:", text_default(w["upstream"].get("port"))), "user": ask_text("User:", text_default(w["upstream"].get("user"))), "password": ask_text("Pass:", text_default(w["upstream"].get("password")), secret=True), "db": ask_text("DB:", text_default(w["upstream"].get("db"))), "table": w["upstream"].get("table")}
            else:
                w["upstream"] = {"host": ask_text("Host:", text_default(w["upstream"].get("host"))), "port": ask_text("Port:", text_default(w["upstream"].get("port"))), "user": ask_text("User:", text_default(w["upstream"].get("user"))), "password": ask_text("Pass:", text_default(w["upstream"].get("password")), secret=True), "db": ask_text("DB:", text_default(w["upstream"].get("db"))), "table": ask_text("Table:", text_default(w["upstream"].get("table")))}
        if c == "Edit Downstream":
            if scope == "database":
                w["downstream"] = {"host": ask_text("Host:", text_default(w["downstream"].get("host"))), "port": ask_text("Port:", text_default(w["downstream"].get("port"))), "user": ask_text("User:", text_default(w["downstream"].get("user"))), "password": ask_text("Pass:", text_default(w["downstream"].get("password")), secret=True), "db": ask_text("DB:", text_default(w["downstream"].get("db"))), "table": w["downstream"].get("table")}
            else:
                w["downstream"] = {"host": ask_text("Host:", text_default(w["downstream"].get("host"))), "port": ask_text("Port:", text_default(w["downstream"].get("port"))), "user": ask_text("User:", text_default(w["downstream"].get("user"))), "password": ask_text("Pass:", text_default(w["downstream"].get("password")), secret=True), "db": ask_text("DB:", text_default(w["downstream"].get("db"))), "table": ask_text("Table:", text_default(w["downstream"].get("table")))}
        if c == "Edit Stage": w["stage"] = {"name": ask_text("Stage Name:", text_default(w.get("stage", {}).get("name")))}
        if c == "Edit PITR": w["pitr"] = configure_pitr(w)
        if c == "Edit Verify Interval":
            val = questionary.text("Interval:", default=text_default(w.get("verify_interval", 50))).ask()
            if val is None:
                continue
            try:
                w["verify_interval"] = int(val)
            except ValueError:
                log.warning(f"Invalid verify interval: {val}")
        if c == "Edit Fast Verify Columns":
            val = questionary.text("Columns (comma-separated):", default=verify_cols_text).ask()
            if val is None:
                continue
            cols = [c.strip() for c in val.split(",") if c.strip()]
            w["verify_columns"] = cols
        if c == "Discard" or c is None: return existing

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
