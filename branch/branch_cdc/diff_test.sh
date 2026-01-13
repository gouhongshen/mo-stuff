#!/usr/bin/env bash
set -euo pipefail

HOST="127.0.0.1"
PORT="6001"
USER_NAME="dump"
PASSWORD="111"
DB_NAME="fake_data_db"
SESSION_INIT="SET experimental_fulltext_index = 1; SET experimental_ivf_index = 1;"
BRANCH_SELF_DIFF="./branch_self_diff.sh"
TABLE_NAME="fake_table"
TABLE_COPY="fake_table_t1"
DDL_FILE="fake_data_ddl.sql"

usage() {
  echo "Usage: $0 [-h host] [-P port] [-u user] [-p password]" >&2
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h) HOST="$2"; shift 2;;
    -P) PORT="$2"; shift 2;;
    -u) USER_NAME="$2"; shift 2;;
    -p) PASSWORD="$2"; shift 2;;
    --) shift; break;;
    -*)
      echo "Invalid option: $1" >&2
      usage
      exit 1
      ;;
    *)
      # ignore unexpected positional args
      shift
      ;;
  esac
done

LOOP_TIMES=10
DELETE_BATCH=10000
INSERT_BATCH=1000   # per insert statement; repeated to reach 1W
INSERT_STMTS=10     # 10 * 1000 = 1W
UPDATE_BATCH=10000

log() {
  echo "[$(date '+%F %T')] $*"
}

MYSQL_CMD=(mysql --init-command "$SESSION_INIT" -h "$HOST" -P "$PORT" -u "$USER_NAME" "-p$PASSWORD" "$DB_NAME")

run_sql() {
  local sql="$1"
  log "SQL -> $sql"
  "${MYSQL_CMD[@]}" --batch --skip-column-names -e "$sql"
}

run_sql_no_log() {
  local sql="$1"
  "${MYSQL_CMD[@]}" --batch --skip-column-names -e "$sql"
}

log "检测: 检查 ${DB_NAME}.${TABLE_NAME} 是否存在且非空"
t0_exists=$("${MYSQL_CMD[@]}" --batch --skip-column-names -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${DB_NAME}' AND table_name='${TABLE_NAME}';")
need_init=1
if [ "${t0_exists:-0}" -gt 0 ]; then
  t0_non_empty=$("${MYSQL_CMD[@]}" --batch --skip-column-names -e "SELECT CASE WHEN EXISTS (SELECT 1 FROM ${TABLE_NAME} LIMIT 1) THEN 1 ELSE 0 END;")
  if [ "$t0_non_empty" -eq 1 ]; then
    need_init=0
    log "检测: ${TABLE_NAME} 已存在且非空，跳过 ${DDL_FILE}"
  else
    log "检测: ${TABLE_NAME} 已存在但为空，将执行 ${DDL_FILE}"
  fi
else
  log "检测: ${TABLE_NAME} 不存在，将执行 ${DDL_FILE}"
fi

if [ "$need_init" -eq 1 ]; then
  log "准备: 连接数据库并执行 ${DDL_FILE}"
  "${MYSQL_CMD[@]}" < "${DDL_FILE}"
else
  log "准备: 跳过执行 ${DDL_FILE}"
fi

if [ ! -x "$BRANCH_SELF_DIFF" ]; then
  log "错误: 无法找到可执行文件 $BRANCH_SELF_DIFF"
  exit 1
fi

if [ "$need_init" -eq 1 ]; then
  log "准备: 重建 ${TABLE_COPY}（因 ${TABLE_NAME} 执行了 ddl）"
  run_sql "DROP TABLE IF EXISTS ${TABLE_COPY};"
  run_sql "CREATE TABLE ${TABLE_COPY} LIKE ${TABLE_NAME};"
else
  log "准备: 跳过 ${TABLE_COPY} 重建（${TABLE_NAME} 未执行 ddl）"
fi

for i in $(seq 1 "$LOOP_TIMES"); do
  op=$(python3 - <<'PY'
import random
print(random.choice(["delete", "insert", "update"]))
PY
  )
  case "$op" in
    delete)
      log "轮次 ${i}/${LOOP_TIMES}: 随机选择 delete，删除 ${DELETE_BATCH} 行"
      run_sql "DELETE FROM ${TABLE_NAME} ORDER BY RAND() LIMIT ${DELETE_BATCH};"
      ;;
    insert)
      log "轮次 ${i}/${LOOP_TIMES}: 随机选择 insert，总计插入 10000 行（${INSERT_STMTS} 次，每次 ${INSERT_BATCH} 行）"
      for j in $(seq 1 "$INSERT_STMTS"); do
        values_list=$(
          python3 - <<PY
import uuid
rows = [f"('{uuid.uuid4()}', '{uuid.uuid4().hex[:16]}')" for _ in range(${INSERT_BATCH})]
print(", ".join(rows))
PY
        )
        run_sql "INSERT INTO ${TABLE_NAME} (md5_id, allow_access) VALUES ${values_list};"
      done
      ;;
    update)
      log "轮次 ${i}/${LOOP_TIMES}: 随机选择 update，更新 ${UPDATE_BATCH} 行"
      run_sql "UPDATE ${TABLE_NAME} AS t JOIN ( SELECT md5_id FROM ${TABLE_NAME} ORDER BY RAND() LIMIT ${UPDATE_BATCH}) AS r USING (md5_id) SET t.allow_access = SUBSTRING(MD5(RAND()), 1, 16);"
      ;;
  esac
  sleep 1

  log "执行 branch_self_diff 导出"
  tmp_out=$(mktemp)
  if ! "$BRANCH_SELF_DIFF" -h "$HOST" -P "$PORT" -u "$USER_NAME" -p "$PASSWORD" -export_dir_path '/tmp/branch_diff_test/' -tbl "${DB_NAME}.${TABLE_NAME}" | tee "$tmp_out"; then
    log "branch_self_diff 导出失败"
    rm -f "$tmp_out"
    exit 1
  fi

  x_path=$(perl -ne '
    if(/file\s+saved\s+to\s*[:：]?\s*([^\s]+)/i){print $1; exit}
    if(m{(^|[\s"'"'"'])(/[^ \t"'"'"']+\.(?:csv|sql))(?:[\s"'"'"']|$)}){print $2; exit}
  ' "$tmp_out")
  rm -f "$tmp_out"

  if [ -z "$x_path" ] || [ ! -f "$x_path" ]; then
    log "未获取到导出文件路径"
    exit 1
  fi
  log "导出文件: $x_path"

  log "执行 branch_self_diff 应用到 ${TABLE_COPY}"
  "$BRANCH_SELF_DIFF" -h "$HOST" -P "$PORT" -u "$USER_NAME" -p "$PASSWORD" -apply_file "$x_path" -apply_to_tbl "${DB_NAME}.${TABLE_COPY}"

  log "校验 ${TABLE_NAME} 与 ${TABLE_COPY} 一致性（EXCEPT 对比，返回一条差异）(sleep 5)"
  sleep 5
  diff_row=$(run_sql_no_log "$(cat <<SQL
SELECT * FROM (
  (SELECT md5_id, allow_access FROM ${TABLE_NAME})
  EXCEPT
  (SELECT md5_id, allow_access FROM ${TABLE_COPY})
  UNION ALL
  (SELECT md5_id, allow_access FROM ${TABLE_COPY})
  EXCEPT
  (SELECT md5_id, allow_access FROM ${TABLE_NAME})
) AS diff_total
LIMIT 1;
SQL
)")
  if [ -n "$diff_row" ]; then
    log "校验失败: 差异行 -> $diff_row"
    exit 1
  fi
  log "校验通过"

  log "清理导出文件"
  rm -f "$x_path"

  log "等待 10 秒进入下一轮"
  sleep 10
done

log "循环已完成 ${LOOP_TIMES} 轮"
