#!/bin/bash
set -euo pipefail

HOST="${HOST:-freetier-01.cn-hangzhou.cluster.cn-qa.matrixone.tech}"
PORT="${PORT:-6001}"
MO_USER="${MO_USER:-019d076b-27d9-732d-915f-5662f32e9d3c:admin:accountadmin}"
MO_PASS="${MO_PASS:-Qiu@0517}"
SRC_ACCOUNT="${SRC_ACCOUNT:-${MO_USER%%:*}}"
USER_REMAINDER="${MO_USER#*:}"
if [ "$USER_REMAINDER" != "$MO_USER" ]; then
  SRC_USER_DEFAULT="${USER_REMAINDER%%:*}"
  ROLE_REMAINDER="${USER_REMAINDER#*:}"
  if [ "$ROLE_REMAINDER" != "$USER_REMAINDER" ]; then
    SRC_ROLE_DEFAULT="$ROLE_REMAINDER"
  else
    SRC_ROLE_DEFAULT=""
  fi
else
  SRC_USER_DEFAULT="$MO_USER"
  SRC_ROLE_DEFAULT=""
fi
SRC_USER="${SRC_USER:-$SRC_USER_DEFAULT}"
SRC_ROLE="${SRC_ROLE:-$SRC_ROLE_DEFAULT}"
OUTDIR="${OUTDIR:-export_sqls}"
BATCH="${BATCH:-2048}"
PARALLEL="${PARALLEL:-6}"
MAX_RETRIES="${MAX_RETRIES:-5}"
DEFAULT_SNAPSHOT="export_sp_$(date +%Y%m%d%H%M%S)_$$"
SNAPSHOT="${SNAPSHOT:-$DEFAULT_SNAPSHOT}"
SKIP_DBS="${SKIP_DBS:-memoria_0326_1 memoria_0328_1 memoria_bak}"
SP="{snapshot = '$SNAPSHOT'}"
SNAPSHOT_CREATED=0

mkdir -p "$OUTDIR"

die() {
  echo "[FATAL] $*" >&2
  exit 1
}

MYSQL_BIN="${MYSQL_BIN:-}"
if [ -z "$MYSQL_BIN" ]; then
  for candidate in \
    /opt/homebrew/opt/mysql@8.4/bin/mysql \
    /opt/homebrew/opt/mysql@8.0/bin/mysql \
    "$(command -v mysql 2>/dev/null || true)"; do
    [ -n "$candidate" ] || continue
    [ -x "$candidate" ] || continue
    MYSQL_BIN="$candidate"
    break
  done
fi
[ -n "$MYSQL_BIN" ] || die "mysql client not found"
[ -x "$MYSQL_BIN" ] || die "mysql client is not executable: $MYSQL_BIN"
echo "Using mysql client: $MYSQL_BIN"

mysql_base() {
  "$MYSQL_BIN" -h "$HOST" -P "$PORT" -u "$MO_USER" "-p$MO_PASS" --batch --raw --skip-column-names "$@"
}

mysql_stream() {
  "$MYSQL_BIN" -h "$HOST" -P "$PORT" -u "$MO_USER" "-p$MO_PASS" --batch --raw --skip-column-names --quick "$@"
}

now_ms() {
  python3 -c "import time; print(int(time.time()*1000))" 2>/dev/null || date +%s000
}

check_source_session() {
  local session_line account_name user_name role_name
  session_line=$(mysql_base -e "SELECT current_account_name(), current_user_name(), current_role_name()") || die "source session probe failed"

  IFS=$'\t' read -r account_name user_name role_name <<EOF
$session_line
EOF

  [ -n "$account_name" ] || die "source session probe returned empty account"
  [ -n "$user_name" ] || die "source session probe returned empty user"
  echo "Source session: account=$account_name user=$user_name role=$role_name"

  if [ "$account_name" != "$SRC_ACCOUNT" ] || [ "$user_name" != "$SRC_USER" ]; then
    die "source session mismatch: expected account '$SRC_ACCOUNT' user '$SRC_USER', got account '$account_name' user '$user_name' role '$role_name'"
  fi

  if [ -n "$SRC_ROLE" ] && [ -n "$role_name" ] && [ "$role_name" != "$SRC_ROLE" ]; then
    echo "[WARN] source role mismatch: expected '$SRC_ROLE', got '$role_name'" >&2
  fi
}

is_skipped_db() {
  local db="$1"
  local skip_db
  for skip_db in $SKIP_DBS; do
    if [ "$db" = "$skip_db" ]; then
      return 0
    fi
  done
  return 1
}

drop_snapshot() {
  [ "$SNAPSHOT_CREATED" -eq 1 ] || return 0
  echo ""
  echo "Dropping snapshot '$SNAPSHOT' ..."
  if ! mysql_base -e "DROP SNAPSHOT IF EXISTS $SNAPSHOT" >/dev/null 2>&1; then
    echo "[WARN] failed to drop snapshot '$SNAPSHOT'" >&2
  fi
  SNAPSHOT_CREATED=0
}

cleanup() {
  local rc=$?
  trap - EXIT
  drop_snapshot
  exit "$rc"
}

trap cleanup EXIT

is_binary_type() {
  case "$1" in
    binary|varbinary|blob|tinyblob|mediumblob|longblob)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

is_text_type() {
  case "$1" in
    char|varchar|text|tinytext|mediumtext|longtext|enum|set)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

is_vector_type() {
  case "$1" in
    vecf*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

serial_extract_type() {
  local dtype="$1"
  local column_type="$2"

  if is_vector_type "$dtype"; then
    echo "$column_type"
    return
  fi

  if [ "$dtype" = "json" ]; then
    echo "JSON"
    return
  fi

  echo "$column_type"
}

is_numeric_type() {
  case "$1" in
    tinyint|smallint|mediumint|int|integer|bigint|float|double|real|decimal|dec|numeric|bool|boolean|bit)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

normalize_data_type() {
  local column_type="$1"
  local dtype
  dtype=$(printf '%s' "$column_type" | tr '[:upper:]' '[:lower:]')
  dtype="${dtype%%(*}"
  dtype="${dtype%% *}"
  printf '%s\n' "$dtype"
}

build_sql_literal_expr() {
  local col="$1"
  local dtype="$2"
  local column_type="$3"
  local qcol="\`$col\`"

  if is_numeric_type "$dtype"; then
    echo "IFNULL(CAST($qcol AS CHAR),'NULL')"
    return
  fi

  if is_vector_type "$dtype" || [ "$dtype" = "json" ]; then
    local extract_type
    extract_type=$(serial_extract_type "$dtype" "$column_type")
    echo "IF($qcol IS NULL,'NULL',CONCAT('serial_extract(UNHEX(''', HEX(serial($qcol)), '''), 0 AS $extract_type)'))"
    return
  fi

  if is_binary_type "$dtype"; then
    echo "IF($qcol IS NULL,'NULL',CONCAT('UNHEX(''', HEX($qcol), ''')'))"
    return
  fi

  if is_text_type "$dtype"; then
    echo "IF($qcol IS NULL,'NULL',CONCAT('CAST(UNHEX(''', HEX($qcol), ''') AS CHAR)'))"
    return
  fi

  echo "IF($qcol IS NULL,'NULL',CONCAT('CAST(UNHEX(''', HEX(CAST($qcol AS CHAR)), ''') AS CHAR)'))"
}

build_checksum_piece() {
  local col="$1"
  local dtype="$2"
  local column_type="$3"
  local qcol="\`$col\`"

  if is_vector_type "$dtype" || [ "$dtype" = "json" ]; then
    echo "IF($qcol IS NULL,'NULL',CONCAT('0x', HEX(serial($qcol))))"
    return
  fi

  if is_binary_type "$dtype" || is_text_type "$dtype"; then
    echo "IF($qcol IS NULL,'NULL',CONCAT('0x', HEX($qcol)))"
    return
  fi

  echo "IF($qcol IS NULL,'NULL',CONCAT('0x', HEX(CAST($qcol AS CHAR))))"
}

build_table_exprs() {
  local meta_file="$1"
  local tbl="$2"
  local col_info
  col_info=$(awk -F'\t' -v tbl="$tbl" '$1 == tbl { print $2 "\t" $3 }' "$meta_file")
  [ -n "$col_info" ] || die "$tbl: no column metadata found"

  local insert_cols=""
  local row_parts=""
  local ck_parts=""
  local col_count=0

  while IFS=$'\t' read -r col column_type; do
    [ -n "$col" ] || continue
    local dtype
    dtype=$(normalize_data_type "$column_type")
    [ -n "$dtype" ] || die "$col: failed to normalize data type from '$column_type'"

    if [ -n "$insert_cols" ]; then
      insert_cols="$insert_cols,\`$col\`"
    else
      insert_cols="\`$col\`"
    fi

    local literal_expr
    literal_expr=$(build_sql_literal_expr "$col" "$dtype" "$column_type")
    if [ -n "$row_parts" ]; then
      row_parts="$row_parts, ',', $literal_expr"
    else
      row_parts="$literal_expr"
    fi

    local ck_piece
    ck_piece=$(build_checksum_piece "$col" "$dtype" "$column_type")
    if [ -n "$ck_parts" ]; then
      ck_parts="$ck_parts,$ck_piece"
    else
      ck_parts="$ck_piece"
    fi

    col_count=$((col_count + 1))
  done <<< "$col_info"

  [ "$col_count" -gt 0 ] || die "$tbl: no columns found"
  printf '%s\n%s\n%s\n' "$insert_cols" "CONCAT('(', $row_parts, ')')" "crc32(concat_ws('|',$ck_parts))"
}

extract_value() {
  local file="$1"
  local tbl="$2"
  awk -F'\t' -v tbl="$tbl" '$1 == tbl { print $2; exit }' "$file"
}

extract_rest_fields() {
  local file="$1"
  local key="$2"
  awk -F'\t' -v key="$key" '
    $1 == key {
      found = 1
      sub(/^[^\t]*\t/, "", $0)
      print
      next
    }
    found && NF > 1 && $2 ~ /^CREATE TABLE / { exit }
    found { print }
  ' "$file"
}

collect_table_counts() {
  local db="$1"
  local tables="$2"
  local out_file="$3"
  local cnt_sql="" tbl err_file

  err_file=$(mktemp)

  while IFS= read -r tbl; do
    [ -n "$tbl" ] || continue
    cnt_sql="${cnt_sql}SELECT '$tbl', COUNT(*) FROM \`$db\`.\`$tbl\`${SP};"
  done <<< "$tables"

  if mysql_base -e "$cnt_sql" > "$out_file" 2>"$err_file"; then
    rm -f "$err_file"
    return 0
  fi

  echo "[WARN] $db: batch count query failed, falling back to per-table counts" >&2
  sed -n '1,5p' "$err_file" >&2
  : > "$out_file"

  while IFS= read -r tbl; do
    [ -n "$tbl" ] || continue
    if ! mysql_base -e "SELECT '$tbl', COUNT(*) FROM \`$db\`.\`$tbl\`${SP}" >> "$out_file" 2>"$err_file"; then
      echo "[WARN] $db.$tbl: count query failed" >&2
      sed -n '1,5p' "$err_file" >&2
      rm -f "$err_file"
      return 1
    fi
  done <<< "$tables"

  rm -f "$err_file"
}

generate_support_scripts() {
  cat > "$OUTDIR/import_mem.sh" <<'IMPORT_EOF'
#!/bin/bash
set -euo pipefail

HOST="${HOST:-freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech}"
PORT="${PORT:-6001}"
MO_USER="${MO_USER:-01998024-f682-7187-ab53-4d54b904e4fb:admin:accountadmin}"
MO_PASS="${MO_PASS:-Admin123}"
SQLDIR="$(cd "$(dirname "$0")" && pwd)"
PARALLEL="${PARALLEL:-6}"
MAX_RETRIES="${MAX_RETRIES:-5}"
MYSQL_BIN="${MYSQL_BIN:-}"

if [ -z "$MYSQL_BIN" ]; then
  for candidate in \
    /opt/homebrew/opt/mysql@8.4/bin/mysql \
    /opt/homebrew/opt/mysql@8.0/bin/mysql \
    "$(command -v mysql 2>/dev/null || true)"; do
    [ -n "$candidate" ] || continue
    [ -x "$candidate" ] || continue
    MYSQL_BIN="$candidate"
    break
  done
fi

[ -n "$MYSQL_BIN" ] || { echo "[FATAL] mysql client not found"; exit 1; }
[ -x "$MYSQL_BIN" ] || { echo "[FATAL] mysql client is not executable: $MYSQL_BIN"; exit 1; }

mysql_target() {
  "$MYSQL_BIN" -h "$HOST" -P "$PORT" -u "$MO_USER" "-p$MO_PASS" --batch --raw --skip-column-names
}

cleanup_failed_db() {
  local db="$1"
  mysql_target -e "DROP DATABASE IF EXISTS \`$db\`" >/dev/null 2>&1 || true
}

import_one() {
  local f="$1"
  local base db logfile expected_ok ok_count rc

  base=$(basename "$f")
  db="${base%.sql}"
  logfile=$(mktemp -t "import_${db}")
  expected_ok=$(awk '/^-- EXPECTED_OK / { print $3; exit }' "$f")

  if ! [[ "$expected_ok" =~ ^[0-9]+$ ]]; then
    echo "[FAIL] $db: missing -- EXPECTED_OK metadata"
    return 1
  fi

  rc=0
  mysql_target < "$f" > "$logfile" 2>&1 || rc=$?
  if [ "$rc" -ne 0 ]; then
    cleanup_failed_db "$db"
    echo "[FAIL] $db: exit code $rc (partial import dropped; see $logfile)"
    return 1
  fi

  if grep -qi "^ERROR" "$logfile"; then
    cleanup_failed_db "$db"
    echo "[FAIL] $db: SQL errors (partial import dropped; see $logfile)"
    grep -i "^ERROR" "$logfile" | head -3
    return 1
  fi

  if grep -qi "^MISMATCH " "$logfile"; then
    cleanup_failed_db "$db"
    echo "[FAIL] $db: verification mismatch (partial import dropped; see $logfile)"
    grep -i "^MISMATCH " "$logfile"
    return 1
  fi

  ok_count=$(grep -c "^OK " "$logfile" || true)
  if [ "$ok_count" -ne "$expected_ok" ]; then
    cleanup_failed_db "$db"
    echo "[FAIL] $db: expected $expected_ok OK lines, got $ok_count (partial import dropped; see $logfile)"
    return 1
  fi

  rm -f "$logfile"
  echo "[OK]   $db ($ok_count tables verified)"
}

export -f mysql_target cleanup_failed_db import_one
export HOST PORT MO_USER MO_PASS MYSQL_BIN

set -- "$SQLDIR"/*.sql
if [ ! -e "$1" ]; then
  echo "[FATAL] no .sql files found in $SQLDIR"
  exit 1
fi

remaining=$(printf '%s\n' "$@")
attempt=1

while [ -n "$remaining" ] && [ "$attempt" -le "$MAX_RETRIES" ]; do
  if [ "$attempt" -gt 1 ]; then
    retry_cnt=$(printf '%s\n' "$remaining" | awk 'NF { c++ } END { print c + 0 }')
    echo ""
    echo "=== Retry $attempt/$MAX_RETRIES: $retry_cnt files remaining ==="
    sleep 2
  fi

  FAIL_FILE=$(mktemp)
  printf '%s\n' "$remaining" | xargs -P"$PARALLEL" -I{} bash -c '
    set +e
    import_one "$1"
    rc=$?
    if [ $rc -ne 0 ]; then
      printf "%s\n" "$1" >> "$2"
    fi
  ' _ "{}" "$FAIL_FILE"

  if [ -s "$FAIL_FILE" ]; then
    remaining=$(cat "$FAIL_FILE")
  else
    remaining=""
  fi
  rm -f "$FAIL_FILE"

  attempt=$((attempt + 1))
done

if [ -n "$remaining" ]; then
  echo ""
  echo "========== FAILURES =========="
  echo "$remaining"
  echo "Total: $(printf '%s\n' "$remaining" | awk 'NF { c++ } END { print c + 0 }') failed"
  exit 1
fi

echo ""
echo "========== ALL OK =========="
IMPORT_EOF

  cat > "$OUTDIR/drop_all.sh" <<'DROP_EOF'
#!/bin/bash
set -euo pipefail

HOST="${HOST:-freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech}"
PORT="${PORT:-6001}"
MO_USER="${MO_USER:-01998024-f682-7187-ab53-4d54b904e4fb:admin:accountadmin}"
MO_PASS="${MO_PASS:-Admin123}"
SQLDIR="$(cd "$(dirname "$0")" && pwd)"
PARALLEL="${PARALLEL:-6}"
MYSQL_BIN="${MYSQL_BIN:-}"

if [ -z "$MYSQL_BIN" ]; then
  for candidate in \
    /opt/homebrew/opt/mysql@8.4/bin/mysql \
    /opt/homebrew/opt/mysql@8.0/bin/mysql \
    "$(command -v mysql 2>/dev/null || true)"; do
    [ -n "$candidate" ] || continue
    [ -x "$candidate" ] || continue
    MYSQL_BIN="$candidate"
    break
  done
fi

[ -n "$MYSQL_BIN" ] || { echo "[FATAL] mysql client not found"; exit 1; }
[ -x "$MYSQL_BIN" ] || { echo "[FATAL] mysql client is not executable: $MYSQL_BIN"; exit 1; }

mysql_target() {
  "$MYSQL_BIN" -h "$HOST" -P "$PORT" -u "$MO_USER" "-p$MO_PASS" --batch --raw --skip-column-names "$@"
}

drop_one() {
  local db
  db=$(basename "$1" .sql)
  mysql_target -e "DROP DATABASE IF EXISTS \`$db\`" >/dev/null 2>&1 && echo "[dropped] $db" || echo "[FAIL]    $db"
}

export -f mysql_target drop_one
export HOST PORT MO_USER MO_PASS MYSQL_BIN

set -- "$SQLDIR"/*.sql
if [ ! -e "$1" ]; then
  echo "[FATAL] no .sql files found in $SQLDIR"
  exit 1
fi

printf '%s\0' "$@" | xargs -0 -P"$PARALLEL" -I{} bash -c 'drop_one "$1"' _ "{}"
DROP_EOF

  chmod +x "$OUTDIR/import_mem.sh" "$OUTDIR/drop_all.sh"
  echo "Generated $OUTDIR/import_mem.sh and $OUTDIR/drop_all.sh"
}

export_db() (
  local db="$1"
  local outfile="$OUTDIR/${db}.sql"
  local verify_file cnt_file meta_file ddl_file row_cnt_file
  local tables ddl_sql table_csv ddl cnt
  local exprs insert_cols row_literal_expr row_ck_expr
  local metrics_line expect_cnt expect_xor expect_sum expect_min expect_max
  local tbl_exported
  local total_exported=0 total_origin=0
  local table_count=0
  local db_start t_meta db_ms meta_ms

  verify_file=$(mktemp)
  cnt_file=$(mktemp)
  meta_file=$(mktemp)
  ddl_file=$(mktemp)
  row_cnt_file=""
  trap 'rm -f "$verify_file" "$cnt_file" "$meta_file" "$ddl_file" ${row_cnt_file:+"$row_cnt_file"}' EXIT

  db_start=$(now_ms)

  echo "-- Export of $db (snapshot: $SNAPSHOT)" > "$outfile"
  echo "DROP DATABASE IF EXISTS \`$db\`;" >> "$outfile"
  echo "CREATE DATABASE \`$db\`;" >> "$outfile"
  echo "USE \`$db\`;" >> "$outfile"
  echo "" >> "$outfile"

  tables=$(mysql_base -e "SHOW TABLES FROM \`$db\`" 2>/dev/null | awk 'NF && $0 !~ /^__mo_/') || die "$db: SHOW TABLES failed"
  [ -n "$tables" ] || die "$db: no tables found"

  table_count=$(printf '%s\n' "$tables" | awk 'NF { c++ } END { print c + 0 }')
  echo "-- EXPECTED_OK $table_count" >> "$outfile"
  echo "" >> "$outfile"

  ddl_sql=""
  table_csv=""
  while IFS= read -r tbl; do
    [ -n "$tbl" ] || continue
    ddl_sql="${ddl_sql}SHOW CREATE TABLE \`$db\`.\`$tbl\`;"
    if [ -n "$table_csv" ]; then
      table_csv="${table_csv},'$tbl'"
    else
      table_csv="'$tbl'"
    fi
  done <<< "$tables"

  collect_table_counts "$db" "$tables" "$cnt_file" || die "$db: count query failed"
  mysql_base -e "$ddl_sql" > "$ddl_file" 2>/dev/null || die "$db: batch SHOW CREATE TABLE failed"
  mysql_base -e "
    SELECT table_name, column_name, column_type
    FROM information_schema.columns
    WHERE table_schema='$db'
      AND table_name IN ($table_csv)
      AND LEFT(table_name, 5) <> '__mo_'
      AND LEFT(column_name, 5) <> '__mo_'
    ORDER BY table_name, ordinal_position
  " > "$meta_file" 2>/dev/null || die "$db: column metadata query failed"
  t_meta=$(now_ms)

  while IFS= read -r tbl; do
    [ -n "$tbl" ] || continue

    ddl=$(extract_rest_fields "$ddl_file" "$tbl")
    [ -n "$ddl" ] && [ "$ddl" != "NULL" ] || die "$db.$tbl: DDL is empty"
    echo "$ddl" | sed 's/\\n/\n/g' >> "$outfile"
    echo ";" >> "$outfile"
    echo "" >> "$outfile"

    cnt=$(extract_value "$cnt_file" "$tbl")
    [[ "$cnt" =~ ^[0-9]+$ ]] || die "$db.$tbl: count not integer: '$cnt'"

    exprs=$(build_table_exprs "$meta_file" "$tbl") || die "$db.$tbl: failed to build expressions"
    insert_cols=$(printf '%s\n' "$exprs" | sed -n '1p')
    row_literal_expr=$(printf '%s\n' "$exprs" | sed -n '2p')
    row_ck_expr=$(printf '%s\n' "$exprs" | sed -n '3p')
    [ -n "$insert_cols" ] || die "$db.$tbl: insert column list is empty"
    [ -n "$row_literal_expr" ] || die "$db.$tbl: row literal expression is empty"
    [ -n "$row_ck_expr" ] || die "$db.$tbl: checksum expression is empty"

    if [ "$cnt" -eq 0 ]; then
      cat >> "$verify_file" <<EOSQL
SELECT CASE
  WHEN (SELECT COUNT(*) FROM \`$db\`.\`$tbl\`) = 0 THEN 'OK $tbl'
  ELSE CONCAT('MISMATCH $tbl count: expect 0 got ', (SELECT COUNT(*) FROM \`$db\`.\`$tbl\`))
END AS verify_result;
EOSQL
      continue
    fi

    metrics_line=$(mysql_base -e "
      SELECT
        COUNT(*) AS cnt,
        COALESCE(CAST(bit_xor(row_ck) AS UNSIGNED),0) AS fp_xor,
        COALESCE(SUM(CAST(row_ck AS UNSIGNED)),0) AS fp_sum,
        COALESCE(MIN(CAST(row_ck AS UNSIGNED)),0) AS fp_min,
        COALESCE(MAX(CAST(row_ck AS UNSIGNED)),0) AS fp_max
      FROM (
        SELECT $row_ck_expr AS row_ck
        FROM \`$db\`.\`$tbl\`${SP}
      ) t
    " 2>/dev/null) || die "$db.$tbl: fingerprint query failed"

    IFS=$'\t' read -r expect_cnt expect_xor expect_sum expect_min expect_max <<EOF
$metrics_line
EOF

    for value in "$expect_cnt" "$expect_xor" "$expect_sum" "$expect_min" "$expect_max"; do
      [[ "$value" =~ ^[0-9]+$ ]] || die "$db.$tbl: fingerprint value not integer: '$value'"
    done

    [ "$expect_cnt" -eq "$cnt" ] || die "$db.$tbl: count mismatch between metadata ($cnt) and fingerprint query ($expect_cnt)"

    total_origin=$((total_origin + cnt))

    row_cnt_file=$(mktemp)
    mysql_stream -e "SELECT $row_literal_expr FROM \`$db\`.\`$tbl\`${SP}" 2>/dev/null | \
      awk -v tbl="$tbl" -v db="$db" -v cols="$insert_cols" -v batch="$BATCH" -v cntfile="$row_cnt_file" '
        BEGIN { count = 0; inbatch = 0 }
        {
          if (inbatch == 0) {
            printf "INSERT INTO `%s`.`%s` (%s) VALUES\n", db, tbl, cols
          } else {
            printf ",\n"
          }
          printf "%s", $0
          inbatch++
          count++
          if (inbatch >= batch) {
            print ";"
            inbatch = 0
          }
        }
        END {
          if (inbatch > 0) {
            print ";"
          }
          print count > cntfile
        }
      ' >> "$outfile" || die "$db.$tbl: data dump failed"

    tbl_exported=$(cat "$row_cnt_file")
    rm -f "$row_cnt_file"
    row_cnt_file=""
    [[ "$tbl_exported" =~ ^[0-9]+$ ]] || die "$db.$tbl: exported count not integer: '$tbl_exported'"
    [ "$tbl_exported" -eq "$cnt" ] || die "$db.$tbl: exported $tbl_exported rows but source snapshot has $cnt"

    total_exported=$((total_exported + tbl_exported))
    echo "" >> "$outfile"

    cat >> "$verify_file" <<EOSQL
SELECT CASE
  WHEN cnt != $expect_cnt THEN CONCAT('MISMATCH $tbl count: expect $expect_cnt got ', cnt)
  WHEN fp_xor != $expect_xor THEN CONCAT('MISMATCH $tbl fp_xor: expect $expect_xor got ', fp_xor)
  WHEN fp_sum != $expect_sum THEN CONCAT('MISMATCH $tbl fp_sum: expect $expect_sum got ', fp_sum)
  WHEN fp_min != $expect_min THEN CONCAT('MISMATCH $tbl fp_min: expect $expect_min got ', fp_min)
  WHEN fp_max != $expect_max THEN CONCAT('MISMATCH $tbl fp_max: expect $expect_max got ', fp_max)
  ELSE 'OK $tbl'
END AS verify_result
FROM (
  SELECT
    COUNT(*) AS cnt,
    COALESCE(CAST(bit_xor(row_ck) AS UNSIGNED),0) AS fp_xor,
    COALESCE(SUM(CAST(row_ck AS UNSIGNED)),0) AS fp_sum,
    COALESCE(MIN(CAST(row_ck AS UNSIGNED)),0) AS fp_min,
    COALESCE(MAX(CAST(row_ck AS UNSIGNED)),0) AS fp_max
  FROM (
    SELECT $row_ck_expr AS row_ck
    FROM \`$db\`.\`$tbl\`
  ) t
) verify_t;
EOSQL
  done <<< "$tables"

  echo "" >> "$outfile"
  echo "-- ========== VERIFICATION ==========" >> "$outfile"
  cat "$verify_file" >> "$outfile"

  [ "$total_exported" -eq "$total_origin" ] || die "$db: total exported $total_exported != source snapshot $total_origin"

  db_ms=$(( $(now_ms) - db_start ))
  meta_ms=$(( t_meta - db_start ))
  echo "[done] $db ($total_exported rows, meta:${meta_ms}ms total:${db_ms}ms) -> $outfile"
)

generate_support_scripts

check_source_session
echo "Creating snapshot '$SNAPSHOT' ..."
mysql_base -e "DROP SNAPSHOT IF EXISTS $SNAPSHOT" >/dev/null 2>&1 || true
mysql_base -e "CREATE SNAPSHOT $SNAPSHOT FOR ACCOUNT" || die "cannot create snapshot '$SNAPSHOT'"
SNAPSHOT_CREATED=1
echo "Snapshot '$SNAPSHOT' created."

export -f mysql_base mysql_stream die now_ms
export -f is_skipped_db is_binary_type is_text_type is_vector_type serial_extract_type is_numeric_type normalize_data_type
export -f build_sql_literal_expr build_checksum_piece build_table_exprs
export -f extract_value extract_rest_fields collect_table_counts export_db
export HOST PORT MO_USER MO_PASS MYSQL_BIN SRC_ACCOUNT SRC_USER SRC_ROLE OUTDIR BATCH SNAPSHOT SKIP_DBS SP

echo "Starting export at $(date)"

all_dbs=$(
  {
    mysql_base -e "SHOW DATABASES LIKE 'mem\_u\_%'"
    mysql_base -e "SHOW DATABASES LIKE 'memoria'"
    mysql_base -e "SHOW DATABASES LIKE 'memoria\_%'"
  } 2>/dev/null | awk 'NF' | sort -u
) || die "failed to list databases"

echo "Skipping databases: $SKIP_DBS"
filtered_dbs=""
while IFS= read -r db; do
  [ -n "$db" ] || continue
  if is_skipped_db "$db"; then
    rm -f "$OUTDIR/${db}.sql"
    continue
  fi
  if [ -n "$filtered_dbs" ]; then
    filtered_dbs="${filtered_dbs}"$'\n'"${db}"
  else
    filtered_dbs="$db"
  fi
done <<< "$all_dbs"
all_dbs="$filtered_dbs"

[ -n "$all_dbs" ] || die "no matching databases found"

total=$(printf '%s\n' "$all_dbs" | awk 'NF { c++ } END { print c + 0 }')
echo "Found $total databases to export"

attempt=1
remaining="$all_dbs"

while [ -n "$remaining" ] && [ "$attempt" -le "$MAX_RETRIES" ]; do
  if [ "$attempt" -gt 1 ]; then
    failed_cnt=$(printf '%s\n' "$remaining" | awk 'NF { c++ } END { print c + 0 }')
    echo ""
    echo "=== Retry $attempt/$MAX_RETRIES: $failed_cnt databases remaining ==="
    sleep 2
  fi

  failed_file=$(mktemp)
  printf '%s\n' "$remaining" | xargs -P"$PARALLEL" -I{} bash -c '
    set +e
    export_db "$1"
    rc=$?
    if [ $rc -ne 0 ]; then
      printf "%s\n" "$1" >> "$2"
    fi
  ' _ "{}" "$failed_file"

  if [ -s "$failed_file" ]; then
    remaining=$(cat "$failed_file")
  else
    remaining=""
  fi
  rm -f "$failed_file"

  attempt=$((attempt + 1))
done

echo ""
drop_snapshot

if [ -n "$remaining" ]; then
  echo "========== EXPORT FAILED =========="
  echo "The following databases failed after $MAX_RETRIES attempts:"
  echo "$remaining"
  exit 1
fi

done_cnt=$(find "$OUTDIR" -maxdepth 1 -name '*.sql' | wc -l | tr -d ' ')
echo "========== EXPORT COMPLETE =========="
echo "Exported $done_cnt databases to $OUTDIR/"
echo "Finished at $(date)"
