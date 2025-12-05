#!/usr/bin/env bash
set -euo pipefail

HOST="127.0.0.1"
PORT="6001"
USER="dump"
PASS="111"
SESSION_INIT="SET experimental_fulltext_index = 1; SET experimental_ivf_index = 1;"

now_ms() {
  perl -MTime::HiRes -e 'printf("%.0f", Time::HiRes::time()*1000)'
}

format_ms() {
  local ms="$1"
  awk -v ms="$ms" 'BEGIN{printf "%.3f", ms/1000}'
}

print_step() {
  local title="$1"
  local detail="${2:-}"
  echo
  echo "Step: ${title}"
  if [[ -n "$detail" ]]; then
    echo "SQL/operation:"
    printf '%s\n' "$detail" | sed 's/^/  /'
  fi
  echo
}

usage() {
  cat <<'USAGE'
Usage: ./branch_self_diff.sh [options] (-export_dir_path DIR -tbl db.tbl | -apply_file FILE -apply_to_tbl db.tbl)
Defaults: -h 127.0.0.1 -P 6001 -u dump -p 111
Options:
  -h HOST              Override host
  -P PORT              Override port
  -u USER              Override user
  -p PASS              Override password
  -export_dir_path DIR Diff mode output directory (required for diff mode)
  -tbl db.tbl          Source table for diff mode, format db.tbl
  -apply_file FILE     Apply mode input file (.csv or .sql)
  -apply_to_tbl db.tbl Target table for apply mode, format db.tbl
  -help                Show this message

Rules:
  -export_dir_path and -apply_file are mutually exclusive

Examples:
  ./branch_self_diff.sh -export_dir_path '/tmp/output' -tbl test.t0
  ./branch_self_diff.sh -apply_file '/tmp/output/diff_example.csv' -apply_to_tbl test.t1

USAGE
}

err() {
  echo "Error: $*" >&2
  exit 1
}

escape_sql_literal() {
  printf "%s" "$1" | sed "s/'/''/g"
}

validate_db_tbl() {
  local value="$1"
  [[ "$value" =~ ^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$ ]] || err "Table must be in format db.tbl: $value"
}

mysql_exec() {
  local sql="$1"
  MYSQL_PWD="$PASS" mysql --init-command "$SESSION_INIT" -h "$HOST" -P "$PORT" -u "$USER" -N -e "$sql"
}

mysql_exec_db() {
  local db="$1"
  local sql="$2"
  MYSQL_PWD="$PASS" mysql --init-command "$SESSION_INIT" -h "$HOST" -P "$PORT" -u "$USER" -D "$db" -N -e "$sql"
}

run_self_diff() {
  local export_dir_path="$1"
  local src_tbl="$2"
  local step_start=""
  local step_end=""

  validate_db_tbl "$src_tbl"
  [[ -d "$export_dir_path" ]] || mkdir -p "$export_dir_path"
  [[ -d "$export_dir_path" ]] || err "export_dir_path must be an existing directory"

  local db="${src_tbl%%.*}"
  local tbl="${src_tbl#*.}"

  local sp_last
  local step0_sql="select sname as sp_last from mo_catalog.mo_snapshots where database_name = '${db}' and table_name = '${tbl}' order by ts desc limit 1;"
  print_step "step0 get last snapshot" "$step0_sql"
  step_start=$(now_ms)
  sp_last=$(mysql_exec "select sname as sp_last from mo_catalog.mo_snapshots where database_name = '${db}' and table_name = '${tbl}' order by ts desc limit 1;")
  step_end=$(now_ms)
  printf "Step done: step0 get last snapshot (耗时 %s s)\n\n" "$(format_ms $((step_end-step_start)))"
  sp_last=${sp_last//$'\n'/}

  local sp_newest="sp_$(date +"%Y%m%d%H%M%S")$(perl -MTime::HiRes -e 'printf "%.3d", (Time::HiRes::time()*1000)%1000')"
  print_step "step1 create newest snapshot" "create snapshot ${sp_newest} for table ${db} ${tbl};"
  step_start=$(now_ms)
  mysql_exec "create snapshot ${sp_newest} for table ${db} ${tbl};"
  step_end=$(now_ms)
  printf "Step done: step1 create newest snapshot (耗时 %s s)\n\n" "$(format_ms $((step_end-step_start)))"

  local tbl_copy="${tbl}_copy"
  local tbl_copy_prev="${tbl}_copy_prev"
  local tbl_empty="${tbl}_empty"

  cleanup_db="$db"
  cleanup_tbl_copy="$tbl_copy"
  cleanup_tbl_empty="$tbl_empty"
  cleanup_tbl_copy_prev="$tbl_copy_prev"
  cleanup_sp_last="$sp_last"

  cleanup_msgs=()
  cleanup_sqls=()
  cleanup_msgs+=("cleanup drop copy table")
  cleanup_sqls+=("drop table if exists ${cleanup_db}.${cleanup_tbl_copy};")
  cleanup_msgs+=("cleanup drop prev copy table")
  cleanup_sqls+=("drop table if exists ${cleanup_db}.${cleanup_tbl_copy_prev};")
  cleanup_msgs+=("cleanup drop empty table")
  cleanup_sqls+=("drop table if exists ${cleanup_db}.${cleanup_tbl_empty};")
  if [[ -n "${cleanup_sp_last:-}" ]]; then
    cleanup_msgs+=("cleanup drop last snapshot")
    cleanup_sqls+=("drop snapshot if exists ${cleanup_sp_last};")
  fi

  cleanup() {
    set +e
    set +u
    for i in "${!cleanup_msgs[@]}"; do
      print_step "${cleanup_msgs[$i]}" "${cleanup_sqls[$i]}"
      step_start=$(now_ms)
      mysql_exec "${cleanup_sqls[$i]}" >/dev/null 2>&1
      step_end=$(now_ms)
      printf "Step done: %s (耗时 %s s)\n\n" "${cleanup_msgs[$i]}" "$(format_ms $((step_end-step_start)))"
    done
    set -euo pipefail
  }
  trap cleanup EXIT

  print_step "step2 drop old copy tables" "drop table if exists ${db}.${tbl_copy}; drop table if exists ${db}.${tbl_copy_prev};"
  step_start=$(now_ms)
  mysql_exec "drop table if exists ${db}.${tbl_copy};"
  mysql_exec "drop table if exists ${db}.${tbl_copy_prev};"
  step_end=$(now_ms)
  printf "Step done: step2 drop old copy tables (耗时 %s s)\n\n" "$(format_ms $((step_end-step_start)))"

  print_step "step3 clone newest snapshot" "create table ${db}.${tbl_copy} clone ${db}.${tbl}{snapshot = \"${sp_newest}\"};"
  step_start=$(now_ms)
  mysql_exec "create table ${db}.${tbl_copy} clone ${db}.${tbl}{snapshot = \"${sp_newest}\"};"
  step_end=$(now_ms)
  printf "Step done: step3 clone newest snapshot (耗时 %s s)\n\n" "$(format_ms $((step_end-step_start)))"

  if [[ -n "$sp_last" ]]; then
    print_step "step4 clone previous snapshot" "create table ${db}.${tbl_copy_prev} clone ${db}.${tbl}{snapshot = \"${sp_last}\"};"
    step_start=$(now_ms)
    mysql_exec "create table ${db}.${tbl_copy_prev} clone ${db}.${tbl}{snapshot = \"${sp_last}\"};"
    step_end=$(now_ms)
    printf "Step done: step4 clone previous snapshot (耗时 %s s)\n\n" "$(format_ms $((step_end-step_start)))"

    local diff_sql="data branch diff ${db}.${tbl_copy} against ${db}.${tbl_copy_prev} output file '${export_dir_path}';"
    print_step "step5 diff new copy against prev copy" "$diff_sql"
    step_start=$(now_ms)
    mysql_exec "$diff_sql"
    step_end=$(now_ms)
    printf "Step done: step5 diff new copy against prev copy (耗时 %s s)\n\n" "$(format_ms $((step_end-step_start)))"
  else
    print_step "step4 create empty table" "create table ${db}.${tbl_empty} like ${db}.${tbl};"
    step_start=$(now_ms)
    mysql_exec "drop table if exists ${db}.${tbl_empty};"
    mysql_exec "create table ${db}.${tbl_empty} like ${db}.${tbl};"
    step_end=$(now_ms)
    printf "Step done: step4 create empty table (耗时 %s s)\n\n" "$(format_ms $((step_end-step_start)))"
    local diff_sql="data branch diff ${db}.${tbl_copy} against ${db}.${tbl_empty} output file '${export_dir_path}';"
    print_step "step5 diff copy against empty table" "$diff_sql"
    step_start=$(now_ms)
    mysql_exec "$diff_sql"
    step_end=$(now_ms)
    printf "Step done: step5 diff copy against empty table (耗时 %s s)\n\n" "$(format_ms $((step_end-step_start)))"
  fi

  trap - EXIT
  cleanup
}

run_apply() {
  local apply_file="$1"
  local apply_to_tbl="$2"
  local step_start=""
  local step_end=""

  validate_db_tbl "$apply_to_tbl"

  local apply_db="${apply_to_tbl%%.*}"
  local apply_tbl="${apply_to_tbl#*.}"

  case "$apply_file" in
    stage://*.sql|stage://*.SQL)
      local tmp_raw tmp_trim tmp_unesc tmp_sql
      rm -f /tmp/data_branch_stage_raw_*.sql /tmp/data_branch_stage_trim_*.sql /tmp/data_branch_stage_unesc_*.sql /tmp/data_branch_apply_*.sql
      tmp_raw=$(mktemp /tmp/data_branch_stage_raw_XXXXXX.sql)
      tmp_trim=$(mktemp /tmp/data_branch_stage_trim_XXXXXX.sql)
      tmp_unesc=$(mktemp /tmp/data_branch_stage_unesc_XXXXXX.sql)
      tmp_sql=$(mktemp /tmp/data_branch_apply_XXXXXX.sql)
      stage_tmp_files=("$tmp_raw" "$tmp_trim" "$tmp_unesc" "$tmp_sql")
      stage_cleanup() {
        rm -f "${stage_tmp_files[@]}"
      }
      trap stage_cleanup EXIT
      local escaped_stage_path
      escaped_stage_path=$(escape_sql_literal "$apply_file")
      local load_sql="select load_file(cast('${escaped_stage_path}' as datalink));"
      print_step "download stage sql via load_file" "$load_sql > ${tmp_raw}"
      step_start=$(now_ms)
      # use --raw/--batch to avoid escaping newlines as \n
      MYSQL_PWD="$PASS" mysql --init-command "$SESSION_INIT" -h "$HOST" -P "$PORT" -u "$USER" --raw --batch --skip-column-names -e "$load_sql" > "$tmp_raw"
      step_end=$(now_ms)
      printf "Step done: download stage sql via load_file (耗时 %s s)\n\n" "$(format_ms $((step_end-step_start)))"

      print_step "trim load_file output to BEGIN section" "extract from first BEGIN to end"
      step_start=$(now_ms)
      awk 'flag||/BEGIN/{flag=1}flag{print}' "$tmp_raw" > "$tmp_trim"
      step_end=$(now_ms)
      printf "Step done: trim load_file output to BEGIN section (耗时 %s s)\n\n" "$(format_ms $((step_end-step_start)))"

      print_step "normalize whitespace" "ensure real newlines; strip lone \\r"
      step_start=$(now_ms)
      perl -0777 -pe 's/\r\n/\n/g; s/\r/\n/g; s/\\\\r?\\\\n/\n/g; s/\\r?\\n/\n/g;' "$tmp_trim" > "$tmp_unesc"
      step_end=$(now_ms)
      printf "Step done: normalize whitespace (耗时 %s s)\n\n" "$(format_ms $((step_end-step_start)))"

      local detail_stage=$'rewrite delete/replace targets to '"${apply_to_tbl}"$'\nsource file: '"${apply_file}"
      print_step "apply stage sql replace targets" "$detail_stage"
      step_start=$(now_ms)
      perl -pe '
        s/(delete\s+from\s+)(`?\w+`?\.`?\w+`?)/$1'"${apply_to_tbl//\//\\/}"'/ig;
        s/(replace\s+into\s+)(`?\w+`?\.`?\w+`?)/$1'"${apply_to_tbl//\//\\/}"'/ig;
      ' "$tmp_unesc" > "$tmp_sql"
      MYSQL_PWD="$PASS" mysql --init-command "$SESSION_INIT" -h "$HOST" -P "$PORT" -u "$USER" -D "$apply_db" < "$tmp_sql"
      step_end=$(now_ms)
      printf "Step done: apply stage sql replace targets (耗时 %s s)\n\n" "$(format_ms $((step_end-step_start)))"

      rm -f "$tmp_raw" "$tmp_trim" "$tmp_unesc" "$tmp_sql"
      trap - EXIT
      ;;
    *.csv|*.CSV)
      local escaped_file
      escaped_file=$(escape_sql_literal "$apply_file")
      local sql
      sql="load data infile '${escaped_file}' into table ${apply_tbl} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' parallel 'true';"
      print_step "apply csv via load data" "$sql"
      step_start=$(now_ms)
      mysql_exec_db "$apply_db" "$sql"
      step_end=$(now_ms)
      printf "Step done: apply csv via load data (耗时 %s s)\n\n" "$(format_ms $((step_end-step_start)))"
      ;;
    *.sql|*.SQL)
      local tmp_sql
      tmp_sql=$(mktemp /tmp/data_branch_apply_XXXXXX.sql)
      perl -pe '
        s/(delete\s+from\s+)(`?\w+`?\.`?\w+`?)/$1'"${apply_to_tbl//\//\\/}"'/ig;
        s/(replace\s+into\s+)(`?\w+`?\.`?\w+`?)/$1'"${apply_to_tbl//\//\\/}"'/ig;
      ' "$apply_file" > "$tmp_sql"
      local detail=$'rewrite delete/replace targets to '"${apply_to_tbl}"$'\noriginal file: '"${apply_file}"
      print_step "apply sql replace targets" "$detail"
      step_start=$(now_ms)
      MYSQL_PWD="$PASS" mysql --init-command "$SESSION_INIT" -h "$HOST" -P "$PORT" -u "$USER" -D "$apply_db" < "$tmp_sql"
      step_end=$(now_ms)
      printf "Step done: apply sql replace targets (耗时 %s s)\n\n" "$(format_ms $((step_end-step_start)))"
      rm -f "$tmp_sql"
      ;;
    *)
      err "apply_file must be a .csv or .sql file"
      ;;
  esac
}

main() {
  local export_dir_path=""
  local src_tbl=""
  local apply_file=""
  local apply_to_tbl=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      -h) HOST="$2"; shift 2;;
      -P) PORT="$2"; shift 2;;
      -u) USER="$2"; shift 2;;
      -p) PASS="$2"; shift 2;;
      -export_dir_path) export_dir_path="$2"; shift 2;;
      -tbl) src_tbl="$2"; shift 2;;
      -apply_file) apply_file="$2"; shift 2;;
      -apply_to_tbl) apply_to_tbl="$2"; shift 2;;
      -help|--help) usage; exit 0;;
      *) err "Unknown argument: $1";;
    esac
  done

  if [[ -n "$export_dir_path" && -n "$apply_file" ]]; then
    err "-export_dir_path and -apply_file cannot be used together"
  fi

  if [[ -n "$export_dir_path" ]]; then
    [[ -n "$src_tbl" ]] || err "-tbl is required when using -export_dir_path"
    run_self_diff "$export_dir_path" "$src_tbl"
  elif [[ -n "$apply_file" ]]; then
    [[ -n "$apply_to_tbl" ]] || err "-apply_to_tbl is required when using -apply_file"
    run_apply "$apply_file" "$apply_to_tbl"
  else
    usage
    exit 1
  fi
}

main "$@"
