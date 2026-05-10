#!/usr/bin/env bash
# pipeline_reload.sh — 测试「导入 → artifact push/pull → yikv_server reload」全流程（本机一体化，无 HTTP）。
#
# 编排调度请用 HTTP agent：tools/pipeline_agent/pipeline_agent.py + tools/schedule_pipeline.py
# （构建机 / 在线机各跑 agent，调度只发 /publishIndex、/deployIndex；同一台机可共用同一 URL）。
#
# 依赖：已构建 yikv_import_pipeline；Python3 + PyYAML；本地 artifact 时 artifact_sync 无需 rclone。
#
# 用法（在 yikv-server 仓库根目录执行，或与 YIKV_ROOT 指向该根）:
#   export PART1=/path/to/part-....parquet
#   export PART2=/path/to/other.parquet    # 仅 phase2 / all 需要
#   export SCHEMA_JSON=./schema.json        # 可选，默认仓库根 schema.json
#   export TABLE=dsp_test5                    # 可选
#
#   ./tools/pipeline_reload.sh phase1        # 新表：首文件 + create_if_missing
#   ./tools/pipeline_reload.sh phase2        # 旧表：在同一索引目录上追加第二文件后重新发布
#   ./tools/pipeline_reload.sh all           # 连续执行 phase1 + phase2
#
#   ./tools/pipeline_reload.sh reload-only   # 仅发 reload；若无 admin socket 且 AUTO_START_SERVER=1（默认），用 $WORK/config.server.json 起服务
#
# 可选环境变量：
#   YIKV_ROOT       默认：本脚本所在目录的上一级（yikv-server）
#   WORK            默认 /data/yikvdb（统一根目录；需写权限；可 export 覆盖）
#   BUILD_DB        导入用 db_path，默认 $WORK/build_db
#   SERVER_DB       在线 yikv_server 的 db_path，默认 $WORK/server_db
#   ARTIFACT_STORE  默认 $WORK/artifact_store（= artifact local.root）
#   ARTIFACT_KEY_PREFIX  默认 yikv-index，见下方「key_prefix」说明
#   ADMIN_SOCKET    默认 $WORK/admin.sock
#   SERVER_CONFIG   默认 $WORK/config.server.json（link_server_table 写入；reload / 自动起服务用）
#   YIKV_SERVER_BIN 默认 $YIKV_ROOT/bazel-bin/yikv_server
#   AUTO_START_SERVER  默认 1；设 0 则要求 admin socket 已存在（不自动启动 yikv_server）
#   ARTIFACT_YAML   默认 $WORK/artifact-storage.yaml（脚本可生成 local provider）
#
# key_prefix（artifact-storage.yaml）:
#   与 env、表名、build_id 拼成品制路径：{local.root}/{env}/{key_prefix}/{table}/{build_id}/
#   用于在同一制品根下区分业务线/项目（多 bucket 共用时也可靠前缀隔离），不改变 yikv 表名。

# 目录分布（默认 WORK=/data/yikvdb）:
#
#   artifact_store/          local.root；制品树 = {env}/{key_prefix}/{table}/{build_id}/
#
#   build_db/                yikv_import_pipeline 的 db_path（构建工作台）
#     └── $TABLE/
#
#   releases/$TABLE/         pull --dest；含多个 build_id/ 与 active -> 当前 build
#
#   server_db/               yikv_server db_path；$TABLE -> releases/$TABLE/active
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YIKV_ROOT="$(cd "${YIKV_ROOT:-$SCRIPT_DIR/..}" && pwd)"
cd "$YIKV_ROOT"

TABLE="${TABLE:-dsp_test5}"
SCHEMA_JSON="${SCHEMA_JSON:-$YIKV_ROOT/schema.json}"
SKIP_RELOAD="${SKIP_RELOAD:-0}"

CMD="${1:-}"

die() { echo "pipeline_reload: ERROR: $*" >&2; exit 1; }

require_file() { [[ -f "$1" ]] || die "missing file: $1"; }

WORK="${WORK:-/data/yikvdb}"
mkdir -p "$WORK"
echo "pipeline_reload: WORK=$WORK"

BUILD_DB="${BUILD_DB:-$WORK/build_db}"
DB_DIR="$BUILD_DB"
RELEASE_ROOT="$WORK/releases/$TABLE"
ARTIFACT_STORE="${ARTIFACT_STORE:-$WORK/artifact_store}"
ARTIFACT_KEY_PREFIX="${ARTIFACT_KEY_PREFIX:-yikv-index}"
ARTIFACT_YAML="${ARTIFACT_YAML:-$WORK/artifact-storage.yaml}"
IMPORT_BIN="$YIKV_ROOT/bazel-bin/yikv_import_pipeline"
ARTIFACT_PY="$YIKV_ROOT/tools/artifact_sync/artifact_sync.py"
CONFIG_PIPELINE="$WORK/config.pipeline.json"
ADMIN_SOCKET="${ADMIN_SOCKET:-$WORK/admin.sock}"
SERVER_DB="${SERVER_DB:-$WORK/server_db}"
SERVER_CONFIG="${SERVER_CONFIG:-$WORK/config.server.json}"
YIKV_SERVER_BIN="${YIKV_SERVER_BIN:-$YIKV_ROOT/bazel-bin/yikv_server}"
AUTO_START_SERVER="${AUTO_START_SERVER:-1}"

ensure_build() {
  if [[ ! -x "$IMPORT_BIN" ]]; then
    echo "pipeline_reload: building yikv_import_pipeline..."
    (cd "$YIKV_ROOT" && bazel build //:yikv_import_pipeline)
  fi
  [[ -x "$IMPORT_BIN" ]] || die "no binary: $IMPORT_BIN"
}

ensure_server_build() {
  if [[ ! -x "$YIKV_SERVER_BIN" ]]; then
    echo "pipeline_reload: building yikv_server..."
    (cd "$YIKV_ROOT" && bazel build //:yikv_server)
  fi
  [[ -x "$YIKV_SERVER_BIN" ]] || die "no binary: $YIKV_SERVER_BIN"
}

# 若无 admin socket，则用 SERVER_CONFIG 后台启动 yikv_server（幂等：已存在 socket 则跳过）
ensure_yikv_server_for_reload() {
  [[ "$SKIP_RELOAD" == "1" ]] && return 0
  if [[ -S "$ADMIN_SOCKET" ]]; then
    echo "pipeline_reload: admin socket ok: $ADMIN_SOCKET"
    return 0
  fi
  if [[ "$AUTO_START_SERVER" != "1" ]]; then
    die "no admin socket $ADMIN_SOCKET (set AUTO_START_SERVER=1 to auto-start yikv_server)"
  fi
  require_file "$SERVER_CONFIG"
  ensure_server_build
  echo "pipeline_reload: starting yikv_server -> $YIKV_SERVER_BIN $SERVER_CONFIG"
  nohup "$YIKV_SERVER_BIN" "$SERVER_CONFIG" >>"$WORK/yikv_server.log" 2>&1 &
  echo $! >"$WORK/yikv_server.pid"
  local n=0
  while [[ ! -S "$ADMIN_SOCKET" ]]; do
    sleep 0.2
    n=$((n + 1))
    if [[ $n -gt 150 ]]; then
      die "yikv_server did not create $ADMIN_SOCKET within 30s (log: $WORK/yikv_server.log)"
    fi
  done
  echo "pipeline_reload: yikv_server up (pid $(<"$WORK/yikv_server.pid"), log $WORK/yikv_server.log)"
}

write_pipeline_config() {
  mkdir -p "$DB_DIR"
  cat >"$CONFIG_PIPELINE" <<EOF
{
  "db_path": "$DB_DIR",
  "listen": "127.0.0.1:59999",
  "arena_seg_gb": 1,
  "arena_max_gb": 4,
  "exclusive_arena_lock": false,
  "admin_unix_socket": "$ADMIN_SOCKET"
}
EOF
}

write_artifact_yaml() {
  if [[ -f "$ARTIFACT_YAML" ]]; then
    echo "pipeline_reload: keep existing $ARTIFACT_YAML"
    return 0
  fi
  mkdir -p "$ARTIFACT_STORE"
  cat >"$ARTIFACT_YAML" <<EOF
provider: local
env: dev
key_prefix: $ARTIFACT_KEY_PREFIX
local:
  root: $ARTIFACT_STORE
EOF
}

send_reload() {
  [[ "$SKIP_RELOAD" == "1" ]] && { echo "pipeline_reload: SKIP_RELOAD=1, skip admin"; return 0; }
  ensure_yikv_server_for_reload
  [[ -S "$ADMIN_SOCKET" ]] || die "admin socket missing: $ADMIN_SOCKET"
  if command -v python3 >/dev/null 2>&1; then
    python3 - <<PY
import socket
import sys
sock = "$ADMIN_SOCKET"
c = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
c.connect(sock)
c.sendall(b"reload $TABLE\n")
sys.stdout.buffer.write(c.recv(4096))
PY
    echo ""
  else
    die "python3 required for reload client"
  fi
}

artifact_push() {
  local build_id
  build_id="$("$ARTIFACT_PY" -c "$ARTIFACT_YAML" push --table "$TABLE" --source "$DB_DIR/$TABLE" | tail -n 1)"
  [[ -n "$build_id" ]] || die "push failed"
  echo "pipeline_reload: pushed build_id=$build_id"
}

artifact_pull_switch() {
  mkdir -p "$RELEASE_ROOT"
  "$ARTIFACT_PY" -c "$ARTIFACT_YAML" pull --table "$TABLE" --dest "$RELEASE_ROOT" --switch-active --max-local-versions 2
  echo "pipeline_reload: pulled to $RELEASE_ROOT active=$(readlink -f "$RELEASE_ROOT/active" 2>/dev/null || true)"
}

# 供 yikv_server 使用：db_path 下仅表名为指向 release active 的 symlink
link_server_table() {
  mkdir -p "$SERVER_DB"
  rm -f "$SERVER_DB/$TABLE"
  ln -sfn "$RELEASE_ROOT/active" "$SERVER_DB/$TABLE"
  echo "pipeline_reload: server table link $SERVER_DB/$TABLE -> $RELEASE_ROOT/active"
  cat >"$SERVER_CONFIG" <<EOF
{
  "db_path": "$SERVER_DB",
  "listen": "0.0.0.0:9000",
  "arena_seg_gb": 1,
  "arena_max_gb": 512,
  "exclusive_arena_lock": true,
  "admin_unix_socket": "$ADMIN_SOCKET"
}
EOF
  echo "pipeline_reload: wrote server config: $SERVER_CONFIG"
}

run_import() {
  local phase_label="$1"
  shift
  echo "pipeline_reload: import ($phase_label) -> $IMPORT_BIN ..."
  "$IMPORT_BIN" \
    --config "$CONFIG_PIPELINE" \
    --index "$TABLE" \
    --schema_json "$SCHEMA_JSON" \
    "$@"
}

phase1_new_table() {
  require_file "$SCHEMA_JSON"
  [[ -n "${PART1:-}" ]] || die "set PART1=/path/to/first.parquet"
  require_file "$PART1"
  ensure_build
  write_pipeline_config
  write_artifact_yaml

  rm -rf "$DB_DIR/$TABLE"
  run_import phase1-new \
    --create_if_missing \
    --input "$PART1"

  artifact_push
  artifact_pull_switch
  link_server_table
  send_reload
  echo "pipeline_reload: phase1 done (server: $SERVER_CONFIG, auto-start if needed)."
}

phase2_old_table_more_file() {
  require_file "$SCHEMA_JSON"
  [[ -n "${PART2:-}" ]] || die "set PART2=/path/to/second.parquet"
  require_file "$PART2"
  ensure_build
  require_file "$CONFIG_PIPELINE"
  require_file "$ARTIFACT_YAML"
  [[ -d "$DB_DIR/$TABLE" ]] || die "no existing index $DB_DIR/$TABLE — run phase1 first with same WORK"

  # 不 recreate：在已有索引上只导入第二个 parquet（切勿再传 PART1，否则会重复写入）
  run_import phase2-append \
    --input "$PART2"

  artifact_push
  artifact_pull_switch
  link_server_table
  send_reload
  echo "pipeline_reload: phase2 done."
}

reload_only() {
  require_file "$SERVER_CONFIG"
  send_reload
}

print_usage() {
  sed -n '2,70p' "$0" | sed 's/^# \{0,1\}//'
}

case "$CMD" in
  phase1) phase1_new_table ;;
  phase2) phase2_old_table_more_file ;;
  all)
    phase1_new_table
    phase2_old_table_more_file
    ;;
  reload-only) reload_only ;;
  help|-h|--help) print_usage ;;
  *) die "usage: $0 phase1|phase2|all|reload-only|help" ;;
esac
