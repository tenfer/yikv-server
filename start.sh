#!/usr/bin/env bash
# Start pipeline_agent and optionally yikv_server.
#
# YIKV_PIPELINE_ROLE (required):
#   build  — only HTTP agent (publish / buildIndex / pushIndex)
#   online — yikv_server (background) + HTTP agent (deploy / pull / reload)
#   both   — same as online (build + online on one host)
#
# Common env: YIKV_ROOT, WORK, SERVER_CONFIG, YIKV_SERVER_BIN, HOST, PORT, ADMIN_SOCKET
# (defaults match pipeline_agent.py). Install deps: pip install -r requirements.txt
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YIKV_ROOT="${YIKV_ROOT:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
export YIKV_ROOT

ROLE="${YIKV_PIPELINE_ROLE:-}"
if [[ -z "$ROLE" ]]; then
  echo "start.sh: set YIKV_PIPELINE_ROLE=build|online|both" >&2
  exit 1
fi
ROLE_LC="$(printf '%s' "$ROLE" | tr '[:upper:]' '[:lower:]')"

WORK="${WORK:-/data/yikvdb}"
ADMIN_SOCKET="${ADMIN_SOCKET:-$WORK/admin.sock}"
SERVER_CONFIG="${SERVER_CONFIG:-$WORK/config.server.json}"
YIKV_SERVER_BIN="${YIKV_SERVER_BIN:-$YIKV_ROOT/bazel-bin/yikv_server}"

need_yikv=0
case "$ROLE_LC" in
  build) need_yikv=0 ;;
  online | both) need_yikv=1 ;;
  *)
    echo "start.sh: YIKV_PIPELINE_ROLE must be build, online, or both (got: $ROLE)" >&2
    exit 1
    ;;
esac

start_yikv_server() {
  mkdir -p "$WORK"
  if [[ ! -f "$SERVER_CONFIG" ]]; then
    echo "start.sh: WARNING: missing $SERVER_CONFIG — yikv_server not started." >&2
    echo "start.sh: Create server JSON (e.g. after first deployIndex link step) or copy from config.example.json" >&2
    return 0
  fi
  if [[ ! -x "$YIKV_SERVER_BIN" ]]; then
    echo "start.sh: building yikv_server..." >&2
    (cd "$YIKV_ROOT" && bazel build //:yikv_server)
  fi
  [[ -x "$YIKV_SERVER_BIN" ]] || { echo "start.sh: not executable: $YIKV_SERVER_BIN" >&2; exit 1; }

  if [[ -S "$ADMIN_SOCKET" ]]; then
    echo "start.sh: admin socket already present: $ADMIN_SOCKET (skip yikv_server)" >&2
    return 0
  fi

  echo "start.sh: starting yikv_server -> $YIKV_SERVER_BIN $SERVER_CONFIG" >&2
  nohup "$YIKV_SERVER_BIN" "$SERVER_CONFIG" >>"$WORK/yikv_server.log" 2>&1 &
  echo $! >"$WORK/yikv_server.pid"

  local n=0
  while [[ ! -S "$ADMIN_SOCKET" ]]; do
    sleep 0.2
    n=$((n + 1))
    if [[ $n -gt 150 ]]; then
      echo "start.sh: yikv_server did not open $ADMIN_SOCKET in 30s (log $WORK/yikv_server.log)" >&2
      exit 1
    fi
  done
  echo "start.sh: yikv_server up (pid $(<"$WORK/yikv_server.pid"))" >&2
}

if [[ "$need_yikv" -eq 1 ]]; then
  start_yikv_server
fi

cd "$YIKV_ROOT"
echo "start.sh: starting pipeline_agent (YIKV_PIPELINE_ROLE=$ROLE_LC) ..." >&2
exec python3 "$SCRIPT_DIR/pipeline_agent.py"
