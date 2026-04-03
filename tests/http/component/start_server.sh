#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="tests/http/.run"
PID_FILE="$RUN_DIR/server.pid"
LOG_FILE="$RUN_DIR/server.log"

mkdir -p "$RUN_DIR"

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE")"
  if kill -0 "$pid" >/dev/null 2>&1; then
    exit 0
  fi
  rm -f "$PID_FILE"
fi

python3 tests/http/component/test_server.py 18080 >"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"
