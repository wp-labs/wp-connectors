#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="tests/http/.run"
PID_FILE="$RUN_DIR/server.pid"

if [[ ! -f "$PID_FILE" ]]; then
  exit 0
fi

pid="$(cat "$PID_FILE")"
if kill -0 "$pid" >/dev/null 2>&1; then
  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" 2>/dev/null || true
fi

rm -f "$PID_FILE"
