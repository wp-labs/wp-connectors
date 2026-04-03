#!/usr/bin/env bash
set -euo pipefail

for _ in $(seq 1 20); do
  if curl -fsS http://127.0.0.1:18080/health >/dev/null 2>&1; then
    exit 0
  fi
  sleep 0.5
done

exit 1
