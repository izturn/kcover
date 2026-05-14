#!/usr/bin/env bash
set -euo pipefail

if command -v chronyd >/dev/null 2>&1; then
    chronyd -x || echo "warning: chronyd failed to start, continuing without it" >&2
fi

exec /app/kcover-agent "$@"