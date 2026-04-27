#!/usr/bin/env bash
set -euo pipefail

chronyd -x

if [[ $# -gt 0 ]]; then
    exec "$@"
fi

exec /app/kcover-agent