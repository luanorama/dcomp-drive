#!/usr/bin/env bash
set -euo pipefail

path="/teste-recuperacao-$(date +%s)-$$.txt"
compose=(docker compose --profile storage)
node=$("${compose[@]}" exec -T controller python scripts/recovery_demo.py seed "$path")
trap 'bash scripts/fault.sh start "$node" >/dev/null' EXIT
bash scripts/fault.sh stop "$node"
"${compose[@]}" exec -T controller python scripts/recovery_demo.py verify "$path" "$node"
