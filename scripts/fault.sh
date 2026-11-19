#!/usr/bin/env bash
set -euo pipefail

action=${1:-}
node=${2:-}
case "$node" in
  node1|node2|node3|node4|node5) ;;
  *) echo "Uso: $0 stop|start|restart NODE ou $0 corrupt NODE disk1|disk2 CHUNK_UUID" >&2; exit 2 ;;
esac

case "$action" in
  stop|start|restart) docker compose --profile storage "$action" "$node" ;;
  corrupt)
    disk=${3:-}
    chunk=${4:-}
    case "$disk" in disk1|disk2) ;; *) exit 2 ;; esac
    docker compose --profile storage exec -T "$node" python -c '
from pathlib import Path
from sys import argv
from uuid import UUID

disk, chunk = argv[1:]
if str(UUID(chunk)) != chunk:
    raise SystemExit("Informe o UUID do chunk")
path = Path("/data", disk, chunk)
with path.open("r+b") as file:
    first = file.read(1)
    file.seek(0)
    file.write(bytes([first[0] ^ 1]) if first else b"\x01")
print(f"Chunk alterado: {path}")
' "$disk" "$chunk"
    ;;
  *) echo "Ação desconhecida: $action" >&2; exit 2 ;;
esac
