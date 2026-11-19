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
    docker compose --profile storage exec -T "$node" \
      python /app/scripts/corrupt_chunk.py "/data/$disk" "$chunk"
    ;;
  *) echo "Ação desconhecida: $action" >&2; exit 2 ;;
esac
