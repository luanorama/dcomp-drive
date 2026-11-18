# DComp Drive — Endpoints

Documento compartilhado: cada pessoa anota o endpoint assim que fica pronto.

---

## Controlador (`http://controller:8000`) — Pessoa A

### Sistema
| Método | Rota | Status |
|--------|------|--------|
| GET | `/health` | ✅ Implementado |

### Arquivos e Diretórios (RF1)
| Método | Rota | Corpo / Query | Resposta | Status |
|--------|------|---------------|----------|--------|
| POST | `/files/allocate` | `{path, size, num_chunks, client_id?}` | `{lease_id, version, warning?, chunks:[{idx, chunk_id, targets:[{node_url, disk_id}]}]}` | ✅ Implementado |
| POST | `/files/commit` | `{path, lease_id, file_hash, chunks:[{idx, chunk_id, hash, size, confirmed:[disk_id]}]}` | `{state}` | ✅ Implementado |
| GET | `/files/locate?path=` | — | `{size, file_hash, chunks:[{idx, chunk_id, hash, replicas:[{node_url, disk_id}]}]}` | ✅ Implementado |
| GET | `/files/stat?path=` | — | `{path, size, modified_at, file_hash, version, state, chunks:[{idx, replicas:[{disk_id, state}]}]}` | ✅ Implementado |
| DELETE | `/files?path=` | — | `{ok: true}` | ✅ Implementado |
| POST | `/files/move` | `{from, to}` | `{ok: true}` | ✅ Implementado |
| POST | `/dirs` | `{path}` | `{ok: true}` | ✅ Implementado |
| GET | `/dirs?path=` | — | `{entries:[{name, type, size}]}` | ✅ Implementado |
| DELETE | `/dirs?path=` | — | `{ok: true}` | ✅ Implementado |

### Corrupção (RF4 — stub para Pessoa B)
| Método | Rota | Corpo | Resposta | Status |
|--------|------|-------|----------|--------|
| POST | `/chunks/corrupt` | `{chunk_id, disk_id}` | `{ok: true}` | ✅ Implementado |

### Cluster (stubs para Pessoa C)
| Método | Rota | Corpo | Resposta | Status |
|--------|------|-------|----------|--------|
| POST | `/nodes/register` | `{node_id, address, disks:[{disk_id, path, total_bytes, used_bytes}]}` | `{ok: true}` | ✅ Stub |
| POST | `/nodes/{node_id}/heartbeat` | `{disks:[{disk_id, total_bytes, used_bytes}]}` | `{ok: true}` | ✅ Stub |
| POST | `/nodes/{node_id}/drain` | — | `{ok: true, queued_chunks: N}` | ✅ Stub |
| GET | `/status` | — | `{nodes, files_by_state, rereplications}` | ✅ Stub |
| GET | `/metrics` | — | `{replicas_by_state, files_by_state, ...}` | ✅ Stub |

---

## Tabela de Nós (para Pessoa C usar)

```sql
-- nodes
node_id        TEXT PRIMARY KEY
address        TEXT NOT NULL            -- ex: "http://node1:9000"
status         TEXT DEFAULT 'alive'     -- alive | suspect | dead | draining
last_heartbeat REAL                     -- epoch Unix (time.time())

-- disks
disk_id        TEXT PRIMARY KEY         -- formato: "node_id/disk_name"
node_id        TEXT REFERENCES nodes
path           TEXT NOT NULL
total_bytes    INTEGER
used_bytes     INTEGER
```

Funções do `repository.py` para a Pessoa C:
- `register_node(conn, node_id, address, disks)` → registra ou atualiza nó
- `update_node_status(conn, node_id, status)` → altera status (alive/suspect/dead/draining)
- `update_node_heartbeat(conn, node_id, disks)` → atualiza heartbeat e espaço
- `mark_node_replicas_lost(conn, node_id)` → marca todas réplicas do nó como 'lost'
- `list_chunks_of_node(conn, node_id)` → lista chunks de um nó (reconciliação)
- `get_under_replicated_chunks(conn, limit)` → fila de re-replicação, priorizados
- `mark_replica_as(conn, chunk_id, disk_id, state)` → altera estado de uma réplica
- `choose_replication_targets(conn, n, exclude_disks, exclude_nodes)` → posicionamento
- `list_expired_leases(conn)` → leases expirados para limpeza
- `delete_pending_replicas_of_lease(conn, path)` → limpa upload incompleto

---

## Nó de Armazenamento (`http://nodeN:9000`) — Pessoa B

| Método | Rota | Status |
|--------|------|--------|
| PUT | `/chunks/{chunk_id}?disk_id=&hash=` | 🔲 Pessoa B |
| GET | `/chunks/{chunk_id}?disk_id=` | 🔲 Pessoa B |
| DELETE | `/chunks/{chunk_id}?disk_id=` | 🔲 Pessoa B |
| GET | `/chunks` | 🔲 Pessoa B |
| POST | `/chunks/{chunk_id}/copy` | 🔲 Pessoa B |
| GET | `/health` | 🔲 Pessoa B |
| GET | `/metrics` | 🔲 Pessoa B |

---

## Códigos de Erro

| Código | Quando |
|--------|--------|
| 404 | Caminho ou chunk inexistente |
| 409 | Lease ativo; diretório não vazio; destino já existe |
| 422 | Hash não confere (gravação ou leitura) |
| 507 | Nós/espaço insuficientes para N réplicas, ou W não atingido |
