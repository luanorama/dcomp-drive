# DComp Drive — Sistema Distribuído de Armazenamento de Arquivos

Sistema distribuído de armazenamento com replicação, verificação de integridade por hash e recuperação automática de falhas, desenvolvido para a disciplina de Sistemas Distribuídos (COMP0470) da Universidade Federal de Sergipe.

Inspirado em HDFS, GFS, Ceph e MinIO.

---

## Arquitetura

```
┌──────────┐       REST/JSON        ┌──────────────────┐       REST/JSON       ┌──────────────────┐
│  Cliente  │ ◄───────────────────► │   Controlador     │ ◄──────────────────► │  Nó de           │
│  (CLI)    │   metadados apenas    │   (metadados +    │   registro,          │  Armazenamento   │
│           │                       │    controle)      │   heartbeat          │  (dados)         │
│           │ ─────────────────────►│                   │                      │                  │
│           │   dados direto aos    │   SQLite WAL      │                      │  Discos (pastas) │
│           │   nós (plano dados)   │                   │                      │                  │
└──────────┘                       └──────────────────┘                      └──────────────────┘
                                          │                                        × 5 nós
                                          │  Monitora heartbeats
                                          │  Dispara re-replicação
                                          │  Gerencia leases
```

**Papéis:**
- **Controlador** — gerencia metadados (arquivos, diretórios, chunks, réplicas), posicionamento, leases de concorrência e detecção de falhas
- **Nó de Armazenamento** — armazena chunks em disco, serve leituras, verifica hashes, copia chunks entre nós
- **Cliente (CLI)** — divide arquivos em chunks, calcula hashes, envia dados diretamente aos nós

---

## Stack Tecnológica

| Componente | Tecnologia |
|------------|-----------|
| Linguagem | Python 3.10+ |
| Web Framework | FastAPI |
| Banco de Metadados | SQLite (modo WAL) |
| Comunicação | REST / JSON |
| Hash | SHA-256 |
| Criptografia | AES-256-GCM (opcional, no cliente) |
| Contêineres | Docker + Docker Compose |

---

## Pré-requisitos

- **Docker** e **Docker Compose** (recomendado)
- Ou **Python 3.10+** e **pip** para execução local

---

## Instalação e Execução

### Opção 1: Docker Compose (recomendado)

Sobe 1 controlador e 5 nós de armazenamento com um único comando:

```bash
# Clonar o repositório
git clone <url-do-repositório>
cd dcomp-drive

# Subir o ambiente completo
docker compose up --build

# Em outro terminal, usar o CLI
docker compose exec client python -m client.cli put /docs/arquivo.txt ./arquivo_local.txt
```

### Opção 2: Execução local (desenvolvimento)

```bash
# Instalar dependências
pip install -r requirements.txt

# Subir o controlador (porta 8000)
DB_PATH=./controller.db uvicorn controller.main:app --host 0.0.0.0 --port 8000 --reload

# Em outro terminal, subir um nó de armazenamento (porta 9000)
NODE_ID=node1 DATA_DIRS=/tmp/dcomp/node1/disk1:/tmp/dcomp/node1/disk2 \
  uvicorn storage_node.main:app --host 0.0.0.0 --port 9000 --reload
```

---

## Configuração

Todas as configurações ficam em [`config.yaml`](config.yaml) e podem ser sobrescritas por variáveis de ambiente:

| Parâmetro | Padrão | Variável de Ambiente | Descrição |
|-----------|--------|---------------------|-----------|
| `replication_factor` | 3 | `REPLICATION_FACTOR` | Número de réplicas por chunk (N) |
| `write_quorum` | 2 | `WRITE_QUORUM` | Réplicas confirmadas antes de responder (W) |
| `chunk_size_mb` | 4 | `CHUNK_SIZE_MB` | Tamanho de cada chunk em MB |
| `encryption.enabled` | false | `ENCRYPTION_ENABLED` | Ativa criptografia AES-256-GCM |
| `heartbeat_interval_s` | 3 | `HEARTBEAT_INTERVAL_S` | Intervalo de heartbeat dos nós |
| `suspect_after_s` | 6 | `SUSPECT_AFTER_S` | Tempo para marcar nó como suspeito |
| `dead_after_s` | 10 | `DEAD_AFTER_S` | Tempo para marcar nó como morto |
| `disk_usage_limit_pct` | 90 | `DISK_USAGE_LIMIT_PCT` | Limite de uso do disco (%) |
| `lease_ttl_s` | 60 | `LEASE_TTL_S` | TTL do lease de escrita |
| `under_replicated_policy` | accept | `UNDER_REPLICATED_POLICY` | Política quando há menos nós que N |
| `scrub_interval_s` | 300 | `SCRUB_INTERVAL_S` | Intervalo de scrubbing em segundo plano |

A chave de criptografia é passada pela variável `DRIVE_KEY`.

---

## Comandos do CLI

```bash
# Criar diretório
dcomp-cli mkdir /projetos

# Enviar arquivo (put)
dcomp-cli put /projetos/relatorio.pdf ./relatorio.pdf

# Baixar arquivo (get)
dcomp-cli get /projetos/relatorio.pdf ./download.pdf

# Listar diretório
dcomp-cli ls /projetos

# Consultar metadados (tamanho, hash, réplicas, estado)
dcomp-cli stat /projetos/relatorio.pdf

# Renomear/mover
dcomp-cli mv /projetos/relatorio.pdf /projetos/final.pdf

# Remover arquivo
dcomp-cli rm /projetos/final.pdf

# Remover diretório (somente se vazio)
dcomp-cli rmdir /projetos

# Ver estado do cluster (nós, discos, replicação)
dcomp-cli status

# Esvaziar nó antes de retirá-lo (drain)
dcomp-cli drain node3
```

---

## API do Controlador

Documentação completa em [`docs/endpoints.md`](docs/endpoints.md).

### Resumo dos endpoints:

| Método | Rota | Descrição |
|--------|------|-----------|
| `GET` | `/health` | Health check |
| `POST` | `/files/allocate` | Aloca espaço e retorna destinos para escrita |
| `POST` | `/files/commit` | Confirma upload com quórum W |
| `GET` | `/files/locate?path=` | Localiza réplicas para leitura |
| `GET` | `/files/stat?path=` | Metadados completos do arquivo |
| `DELETE` | `/files?path=` | Remove arquivo |
| `POST` | `/files/move` | Renomeia/move arquivo |
| `POST` | `/dirs` | Cria diretório |
| `GET` | `/dirs?path=` | Lista diretório |
| `DELETE` | `/dirs?path=` | Remove diretório (só se vazio) |
| `POST` | `/chunks/corrupt` | Reporta réplica corrompida |
| `POST` | `/nodes/register` | Registra nó de armazenamento |
| `POST` | `/nodes/{id}/heartbeat` | Heartbeat do nó |
| `POST` | `/nodes/{id}/drain` | Esvazia nó para remoção |
| `GET` | `/status` | Estado do cluster |
| `GET` | `/metrics` | Métricas em JSON |

---

## Fluxo de Escrita (put)

1. Cliente divide o arquivo em chunks de 4 MB e calcula SHA-256 de cada chunk e do arquivo
2. Cliente → Controlador: `POST /files/allocate` → recebe lease e lista de N destinos por chunk
3. Cliente → Nós: `PUT /chunks/{chunk_id}` para cada destino, em paralelo
4. Cliente → Controlador: `POST /files/commit` com os disk_ids confirmados
5. Controlador valida quórum W e marca o arquivo como íntegro (ou sub-replicado)
6. Re-replicação completa as réplicas faltantes em segundo plano

## Fluxo de Leitura (get)

1. Cliente → Controlador: `GET /files/locate` → recebe réplicas vivas
2. Cliente → Nó: `GET /chunks/{chunk_id}` de uma réplica
3. Nó confere o hash antes de devolver; cliente confere novamente
4. Se hash errado, tenta outra réplica e reporta corrupção ao controlador

---

## Testes

```bash
# Rodar os testes
pip install pytest
python -m pytest tests/ -v

# Resultado esperado: 19 passed
```

### Testes implementados

| Teste | O que valida |
|-------|-------------|
| `test_health` | Controlador responde `/health` |
| `test_create_and_list_dir` | Criar e listar diretório |
| `test_create_nested_dir` | Diretórios aninhados |
| `test_create_dir_no_parent_fails` | Erro 404 sem pai |
| `test_create_dir_duplicate_fails` | Erro 409 diretório duplicado |
| `test_remove_empty_dir` | Remover diretório vazio |
| `test_remove_nonempty_dir_fails` | Erro 409 não vazio |
| `test_allocate_returns_lease_and_targets` | Alocação retorna lease e destinos |
| `test_commit_full_flow` | Fluxo allocate → commit completo |
| `test_locate_after_commit` | Localização após commit |
| `test_stat_after_commit` | Metadados após commit |
| `test_second_allocate_same_path_returns_409` | Concorrência: dois escritores → 409 |
| `test_locate_during_write_returns_409` | Leitura durante escrita → 409 |
| `test_report_corrupt_marks_replica` | Reportar corrupção funciona |
| `test_move_file` | Renomear arquivo |
| `test_delete_file` | Remover arquivo |
| `test_data_persists_after_restart` | Persistência (RF10) |

---

## Estrutura do Projeto

```
dcomp-drive/
├── config.yaml                    # Configuração global
├── requirements.txt               # Dependências Python
├── controller/                    # Controlador / nó de metadados
│   ├── main.py                    # FastAPI app + /health
│   ├── config.py                  # Leitura de config + env vars
│   ├── db.py                      # Schema SQLite WAL
│   ├── repository.py              # Funções de acesso ao banco
│   ├── logger.py                  # Logger JSON estruturado
│   └── routers/
│       ├── files.py               # Endpoints /files/*, /dirs/*, /chunks/corrupt
│       └── cluster.py             # Endpoints /nodes/*, /status, /metrics
├── storage_node/                  # Nó de armazenamento
│   ├── routers/
│   │   └── chunks.py              # Endpoints de chunks (Pessoa B)
│   ├── agent.py                   # Registro e heartbeat (Pessoa C)
│   └── scrubbing.py               # Verificação de integridade (Pessoa B)
├── client/                        # Cliente CLI (Pessoa B)
├── deploy/                        # Docker Compose e configs de deploy
├── scripts/                       # Scripts de injeção de falha
├── docs/
│   └── endpoints.md               # Contrato de API compartilhado
└── tests/
    └── test_controller.py         # Testes do controlador (19 testes)
```

---

## Decisões Técnicas

| Decisão | Escolha | Justificativa |
|---------|---------|--------------|
| Consistência | Forte nos metadados, N=3, W=2 | CAP: priorizamos consistência; leitura disponível com 1 réplica viva |
| Concorrência | Lease de escrita por path | Garante que ninguém leia arquivo parcial (RF8) |
| Hash | SHA-256 sobre dados cifrados | Nós fazem scrubbing sem precisar da chave |
| Criptografia | AES-256-GCM no cliente | Nós nunca veem texto claro |
| Posicionamento | Greedy por espaço livre | Nunca mesmo disco, nunca mesma máquina quando possível |
| Persistência | SQLite WAL | Sobrevive a reinícios sem reconstrução manual |

---

## Divisão de Tarefas

| Pessoa | Responsabilidade | Requisitos |
|--------|-----------------|------------|
| **A (Raissa)** | Controlador, metadados, posicionamento, concorrência, persistência | RF1, RF3, RF7, RF8, RF10 |
| **B** | Nó de armazenamento, CLI, chunks, hash, criptografia, scrubbing | RF2, RF4, RF9 |
| **C** | Docker Compose, heartbeat, detecção de falhas, re-replicação, drain, logs | RF5, RF6, RF11, RF12 |

---

## Limitações Conhecidas

- **Controlador é ponto único de falha**: não implementamos Raft/consenso; o controlador único é uma limitação registrada. Ele sobrevive a reinícios (RF10), mas indisponibilidade dele bloqueia novas escritas e leituras de metadados
- **Sem interface web**: apenas CLI e API REST
- **Sem domínios de falha**: a regra de não repetição considera disco e máquina, mas não rack/zona
- **Sem erasure coding**: apenas replicação completa (N cópias)

---

## Licença

Projeto acadêmico — Universidade Federal de Sergipe, 2026.
