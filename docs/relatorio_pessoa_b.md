# Relatório técnico — Pessoa B (nó e cliente)

## Escopo e arquitetura

A Pessoa B implementa o serviço de chunks (`storage_node/`), o cliente CLI
(`client/`), a parte do cliente de RF1, a interface RF2, a transferência com
replicação de RF3, hash SHA-256 e scrubbing (RF4), cifra opcional AES-256-GCM
(RF9), logs JSON (RF12) e métricas de observabilidade. O controlador continua
responsável por metadados, leases, posicionamento e quórum. O plano de dados
passa diretamente entre o cliente e os nós; o controlador recebe apenas
metadados e confirmações.

O cliente divide arquivos em chunks e envia cada chunk em paralelo aos destinos
da alocação. Cada nó valida e persiste o conteúdo. Só depois de W confirmações
por chunk o cliente envia o commit. Na leitura, o cliente consulta réplicas,
confere o hash e substitui o destino local atomicamente após validar o arquivo
inteiro.

A Pessoa A mantém metadados confirmados por transações SQLite; para o perfil
padrão, N=3 e W=2. A nova versão só fica visível depois do commit. Se o commit
ficar com menos de N réplicas, o estado é `under_replicated` e a Pessoa C deve
completar as cópias em segundo plano. Essa escolha prioriza consistência dos
metadados; escrita e leitura de metadados dependem da disponibilidade do
controlador.

## Decisões

- **Hash:** SHA-256 minúsculo sobre os bytes armazenados. O hash do arquivo é o
  SHA-256 da concatenação dos chunks em ordem. Assim cada nó consegue verificar
  o conteúdo e executar scrubbing sem conhecer a chave.
- **Criptografia e chave:** AES-256-GCM no cliente. `DRIVE_KEY` contém 32 bytes,
  em hexadecimal (64 caracteres) ou base64. A chave não vai ao controlador ou
  aos nós, nem aparece em logs. A opção vem da configuração global e a leitura
  exige a mesma opção e chave usada na gravação.
- **Formato cifrado:** `DCDRIVE1` (8 bytes), nonce aleatório (12 bytes) e
  `ciphertext || tag` (tag de 16 bytes). Cada chunk usa um nonce gerado uma vez.
  O AAD é `chunk_id:idx`; trocar ou reordenar chunks falha na autenticação.
  Overhead: 36 bytes por chunk, inclusive em arquivos vazios.
- **Disco:** `<disco>/chunks/<uuid[:2]>/<uuid>.<sha256>.chunk`. O hash esperado
  faz parte do nome. Escritas usam `.tmp`, `fsync`, releitura e `os.replace`;
  chunks corrompidos saem da listagem ativa e vão para `.corrupt`. Caminhos
  externos nunca são montados com dados sem validação.
- **Consistência:** o cliente espera W confirmações em cada chunk antes de
  chamar `/files/commit`. Se W não for atingido, não envia commit; o staging
  expira pelo TTL do lease do controlador.

## Protocolos e fluxos

1. **Gravação:** o cliente valida caminho e arquivo, consulta `/health` para
   obter W e chama `POST /files/allocate` com o tamanho armazenado e a quantidade
   de chunks. Para cada alocação, calcula o hash e envia `PUT
   /chunks/{chunk_id}?disk_id=…&hash=…` aos nós em paralelo. Retentativas usam os
   mesmos bytes. Com W respostas válidas por chunk, chama `POST /files/commit`
   com hash, tamanho armazenado e discos confirmados. O controlador pode
   responder `intact` ou `under_replicated`; o CLI informa avisos de alocação e
   sub-replicação.
2. **Leitura:** `GET /files/locate` retorna hash e réplicas vivas. O cliente
   baixa cada chunk, confere hash e reporta réplicas com resposta 422 ou conteúdo
   divergente em `POST /chunks/corrupt`. Se uma réplica falha, tenta a próxima.
   O temporário fica na pasta do destino e só substitui um arquivo existente
   depois da validação completa.
3. **Nó:** `PUT` valida UUID, hash e `disk_id`; `GET` verifica o conteúdo antes
   de servir; `DELETE` é idempotente; `/chunks` lista hash e tamanho pelo nome do
   arquivo. `/chunks/{id}/copy` verifica a origem antes do envio e retorna
   sucesso apenas após confirmação do destino.
4. **Scrubbing:** `scrub_once` recalcula SHA-256 em blocos de 1 MiB. Uma
   divergência é movida à quarentena e denunciada ao controlador. Resposta 200
   ou 404 descarta a quarentena; falha mantém o arquivo e provoca nova denúncia
   no próximo ciclo. O loop periódico usa thread daemon; `POST /scrub` permite
   executar uma passada para teste e demonstração.

Os erros do controlador são interpretados em `detail.error`, na lista de
validação do FastAPI, no formato JSON simples e em texto puro. Os estados são
mantidos em inglês em `--json`; só a apresentação humana os traduz. Os eventos
JSON incluem horário UTC, componente, nível e campos da operação. O cliente
escreve todos os eventos em `DCOMP_LOG_FILE`; a saída de terminal mostra apenas
avisos por padrão. `/metrics` do nó apresenta contadores, latências recentes,
bytes, falhas de hash, estado do scrubbing e espaço por disco.
Eventos principais incluem `put_started`, `chunk_uploaded`, `put_done`,
`get_started`, `replica_read_failed`, `get_done`, `chunk_put`, `chunk_copy_done`,
`scrub_corrupt_detected` e `corrupt_reported`.

### Formatos principais

- A alocação recebe `{path, size, num_chunks, client_id?, replication_factor?}`
  e devolve `{lease_id, version, warning, chunks:[{idx, chunk_id,
  targets:[{node_url, disk_id}]}]}`.
- O commit recebe `{path, lease_id, file_hash, chunks:[{idx, chunk_id, hash,
  size, confirmed:[disk_id]}]}`. `size` é o tamanho armazenado, já incluindo a
  cifra quando ligada.
- `PUT /chunks/{chunk_id}` recebe bytes crus (`application/octet-stream`) e
  devolve `{ok, hash, size}`. `GET` devolve bytes crus e `X-Chunk-Hash`.
- `POST /chunks/{chunk_id}/copy` recebe
  `{disk_id, target_url, target_disk_id}` e devolve `{ok, bytes, duration_s}`.
  `POST /chunks/corrupt` recebe `{chunk_id, disk_id}`.
- Os dados ativos ficam em
  `<disco>/chunks/<uuid[:2]>/<uuid>.<sha256>.chunk`; a quarentena usa o sufixo
  `.corrupt` e staging temporário usa `<disco>/tmp/<uuid>.part`.

## Limitações conhecidas

- O controlador não oferece renovação nem aborto de lease. Uploads que excedam
  o TTL podem falhar; o staging é limpo pelo housekeeping da Pessoa A. Ajuste
  `LEASE_TTL_S` no controlador para arquivos grandes.
- Chunks de versões substituídas ficam retidos por 60 segundos. Uma leitura
  longa durante sobrescrita pode perder acesso à versão anterior; repita a
  leitura se isso ocorrer.
- Chunks são processados em sequência; réplicas de um chunk são atendidas em
  paralelo. Não há controle de banda nem limite de concorrência entre clientes.
- `stat.size` representa bytes armazenados. Com cifra habilitada, inclui 36
  bytes por chunk.
- A cifra é global e estrita: dados cifrados exigem cifra e chave na leitura;
  conteúdo claro exige cifra desligada. A chave permanece sob gestão do usuário
  no ambiente do cliente. Se a chave for perdida, os chunks cifrados não podem
  ser recuperados.
- O cliente confia no W anunciado por `/health` e usa `write_quorum` local como
  fallback se essa consulta falhar.
- O agente de registro e heartbeat é trabalho da Pessoa C. Sem
  `storage_node.agent`, os nós não se registram nem enviam heartbeat sozinhos;
  o `LocalCluster` faz o registro e heartbeat sintéticos para os testes.
- A verificação foi executada em Python 3.13. Python 3.14 não instalou a versão
  fixada de `pydantic-core` no ambiente disponível, pois o PyO3 dessa versão não
  reconhece Python 3.14. O código B usa sintaxe compatível com Python 3.10+.
- A validação com cinco nós mede funcionamento local, não desempenho de
  produção; não foi medido um limite de arquivos nem reivindicada capacidade de
  produção. O controlador continua um ponto único de falha, conforme o limite
  documentado pela Pessoa A.

## Evidências

Data: **18/11/2026** (America/Bahia). Implementação testada nos commits
`ad21f5b` (núcleo do nó), `f9e4949` (CLI e harness), `5b6c0a9` (quarentena após
denúncia confirmada) e `f30582a` (cálculo do overhead de cifra).

Baseline da Pessoa A, antes das alterações B:

```text
28 passed, 1 warning in 17.88s
```

Resultado da suíte completa, executada em Python 3.13.14:

```text
python3 -m pytest -q
......................................................................   [100%]
70 passed, 1 warning in 148.77s (0:02:28)
```

O aviso é a depreciação de `anyio.abc.BlockingPortal` reportada pelo
`TestClient` do Starlette. A verificação de `except` silencioso não encontrou
ocorrências; `DRIVE_KEY` aparece somente nas leituras e mensagens de
configuração do cliente; `git diff --stat -- controller` ficou vazio. Os dois
comandos de ajuda (`python3 -m client.cli --help` e `scripts/dcomp-cli --help`)
retornaram código 0.

O smoke manual foi iniciado por
`python3 -m devtools.localcluster --directory <temp> --nodes 5` e usou os cinco
nós reais; os hashes SHA-256 do arquivo fonte e do baixado foram iguais:

```text
OK diretório criado
OK /smoke/pessoa-b.txt — 21 B, 1 chunks, íntegro, réplicas 3/3, 0.76 s
OK /smoke/pessoa-b.txt para <destino local> — 21 B, 1 chunks, 0.48 s
state: intact; réplicas ok em node1/disk1, node2/disk1 e node3/disk1
sha256 source:     276ea815b5b6584b386b66239b75a49d63fc48916e09df65c8cf3da7ee6ea886
sha256 downloaded: 276ea815b5b6584b386b66239b75a49d63fc48916e09df65c8cf3da7ee6ea886
hash match: True
```

## Como explicar em 2 minutos

O controlador guarda nomes, versões e destinos, e os nós guardam os bytes. O
cliente divide o arquivo e manda cada chunk diretamente às réplicas que o
controlador alocou. A escrita só é confirmada depois de W gravações verificadas
por chunk. SHA-256 valida os chunks e o arquivo inteiro, inclusive durante a
leitura e o scrubbing. Se a cifra estiver ligada, o cliente usa AES-GCM e os nós
recebem somente bytes cifrados. Leituras tentam outras réplicas e publicam o
arquivo local apenas depois de verificar tudo. Logs JSON e `/metrics` permitem
acompanhar falhas, latência, vazão e scrubbing.

**Perguntas prováveis**

1. **Por que calcular o hash sobre o cifrado?** Os nós podem verificar os bytes
   armazenados e fazer scrubbing sem acesso à chave.
2. **O que acontece com uma réplica corrompida?** O nó a põe em quarentena,
   denuncia ao controlador e a descarta após resposta terminal; o cliente tenta
   outra réplica e a Pessoa C pode re-replicar.
3. **Onde fica a chave?** Em `DRIVE_KEY` no ambiente do cliente. Não é enviada
   pela rede nem registrada nos logs.
4. **O que o scrub faz?** Recalcula SHA-256 em blocos de 1 MiB, compara com o
   hash do nome e denuncia divergências.
5. **O que ocorre se o lease expira?** O commit é recusado e o controlador
   limpa o staging; o cliente precisa iniciar outro `put`.
6. **Por que cliente→nó diretamente?** Para o conteúdo não atravessar o
   controlador, que cuida do plano de controle e dos metadados.

## Pedidos para A/C e atualização do README

- **Pessoa C:** criar `storage_node/agent.py` com `async def run(settings,
  list_disks)` para registro/heartbeat e manter detecção de falhas e
  re-replicação; o Dockerfile deve incluir `controller/config.py`, `common/`,
  `storage_node/` e `client/`.
- **Pessoa A/C:** `controller/logger.py` descarta campos fora do esquema e pode
  lançar `KeyError` para nomes reservados. B isolou seus eventos em
  `common/jsonlog.py`; vale corrigir ou documentar o contrato do logger
  compartilhado.
- **README:** documentar `scripts/dcomp-cli`, `python -m devtools.localcluster`,
  requisitos de `DRIVE_KEY`, variáveis `NODE_ADDRESS`/`DATA_DIRS`, os fluxos de
  cifra e tamanho armazenado, e atualizar o resultado esperado da suíte para
  70 testes nesta entrega. O README não foi alterado por restrição de
  escopo.
