# Relatório técnico — Pessoa A (controlador e metadados)

## Escopo

Implementação de RF1, RF3, RF7, RF8 e RF10 no controlador FastAPI, usando SQLite
em WAL. Os endpoints mantidos são `/health`, `/files/allocate`, `/files/commit`,
`/files/locate`, `/files/stat`, `/files`, `/files/move`, `/dirs` e `/chunks/corrupt`.

## Alterações

1. **Versionamento seguro**: introduzidas as tabelas `staged_chunks` e
   `staged_replicas`. A alocação não modifica metadados já commitados, permitindo
   leitura da versão anterior durante uma nova escrita. O commit usa transação
   SQLite única para validar o lease e promover a nova versão.
2. **Quórum**: validação global dos chunks e dos discos recebidos, prevenindo
   confirmações inventadas, duplicatas, chunks de outro arquivo e uploads
   incompletos. Falha de W descarta somente o staging da escrita corrente.
3. **Concorrência**: o `BEGIN IMMEDIATE` serializa escritores. Leituras utilizam
   snapshots transacionais para não misturar versões de entry e chunks.
4. **Expiração e lixo físico**: task própria do controlador expira leases,
   cancela staging e agenda deleções em fila SQLite durável, com retentativas;
   blocos antigos só são excluídos após breve período de retenção.
5. **Alocação**: primeiro tenta discos em máquinas diferentes. Caso não seja
   possível preencher N, usa discos adicionais da mesma máquina, sem repetir
   disco. Discos acima do limite e nós fora do estado alive são ignorados.
6. **N por arquivo (opcional)**: argumento `replication_factor` em allocate,
   registrado em `entries` e usado na lista de chunks sub-replicados.
7. **Testes**: `tests/test_pessoa_a_regressoes.py` exercita overwrite,
   rollback de quórum, validação de discos, expiração e concorrência real.

## Consistência e limitações

- Consistência de metadados confirmados por arquivo. O cliente recebe novo
  estado somente após quorum W por chunk; a transferência real de bytes não
  passa pelo controlador.
- O controlador assume honestidade das confirmações do cliente e não comprova
  remotamente o hash: as verificações efetivas cabem aos nós e ao cliente.
- O sistema tem controlador único com persistência local; não há eleição de
  líder nem tolerância a falha simultânea do controlador.
- RF4/RF5/RF6 e integração de upload/download permanecem dependentes dos
  componentes das Pessoas B e C. O housekeeping de A não substitui o monitor
  de falhas nem o worker de re-replicação de C.
- A garantia de leitura entre cliente e nós depende da retenção de chunks
  antigos e do tempo da transferência. O período padrão de 60 segundos não é
  garantia para downloads que durem mais de 60 segundos; uma solução robusta
  exige pin de leitura/retenção por versão.
- Instâncias múltiplas do controlador devem compartilhar o mesmo SQLite e
  coordenar workers de limpeza, ou executar uma instância única.

## Evidências de teste

Executar `python -m pytest -q tests/test_controller.py tests/test_pessoa_a_regressoes.py`
e anexar resultado, logs JSON, data, ambiente e commit do Git. Não registrar
"testes passaram" sem execução real. A migração não reconstrói backups de
alocações incompletas criadas pela versão antiga do código.
