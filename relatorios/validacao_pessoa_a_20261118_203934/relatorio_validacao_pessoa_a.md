# Relatório automatizado — DComp Drive (Pessoa A)

**Início (UTC):** 2026-11-18T23:39:34.639876+00:00  
**Repositório local:** `/home/raiz/GitHub/dcomp-drive`  
**Branch / commit:** `fix/versionamento-leases` / `4778ac1393a1b92cccbdf0998aef24da2e1cae4c`  
**Python:** 3.14.8 — Linux-7.2.13-100.fc43.x86_64-x86_64-with-glibc2.42  
**Isolamento:** controlador próprio em porta livre e banco SQLite temporário; não alterou o banco local existente.

## Resultado resumido

- **Testes HTTP:** 16 aprovados, 0 reprovados, 0 não executados.
- **Suíte pytest:** PASSOU — pytest terminou com código 0. {"errors": 0, "failures": 0, "skipped": 0, "tests": 28}

## Casos avaliados

| ID | Teste | Requisito | Esperado | Resultado |
|---|---|---|---|---|
| 01 | Health e configuração | API / N / W | HTTP 200; ok=true, N=3, W=2 | **PASSOU** |
| 02 | Criar/listar/remover diretório | RF1 | 201, 200, 201, 409, 200 | **PASSOU** |
| 03 | Registrar nós e listar cluster | RF6 / RF7 | 2 nós alive e 3 discos | **PASSOU** |
| 04 | Alocação com fallback em 2 máquinas | RF3 / RF7 | 200; 3 discos sem repetição | **PASSOU** |
| 05 | Bloqueio de escrita e leitura pendente | RF8 | 409 e 409 | **PASSOU** |
| 06 | Commit íntegro com W e N | RF3 | 200; intact | **PASSOU** |
| 07 | Stat/locate da versão confirmada | RF1 / RF8 | 200; versão 1; 3 réplicas | **PASSOU** |
| 08 | Sobrescrita mantém versão anterior | RF8 | 200; leitura da versão 1 | **PASSOU** |
| 09 | Quórum insuficiente e rollback | RF3 / RF8 | 507; versão anterior mantida | **PASSOU** |
| 10 | Commit sub-replicado com W=2 | RF3 | 200; under_replicated | **PASSOU** |
| 11 | N por arquivo e validação N<W | RF3 | 200 N=2; 422 N=1 | **PASSOU** |
| 12 | Expiração e limpeza de lease | RF8 | 404 após worker; fila com 3 itens | **PASSOU** |
| 13 | Duas alocações simultâneas | RF8 | um 200 e outro 409 | **PASSOU** |
| 14 | Rejeição de commit inválido | RF3 / RF8 | 422 + 422 + 200 | **PASSOU** |
| 15 | Mover e excluir arquivo | RF1 | 200; original 404; destino 200; delete | **PASSOU** |
| 16 | Persistência SQLite entre conexões | RF10 | versão 2 na nova conexão | **PASSOU** |

## Evidências e observações

### 01 — Health e configuração (PASSOU)
- Esperado: HTTP 200; ok=true, N=3, W=2
- Obtido: Condições verificadas | HTTP: 200
- Detalhe: Servidor ativo; N=3, W=2.

### 02 — Criar/listar/remover diretório (PASSOU)
- Esperado: 201, 200, 201, 409, 200
- Obtido: Condições verificadas | HTTP: 201, 200, 201, 409, 200
- Detalhe: Criar, listar, impedir exclusão de pasta não vazia e remover pasta vazia.

### 03 — Registrar nós e listar cluster (PASSOU)
- Esperado: 2 nós alive e 3 discos
- Obtido: Condições verificadas | HTTP: 200, 200, 200
- Detalhe: 2 máquinas e 3 discos simulados, sem transferir dados reais.

### 04 — Alocação com fallback em 2 máquinas (PASSOU)
- Esperado: 200; 3 discos sem repetição
- Obtido: Condições verificadas | HTTP: 200
- Detalhe: 3 réplicas em discos distintos, usando 2 máquinas (fallback correto).

### 05 — Bloqueio de escrita e leitura pendente (PASSOU)
- Esperado: 409 e 409
- Obtido: Condições verificadas | HTTP: 409, 409
- Detalhe: Segundo escritor bloqueado; arquivo novo pendente não é legível.

### 06 — Commit íntegro com W e N (PASSOU)
- Esperado: 200; intact
- Obtido: Condições verificadas | HTTP: 200
- Detalhe: 3 réplicas declaradas/confirmadas nos metadados (simulação, sem transferência).

### 07 — Stat/locate da versão confirmada (PASSOU)
- Esperado: 200; versão 1; 3 réplicas
- Obtido: Condições verificadas | HTTP: 200, 200
- Detalhe: Stat e locate mostram versão 1, metadados e três réplicas ok.

### 08 — Sobrescrita mantém versão anterior (PASSOU)
- Esperado: 200; leitura da versão 1
- Obtido: Condições verificadas | HTTP: 200, 200, 200
- Detalhe: Sobrescrita isolada em staging; locate e stat continuam vendo versão 1.

### 09 — Quórum insuficiente e rollback (PASSOU)
- Esperado: 507; versão anterior mantida
- Obtido: Condições verificadas | HTTP: 507, 200
- Detalhe: W=2 não atingido com 1 disco; versão anterior preservada.

### 10 — Commit sub-replicado com W=2 (PASSOU)
- Esperado: 200; under_replicated
- Obtido: Condições verificadas | HTTP: 200, 200, 200, 200
- Detalhe: W=2 atingido mas N=3 incompleto; versão 2 under_replicated.

### 11 — N por arquivo e validação N<W (PASSOU)
- Esperado: 200 N=2; 422 N=1
- Obtido: Condições verificadas | HTTP: 200, 200, 200, 422
- Detalhe: N=2 por arquivo confirmado e persistido; N=1 < W=2 rejeitado.

### 12 — Expiração e limpeza de lease (PASSOU)
- Esperado: 404 após worker; fila com 3 itens
- Obtido: Condições verificadas | HTTP: 200, 200, 200, 200, 404
- Detalhe: Worker removeu lease e staging; 3 exclusões físicas aguardam nós reais.

### 13 — Duas alocações simultâneas (PASSOU)
- Esperado: um 200 e outro 409
- Obtido: Condições verificadas | HTTP: 200, 409
- Detalhe: 2 requisições reais por threads; uma aceita e outra rejeitada.

### 14 — Rejeição de commit inválido (PASSOU)
- Esperado: 422 + 422 + 200
- Obtido: Condições verificadas | HTTP: 200, 422, 422, 200
- Detalhe: Discos falsos/duplicados e chunk antigo rejeitados; commit válido aceito.

### 15 — Mover e excluir arquivo (PASSOU)
- Esperado: 200; original 404; destino 200; delete
- Obtido: Condições verificadas | HTTP: 200, 404, 200, 200, 404
- Detalhe: Move, consulta no destino e delete de metadados concluídos.

### 16 — Persistência SQLite entre conexões (PASSOU)
- Esperado: versão 2 na nova conexão
- Obtido: Condições verificadas
- Detalhe: Nova conexão SQLite recuperou metadados da versão 2.

## Limitações e escopo

- Os nós `maquina-a` e `maquina-b` são simulados apenas no cadastro. Os testes verificam o **plano de controle/metadados**, não a gravação dos bytes.
- Os hashes como `versao-1` são marcadores de teste, **não hashes SHA-256**.
- Não comprova scrubbing, cópias reais, integridade física, recuperação entre máquinas ou operação do cliente/CLI. Estes exigem componentes das Pessoas B/C.
- O teste de persistência lê o mesmo SQLite por uma nova conexão; não simula falha elétrica.
- O teste de expiração ajusta a coluna `expires_at` apenas no banco temporário.

## Arquivos de evidência

- `resultados.json`: requisições/respostas e asserções por cenário.
- `servidor_teste.log`: logs da instância temporária.
- `pytest_saida.txt` e `pytest_junit.xml`: resultados da suíte existente, quando disponível.

## Conclusão

**Os cenários automatizados do controlador foram aprovados neste ambiente.** Ainda é necessário demonstrar integração real com nós e cliente para concluir os requisitos distribuídos da disciplina.
