# Demonstração da Parte 1

Use o ambiente de teste do Compose. O roteiro cria um arquivo próprio e altera
uma única réplica desse arquivo. Rode os comandos na raiz do repositório,
em um terminal Bash. A configuração padrão usa três réplicas, quórum dois e
cifra desligada.

## 1. Subir o ambiente e testar gravação e leitura

```bash
docker compose --profile storage up --build -d
curl -fsS http://localhost:8000/status | python3 -m json.tool
```

Aguarde os cinco nós aparecerem como `alive`. O controlador pode ficar pronto
antes dos primeiros registros dos nós.

```bash
compose=(docker compose --profile storage)
dcomp() {
  "${compose[@]}" exec -T controller python -m client \
    --controller http://controller:8000 "$@"
}
demo_dir=$(mktemp -d ./.dcomp-demo.XXXXXX)
remote="/demo-${demo_dir##*.}.txt"
printf 'DComp Drive: teste de integridade\n' > "$demo_dir/original.txt"

dcomp put "$remote" "$demo_dir/original.txt"
dcomp stat "$remote"
dcomp get "$remote" "$demo_dir/baixado.txt"
sha256sum "$demo_dir/original.txt" "$demo_dir/baixado.txt"
cmp "$demo_dir/original.txt" "$demo_dir/baixado.txt"
```

Mostre as três réplicas no `stat` e os hashes iguais após o download.

## 2. Derrubar um nó e recuperar a replicação

```bash
set -o pipefail
bash scripts/test-recovery.sh 2>&1 | tee "$demo_dir/recuperacao.log"
dcomp status
```

O teste grava outro arquivo de amostra, para um nó que possui uma réplica,
aguarda o restabelecimento de três cópias e confere o SHA-256 de cada uma.
Ele religa o nó ao sair. Se o `status` ainda mostrar o nó como morto, aguarde
o novo registro e rode `dcomp status` novamente até os cinco voltarem a `alive`.

`detection_time_s` conta do último heartbeat até o nó ser declarado morto.
`recovery_time_s` conta a espera após essa declaração. Quando a cópia termina
enquanto o nó ainda está suspeito, essa espera é zero.

## 3. Detectar uma cópia corrompida e ler o arquivo íntegro

Use o arquivo do passo 1. Descubra o identificador do chunk e uma réplica
ativa pelo controlador.

```bash
read -r node disk chunk < <(
  curl -fsS --get --data-urlencode "path=$remote" \
    http://localhost:8000/files/locate |
    python3 -c 'import json, sys
c = json.load(sys.stdin)["chunks"][0]
node, disk = c["replicas"][0]["disk_id"].split("/", 1)
print(node, disk, c["chunk_id"])'
)
printf 'Réplica da demonstração: %s/%s, chunk %s\n' "$node" "$disk" "$chunk"
```

Altere um byte e tente ler diretamente a réplica selecionada. A leitura
deve retornar HTTP 422 se o scrubbing ainda não tiver detectado a corrupção.

```bash
bash scripts/fault.sh corrupt "$node" "$disk" "$chunk"
"${compose[@]}" exec -T controller python -c 'import sys, httpx
node, disk, chunk = sys.argv[1:]
r = httpx.get(f"http://{node}:9000/chunks/{chunk}",
              params={"disk_id": f"{node}/{disk}"})
print(f"HTTP {r.status_code}: {r.text}")' "$node" "$disk" "$chunk"

dcomp get "$remote" "$demo_dir/apos-corrupcao.txt"
sha256sum "$demo_dir/original.txt" "$demo_dir/apos-corrupcao.txt"
cmp "$demo_dir/original.txt" "$demo_dir/apos-corrupcao.txt"
dcomp stat "$remote"
"${compose[@]}" logs --since 2m "$node" | grep -E 'chunk_corrupt_detected|scrub_corrupt_detected|corrupt_reported'
```

Se o scrubbing já tiver descartado a réplica, o acesso direto pode retornar
404. Nesse caso, mostre o evento de corrupção nos logs, a leitura íntegra
pelo CLI e o `stat` quando o controlador voltar a mostrar três cópias.

Os arquivos de amostra e o log ficam em `$demo_dir`. Para a apresentação,
mostre esses resultados na ordem do roteiro. No vídeo, corte a espera da
subida dos contêineres e da detecção para ficar dentro de cinco minutos.
