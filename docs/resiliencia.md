# Resiliência e operação

Cada nó registra seu endereço e seus discos no controlador ao subir. Depois
envia o uso dos discos em heartbeats. Se o controlador pedir um novo registro,
o agente repete a operação. Os discos do Compose usam volumes próprios com
identificadores no formato `node1/disk1`.

O controlador consulta os últimos sinais a cada segundo. Com os valores padrão,
um nó fica suspeito a partir de 6 s sem sinal e morto a partir de 10 s. A atualização
do nó morto e de suas réplicas é feita na mesma transação. Um disco que passa a
reportar capacidade zero deixa de receber gravações e perde as réplicas do mapa
de leitura até que elas sejam recuperadas.

A recuperação consulta os metadados por ordem de menor número de fontes. O
controlador reaproveita a regra de posicionamento para escolher um disco com
espaço num nó que ainda não tem uma cópia. Se faltarem máquinas, pode usar outro disco
de um nó que já tem uma cópia. O controlador solicita a transferência diretamente
ao nó de origem. Apenas após a confirmação do destino ele publica a nova
réplica. Há até duas cópias simultâneas na configuração padrão e uma por
arquivo de cada vez. Se não houver destino, o controlador aguarda recursos.
No drain, o nó continua atendendo leituras enquanto suas cópias são migradas.

Quando um nó morto se registra de volta, o controlador consulta o inventário.
Os chunks que podem ser reaproveitados são lidos e têm o SHA-256 comparado
com os metadados. As cópias obsoletas entram na fila de exclusão.

O ambiente inicial tem um controlador e cinco nós simulados por contêineres.
Volumes no mesmo host compartilham seu disco físico e não protegem contra
falha desse host. O controlador único é outro ponto de falha e não há garantia
de recuperação se todas as cópias de um chunk forem perdidas. O número de
arquivos e a vazão suportados dependem dos testes de desempenho da equipe.
