# Camada Publish

Views de consumo final, divididas em duas sub-camadas de propósito distinto —
não é duplicação de lógica, é atendimento a dois tipos de consumidor diferentes.

## `agregadas/`

5 views já calculadas (`GROUP BY` fechado), respondendo diretamente a uma
pergunta de negócio. Uso: consulta SQL direta, sem necessidade de montar
relacionamento (analista rodando `SELECT * FROM vw_receita_mensal`, script
simples, ferramenta que só lê tabela plana).

Não têm chave de relacionamento entre si — cada uma é uma "resposta pronta"
isolada.

## `granulares/`

7 views não agregadas, espelhando a estrutura de fato/dimensão da camada Gold
(mesmas chaves `sk_*`), sem duplicar a lógica de negócio nelas — apenas expondo
as tabelas físicas através de `publish`, preservando o princípio de governança
(consumidor de BI nunca acessa `gold` diretamente).

Uso: modelo relacional para ferramentas de BI (Power BI), onde a agregação
acontece dinamicamente via medida DAX, com cross-filtering real entre visuais.

**Nota:** `vw_dim_cliente.sql` faz parte deste conjunto (4 dimensões + 3 fatos =
7 views granulares) — se ausente nesta pasta, falta ser adicionado.

## Por que essa separação, não só um schema com 12 views misturadas

Documentado em detalhe em `docs/logs/log-sessoes.md`, seção 28.2. Resumo: a
tentativa inicial de conectar o Power BI só nas 5 views agregadas revelou que
elas não se relacionam entre si (sem chave compartilhada, sem granularidade
comum) — impossibilitando cross-filtering. A alternativa mais simples técnica
teria sido conectar o Power BI direto em `gold`, mas isso furaria o princípio de
governança que motivou criar `publish` em primeiro lugar. A solução foi replicar
a estrutura de Star Schema como views não agregadas, dentro do próprio schema
`publish`.
