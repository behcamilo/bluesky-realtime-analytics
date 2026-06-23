# Interfaces entre os componentes

Como os componentes do pipeline se comunicam e qual o formato dos dados trocados em cada
ligação.

```
Jetstream -> Coletor -> Kafka -> Spark -> Postgres -> Grafana
```

Em cada ligação a interface é o canal de comunicação e o formato dos dados (a mensagem
ou o esquema da tabela). O Kafka deixa o coletor e o Spark independentes: um não precisa
conhecer o outro nem estar rodando ao mesmo tempo.

## Classificação das ligações

| Ligação | Paradigma | Sincronismo | Acoplamento |
|---|---|---|---|
| Jetstream → Coletor | fluxo (stream) | assíncrono | serviço externo |
| Coletor → Kafka | publish-subscribe | assíncrono | desacoplado (espaço + tempo) |
| Kafka → Spark | publish-subscribe | assíncrono | desacoplado |
| Spark → Postgres | request-reply | síncrono | acoplado |
| Postgres → Grafana | request-reply | síncrono | acoplado |

## 1. Jetstream para Coletor
Conexão WebSocket (WSS) em `wss://.../subscribe`, filtrando as coleções de interesse
(post, like, repost, follow). O Jetstream envia eventos JSON continuamente
(`kind=commit`, com `commit.collection` e `commit.record`). Se a conexão cai, o coletor
reconecta sozinho.

## 2. Coletor para Kafka
O coletor publica no tópico `bluesky-events` (protocolo Kafka, broker `kafka:9092`).
Comunicação assíncrona: o coletor não conhece quem vai consumir. Cada mensagem é um JSON:

| Campo | Tipo | Obrigatório | Observação |
|---|---|---|---|
| `tipo` | string | sim | post / like / repost / follow |
| `did` | string | sim | autor do evento |
| `timestamp` | number | sim | epoch em segundos |
| `texto` | string | não | só posts |
| `langs` | array | não | só posts |
| `embed_tipo` | string | não | só posts |

## 3. Kafka para Spark
O Spark se inscreve no mesmo tópico e processa as mensagens em lotes a cada 15s
(micro-batches), no mesmo formato da etapa 2. Cada mensagem tem um número de ordem
(offset) que o Spark utiliza para guardar no checkpoint até onde já leu, portanto, se cair e
voltar, retoma daquele ponto, sem perder nem repetir mensagens.

## 4. Spark para Postgres
Escrita via JDBC (`postgres:5432`), nos schemas `silver` e `gold`. O contrato é o esquema
das tabelas (`infra/postgres/init.sql`):
- `silver.eventos`: append (posts limpos).
- `gold.fato_eventos` e os fatos de termo (palavra, hashtag, dominio): upsert, então
  reprocessar não duplica.

## 5. Postgres para Grafana
O Grafana consulta por SQL. A interface de leitura expõe a camada `gold` (painéis
agregados) e a `silver` (painel de detalhe, post a post):

| Tabela / objeto | Conteúdo |
|---|---|
| `fato_eventos` | contagem por janela de tempo × tipo × idioma × tipo de conteúdo |
| `fato_palavra` / `fato_hashtag` / `fato_dominio` | top termos por janela de tempo × idioma |
| `dim_tipo_evento` / `dim_idioma` / `dim_tipo_conteudo` | tabela de tipos, código → descrição |
| `dim_tempo` (view) | atributos de tempo (hora, dia, período) |
| `silver.eventos` | posts individuais (texto, idioma, conteúdo) |

## Observações
Como cada ligação tem um contrato definido, dá para trocar a implementação de um
componente sem afetar os outros (por exemplo, trocar o motor de processamento mantendo o
tópico e as tabelas). O acoplamento fraco vem do Kafka e a tolerância a falhas aparece na
reconexão do coletor, nos offsets do Spark e na escrita idempotente no banco.
