"""
Processamento em streaming dos eventos do Bluesky.

Lê o tópico Kafka, limpa e enriquece os eventos (camada silver) e agrega em
esquema estrela (camada gold) no PostgreSQL, com escrita idempotente.

Autor: Beatriz
Criado em: 23/06/2026
Atualizado em: 08/07/2026
"""

import os
from datetime import date, datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    from_json, col, window, count, to_timestamp, when, lit, coalesce,
    element_at, explode, split, lower, regexp_replace, length, expr, row_number,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType,
)

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/opt/checkpoints")
RETENCAO_DIAS = int(os.environ.get("RETENCAO_DIAS", "1"))

PG_CONN = dict(
    host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
    user=POSTGRES_USER, password=POSTGRES_PASSWORD,
)

spark = (
    SparkSession.builder
    .appName("ProcessamentoBluesky")
    .master("local[*]")
    .config("spark.jars", "/opt/postgresql-jdbc.jar")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Schema do evento publicado pelo coletor.
schema = StructType([
    StructField("tipo", StringType()),
    StructField("did", StringType()),
    StructField("timestamp", DoubleType()),
    StructField("texto", StringType()),
    StructField("langs", ArrayType(StringType())),
    StructField("embed_tipo", StringType()),
])

# Stopwords pt/en/es.
STOPWORDS = {
    "que", "não", "nao", "para", "com", "uma", "por", "mais", "como", "mas",
    "dos", "das", "ele", "ela", "isso", "está", "esta", "são", "sao", "foi",
    "tem", "muito", "você", "voce", "meu", "minha", "sem", "pra",
    "and", "for", "you", "this", "that", "with", "have", "are", "was", "but",
    "not", "your", "all", "can", "out", "just", "they", "what", "when", "from",
    "his", "her", "its", "our", "who", "will", "one", "get", "got", "like", "the",
    "los", "las", "una", "del", "pero", "más", "son", "muy", "este", "porque",
}

# bronze: leitura do Kafka
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# silver: limpeza + enriquecimento (idioma e tipo_conteudo derivados)
df_silver = (
    df_raw
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_ts", to_timestamp(col("timestamp")))
    # validação: descarta eventos sem campos obrigatórios
    .filter(col("tipo").isNotNull() & col("did").isNotNull() & col("event_ts").isNotNull())
    .withColumn(
        "idioma",
        when(col("tipo") != "post", lit("na")).otherwise(
            coalesce(
                lower(split(element_at(col("langs"), 1), "-").getItem(0)),
                lit("und"),
            )
        ),
    )
    .withColumn(
        "tipo_conteudo",
        when(col("tipo") != "post", lit("na"))
        .when(col("embed_tipo").isNull(), lit("txt"))
        .when(col("embed_tipo").contains("images"), lit("img"))
        .when(col("embed_tipo").contains("video"), lit("vid"))
        .when(col("embed_tipo").contains("external"), lit("link"))
        .when(col("embed_tipo").contains("record"), lit("cit"))
        .otherwise(lit("txt")),
    )
    .select(
        col("event_ts"),
        col("tipo").alias("tipo_evento"),
        col("did").alias("autor_did"),
        col("idioma"),
        col("tipo_conteudo"),
        # Postgres não aceita 0x00 em texto UTF-8; remove antes de gravar.
        regexp_replace(col("texto"), "\\x00", "").alias("texto"),
    )
)

# silver guarda só posts; os demais tipos só contam na gold
df_silver_posts = df_silver.filter(col("tipo_evento") == "post")

# gold: fato principal — grão: janela 30s × tipo_evento × idioma × tipo_conteudo
fato_eventos = (
    df_silver
    .withWatermark("event_ts", "1 minute")
    .groupBy(
        window(col("event_ts"), "30 seconds"),
        col("tipo_evento"),
        col("idioma"),
        col("tipo_conteudo"),
    )
    .agg(count("*").alias("qtd"))
    .select(
        col("window.start").alias("janela_inicio"),
        col("window.end").alias("janela_fim"),
        col("tipo_evento"),
        col("idioma"),
        col("tipo_conteudo"),
        col("qtd"),
    )
)

# base dos fatos de termo: posts com texto
df_posts = df_silver.filter((col("tipo_evento") == "post") & col("texto").isNotNull())


def agregar_termos(df_termos, coluna):
    """Conta termos por janela de 30s × idioma.

    Parâmetros:
        df_termos: DataFrame com a coluna de termo já extraída.
        coluna (str): nome da coluna de termo (palavra, hashtag ou dominio).
    Retorna:
        DataFrame agregado com janela_inicio, janela_fim, idioma, <coluna>, qtd.
    """
    return (
        df_termos
        .withWatermark("event_ts", "1 minute")
        .groupBy(window(col("event_ts"), "30 seconds"), col("idioma"), col(coluna))
        .agg(count("*").alias("qtd"))
        .select(
            col("window.start").alias("janela_inicio"),
            col("window.end").alias("janela_fim"),
            col("idioma"),
            col(coluna),
            col("qtd"),
        )
    )


# palavras: tokeniza e filtra (links, curtas, stopwords)
palavras = (
    df_posts
    .withColumn("palavra_raw", explode(split(lower(col("texto")), "\\s+")))
    .filter(~col("palavra_raw").contains("http"))
    .withColumn("palavra", regexp_replace(col("palavra_raw"), "[^\\p{L}]", ""))
    .filter(length(col("palavra")) >= 3)
    .filter(~col("palavra").isin(*STOPWORDS))
)
fato_palavra = agregar_termos(palavras, "palavra")

# hashtags
hashtags = (
    df_posts
    .withColumn(
        "hashtag",
        explode(expr("regexp_extract_all(lower(texto), '#([\\\\p{L}0-9_]+)', 1)")),
    )
    .filter(length(col("hashtag")) >= 2)
)
fato_hashtag = agregar_termos(hashtags, "hashtag")

# domínios dos links
dominios = (
    df_posts
    .withColumn(
        "dominio",
        explode(expr("regexp_extract_all(lower(texto), 'https?://([^/\\\\s]+)', 1)")),
    )
    .filter(length(col("dominio")) >= 3)
)
fato_dominio = agregar_termos(dominios, "dominio")


_ultima_manutencao = None

# Tabelas particionadas por dia (mesma retenção da silver).
PARTICIONADAS = [("silver", "eventos"), ("gold", "fato_palavra")]


def manter_particoes():
    """Cria as partições de ontem/hoje e dropa as mais antigas que a retenção."""
    hoje = date.today()
    corte = hoje - timedelta(days=RETENCAO_DIAS)
    conn = psycopg2.connect(**PG_CONN)
    try:
        with conn, conn.cursor() as cur:
            for schema, tabela in PARTICIONADAS:
                for delta in (1, 0):
                    dia = hoje - timedelta(days=delta)
                    cur.execute(
                        f"CREATE TABLE IF NOT EXISTS {schema}.{tabela}_{dia:%Y%m%d} "
                        f"PARTITION OF {schema}.{tabela} "
                        f"FOR VALUES FROM ('{dia}') TO ('{dia + timedelta(days=1)}')"
                    )
                cur.execute(
                    "SELECT c.relname FROM pg_inherits i "
                    "JOIN pg_class c ON c.oid = i.inhrelid "
                    "JOIN pg_class p ON p.oid = i.inhparent "
                    "JOIN pg_namespace n ON n.oid = p.relnamespace "
                    "WHERE n.nspname = %s AND p.relname = %s "
                    "AND c.relname ~ %s",
                    (schema, tabela, f"^{tabela}_[0-9]{{8}}$"),
                )
                for (relname,) in cur.fetchall():
                    if datetime.strptime(relname[-8:], "%Y%m%d").date() < corte:
                        cur.execute(f"DROP TABLE IF EXISTS {schema}.{relname}")
    finally:
        conn.close()


def gravar_silver(batch_df, batch_id):
    """Handler de foreachBatch: grava os posts do lote na silver (append via JDBC).

    Roda a manutenção de partições/retenção uma vez por dia.

    Parâmetros:
        batch_df: DataFrame do micro-batch (posts limpos).
        batch_id: identificador do lote fornecido pelo Spark.
    """
    global _ultima_manutencao
    if batch_df.isEmpty():
        return

    if _ultima_manutencao != date.today():
        manter_particoes()
        _ultima_manutencao = date.today()

    print(f"[silver.eventos batch {batch_id}] append {batch_df.count()} posts", flush=True)
    (
        batch_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "silver.eventos")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def fazer_upsert(tabela, colunas, chave, top_n=None):
    """Cria um handler de foreachBatch que faz UPSERT no Postgres (ON CONFLICT).

    Parâmetros:
        tabela (str): destino no formato schema.tabela.
        colunas (list[str]): colunas a inserir.
        chave (list[str]): colunas do ON CONFLICT.
        top_n (int | None): se definido, mantém só os N maiores por janela/idioma.
    Retorna:
        função (batch_df, batch_id) para usar no foreachBatch.
    """
    cols_sql = ", ".join(colunas)
    chave_sql = ", ".join(chave)
    set_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in colunas if c not in chave)
    sql = (
        f"INSERT INTO {tabela} ({cols_sql}) VALUES %s "
        f"ON CONFLICT ({chave_sql}) DO UPDATE SET {set_sql}"
    )

    def gravar(batch_df, batch_id):
        """Aplica o top_n (se houver) e faz o UPSERT do lote no Postgres."""
        if top_n:
            ranking = Window.partitionBy("janela_inicio", "idioma").orderBy(col("qtd").desc())
            batch_df = (
                batch_df.withColumn("_rk", row_number().over(ranking))
                .filter(col("_rk") <= top_n)
                .drop("_rk")
            )
        linhas = [tuple(row[c] for c in colunas) for row in batch_df.collect()]
        if not linhas:
            return

        print(f"[{tabela} batch {batch_id}] upsert {len(linhas)} linhas", flush=True)

        conn = psycopg2.connect(**PG_CONN)
        try:
            with conn, conn.cursor() as cur:
                execute_values(cur, sql, linhas)
        finally:
            conn.close()

    return gravar


# escrita das camadas: (df, checkpoint, outputMode, handler)
COLS_EVENTOS = ["janela_inicio", "janela_fim", "tipo_evento", "idioma", "tipo_conteudo", "qtd"]
saidas = [
    (df_silver_posts, "silver_eventos", "append", gravar_silver),
    (fato_eventos, "fato_eventos", "update",
     fazer_upsert("gold.fato_eventos", COLS_EVENTOS,
                  ["janela_inicio", "tipo_evento", "idioma", "tipo_conteudo"])),
    (fato_palavra, "fato_palavra", "append",
     fazer_upsert("gold.fato_palavra", ["janela_inicio", "janela_fim", "idioma", "palavra", "qtd"],
                  ["janela_inicio", "idioma", "palavra"], top_n=50)),
    (fato_hashtag, "fato_hashtag", "update",
     fazer_upsert("gold.fato_hashtag", ["janela_inicio", "janela_fim", "idioma", "hashtag", "qtd"],
                  ["janela_inicio", "idioma", "hashtag"])),
    (fato_dominio, "fato_dominio", "update",
     fazer_upsert("gold.fato_dominio", ["janela_inicio", "janela_fim", "idioma", "dominio", "qtd"],
                  ["janela_inicio", "idioma", "dominio"])),
]

for df_saida, nome, modo, handler in saidas:
    (
        df_saida.writeStream
        .foreachBatch(handler)
        .outputMode(modo)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{nome}")
        .trigger(processingTime="15 seconds")
        .start()
    )

spark.streams.awaitAnyTermination()
