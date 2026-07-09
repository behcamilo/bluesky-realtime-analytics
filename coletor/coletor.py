"""
Coletor de eventos do Bluesky.

Conecta no Jetstream via WebSocket, normaliza cada evento (post, like, repost,
follow) e publica no tópico Kafka.

Autor: Beatriz
Criado em: 23/06/2026
Atualizado em: 08/07/2026
"""

import os
import json
import asyncio

import websockets
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
JETSTREAM_URL = os.environ["JETSTREAM_URL"]

# Coleções de interesse -> tipo.
COLLECTION_TO_TIPO = {
    "app.bsky.feed.post": "post",
    "app.bsky.feed.like": "like",
    "app.bsky.feed.repost": "repost",
    "app.bsky.graph.follow": "follow",
}

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def delivery_report(err, msg):
    """Callback de entrega do Kafka; registra falha de publicação.

    Parâmetros:
        err: erro de entrega (None se publicou com sucesso).
        msg: mensagem entregue pelo produtor.
    """
    if err is not None:
        print(f"[ERRO] Falha ao publicar: {err}", flush=True)


def montar_url():
    """Monta a URL do Jetstream com as coleções de interesse como query params.

    Retorna:
        str: URL final do WebSocket.
    """
    params = "&".join(f"wantedCollections={c}" for c in COLLECTION_TO_TIPO)
    separador = "&" if "?" in JETSTREAM_URL else "?"
    return f"{JETSTREAM_URL}{separador}{params}"


def processar(raw):
    """Normaliza um evento JSON do Jetstream e publica no Kafka.

    Descarta o que não for um commit de criação de post/like/repost/follow.

    Parâmetros:
        raw: mensagem crua (str/bytes) recebida do WebSocket.
    """
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return

    # só commits de criação
    if msg.get("kind") != "commit":
        return
    commit = msg.get("commit", {})
    if commit.get("operation") != "create":
        return

    tipo = COLLECTION_TO_TIPO.get(commit.get("collection"))
    if tipo is None:
        return

    evento = {
        "tipo": tipo,
        "did": msg.get("did"),
        "timestamp": msg.get("time_us", 0) / 1_000_000,  # us -> s
    }
    # só posts têm texto, idioma e embed
    if tipo == "post":
        record = commit.get("record", {})
        evento["texto"] = record.get("text")
        evento["langs"] = record.get("langs")
        embed = record.get("embed")
        evento["embed_tipo"] = embed.get("$type") if isinstance(embed, dict) else None
    producer.produce(
        KAFKA_TOPIC,
        value=json.dumps(evento).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)


async def consumir():
    """Loop principal: conecta no Jetstream e processa os eventos recebidos.

    Reconecta sozinho caso a conexão WebSocket caia.
    """
    url = montar_url()
    print(f"Coletor iniciado. bootstrap={KAFKA_BOOTSTRAP_SERVERS} topic={KAFKA_TOPIC}", flush=True)
    print(f"Conectando no Jetstream: {url}", flush=True)

    # async-for reconecta sozinho
    async for ws in websockets.connect(url, max_size=None):
        print("[OK] Conectado ao Jetstream.", flush=True)
        try:
            async for raw in ws:
                processar(raw)
        except websockets.ConnectionClosed:
            print("[WARN] Conexão fechada, reconectando...", flush=True)
            continue
        finally:
            producer.flush(5)


if __name__ == "__main__":
    asyncio.run(consumir())
