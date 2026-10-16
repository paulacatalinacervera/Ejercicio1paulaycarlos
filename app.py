from __future__ import annotations

import asyncio
import time
import functools
import os
import random
import sys
import traceback
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import socketio
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pyspark.sql import SparkSession

from state import GameState
from producer import Producer
from consumer import Consumer
from server_producer import ServerProducer, KAFKA_BROKER, KAFKA_TOPIC
from kafka_ingest import KafkaIngest
from redis_store import RedisStore
from models import ACTIONS

PORT = int(os.environ.get("PORT", "3003"))
HERE = Path(__file__).resolve().parent

##FastAPI + Socket.IO app
sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    ping_interval=25,
    ping_timeout=60,
)
app = FastAPI(title="Telemetry & Matchmaking")
app.mount("/static", StaticFiles(directory=str(HERE / "static")), name="static")
asgi_app = socketio.ASGIApp(sio, other_asgi_app=app)

#--- Sesión de Apache Spark ---
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[4]")
spark = (
    SparkSession.builder.appName("TelemetryMatchmaking")
    .master(SPARK_MASTER)
    .config("spark.ui.enabled", "true")
    .config("spark.ui.port", "4040")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "127.0.0.1"))
    .config(
        "spark.driver.bindAddress", os.environ.get("SPARK_DRIVER_BIND", "127.0.0.1")
    )
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
SPARK_VERSION = spark.version
SPARK_APP_ID = spark.sparkContext.applicationId
SPARK_UI_URL = spark.sparkContext.uiWebUrl
SPARK_MASTER_URL = spark.sparkContext.master

#Flujo del pipeline: productor -> consumidor -> Spark -> productor de servidor -> consumidor de servidor
state = GameState()
producer = Producer(state)
server_producer = ServerProducer(state)
consumer = Consumer(state, producer, spark, server_producer=server_producer)
kafka_ingest = KafkaIngest(state)
redis_store = RedisStore()

#--- inicialización ---
producer.set_client_count(40)

throughput_history: list[dict] = []
MAX_HISTORY = 60
recent_feed: list[dict] = []
last_player_emit = 0.0
last_stats_emit = 0.0
last_reconcile = 0.0

def _prepend_feed(feed_dicts: list[dict]) -> None:
    global recent_feed
    recent_feed = (feed_dicts + recent_feed)[:80]

def _make_feed_events(count: int = 12) -> list[dict]:
    players = list(state.players.values())
    if not players:
        return []
    events: list[dict] = []
    for _ in range(max(1, int(count))):
        p = random.choice(players)
        if random.random() < 0.5:
            events.append(
                {
                    "id": f"e_{random.getrandbits(32):x}",
                    "type": "action",
                    "player_name": p.name,
                    "action": random.choice(ACTIONS),
                    "ping": p.ping,
                    "timestamp": time.time() * 1000,
                }
            )
        else:
            victim = random.choice(players) if len(players) > 1 else p
            events.append(
                {
                    "id": f"e_{random.getrandbits(32):x}",
                    "type": "kill",
                    "player_name": p.name,
                    "victim_name": victim.name,
                    "timestamp": time.time() * 1000,
                }
            )
    return events

#
async def _in_thread(fn, *args):
    if hasattr(asyncio, "to_thread"):
        return await asyncio.to_thread(fn, *args)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(fn, *args))


# --- Rutas HTTP ---
@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    html = (HERE / "templates" / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "players": len(state.players),
            "matches": len(state.matches),
            "servers": len(state.servers),
            "alerts": len(state.alerts),
            "pipelineAlive": state.pipeline_alive,
            "lastBatchAt": state.last_batch_at,
        }
    )


@app.get("/spark")
async def spark_status() -> JSONResponse:
    return JSONResponse(
        {
            "engine": "Apache Spark (PySpark)",
            "version": SPARK_VERSION,
            "appName": "TelemetryMatchmaking",
            "applicationId": SPARK_APP_ID,
            "master": SPARK_MASTER_URL,
            "sparkUI": SPARK_UI_URL,
            "batchesProcessed": consumer.batches_processed,
            "lastBatchAt": state.last_batch_at,
            "lastBatchMs": state.last_batch_ms,
            "pipelineAlive": state.pipeline_alive,
            "lastError": consumer.last_error,
            "cheatDetection": "Spark SQL window function lag() over (PARTITION BY player_id ORDER BY timestamp)",
            "matchmaking": "Spark groupBy(region) + row_number() window + driver-side spread check",
            "serverAnomalyDetection": "Spark SQL window function lag() over (PARTITION BY server_id ORDER BY timestamp) for CPU spikes / packet loss",
        }
    )


@app.get("/kafka")
async def kafka_status() -> JSONResponse:
    return JSONResponse(
        {
            "transport": "Kafka (game server telemetry)",
            "bootstrapServers": KAFKA_BROKER,
            "topic": KAFKA_TOPIC,
            "producerOk": server_producer.kafka_ok,
            "producerFallback": server_producer.fallback,
            "producerRunning": server_producer.is_running,
            "produced": server_producer.produced,
            "ingestAlive": kafka_ingest.alive,
            "ingested": kafka_ingest.ingested,
            "lastMessageAt": kafka_ingest.last_message_at,
            "ingestError": kafka_ingest.error,
            "producerError": server_producer.kafka_error,
            "servers": len(state.servers),
        }
    )


@app.get("/redis")
async def redis_status() -> JSONResponse:
    return JSONResponse(redis_store.summary())


# --- bucle principal de consumo (micro-lotes de 1 s) ---
async def _pipeline_tick(
    batch: list, server_batch: list, ended_matches: list, now: float
) -> None:
    """One micro-batch cycle. Never raises: a bad batch must not kill the loop."""
    # 0. periodic reconcile so the player pool tracks the target count
    global last_reconcile
    if now - last_reconcile > 5000:
        last_reconcile = now
        producer._reconcile()

    # 1. vaciar los eventos generados + reducir (de bajo costo, permanece en el bucle)
    feed_events = consumer.reduce_batch(batch)
    position_events = [e for e in batch if e.type == "position"]
    if position_events:
        # 2. Detección de trampas en SPARK. La tarea se bloquea durante unos cientos de milisegundos, por lo que
        #    se ejecuta en un hilo de trabajo mediante `to_thread`; ejecutarla de forma síncrona (en el mismo hilo) bloqueaba
        #    todo el bucle de eventos (las peticiones HTTP `/health` y socket.io dejaban de responder).
        try:
            new_alerts = await _in_thread(
                consumer._detect_cheats_spark, position_events
            )
        except Exception:  # noqa: BLE001
            consumer.last_error = traceback.format_exc()
            print(f"[pipeline] cheat detection failed:\n{consumer.last_error}", flush=True)
            new_alerts = []
    else:
        new_alerts = []

    if feed_events:
        feed_dicts = [e.to_dict() for e in feed_events]
        _prepend_feed(feed_dicts)
        await sio.emit("telemetry:feed", feed_dicts)
    for alert in new_alerts:
        await sio.emit("cheat:alert", alert.to_dict())

    # 2-Telemetría del servidor (respaldada por Kafka): reduce + detección de anomalías con Spark
    consumer.reduce_server_batch(server_batch)
    if server_batch:
        try:
            server_alerts = await _in_thread(
                consumer._detect_server_anomalies_spark, server_batch
            )
        except Exception:  # noqa: BLE001
            consumer.last_error = traceback.format_exc()
            print(f"[pipeline] server anomaly detection failed:\n{consumer.last_error}", flush=True)
            server_alerts = []
    else:
        server_alerts = []
    for alert in server_alerts:
        await sio.emit("server:alert", alert.to_dict())

    # 3. Emparejamiento (matchmaking): `groupBy` + `window` de Spark fuera del bucle, seguidos de la mutación;
    #    esta última se ejecuta parcialmente en el hilo del bucle (ya que invoca a `producer.on_player_enter_match()`,
    #    lo cual requiere que el bucle esté en ejecución).
    try:
        ranked_regions = await _in_thread(consumer.rank_queue_spark)
        new_matches = consumer.form_matches(ranked_regions)
    except Exception:  # noqa: BLE001
        consumer.last_error = traceback.format_exc()
        print(f"[pipeline] matchmaking failed:\n{consumer.last_error}", flush=True)
        new_matches = []
    for match in new_matches:
        await sio.emit("matchmaking:match-formed", match.to_dict())

    # 4. Eliminar los enfrentamientos finalizados y notificar al productor para que detenga sus temporizadores.
    for m in ended_matches:
        ended_players = [state.players[pid] for pid in m.players if pid in state.players]
        consumer.on_match_ended(m, ended_players)


async def pipeline_loop() -> None:
    while True:
        try:
            batch = producer.drain_events()
            server_batch = state.drain_server_events()
            now = time.time() * 1000
            ended = state.prune_matches()
            await _pipeline_tick(batch, server_batch, ended, now)

            # periodic broadcasts
            await _broadcasts(now)

            # persistir el estado en tiempo real en Redis (con limitación de tasa dentro del almacén)
            await _in_thread(redis_store.sync, state)

            # indicador de estado de salud (determina la etiqueta «activo» y el endpoint /health)
            state.pipeline_alive = True
            state.last_batch_at = time.time() * 1000
            state.last_batch_ms = state.last_batch_at - now
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            state.pipeline_alive = False
            consumer.last_error = traceback.format_exc()
            print(f"[pipeline] tick failed:\n{consumer.last_error}", flush=True)
        await asyncio.sleep(1.0)


async def _broadcasts(now: float) -> None:
    global last_player_emit, last_stats_emit
    if now - last_player_emit > 1500:
        last_player_emit = now
        players = [p.to_dict() for p in state.players.values()]
        await sio.emit("players:update", players)
        await sio.emit("servers:update", [s.to_dict() for s in state.servers.values()])

    if now - last_stats_emit > 1000:
        last_stats_emit = now
        stats = state.stats(throughput_history=throughput_history)
        throughput_history.append({"t": int(now), "eps": stats.events_per_second})
        del throughput_history[:-MAX_HISTORY]
        stats.throughput_history = throughput_history.copy()
        await sio.emit("stats:update", stats.to_dict())


# --- manejadores de socket.io ---
@sio.event
async def connect(sid, environ):  # noqa: ANN001
    print(f"[dashboard] connected: {sid}")
    snapshot = {
        "players": [p.to_dict() for p in state.players.values()],
        "queue": [pid for pid, p in state.players.items() if p.status == "in_queue"],
        "matches": [m.to_dict() for m in state.matches],
        "alerts": [a.to_dict() for a in state.alerts[:40]],
        "servers": [s.to_dict() for s in state.servers.values()],
        "serverAlerts": [a.to_dict() for a in state.server_alerts[:40]],
        "stats": state.stats(throughput_history=throughput_history).to_dict(),
        "producerRunning": producer.is_running,
        "serverProducerRunning": server_producer.is_running,
        "clientCount": producer.client_count,
    }
    await sio.emit("state:snapshot", snapshot, to=sid)
    await sio.emit("telemetry:feed", recent_feed, to=sid)
    await sio.emit(
        "players:update",
        [p.to_dict() for p in state.players.values()],
        to=sid,
    )


@sio.event
async def disconnect(sid):
    print(f"[dashboard] disconnected: {sid}")


@sio.on("control")
async def on_control(sid, data):
    msg_type = data.get("type")
    if msg_type == "set-clients":
        producer.set_client_count(int(data.get("count", 40)))
    elif msg_type == "toggle-producer":
        running = bool(data.get("running", False))
        producer.set_running(running)
        await sio.emit("producer:status", {"running": producer.is_running})
    elif msg_type == "inject-cheater":
        producer.inject_cheater()
    elif msg_type == "set-rate":
        producer.set_rate(float(data.get("rate", 1.5)))
    elif msg_type == "toggle-server-producer":
        running = bool(data.get("running", True))
        server_producer.set_running(running)
        await sio.emit("server-producer:status", {"running": server_producer.is_running})
    elif msg_type == "sync-redis":
        await _in_thread(redis_store.sync, state)
        try:
            s = redis_store.status()
        except Exception:
            s = None
        if not s:
            s = {
                "ok": getattr(redis_store, "ok", False),
                "writes": getattr(redis_store, "writes", 0),
                "lastSyncAt": getattr(redis_store, "last_sync_at", 0),
                "error": getattr(redis_store, "last_error", None),
                "players": len(state.players),
                "cheaters": len([p for p in state.players.values() if p.is_cheater]),
            }
        await sio.emit("redis:synced", s)
    elif msg_type == "clear-alerts":
        consumer.clear_alerts()
        await sio.emit("alerts:cleared")
        await sio.emit("servers:cleared")
    elif msg_type == "add-event":
        events = _make_feed_events(data.get("count", 12))
        if events:
            _prepend_feed(events)
            await sio.emit("telemetry:feed", events)


# --- inicio: arrancar el bucle de la canalización ---
@app.on_event("startup")
async def _startup() -> None:
    asyncio.create_task(pipeline_loop())
    kafka_ingest.start()
    print(
        f"Telemetry & Matchmaking service (Python + Apache Spark {SPARK_VERSION}) on port {PORT}\n"
        f"  producer: {producer.client_count} simulated clients\n"
        f"  server producer: Kafka -> {KAFKA_TOPIC} @ {KAFKA_BROKER}"
        f"{' [fallback: in-memory]' if server_producer.fallback else ''}\n"
        f"  consumer: Spark {SPARK_MASTER_URL}, 1s micro-batch window\n"
        f"  cheat detection: lag() window function\n"
        f"  server anomaly detection: lag() window function (Kafka stream)\n"
        f"  matchmaking: groupBy + row_number() window\n"
        f"  redis: {redis_store.url} (prefix {redis_store.prefix!r})"
        f"{'' if redis_store.ok else ' [unavailable]'}\n"
        f"  spark UI: {SPARK_UI_URL}\n"
        f"  application id: {SPARK_APP_ID}",
        flush=True,
    )


@app.on_event("shutdown")
async def _shutdown() -> None:
    kafka_ingest.stop()


if __name__ == "__main__":
    import uvicorn

    # La recarga está desactivada por defecto: el mecanismo de recarga vuelve a importar este módulo
    # ante cada cambio de archivo, lo que recrea la SparkSession y provoca fugas de JVM/workers.
    uvicorn.run(
        "app:asgi_app",
        host="0.0.0.0",
        port=PORT,
        reload=os.environ.get("RELOAD", "0") == "1",
    )
