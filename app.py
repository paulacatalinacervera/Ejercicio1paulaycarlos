from __future__ import annotations

import asyncio
import time
import functools
import os
import random
import sys
import traceback
from pathlib import Path

os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
os.environ.setdefault('PYSPARK_DRIVER_PYTHON', sys.executable)

import socketio
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
try:
    from pyspark.sql import SparkSession
    _HAS_SPARK = True
except Exception:
    SparkSession = None
    _HAS_SPARK = False

from state import GameState
from producer import Producer
from consumer import Consumer
from server_producer import ServerProducer, KAFKA_BROKER, KAFKA_TOPIC
from kafka_ingest import KafkaIngest
from mongo_store import MongoStore
from models import ACTIONS

PORT = int(os.environ.get('PORT', '3003'))
HERE = Path(__file__).resolve().parent

sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*', ping_interval=25, ping_timeout=60)
app = FastAPI(title='Telemetry & Matchmaking')
app.mount('/static', StaticFiles(directory=str(HERE / 'static')), name='static')
asgi_app = socketio.ASGIApp(sio, other_asgi_app=app)

SPARK_MASTER = os.environ.get('SPARK_MASTER', 'local[4]')
spark = None
SPARK_VERSION = 'N/A'
SPARK_APP_ID = 'N/A'
SPARK_UI_URL = None
SPARK_MASTER_URL = SPARK_MASTER
if _HAS_SPARK and SparkSession:
    try:
        spark = (
            SparkSession.builder.appName('TelemetryMatchmaking')
            .master(SPARK_MASTER)
            .config('spark.ui.enabled', 'true')
            .config('spark.ui.port', '4040')
            .config('spark.sql.shuffle.partitions', '4')
            .config('spark.driver.host', os.environ.get('SPARK_DRIVER_HOST', '127.0.0.1'))
            .config('spark.driver.bindAddress', os.environ.get('SPARK_DRIVER_BIND', '127.0.0.1'))
            .config('spark.sql.adaptive.enabled', 'true')
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel('WARN')
        SPARK_VERSION = spark.version
        SPARK_APP_ID = spark.sparkContext.applicationId
        SPARK_UI_URL = spark.sparkContext.uiWebUrl
        SPARK_MASTER_URL = spark.sparkContext.master
    except Exception as e:
        print(f'Spark init failed: {e}', flush=True)

state = GameState()
producer = Producer(state)
server_producer = ServerProducer(state)
consumer = Consumer(state, producer, spark, server_producer=server_producer) if spark else Consumer(state, producer, spark, server_producer=server_producer)
kafka_ingest = KafkaIngest(state)
mongo_store = MongoStore()

producer.set_client_count(40)

throughput_history = []
MAX_HISTORY = 60
recent_feed = []
last_player_emit = 0.0
last_stats_emit = 0.0
last_reconcile = 0.0


def _prepend_feed(feed_dicts):
    global recent_feed
    recent_feed = (feed_dicts + recent_feed)[:80]


def _make_feed_events(count=12):
    players = list(state.players.values())
    if not players:
        return []
    events = []
    for _ in range(max(1, int(count))):
        p = random.choice(players)
        if random.random() < 0.5:
            events.append({'id': f'e_{random.getrandbits(32):x}', 'type': 'action', 'player_name': p.name, 'action': random.choice(ACTIONS), 'ping': p.ping, 'timestamp': time.time() * 1000})
        else:
            victim = random.choice(players) if len(players) > 1 else p
            events.append({'id': f'e_{random.getrandbits(32):x}', 'type': 'kill', 'player_name': p.name, 'victim_name': victim.name, 'timestamp': time.time() * 1000})
    return events


async def _in_thread(fn, *args):
    if hasattr(asyncio, 'to_thread'):
        return await asyncio.to_thread(fn, *args)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(fn, *args))


@app.get('/', response_class=HTMLResponse)
async def index():
    html = (HERE / 'templates' / 'index.html').read_text(encoding='utf-8')
    return HTMLResponse(html)


@app.get('/health')
async def health():
    return JSONResponse({'status': 'ok', 'players': len(state.players), 'matches': len(state.matches), 'servers': len(state.servers), 'alerts': len(state.alerts), 'pipelineAlive': state.pipeline_alive, 'lastBatchAt': state.last_batch_at})


@app.get('/spark')
async def spark_status():
    return JSONResponse({'engine': 'Apache Spark (PySpark)', 'version': SPARK_VERSION, 'appName': 'TelemetryMatchmaking', 'applicationId': SPARK_APP_ID, 'master': SPARK_MASTER_URL, 'sparkUI': SPARK_UI_URL, 'batchesProcessed': getattr(consumer, 'batches_processed', 0), 'lastBatchAt': state.last_batch_at, 'lastBatchMs': state.last_batch_ms, 'pipelineAlive': state.pipeline_alive, 'lastError': getattr(consumer, 'last_error', None), 'cheatDetection': 'Spark SQL window function lag()', 'matchmaking': 'Spark groupBy(region) + row_number()', 'serverAnomalyDetection': 'Spark SQL window function lag()'})


@app.get('/kafka')
async def kafka_status():
    return JSONResponse({'transport': 'Kafka (game server telemetry)', 'bootstrapServers': KAFKA_BROKER, 'topic': KAFKA_TOPIC, 'producerOk': server_producer.kafka_ok, 'producerFallback': server_producer.fallback, 'producerRunning': server_producer.is_running, 'produced': server_producer.produced, 'ingestAlive': kafka_ingest.alive, 'ingested': kafka_ingest.ingested, 'lastMessageAt': kafka_ingest.last_message_at, 'ingestError': kafka_ingest.error, 'producerError': server_producer.kafka_error, 'servers': len(state.servers)})


@app.get('/mongo')
@app.get('/mongodb')
@app.get('/redis')
async def mongo_status():
    try:
        return JSONResponse(mongo_store.summary())
    except Exception as e:
        return JSONResponse({'ok': False, 'error': str(e)})


async def _pipeline_tick(batch, server_batch, ended_matches, now):
    global last_reconcile
    if now - last_reconcile > 5000:
        last_reconcile = now
        producer._reconcile()

    feed_events = consumer.reduce_batch(batch)
    position_events = [e for e in batch if getattr(e, 'type', None) == 'position']
    if position_events and spark:
        try:
            new_alerts = await _in_thread(consumer._detect_cheats_spark, position_events)
        except Exception:
            consumer.last_error = traceback.format_exc()
            print(f'[pipeline] cheat detection failed:\n{consumer.last_error}', flush=True)
            new_alerts = []
    else:
        new_alerts = []

    if feed_events:
        feed_dicts = [e.to_dict() for e in feed_events]
        _prepend_feed(feed_dicts)
        await sio.emit('telemetry:feed', feed_dicts)
        for ev in feed_dicts:
            try:
                await _in_thread(mongo_store.store_event, ev)
            except Exception:
                pass
    for alert in new_alerts:
        try:
            alert_dict = alert.to_dict()
            await sio.emit('cheat:alert', alert_dict)
            await _in_thread(mongo_store.store_event, {'type': 'cheat_alert', **alert_dict})
        except Exception:
            pass

    consumer.reduce_server_batch(server_batch)
    if server_batch and spark:
        try:
            server_alerts = await _in_thread(consumer._detect_server_anomalies_spark, server_batch)
        except Exception:
            consumer.last_error = traceback.format_exc()
            print(f'[pipeline] server anomaly detection failed:\n{consumer.last_error}', flush=True)
            server_alerts = []
    else:
        server_alerts = []
    for alert in server_alerts:
        try:
            alert_dict = alert.to_dict()
            await sio.emit('server:alert', alert_dict)
            await _in_thread(mongo_store.store_event, {'type': 'server_alert', **alert_dict})
        except Exception:
            pass

    try:
        if spark:
            ranked_regions = await _in_thread(consumer.rank_queue_spark)
            new_matches = consumer.form_matches(ranked_regions)
        else:
            new_matches = []
    except Exception:
        consumer.last_error = traceback.format_exc()
        print(f'[pipeline] matchmaking failed:\n{consumer.last_error}', flush=True)
        new_matches = []
    for match in new_matches:
        try:
            match_dict = match.to_dict()
            await sio.emit('matchmaking:match-formed', match_dict)
            await _in_thread(mongo_store.store_event, {'type': 'match_formed', **match_dict})
        except Exception:
            pass

    for m in ended_matches:
        ended_players = [state.players[pid] for pid in m.players if pid in state.players]
        consumer.on_match_ended(m, ended_players)
        try:
            await _in_thread(mongo_store.store_event, {'type': 'match_ended', 'match_id': m.id})
        except Exception:
            pass


async def pipeline_loop():
    while True:
        try:
            batch = producer.drain_events()
            server_batch = state.drain_server_events()
            now = time.time() * 1000
            ended = state.prune_matches()
            await _pipeline_tick(batch, server_batch, ended, now)
            await _broadcasts(now)
            try:
                await _in_thread(mongo_store.sync, state)
            except Exception:
                pass
            state.pipeline_alive = True
            state.last_batch_at = time.time() * 1000
            state.last_batch_ms = state.last_batch_at - now
        except asyncio.CancelledError:
            raise
        except Exception:
            state.pipeline_alive = False
            consumer.last_error = traceback.format_exc()
            print(f'[pipeline] tick failed:\n{consumer.last_error}', flush=True)
        await asyncio.sleep(1.0)


async def _broadcasts(now):
    global last_player_emit, last_stats_emit
    if now - last_player_emit > 1500:
        last_player_emit = now
        players = [p.to_dict() for p in state.players.values()]
        await sio.emit('players:update', players)
        await sio.emit('servers:update', [s.to_dict() for s in state.servers.values()])
    if now - last_stats_emit > 1000:
        last_stats_emit = now
        stats = state.stats(throughput_history=throughput_history)
        throughput_history.append({'t': int(now), 'eps': stats.events_per_second})
        del throughput_history[:-MAX_HISTORY]
        stats.throughput_history = throughput_history.copy()
        await sio.emit('stats:update', stats.to_dict())


@sio.event
async def connect(sid, environ):
    print(f'[dashboard] connected: {sid}')
    snapshot = {'players': [p.to_dict() for p in state.players.values()], 'queue': [pid for pid, p in state.players.items() if p.status == 'in_queue'], 'matches': [m.to_dict() for m in state.matches], 'alerts': [a.to_dict() for a in state.alerts[:40]], 'servers': [s.to_dict() for s in state.servers.values()], 'serverAlerts': [a.to_dict() for a in state.server_alerts[:40]], 'stats': state.stats(throughput_history=throughput_history).to_dict(), 'producerRunning': producer.is_running, 'serverProducerRunning': server_producer.is_running, 'clientCount': producer.client_count}
    await sio.emit('state:snapshot', snapshot, to=sid)
    await sio.emit('telemetry:feed', recent_feed, to=sid)
    await sio.emit('players:update', [p.to_dict() for p in state.players.values()], to=sid)


@sio.event
async def disconnect(sid):
    print(f'[dashboard] disconnected: {sid}')


@sio.on('control')
async def on_control(sid, data):
    msg_type = data.get('type')
    if msg_type == 'set-clients':
        producer.set_client_count(int(data.get('count', 40)))
    elif msg_type == 'toggle-producer':
        running = bool(data.get('running', False))
        producer.set_running(running)
        await sio.emit('producer:status', {'running': producer.is_running})
    elif msg_type == 'inject-cheater':
        producer.inject_cheater()
    elif msg_type == 'set-rate':
        producer.set_rate(float(data.get('rate', 1.5)))
    elif msg_type == 'toggle-server-producer':
        running = bool(data.get('running', True))
        server_producer.set_running(running)
        await sio.emit('server-producer:status', {'running': server_producer.is_running})
    elif msg_type == 'sync-redis' or msg_type == 'sync-mongo':
        try:
            await _in_thread(mongo_store.sync, state)
            s = mongo_store.summary()
        except Exception as e:
            s = {'ok': False, 'error': str(e)}
        await sio.emit('redis:synced', s)
    elif msg_type == 'clear-alerts':
        consumer.clear_alerts()
        await sio.emit('alerts:cleared')
        await sio.emit('servers:cleared')
    elif msg_type == 'add-event':
        events = _make_feed_events(data.get('count', 12))
        if events:
            _prepend_feed(events)
            await sio.emit('telemetry:feed', events)


@app.on_event('startup')
async def _startup():
    asyncio.create_task(pipeline_loop())
    kafka_ingest.start()
    print(f'Telemetry & Matchmaking service (Python + Apache Spark {SPARK_VERSION}) on port {PORT}', flush=True)


@app.on_event('shutdown')
async def _shutdown():
    kafka_ingest.stop()


if __name__ == '__main__':
    import uvicorn
    uvicorn.run('app:asgi_app', host='0.0.0.0', port=PORT, reload=os.environ.get('RELOAD', '0') == '1')
