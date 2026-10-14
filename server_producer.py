from __future__ import annotations

import asyncio
import itertools
import os
import random
import time

from models import ServerMetricEvent, ServerInfo, SERVER_STATUS_RUNNING
from state import GameState

KAFKA_BROKER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "telemetry.server.metrics")

FLAKY_SERVER_RATIO = 0.25
DEGRADE_START = 0.02
DEGRADE_END = 0.03

_SERVER_SEQ = itertools.count(1)
_METRIC_SEQ = itertools.count(1)


def _server_id(match_id: str) -> str:
    return f"srv_{match_id}"


def _metric_id() -> str:
    return f"sme_{next(_METRIC_SEQ):x}{random.random().hex()[2:6]}"


class ServerProducer:
    def __init__(self, state: GameState) -> None:
        self.state = state
        self.rate = 1.0
        self.running = True
        self._tasks: dict[str, asyncio.Task] = {}
        self._produced = 0
        self.fallback = False
        self._kafka_producer = None
        self._kafka_error: str | None = None
        self._last_connect_attempt = 0.0
        self._create_producer()

    # --- Kafka transport ---
    def _create_producer(self) -> None:
        self._last_connect_attempt = time.time()
        try:
            from kafka import KafkaProducer as _KP

            self._kafka_producer = _KP(
                bootstrap_servers=KAFKA_BROKER,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: __import__("json").dumps(v).encode("utf-8"),
                acks="all",
                linger_ms=20,
                max_block_ms=1500,
            )
        except Exception as exc:  # noqa: BLE001
            self._kafka_error = f"Kafka unavailable → in-memory fallback: {exc}"
            self.fallback = True
            self._kafka_producer = None

    def _broker_reachable(self) -> bool:
        """TCP probe to the bootstrap server: cheap + honest health check that
        doesn't depend on KafkaProducer's lazy connection behavior."""
        try:
            import socket

            host_port = KAFKA_BROKER.split("://")[-1].split(",")[0]
            host, port = host_port.rsplit(":", 1)
            with socket.create_connection((host, int(port)), timeout=0.5):
                return True
        except Exception:
            return False

    def _maybe_reconnect(self) -> None:
        """If the producer is in fallback, retry the broker every 5s. Once the
        broker is reachable again this re-creates the producer and clears the
        fallback, so the pipeline recovers without recreating the container."""
        if not self.fallback or self.kafka_ok:
            return
        now = time.time()
        if now - self._last_connect_attempt < 5.0:
            return
        if not self._broker_reachable():
            self._last_connect_attempt = now
            return
        self._create_producer()
        if self.kafka_ok:
            self.fallback = False
            self._kafka_error = None
            print(f"[server_producer] reconnected to Kafka @ {KAFKA_BROKER}", flush=True)

    @property
    def kafka_ok(self) -> bool:
        return self._kafka_producer is not None

    @property
    def is_running(self) -> bool:
        return self.running

    @property
    def produced(self) -> int:
        return self._produced

    @property
    def kafka_error(self) -> str | None:
        return self._kafka_error

    # --- controls ---
    def set_running(self, running: bool) -> None:
        self.running = running
        if not running:
            for t in list(self._tasks.values()):
                t.cancel()
            self._tasks.clear()
        else:
            for info in self.state.servers.values():
                self._attach_loop(info)

    # --- match lifecycle ---
    def on_server_spawned(self, match) -> None:
        info = ServerInfo(
            id=_server_id(match.id),
            name=f"gs-{match.region}-{next(_SERVER_SEQ)}",
            region=match.region,
            match_id=match.id,
            cpu=round(random.uniform(25, 45), 1),
            memory=round(random.uniform(40, 55), 1),
            tick_rate=round(random.uniform(58, 64), 1),
            packet_loss=round(random.uniform(0, 0.8), 2),
            active_players=len(match.players),
            status=SERVER_STATUS_RUNNING,
            degraded=random.random() < FLAKY_SERVER_RATIO,
            created_at=time.time() * 1000,
            last_event_at=time.time() * 1000,
        )
        self.state.servers[info.id] = info
        self._attach_loop(info)

    def on_server_stopped(self, match) -> None:
        sid = _server_id(match.id)
        task = self._tasks.pop(sid, None)
        if task:
            task.cancel()
        self.state.servers.pop(sid, None)

    # --- asyncio task management ---
    def _attach_loop(self, info: ServerInfo) -> None:
        if info.id in self._tasks:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._tasks[info.id] = loop.create_task(self._metric_loop(info))

    # --- metric generation & emission ---
    async def _metric_loop(self, info: ServerInfo) -> None:
        interval = max(0.4, 1.0 / self.rate)
        while True:
            await asyncio.sleep(interval)
            if not self.running or info.id not in self.state.servers:
                task = self._tasks.pop(info.id, None)
                if task:
                    task.cancel()
                return
            self._tick(info)
            self._publish(info)

    def _tick(self, info: ServerInfo) -> None:
        if info.degraded:
            if random.random() < DEGRADE_END:
                info.degraded = False
            cpu = 90 + random.random() * 9
            ploss = 2.5 + random.random() * 5.5
        else:
            if random.random() < DEGRADE_START:
                info.degraded = True
            cpu = max(10, min(80, info.cpu + (random.random() - 0.5) * 10))
            ploss = max(0.0, min(1.5, info.packet_loss + (random.random() - 0.5) * 1.0))
        info.cpu = round(cpu, 1)
        info.packet_loss = round(ploss, 2)
        info.memory = round(max(35, min(85, info.memory + random.random() * 0.8)), 1)
        info.tick_rate = round(
            max(20, min(64, 64 - max(0.0, cpu - 60) * 0.5 + random.random() * 4)), 1
        )
        info.active_players = sum(
            1 for p in self.state.players.values() if p.match_id == info.match_id
        )
        info.last_event_at = time.time() * 1000

    def _publish(self, info: ServerInfo) -> None:
        ev = ServerMetricEvent(
            id=_metric_id(),
            server_id=info.id,
            server_name=info.name,
            region=info.region,
            match_id=info.match_id,
            cpu=info.cpu,
            memory=info.memory,
            tick_rate=info.tick_rate,
            packet_loss=info.packet_loss,
            active_players=info.active_players,
            timestamp=info.last_event_at,
        )
        self.state.record_event() 
        self._produced += 1
        self._maybe_reconnect()
        if self.kafka_ok:
            try:
                self._kafka_producer.send(KAFKA_TOPIC, key=info.id, value=ev.to_dict())
                return
            except Exception as exc:
                self._kafka_producer = None
                self.fallback = True
                self._kafka_error = f"Kafka send failed → in-memory fallback: {exc}"
        self.state.append_server_event(ev)