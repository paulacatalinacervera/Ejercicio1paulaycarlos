"""Estado central en memoria compartido por el productor y el consumidor."""
from __future__ import annotations

import threading
import time
import random

from models import (
    Player,
    GameMatch,
    CheatAlert,
    ServerInfo,
    ServerAlert,
    StatsSnapshot,
    NAMES,
    REGIONS,
)

_name_cursor = [0]

_server_buf_lock = threading.Lock()

def _now() -> float:
    return time.time() * 1000

def region_for(ping: int) -> str:
    if ping < 45:
        return "NA-East"
    if ping < 75:
        return "EU-West"
    if ping < 110:
        return "NA-West"
    if ping < 150:
        return "Asia"
    return "SA"


class GameState:
    def __init__(self) -> None:
        self.players: dict[str, Player] = {}
        self.matches: list[GameMatch] = []
        self.alerts: list[CheatAlert] = []
        self.flagged_players: set[str] = set()
        self.servers: dict[str, ServerInfo] = {}
        self.pending_server_events: list = []  # agotado por el consumidor (respaldado por Kafka)
        self.server_alerts: list[ServerAlert] = []
        self.total_events = 0
        self.matches_formed = 0
        self.event_timestamps: list[float] = []
        # estado del pipeline; por tanto, un bucle bloqueado resulta visible desde /health y /spark
        self.last_batch_at: float = 0.0
        self.last_batch_ms: float = 0.0
        self.pipeline_alive: bool = False

    def create_player(self, cheater: bool = False) -> Player:
        global _name_cursor
        _name_cursor[0] += 1
        idx = _name_cursor[0]
        name = NAMES[(idx - 1) % len(NAMES)] + "_" + str(idx)
        skill = round(20 + random.random() * 80)
        base_ping = round(15 + random.random() * 180)
        region = region_for(base_ping)
        now = _now()
        player = Player(
            id="p_" + format(random.getrandbits(28), "x"),
            name=name,
            skill=skill,
            region=region,
            ping=base_ping,
            x=random.random() * 1000,
            y=random.random() * 1000,
            status="in_queue",
            is_cheater=cheater,
            match_id=None,
            queue_enter_at=now,
            match_enter_at=0.0,
            last_event_at=now,
            target_x=random.random() * 1000,
            target_y=random.random() * 1000,
        )
        self.players[player.id] = player
        return player

    def remove_player(self, pid: str) -> None:
        self.players.pop(pid, None)
        self.flagged_players.discard(pid)

    def record_event(self) -> None:
        self.total_events += 1
        self.event_timestamps.append(_now())

    def append_server_event(self, ev) -> None:
        with _server_buf_lock:
            self.pending_server_events.append(ev)

    def drain_server_events(self) -> list:
        with _server_buf_lock:
            out = self.pending_server_events
            self.pending_server_events = []
        return out

    def prune_alerts(self, max_count: int = 60) -> None:
        del self.alerts[max_count:]

    def prune_server_alerts(self, max_count: int = 40) -> None:
        del self.server_alerts[max_count:]

    def prune_matches(self, duration_ms: int = 25000) -> list[GameMatch]:
        """Elimina las partidas finalizadas y devuelve a sus jugadores a la cola.
        Devuelve la lista de partidas terminadas para que el consumidor pueda notificar al
        productor que detenga sus temporizadores.
        """
        now = _now()
        remaining: list[GameMatch] = []
        ended: list[GameMatch] = []
        for m in self.matches:
            if now - m.created_at < duration_ms:
                remaining.append(m)
            else:
                ended.append(m)
                for pid in m.players:
                    p = self.players.get(pid)
                    if p:
                        p.status = "in_queue"
                        p.match_id = None
                        p.queue_enter_at = now
                        p.kills = 0
                        p.deaths = 0
                        p.flagged_reasons = []
                        self.flagged_players.discard(p.id)
                        # pequeña probabilidad de convertirse en tramposo en la siguiente ronda
                        p.is_cheater = random.random() < 0.08
                        p.x = random.random() * 1000
                        p.y = random.random() * 1000
                        p.target_x = random.random() * 1000
                        p.target_y = random.random() * 1000
        self.matches = remaining
        return ended

    def stats(self, throughput_history: list | None = None) -> StatsSnapshot:
        now = _now()
        one_second_ago = now - 1000
        # eliminar marcas de tiempo antiguas.
        cutoff = now - 30000
        self.event_timestamps = [t for t in self.event_timestamps if t >= cutoff]
        eps = sum(1 for t in self.event_timestamps if t >= one_second_ago)

        in_queue = sum(1 for p in self.players.values() if p.status == "in_queue")
        in_match = sum(1 for p in self.players.values() if p.status == "in_match")
        pings = [p.ping for p in self.players.values()]
        avg_ping = round(sum(pings) / len(pings)) if pings else 0

        cpus = [s.cpu for s in self.servers.values()]
        avg_cpu = round(sum(cpus) / len(cpus)) if cpus else 0

        buckets = [
            {"bucket": "0-50", "count": 0},
            {"bucket": "51-100", "count": 0},
            {"bucket": "101-150", "count": 0},
            {"bucket": "151-200", "count": 0},
            {"bucket": "200+", "count": 0},
        ]
        for p in self.players.values():
            if p.ping <= 50:
                buckets[0]["count"] += 1
            elif p.ping <= 100:
                buckets[1]["count"] += 1
            elif p.ping <= 150:
                buckets[2]["count"] += 1
            elif p.ping <= 200:
                buckets[3]["count"] += 1
            else:
                buckets[4]["count"] += 1

        return StatsSnapshot(
            events_per_second=eps,
            total_events=self.total_events,
            active_players=len(self.players),
            in_queue=in_queue,
            in_match=in_match,
            cheaters_flagged=len(self.flagged_players),
            matches_formed=self.matches_formed,
            avg_ping=avg_ping,
            server_count=len(self.servers),
            avg_cpu=avg_cpu,
            throughput_history=throughput_history or [],
            ping_buckets=buckets,
        )
