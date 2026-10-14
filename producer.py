from __future__ import annotations

import asyncio
import random
import time
import itertools

from models import Player, TelemetryEvent, ACTIONS
from state import GameState

MAX_NORMAL_SPEED = 180
TELEPORT_PROBABILITY = 0.04
SPEED_HACK_MULT = 2.6

_event_seq = itertools.count(1)


def _uid(prefix: str) -> str:
    return f"{prefix}_{next(_event_seq):x}{random.random().hex()[2:6]}"


def _lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


class Producer:
    def __init__(self, state: GameState) -> None:
        self.state = state
        self.rate = 1.5          # events / sec per client
        self.running = True
        self.target_clients = 40
        self.pending_events: list[TelemetryEvent] = []
        self._tasks: dict[str, asyncio.Task] = {}
        self._kill_tasks: dict[str, asyncio.Task] = {}
        self._loop_task: asyncio.Task | None = None

    # --- public controls ---
    def set_client_count(self, count: int) -> None:
        self.target_clients = max(4, min(120, count))
        self._reconcile()

    def set_rate(self, rate: float) -> None:
        self.rate = max(0.3, min(6.0, rate))
        # restart timers so new rate takes effect
        self._cancel_all_tasks()
        for p in self.state.players.values():
            if p.status == "in_match":
                self._attach_timers(p)

    def set_running(self, running: bool) -> None:
        self.running = running
        if not running:
            self._cancel_all_tasks()
        else:
            for p in self.state.players.values():
                if p.status == "in_match":
                    self._attach_timers(p)

    def inject_cheater(self) -> None:
        p = self.state.create_player(cheater=True)
        p.status = "in_queue"
        p.queue_enter_at = time.time() * 1000

    @property
    def is_running(self) -> bool:
        return self.running

    @property
    def client_count(self) -> int:
        return len(self.state.players)

    # --- pool management ---
    def _reconcile(self) -> None:
        current = len(self.state.players)
        if current < self.target_clients:
            for _ in range(self.target_clients - current):
                self.state.create_player(cheater=random.random() < 0.12)
        elif current > self.target_clients:
            ids = list(self.state.players.keys())[: current - self.target_clients]
            for pid in ids:
                self._detach_timers(pid)
                self.state.remove_player(pid)

    def _cancel_all_tasks(self) -> None:
        for t in list(self._tasks.values()):
            t.cancel()
        self._tasks.clear()
        for t in list(self._kill_tasks.values()):
            t.cancel()
        self._kill_tasks.clear()

    def _attach_timers(self, p: Player) -> None:
        if p.id in self._tasks:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._tasks[p.id] = loop.create_task(self._position_loop(p))
        self._kill_tasks[p.id] = loop.create_task(self._kill_loop(p))

    def _detach_timers(self, pid: str) -> None:
        t = self._tasks.pop(pid, None)
        if t:
            t.cancel()
        t = self._kill_tasks.pop(pid, None)
        if t:
            t.cancel()

    def on_player_enter_match(self, p: Player) -> None:
        self._attach_timers(p)

    def on_player_leave_match(self, p: Player) -> None:
        self._detach_timers(p.id)

    # --- async event loops ---
    async def _position_loop(self, p: Player) -> None:
        interval = max(0.15, 1.0 / self.rate)
        while True:
            await asyncio.sleep(interval)
            if not self.running or p.status != "in_match":
                self._detach_timers(p.id)
                return
            self._emit_position_event(p)
            if random.random() < 0.25:
                self._emit_action_event(p)
            # fluctuate ping slightly
            p.ping = max(8, round(p.ping + (random.random() - 0.5) * 8))
            p.last_event_at = time.time() * 1000

    async def _kill_loop(self, p: Player) -> None:
        while True:
            await asyncio.sleep(4 + random.random() * 4)
            if not self.running or p.status != "in_match":
                self._detach_timers(p.id)
                return
            if random.random() < 0.5:
                self._emit_kill_event(p)

    # --- movement & event emission ---
    def _move_player(self, p: Player, dt: float) -> None:
        dx = p.target_x - p.x
        dy = p.target_y - p.y
        dist = (dx * dx + dy * dy) ** 0.5
        if dist < 25:
            p.target_x = random.random() * 1000
            p.target_y = random.random() * 1000
            return
        speed = MAX_NORMAL_SPEED
        if p.is_cheater:
            if random.random() < TELEPORT_PROBABILITY:
                # TELEPORT: jump to a random location
                p.x = random.random() * 1000
                p.y = random.random() * 1000
                return
            speed = MAX_NORMAL_SPEED * SPEED_HACK_MULT
        move = speed * dt
        t = min(1.0, move / max(1.0, dist))
        new_x = _lerp(p.x, p.target_x, t)
        new_y = _lerp(p.y, p.target_y, t)
        p.x = max(0.0, min(1000.0, new_x))
        p.y = max(0.0, min(1000.0, new_y))

    def _emit_position_event(self, p: Player) -> None:
        dt = 1.0 / self.rate
        self._move_player(p, dt)
        ev = TelemetryEvent(
            id=_uid("e"),
            player_id=p.id,
            player_name=p.name,
            type="position",
            x=round(p.x),
            y=round(p.y),
            ping=p.ping,
            timestamp=time.time() * 1000,
        )
        self.state.record_event()
        self.pending_events.append(ev)

    def _emit_action_event(self, p: Player) -> None:
        ev = TelemetryEvent(
            id=_uid("e"),
            player_id=p.id,
            player_name=p.name,
            type="action",
            x=round(p.x),
            y=round(p.y),
            ping=p.ping,
            action=random.choice(ACTIONS),
            timestamp=time.time() * 1000,
        )
        self.state.record_event()
        self.pending_events.append(ev)

    def _emit_kill_event(self, p: Player) -> None:
        candidates = [
            q for q in self.state.players.values()
            if q.status == "in_match" and q.id != p.id
        ]
        if not candidates:
            return
        victim = random.choice(candidates)
        p.kills += 1
        victim.deaths += 1
        ev = TelemetryEvent(
            id=_uid("e"),
            player_id=p.id,
            player_name=p.name,
            type="kill",
            x=round(p.x),
            y=round(p.y),
            ping=p.ping,
            victim_id=victim.id,
            victim_name=victim.name,
            timestamp=time.time() * 1000,
        )
        self.state.record_event()
        self.pending_events.append(ev)

    def drain_events(self) -> list[TelemetryEvent]:
        out = self.pending_events
        self.pending_events = []
        return out
