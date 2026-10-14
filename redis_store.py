"""Persistencia del estado en Redis.

Guarda el estado vivo del pipeline en estructuras separadas (a modo de tablas):

  telemetry:players          SET   ids de todos los jugadores
  telemetry:player:<id>      HASH  estado, posicion, ping, region, skill, cheater
  telemetry:cheaters         SET   ids marcados como tramposos
  telemetry:stats            HASH  cantidad de jugadores y contadores globales

Todas las keys llevan un prefijo configurable (``REDIS_PREFIX``) para no
pisar otras aplicaciones que compartan el mismo Redis. Si el servidor no esta
disponible, la app sigue funcionando y reintenta conectarse cada 5 s.
"""
from __future__ import annotations

import os
import time

try:
    import redis  # type: ignore
except ImportError:
    redis = None

REDIS_URL = os.environ.get("REDIS_URL", "redis://taller-redis:6379/0")
REDIS_PREFIX = os.environ.get("REDIS_PREFIX", "telemetry:")

SYNC_INTERVAL = 2.0  # segundos entre escrituras


class RedisStore:
    def __init__(self, url: str = REDIS_URL, prefix: str = REDIS_PREFIX) -> None:
        self.url = url
        self.prefix = prefix
        self.key_players = prefix + "players"
        self.key_cheaters = prefix + "cheaters"
        self.key_stats = prefix + "stats"
        self._client = None
        self._error: str | None = None
        self._last_connect = 0.0
        self._last_sync = 0.0
        self._known_ids: set[str] = set()
        self.writes = 0
        self.last_sync_at = 0.0
        if redis is not None:
            self._connect()

    def _connect(self) -> None:
        self._last_connect = time.time()
        try:
            self._client = redis.Redis.from_url(  # type: ignore[union-attr]
                self.url,
                decode_responses=True,
                socket_connect_timeout=1.0,
                socket_timeout=1.0,
            )
            self._client.ping()
            self._error = None
            print(f"[redis] connected @ {self.url} (prefix {self.prefix!r})", flush=True)
        except Exception as exc:  # noqa: BLE001
            self._client = None
            self._error = f"Redis unavailable: {exc}"

    def _maybe_reconnect(self) -> None:
        if self._client is not None or redis is None:
            return
        if time.time() - self._last_connect < 5.0:
            return
        self._connect()

    @property
    def ok(self) -> bool:
        return self._client is not None

    @property
    def error(self) -> str | None:
        return self._error

    def _player_key(self, pid: str) -> str:
        return self.prefix + "player:" + pid

    # --- escritura ---
    def sync(self, state, force: bool = False) -> bool:  # noqa: ANN001
        """Vuelca el estado a Redis. Devuelve True si escribio."""
        self._maybe_reconnect()
        if self._client is None:
            return False

        now = time.time()
        if not force and now - self._last_sync < SYNC_INTERVAL:
            return False
        self._last_sync = now

        players = state.players
        ids = list(players.keys())
        cheaters = [
            p.id
            for p in players.values()
            if p.is_cheater or p.flagged_reasons or p.id in state.flagged_players
        ]
        removed = self._known_ids - set(ids)

        try:
            pipe = self._client.pipeline(transaction=False)

            pipe.delete(self.key_players)
            if ids:
                pipe.sadd(self.key_players, *ids)

            pipe.delete(self.key_cheaters)
            if cheaters:
                pipe.sadd(self.key_cheaters, *cheaters)

            for p in players.values():
                pipe.hset(
                    self._player_key(p.id),
                    mapping={
                        "id": p.id,
                        "name": p.name,
                        "region": p.region,
                        "skill": p.skill,
                        "status": p.status,
                        "x": p.x,
                        "y": p.y,
                        "ping": p.ping,
                        "match_id": p.match_id or "",
                        "is_cheater": int(bool(p.is_cheater)),
                        "updated_at": int(now * 1000),
                    },
                )

            if removed:
                pipe.delete(*[self._player_key(pid) for pid in removed])

            in_queue = sum(1 for p in players.values() if p.status == "in_queue")
            in_match = sum(1 for p in players.values() if p.status == "in_match")
            pipe.hset(
                self.key_stats,
                mapping={
                    "total_players": len(players),
                    "in_queue": in_queue,
                    "in_match": in_match,
                    "cheaters": len(cheaters),
                    "matches": len(state.matches),
                    "updated_at": int(now * 1000),
                },
            )

            pipe.execute()
        except Exception as exc:  # noqa: BLE001
            self._client = None
            self._error = f"Redis write failed: {exc}"
            return False

        self._known_ids = set(ids)
        self.writes += 1
        self.last_sync_at = now * 1000
        return True

    # --- lectura (para diagnostico / endpoints) ---
    def summary(self) -> dict:
        self._maybe_reconnect()
        base = {
            "ok": self.ok,
            "url": self.url,
            "prefix": self.prefix,
            "writes": self.writes,
            "lastSyncAt": self.last_sync_at,
            "error": self._error,
        }
        if self._client is None:
            return base
        try:
            base.update(
                {
                    "players": self._client.scard(self.key_players),
                    "cheaters": self._client.scard(self.key_cheaters),
                    "stats": self._client.hgetall(self.key_stats),
                }
            )
        except Exception as exc:  # noqa: BLE001
            base["error"] = f"Redis read failed: {exc}"
        return base
