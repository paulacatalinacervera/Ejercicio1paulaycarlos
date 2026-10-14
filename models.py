from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Optional

STATUS_IN_MATCH = "in_match"
STATUS_IN_QUEUE = "in_queue"
STATUS_IDLE = "idle"

EVENT_POSITION = "position"
EVENT_ACTION = "action"
EVENT_KILL = "kill"
EVENT_DEATH = "death"

EVENT_SERVER_METRIC = "server_metric"

SERVER_STATUS_RUNNING = "running"
SERVER_STATUS_STOPPED = "stopped"

ACTIONS = ["shoot", "jump", "reload", "ability", "loot", "revive"]

REGIONS = ["NA-East", "NA-West", "EU-West", "EU-Nordic", "Asia", "SA"]

NAMES = [
    "ShadowKnight", "PixelPirate", "NovaStrike", "GhostByte", "RapidRabbit",
    "VortexViper", "CrimsonFox", "IronWolf", "NeonNomad", "SilentSpecter",
    "BlazeFury", "FrostByte", "StormRider", "LunarEcho", "VenomStrike",
    "QuantumQ", "RogueRaven", "TitaniumTiger", "MysticMango", "ZeroChill",
    "ApexAssault", "BlitzPhantom", "CyberCobra", "DarkDawn", "EmberEdge",
    "HyperHawk", "JadeJaguar", "KryptonKing", "LethalLynx", "MidnightMist",
]

@dataclass
class Player:
    id: str
    name: str
    skill: int
    region: str
    ping: int
    x: float
    y: float
    kills: int = 0
    deaths: int = 0
    status: str = STATUS_IN_QUEUE
    is_cheater: bool = False
    flagged_reasons: list = field(default_factory=list)
    match_id: Optional[str] = None
    queue_enter_at: float = 0.0
    match_enter_at: float = 0.0
    last_event_at: float = 0.0
    target_x: float = 0.0
    target_y: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TelemetryEvent:
    id: str
    player_id: str
    player_name: str
    type: str
    x: float
    y: float
    ping: int
    action: Optional[str] = None
    victim_id: Optional[str] = None
    victim_name: Optional[str] = None
    timestamp: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class GameMatch:
    id: str
    players: list
    team_a: list
    team_b: list
    avg_skill: int
    avg_ping: int
    region: str
    created_at: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CheatAlert:
    id: str
    player_id: str
    player_name: str
    reason: str
    speed: int
    distance: int
    details: str
    timestamp: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ServerMetricEvent:
    """Server-side telemetry record (published to Kafka by the ServerProducer)."""
    id: str
    server_id: str
    server_name: str
    region: str
    match_id: Optional[str] = None
    cpu: float = 0.0
    memory: float = 0.0
    tick_rate: float = 0.0
    packet_loss: float = 0.0
    active_players: int = 0
    timestamp: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ServerInfo:
    """Current aggregated state of a running game server (one per live match)."""
    id: str
    name: str
    region: str
    match_id: str
    cpu: float = 0.0
    memory: float = 0.0
    tick_rate: float = 0.0
    packet_loss: float = 0.0
    active_players: int = 0
    status: str = SERVER_STATUS_RUNNING
    degraded: bool = False
    created_at: float = 0.0
    last_event_at: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ServerAlert:
    """Spark-flagged server anomaly (CPU spike / packet loss)."""
    id: str
    server_id: str
    server_name: str
    match_id: Optional[str] = None
    region: str = ""
    reason: str = ""
    severity: str = ""
    details: str = ""
    cpu: float = 0.0
    packet_loss: float = 0.0
    timestamp: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class StatsSnapshot:
    events_per_second: int = 0
    total_events: int = 0
    active_players: int = 0
    in_queue: int = 0
    in_match: int = 0
    cheaters_flagged: int = 0
    matches_formed: int = 0
    avg_ping: int = 0
    server_count: int = 0
    avg_cpu: int = 0
    throughput_history: list = field(default_factory=list)
    ping_buckets: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)
