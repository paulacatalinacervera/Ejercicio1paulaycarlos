from __future__ import annotations

import time
import random
import itertools

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

from models import Player, GameMatch, CheatAlert, ServerAlert, TelemetryEvent, ServerMetricEvent
from state import GameState
from producer import Producer

SPEED_LIMIT = 260
TELEPORT_DISTANCE = 400
TEAM_SIZE = 3
MATCH_SIZE = TEAM_SIZE * 2
SKILL_WINDOW = 18
PING_WINDOW = 60
MAX_QUEUE_WAIT = 8000

SERVER_CPU_SPIKE = 90        
SERVER_CPU_DELTA = 30        
SERVER_PACKET_LOSS = 3.0     

_alert_seq = itertools.count(1)
_match_seq = itertools.count(1)


def _alert_id() -> str:
    return f"a_{next(_alert_seq)}_{random.random().hex()[2:6]}"


def _match_id() -> str:
    return f"m_{next(_match_seq)}"

# --- Spark schemas (explicit, so nullable fields work with createDataFrame) ---
_LAST_POS_SCHEMA = StructType([
    StructField("player_id", StringType(), False),
    StructField("x", DoubleType(), False),
    StructField("y", DoubleType(), False),
    StructField("ts", DoubleType(), False),
])

_QUEUE_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("skill", IntegerType(), False),
    StructField("ping", IntegerType(), False),
    StructField("region", StringType(), False),
    StructField("queue_enter_at", DoubleType(), False),
])

_SERVER_METRIC_SCHEMA = StructType([
    StructField("server_id", StringType(), False),
    StructField("cpu", DoubleType(), False),
    StructField("packet_loss", DoubleType(), False),
    StructField("tick_rate", DoubleType(), False),
    StructField("ts", DoubleType(), False),
])

class Consumer:
    """Spark-backed consumer. Same public interface as the pure-Python one."""

    def __init__(
        self,
        state: GameState,
        producer: Producer,
        spark: SparkSession,
        server_producer=None,
    ) -> None:
        self.state = state
        self.producer = producer
        self.server_producer = server_producer
        self.spark = spark
        # "last known position" table. Kept as plain rows on the driver and
        # rebuilt into a DataFrame every batch: keeping it as a DataFrame made
        # the plan grow by one union + one window per batch (Stage 143, 145,
        # 157...) until each job became enormous.
        self._last_pos_rows: list[tuple] = []
        self._last_server_rows: list[tuple] = []
        # server ids that already have a live anomaly alert (one alert per episode)
        self._server_alerted: set[str] = set()
        self.batches_processed = 0
        self.last_error: str | None = None

    # ------------------------------------------------------------------ #
    # process(): reduce + Spark cheat detection
    # Returns (feed_events, new_alerts)
    # ------------------------------------------------------------------ #
    def reduce_batch(self, batch: list[TelemetryEvent]) -> list[TelemetryEvent]:
        """Driver-side reduce: apply kills/deaths/ping/pos to player state.

        Cheap and non-blocking, so it stays on the event loop.
        """
        self.batches_processed += 1

        # --- 3. REDUCE: apply kills/deaths/ping/pos to player state (driver) ---
        for ev in batch:
            p = self.state.players.get(ev.player_id)
            if not p:
                continue
            if ev.type == "kill":
                p.kills += 1
                if ev.victim_id:
                    v = self.state.players.get(ev.victim_id)
                    if v:
                        v.deaths += 1
            p.ping = ev.ping
            p.x = ev.x
            p.y = ev.y
            p.last_event_at = ev.timestamp

        # --- feed events for the dashboard ---
        feed = [e for e in batch if e.type in ("kill", "action", "death")]
        feed.reverse()
        return feed[-40:]

    def process(self, batch: list[TelemetryEvent]) -> tuple[list[TelemetryEvent], list[CheatAlert]]:
        """Synchronous convenience wrapper: reduce, then Spark cheat detection."""
        feed = self.reduce_batch(batch)
        position_events = [e for e in batch if e.type == "position"]
        if not position_events:
            return feed, []
        return feed, self._detect_cheats_spark(position_events)

    def _detect_cheats_spark(self, position_events: list[TelemetryEvent]) -> list[CheatAlert]:
        """Use a Spark window function (lag) to detect impossible movement."""
        now = time.time() * 1000

        # Build a DataFrame of this batch's position events
        batch_rows = [
            (e.player_id, float(e.x), float(e.y), float(e.timestamp))
            for e in position_events
        ]
        batch_pos_df = self.spark.createDataFrame(batch_rows, schema=_LAST_POS_SCHEMA)

        # Union with the last known positions so lag() can see the previous
        # position even across batch boundaries. Flat plan: the history lives
        # in `self._last_pos_rows` (driver side), not in the DataFrame lineage.
        if self._last_pos_rows:
            prev_pos_df = self.spark.createDataFrame(
                self._last_pos_rows, schema=_LAST_POS_SCHEMA
            )
            combined = prev_pos_df.union(batch_pos_df)
        else:
            combined = batch_pos_df

        # Window: partition by player, order by timestamp
        w = Window.partitionBy("player_id").orderBy("ts")

        # lag() gives us the previous position per player
        with_lag = (
            combined
            .withColumn("prev_x", F.lag("x").over(w))
            .withColumn("prev_y", F.lag("y").over(w))
            .withColumn("prev_t", F.lag("ts").over(w))
        )

        # Compute Euclidean distance and speed (units/sec)
        with_metrics = (
            with_lag
            .withColumn(
                "distance",
                F.sqrt(
                    F.pow(F.col("x") - F.col("prev_x"), 2)
                    + F.pow(F.col("y") - F.col("prev_y"), 2)
                ),
            )
            .withColumn("dt", (F.col("ts") - F.col("prev_t")) / 1000.0)
            .withColumn(
                "speed",
                F.when(F.col("dt") > 0, F.col("distance") / F.col("dt")).otherwise(0.0),
            )
        )

        # Filter: rows that have a previous position AND violate the rules.
        # These are potential cheaters from THIS batch.
        cheaters_df = with_metrics.filter(
            F.col("prev_x").isNotNull()
            & (
                (F.col("distance") >= TELEPORT_DISTANCE)
                | (F.col("speed") > SPEED_LIMIT)
            )
        ).select(
            "player_id", "x", "y", "ts", "prev_t", "distance", "speed"
        )

        # Collect results to the driver and build CheatAlert objects
        new_alerts: list[CheatAlert] = []
        for row in cheaters_df.collect():
            p = self.state.players.get(row.player_id)
            if not p or p.id in self.state.flagged_players:
                continue
            dist = float(row.distance)
            speed = float(row.speed)
            if dist >= TELEPORT_DISTANCE:
                reason = "Teleportation (impossible position delta)"
                details = f"jumped {dist:.0f}u in {(row.ts - row.prev_t):.0f}ms"
            else:
                reason = "Speed hack (movement above legal cap)"
                details = f"{speed:.0f} u/s (cap {SPEED_LIMIT} u/s)"
            p.is_cheater = True
            p.flagged_reasons.append(reason)
            self.state.flagged_players.add(p.id)
            alert = CheatAlert(
                id=_alert_id(),
                player_id=p.id,
                player_name=p.name,
                reason=reason,
                speed=round(speed),
                distance=round(dist),
                details=details,
                timestamp=now,
            )
            self.state.alerts.insert(0, alert)
            self.state.prune_alerts()
            new_alerts.append(alert)

        # --- Update the last known positions (materialized to the driver) ---
        # Keep only the latest position per player (drop stale rows) and drop
        # players that left the pool, so the table stays small and the next
        # batch's plan stays flat.
        latest_w = Window.partitionBy("player_id").orderBy(F.col("ts").desc())
        current_ids = set(self.state.players.keys())
        self._last_pos_rows = [
            (r["player_id"], float(r["x"]), float(r["y"]), float(r["ts"]))
            for r in (
                combined.withColumn("rn", F.row_number().over(latest_w))
                .filter(F.col("rn") == 1)
                .select("player_id", "x", "y", "ts")
                .collect()
            )
            if r["player_id"] in current_ids
        ]

        return new_alerts

    # ------------------------------------------------------------------ #
    # Server telemetry (Kafka-backed): reduce + Spark anomaly detection
    # ------------------------------------------------------------------ #
    def _server_bad(self, s) -> bool:
        return s.cpu >= SERVER_CPU_SPIKE or s.packet_loss > SERVER_PACKET_LOSS

    def reduce_server_batch(self, events: list[ServerMetricEvent]) -> None:
        """Apply Kafka-ingested server metrics to the ServerInfo state. Driver-side."""
        for ev in events:
            info = self.state.servers.get(ev.server_id)
            if not info:
                continue
            info.cpu = ev.cpu
            info.memory = ev.memory
            info.tick_rate = ev.tick_rate
            info.packet_loss = ev.packet_loss
            info.active_players = ev.active_players
            info.last_event_at = ev.timestamp
            # end a live anomaly episode as soon as the server looks healthy again
            if not self._server_bad(info):
                self._server_alerted.discard(info.id)

    def _detect_server_anomalies_spark(
        self, events: list[ServerMetricEvent]
    ) -> list[ServerAlert]:
        """Use a Spark window function (lag) to flag CPU spikes / packet loss."""
        now = time.time() * 1000
        rows = [
            (e.server_id, float(e.cpu), float(e.packet_loss), float(e.tick_rate), float(e.timestamp))
            for e in events
        ]
        batch_df = self.spark.createDataFrame(rows, schema=_SERVER_METRIC_SCHEMA)

        if self._last_server_rows:
            prev_df = self.spark.createDataFrame(self._last_server_rows, schema=_SERVER_METRIC_SCHEMA)
            combined = prev_df.union(batch_df)
        else:
            combined = batch_df

        w = Window.partitionBy("server_id").orderBy("ts")
        with_lag = (
            combined
            .withColumn("prev_cpu", F.lag("cpu").over(w))
            .withColumn("prev_loss", F.lag("packet_loss").over(w))
            .withColumn("prev_t", F.lag("ts").over(w))
        )
        flagged = with_lag.filter(
            F.col("prev_cpu").isNotNull()
            & (
                ((F.col("cpu") >= SERVER_CPU_SPIKE) & (F.col("cpu") - F.col("prev_cpu") >= SERVER_CPU_DELTA))
                | (F.col("packet_loss") > SERVER_PACKET_LOSS)
            )
        ).select("server_id", "cpu", "packet_loss", "tick_rate", "ts", "prev_cpu", "prev_t")

        new_alerts: list[ServerAlert] = []
        for row in flagged.collect():
            info = self.state.servers.get(row.server_id)
            if not info or info.id in self._server_alerted:
                continue
            cpu = float(row.cpu)
            loss = float(row.packet_loss)
            if loss > SERVER_PACKET_LOSS:
                reason = "Packet loss"
                severity = "high" if loss > 6 else "medium"
                details = f"{loss:.1f}% loss (> {SERVER_PACKET_LOSS}% cap)"
            else:
                reason = "CPU spike"
                severity = "high" if cpu >= 97 else "medium"
                details = f"CPU {cpu:.0f}% jumped {cpu - float(row.prev_cpu):.0f}% in {(row.ts - row.prev_t):.0f}ms"
            self._server_alerted.add(info.id)
            alert = ServerAlert(
                id=_alert_id(),
                server_id=info.id,
                server_name=info.name,
                match_id=info.match_id,
                region=info.region,
                reason=reason,
                severity=severity,
                details=details,
                cpu=round(cpu),
                packet_loss=round(loss, 1),
                timestamp=now,
            )
            self.state.server_alerts.insert(0, alert)
            self.state.prune_server_alerts()
            new_alerts.append(alert)

        latest_w = Window.partitionBy("server_id").orderBy(F.col("ts").desc())
        current_ids = set(self.state.servers.keys())
        self._last_server_rows = [
            (r["server_id"], float(r["cpu"]), float(r["packet_loss"]), float(r["tick_rate"]), float(r["ts"]))
            for r in (
                combined.withColumn("rn", F.row_number().over(latest_w))
                .filter(F.col("rn") == 1)
                .select("server_id", "cpu", "packet_loss", "tick_rate", "ts")
                .collect()
            )
            if r["server_id"] in current_ids
        ]

        return new_alerts

    # ------------------------------------------------------------------ #
    # run_matchmaking(): split in two so the blocking Spark part can run off
    # the event loop (it only reads state) while the mutation part — which
    # calls producer.on_player_enter_match() and therefore needs the
    # asyncio loop thread — stays on the loop.
    # ------------------------------------------------------------------ #
    #agrupa y ordena la cola
    def rank_queue_spark(self) -> list[tuple]:
        """SPARK: group the queue by region and rank by skill.

        Returns [(region, [player_struct, ...] sorted by skill_rank)].
        Pure read, safe to call from a worker thread.
        """
        queued = [p for p in self.state.players.values() if p.status == "in_queue"]
        if len(queued) < MATCH_SIZE:
            return []

        # --- 2. SPARK: load queued players, group by region, rank by skill ---
        rows = [
            (p.id, p.name, int(p.skill), int(p.ping), p.region, float(p.queue_enter_at))
            for p in queued
        ]
        df = self.spark.createDataFrame(rows, schema=_QUEUE_SCHEMA)

        # Window: partition by region, order by skill → row_number gives a
        # stable skill rank inside each region.
        region_skill_w = Window.partitionBy("region").orderBy("skill")
        ranked = df.withColumn("skill_rank", F.row_number().over(region_skill_w))

        # Collect per-region player lists (already sorted by skill_rank)
        region_rows = ranked.groupBy("region").agg(
            F.collect_list(
                F.struct(
                    "skill_rank", "id", "name", "skill", "ping", "queue_enter_at"
                )
            ).alias("players")
        ).collect()

        return [
            (rrow.region, sorted(rrow.players, key=lambda r: r.skill_rank))
            for rrow in region_rows
        ]
    #arma las partidas 3v3
    def form_matches(self, ranked_regions: list[tuple]) -> list[GameMatch]:
        """Driver-side windowing: form 3v3 matches within skill/ping spread."""
        now = time.time() * 1000
        new_matches: list[GameMatch] = []
        for region, ranked_players in ranked_regions:
            if len(ranked_players) < MATCH_SIZE:
                continue
            players_in_region = list(ranked_players)

            # --- driver-side windowing: form 3v3 matches within skill/ping spread ---
            i = 0
            while i + MATCH_SIZE <= len(players_in_region):
                window = players_in_region[i : i + MATCH_SIZE]
                waited_long = any(
                    now - p.queue_enter_at > MAX_QUEUE_WAIT for p in window
                )
                skill_spread = window[-1].skill - window[0].skill
                pings = [p.ping for p in window]
                ping_spread = max(pings) - min(pings)
                skill_ok = skill_spread <= (SKILL_WINDOW * 2 if waited_long else SKILL_WINDOW)
                ping_ok = ping_spread <= (PING_WINDOW * 2 if waited_long else PING_WINDOW)

                if skill_ok and ping_ok:
                    team_a = [p.id for p in window[:TEAM_SIZE]]
                    team_b = [p.id for p in window[TEAM_SIZE:]]
                    all_ids = team_a + team_b
                    avg_skill = round(sum(p.skill for p in window) / len(window))
                    avg_ping = round(sum(p.ping for p in window) / len(window))
                    match = GameMatch(
                        id=_match_id(),
                        players=all_ids,
                        team_a=team_a,
                        team_b=team_b,
                        avg_skill=avg_skill,
                        avg_ping=avg_ping,
                        region=region,
                        created_at=now,
                    )
                    self.state.matches.append(match)
                    self.state.matches_formed += 1
                    # transition players into match
                    for r in window:
                        p = self.state.players.get(r.id)
                        if not p:
                            continue
                        p.status = "in_match"
                        p.match_id = match.id
                        p.match_enter_at = now
                        p.x = random.random() * 1000
                        p.y = random.random() * 1000
                        p.target_x = random.random() * 1000
                        p.target_y = random.random() * 1000
                        p.kills = 0
                        p.deaths = 0
                        self.producer.on_player_enter_match(p)
                    if self.server_producer:
                        self.server_producer.on_server_spawned(match)
                    new_matches.append(match)
                    matched = set(all_ids)
                    players_in_region = [
                        p for p in players_in_region if p.id not in matched
                    ]
                    i = 0
                else:
                    i += 1
        return new_matches

    def run_matchmaking(self) -> list[GameMatch]:
        """Synchronous convenience wrapper (loop-thread only)."""
        return self.form_matches(self.rank_queue_spark())

    def on_match_ended(self, match: GameMatch, players: list[Player]) -> None:
        for p in players:
            self.producer.on_player_leave_match(p)
        if self.server_producer:
            self.server_producer.on_server_stopped(match)
        if self._last_pos_rows:
            ended_ids = {p.id for p in players}
            if ended_ids:
                self._last_pos_rows = [
                    row for row in self._last_pos_rows if row[0] not in ended_ids
                ]
        if self._last_server_rows:
            self._last_server_rows = [
                row for row in self._last_server_rows if row[0] != f"srv_{match.id}"
            ]
        self._server_alerted.discard(f"srv_{match.id}")

    def clear_alerts(self) -> None:
        self.state.alerts = []
        self.state.flagged_players.clear()
        self.state.server_alerts = []
        self._server_alerted.clear()
        for p in self.state.players.values():
            p.flagged_reasons = []
