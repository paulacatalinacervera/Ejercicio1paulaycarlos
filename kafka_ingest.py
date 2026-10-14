from __future__ import annotations

import json
import threading
import time

from server_producer import KAFKA_BROKER, KAFKA_TOPIC
from models import ServerMetricEvent
from state import GameState


class KafkaIngest(threading.Thread):
    def __init__(
        self,
        state: GameState,
        broker: str | None = None,
        topic: str | None = None,
    ) -> None:
        super().__init__(daemon=True, name="kafka-server-ingest")
        self.state = state
        self.broker = broker or KAFKA_BROKER
        self.topic = topic or KAFKA_TOPIC
        self.ingested = 0
        self.last_message_at: float = 0.0
        self.alive = False
        self.error: str | None = None
        self._consumer = None
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                from kafka import KafkaConsumer  # type: ignore

                self._consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.broker,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    group_id="telemetry-server-ingest",
                    request_timeout_ms=20000,
                    session_timeout_ms=10000,
                    max_poll_records=200,
                )
            except Exception as exc:  # noqa: BLE001
                self.error = f"cannot connect to broker {self.broker}: {exc}"
                self.alive = False
                self._stop.wait(3.0)
                continue

            self.alive = True
            self.error = None
            try:
                while not self._stop.is_set():
                    records = self._consumer.poll(timeout_ms=1000)
                    if not records:
                        continue
                    for batch in records.values():
                        for msg in batch:
                            if self._stop.is_set():
                                break
                            try:
                                data = json.loads(msg.value.decode("utf-8"))
                                ev = ServerMetricEvent(**data)
                                self.state.append_server_event(ev)
                                self.ingested += 1
                                self.last_message_at = time.time() * 1000
                            except Exception:
                                continue
            except Exception as exc:
                self.error = str(exc)
                self.alive = False
                self._stop.wait(3.0)
            finally:
                try:
                    if self._consumer:
                        self._consumer.close()
                except Exception:
                    pass
                self._consumer = None