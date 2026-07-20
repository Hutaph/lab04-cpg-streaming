"""Kafka adapter implementing EventWriterPort for production message streaming."""

from typing import Any
from src.application.ports import EventWriterPort


class KafkaEventProducer(EventWriterPort):
    """Adapter publishing JSON-serialized CPG events directly to Apache Kafka topics."""

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers

    def write_event(self, topic: str, event_key: str, event: dict[str, Any]) -> None:
        """TODO: Initialize kafka-python producer and push messages using deterministic key."""
        raise NotImplementedError("KafkaEventProducer will be implemented in Phase 7")

    def flush(self) -> None:
        """TODO: Call flush on underlying kafka producer block until message delivery."""
        raise NotImplementedError("KafkaEventProducer will be implemented in Phase 7")
