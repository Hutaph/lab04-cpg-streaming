"""Kafka adapter implementing EventPublisherPort for event production streaming."""

import json
from typing import Any
from src.application.ports import EventPublisherPort
from src.domain.errors import PublishError


class KafkaEventProducer(EventPublisherPort):
    """Adapter publishing JSON-serialized CPG events directly to Apache Kafka topics."""

    def __init__(self, bootstrap_servers: str, producer_instance: Any = None):
        self.bootstrap_servers = bootstrap_servers
        self._producer = producer_instance
        self._initialized = producer_instance is not None

    def _init_producer(self) -> None:
        if self._initialized:
            return
        try:
            from kafka import KafkaProducer
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                retries=5,
                acks="all",
            )
            self._initialized = True
        except ImportError as exc:
            raise PublishError(
                "kafka-python package is missing. Install dependency to use Kafka production adapter."
            ) from exc
        except Exception as exc:
            raise PublishError(f"Failed to initialize Kafka producer: {exc}") from exc

    def publish_event(self, topic: str, event_key: str, event: dict[str, Any]) -> None:
        """Publishes event payload synchronously to Kafka with partitioning key."""
        self._init_producer()
        try:
            # Send message asynchronously and verify response block to ensure delivery
            future = self._producer.send(topic, key=event_key, value=event)
            # Synchronous verification of delivery receipt
            future.get(timeout=10.0)
        except Exception as exc:
            raise PublishError(f"Failed to deliver message to Kafka topic {topic}: {exc}") from exc

    def flush(self) -> None:
        """Blocks until all outstanding messages in the queue are sent."""
        if self._initialized and self._producer:
            try:
                self._producer.flush()
            except Exception as exc:
                raise PublishError(f"Failed to flush Kafka producer: {exc}") from exc
