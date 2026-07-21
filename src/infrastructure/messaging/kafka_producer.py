"""Kafka adapter implementing EventPublisherPort for event production streaming."""

import json
import logging
from typing import Any
from application.ports import EventPublisherPort
from domain.errors import PublishError

logger = logging.getLogger(__name__)


class KafkaEventProducer(EventPublisherPort):
    """Adapter publishing JSON-serialized CPG events directly to Apache Kafka topics."""

    def __init__(self, bootstrap_servers: str, producer_instance: Any = None):
        self.bootstrap_servers = bootstrap_servers
        self._producer = producer_instance
        self._initialized = producer_instance is not None
        self._errors: list[str] = []

    def _init_producer(self) -> None:
        if self._initialized:
            return
        try:
            from confluent_kafka import Producer

            conf = {
                "bootstrap.servers": self.bootstrap_servers,
                "acks": "all",
                "retries": 5,
                "delivery.timeout.ms": 10000,
                "enable.idempotence": True,
            }
            self._producer = Producer(conf)
            self._initialized = True
        except Exception as exc:
            raise PublishError(f"Failed to initialize Kafka producer: {exc}") from exc

    def _delivery_report(self, err: Any, msg: Any) -> None:
        """Callback triggered by poll() or flush() when a message is acknowledged or failed."""
        if err is not None:
            err_msg = f"Message delivery failed: {err}"
            logger.error(err_msg)
            self._errors.append(err_msg)
        else:
            logger.debug(f"Message delivered to topic {msg.topic()} partition {msg.partition()} offset {msg.offset()}")

    def publish_event(self, topic: str, event_key: str, event: dict[str, Any]) -> None:
        """Queue event payload to Kafka with partitioning key."""
        self._init_producer()
        try:
            # Serialize event to JSON UTF-8
            payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
            key_bytes = event_key.encode("utf-8") if isinstance(event_key, str) else event_key

            # Produce message (asynchronous)
            self._producer.produce(
                topic=topic,
                key=key_bytes,
                value=payload,
                callback=self._delivery_report,
            )
            # Serve delivery callbacks periodically
            self._producer.poll(0)
        except Exception as exc:
            raise PublishError(f"Failed to queue message to Kafka topic {topic}: {exc}") from exc

    def flush(self) -> None:
        """Blocks until all outstanding messages in the queue are sent and checks for errors."""
        if self._initialized and self._producer:
            try:
                # Blocks until all messages in the queue are delivered/failed
                self._producer.flush(timeout=10.0)
                # Check if any messages in the batch encountered errors in delivery callback
                if self._errors:
                    errs_summary = "; ".join(self._errors)
                    self._errors.clear()
                    raise PublishError(f"Delivery failures during event streaming: {errs_summary}")
            except PublishError:
                raise
            except Exception as exc:
                raise PublishError(f"Failed to flush Kafka producer: {exc}") from exc
