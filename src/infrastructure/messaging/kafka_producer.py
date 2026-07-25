"""Kafka adapter implementing EventPublisherPort for event production streaming."""

import json
from enum import Enum
import logging
from typing import Any
from application.ports import EventPublisherPort
from domain.errors import PublishError, EventSerializationError

logger = logging.getLogger(__name__)


class ProducerState(Enum):
    READY = "READY"
    STAGING = "STAGING"
    PUBLISHING = "PUBLISHING"
    FAILED = "FAILED"


class KafkaEventProducer(EventPublisherPort):
    """Adapter publishing JSON-serialized CPG events directly to Apache Kafka topics."""

    def __init__(self, bootstrap_servers: str, producer_instance: Any = None, flush_timeout_seconds: float = 60.0):
        self.bootstrap_servers = bootstrap_servers
        self.flush_timeout_seconds = flush_timeout_seconds
        self._producer = producer_instance
        self._initialized = producer_instance is not None
        self._errors: list[str] = []
        self._failed = False
        self._batch_records: list[tuple[str, bytes, bytes]] = []
        self._state = ProducerState.READY

    def _init_producer(self) -> None:
        if self._state == ProducerState.FAILED:
            raise PublishError("Producer is in a failed state and cannot be reused")
        if self._initialized:
            return
        try:
            from confluent_kafka import Producer

            delivery_timeout_ms = max(10_000, int(self.flush_timeout_seconds * 1000))
            conf = {
                "bootstrap.servers": self.bootstrap_servers,
                "acks": "all",
                "retries": 5,
                "delivery.timeout.ms": delivery_timeout_ms,
                "enable.idempotence": True,
            }
            self._producer = Producer(conf)
            self._initialized = True
        except Exception as exc:
            self._state = ProducerState.FAILED
            self._failed = True
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
        if self._state == ProducerState.FAILED:
            raise PublishError("Producer is in a failed state and cannot be reused")
        if self._state == ProducerState.PUBLISHING:
            raise PublishError("Cannot publish event while producer is flushing")

        self._state = ProducerState.STAGING

        try:
            # Check for non-finite values (NaN, Infinity) which are invalid in JSON
            payload = json.dumps(event, ensure_ascii=False, allow_nan=False).encode("utf-8")
            key_bytes = event_key.encode("utf-8") if isinstance(event_key, str) else event_key
            self._batch_records.append((topic, key_bytes, payload))
        except (ValueError, TypeError) as exc:
            # Serialization errors occur BEFORE any Kafka side-effects, so producer remains READY/reusable!
            self._batch_records.clear()
            self._state = ProducerState.READY
            raise EventSerializationError(f"Failed to serialize event for topic {topic}: {exc}") from exc

    def flush(self) -> None:
        """Blocks until all outstanding messages in the queue are sent and checks for errors."""
        if self._state == ProducerState.FAILED:
            raise PublishError("Producer is in a failed state and cannot be reused")

        if self._state == ProducerState.READY:
            return

        self._init_producer()

        # Detach records atomically to prevent double publish or carry over
        records = tuple(self._batch_records)
        self._batch_records.clear()

        self._state = ProducerState.PUBLISHING

        # Enqueue loop
        try:
            for topic, key_bytes, payload in records:
                self._producer.produce(
                    topic=topic,
                    key=key_bytes,
                    value=payload,
                    callback=self._delivery_report,
                )
                self._producer.poll(0)
        except Exception as exc:
            self._state = ProducerState.FAILED
            self._failed = True
            raise PublishError(f"Failed to queue messages to Kafka: {exc}") from exc

        try:
            # Blocks until all messages in the queue are delivered/failed
            undelivered = self._producer.flush(timeout=self.flush_timeout_seconds)
            if undelivered > 0:
                self._state = ProducerState.FAILED
                self._failed = True
                self._errors.clear()
                raise PublishError(f"Flush timeout: {undelivered} messages remained undelivered after timeout")

            # Check if any messages in the batch encountered errors in delivery callback
            if self._errors:
                self._state = ProducerState.FAILED
                self._failed = True
                errs_summary = "; ".join(self._errors)
                self._errors.clear()
                raise PublishError(f"Delivery failures during event streaming: {errs_summary}")
        except PublishError:
            raise
        except Exception as exc:
            self._state = ProducerState.FAILED
            self._failed = True
            self._errors.clear()
            raise PublishError(f"Failed to flush Kafka producer: {exc}") from exc

        self._state = ProducerState.READY
