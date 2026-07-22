"""Unit tests for KafkaEventProducer adapter class."""

from unittest.mock import Mock
import pytest
from infrastructure.messaging.kafka_producer import KafkaEventProducer
from domain.errors import PublishError


def test_producer_publish_and_flush_success() -> None:
    """Verify happy path of publish and flush."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0

    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.publish_event("test-topic", "test-key", {"foo": "bar"})
    producer.flush()

    mock_raw_producer.produce.assert_called_once()
    mock_raw_producer.flush.assert_called_once_with(timeout=10.0)


def test_producer_buffer_error_handling() -> None:
    """Verify that BufferError from confluent_kafka translates to a PublishError."""
    mock_raw_producer = Mock()
    mock_raw_producer.produce.side_effect = BufferError("Local queue full")

    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    with pytest.raises(PublishError) as exc_info:
        producer.publish_event("test-topic", "test-key", {"foo": "bar"})

    assert "Failed to queue message" in str(exc_info.value)


def test_producer_delivery_callback_error_detection() -> None:
    """Verify that delivery callback reports errors and flush fails on reported errors."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0

    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    # Simulate error in delivery callback
    # We directly invoke the callback method for testing
    producer.publish_event("test-topic", "test-key", {"foo": "bar"})

    # Trigger delivery report failure callback
    producer._delivery_report("Broker unavailable", None)

    with pytest.raises(PublishError) as exc_info:
        producer.flush()

    assert "Delivery failures during event streaming" in str(exc_info.value)
    assert "Broker unavailable" in str(exc_info.value)


def test_producer_flush_timeout_raises_publish_error() -> None:
    """Verify that if flush() returns a non-zero count of undelivered messages, it raises PublishError."""
    mock_raw_producer = Mock()
    # Mock flush to return 2 (2 undelivered messages)
    mock_raw_producer.flush.return_value = 2

    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.publish_event("test-topic", "test-key", {"foo": "bar"})

    with pytest.raises(PublishError) as exc_info:
        producer.flush()

    assert "Flush timeout: 2 messages remained undelivered" in str(exc_info.value)


def test_producer_clear_errors_isolates_batches() -> None:
    """Verify that clear_errors resets error state between batches."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0

    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer._delivery_report("Old error", None)
    producer.clear_errors()

    # Flush should not raise since errors were cleared
    producer.flush()
