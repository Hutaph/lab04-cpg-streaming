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
    # The producer should be poisoned
    with pytest.raises(PublishError) as exc_info2:
        producer.publish_event("test-topic", "test-key2", {"foo": "baz"})
    assert "failed state and cannot be reused" in str(exc_info2.value)


def test_producer_delivery_callback_error_detection() -> None:
    """Verify that delivery callback reports errors and flush fails on reported errors."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0

    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

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


def test_failed_producer_cannot_be_reused() -> None:
    """Verify that a producer that has failed cannot execute write or flush again."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0

    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)
    producer.publish_event("test-topic", "key1", {"x": 1})

    # Trigger callback failure to poison it
    producer._delivery_report("Some async error", None)
    with pytest.raises(PublishError):
        producer.flush()

    # Try publishing again
    with pytest.raises(PublishError) as exc_info:
        producer.publish_event("test-topic", "key2", {"x": 2})
    assert "failed state and cannot be reused" in str(exc_info.value)

    # Try flushing again
    with pytest.raises(PublishError) as exc_info2:
        producer.flush()
    assert "failed state and cannot be reused" in str(exc_info2.value)


def test_mid_batch_buffer_error_poison_producer() -> None:
    """Verify that if a produce call mid-batch raises BufferError, the producer is poisoned."""
    mock_raw_producer = Mock()
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    # First event enqueued successfully
    producer.publish_event("test-topic", "key1", {"x": 1})

    # Second event triggers BufferError
    mock_raw_producer.produce.side_effect = BufferError("Queue full")
    with pytest.raises(PublishError):
        producer.publish_event("test-topic", "key2", {"x": 2})

    # Subsequent event cannot even call produce because producer is poisoned
    mock_raw_producer.produce.side_effect = None
    with pytest.raises(PublishError) as exc_info:
        producer.publish_event("test-topic", "key3", {"x": 3})
    assert "failed state and cannot be reused" in str(exc_info.value)


def test_callback_failure_poison_producer() -> None:
    """Verify that a delivery callback failure poisons the producer."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.publish_event("test-topic", "key1", {"x": 1})
    producer._delivery_report("Delivery failure", None)

    with pytest.raises(PublishError):
        producer.flush()

    with pytest.raises(PublishError) as exc_info:
        producer.publish_event("test-topic", "key2", {"x": 2})
    assert "failed state and cannot be reused" in str(exc_info.value)


def test_flush_timeout_poison_producer() -> None:
    """Verify that flush timeout (undelivered > 0) poisons the producer."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 1
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.publish_event("test-topic", "key1", {"x": 1})

    with pytest.raises(PublishError):
        producer.flush()

    with pytest.raises(PublishError) as exc_info:
        producer.publish_event("test-topic", "key2", {"x": 2})
    assert "failed state and cannot be reused" in str(exc_info.value)


def test_successful_flush_allows_next_batch() -> None:
    """Verify that successful flush leaves producer in READY state allowing next batch."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    # Batch A
    producer.publish_event("test-topic", "key1", {"x": 1})
    producer.flush()

    # Batch B (allowed)
    producer.publish_event("test-topic", "key2", {"x": 2})
    producer.flush()
