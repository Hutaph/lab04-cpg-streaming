"""Unit tests for KafkaEventProducer adapter class."""

from unittest.mock import Mock
import pytest
from infrastructure.messaging.kafka_producer import KafkaEventProducer
from domain.errors import PublishError, EventSerializationError


def test_producer_publish_and_flush_success() -> None:
    """Verify happy path of publish and flush."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0

    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.publish_event("test-topic", "test-key", {"foo": "bar"})
    producer.flush()

    mock_raw_producer.produce.assert_called_once()
    mock_raw_producer.flush.assert_called_once_with(timeout=60.0)


def test_producer_buffer_error_handling() -> None:
    """Verify that BufferError from confluent_kafka translates to a PublishError."""
    mock_raw_producer = Mock()
    mock_raw_producer.produce.side_effect = BufferError("Local queue full")

    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.publish_event("test-topic", "test-key", {"foo": "bar"})
    with pytest.raises(PublishError) as exc_info:
        producer.flush()

    assert "Failed to queue messages to Kafka" in str(exc_info.value)
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

    # First and second events buffered
    producer.publish_event("test-topic", "key1", {"x": 1})
    producer.publish_event("test-topic", "key2", {"x": 2})

    # When flush is called, first succeeds, second triggers BufferError
    mock_raw_producer.produce.side_effect = [None, BufferError("Queue full")]
    with pytest.raises(PublishError):
        producer.flush()

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


# --- New Serialization Boundary Unit Tests ---


def test_batch_is_fully_serialized_before_first_produce() -> None:
    """Verify that all events are serialized before calling first confluent_kafka produce."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.publish_event("test-topic", "key1", {"x": 1})
    producer.publish_event("test-topic", "key2", {"x": 2})

    # Assert no produce calls occurred yet
    mock_raw_producer.produce.assert_not_called()

    producer.flush()
    # Now they are produced
    assert mock_raw_producer.produce.call_count == 2


def test_middle_serialization_failure_prevents_all_kafka_produce() -> None:
    """Verify that serialization failure on a middle event prevents any event from being produced."""
    mock_raw_producer = Mock()
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    # Event 1 is fine
    producer.publish_event("test-topic", "key1", {"x": 1})

    # Event 2 has type that raises TypeError in json.dumps (e.g., set is not JSON serializable)
    with pytest.raises(EventSerializationError):
        producer.publish_event("test-topic", "key2", {"x": {1, 2, 3}})

    # Assert no produce calls occurred
    mock_raw_producer.produce.assert_not_called()

    # Also assert the producer is still technically READY (not failed)
    assert not producer._failed


def test_final_serialization_failure_prevents_all_kafka_produce() -> None:
    """Verify that serialization failure on the final event prevents any event from being produced."""
    mock_raw_producer = Mock()
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.publish_event("test-topic", "key1", {"x": 1})
    producer.publish_event("test-topic", "key2", {"x": 2})

    # Event 3 has non-serializable object
    class NonSerializable:
        pass

    with pytest.raises(EventSerializationError):
        producer.publish_event("test-topic", "key3", {"x": NonSerializable()})

    mock_raw_producer.produce.assert_not_called()
    assert not producer._failed


def test_unicode_event_serialization() -> None:
    """Verify that Unicode symbols and Vietnamese pathing serialize correctly without errors."""
    mock_raw_producer = Mock()
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    unicode_event = {"path": "thư_mục/mã.py", "symbol": "biến_số", "data": "dữ_liệu"}
    producer.publish_event("test-topic", "key1", unicode_event)

    assert len(producer._batch_records) == 1
    topic, key, payload = producer._batch_records[0]

    # Payload must be valid UTF-8 JSON bytes
    import json

    decoded = json.loads(payload.decode("utf-8"))
    assert decoded == unicode_event


def test_non_finite_json_value_is_rejected() -> None:
    """Verify that NaN and Infinity JSON values are explicitly rejected during serialization."""
    mock_raw_producer = Mock()
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    with pytest.raises(EventSerializationError) as exc_info:
        producer.publish_event("test-topic", "key1", {"value": float("nan")})

    assert "Failed to serialize event" in str(exc_info.value)
    mock_raw_producer.produce.assert_not_called()


# --- Staged Batch Lifecycle and State-Transition Tests ---


def test_serialization_failure_discards_staged_records() -> None:
    """Verify that if some events are staged, and a later event fails serialization, all staged events are discarded."""
    mock_raw_producer = Mock()
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.publish_event("test-topic", "key1", {"x": 1})
    producer.publish_event("test-topic", "key2", {"x": 2})
    assert len(producer._batch_records) == 2

    # Event 3 fails serialization
    with pytest.raises(EventSerializationError):
        producer.publish_event("test-topic", "key3", {"x": {1, 2}})

    # Batch records must be cleared completely
    assert len(producer._batch_records) == 0


def test_records_from_failed_serialization_never_reach_next_flush() -> None:
    """Verify that if a batch fails serialization, those records are not sent when flushing a later clean batch."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    # First attempt fails serialization on second record
    producer.publish_event("test-topic", "key1", {"x": 1})
    with pytest.raises(EventSerializationError):
        producer.publish_event("test-topic", "key2", {"x": {1, 2}})

    # Second attempt succeeds
    producer.publish_event("test-topic", "key3", {"y": 2})
    producer.flush()

    # Verify only key3 was produced
    assert mock_raw_producer.produce.call_count == 1
    produced_args = mock_raw_producer.produce.call_args[1]
    assert produced_args["key"] == b"key3"


def test_clean_batch_can_start_after_pre_network_serialization_failure() -> None:
    """Verify that we can start and successfully publish a new clean batch on the same producer after a serialization failure."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    with pytest.raises(EventSerializationError):
        producer.publish_event("test-topic", "key1", {"x": {1, 2}})

    # Clean batch
    producer.publish_event("test-topic", "key2", {"y": 2})
    producer.flush()

    mock_raw_producer.produce.assert_called_once()
    assert mock_raw_producer.produce.call_args[1]["key"] == b"key2"


def test_serialization_failure_does_not_call_underlying_flush() -> None:
    """Verify that serialization failure doesn't trigger any network flush or poll."""
    mock_raw_producer = Mock()
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    with pytest.raises(EventSerializationError):
        producer.publish_event("test-topic", "key1", {"x": {1, 2}})

    mock_raw_producer.flush.assert_not_called()
    mock_raw_producer.poll.assert_not_called()


def test_successful_flush_clears_staged_batch() -> None:
    """Verify successful flush clears _batch_records."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.publish_event("test-topic", "key1", {"x": 1})
    assert len(producer._batch_records) == 1
    producer.flush()
    assert len(producer._batch_records) == 0


def test_second_flush_does_not_republish_previous_batch() -> None:
    """Verify second flush on the same producer doesn't republish the same records."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.publish_event("test-topic", "key1", {"x": 1})
    producer.flush()
    assert mock_raw_producer.produce.call_count == 1

    # Second flush is a safe no-op
    producer.flush()
    assert mock_raw_producer.produce.call_count == 1


def test_empty_flush_is_safe() -> None:
    """Verify empty flush is a safe no-op."""
    mock_raw_producer = Mock()
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.flush()
    mock_raw_producer.produce.assert_not_called()
    mock_raw_producer.flush.assert_not_called()


def test_new_batch_after_success_contains_only_new_records() -> None:
    """Verify new batch contains only new records."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.publish_event("test-topic", "key1", {"x": 1})
    producer.flush()

    producer.publish_event("test-topic", "key2", {"y": 2})
    assert len(producer._batch_records) == 1
    assert producer._batch_records[0][1] == b"key2"


def test_failed_producer_rejects_publish_event() -> None:
    """Verify publish_event raises error if producer is failed."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.publish_event("test-topic", "key1", {"x": 1})
    producer._delivery_report("Delivery failure", None)
    with pytest.raises(PublishError):
        producer.flush()

    # publish raises error
    with pytest.raises(PublishError):
        producer.publish_event("test-topic", "key2", {"y": 2})


def test_failed_producer_rejects_second_flush() -> None:
    """Verify second flush raises error if first failed."""
    mock_raw_producer = Mock()
    mock_raw_producer.flush.return_value = 0
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", producer_instance=mock_raw_producer)

    producer.publish_event("test-topic", "key1", {"x": 1})
    producer._delivery_report("Delivery failure", None)
    with pytest.raises(PublishError):
        producer.flush()

    # flush raises error
    with pytest.raises(PublishError):
        producer.flush()
