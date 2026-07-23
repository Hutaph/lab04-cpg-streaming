import os
import pytest
import json
import time
from pathlib import Path
from confluent_kafka import Consumer
from domain.errors import PublishError
from domain.models import SourceFile
from domain.enums import ParseStatus
from infrastructure.messaging.kafka_producer import KafkaEventProducer
from infrastructure.state.sqlite_state_store import SqliteStateStore
from application.services.process_file import ProcessFileService


@pytest.mark.kafka
def test_kafka_producer_publish_and_consume():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = KafkaEventProducer(bootstrap_servers=bootstrap_servers)

    test_topic = "cpg.nodes"
    test_key = "test_file_id"
    test_payload = {
        "event_id": "test_evt_123",
        "event_type": "NODE_UPSERT",
        "event_time": "2026-07-21T12:00:00Z",
        "repository_id": "huggingface/transformers-pr-agent",
        "commit_sha": "abc12345",
        "file_id": "test_file_id",
        "file_path": "tests/fixtures/simple_sequence.py",
        "content_hash": "hash123",
        "parser_version": "1.0.0",
        "schema_version": "1.0",
        "node": {
            "node_id": "node_123",
            "node_type": "Variable",
            "name": "x",
            "qualified_name": "x",
            "ast_path": "Module.Assign.Name",
            "line_start": 1,
            "column_start": 0,
            "line_end": 1,
            "column_end": 1,
            "properties": {},
        },
    }

    # Publish event
    producer.publish_event(test_topic, test_key, test_payload)
    producer.flush()

    # Consume and verify
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": "test-kafka-integration-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    }
    consumer = Consumer(conf)
    consumer.subscribe([test_topic])

    msg = consumer.poll(timeout=5.0)
    assert msg is not None, "Message not received in time"
    assert msg.error() is None

    key = msg.key().decode("utf-8")
    val = json.loads(msg.value().decode("utf-8"))

    assert key == test_key
    assert val["event_id"] == "test_evt_123"
    assert val["event_type"] == "NODE_UPSERT"

    consumer.close()


def test_kafka_producer_delivery_failure_retains_state(tmp_path):
    # Configure SQLite state store with a temporary DB
    db_path = tmp_path / "test_state.sqlite3"
    state_store = SqliteStateStore(db_path=db_path, repository_id="huggingface/transformers-pr-agent")

    # Configure non-existent Kafka bootstrap server to force a failure (timeout 1s to run quickly)
    try:
        from confluent_kafka import Producer

        # Directly mock or inject non-existent bootstrap server with low delivery timeout
        # Wait, if we use KAFKA_DELIVERY_TIMEOUT_SECONDS = 1, it fails faster
        conf = {
            "bootstrap.servers": "localhost:12345",
            "acks": "all",
            "retries": 0,
            "message.timeout.ms": 1000,
        }
        mock_raw_producer = Producer(conf)
        producer = KafkaEventProducer(bootstrap_servers="localhost:12345", producer_instance=mock_raw_producer)
    except Exception:
        # Fallback to standard
        producer = KafkaEventProducer(bootstrap_servers="localhost:12345")

    # Configure mock/dummy objects for parser & repo
    class DummyRepo:
        def read_file(self, path):
            return b"x = 42"

        def resolve_path(self, path):
            return Path("dummy")

        def get_commit_hash(self):
            return "dummy_sha"

    class DummyParser:
        def parse_file(self, path, source, sha):
            from domain.models import ParsedFileGraph, FileMetadata, SourceFile
            from parsing.identifiers import IdentifierGenerator

            file_id = IdentifierGenerator.generate_file_id("huggingface/transformers-pr-agent", path)
            content_hash = IdentifierGenerator.generate_content_hash(source)
            meta = FileMetadata(
                file_id=file_id,
                repository_id="huggingface/transformers-pr-agent",
                file_path=str(path),
                content_hash=content_hash,
                size_bytes=6,
                line_count=1,
                function_count=0,
                class_count=0,
                import_count=0,
                node_count=0,
                edge_count=0,
                parse_duration_ms=0,
                parse_status=ParseStatus.SUCCESS,
                parser="python.ast",
            )
            src_file = SourceFile(
                repository_id="huggingface/transformers-pr-agent",
                repository_root="dummy",
                relative_path=str(path),
                commit_sha=sha,
                size_bytes=6,
            )
            return ParsedFileGraph(
                source_file=src_file, file_id=file_id, content_hash=content_hash, nodes=[], edges=[], metadata=meta
            )

    class DummyValidator:
        def validate(self, event_type, payload):
            pass

    service = ProcessFileService(
        repo_adapter=DummyRepo(),
        parser=DummyParser(),
        state_store=state_store,
        validator=DummyValidator(),
        writer=producer,
    )

    source_file = SourceFile(
        repository_id="huggingface/transformers-pr-agent",
        repository_root="dummy",
        relative_path="dummy.py",
        commit_sha="dummy_sha",
        size_bytes=6,
    )

    file_id = "huggingface/transformers-pr-agent:dummy.py"

    # Attempt execution - should raise PublishError due to Kafka unreachable
    with pytest.raises(PublishError):
        service.execute(source_file)

    # Verify that the SQLite state store has NOT committed the file state
    assert state_store.get(file_id) is None, "State should not have been committed"


@pytest.mark.kafka
def test_kafka_topic_routing_and_keys():
    """Verify that events of different types are correctly routed to their respective topics and use the file_id as the message key."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = KafkaEventProducer(bootstrap_servers=bootstrap_servers)

    test_file_id = "test_routing_file_id"

    events = [
        (
            "cpg.nodes",
            "NODE_UPSERT",
            {
                "file_id": test_file_id,
                "event_type": "NODE_UPSERT",
                "event_id": "n_up",
                "schema_version": "1.0",
                "event_time": "2026-07-21T12:00:00Z",
                "repository_id": "repo",
                "commit_sha": "sha",
                "file_path": "a.py",
                "content_hash": "h",
                "parser_version": "1.0",
                "node": {
                    "node_id": "n1",
                    "node_type": "Module",
                    "name": "M",
                    "qualified_name": "M",
                    "ast_path": "M",
                    "line_start": None,
                    "column_start": None,
                    "line_end": None,
                    "column_end": None,
                    "properties": {},
                },
            },
        ),
        (
            "cpg.edges",
            "EDGE_UPSERT",
            {
                "file_id": test_file_id,
                "event_type": "EDGE_UPSERT",
                "event_id": "e_up",
                "schema_version": "1.0",
                "event_time": "2026-07-21T12:00:00Z",
                "repository_id": "repo",
                "commit_sha": "sha",
                "file_path": "a.py",
                "content_hash": "h",
                "parser_version": "1.0",
                "edge": {
                    "edge_id": "e1",
                    "source_id": "s",
                    "target_id": "t",
                    "edge_type": "AST_CHILD",
                    "properties": {},
                },
            },
        ),
        (
            "source.metadata",
            "FILE_METADATA_UPSERT",
            {
                "file_id": test_file_id,
                "event_type": "FILE_METADATA_UPSERT",
                "event_id": "m_up",
                "schema_version": "1.0",
                "event_time": "2026-07-21T12:00:00Z",
                "repository_id": "repo",
                "commit_sha": "sha",
                "file_path": "a.py",
                "content_hash": "h",
                "parser_version": "1.0",
                "metadata": {
                    "size_bytes": 10,
                    "line_count": 2,
                    "function_count": 0,
                    "class_count": 0,
                    "import_count": 0,
                    "node_count": 0,
                    "edge_count": 0,
                    "parse_duration_ms": 1,
                    "parse_status": "SUCCESS",
                    "parser": "python.ast",
                },
            },
        ),
        (
            "parser.errors",
            "PARSER_ERROR",
            {
                "file_id": test_file_id,
                "event_type": "PARSER_ERROR",
                "event_id": "err",
                "schema_version": "1.0",
                "event_time": "2026-07-21T12:00:00Z",
                "repository_id": "repo",
                "commit_sha": "sha",
                "file_path": "a.py",
                "content_hash": "h",
                "parser_version": "1.0",
                "error": {"error_type": "SyntaxError", "message": "error msg", "retryable": False},
            },
        ),
    ]

    for topic, _, payload in events:
        producer.publish_event(topic, test_file_id, payload)
    producer.flush()

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"test-kafka-routing-verifier-group-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    }

    consumer = Consumer(conf)
    all_topics = ["cpg.nodes", "cpg.edges", "source.metadata", "parser.errors"]
    consumer.subscribe(all_topics)
    time.sleep(3.0)  # Give coordinator time to assign partitions

    received_events = {}

    empty_polls = 0
    while empty_polls < 15 and len(received_events) < len(events):
        msg = consumer.poll(1.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            continue
        empty_polls = 0

        topic = msg.topic()
        key = msg.key().decode("utf-8")
        try:
            val = json.loads(msg.value().decode("utf-8"))
        except json.JSONDecodeError:
            continue

        if key == test_file_id:
            received_events[topic] = val

    consumer.close()

    assert len(received_events) == len(events), f"Expected {len(events)} events, got {len(received_events)}"
    for topic, expected_type, _ in events:
        assert topic in received_events
        assert received_events[topic]["event_type"] == expected_type


@pytest.mark.kafka
def test_kafka_per_topic_partition_consistency():
    """Verify that multiple events with the same file_id on a single topic route to the same partition, and offsets are strictly increasing."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = KafkaEventProducer(bootstrap_servers=bootstrap_servers)

    test_file_id = "consistency_test_file_id"
    test_topic = "cpg.nodes"

    # Send 5 events for the same file ID
    for i in range(5):
        payload = {
            "event_id": f"node_consistency_{i}",
            "event_type": "NODE_UPSERT",
            "event_time": "2026-07-21T12:00:00Z",
            "repository_id": "repo",
            "commit_sha": "sha",
            "file_id": test_file_id,
            "file_path": "a.py",
            "content_hash": "h",
            "parser_version": "1.0",
            "schema_version": "1.0",
            "node": {
                "node_id": f"n{i}",
                "node_type": "Module",
                "name": "M",
                "qualified_name": "M",
                "ast_path": "M",
                "line_start": None,
                "column_start": None,
                "line_end": None,
                "column_end": None,
                "properties": {},
            },
        }
        producer.publish_event(test_topic, test_file_id, payload)
    producer.flush()

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"test-kafka-consistency-verifier-group-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    }

    consumer = Consumer(conf)
    consumer.subscribe([test_topic])
    time.sleep(3.0)  # Wait for coordinator partition assignment

    received_messages = []
    empty_polls = 0
    while empty_polls < 15 and len(received_messages) < 5:
        msg = consumer.poll(1.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            continue
        empty_polls = 0

        key = msg.key().decode("utf-8")
        if key == test_file_id:
            received_messages.append(msg)

    consumer.close()

    assert len(received_messages) == 5

    # Assert partition consistency
    first_partition = received_messages[0].partition()
    for msg in received_messages:
        assert msg.partition() == first_partition, (
            "All events for the same file_id on this topic must go to the same partition"
        )

    # Assert offset ordering
    offsets = [msg.offset() for msg in received_messages]
    assert offsets == sorted(offsets), "Offsets must be strictly increasing"
