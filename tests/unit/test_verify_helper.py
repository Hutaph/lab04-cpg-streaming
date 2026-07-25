"""Unit tests for verification helper functions in src/infrastructure/verification/."""

from infrastructure.verification.kafka_connect import (
    calculate_dlq_delta,
    redact_connector_config,
)


def test_redact_connector_config() -> None:
    """Verify that sensitive config keys are redacted successfully."""
    config = {
        "topics": "cpg.nodes",
        "neo4j.authentication.basic.password": "supersecurepassword123",
        "password": "hello",
        "mongodb.connection.uri": "mongodb://root:pass@localhost:27017",
        "batch.size": 100,
    }
    redacted = redact_connector_config(config)
    assert redacted["topics"] == "cpg.nodes"
    assert redacted["neo4j.authentication.basic.password"] == "REDACTED (len=22)"
    assert redacted["password"] == "REDACTED (len=5)"
    assert redacted["mongodb.connection.uri"] == "REDACTED (len=35)"
    assert redacted["batch.size"] == 100


def test_calculate_dlq_delta() -> None:
    """Verify that DLQ offset delta calculations are correct across partitions."""
    # Scenario A: empty before, some after
    before: dict[int, int] = {}
    after = {0: 10, 1: 5}
    assert calculate_dlq_delta(before, after) == 15

    # Scenario B: same values
    before = {0: 10, 1: 5}
    after = {0: 10, 1: 5}
    assert calculate_dlq_delta(before, after) == 0

    # Scenario C: increased offset
    before = {0: 10, 1: 5}
    after = {0: 12, 1: 5}
    assert calculate_dlq_delta(before, after) == 2

    # Scenario D: new partition added
    before = {0: 10}
    after = {0: 10, 1: 5}
    assert calculate_dlq_delta(before, after) == 5
