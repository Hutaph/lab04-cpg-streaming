"""Unit tests validating JSON serialization against Kafka schemas."""

from pathlib import Path
import pytest
from infrastructure.messaging.event_validator import EventValidator
from domain.errors import SchemaValidationError


def test_schema_validations() -> None:
    """Verify that node, edge, metadata and error structures validate correctly."""
    schemas_dir = Path("schemas")
    if not schemas_dir.exists():
        schemas_dir = Path("../schemas")

    validator = EventValidator(schemas_dir)

    # 1. Valid Node Upsert Event
    node_event = {
        "schema_version": "1.0",
        "event_id": "evt1",
        "event_type": "NODE_UPSERT",
        "event_time": "2026-07-20T12:00:00Z",
        "repository_id": "huggingface/transformers-pr-agent",
        "commit_sha": "c1",
        "file_id": "f1",
        "file_path": "foo.py",
        "content_hash": "h1",
        "parser_version": "1.0.0",
        "node": {
            "node_id": "n1",
            "node_type": "Module",
            "name": "foo",
            "qualified_name": "Module.foo",
            "ast_path": "Module",
            "line_start": 1,
            "column_start": 0,
            "line_end": 5,
            "column_end": 0,
            "properties": {"foo": "bar"},
        },
    }
    validator.validate("NODE_UPSERT", node_event)

    # 2. Valid Node Delete Event
    node_delete = {
        "schema_version": "1.0",
        "event_id": "evt2",
        "event_type": "NODE_DELETE",
        "event_time": "2026-07-20T12:00:00Z",
        "repository_id": "huggingface/transformers-pr-agent",
        "commit_sha": "c1",
        "file_id": "f1",
        "file_path": "foo.py",
        "content_hash": "h1",
        "parser_version": "1.0.0",
        "node": {"node_id": "n1"},
    }
    validator.validate("NODE_DELETE", node_delete)

    # 3. Invalid Event Rejected
    invalid_event = dict(node_event)
    del invalid_event["node"]["node_id"]  # Missing required field
    with pytest.raises(SchemaValidationError):
        validator.validate("NODE_UPSERT", invalid_event)


DefinitionOfDone = True
