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


@pytest.mark.parametrize(
    "event_type,payload",
    [
        (
            "NODE_UPSERT",
            {
                "schema_version": "1.0",
                "event_id": "e1",
                "event_type": "NODE_UPSERT",
                "event_time": "2026-07-20T12:00:00Z",
                "repository_id": "repo",
                "commit_sha": "sha",
                "file_id": "fid",
                "file_path": "a.py",
                "content_hash": "ch",
                "parser_version": "1.0.0",
                "node": {
                    "node_id": "n1",
                    "node_type": "Module",
                    "name": "foo",
                    "qualified_name": "foo",
                    "ast_path": "foo",
                    "properties": {},
                },
            },
        ),
        (
            "NODE_DELETE",
            {
                "schema_version": "1.0",
                "event_id": "e2",
                "event_type": "NODE_DELETE",
                "event_time": "2026-07-20T12:00:00Z",
                "repository_id": "repo",
                "commit_sha": "sha",
                "file_id": "fid",
                "file_path": "a.py",
                "content_hash": "ch",
                "parser_version": "1.0.0",
                "node": {"node_id": "n1"},
            },
        ),
        (
            "EDGE_UPSERT",
            {
                "schema_version": "1.0",
                "event_id": "e3",
                "event_type": "EDGE_UPSERT",
                "event_time": "2026-07-20T12:00:00Z",
                "repository_id": "repo",
                "commit_sha": "sha",
                "file_id": "fid",
                "file_path": "a.py",
                "content_hash": "ch",
                "parser_version": "1.0.0",
                "edge": {
                    "edge_id": "ed1",
                    "source_id": "n1",
                    "target_id": "n2",
                    "edge_type": "AST_CHILD",
                    "properties": {},
                },
            },
        ),
        (
            "EDGE_DELETE",
            {
                "schema_version": "1.0",
                "event_id": "e4",
                "event_type": "EDGE_DELETE",
                "event_time": "2026-07-20T12:00:00Z",
                "repository_id": "repo",
                "commit_sha": "sha",
                "file_id": "fid",
                "file_path": "a.py",
                "content_hash": "ch",
                "parser_version": "1.0.0",
                "edge": {"edge_id": "ed1"},
            },
        ),
        (
            "FILE_METADATA_UPSERT",
            {
                "schema_version": "1.0",
                "event_id": "e5",
                "event_type": "FILE_METADATA_UPSERT",
                "event_time": "2026-07-20T12:00:00Z",
                "repository_id": "repo",
                "commit_sha": "sha",
                "file_id": "fid",
                "file_path": "a.py",
                "content_hash": "ch",
                "parser_version": "1.0.0",
                "metadata": {
                    "size_bytes": 10,
                    "line_count": 2,
                    "function_count": 0,
                    "class_count": 0,
                    "import_count": 0,
                    "node_count": 1,
                    "edge_count": 0,
                    "parse_duration_ms": 1,
                    "parse_status": "SUCCESS",
                    "parser": "python.ast",
                },
            },
        ),
        (
            "PARSER_ERROR",
            {
                "schema_version": "1.0",
                "event_id": "e6",
                "event_type": "PARSER_ERROR",
                "event_time": "2026-07-20T12:00:00Z",
                "repository_id": "repo",
                "commit_sha": "sha",
                "file_id": "fid",
                "file_path": "a.py",
                "content_hash": "ch",
                "parser_version": "1.0.0",
                "error": {"error_type": "SyntaxError", "message": "msg", "retryable": False},
            },
        ),
    ],
)
def test_all_valid_event_types(event_type, payload):
    schemas_dir = Path("schemas")
    if not schemas_dir.exists():
        schemas_dir = Path("../schemas")
    validator = EventValidator(schemas_dir)
    validator.validate(event_type, payload)


def test_unknown_event_type_raises_exception():
    schemas_dir = Path("schemas")
    if not schemas_dir.exists():
        schemas_dir = Path("../schemas")
    validator = EventValidator(schemas_dir)
    with pytest.raises(SchemaValidationError):
        validator.validate("UNKNOWN_EVENT", {"some": "data"})


def test_all_json_schemas_match_runtime_schema_version() -> None:
    """Verify that all JSON Schema files define schema_version const matching SCHEMA_VERSION."""
    import json
    from pathlib import Path
    from domain.constants import SCHEMA_VERSION

    schemas_dir = Path("schemas")
    if not schemas_dir.exists():
        schemas_dir = Path("../schemas")

    assert schemas_dir.exists(), f"Schemas directory not found: {schemas_dir}"

    for path in schemas_dir.glob("*.schema.json"):
        with open(path, "r", encoding="utf-8") as f:
            schema = json.load(f)

        properties = schema.get("properties", {})
        schema_version_prop = properties.get("schema_version", {})

        const_val = schema_version_prop.get("const")
        assert const_val == SCHEMA_VERSION, (
            f"Schema version constraint in {path.name} is '{const_val}', "
            f"but runtime SCHEMA_VERSION is '{SCHEMA_VERSION}'"
        )
