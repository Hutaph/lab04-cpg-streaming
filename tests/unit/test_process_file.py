"""Unit tests for ProcessFileService execution flows."""

from unittest.mock import Mock
import pytest
from application.services.process_file import ProcessFileService
from domain.models import SourceFile, FileState, ParsedFileGraph, FileMetadata
from domain.enums import ParseStatus
from domain.errors import PublishError


def test_unchanged_skip_flow() -> None:
    """Verify that if the content hash matches the database state, skip is returned."""
    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    state = Mock()
    state.get.return_value = FileState("file_id", "hash_abc", [], [])

    # The computed hash of b"x = 1" is "a053...". We force mocking to return equal hash
    # Or we can let it calculate naturally
    import hashlib

    content_hash = hashlib.sha256(b"x = 1").hexdigest()
    state.get.return_value = FileState("file_id", content_hash, [], [])

    parser = Mock()
    validator = Mock()
    writer = Mock()

    service = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=writer,
    )

    sf = SourceFile("test_repo", "root", "foo.py", "c1", 5)
    res = service.execute(sf)

    assert res.status == ParseStatus.SKIPPED_UNCHANGED
    # Assert parser was never called
    parser.parse_file.assert_not_called()


def test_writer_failure_does_not_commit() -> None:
    """Verify that a database commit is skipped if the publisher writer encounters an exception."""
    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    state = Mock()
    state.get.return_value = None  # Force fresh parse

    parser = Mock()
    # Mock parser output graph
    meta = FileMetadata("f1", "test_repo", "foo.py", "hash_abc", 5, 1, 0, 0, 0, 0, 0, 1, ParseStatus.SUCCESS)
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id="f1",
        content_hash="hash_abc",
        nodes=[],
        edges=[],
        metadata=meta,
    )

    validator = Mock()
    from application.ports import EventWriterPort

    writer = Mock(spec=EventWriterPort)
    # Force writer to raise exception on event write/publish
    writer.write_event.side_effect = Exception("Kafka Down")

    service = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=writer,
    )

    sf = SourceFile("test_repo", "root", "foo.py", "c1", 5)

    with pytest.raises(PublishError):
        service.execute(sf)

    # State store commit must NOT have been called
    state.commit.assert_not_called()


def test_process_file_publishes_events_in_logical_order() -> None:
    """Verify that the process file service invokes the publisher/writer in the correct logical order."""
    from typing import Any
    from domain.models import CodeNode, CodeEdge

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    # Set up previous state containing nodes and edges to trigger removals
    state = Mock()
    state.get.return_value = FileState("file_id", "old_hash", ["old_node_id"], ["old_edge_id"])

    # Mock parser
    meta = FileMetadata("file_id", "test_repo", "foo.py", "new_hash", 5, 1, 0, 0, 0, 1, 1, 1, ParseStatus.SUCCESS)
    new_node = CodeNode("new_node_id", "file_id", "Module", "Module", None, None, 1, 0, 1, 0)
    new_edge = CodeEdge("new_edge_id", "file_id", "new_node_id", "new_node_id", "AST_CHILD")

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id="file_id",
        content_hash="new_hash",
        nodes=[new_node],
        edges=[new_edge],
        metadata=meta,
    )

    validator = Mock()

    # We want to record the order of write_event/publish_event calls
    call_sequence = []

    class RecordingWriter:
        def write_event(self, topic: str, event_key: str, event: dict[str, Any]) -> None:
            call_sequence.append((topic, event["event_type"]))

        def publish_event(self, topic: str, event_key: str, event: dict[str, Any]) -> None:
            call_sequence.append((topic, event["event_type"]))

        def flush(self) -> None:
            pass

    service = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=RecordingWriter(),
    )

    sf = SourceFile("test_repo", "root", "foo.py", "c1", 5)
    service.execute(sf)

    # The expected logical sequence of events sent to the publisher is:
    # 1. EDGE_DELETE (sent to cpg.edges)
    # 2. NODE_DELETE (sent to cpg.nodes)
    # 3. NODE_UPSERT (sent to cpg.nodes)
    # 4. EDGE_UPSERT (sent to cpg.edges)
    # 5. FILE_METADATA_UPSERT (sent to source.metadata)
    expected = [
        ("cpg.edges", "EDGE_DELETE"),
        ("cpg.nodes", "NODE_DELETE"),
        ("cpg.nodes", "NODE_UPSERT"),
        ("cpg.edges", "EDGE_UPSERT"),
        ("source.metadata", "FILE_METADATA_UPSERT"),
    ]
    assert call_sequence == expected


def test_validation_failure_prevents_all_publishing() -> None:
    """Verify that if validation fails on any event in the batch, the validation exception is raised and no events are published or state committed."""
    from domain.errors import SchemaValidationError
    from domain.models import CodeNode

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    state = Mock()
    state.get.return_value = None  # fresh parse

    # Mock parser output graph
    meta = FileMetadata("file_id", "test_repo", "foo.py", "hash_abc", 5, 1, 0, 0, 0, 1, 0, 1, ParseStatus.SUCCESS)
    new_node = CodeNode("new_node_id", "file_id", "Module", "Module", None, None, 1, 0, 1, 0)

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id="file_id",
        content_hash="hash_abc",
        nodes=[new_node],
        edges=[],
        metadata=meta,
    )

    # Force validator to raise validation error
    validator = Mock()
    validator.validate.side_effect = SchemaValidationError("Invalid schema")

    writer = Mock()
    service = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=writer,
    )

    sf = SourceFile("test_repo", "root", "foo.py", "c1", 5)

    with pytest.raises(SchemaValidationError):
        service.execute(sf)

    # Verify that writer was never called to publish or write events
    writer.write_event.assert_not_called()
    writer.publish_event.assert_not_called()
    writer.flush.assert_not_called()

    # Verify state store was not committed
    state.commit.assert_not_called()


def test_syntax_error_emits_parser_error() -> None:
    """Verify that a parsing/syntax error results in a PARSER_ERROR event published to parser.errors without committing state."""
    from domain.errors import ParsingError

    repo = Mock()
    repo.read_file.return_value = b"invalid python source code"

    state = Mock()
    state.get.return_value = None

    parser = Mock()
    parser.parse_file.side_effect = ParsingError("Syntax error at line 1")

    validator = Mock()  # mock validator passes validation by default

    writer = Mock()
    service = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=writer,
    )

    sf = SourceFile("test_repo", "root", "foo.py", "c1", 5)
    res = service.execute(sf)

    # Verify status is FAILED
    assert res.status == ParseStatus.FAILED
    assert res.error == "Syntax error at line 1"

    # Verify a PARSER_ERROR event was sent to parser.errors
    if hasattr(writer, "publish_event"):
        writer.publish_event.assert_called_once()
        # Verify first argument (topic) is parser.errors
        assert writer.publish_event.call_args[0][0] == "parser.errors"
    else:
        writer.write_event.assert_called_once()
        assert writer.write_event.call_args[0][0] == "parser.errors"

    # Verify state store was not committed
    state.commit.assert_not_called()


def test_invalid_parser_error_event_is_not_published() -> None:
    """Verify that if the PARSER_ERROR event itself fails schema validation, the validation exception is raised and not published."""
    from domain.errors import ParsingError, SchemaValidationError

    repo = Mock()
    repo.read_file.return_value = b"invalid python source code"

    state = Mock()
    state.get.return_value = None

    parser = Mock()
    parser.parse_file.side_effect = ParsingError("Syntax error")

    validator = Mock()
    # Force validator to fail on PARSER_ERROR
    validator.validate.side_effect = SchemaValidationError("Invalid error schema")

    writer = Mock()
    service = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=writer,
    )

    sf = SourceFile("test_repo", "root", "foo.py", "c1", 5)

    with pytest.raises(SchemaValidationError):
        service.execute(sf)

    # Verify that writer was not called
    writer.write_event.assert_not_called()
    writer.publish_event.assert_not_called()
    state.commit.assert_not_called()
