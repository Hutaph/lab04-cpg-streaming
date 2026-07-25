"""Unit tests for ProcessFileService execution flows."""

from unittest.mock import Mock
from pathlib import Path
import pytest
from application.services.process_file import ProcessFileService
from domain.models import SourceFile, FileState, ParsedFileGraph, FileMetadata
from domain.enums import ParseStatus
from domain.errors import PublishError
from domain.constants import PARSER_VERSION, SCHEMA_VERSION


def test_unchanged_skip_flow() -> None:
    """Verify that if the content hash matches the database state, skip is returned."""
    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    state = Mock()
    state.get.return_value = FileState("file_id", "hash_abc", [], [], PARSER_VERSION, SCHEMA_VERSION)

    # The computed hash of b"x = 1" is "a053...". We force mocking to return equal hash
    # Or we can let it calculate naturally
    import hashlib

    content_hash = hashlib.sha256(b"x = 1").hexdigest()
    state.get.return_value = FileState("file_id", content_hash, [], [], PARSER_VERSION, SCHEMA_VERSION)

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
    # Assert writer and state store were not modified
    writer.write_event.assert_not_called()
    writer.publish_event.assert_not_called()
    writer.flush.assert_not_called()
    state.commit.assert_not_called()


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
    # Existing DB has nodes and edges to be deleted
    state.get.return_value = FileState(
        "file_id", "old_hash", ["old_node_id"], ["old_edge_id"], PARSER_VERSION, SCHEMA_VERSION
    )

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


def test_process_file_publishes_posix_file_path_for_windows_style_input() -> None:
    """Verify process boundaries use the canonical POSIX file path."""
    from domain.models import CodeNode

    repo = Mock()
    repo.read_file.return_value = b"x = 1"
    state = Mock()
    state.get.return_value = None

    meta = FileMetadata("file_id", "test_repo", "src/task.py", "hash_abc", 5, 1, 0, 0, 0, 1, 0, 1, ParseStatus.SUCCESS)
    node = CodeNode("node_id", "file_id", "Module", "Module", None, None, 1, 0, 1, 0)
    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id="file_id",
        content_hash="hash_abc",
        nodes=[node],
        edges=[],
        metadata=meta,
    )
    validator = Mock()
    writer = Mock()
    published: list[dict] = []
    writer.publish_event.side_effect = lambda topic, key, evt: published.append(evt)

    service = ProcessFileService(repo, parser, state, validator, writer)
    result = service.execute(SourceFile("test_repo", "root", "src\\task.py", "c1", 5))

    assert result.file_path == "src/task.py"
    assert {event["file_path"] for event in published} == {"src/task.py"}
    repo.read_file.assert_called_once_with(Path("src/task.py"))
    parser.parse_file.assert_called_once_with(Path("src/task.py"), b"x = 1", "c1")
    state.commit.assert_called_once()
    assert state.commit.call_args.args[1] == "src/task.py"


def test_parser_error_event_uses_posix_file_path() -> None:
    """Verify parser error events use the canonical POSIX file path."""
    from domain.errors import ParsingError

    repo = Mock()
    repo.read_file.return_value = b"def broken(:\n"
    state = Mock()
    state.get.return_value = None
    parser = Mock()
    parser.parse_file.side_effect = ParsingError("Syntax error")
    validator = Mock()
    writer = Mock()
    published: list[dict] = []
    writer.publish_event.side_effect = lambda topic, key, evt: published.append(evt)

    service = ProcessFileService(repo, parser, state, validator, writer)
    result = service.execute(SourceFile("test_repo", "root", "pkg\\broken.py", "c1", 5))

    assert result.file_path == "pkg/broken.py"
    assert published[0]["file_path"] == "pkg/broken.py"


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


def test_partial_delivery_failure_retains_state() -> None:
    """Verify that a partial delivery failure (some successful events, one failure) raises PublishError and prevents state store commit."""
    from domain.models import CodeNode

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    state = Mock()
    state.get.return_value = None  # fresh parse

    # Mock parser output with two nodes to ensure a batch of events is sent
    meta = FileMetadata("file_id", "test_repo", "foo.py", "hash_abc", 5, 1, 0, 0, 0, 2, 0, 1, ParseStatus.SUCCESS)
    node1 = CodeNode("node_1", "file_id", "Module", "Module", None, None, 1, 0, 1, 0)
    node2 = CodeNode("node_2", "file_id", "Module", "Module", None, None, 1, 0, 1, 0)

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id="file_id",
        content_hash="hash_abc",
        nodes=[node1, node2],
        edges=[],
        metadata=meta,
    )

    validator = Mock()

    # Create a mock publisher that simulates a partial delivery failure on flush
    # It receives multiple calls to publish_event, and then flush raises PublishError
    from application.ports import EventPublisherPort

    writer = Mock(spec=EventPublisherPort)
    writer.flush.side_effect = PublishError("Flush failed: event delivery failed for node_2")

    service = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=writer,
    )

    sf = SourceFile("test_repo", "root", "foo.py", "c1", 5)

    with pytest.raises(PublishError) as exc_info:
        service.execute(sf)

    assert "Flush failed" in str(exc_info.value)

    # State store commit must NOT have been called
    state.commit.assert_not_called()


def test_deterministic_retry_event_ids() -> None:
    """Verify that a retry after a simulated crash produces identical entity IDs and event IDs."""
    from domain.models import CodeNode
    from typing import Any

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    state = Mock()
    state.get.return_value = None  # fresh parse

    meta = FileMetadata("file_id", "test_repo", "foo.py", "hash_abc", 5, 1, 0, 0, 0, 1, 0, 1, ParseStatus.SUCCESS)
    node1 = CodeNode("node_1", "file_id", "Module", "Module", None, None, 1, 0, 1, 0)

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id="file_id",
        content_hash="hash_abc",
        nodes=[node1],
        edges=[],
        metadata=meta,
    )

    validator = Mock()

    # Capture the events published in the first run
    first_run_events = []

    class FirstRunWriter:
        def publish_event(self, topic: str, event_key: str, event: dict[str, Any]) -> None:
            first_run_events.append(event)

        def write_event(self, topic: str, event_key: str, event: dict[str, Any]) -> None:
            first_run_events.append(event)

        def flush(self) -> None:
            pass

    service_run1 = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=FirstRunWriter(),
    )

    sf = SourceFile("test_repo", "root", "foo.py", "c1", 5)
    # First execution succeeds, but we simulate a crash (state.commit is not called/fails)
    service_run1.execute(sf)

    # Second execution (retry) with the same repo/parser state and previous state still empty
    second_run_events = []

    class SecondRunWriter:
        def publish_event(self, topic: str, event_key: str, event: dict[str, Any]) -> None:
            second_run_events.append(event)

        def write_event(self, topic: str, event_key: str, event: dict[str, Any]) -> None:
            second_run_events.append(event)

        def flush(self) -> None:
            pass

    service_run2 = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=SecondRunWriter(),
    )

    service_run2.execute(sf)

    assert len(first_run_events) == len(second_run_events)
    assert len(first_run_events) > 0

    for ev1, ev2 in zip(first_run_events, second_run_events):
        # Event type must match
        assert ev1["event_type"] == ev2["event_type"]
        # Event ID must be identical across retries
        assert ev1["event_id"] == ev2["event_id"]
        # Content hash must be identical
        assert ev1["content_hash"] == ev2["content_hash"]
        # Entity payload IDs must match
        if "node" in ev1:
            assert ev1["node"]["node_id"] == ev2["node"]["node_id"]


def test_previous_state_preserved_on_publish_failure() -> None:
    """Verify that if a file was previously successfully processed, and a subsequent edit fails to publish, the previous state is preserved in the database."""
    from domain.models import CodeNode

    repo = Mock()
    repo.read_file.return_value = b"x = 2"  # Edit version B

    # Previous state contains state A
    state = Mock()
    state.get.return_value = FileState(
        "file_id", "hash_version_A", ["node_A"], ["edge_A"], PARSER_VERSION, SCHEMA_VERSION
    )

    # Mock parser output for version B
    meta = FileMetadata("file_id", "test_repo", "foo.py", "hash_version_B", 5, 1, 0, 0, 0, 1, 0, 1, ParseStatus.SUCCESS)
    node_B = CodeNode("node_B", "file_id", "Module", "Module", None, None, 1, 0, 1, 0)

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id="file_id",
        content_hash="hash_version_B",
        nodes=[node_B],
        edges=[],
        metadata=meta,
    )

    validator = Mock()

    # Simulate publisher failure on flush
    from application.ports import EventPublisherPort

    writer = Mock(spec=EventPublisherPort)
    writer.flush.side_effect = PublishError("Flush failed")

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

    # State store commit must NOT have been called to write version B
    state.commit.assert_not_called()
    # Confirm state store delete was not called either
    state.delete.assert_not_called()


def test_unicode_identifiers_serialization() -> None:
    """Verify that Unicode identifiers serialize and validate without errors."""
    from domain.models import CodeNode

    repo = Mock()
    repo.read_file.return_value = "dữ_liệu = 1".encode("utf-8")

    state = Mock()
    state.get.return_value = None

    meta = FileMetadata(
        "file_id", "test_repo", "tiếng_việt.py", "hash_abc", 15, 1, 0, 0, 0, 1, 0, 1, ParseStatus.SUCCESS
    )
    node = CodeNode("node_1", "file_id", "Module", "dữ_liệu", None, None, 1, 0, 1, 0, {"tên": "giá_trị"})

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id="file_id",
        content_hash="hash_abc",
        nodes=[node],
        edges=[],
        metadata=meta,
    )

    validator = Mock()
    writer = Mock()

    service = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=writer,
    )

    sf = SourceFile("test_repo", "root", "tiếng_việt.py", "c1", 15)
    res = service.execute(sf)

    assert res.status == ParseStatus.SUCCESS
    if hasattr(writer, "publish_event"):
        writer.publish_event.assert_called()


def test_repository_aborts_after_infrastructure_failure() -> None:
    """Verify that if processing a file raises PublishError, ProcessRepositoryService aborts immediately without processing remaining files."""
    from application.services.process_repository import ProcessRepositoryService

    discover_service = Mock()
    sf1 = SourceFile("repo", "root", "file1.py", "c1", 10)
    sf2 = SourceFile("repo", "root", "file2.py", "c1", 10)
    discover_service.execute.return_value = [sf1, sf2]

    process_file_service = Mock()
    process_file_service.execute.side_effect = PublishError("Kafka broker down")

    repo_service = ProcessRepositoryService(
        discover_service=discover_service,
        process_file_service=process_file_service,
    )

    with pytest.raises(PublishError):
        repo_service.execute()

    # The loop should have aborted after the first file
    process_file_service.execute.assert_called_once_with(sf1)


def test_parser_version_change_reprocesses_unchanged_source() -> None:
    """Verify that if content_hash matches but parser_version is different, skipping is disabled and file is reprocessed."""
    from pathlib import Path
    from parsing.identifiers import IdentifierGenerator

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    # Previous state has the same content_hash but a different parser_version ("0.9.0")
    import hashlib

    content_hash = hashlib.sha256(b"x = 1").hexdigest()
    expected_file_id = IdentifierGenerator.generate_file_id("test_repo", Path("foo.py"))

    state = Mock()
    state.get.return_value = FileState(expected_file_id, content_hash, [], [], "0.9.0", SCHEMA_VERSION)

    # Mock parser to return new graph
    meta = FileMetadata(
        expected_file_id, "test_repo", "foo.py", content_hash, 5, 1, 0, 0, 0, 0, 0, 1, ParseStatus.SUCCESS
    )
    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id=expected_file_id,
        content_hash=content_hash,
        nodes=[],
        edges=[],
        metadata=meta,
    )

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

    # Status should be SUCCESS (reprocessed), not SKIPPED_UNCHANGED
    assert res.status == ParseStatus.SUCCESS
    parser.parse_file.assert_called_once()
    state.commit.assert_called_once_with(
        expected_file_id, "foo.py", content_hash, [], [], PARSER_VERSION, SCHEMA_VERSION
    )


def test_legacy_null_parser_version_reprocesses() -> None:
    """Verify that if the legacy db row has a NULL parser_version, skipping is disabled and file is reprocessed."""
    from pathlib import Path
    from parsing.identifiers import IdentifierGenerator

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    # Previous state has same content_hash but parser_version is None (legacy null row)
    import hashlib

    content_hash = hashlib.sha256(b"x = 1").hexdigest()
    expected_file_id = IdentifierGenerator.generate_file_id("test_repo", Path("foo.py"))

    state = Mock()
    state.get.return_value = FileState(expected_file_id, content_hash, [], [], None, SCHEMA_VERSION)

    # Mock parser
    meta = FileMetadata(
        expected_file_id, "test_repo", "foo.py", content_hash, 5, 1, 0, 0, 0, 0, 0, 1, ParseStatus.SUCCESS
    )
    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id=expected_file_id,
        content_hash=content_hash,
        nodes=[],
        edges=[],
        metadata=meta,
    )

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

    assert res.status == ParseStatus.SUCCESS
    parser.parse_file.assert_called_once()
    state.commit.assert_called_once_with(
        expected_file_id, "foo.py", content_hash, [], [], PARSER_VERSION, SCHEMA_VERSION
    )


# --- Event ID Contract Tests ---


def test_event_id_is_stable_for_same_processing_contract() -> None:
    """Verify that same inputs with same versions produce identical event ID."""
    from parsing.identifiers import IdentifierGenerator

    id1 = IdentifierGenerator.generate_event_id("NODE_UPSERT", "node_1", "hash_abc", "1.0.0", "1.0")
    id2 = IdentifierGenerator.generate_event_id("NODE_UPSERT", "node_1", "hash_abc", "1.0.0", "1.0")
    assert id1 == id2


def test_event_id_changes_with_parser_version() -> None:
    """Verify that event ID changes when parser version changes."""
    from parsing.identifiers import IdentifierGenerator

    id1 = IdentifierGenerator.generate_event_id("NODE_UPSERT", "node_1", "hash_abc", "1.0.0", "1.0")
    id2 = IdentifierGenerator.generate_event_id("NODE_UPSERT", "node_1", "hash_abc", "2.0.0", "1.0")
    assert id1 != id2


def test_event_id_changes_with_schema_version() -> None:
    """Verify that event ID changes when schema version changes."""
    from parsing.identifiers import IdentifierGenerator

    id1 = IdentifierGenerator.generate_event_id("NODE_UPSERT", "node_1", "hash_abc", "1.0.0", "1.0")
    id2 = IdentifierGenerator.generate_event_id("NODE_UPSERT", "node_1", "hash_abc", "1.0.0", "2.0")
    assert id1 != id2


def test_event_id_does_not_depend_on_event_time() -> None:
    """Verify event ID does not contain event_time and is independent of it."""
    from parsing.identifiers import IdentifierGenerator

    # generate_event_id doesn't even accept time, confirming independence
    id1 = IdentifierGenerator.generate_event_id("NODE_UPSERT", "node_1", "hash_abc")
    id2 = IdentifierGenerator.generate_event_id("NODE_UPSERT", "node_1", "hash_abc")
    assert id1 == id2


def test_event_id_is_stable_across_processes() -> None:
    """Verify event ID is deterministic across execution."""
    from parsing.identifiers import IdentifierGenerator

    id1 = IdentifierGenerator.generate_event_id("NODE_UPSERT", "node_1", "hash_abc", "1.0.0", "1.0")
    # Expected value is pre-calculated SHA-256 string
    expected = "49dd2962c937e062578cc558fadd667a2e0d80de64734105356df8f1034a0338"
    assert id1 == expected


# --- Forced Snapshot Refresh & Schema/Parser reprocessing Tests ---


def test_parser_version_change_forces_full_snapshot_upsert() -> None:
    """Verify that a different parser version in previous state triggers reprocessing and full node/edge upsert."""
    from domain.models import CodeNode, CodeEdge

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    content_hash = "hash_abc"
    expected_file_id = "file_1"

    # Previous state has different parser version "0.9.0"
    state = Mock()
    state.get.return_value = FileState(expected_file_id, content_hash, ["node_1"], ["edge_1"], "0.9.0", SCHEMA_VERSION)

    # Current graph has same node_1 and edge_1
    meta = FileMetadata(
        expected_file_id, "test_repo", "foo.py", content_hash, 5, 1, 0, 0, 0, 1, 1, 1, ParseStatus.SUCCESS
    )
    node1 = CodeNode("node_1", expected_file_id, "Module", "Module", None, None, 1, 0, 1, 0)
    edge1 = CodeEdge("edge_1", expected_file_id, "node_1", "node_1", "AST_CHILD")

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id=expected_file_id,
        content_hash=content_hash,
        nodes=[node1],
        edges=[edge1],
        metadata=meta,
    )

    validator = Mock()
    writer = Mock()
    # Capture events published
    published = []
    writer.publish_event.side_effect = lambda topic, key, evt: published.append(evt)

    service = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=writer,
    )

    sf = SourceFile("test_repo", "root", "foo.py", "c1", 5)
    res = service.execute(sf)

    assert res.status == ParseStatus.SUCCESS
    # Verify that node_1 and edge_1 are upserted even though their IDs match previous state
    node_upserts = [e for e in published if e["event_type"] == "NODE_UPSERT"]
    edge_upserts = [e for e in published if e["event_type"] == "EDGE_UPSERT"]
    assert len(node_upserts) == 1
    assert node_upserts[0]["node"]["node_id"] == "node_1"
    assert len(edge_upserts) == 1
    assert edge_upserts[0]["edge"]["edge_id"] == "edge_1"


def test_schema_version_change_forces_full_snapshot_upsert() -> None:
    """Verify that a different schema version in previous state triggers reprocessing and full snapshot upsert."""
    from domain.models import CodeNode

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    content_hash = "hash_abc"
    expected_file_id = "file_1"

    # Previous state has different schema version "0.9"
    state = Mock()
    state.get.return_value = FileState(expected_file_id, content_hash, ["node_1"], [], PARSER_VERSION, "0.9")

    meta = FileMetadata(
        expected_file_id, "test_repo", "foo.py", content_hash, 5, 1, 0, 0, 0, 1, 0, 1, ParseStatus.SUCCESS
    )
    node1 = CodeNode("node_1", expected_file_id, "Module", "Module", None, None, 1, 0, 1, 0)

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id=expected_file_id,
        content_hash=content_hash,
        nodes=[node1],
        edges=[],
        metadata=meta,
    )

    validator = Mock()
    writer = Mock()
    published = []
    writer.publish_event.side_effect = lambda topic, key, evt: published.append(evt)

    service = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=writer,
    )

    sf = SourceFile("test_repo", "root", "foo.py", "c1", 5)
    res = service.execute(sf)

    assert res.status == ParseStatus.SUCCESS
    # Should reprocess and publish upserts
    node_upserts = [e for e in published if e["event_type"] == "NODE_UPSERT"]
    assert len(node_upserts) == 1


def test_forced_refresh_deletes_removed_entities() -> None:
    """Verify that if a version mismatch triggers reprocess, entities in old state that are no longer present are deleted."""
    from domain.models import CodeNode

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    content_hash = "hash_abc"
    expected_file_id = "file_1"

    # Previous state has old_node_1 under different parser version
    state = Mock()
    state.get.return_value = FileState(expected_file_id, content_hash, ["old_node_1"], [], "0.9.0", SCHEMA_VERSION)

    # Current graph has new_node_1
    meta = FileMetadata(
        expected_file_id, "test_repo", "foo.py", content_hash, 5, 1, 0, 0, 0, 1, 0, 1, ParseStatus.SUCCESS
    )
    node1 = CodeNode("new_node_1", expected_file_id, "Module", "Module", None, None, 1, 0, 1, 0)

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id=expected_file_id,
        content_hash=content_hash,
        nodes=[node1],
        edges=[],
        metadata=meta,
    )

    validator = Mock()
    writer = Mock()
    published = []
    writer.publish_event.side_effect = lambda topic, key, evt: published.append(evt)

    service = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=writer,
    )

    sf = SourceFile("test_repo", "root", "foo.py", "c1", 5)
    res = service.execute(sf)

    assert res.status == ParseStatus.SUCCESS
    # Check that old_node_1 was deleted and new_node_1 was upserted
    deletes = [e for e in published if e["event_type"] == "NODE_DELETE"]
    upserts = [e for e in published if e["event_type"] == "NODE_UPSERT"]
    assert len(deletes) == 1
    assert deletes[0]["node"]["node_id"] == "old_node_1"
    assert len(upserts) == 1
    assert upserts[0]["node"]["node_id"] == "new_node_1"


def test_failed_forced_refresh_preserves_previous_state() -> None:
    """Verify that if a forced refresh encounters a publish failure, the previous state in SQLite is left unmodified."""
    from domain.models import CodeNode

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    content_hash = "hash_abc"
    expected_file_id = "file_1"

    # Previous state
    state = Mock()
    state.get.return_value = FileState(expected_file_id, content_hash, ["old_node_1"], [], "0.9.0", "0.9")

    # Mock parser
    meta = FileMetadata(
        expected_file_id, "test_repo", "foo.py", content_hash, 5, 1, 0, 0, 0, 1, 0, 1, ParseStatus.SUCCESS
    )
    node1 = CodeNode("new_node_1", expected_file_id, "Module", "Module", None, None, 1, 0, 1, 0)

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id=expected_file_id,
        content_hash=content_hash,
        nodes=[node1],
        edges=[],
        metadata=meta,
    )

    validator = Mock()
    writer = Mock()
    # Simulate publish flush failure
    writer.flush.side_effect = PublishError("Kafka broker failed")

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


def test_successful_forced_refresh_updates_processing_versions() -> None:
    """Verify that a successful forced refresh commits the current PARSER_VERSION and SCHEMA_VERSION to SQLite."""
    from pathlib import Path
    from parsing.identifiers import IdentifierGenerator
    from domain.models import CodeNode
    import hashlib

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    content_hash = hashlib.sha256(b"x = 1").hexdigest()
    expected_file_id = IdentifierGenerator.generate_file_id("test_repo", Path("foo.py"))

    # Previous state
    state = Mock()
    state.get.return_value = FileState(expected_file_id, content_hash, ["old_node_1"], [], "0.9.0", "0.9")

    meta = FileMetadata(
        expected_file_id, "test_repo", "foo.py", content_hash, 5, 1, 0, 0, 0, 1, 0, 1, ParseStatus.SUCCESS
    )
    node1 = CodeNode("new_node_1", expected_file_id, "Module", "Module", None, None, 1, 0, 1, 0)

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id=expected_file_id,
        content_hash=content_hash,
        nodes=[node1],
        edges=[],
        metadata=meta,
    )

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
    service.execute(sf)

    # State commit should be called with current PARSER_VERSION and SCHEMA_VERSION
    state.commit.assert_called_once_with(
        expected_file_id, "foo.py", content_hash, ["new_node_1"], [], PARSER_VERSION, SCHEMA_VERSION
    )


# --- Legacy Schema, Logical Ordering, and Forced Refresh Metadata Tests ---


def test_legacy_null_schema_version_forces_full_snapshot_refresh() -> None:
    """Verify that a legacy row in SQLite state (schema_version = None) is not skipped and triggers a full snapshot refresh."""
    from pathlib import Path
    from domain.models import CodeNode, CodeEdge
    import hashlib
    from parsing.identifiers import IdentifierGenerator

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    content_hash = hashlib.sha256(b"x = 1").hexdigest()
    expected_file_id = IdentifierGenerator.generate_file_id("test_repo", Path("foo.py"))

    # Previous state has schema_version = None
    state = Mock()
    state.get.return_value = FileState(expected_file_id, content_hash, ["node_1"], ["edge_1"], PARSER_VERSION, None)

    meta = FileMetadata(
        expected_file_id, "test_repo", "foo.py", content_hash, 5, 1, 0, 0, 0, 1, 1, 1, ParseStatus.SUCCESS
    )
    node1 = CodeNode("node_1", expected_file_id, "Module", "Module", None, None, 1, 0, 1, 0)
    edge1 = CodeEdge("edge_1", expected_file_id, "node_1", "node_1", "AST_CHILD")

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id=expected_file_id,
        content_hash=content_hash,
        nodes=[node1],
        edges=[edge1],
        metadata=meta,
    )

    validator = Mock()
    writer = Mock()
    published = []
    writer.publish_event.side_effect = lambda topic, key, evt: published.append(evt)

    service = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=writer,
    )

    sf = SourceFile("test_repo", "root", "foo.py", "c1", 5)
    res = service.execute(sf)

    assert res.status == ParseStatus.SUCCESS

    # Must reprocess and publish upserts for all nodes and edges
    node_upserts = [e for e in published if e["event_type"] == "NODE_UPSERT"]
    edge_upserts = [e for e in published if e["event_type"] == "EDGE_UPSERT"]
    meta_upserts = [e for e in published if e["event_type"] == "FILE_METADATA_UPSERT"]

    assert len(node_upserts) == 1
    assert len(edge_upserts) == 1
    assert len(meta_upserts) == 1

    # SQLite state must be updated to current versions after success
    state.commit.assert_called_once_with(
        expected_file_id, "foo.py", content_hash, ["node_1"], ["edge_1"], PARSER_VERSION, SCHEMA_VERSION
    )


def test_failed_legacy_schema_refresh_preserves_previous_state() -> None:
    """Verify that if publishing fails during refresh of a legacy row (schema_version = None), the old state is untouched."""
    from pathlib import Path
    from domain.models import CodeNode
    import hashlib
    from parsing.identifiers import IdentifierGenerator

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    content_hash = hashlib.sha256(b"x = 1").hexdigest()
    expected_file_id = IdentifierGenerator.generate_file_id("test_repo", Path("foo.py"))

    # Previous state has schema_version = None
    state = Mock()
    state.get.return_value = FileState(expected_file_id, content_hash, ["node_1"], [], PARSER_VERSION, None)

    meta = FileMetadata(
        expected_file_id, "test_repo", "foo.py", content_hash, 5, 1, 0, 0, 0, 1, 0, 1, ParseStatus.SUCCESS
    )
    node1 = CodeNode("node_1", expected_file_id, "Module", "Module", None, None, 1, 0, 1, 0)

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id=expected_file_id,
        content_hash=content_hash,
        nodes=[node1],
        edges=[],
        metadata=meta,
    )

    validator = Mock()
    writer = Mock()
    writer.flush.side_effect = PublishError("Kafka broker failed")

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

    # State store commit must NOT have been called, preserving previous null state
    state.commit.assert_not_called()


def test_forced_refresh_preserves_logical_event_order() -> None:
    """Verify forced refresh publishes events in EDGE_DELETE -> NODE_DELETE -> NODE_UPSERT -> EDGE_UPSERT -> METADATA sequence."""
    from pathlib import Path
    from domain.models import CodeNode, CodeEdge
    import hashlib
    from parsing.identifiers import IdentifierGenerator

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    content_hash = hashlib.sha256(b"x = 1").hexdigest()
    expected_file_id = IdentifierGenerator.generate_file_id("test_repo", Path("foo.py"))

    # Previous state has old_node_1 and old_edge_1 under old version
    state = Mock()
    state.get.return_value = FileState(
        expected_file_id,
        content_hash,
        ["old_node_1"],
        ["old_edge_1"],
        "0.9.0",
        SCHEMA_VERSION,
    )

    # Current graph has new_node_1 and new_edge_1
    meta = FileMetadata(
        expected_file_id, "test_repo", "foo.py", content_hash, 5, 1, 0, 0, 0, 1, 1, 1, ParseStatus.SUCCESS
    )
    node1 = CodeNode("new_node_1", expected_file_id, "Module", "Module", None, None, 1, 0, 1, 0)
    edge1 = CodeEdge("new_edge_1", expected_file_id, "new_node_1", "new_node_1", "AST_CHILD")

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id=expected_file_id,
        content_hash=content_hash,
        nodes=[node1],
        edges=[edge1],
        metadata=meta,
    )

    validator = Mock()
    writer = Mock()
    published_types = []
    writer.publish_event.side_effect = lambda topic, key, evt: published_types.append(evt["event_type"])

    service = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=writer,
    )

    sf = SourceFile("test_repo", "root", "foo.py", "c1", 5)
    res = service.execute(sf)

    assert res.status == ParseStatus.SUCCESS

    # We expect the strict ordering: EDGE_DELETE, NODE_DELETE, NODE_UPSERT, EDGE_UPSERT, FILE_METADATA_UPSERT
    expected_sequence = ["EDGE_DELETE", "NODE_DELETE", "NODE_UPSERT", "EDGE_UPSERT", "FILE_METADATA_UPSERT"]
    assert published_types == expected_sequence


@pytest.mark.parametrize(
    "prev_parser,prev_schema",
    [
        ("0.9.0", SCHEMA_VERSION),
        (PARSER_VERSION, "0.9"),
        (None, SCHEMA_VERSION),
        (PARSER_VERSION, None),
    ],
)
def test_forced_refresh_emits_exactly_one_metadata_event(prev_parser, prev_schema) -> None:
    """Verify forced refresh emits exactly one metadata event, positioned last."""
    from pathlib import Path
    from domain.models import CodeNode
    import hashlib
    from parsing.identifiers import IdentifierGenerator

    repo = Mock()
    repo.read_file.return_value = b"x = 1"

    content_hash = hashlib.sha256(b"x = 1").hexdigest()
    expected_file_id = IdentifierGenerator.generate_file_id("test_repo", Path("foo.py"))

    state = Mock()
    state.get.return_value = FileState(expected_file_id, content_hash, ["n1"], [], prev_parser, prev_schema)

    meta = FileMetadata(
        expected_file_id, "test_repo", "foo.py", content_hash, 5, 1, 0, 0, 0, 1, 0, 1, ParseStatus.SUCCESS
    )
    node1 = CodeNode("n1", expected_file_id, "Module", "Module", None, None, 1, 0, 1, 0)

    parser = Mock()
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=Mock(),
        file_id=expected_file_id,
        content_hash=content_hash,
        nodes=[node1],
        edges=[],
        metadata=meta,
    )

    validator = Mock()
    writer = Mock()
    published = []
    writer.publish_event.side_effect = lambda topic, key, evt: published.append(evt)

    service = ProcessFileService(
        repo_adapter=repo,
        parser=parser,
        state_store=state,
        validator=validator,
        writer=writer,
    )

    sf = SourceFile("test_repo", "root", "foo.py", "c1", 5)
    res = service.execute(sf)

    assert res.status == ParseStatus.SUCCESS

    meta_events = [e for e in published if e["event_type"] == "FILE_METADATA_UPSERT"]
    assert len(meta_events) == 1
    assert published[-1]["event_type"] == "FILE_METADATA_UPSERT"

    # Verify payload content matches constants
    meta_payload = meta_events[0]
    assert meta_payload["content_hash"] == content_hash
    assert meta_payload["parser_version"] == PARSER_VERSION
    assert meta_payload["schema_version"] == SCHEMA_VERSION
    assert meta_payload["repository_id"] == "test_repo"
    assert meta_payload["file_id"] == expected_file_id
