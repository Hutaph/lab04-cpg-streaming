"""Unit tests for ProcessFileService execution flows."""

from pathlib import Path
from unittest.mock import Mock
import pytest
from src.application.services.process_file import ProcessFileService
from src.domain.models import SourceFile, FileState, ParsedFileGraph, FileMetadata
from src.domain.enums import ParseStatus
from src.domain.errors import PublishError


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
    from src.application.ports import EventWriterPort
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
