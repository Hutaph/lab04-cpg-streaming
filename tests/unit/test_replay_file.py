"""Unit tests for ReplayFileService path and hash contracts."""

from pathlib import Path
from unittest.mock import Mock

from application.services.replay_file import ReplayFileService
from domain.constants import PARSER_VERSION, SCHEMA_VERSION
from domain.enums import ParseStatus
from domain.models import FileMetadata, FileState, ParsedFileGraph, ProcessingResult, SourceFile
from parsing.identifiers import IdentifierGenerator


class FakeRepository:
    """Small repository adapter used to assert normalized replay paths."""

    def __init__(self, root: Path):
        self.root = root
        self.resolved_paths: list[Path] = []
        self.read_paths: list[Path] = []

    def resolve_path(self, relative_path: Path) -> Path:
        self.resolved_paths.append(relative_path)
        return self.root / relative_path

    def read_file(self, relative_path: Path) -> bytes:
        self.read_paths.append(relative_path)
        return (self.root / relative_path).read_bytes()

    def get_commit_hash(self) -> str:
        return "commit"


def test_replay_file_service_returns_posix_file_path(tmp_path: Path) -> None:
    """Verify replay uses canonical POSIX paths for state lookup, parsing, and result."""
    source_root = tmp_path / "source"
    source_file = source_root / "src" / "task.py"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("x = 1\n", encoding="utf-8")

    repository_id = "repo"
    file_id = IdentifierGenerator.generate_file_id(repository_id, "src/task.py")
    source_bytes = source_file.read_bytes()
    content_hash = IdentifierGenerator.generate_content_hash(source_bytes)

    repo = FakeRepository(source_root)
    state = Mock()
    state.get.return_value = FileState(file_id, "old_hash", [], [], PARSER_VERSION, SCHEMA_VERSION)

    parser = Mock()
    metadata = FileMetadata(
        file_id,
        repository_id,
        "src/task.py",
        content_hash,
        len(source_bytes),
        1,
        0,
        0,
        0,
        0,
        0,
        1,
        ParseStatus.SUCCESS,
    )
    parser.parse_file.return_value = ParsedFileGraph(
        source_file=SourceFile(repository_id, str(source_root), "src/task.py", "commit", len(source_bytes)),
        file_id=file_id,
        content_hash=content_hash,
        nodes=[],
        edges=[],
        metadata=metadata,
    )

    process_service = Mock()
    process_service.execute.return_value = ProcessingResult(
        status=ParseStatus.SUCCESS,
        file_id=file_id,
        file_path="src/task.py",
        content_hash=content_hash,
        node_count=0,
        edge_count=0,
        emitted_event_counts={"source.metadata": 1},
    )

    service = ReplayFileService(repo, parser, state, process_service, repository_id)
    result = service.execute(Path("src\\task.py"))

    assert result["file_path"] == "src/task.py"
    assert repo.resolved_paths == [Path("src/task.py"), Path("")]
    assert repo.read_paths == [Path("src/task.py")]
    parser.parse_file.assert_called_once_with(Path("src/task.py"), source_bytes, "commit")
    process_service.execute.assert_called_once()
    assert process_service.execute.call_args.args[0].relative_path == "src/task.py"
