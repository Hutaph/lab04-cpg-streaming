"""Unit tests for repository-wide Python discovery and parser scopes."""

from pathlib import Path
import subprocess
from typing import Any
from unittest.mock import Mock

import pytest

from application.services.discover_repository import DiscoverRepositoryService
from application.services.process_repository import ProcessRepositoryService
from domain.enums import ParseStatus
from domain.models import ProcessingResult
from infrastructure.filesystem.git_source_repository import GitSourceRepository


class RecordingManifestWriter:
    """Captures manifest records written by the discovery service."""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def write_manifest(self, records: list[dict[str, Any]]) -> None:
        self.records = records


@pytest.fixture()
def sample_repository(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    files = {
        "src/main.py": "def main():\n    return 1\n",
        "tools/release.py": "def release():\n    return 2\n",
        "examples/demo.py": "print('demo')\n",
        "tests/test_main.py": "def test_main():\n    assert True\n",
        "setup.py": "from setuptools import setup\n",
        "src/generated/client_pb2.py": "# generated\n",
        "README.md": "# fixture\n",
    }
    for relative_path, content in files.items():
        target = repo / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")

    subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "fixture"], cwd=repo, check=True, capture_output=True, text=True)
    return repo


def test_raw_scope_enumerates_all_python_files_from_repository_root(
    sample_repository: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PARSER_SCOPE", "raw")
    adapter = GitSourceRepository(sample_repository, clone_url="")

    raw_paths = [path.as_posix() for path in adapter.list_python_files()]
    selected_paths = [path.as_posix() for path in adapter.list_files()]

    assert raw_paths == sorted(raw_paths)
    assert len(raw_paths) == len(set(raw_paths))
    assert raw_paths == [
        "examples/demo.py",
        "setup.py",
        "src/generated/client_pb2.py",
        "src/main.py",
        "tests/test_main.py",
        "tools/release.py",
    ]
    assert selected_paths == raw_paths


def test_eligible_scope_keeps_valid_files_outside_src_and_excludes_configured_files(
    sample_repository: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PARSER_SCOPE", "final")
    adapter = GitSourceRepository(sample_repository, clone_url="")

    eligible_paths = [path.as_posix() for path in adapter.list_files()]

    assert eligible_paths == [
        "examples/demo.py",
        "src/main.py",
        "tools/release.py",
    ]
    assert adapter.get_exclusion_reason(Path("tests/test_main.py"), "final") == "Excluded test file"
    assert adapter.get_exclusion_reason(Path("setup.py"), "final") == "Excluded setup/build file"
    assert adapter.get_exclusion_reason(Path("src/generated/client_pb2.py"), "final") == "Excluded generated file"


def test_manifest_records_raw_files_and_exactly_marks_eligible_set(
    sample_repository: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PARSER_SCOPE", "final")
    adapter = GitSourceRepository(sample_repository, clone_url="")
    writer = RecordingManifestWriter()
    service = DiscoverRepositoryService(adapter, writer, repository_id="sample/repo")

    source_files = service.execute()

    manifest_paths = [record["file_path"] for record in writer.records if record["included"]]
    source_file_paths = [source_file.relative_path for source_file in source_files]
    expected_eligible = ["examples/demo.py", "src/main.py", "tools/release.py"]

    assert manifest_paths == expected_eligible
    assert source_file_paths == expected_eligible
    assert set(manifest_paths) == set(expected_eligible)
    assert len(manifest_paths) == len(set(manifest_paths))
    assert manifest_paths == sorted(manifest_paths)
    assert all(path.endswith(".py") for path in manifest_paths)
    assert all("\\" not in path for path in manifest_paths)
    assert all(not path.startswith("../") for path in manifest_paths)
    assert len(writer.records) == 6
    assert {record["file_path"] for record in writer.records} == {
        "examples/demo.py",
        "setup.py",
        "src/generated/client_pb2.py",
        "src/main.py",
        "tests/test_main.py",
        "tools/release.py",
    }
    assert all(record["repository_id"] == "sample/repo" for record in writer.records)
    assert all(record["commit_sha"] for record in writer.records)
    assert all(record["content_sha256"] for record in writer.records)


def test_smoke_scope_is_deterministic_subset_and_does_not_change_full_scope(
    sample_repository: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    adapter = GitSourceRepository(sample_repository, clone_url="")

    monkeypatch.setenv("PARSER_SCOPE", "final")
    full_paths = [path.as_posix() for path in adapter.list_files()]

    monkeypatch.setenv("PARSER_SCOPE", "smoke")
    smoke_paths = [path.as_posix() for path in adapter.list_files()]

    monkeypatch.setenv("PARSER_SCOPE", "final")
    full_paths_after_smoke = [path.as_posix() for path in adapter.list_files()]

    assert smoke_paths == ["src/main.py"]
    assert set(smoke_paths).issubset(full_paths)
    assert full_paths_after_smoke == full_paths


def test_parser_full_orchestration_processes_complete_eligible_manifest(
    sample_repository: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PARSER_SCOPE", "final")
    adapter = GitSourceRepository(sample_repository, clone_url="")
    writer = RecordingManifestWriter()
    discover_service = DiscoverRepositoryService(adapter, writer, repository_id="sample/repo")
    process_file_service = Mock()
    process_file_service.topic_nodes = "nodes"
    process_file_service.topic_edges = "edges"
    process_file_service.topic_metadata = "metadata"
    process_file_service.execute.side_effect = lambda source_file: ProcessingResult(
        status=ParseStatus.SUCCESS,
        file_id=source_file.relative_path,
        file_path=source_file.relative_path,
        content_hash="hash",
        node_count=0,
        edge_count=0,
        emitted_event_counts={},
    )

    service = ProcessRepositoryService(discover_service, process_file_service)
    summary = service.execute()

    processed_paths = [call.args[0].relative_path for call in process_file_service.execute.call_args_list]
    manifest_paths = [record["file_path"] for record in writer.records if record["included"]]

    assert processed_paths == ["examples/demo.py", "src/main.py", "tools/release.py"]
    assert processed_paths == manifest_paths
    assert summary["discovered"] == 3
    assert summary["eligible"] == 3
