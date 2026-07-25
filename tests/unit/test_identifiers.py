"""Unit tests for deterministic stable identifier generation logic."""

from pathlib import Path
import pytest
from parsing.identifiers import IdentifierGenerator, normalize_relative_path


def test_path_normalization() -> None:
    """Verify that backslashes are normalized to slashes and traversal is blocked."""
    assert normalize_relative_path("pkg/new_feature.py") == "pkg/new_feature.py"
    assert IdentifierGenerator.normalize_path(Path("src\\foo\\bar.py")) == "src/foo/bar.py"
    assert IdentifierGenerator.normalize_path(Path("foo/bar.py")) == "foo/bar.py"
    assert IdentifierGenerator.normalize_path("src//foo/./bar.py") == "src/foo/bar.py"

    with pytest.raises(ValueError):
        IdentifierGenerator.normalize_path(Path("../outside.py"))
    with pytest.raises(ValueError):
        IdentifierGenerator.normalize_path(Path("foo/../../outside.py"))
    with pytest.raises(ValueError):
        IdentifierGenerator.normalize_path(Path("/absolute/path.py"))
    with pytest.raises(ValueError):
        IdentifierGenerator.normalize_path("C:\\repo\\foo.py")


def test_file_id_deterministic() -> None:
    """Verify file_id is stable and does not change on content changes."""
    repo_id = "huggingface/transformers-pr-agent"
    path = Path("src/transformers/activations.py")

    file_id_1 = IdentifierGenerator.generate_file_id(repo_id, path)
    file_id_2 = IdentifierGenerator.generate_file_id(repo_id, path)
    assert file_id_1 == file_id_2

    # Change content hash simulate
    h1 = IdentifierGenerator.generate_content_hash(b"print(1)")
    h2 = IdentifierGenerator.generate_content_hash(b"print(2)")
    assert h1 != h2

    # Verify path hash stays equal
    assert file_id_1 == IdentifierGenerator.generate_file_id(repo_id, path)


def test_file_id_matches_for_posix_and_windows_style_paths() -> None:
    """Verify separator aliases share the same stable file_id."""
    repo_id = "huggingface/transformers-pr-agent"
    assert IdentifierGenerator.generate_file_id(repo_id, "src/task.py") == IdentifierGenerator.generate_file_id(
        repo_id, "src\\task.py"
    )


def test_node_id_stability() -> None:
    """Verify node_id does not use content hash and is deterministic."""
    file_id = "some_file_id"
    node_id_1 = IdentifierGenerator.generate_node_id(file_id, "FunctionDef", "Module", "foo", "Module.body[0]")
    node_id_2 = IdentifierGenerator.generate_node_id(file_id, "FunctionDef", "Module", "foo", "Module.body[0]")
    assert node_id_1 == node_id_2
