"""Unit tests for metadata stats extraction logic."""

from parsing.metadata import MetadataExtractor
from domain.enums import ParseStatus


def test_metadata_extraction() -> None:
    """Verify metadata count extraction matches source structures."""
    source_bytes = b"import math\nclass A:\n    pass\ndef f():\n    pass"
    extractor = MetadataExtractor()

    meta = extractor.extract(
        source_code=source_bytes,
        file_id="test_file_id",
        repository_id="test_repo",
        file_path="foo.py",
        content_hash="h1",
        node_count=10,
        edge_count=8,
        parse_duration_ms=5,
        status=ParseStatus.SUCCESS,
    )

    assert meta.size_bytes == len(source_bytes)
    assert meta.line_count == 5
    assert meta.class_count == 1
    assert meta.function_count == 1
    assert meta.import_count == 1
    assert meta.node_count == 10
    assert meta.edge_count == 8


def test_metadata_empty_file() -> None:
    """Verify empty files output 0 line count."""
    source_bytes = b""
    extractor = MetadataExtractor()

    meta = extractor.extract(
        source_code=source_bytes,
        file_id="test_file_id",
        repository_id="test_repo",
        file_path="foo.py",
        content_hash="h1",
        node_count=0,
        edge_count=0,
        parse_duration_ms=1,
        status=ParseStatus.SUCCESS,
    )

    assert meta.size_bytes == 0
    assert meta.line_count == 0
