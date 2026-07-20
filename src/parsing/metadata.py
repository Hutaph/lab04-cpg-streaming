"""Extracts metadata statistics from Python source code files."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FileMetadata:
    """Metrics gathered about a source file."""

    size_bytes: int
    line_count: int
    function_count: int
    class_count: int
    import_count: int


class MetadataExtractor:
    """TODO: Traverses Python AST to count classes, functions, lines, and imports."""

    def extract(self, source_code: str) -> FileMetadata:
        """Analyzes string source text and returns stats."""
        raise NotImplementedError("Metadata extractor will be implemented in Phase 2")
