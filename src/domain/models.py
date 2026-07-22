"""Domain models for CPG parser status and entities."""

from dataclasses import dataclass, field
from typing import Any
from domain.enums import ParseStatus


@dataclass(frozen=True, slots=True)
class SourceFile:
    """Represents a source file to process."""

    repository_id: str
    repository_root: str
    relative_path: str
    commit_sha: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class CodeNode:
    """Represents a node vertex in the CPG graph."""

    node_id: str
    file_id: str
    node_type: str
    ast_path: str
    name: str | None
    qualified_name: str | None
    line_start: int | None
    column_start: int | None
    line_end: int | None
    column_end: int | None
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CodeEdge:
    """Represents a directed relationship edge in the CPG graph."""

    edge_id: str
    file_id: str
    source_id: str
    target_id: str
    edge_type: str
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class FileMetadata:
    """Aggregated stats and parser metrics for a source file."""

    file_id: str
    repository_id: str
    file_path: str
    content_hash: str
    size_bytes: int
    line_count: int
    function_count: int
    class_count: int
    import_count: int
    node_count: int
    edge_count: int
    parse_duration_ms: int
    parse_status: ParseStatus
    parser: str = "python.ast"


@dataclass(frozen=True, slots=True)
class ParsedFileGraph:
    """Represents a successfully parsed CPG subgraph for a file."""

    source_file: SourceFile
    file_id: str
    content_hash: str
    nodes: list[CodeNode]
    edges: list[CodeEdge]
    metadata: FileMetadata


@dataclass(frozen=True, slots=True)
class FileState:
    """State stored locally to check hash and diff changes."""

    file_id: str
    content_hash: str
    node_ids: list[str]
    edge_ids: list[str]
    parser_version: str | None = None
    schema_version: str | None = None


@dataclass(frozen=True, slots=True)
class GraphDiff:
    """Represents the difference between previous state and current graph."""

    removed_node_ids: list[str]
    removed_edge_ids: list[str]
    current_nodes: list[CodeNode]
    current_edges: list[CodeEdge]


@dataclass(frozen=True, slots=True)
class ProcessingResult:
    """The result of processing a single source file."""

    status: ParseStatus
    file_id: str
    file_path: str
    content_hash: str
    node_count: int
    edge_count: int
    emitted_event_counts: dict[str, int]
    error: str | None = None
