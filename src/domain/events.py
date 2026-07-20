"""Domain events representing updates to the Code Property Graph."""

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class DomainEvent:
    """Base class for all CPG-related streaming events."""

    schema_version: int
    event_type: str
    timestamp: str
    repo: str
    commit_hash: str
    file_path: str
    file_hash: str


@dataclass(frozen=True, slots=True)
class NodeEvent(DomainEvent):
    """Event emitted when a CPG Node is parsed."""

    id: str
    label: str
    ast_path: str
    line: int | None = None
    column: int | None = None
    end_line: int | None = None
    end_column: int | None = None
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EdgeEvent(DomainEvent):
    """Event emitted when a CPG Edge is established."""

    id: str
    source: str
    target: str
    edge_type: str
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MetadataEvent(DomainEvent):
    """Event emitted with source file metadata stats."""

    id: str
    size_bytes: int
    line_count: int
    parser: str = "python.ast"


@dataclass(frozen=True, slots=True)
class ErrorEvent(DomainEvent):
    """Event emitted when parsing fails."""

    id: str
    error_type: str
    message: str
    line: int | None = None
    column: int | None = None
