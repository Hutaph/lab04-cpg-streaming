"""Domain models for CPG parser status and entities."""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True, slots=True)
class FileState:
    """State tracking information for a source file."""

    file_path: str
    content_hash: str
    last_parsed_at: datetime
    commit_hash: str
    status: str


@dataclass(frozen=True, slots=True)
class CpgGraph:
    """Represents a locally parsed CPG subgraph for a file."""

    nodes: list[dict] = field(default_factory=list)
    edges: list[dict] = field(default_factory=list)
    metadata: list[dict] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)
