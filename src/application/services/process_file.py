"""Service to parse a single Python source file and publish CPG events."""

from pathlib import Path
from src.application.ports import EventWriterPort, StateStore


class ProcessFileService:
    """Orchestrates parsing of a single file and publishing node/edge/metadata events."""

    def __init__(self, state_store: StateStore, writer: EventWriterPort):
        self.state_store = state_store
        self.writer = writer

    def execute(self, file_path: Path) -> dict[str, int]:
        """TODO: Read file, generate stable IDs, construct AST/CFG/DFG, and write to Kafka/JSONL."""
        raise NotImplementedError("ProcessFileService will be implemented in Phase 2")
