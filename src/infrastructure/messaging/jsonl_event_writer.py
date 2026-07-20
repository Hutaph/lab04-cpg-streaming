"""Local JSONL writer implementing EventWriterPort for dry-run/local analysis."""

from pathlib import Path
from typing import Any
from src.application.ports import EventWriterPort


class JsonlEventWriter(EventWriterPort):
    """Adapter logging CPG events locally to JSON Lines files for development dry-runs."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir

    def write_event(self, topic: str, event_key: str, event: dict[str, Any]) -> None:
        """TODO: Append serialized JSON event as a line to the appropriate local topic file."""
        raise NotImplementedError("JsonlEventWriter will be implemented in Phase 6")

    def flush(self) -> None:
        """Nothing to do locally since we write synchronously or buffer-write."""
        pass
