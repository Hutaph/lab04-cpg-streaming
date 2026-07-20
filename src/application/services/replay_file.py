"""Service to explicitly replay and update parsed state of a modified file."""

from pathlib import Path


class ReplayFileService:
    """Manages the idempotent replay flow: delete stale graph items and republish updated nodes/edges."""

    def execute(self, file_path: Path) -> None:
        """TODO: Implement file update replay flow, issuing delete events for old items."""
        raise NotImplementedError("ReplayFileService will be implemented in Phase 13")
