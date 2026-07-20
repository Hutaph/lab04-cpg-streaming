"""SQLite database adapter implementing StateStore for local parsing history tracking."""

from pathlib import Path
from src.application.ports import StateStore


class SqliteStateStore(StateStore):
    """Local SQLite state persistence storing processed file paths and hash diffs."""

    def __init__(self, db_path: Path):
        self.db_path = db_path

    def get_file_hash(self, file_path: str) -> str | None:
        """TODO: Read previous hash from file_states SQLite database table."""
        raise NotImplementedError("SqliteStateStore will be implemented in Phase 11")

    def update_file_state(self, file_path: str, content_hash: str, commit_hash: str, status: str) -> None:
        """TODO: Insert or replace file metadata state in SQLite database."""
        raise NotImplementedError("SqliteStateStore will be implemented in Phase 11")
