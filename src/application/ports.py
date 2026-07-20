"""Interface definitions (ports) for secondary adapters to implement."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class SourceRepository(ABC):
    """Port for discovering files and reading commits from the target git repository."""

    @abstractmethod
    def get_commit_hash(self) -> str:
        """Returns the current commit hash (HEAD) of the repository."""
        raise NotImplementedError

    @abstractmethod
    def list_files(self) -> list[Path]:
        """Lists all files to parse in the repository."""
        raise NotImplementedError


class StateStore(ABC):
    """Port for persisting the incremental parse state of files."""

    @abstractmethod
    def get_file_hash(self, file_path: str) -> str | None:
        """Returns the last successfully processed content hash of a file."""
        raise NotImplementedError

    @abstractmethod
    def update_file_state(self, file_path: str, content_hash: str, commit_hash: str, status: str) -> None:
        """Saves the last processed file state details."""
        raise NotImplementedError


class EventWriterPort(ABC):
    """Port for publishing parsed CPG events."""

    @abstractmethod
    def write_event(self, topic: str, event_key: str, event: dict[str, Any]) -> None:
        """Writes/publishes a single event with key to the corresponding topic."""
        raise NotImplementedError

    @abstractmethod
    def flush(self) -> None:
        """Forces pending events to flush/publish."""
        raise NotImplementedError
