"""Application service to list and filter source files within the repository."""

from pathlib import Path
from src.application.ports import SourceRepository


class DiscoverRepositoryService:
    """Service responsible for locating Python files that require parsing."""

    def __init__(self, repo_adapter: SourceRepository):
        self.repo_adapter = repo_adapter

    def execute(self) -> list[Path]:
        """TODO: Implement file list discovery using filters and current repo state."""
        return self.repo_adapter.list_files()
