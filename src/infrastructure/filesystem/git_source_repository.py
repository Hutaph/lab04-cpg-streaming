"""Git command adapter implementing SourceRepository port to explore code directories."""

from pathlib import Path
from src.application.ports import SourceRepository


class GitSourceRepository(SourceRepository):
    """Adapter for interacting with the target repository cloned on disk."""

    def __init__(self, repo_path: Path):
        self.repo_path = repo_path

    def get_commit_hash(self) -> str:
        """TODO: Execute git shell command to get current commit hash (HEAD)."""
        raise NotImplementedError("GitSourceRepository.get_commit_hash will be implemented in Phase 1")

    def list_files(self) -> list[Path]:
        """TODO: Traverse repository using glob filter to list source Python files."""
        raise NotImplementedError("GitSourceRepository.list_files will be implemented in Phase 1")
