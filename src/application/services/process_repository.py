"""Service to run incremental parse across the entire source repository."""

from src.application.ports import SourceRepository


class ProcessRepositoryService:
    """Orchestrates discover, filter, diff, and parse workflows for the entire repository."""

    def __init__(self, repo_adapter: SourceRepository):
        self.repo_adapter = repo_adapter

    def execute(self) -> None:
        """TODO: Implement incremental scanning of target repository files."""
        raise NotImplementedError("ProcessRepositoryService will be implemented in Phase 11")
