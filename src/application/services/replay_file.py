"""Service to explicitly replay and update parsed state of a modified file."""

from pathlib import Path
from typing import Any
from application.ports import SourceRepositoryPort, ParserPort, StateStorePort
from application.services.process_file import ProcessFileService
from domain.models import SourceFile
from parsing.identifiers import IdentifierGenerator
from parsing.diff import CpgDiffer


class ReplayFileService:
    """Manages the idempotent replay flow for one explicitly modified file path."""

    def __init__(
        self,
        repo_adapter: SourceRepositoryPort,
        parser: ParserPort,
        state_store: StateStorePort,
        process_file_service: ProcessFileService,
        repository_id: str,
    ):
        self.repo_adapter = repo_adapter
        self.parser = parser
        self.state_store = state_store
        self.process_file_service = process_file_service
        self.repository_id = repository_id
        self.cpg_differ = CpgDiffer()

    def execute(self, relative_file_path: Path) -> dict[str, Any]:
        """Validates, parses, diffs, and executes event publish of a single replayed file."""
        normalized_file_path = IdentifierGenerator.normalize_path(relative_file_path)
        relative_file_path = Path(normalized_file_path)
        abs_path = self.repo_adapter.resolve_path(relative_file_path)
        if not abs_path.exists():
            raise FileNotFoundError(f"Source file to replay does not exist: {abs_path}")

        # 1. Read metadata from repo
        commit_sha = self.repo_adapter.get_commit_hash()
        size = abs_path.stat().st_size

        source_file = SourceFile(
            repository_id=self.repository_id,
            repository_root=str(self.repo_adapter.resolve_path(Path(""))),
            relative_path=normalized_file_path,
            commit_sha=commit_sha,
            size_bytes=size,
        )

        file_id = IdentifierGenerator.generate_file_id(self.repository_id, relative_file_path)

        # 2. Load previous state
        prev_state = self.state_store.get(file_id)
        old_hash = prev_state.content_hash if prev_state else "None"

        # Calculate new hash
        source_bytes = self.repo_adapter.read_file(relative_file_path)

        # 3. Simulate diff count first for returning metrics
        removed_nodes = 0
        removed_edges = 0
        upsert_nodes = 0
        upsert_edges = 0

        # Calculate counts
        try:
            current_graph = self.parser.parse_file(relative_file_path, source_bytes, commit_sha)
            diff = self.cpg_differ.compute_diff(prev_state, current_graph)
            removed_nodes = len(diff.removed_node_ids)
            removed_edges = len(diff.removed_edge_ids)
            upsert_nodes = len(diff.current_nodes)
            upsert_edges = len(diff.current_edges)
        except Exception:
            # Let the process_file_service handle syntax error event production
            pass

        # 4. Process file (publish events + SQLite commit)
        res = self.process_file_service.execute(source_file)

        return {
            "file_path": normalized_file_path,
            "status": res.status.value,
            "old_content_hash": old_hash,
            "new_content_hash": res.content_hash,
            "removed_node_count": removed_nodes,
            "removed_edge_count": removed_edges,
            "upsert_node_count": upsert_nodes,
            "upsert_edge_count": upsert_edges,
            "error": res.error,
        }
