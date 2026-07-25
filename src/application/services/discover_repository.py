"""Application service to list and filter source files within the repository."""

import os
from pathlib import Path
from typing import Any
from application.ports import SourceRepositoryPort, ManifestWriterPort
from domain.models import SourceFile


class DiscoverRepositoryService:
    """Orchestrates repository scanning, filter matchings, and manifest logging."""

    def __init__(
        self,
        repo_adapter: SourceRepositoryPort,
        manifest_writer: ManifestWriterPort,
        repository_id: str,
    ):
        self.repo_adapter = repo_adapter
        self.manifest_writer = manifest_writer
        self.repository_id = repository_id

    def execute(self, source_root: Path | None = None) -> list[SourceFile]:
        """Clones if necessary, scans directory structures, logs manifest, and returns SourceFiles."""
        self.repo_adapter.clone_repository()
        commit_sha = self.repo_adapter.get_commit_hash()
        scope = os.getenv("PARSER_SCOPE", "final")

        raw_paths = self.repo_adapter.list_python_files(source_root)
        selected_paths = self.repo_adapter.list_files(source_root)
        self._assert_path_contract(raw_paths)
        self._assert_path_contract(selected_paths)

        selected_set = {p.as_posix() for p in selected_paths}
        records: list[dict[str, Any]] = []
        source_files: list[SourceFile] = []

        for p in raw_paths:
            posix_path = p.as_posix()
            included = posix_path in selected_set
            abs_p = self.repo_adapter.resolve_path(p)
            size = abs_p.stat().st_size if abs_p.exists() else 0
            reason = None if included else self.repo_adapter.get_exclusion_reason(p, scope)
            if not included and reason is None:
                reason = "Excluded by scope include pattern"

            records.append(
                {
                    "repository_id": self.repository_id,
                    "commit_sha": commit_sha,
                    "file_path": posix_path,
                    "size_bytes": size,
                    "content_sha256": self._content_hash(p),
                    "scope": scope,
                    "included": included,
                    "exclusion_reason": reason,
                }
            )

            if included:
                source_files.append(
                    SourceFile(
                        repository_id=self.repository_id,
                        repository_root=str(self.repo_adapter.resolve_path(Path(""))),
                        relative_path=posix_path,
                        commit_sha=commit_sha,
                        size_bytes=size,
                    )
                )

        self.manifest_writer.write_manifest(records)
        return source_files

    def _content_hash(self, relative_path: Path) -> str | None:
        hash_method = getattr(self.repo_adapter, "compute_content_hash", None)
        if not callable(hash_method):
            return None
        return str(hash_method(relative_path))

    @staticmethod
    def _assert_path_contract(paths: list[Path]) -> None:
        path_strings = [path.as_posix() for path in paths]
        assert len(path_strings) == len(set(path_strings))
        assert path_strings == sorted(path_strings)
        assert all(path.endswith(".py") for path in path_strings)
        assert all("\\" not in path for path in path_strings)
        assert all(not path.startswith("../") for path in path_strings)
