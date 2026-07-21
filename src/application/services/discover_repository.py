"""Application service to list and filter source files within the repository."""

from pathlib import Path
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
        # 1. Clone repository
        self.repo_adapter.clone_repository()

        # 2. Get active commit hash
        commit_sha = self.repo_adapter.get_commit_hash()

        # 3. Discovered Python files (all *.py in repo)
        # To identify all, we can list files using a broad list
        # Eligible python files are returned by list_files adapter
        eligible_paths = self.repo_adapter.list_files(source_root)
        eligible_set = {p.as_posix() for p in eligible_paths}

        # Resolve all *.py to count discovered_python_files
        all_py_paths = []
        target_dir = source_root if source_root else self.repo_adapter.resolve_path(Path("src"))
        if not target_dir.exists():
            target_dir = self.repo_adapter.resolve_path(Path(""))

        for root, _, files in os_walk_helper(target_dir):
            for file in files:
                if file.endswith(".py"):
                    abs_file = Path(root) / file
                    rel_to_repo = abs_file.relative_to(self.repo_adapter.resolve_path(Path("")))
                    all_py_paths.append(rel_to_repo)

        records = []
        source_files = []

        # Remove duplicate paths and sort deterministically
        all_py_paths = sorted(list(set(all_py_paths)))

        for p in all_py_paths:
            posix_path = p.as_posix()
            included = posix_path in eligible_set
            abs_p = self.repo_adapter.resolve_path(p)
            size = abs_p.stat().st_size if abs_p.exists() else 0

            reason = None
            if not included:
                reason = "Excluded by filter pattern configuration matches"

            records.append(
                {
                    "repository_id": self.repository_id,
                    "commit_sha": commit_sha,
                    "file_path": posix_path,
                    "size_bytes": size,
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


def os_walk_helper(dir_path: Path) -> list[tuple[str, list[str], list[str]]]:
    """Helper utilizing os.walk to avoid recursion limit depth issues."""
    import os

    results = []
    for root, dirs, files in os.walk(str(dir_path)):
        # Avoid traversing hidden folders like .git
        if ".git" in dirs:
            dirs.remove(".git")
        results.append((root, dirs, files))
    return results
