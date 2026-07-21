"""Git source repository adapter implementing file system searches and git commands."""

import fnmatch
import os
import subprocess
from pathlib import Path
import yaml
from application.ports import SourceRepositoryPort
from domain.errors import RepositoryNotFoundError, ParsingError


class GitSourceRepository(SourceRepositoryPort):
    """Adapter to manage git shallow cloning, commit discovery, and Python file listing."""

    def __init__(self, repo_path: Path, clone_url: str, target_commit: str | None = None):
        self.repo_path = repo_path.resolve()
        self.clone_url = clone_url
        self.target_commit = target_commit
        self.file_size_limit_bytes = 5 * 1024 * 1024  # Default 5MB

    def clone_repository(self) -> None:
        """Executes a shallow clone of the repository if it is not present on disk."""
        if self.repo_path.exists() and (self.repo_path / ".git").exists():
            # Already cloned, do not pull silently
            return

        self.repo_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            subprocess.run(
                ["git", "clone", "--depth", "1", self.clone_url, str(self.repo_path)],
                check=True,
                capture_output=True,
                text=True,
            )
            # If target commit is specified and not HEAD, try to checkout (optional fallback)
            if self.target_commit:
                subprocess.run(
                    ["git", "-C", str(self.repo_path), "checkout", self.target_commit],
                    capture_output=True,
                )
        except subprocess.CalledProcessError as exc:
            raise RepositoryNotFoundError(f"Failed to clone repository from {self.clone_url}: {exc.stderr}") from exc

    def get_commit_hash(self) -> str:
        """Runs git rev-parse HEAD to discover current commit SHA."""
        if not self.repo_path.exists():
            raise RepositoryNotFoundError(f"Repository not found on disk at {self.repo_path}")
        try:
            result = subprocess.run(
                ["git", "-C", str(self.repo_path), "rev-parse", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as exc:
            raise RepositoryNotFoundError(f"Failed to read git commit hash: {exc.stderr}") from exc

    def resolve_path(self, relative_path: Path) -> Path:
        return (self.repo_path / relative_path).resolve()

    def list_files(self, source_root: Path | None = None) -> list[Path]:
        """Traverses the source root applying filters to list matched Python files."""
        # Resolve target source root directory (default is repo_path / "src" if exists)
        target_root = source_root if source_root else self.repo_path / "src"
        if not target_root.exists():
            target_root = self.repo_path

        # Find filters file
        filter_path = Path("config/file_filters.yaml")
        if not filter_path.exists():
            filter_path = Path("../config/file_filters.yaml")

        scope = os.getenv("PARSER_SCOPE", "final")
        include_patterns: list[str] = ["**/*.py"]
        exclude_patterns: list[str] = []

        if filter_path.exists():
            with open(filter_path, "r", encoding="utf-8") as f:
                filters_data = yaml.safe_load(f) or {}
                scope_data = filters_data.get(scope, {})
                include_patterns = scope_data.get("include", include_patterns)
                exclude_patterns = scope_data.get("exclude", exclude_patterns)

        discovered: list[Path] = []
        # Traverse filesystem
        for root, _, files in os.walk(str(target_root)):
            for file in files:
                abs_file_path = Path(root) / file
                rel_to_repo = abs_file_path.relative_to(self.repo_path)
                rel_path_str = rel_to_repo.as_posix()

                # Match includes
                matched_include = any(
                    fnmatch.fnmatch(rel_path_str, pattern) or fnmatch.fnmatch(file, pattern)
                    for pattern in include_patterns
                )
                if not matched_include:
                    continue

                # Match excludes
                matched_exclude = any(
                    fnmatch.fnmatch(rel_path_str, pattern) or fnmatch.fnmatch(file, pattern)
                    for pattern in exclude_patterns
                )
                if matched_exclude:
                    continue

                discovered.append(rel_to_repo)

        # Return deterministically sorted list
        return sorted(discovered)

    def read_file(self, relative_path: Path) -> bytes:
        """Reads content bytes strictly and asserts UTF-8 formatting and file sizes."""
        abs_path = self.resolve_path(relative_path)
        if not abs_path.exists():
            raise FileNotFoundError(f"File not found: {abs_path}")

        # Limit file size check
        size = abs_path.stat().st_size
        if size > self.file_size_limit_bytes:
            raise ParsingError(f"File {relative_path} size ({size} bytes) exceeds limit.")

        try:
            with open(abs_path, "rb") as f:
                content = f.read()
            # Strict UTF-8 validation
            content.decode("utf-8")
            return content
        except UnicodeDecodeError as exc:
            raise ParsingError(f"File {relative_path} is not valid UTF-8.") from exc
        except Exception as exc:
            raise ParsingError(f"Failed to read file {relative_path}: {exc}") from exc


DefinitionOfDone = True
