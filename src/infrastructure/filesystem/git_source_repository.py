"""Git source repository adapter implementing file system searches and git commands."""

from collections.abc import Iterable
import fnmatch
import hashlib
import os
import subprocess
from pathlib import Path
from typing import Any
import yaml
from application.ports import SourceRepositoryPort
from domain.errors import RepositoryNotFoundError, ParsingError


DEFAULT_FILTER_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config/file_filters.yaml"


class GitSourceRepository(SourceRepositoryPort):
    """Adapter to manage git shallow cloning, commit discovery, and Python file listing."""

    def __init__(self, repo_path: Path, clone_url: str, target_commit: str | None = None):
        self.repo_path = repo_path.resolve()
        self.clone_url = clone_url
        self.target_commit = target_commit
        self.file_size_limit_bytes = 5 * 1024 * 1024  # Default 5MB
        self._filter_config_cache: dict[str, Any] | None = None
        self._scope_cache: dict[str, dict[str, Any]] = {}

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

    def list_python_files(self, source_root: Path | None = None) -> list[Path]:
        """Traverses the repository tree and returns every Python file before filtering."""
        target_root = self._resolve_source_root(source_root)
        discovered: list[Path] = []

        for root, dirs, files in os.walk(str(target_root)):
            self._prune_ignored_directories(dirs)
            for file_name in files:
                if not file_name.endswith(".py"):
                    continue
                abs_file_path = Path(root) / file_name
                try:
                    rel_to_repo = abs_file_path.relative_to(self.repo_path)
                except ValueError:
                    continue
                discovered.append(rel_to_repo)

        return sorted(set(discovered), key=lambda path: path.as_posix())

    def list_files(self, source_root: Path | None = None) -> list[Path]:
        """Applies scope filters to repository-wide raw Python files."""
        scope = os.getenv("PARSER_SCOPE", "final")
        scope_data = self._load_scope(scope)
        include_patterns = scope_data.get("include", ["**/*.py"])

        selected: list[Path] = []
        for relative_path in self.list_python_files(source_root):
            rel_path_str = relative_path.as_posix()
            if not self._matches_any(rel_path_str, include_patterns):
                continue
            if self._get_exclusion_reason_from_scope_data(relative_path, scope_data):
                continue
            selected.append(relative_path)

        limit = self.get_scope_limit(scope)
        if limit is not None:
            selected = selected[:limit]
        return selected

    def get_exclusion_reason(self, relative_path: Path, scope: str) -> str | None:
        """Returns the first configured exclusion reason matched by the path."""
        scope_data = self._load_scope(scope)
        return self._get_exclusion_reason_from_scope_data(relative_path, scope_data)

    def _get_exclusion_reason_from_scope_data(self, relative_path: Path, scope_data: dict[str, Any]) -> str | None:
        rel_path_str = relative_path.as_posix()
        for rule in scope_data.get("exclude", []):
            pattern, reason = self._normalize_exclusion_rule(rule)
            if self._matches_pattern(rel_path_str, pattern):
                return reason
        return None

    def get_scope_limit(self, scope: str) -> int | None:
        """Returns the configured max file count for bounded scopes."""
        scope_data = self._load_scope(scope)
        limit = scope_data.get("max_files")
        if limit is None:
            return None
        return int(limit)

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

    def compute_content_hash(self, relative_path: Path) -> str:
        """Computes a stable content hash for manifest audit records."""
        digest = hashlib.sha256()
        with open(self.resolve_path(relative_path), "rb") as file_obj:
            for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _resolve_source_root(self, source_root: Path | None) -> Path:
        if source_root is None:
            return self.repo_path
        candidate = source_root
        if not candidate.is_absolute():
            candidate = self.repo_path / candidate
        resolved = candidate.resolve()
        try:
            resolved.relative_to(self.repo_path)
        except ValueError as exc:
            raise RepositoryNotFoundError(f"Source root escapes repository root: {source_root}") from exc
        if not resolved.exists():
            raise RepositoryNotFoundError(f"Source root does not exist: {resolved}")
        return resolved

    def _load_filter_config(self) -> dict[str, Any]:
        if self._filter_config_cache is not None:
            return self._filter_config_cache
        filter_path = DEFAULT_FILTER_CONFIG_PATH
        if not filter_path.exists():
            filter_path = Path("config/file_filters.yaml")
        if not filter_path.exists():
            self._filter_config_cache = {}
            return self._filter_config_cache
        with open(filter_path, "r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f) or {}
        if not isinstance(loaded, dict):
            self._filter_config_cache = {}
            return self._filter_config_cache
        self._filter_config_cache = loaded
        return self._filter_config_cache

    def _load_scope(self, scope: str) -> dict[str, Any]:
        if scope in self._scope_cache:
            return self._scope_cache[scope]
        filters_data = self._load_filter_config()
        raw_scope = filters_data.get(scope, {})
        if not isinstance(raw_scope, dict):
            self._scope_cache[scope] = {"include": ["**/*.py"], "exclude": []}
            return self._scope_cache[scope]

        parent_name = raw_scope.get("extends")
        if isinstance(parent_name, str):
            parent_scope = self._load_scope(parent_name)
            merged = dict(parent_scope)
            merged.update({key: value for key, value in raw_scope.items() if key != "extends"})
            self._scope_cache[scope] = merged
            return merged
        self._scope_cache[scope] = raw_scope
        return self._scope_cache[scope]

    @staticmethod
    def _normalize_exclusion_rule(rule: Any) -> tuple[str, str]:
        if isinstance(rule, dict):
            pattern = str(rule.get("pattern", ""))
            reason = str(rule.get("reason", "Excluded by filter pattern configuration"))
            return pattern, reason
        return str(rule), "Excluded by filter pattern configuration"

    @staticmethod
    def _matches_pattern(rel_path_str: str, pattern: str) -> bool:
        file_name = rel_path_str.rsplit("/", maxsplit=1)[-1]
        pattern_variants = {pattern}
        if pattern.startswith("**/"):
            pattern_variants.add(pattern[3:])
        if "/**/" in pattern:
            pattern_variants.add(pattern.replace("/**/", "/"))
        return any(
            fnmatch.fnmatch(rel_path_str, variant) or fnmatch.fnmatch(file_name, variant)
            for variant in pattern_variants
        )

    @classmethod
    def _matches_any(cls, rel_path_str: str, patterns: Iterable[str]) -> bool:
        return any(cls._matches_pattern(rel_path_str, pattern) for pattern in patterns)

    @staticmethod
    def _prune_ignored_directories(dirs: list[str]) -> None:
        for ignored in (".git", ".mypy_cache", ".pytest_cache", ".ruff_cache"):
            if ignored in dirs:
                dirs.remove(ignored)


DefinitionOfDone = True
