"""Stable identifier generator for CPG entities and events using SHA-256."""

import hashlib
from pathlib import Path, PureWindowsPath


from domain.constants import PARSER_VERSION, SCHEMA_VERSION


def normalize_relative_path(file_path: str | Path) -> str:
    """Return the canonical relative POSIX path used at parser/event/state boundaries."""
    raw_path = str(file_path).strip()
    if not raw_path:
        raise ValueError("Path must not be empty.")

    if PureWindowsPath(raw_path).drive:
        raise ValueError(f"Absolute path is not allowed: {raw_path}")

    path_str = raw_path.replace("\\", "/")
    if path_str.startswith("/"):
        raise ValueError(f"Absolute path is not allowed: {path_str}")

    parts = [part for part in path_str.split("/") if part not in ("", ".")]
    if not parts:
        raise ValueError("Path must not resolve to repository root.")
    if any(part == ".." for part in parts):
        raise ValueError(f"Path contains invalid directory traversal: {path_str}")

    return "/".join(parts)


class IdentifierGenerator:
    """Utility class to compute deterministic hashes for CPG files, nodes, edges, and events."""

    @staticmethod
    def normalize_path(file_path: str | Path) -> str:
        """Return the canonical relative POSIX path used for stable IDs."""
        return normalize_relative_path(file_path)

    @classmethod
    def generate_file_id(cls, repository_id: str, relative_path: str | Path) -> str:
        """Computes stable file_id: SHA256(repository_id + '|' + normalized_relative_path)."""
        normalized = cls.normalize_path(relative_path)
        raw = f"{repository_id}|{normalized}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def generate_content_hash(raw_source_bytes: bytes) -> str:
        """Computes SHA256 of raw unmodified file source bytes."""
        return hashlib.sha256(raw_source_bytes).hexdigest()

    @staticmethod
    def generate_node_id(
        file_id: str,
        node_type: str,
        qualified_scope: str,
        semantic_key: str,
        ast_path: str,
    ) -> str:
        """Computes stable node_id: SHA256(file_id + '|' + node_type + '|' + qualified_scope + '|' + semantic_key + '|' + ast_path)."""
        raw = f"{file_id}|{node_type}|{qualified_scope}|{semantic_key}|{ast_path}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def generate_edge_id(
        source_id: str,
        edge_type: str,
        target_id: str,
        deterministic_role: str,
    ) -> str:
        """Computes stable edge_id: SHA256(source_id + '|' + edge_type + '|' + target_id + '|' + deterministic_role)."""
        raw = f"{source_id}|{edge_type}|{target_id}|{deterministic_role}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def generate_event_id(
        event_type: str,
        entity_id: str,
        content_hash: str,
        parser_version: str = PARSER_VERSION,
        schema_version: str = SCHEMA_VERSION,
    ) -> str:
        """Computes stable event_id: SHA256(schema_version + '|' + parser_version + '|' + event_type + '|' + entity_id + '|' + content_hash)."""
        raw = f"{schema_version}|{parser_version}|{event_type}|{entity_id}|{content_hash}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
