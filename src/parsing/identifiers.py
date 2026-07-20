"""Stable identifier generator for CPG entities and events using SHA-256."""

import hashlib
from pathlib import Path


class IdentifierGenerator:
    """Utility class to compute deterministic hashes for CPG files, nodes, edges, and events."""

    @staticmethod
    def normalize_path(file_path: Path) -> str:
        """Converts backslashes to forward slashes and returns relative POSIX path string.

        Ensures no absolute paths or directory traversal outside repository context is used.
        """
        # Ensure path is converted to a POSIX path format with forward slashes
        path_str = str(file_path).replace("\\", "/")
        
        # Check and clean directory traversal risks
        if path_str.startswith("../") or "/../" in path_str:
            raise ValueError(f"Path contains invalid directory traversal: {path_str}")
        if Path(path_str).is_absolute():
            raise ValueError(f"Absolute path is not allowed: {path_str}")
            
        return path_str

    @classmethod
    def generate_file_id(cls, repository_id: str, relative_path: Path) -> str:
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
    ) -> str:
        """Computes stable event_id: SHA256(event_type + '|' + entity_id + '|' + content_hash)."""
        raw = f"{event_type}|{entity_id}|{content_hash}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
