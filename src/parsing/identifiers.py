"""Generates deterministic stable identifiers (hashes) for CPG elements."""


class IdentifierGenerator:
    """Generates SHA-256 stable identifiers for nodes and edges to enforce idempotency."""

    @staticmethod
    def generate_node_id(file_path: str, content_hash: str, ast_path: str, label: str) -> str:
        """TODO: Generate stable hash-based node ID."""
        raise NotImplementedError("Deterministic stable identifiers will be implemented in Phase 2")

    @staticmethod
    def generate_edge_id(edge_type: str, source_id: str, target_id: str, field_name: str | None = None, index: int | None = None) -> str:
        """TODO: Generate stable hash-based edge ID."""
        raise NotImplementedError("Deterministic stable identifiers will be implemented in Phase 2")
