"""Generation identifier helper for Neo4j CPG graph entities.

The canonical generation_id used by the Neo4j Kafka Sink Connector Cypher queries is a
colon-separated string (NOT a cryptographic hash):

    file_id:content_hash:parser_version:schema_version

This module provides a Python helper that returns this identical string so that
tests and scripts can assert exact equality against values stored in Neo4j.
"""


def build_generation_id(file_id: str, content_hash: str, parser_version: str, schema_version: str) -> str:
    """Build the canonical generation identifier for a CPG graph entity.

    Returns the same string that the Neo4j Kafka Sink Connector Cypher stores as
    ``generation_id`` on CPGNode, CPGEdge, CPGNodeTombstone and CPGEdgeTombstone:

        ``file_id:content_hash:parser_version:schema_version``

    This is a deterministic identifier, not a cryptographic hash.
    Two entities belong to the same generation if and only if all four fields match.

    Args:
        file_id: Stable SHA-256 file identifier (from Task 3 IdentifierGenerator).
        content_hash: SHA-256 digest of the raw source bytes at parse time.
        parser_version: Parser version string (e.g. ``"1.0.0"``).
        schema_version: Event schema version string (e.g. ``"1.0"``).

    Returns:
        Canonical generation identifier string.
    """
    return f"{file_id}:{content_hash}:{parser_version}:{schema_version}"
