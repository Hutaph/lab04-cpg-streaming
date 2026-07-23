import sys
from pathlib import Path

# Add scripts folder to sys.path to allow importing deploy_connectors
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.append(str(scripts_dir))

import deploy_connectors  # noqa: E402


def test_mask_sensitive() -> None:
    """Verify that sensitive values are masked correctly in log messages."""
    assert deploy_connectors.mask_sensitive("neo4j password is secret123", "secret123") == "neo4j password is [hidden]"
    assert deploy_connectors.mask_sensitive("no matching password here", "secret") == "no matching password here"
    assert deploy_connectors.mask_sensitive("some message", "") == "some message"
    assert deploy_connectors.mask_sensitive("default placeholder", "CHANGE_ME_NEO4J_PASSWORD") == "default placeholder"


def test_load_env() -> None:
    """Verify that environment variables are parsed correctly from .env and os.environ."""
    env = deploy_connectors.load_env()
    assert isinstance(env, dict)
    assert "KAFKA_BOOTSTRAP_SERVERS" in env
    assert env.get("TOPIC_NODES") == "cpg.nodes"
    assert env.get("TOPIC_EDGES") == "cpg.edges"


def test_generation_id_determinism() -> None:
    """Verify build_generation_id is deterministic and sensitive to all four inputs.

    The canonical generation_id is a colon-separated string — NOT a hash.
    The returned value must exactly match what the Neo4j Connector Cypher stores.
    """
    import generation_helper

    # Same inputs → identical string
    g1 = generation_helper.build_generation_id("file_1", "hash_1", "1.0.0", "1.0")
    g2 = generation_helper.build_generation_id("file_1", "hash_1", "1.0.0", "1.0")
    assert g1 == g2

    # Exact canonical format must match Cypher connector storage
    assert g1 == "file_1:hash_1:1.0.0:1.0"

    # Each input field independently changes the generation identifier
    assert g1 != generation_helper.build_generation_id("file_2", "hash_1", "1.0.0", "1.0")
    assert g1 != generation_helper.build_generation_id("file_1", "hash_2", "1.0.0", "1.0")
    assert g1 != generation_helper.build_generation_id("file_1", "hash_1", "2.0.0", "1.0")
    assert g1 != generation_helper.build_generation_id("file_1", "hash_1", "1.0.0", "2.0")

    # Unicode inputs produce stable canonical strings
    u1 = generation_helper.build_generation_id("file_⚡", "hash_☃", "1.0.0-🎨", "1.0-🚀")
    u2 = generation_helper.build_generation_id("file_⚡", "hash_☃", "1.0.0-🎨", "1.0-🚀")
    assert u1 == u2
    assert u1 == "file_⚡:hash_☃:1.0.0-🎨:1.0-🚀"
