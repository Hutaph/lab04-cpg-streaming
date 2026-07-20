"""Unit tests for SQLite state store adapter."""

from pathlib import Path
from infrastructure.state.sqlite_state_store import SqliteStateStore


def test_sqlite_state_store_flow(tmp_path: Path) -> None:
    """Verify commit, retrieve, and override cycles in SQLite state store."""
    db_file = tmp_path / "test_state.db"
    store = SqliteStateStore(db_file, "huggingface/transformers-pr-agent")

    file_id = "test_file_id"
    file_path = "src/transformers/activations.py"
    content_hash = "hash_v1"
    node_ids = ["n2", "n1"]  # out of order
    edge_ids = ["e2", "e1"]

    # Commit state
    store.commit(file_id, file_path, content_hash, node_ids, edge_ids)

    # Retrieve state
    state = store.get(file_id)
    assert state is not None
    assert state.content_hash == "hash_v1"

    # Assert JSON lists are stored sorted deterministically
    assert state.node_ids == ["n1", "n2"]
    assert state.edge_ids == ["e1", "e2"]

    # Overwrite state
    store.commit(file_id, file_path, "hash_v2", ["n3"], ["e3"])
    state = store.get(file_id)
    assert state is not None
    assert state.content_hash == "hash_v2"
    assert state.node_ids == ["n3"]
    assert state.edge_ids == ["e3"]

    # Delete state
    store.delete(file_id)
    assert store.get(file_id) is None
