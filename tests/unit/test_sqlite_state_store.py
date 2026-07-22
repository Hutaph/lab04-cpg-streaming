"""Unit tests for SQLite state store adapter."""

import sqlite3
from pathlib import Path
import pytest
from infrastructure.state.sqlite_state_store import SqliteStateStore
from domain.errors import StateStoreError
from domain.constants import PARSER_VERSION, SCHEMA_VERSION


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
    store.commit(file_id, file_path, content_hash, node_ids, edge_ids, PARSER_VERSION, SCHEMA_VERSION)

    # Retrieve state
    state = store.get(file_id)
    assert state is not None
    assert state.content_hash == "hash_v1"
    assert state.parser_version == PARSER_VERSION
    assert state.schema_version == SCHEMA_VERSION

    # Assert JSON lists are stored sorted deterministically
    assert state.node_ids == ["n1", "n2"]
    assert state.edge_ids == ["e1", "e2"]

    # Overwrite state
    store.commit(file_id, file_path, "hash_v2", ["n3"], ["e3"], PARSER_VERSION, SCHEMA_VERSION)
    state = store.get(file_id)
    assert state is not None
    assert state.content_hash == "hash_v2"
    assert state.parser_version == PARSER_VERSION
    assert state.schema_version == SCHEMA_VERSION
    assert state.node_ids == ["n3"]
    assert state.edge_ids == ["e3"]

    # Delete state
    store.delete(file_id)
    assert store.get(file_id) is None


def test_sqlite_state_store_migration_idempotent(tmp_path: Path) -> None:
    """Verify that migration is idempotent and doesn't fail if run twice or if table already exists."""
    db_file = tmp_path / "test_state.db"

    # 1. Initialize DB and commit legacy data (simulating before migration)
    with sqlite3.connect(str(db_file)) as conn:
        conn.execute(
            """
            CREATE TABLE file_state (
                file_id TEXT PRIMARY KEY,
                repository_id TEXT NOT NULL,
                file_path TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                node_ids_json TEXT NOT NULL,
                edge_ids_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO file_state VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("legacy_file", "repo", "path.py", "hash_leg", "[]", "[]", "2026-07-22"),
        )
        conn.commit()

    # 2. Instantiate SqliteStateStore, which runs the idempotent migration adding parser_version
    store = SqliteStateStore(db_file, "repo")

    state = store.get("legacy_file")
    assert state is not None
    assert state.content_hash == "hash_leg"
    assert state.parser_version is None  # Legacy column is NULL
    assert state.schema_version is None  # Legacy column is NULL

    # 3. Instantiate again to verify it is idempotent
    store_again = SqliteStateStore(db_file, "repo")
    state_again = store_again.get("legacy_file")
    assert state_again is not None
    assert state_again.parser_version is None
    assert state_again.schema_version is None


def test_sqlite_state_store_commit_failure_rollback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that state commit failure is handled gracefully and does not corrupt state."""
    db_file = tmp_path / "test_state.db"
    store = SqliteStateStore(db_file, "repo")

    # Commit initial state successfully
    store.commit("f1", "f1.py", "hash_init", [], [], PARSER_VERSION, SCHEMA_VERSION)

    # Mock sqlite3.connect to raise OperationalError on commit
    def mock_connect(*args, **kwargs):
        raise sqlite3.OperationalError("Unable to write")

    import sqlite3 as sq

    monkeypatch.setattr(sq, "connect", mock_connect)

    with pytest.raises(StateStoreError):
        store.commit("f1", "f1.py", "hash_new", [], [], PARSER_VERSION, SCHEMA_VERSION)

    monkeypatch.undo()

    # Confirm initial state f1 is still intact in the original db
    state = store.get("f1")
    assert state is not None
    assert state.content_hash == "hash_init"
