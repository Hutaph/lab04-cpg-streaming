"""SQLite implementation of StateStorePort for parsing incremental history tracking."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from application.ports import StateStorePort
from domain.models import FileState
from domain.errors import StateStoreError
from parsing.identifiers import normalize_relative_path


class SqliteStateStore(StateStorePort):
    """Manages transactional file state commits and queries inside a local SQLite file."""

    def __init__(self, db_path: Path, repository_id: str):
        self.db_path = db_path.resolve()
        self.repository_id = repository_id
        self._init_db()

    def _init_db(self) -> None:
        """Ensures parent directory and database schema exist on start."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS file_state (
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
                # Idempotently add columns if missing (migration)
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(file_state)")
                columns = [row[1] for row in cursor.fetchall()]
                if "parser_version" not in columns:
                    conn.execute("ALTER TABLE file_state ADD COLUMN parser_version TEXT")
                if "schema_version" not in columns:
                    conn.execute("ALTER TABLE file_state ADD COLUMN schema_version TEXT")
                conn.commit()
        except sqlite3.Error as exc:
            raise StateStoreError(f"Failed to initialize SQLite state store: {exc}") from exc

    def get(self, file_id: str) -> FileState | None:
        """Retrieves last parsed file state by its deterministic ID."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT content_hash, node_ids_json, edge_ids_json, parser_version, schema_version FROM file_state WHERE file_id = ?",
                    (file_id,),
                )
                row = cursor.fetchone()
                if not row:
                    return None

                content_hash, node_ids_json, edge_ids_json, parser_version, schema_version = row
                node_ids = json.loads(node_ids_json)
                edge_ids = json.loads(edge_ids_json)
                return FileState(
                    file_id=file_id,
                    content_hash=content_hash,
                    node_ids=node_ids,
                    edge_ids=edge_ids,
                    parser_version=parser_version,
                    schema_version=schema_version,
                )
        except sqlite3.Error as exc:
            raise StateStoreError(f"Failed to read file state for {file_id}: {exc}") from exc

    def commit(
        self,
        file_id: str,
        file_path: str,
        content_hash: str,
        node_ids: list[str],
        edge_ids: list[str],
        parser_version: str,
        schema_version: str,
    ) -> None:
        """Saves file parsing results and IDs as sorted deterministic JSON lists."""
        normalized_file_path = normalize_relative_path(file_path)
        node_ids_json = json.dumps(sorted(node_ids))
        edge_ids_json = json.dumps(sorted(edge_ids))
        updated_at = datetime.now(timezone.utc).isoformat()

        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute(
                    """
                    INSERT INTO file_state (
                        file_id, repository_id, file_path, content_hash, node_ids_json, edge_ids_json, updated_at, parser_version, schema_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(file_id) DO UPDATE SET
                        content_hash = excluded.content_hash,
                        node_ids_json = excluded.node_ids_json,
                        edge_ids_json = excluded.edge_ids_json,
                        updated_at = excluded.updated_at,
                        parser_version = excluded.parser_version,
                        schema_version = excluded.schema_version
                    """,
                    (
                        file_id,
                        self.repository_id,
                        normalized_file_path,
                        content_hash,
                        node_ids_json,
                        edge_ids_json,
                        updated_at,
                        parser_version,
                        schema_version,
                    ),
                )
                conn.commit()
        except sqlite3.Error as exc:
            raise StateStoreError(f"Failed to commit file state for {file_id}: {exc}") from exc

    def delete(self, file_id: str) -> None:
        """Deletes file state history from table."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute("DELETE FROM file_state WHERE file_id = ?", (file_id,))
                conn.commit()
        except sqlite3.Error as exc:
            raise StateStoreError(f"Failed to delete file state for {file_id}: {exc}") from exc
