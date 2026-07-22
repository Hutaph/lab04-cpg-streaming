"""Port definitions (Protocols) for secondary adapters to implement."""

from pathlib import Path
from typing import Any, Protocol
from domain.models import FileState, ParsedFileGraph


class SourceRepositoryPort(Protocol):
    """Port for cloning source repository and traversing candidate files."""

    def clone_repository(self) -> None:
        """Clones the repository if it does not already exist."""
        ...

    def get_commit_hash(self) -> str:
        """Returns the current commit hash (HEAD) of the repository."""
        ...

    def list_files(self, source_root: Path | None = None) -> list[Path]:
        """Lists paths to all eligible files inside the source root."""
        ...

    def read_file(self, relative_path: Path) -> bytes:
        """Reads raw bytes of a file from the repository path."""
        ...

    def resolve_path(self, relative_path: Path) -> Path:
        """Resolves target relative path to absolute workspace path."""
        ...


class ParserPort(Protocol):
    """Port for parsing source code into a CPG graph model."""

    def parse_file(self, relative_path: Path, source_code: bytes, commit_sha: str) -> ParsedFileGraph:
        """Parses a file and wraps the output in a ParsedFileGraph model."""
        ...


class EventWriterPort(Protocol):
    """Port for writing events locally (e.g. JSONL) in dry-run mode."""

    def write_event(self, topic: str, event_key: str, event: dict[str, Any]) -> None:
        """Writes a single event to a local file/destination."""
        ...

    def flush(self) -> None:
        """Flushes any buffers to local disk."""
        ...

    def clean(self) -> None:
        """Cleans/removes output files before run starts."""
        ...


class EventPublisherPort(Protocol):
    """Port for publishing events to external stream brokers (e.g. Kafka)."""

    def publish_event(self, topic: str, event_key: str, event: dict[str, Any]) -> None:
        """Publishes event payload with partition key."""
        ...

    def flush(self) -> None:
        """Blocks until all outstanding messages are delivered."""
        ...


class StateStorePort(Protocol):
    """Port for managing local SQLite file parser states."""

    def get(self, file_id: str) -> FileState | None:
        """Retrieves last parsed file state."""
        ...

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
        """Commits/updates file state atomically."""
        ...

    def delete(self, file_id: str) -> None:
        """Deletes file state (e.g. if file deleted from repo)."""
        ...


class EventValidatorPort(Protocol):
    """Port for validating event structures against JSON schemas."""

    def validate(self, event_type: str, payload: dict[str, Any]) -> None:
        """Validates payload dict against schema, raises SchemaValidationError if invalid."""
        ...


class ManifestWriterPort(Protocol):
    """Port for outputting run manifest files."""

    def write_manifest(self, records: list[dict[str, Any]]) -> None:
        """Writes array of file audit metadata records to manifest file."""
        ...


class ClockPort(Protocol):
    """Port for providing ISO 8601 UTC timestamps."""

    def now_iso(self) -> str:
        """Returns current time as an ISO-8601 string ending with Z."""
        ...
