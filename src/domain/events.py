"""Domain events representing updates to the Code Property Graph."""

from dataclasses import dataclass
from typing import Any
from domain.enums import EventType


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """Common envelope attributes for all Kafka streaming events."""

    schema_version: str
    event_id: str
    event_type: EventType
    event_time: str
    repository_id: str
    commit_sha: str
    file_id: str
    file_path: str
    content_hash: str
    parser_version: str
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Serializes event envelope and unpacks payload to root/nested dict format."""
        result = {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "event_time": self.event_time,
            "repository_id": self.repository_id,
            "commit_sha": self.commit_sha,
            "file_id": self.file_id,
            "file_path": self.file_path,
            "content_hash": self.content_hash,
            "parser_version": self.parser_version,
        }
        result.update(self.payload)
        return result


class EventFactory:
    """Factory to create EventEnvelopes with correct contracts."""

    def __init__(
        self,
        repository_id: str,
        commit_sha: str,
        file_id: str,
        file_path: str,
        content_hash: str,
        parser_version: str = "1.0.0",
        schema_version: str = "1.0",
    ):
        self.repository_id = repository_id
        self.commit_sha = commit_sha
        self.file_id = file_id
        self.file_path = file_path
        self.content_hash = content_hash
        self.parser_version = parser_version
        self.schema_version = schema_version

    def _create(self, event_type: EventType, event_id: str, event_time: str, payload: dict[str, Any]) -> EventEnvelope:
        return EventEnvelope(
            schema_version=self.schema_version,
            event_id=event_id,
            event_type=event_type,
            event_time=event_time,
            repository_id=self.repository_id,
            commit_sha=self.commit_sha,
            file_id=self.file_id,
            file_path=self.file_path,
            content_hash=self.content_hash,
            parser_version=self.parser_version,
            payload=payload,
        )

    def create_node_upsert(self, event_id: str, event_time: str, node: dict[str, Any]) -> EventEnvelope:
        """Create a NODE_UPSERT event envelope."""
        return self._create(EventType.NODE_UPSERT, event_id, event_time, {"node": node})

    def create_node_delete(self, event_id: str, event_time: str, node_id: str) -> EventEnvelope:
        """Create a NODE_DELETE event envelope."""
        return self._create(EventType.NODE_DELETE, event_id, event_time, {"node": {"node_id": node_id}})

    def create_edge_upsert(self, event_id: str, event_time: str, edge: dict[str, Any]) -> EventEnvelope:
        """Create an EDGE_UPSERT event envelope."""
        return self._create(EventType.EDGE_UPSERT, event_id, event_time, {"edge": edge})

    def create_edge_delete(self, event_id: str, event_time: str, edge_id: str) -> EventEnvelope:
        """Create an EDGE_DELETE event envelope."""
        return self._create(EventType.EDGE_DELETE, event_id, event_time, {"edge": {"edge_id": edge_id}})

    def create_file_metadata_upsert(self, event_id: str, event_time: str, metadata: dict[str, Any]) -> EventEnvelope:
        """Create a FILE_METADATA_UPSERT event envelope."""
        return self._create(EventType.FILE_METADATA_UPSERT, event_id, event_time, {"metadata": metadata})

    def create_parser_error(self, event_id: str, event_time: str, error: dict[str, Any]) -> EventEnvelope:
        """Create a PARSER_ERROR event envelope."""
        return self._create(EventType.PARSER_ERROR, event_id, event_time, {"error": error})
