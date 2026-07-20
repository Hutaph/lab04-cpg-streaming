"""Service to parse a single Python source file and publish CPG events."""

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from src.application.ports import (
    SourceRepositoryPort,
    ParserPort,
    EventWriterPort,
    EventPublisherPort,
    StateStorePort,
    EventValidatorPort,
)
from src.domain.models import SourceFile, ProcessingResult, FileState
from src.domain.enums import ParseStatus, EventType
from src.domain.events import EventFactory, EventEnvelope
from src.domain.errors import ParsingError, PublishError, SchemaValidationError
from src.parsing.identifiers import IdentifierGenerator
from src.parsing.diff import CpgDiffer


class ProcessFileService:
    """Orchestrates reading, parsing, diffing, validating, and writing of file CPG graphs."""

    def __init__(
        self,
        repo_adapter: SourceRepositoryPort,
        parser: ParserPort,
        state_store: StateStorePort,
        validator: EventValidatorPort,
        writer: EventWriterPort | EventPublisherPort,
        topic_nodes: str = "cpg.nodes",
        topic_edges: str = "cpg.edges",
        topic_metadata: str = "source.metadata",
        topic_errors: str = "parser.errors",
    ):
        self.repo_adapter = repo_adapter
        self.parser = parser
        self.state_store = state_store
        self.validator = validator
        self.writer = writer
        self.topic_nodes = topic_nodes
        self.topic_edges = topic_edges
        self.topic_metadata = topic_metadata
        self.topic_errors = topic_errors
        self.cpg_differ = CpgDiffer()

    def _write(self, topic: str, file_id: str, event: dict[str, Any]) -> None:
        """Helper to invoke either EventWriterPort or EventPublisherPort dynamically."""
        if hasattr(self.writer, "publish_event"):
            self.writer.publish_event(topic, file_id, event)  # type: ignore
        else:
            self.writer.write_event(topic, file_id, event)  # type: ignore

    def execute(self, source_file: SourceFile) -> ProcessingResult:
        """Parses a single file, publishes events to target destination, and commits SQLite state."""
        relative_path = Path(source_file.relative_path)
        
        # 1. Read raw source bytes strict
        try:
            source_bytes = self.repo_adapter.read_file(relative_path)
        except Exception as exc:
            # Handle read error
            return self._handle_failure(source_file, "ReadFileError", str(exc), "0")

        # 2. Compute IDs
        file_id = IdentifierGenerator.generate_file_id(source_file.repository_id, relative_path)
        content_hash = IdentifierGenerator.generate_content_hash(source_bytes)

        # 3. Load previous state
        prev_state = self.state_store.get(file_id)

        # 4. Check if unchanged
        if prev_state and prev_state.content_hash == content_hash:
            return ProcessingResult(
                status=ParseStatus.SKIPPED_UNCHANGED,
                file_id=file_id,
                file_path=str(relative_path),
                content_hash=content_hash,
                node_count=0,
                edge_count=0,
                emitted_event_counts={},
            )

        # 5. Parse current graph
        try:
            current_graph = self.parser.parse_file(relative_path, source_bytes, source_file.commit_sha)
        except ParsingError as exc:
            return self._handle_failure(source_file, "SyntaxError", str(exc), content_hash)

        # 6. Compute Diff
        diff = self.cpg_differ.compute_diff(prev_state, current_graph)

        # 7. Generate events using EventFactory
        factory = EventFactory(
            repository_id=source_file.repository_id,
            commit_sha=source_file.commit_sha,
            file_id=file_id,
            file_path=str(relative_path),
            content_hash=content_hash,
            parser_version="1.0.0",
            schema_version="1.0",
        )

        event_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        events_to_send: list[tuple[str, EventEnvelope]] = []

        # 1. EDGE_DELETE events
        for e_id in diff.removed_edge_ids:
            evt_id = IdentifierGenerator.generate_event_id(EventType.EDGE_DELETE.value, e_id, content_hash)
            events_to_send.append(
                (self.topic_edges, factory.create_edge_delete(evt_id, event_time, e_id))
            )

        # 2. NODE_DELETE events
        for n_id in diff.removed_node_ids:
            evt_id = IdentifierGenerator.generate_event_id(EventType.NODE_DELETE.value, n_id, content_hash)
            events_to_send.append(
                (self.topic_nodes, factory.create_node_delete(evt_id, event_time, n_id))
            )

        # 3. NODE_UPSERT events
        for node in diff.current_nodes:
            evt_id = IdentifierGenerator.generate_event_id(EventType.NODE_UPSERT.value, node.node_id, content_hash)
            # Map domain object back to contract schema dict
            node_dict = {
                "node_id": node.node_id,
                "node_type": node.node_type,
                "name": node.name,
                "qualified_name": node.qualified_name,
                "ast_path": node.ast_path,
                "line_start": node.line_start,
                "column_start": node.column_start,
                "line_end": node.line_end,
                "column_end": node.column_end,
                "properties": node.properties,
            }
            events_to_send.append(
                (self.topic_nodes, factory.create_node_upsert(evt_id, event_time, node_dict))
            )

        # 4. EDGE_UPSERT events
        for edge in diff.current_edges:
            evt_id = IdentifierGenerator.generate_event_id(EventType.EDGE_UPSERT.value, edge.edge_id, content_hash)
            edge_dict = {
                "edge_id": edge.edge_id,
                "source_id": edge.source_id,
                "target_id": edge.target_id,
                "edge_type": edge.edge_type,
                "properties": edge.properties,
            }
            events_to_send.append(
                (self.topic_edges, factory.create_edge_upsert(evt_id, event_time, edge_dict))
            )

        # 5. FILE_METADATA_UPSERT event
        meta = current_graph.metadata
        evt_id = IdentifierGenerator.generate_event_id(EventType.FILE_METADATA_UPSERT.value, file_id, content_hash)
        meta_dict = {
            "size_bytes": meta.size_bytes,
            "line_count": meta.line_count,
            "function_count": meta.function_count,
            "class_count": meta.class_count,
            "import_count": meta.import_count,
            "node_count": meta.node_count,
            "edge_count": meta.edge_count,
            "parse_duration_ms": meta.parse_duration_ms,
            "parse_status": meta.parse_status.value,
            "parser": meta.parser,
        }
        events_to_send.append(
            (self.topic_metadata, factory.create_file_metadata_upsert(evt_id, event_time, meta_dict))
        )

        # Validate events
        serialized_events = []
        for topic, envelope in events_to_send:
            evt_dict = envelope.to_dict()
            try:
                self.validator.validate(envelope.event_type.value, evt_dict)
            except SchemaValidationError as exc:
                # Syntax error event generated on validation fail
                return self._handle_failure(source_file, "SchemaValidationError", str(exc), content_hash)
            serialized_events.append((topic, evt_dict))

        # Publish/Write events and flush
        counts = {}
        try:
            for topic, evt in serialized_events:
                self._write(topic, file_id, evt)
                counts[topic] = counts.get(topic, 0) + 1
            self.writer.flush()
        except Exception as exc:
            # Fatal publish failure, abort transactional DB commit
            raise PublishError(f"Failed to publish event stream batch for {relative_path}: {exc}") from exc

        # 8. Commit to SQLite State DB only after successful delivery ack
        node_ids = [n.node_id for n in current_graph.nodes]
        edge_ids = [e.edge_id for e in current_graph.edges]
        self.state_store.commit(file_id, str(relative_path), content_hash, node_ids, edge_ids)

        return ProcessingResult(
            status=ParseStatus.SUCCESS,
            file_id=file_id,
            file_path=str(relative_path),
            content_hash=content_hash,
            node_count=len(unique_items(node_ids)),
            edge_count=len(unique_items(edge_ids)),
            emitted_event_counts=counts,
        )

    def _handle_failure(
        self,
        source_file: SourceFile,
        error_type: str,
        message: str,
        content_hash: str,
    ) -> ProcessingResult:
        """Logs PARSER_ERROR event, validates, publishes, and avoids state commit."""
        relative_path = Path(source_file.relative_path)
        file_id = IdentifierGenerator.generate_file_id(source_file.repository_id, relative_path)
        
        factory = EventFactory(
            repository_id=source_file.repository_id,
            commit_sha=source_file.commit_sha,
            file_id=file_id,
            file_path=str(relative_path),
            content_hash=content_hash,
            parser_version="1.0.0",
            schema_version="1.0",
        )
        event_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        evt_id = IdentifierGenerator.generate_event_id(EventType.PARSER_ERROR.value, file_id, content_hash)

        error_dict = {
            "error_type": error_type,
            "message": message,
            "line": None,
            "column": None,
            "retryable": False,
        }
        envelope = factory.create_parser_error(evt_id, event_time, error_dict)
        evt_dict = envelope.to_dict()

        # Validate
        try:
            self.validator.validate(envelope.event_type.value, evt_dict)
        except SchemaValidationError:
            pass  # Avoid infinite error loop, push raw

        # Push to dead-letter error queue
        try:
            self._write(self.topic_errors, file_id, evt_dict)
            self.writer.flush()
        except Exception:
            # Best effort error logging
            pass

        return ProcessingResult(
            status=ParseStatus.FAILED,
            file_id=file_id,
            file_path=str(relative_path),
            content_hash=content_hash,
            node_count=0,
            edge_count=0,
            emitted_event_counts={self.topic_errors: 1},
            error=message,
        )


def unique_items(items: list[Any]) -> list[Any]:
    return list(set(items))
