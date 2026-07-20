"""Validates outgoing CPG event dictionaries against JSON Schema specifications."""

import json
from pathlib import Path
from typing import Any
import jsonschema
from src.application.ports import EventValidatorPort
from src.domain.errors import SchemaValidationError
from src.domain.enums import EventType


class EventValidator(EventValidatorPort):
    """Caching validator loading schemas from schemas/ root folder."""

    def __init__(self, schemas_dir: Path):
        self.schemas_dir = schemas_dir.resolve()
        self._schemas: dict[str, dict[str, Any]] = {}
        self._mapping = {
            EventType.NODE_UPSERT.value: "node-event.schema.json",
            EventType.NODE_DELETE.value: "node-event.schema.json",
            EventType.EDGE_UPSERT.value: "edge-event.schema.json",
            EventType.EDGE_DELETE.value: "edge-event.schema.json",
            EventType.FILE_METADATA_UPSERT.value: "metadata-event.schema.json",
            EventType.PARSER_ERROR.value: "error-event.schema.json",
        }

    def _get_schema(self, event_type: str) -> dict[str, Any]:
        """Loads and caches schema file on demand."""
        if event_type not in self._schemas:
            schema_file = self._mapping.get(event_type)
            if not schema_file:
                raise SchemaValidationError(f"No schema mapping defined for event type: {event_type}")

            schema_path = self.schemas_dir / schema_file
            if not schema_path.exists():
                # Fallback check
                fallback_path = Path("schemas") / schema_file
                if fallback_path.exists():
                    schema_path = fallback_path

            if not schema_path.exists():
                raise SchemaValidationError(f"Schema file not found at {schema_path} for event {event_type}")

            with open(schema_path, "r", encoding="utf-8") as f:
                self._schemas[event_type] = json.load(f)

        return self._schemas[event_type]

    def validate(self, event_type: str, payload: dict[str, Any]) -> None:
        """Validates payload dict against cached schema.

        Raises SchemaValidationError if invalid.
        """
        schema = self._get_schema(event_type)
        try:
            jsonschema.validate(instance=payload, schema=schema)
        except jsonschema.ValidationError as exc:
            schema_file = self._mapping.get(event_type, "unknown")
            file_path = payload.get("file_path", "unknown")
            raise SchemaValidationError(
                f"Validation failed for event {event_type} in file {file_path}.\n"
                f"Schema path: schemas/{schema_file}\n"
                f"Reason: {exc.message}"
            ) from exc
