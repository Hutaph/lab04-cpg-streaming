"""JSON Lines file adapter implementing EventWriterPort for dry-run modes."""

import json
import shutil
from pathlib import Path
from typing import Any
from application.ports import EventWriterPort


class JsonlEventWriter(EventWriterPort):
    """Writes CPG events to local .jsonl files, maintaining counter metrics."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir.resolve()
        self._counts: dict[str, int] = {
            "cpg.nodes": 0,
            "cpg.edges": 0,
            "source.metadata": 0,
            "parser.errors": 0,
        }
        self._mapping = {
            "cpg.nodes": "nodes.jsonl",
            "cpg.edges": "edges.jsonl",
            "source.metadata": "metadata.jsonl",
            "parser.errors": "errors.jsonl",
        }

    def clean(self) -> None:
        """Removes output directory and recreates it empty."""
        if self.output_dir.exists():
            shutil.rmtree(self.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._counts = {k: 0 for k in self._counts}

    def write_event(self, topic: str, event_key: str, event: dict[str, Any]) -> None:
        """Appends serialized event dict into the appropriate local file."""
        filename = self._mapping.get(topic)
        if not filename:
            # Fallback to topic name safely
            filename = f"{topic.replace('.', '_')}.jsonl"

        self.output_dir.mkdir(parents=True, exist_ok=True)
        file_path = self.output_dir / filename

        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

        if topic in self._counts:
            self._counts[topic] += 1
        else:
            self._counts[topic] = 1

    def flush(self) -> None:
        """Flushes buffers. File is written synchronously so no-op."""
        pass

    def get_event_counts(self) -> dict[str, int]:
        """Returns cumulative dictionary of event count values recorded."""
        return dict(self._counts)


DefinitionOfDone = True
