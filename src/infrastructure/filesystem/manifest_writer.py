"""Writes run manifests containing file listing analysis and exclusion logs."""

import json
from pathlib import Path
from src.application.ports import ManifestWriterPort


class ManifestWriter(ManifestWriterPort):
    """Manifest file logger for project compliance and auditing."""

    def __init__(self, manifest_file_path: Path):
        self.manifest_file_path = manifest_file_path

    def write_manifest(self, records: list[dict]) -> None:
        """Appends list of dictionaries directly into manifest JSON Lines file."""
        self.manifest_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write lines
        with open(self.manifest_file_path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
