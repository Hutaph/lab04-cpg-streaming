"""Coordinates individual builders to parse a Python file into a unified CPG graph."""

from pathlib import Path
from src.domain.models import CpgGraph


class CpgParser:
    """Combines AST, CFG, DFG, and Call builders into a single parsing execution."""

    def __init__(self, repo_path: Path):
        self.repo_path = repo_path

    def parse_file(self, file_path: Path) -> CpgGraph:
        """Parses a file and wraps the output in a CpgGraph model."""
        raise NotImplementedError("CPG Parser orchestrator will be implemented in Phase 2")
