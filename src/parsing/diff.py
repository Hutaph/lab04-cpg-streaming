"""Handles graph diffing for modified files to identify deleted elements."""

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class GraphDiff:
    """Contains information on what nodes and edges to remove/add."""

    nodes_to_delete: list[str] = field(default_factory=list)
    edges_to_delete: list[str] = field(default_factory=list)


class CpgDiffer:
    """TODO: Diff current CPG structure against previous sqlite state to emit delete events."""

    def compute_diff(self, file_path: str, new_nodes: list[str], new_edges: list[str]) -> GraphDiff:
        """Finds stale nodes and edges that are no longer present in the updated file parse."""
        raise NotImplementedError("CPG diff processor will be implemented in Phase 11")
