"""Handles graph diffing for modified files to identify deleted elements."""

from src.domain.models import FileState, ParsedFileGraph, GraphDiff


class CpgDiffer:
    """Compares current parsed CPG subgraph against historical state to compute additions and deletions."""

    def compute_diff(self, previous: FileState | None, current: ParsedFileGraph) -> GraphDiff:
        """Finds stale node/edge IDs and compiles current nodes/edges to upsert."""
        if not previous:
            return GraphDiff(
                removed_node_ids=[],
                removed_edge_ids=[],
                current_nodes=current.nodes,
                current_edges=current.edges,
            )

        old_node_ids = set(previous.node_ids)
        old_edge_ids = set(previous.edge_ids)

        current_node_ids = {n.node_id for n in current.nodes}
        current_edge_ids = {e.edge_id for e in current.edges}

        removed_node_ids = list(old_node_ids - current_node_ids)
        removed_edge_ids = list(old_edge_ids - current_edge_ids)

        # Upserting all current elements ensures properties are updated even if the ID is unchanged.
        return GraphDiff(
            removed_node_ids=removed_node_ids,
            removed_edge_ids=removed_edge_ids,
            current_nodes=current.nodes,
            current_edges=current.edges,
        )
