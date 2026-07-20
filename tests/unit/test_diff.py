"""Unit tests for graph diffing logic (detecting deleted nodes and edges)."""

from src.parsing.diff import CpgDiffer
from src.domain.models import FileState, ParsedFileGraph, CodeNode, CodeEdge, FileMetadata, SourceFile
from src.domain.enums import ParseStatus


def test_diff_deletions() -> None:
    """Verify that node and edge differences are properly detected."""
    # Previous state contains nodes 1,2,3 and edges a,b
    prev_state = FileState(
        file_id="test_file_id",
        content_hash="h1",
        node_ids=["node1", "node2", "node3"],
        edge_ids=["edgeA", "edgeB"],
    )

    # Current parse contains nodes 1,2,4 and edges a,c
    sf = SourceFile("test_repo", "workspace", "foo.py", "c1", 100)
    meta = FileMetadata("test_file_id", "test_repo", "foo.py", "h2", 100, 5, 0, 0, 0, 3, 2, 5, ParseStatus.SUCCESS)

    current_graph = ParsedFileGraph(
        source_file=sf,
        file_id="test_file_id",
        content_hash="h2",
        nodes=[
            CodeNode("node1", "test_file_id", "Module", "Module", None, None, 1, 0, 5, 0),
            CodeNode("node2", "test_file_id", "Pass", "Module.body[0]", None, None, 2, 0, 2, 0),
            CodeNode("node4", "test_file_id", "Expr", "Module.body[1]", None, None, 3, 0, 3, 0),
        ],
        edges=[
            CodeEdge("edgeA", "test_file_id", "node1", "node2", "AST_CHILD"),
            CodeEdge("edgeC", "test_file_id", "node1", "node4", "AST_CHILD"),
        ],
        metadata=meta,
    )

    differ = CpgDiffer()
    diff = differ.compute_diff(prev_state, current_graph)

    # Removed elements
    assert diff.removed_node_ids == ["node3"]
    assert diff.removed_edge_ids == ["edgeB"]

    # Current list elements are still fully included for upserts
    assert len(diff.current_nodes) == 3
    assert len(diff.current_edges) == 2
