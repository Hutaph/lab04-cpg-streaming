"""Unit tests for DFG variable reachability tracing."""

import ast
from src.parsing.ast_builder import AstBuilder
from src.parsing.dfg_builder import DfgBuilder


def test_variable_reachability() -> None:
    """Verify that variable reads link to their defining assignments."""
    source = "a = 1\nb = a\nprint(b)"
    tree = ast.parse(source)
    file_id = "test_file_id"

    ast_builder = AstBuilder()
    dfg_builder = DfgBuilder()

    nodes, edges, node_id_mapping = ast_builder.build(tree, file_id)
    dfg_edges = dfg_builder.build(tree, file_id, node_id_mapping)

    # Should establish DFG_DEF_USE edges
    assert len(dfg_edges) == 2
    assert all(e.edge_type == "DFG_DEF_USE" for e in dfg_edges)
    
    # Assert properties
    assert dfg_edges[0].properties["variable"] == "a"
    assert dfg_edges[1].properties["variable"] == "b"


def test_scope_isolation() -> None:
    """Verify that variables from different function scopes are isolated."""
    source = "def foo(x):\n    a = x\ndef bar(y):\n    a = y"
    tree = ast.parse(source)
    file_id = "test_file_id"

    ast_builder = AstBuilder()
    dfg_builder = DfgBuilder()

    nodes, edges, node_id_mapping = ast_builder.build(tree, file_id)
    dfg_edges = dfg_builder.build(tree, file_id, node_id_mapping)

    # Variables within foo and bar should resolve separately
    assert len(dfg_edges) == 2
    scopes = {e.properties["scope"] for e in dfg_edges}
    assert "Module.foo" in scopes
    assert "Module.bar" in scopes
