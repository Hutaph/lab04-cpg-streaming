"""Unit tests for Call-graph resolution logic."""

import ast
from src.parsing.ast_builder import AstBuilder
from src.parsing.call_builder import CallBuilder


def test_function_call_resolution() -> None:
    """Verify that invocation sites link to stable targets."""
    source = "def foo(x):\n    pass\nfoo(1)"
    tree = ast.parse(source)
    file_id = "test_file_id"

    ast_builder = AstBuilder()
    call_builder = CallBuilder()

    nodes, edges, node_id_mapping = ast_builder.build(tree, file_id)
    ext_nodes, call_edges = call_builder.build(tree, file_id, node_id_mapping)

    # Call to foo should resolve to local FunctionDef node
    assert len(call_edges) == 1
    assert call_edges[0].edge_type == "CALLS"
    assert call_edges[0].properties["resolution_type"] == "LOCAL_FUNCTION"
    assert len(ext_nodes) == 0  # No external symbols since it is resolved locally


def test_imported_symbol_resolution() -> None:
    """Verify that imported functions produce ExternalSymbol target nodes."""
    source = "from math import sqrt\nsqrt(16)"
    tree = ast.parse(source)
    file_id = "test_file_id"

    ast_builder = AstBuilder()
    call_builder = CallBuilder()

    nodes, edges, node_id_mapping = ast_builder.build(tree, file_id)
    ext_nodes, call_edges = call_builder.build(tree, file_id, node_id_mapping)

    # sqrt call resolves to math.sqrt
    assert len(call_edges) == 1
    assert call_edges[0].properties["resolution_type"] == "IMPORTED_SYMBOL"
    assert len(ext_nodes) == 1
    assert ext_nodes[0].node_type == "ExternalSymbol"
    assert ext_nodes[0].qualified_name == "math.sqrt"
