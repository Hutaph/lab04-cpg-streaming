"""Unit tests for AST Node and Edge generation."""

import ast
from src.parsing.ast_builder import AstBuilder


def test_simple_ast_construction() -> None:
    """Verify that Python nodes map correctly to AST entities and structural links."""
    source = "def foo(x):\n    return x + 1"
    tree = ast.parse(source)
    file_id = "test_file_id"
    builder = AstBuilder()

    nodes, edges, node_id_mapping = builder.build(tree, file_id)

    # Check node count
    assert len(nodes) > 0
    assert len(edges) > 0

    # Ensure Module is present
    module_nodes = [n for n in nodes if n.node_type == "Module"]
    assert len(module_nodes) == 1
    assert module_nodes[0].qualified_name == "Module"

    # Ensure FunctionDef is present
    func_nodes = [n for n in nodes if n.node_type == "FunctionDef"]
    assert len(func_nodes) == 1
    assert func_nodes[0].name == "foo"
    assert func_nodes[0].qualified_name == "Module.foo"

    # Verify AST child edges have properties
    ast_child_edges = [e for e in edges if e.edge_type == "AST_CHILD"]
    assert len(ast_child_edges) > 0
    for edge in ast_child_edges:
        assert "field" in edge.properties
        assert "child_index" in edge.properties

    # Check object mapping matches IDs
    assert id(tree) in node_id_mapping
