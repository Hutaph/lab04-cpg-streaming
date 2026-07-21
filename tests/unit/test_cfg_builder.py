"""Unit tests for CFG builder next execution transitions."""

import ast
from parsing.ast_builder import AstBuilder
from parsing.cfg_builder import CfgBuilder


def test_sequential_cfg() -> None:
    """Verify that linear statements are connected sequentially."""
    source = "x = 1\ny = 2\nprint(y)"
    tree = ast.parse(source)
    file_id = "test_file_id"

    ast_builder = AstBuilder()
    cfg_builder = CfgBuilder()

    nodes, edges, node_id_mapping = ast_builder.build(tree, file_id)
    synth_nodes, cfg_edges = cfg_builder.build(tree, file_id, node_id_mapping)

    # There should be ScopeEntry and ScopeExit nodes
    assert len(synth_nodes) == 2
    assert {n.node_type for n in synth_nodes} == {"ScopeEntry", "ScopeExit"}

    # Linear next edges
    next_edges = [e for e in cfg_edges if e.edge_type == "CFG_NEXT"]
    assert len(next_edges) > 0


def test_if_else_cfg() -> None:
    """Verify that branching structures result in CFG_TRUE and CFG_FALSE transitions."""
    source = "if x > 5:\n    y = 1\nelse:\n    y = 0"
    tree = ast.parse(source)
    file_id = "test_file_id"

    ast_builder = AstBuilder()
    cfg_builder = CfgBuilder()

    nodes, edges, node_id_mapping = ast_builder.build(tree, file_id)
    synth_nodes, cfg_edges = cfg_builder.build(tree, file_id, node_id_mapping)

    true_edges = [e for e in cfg_edges if e.edge_type == "CFG_TRUE"]
    false_edges = [e for e in cfg_edges if e.edge_type == "CFG_FALSE"]
    assert len(true_edges) == 1
    assert len(false_edges) == 1
