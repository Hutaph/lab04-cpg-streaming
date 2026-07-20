"""Builds data-flow graphs (DFG) representing def-use relations of variables."""

import ast
from typing import Any
from src.domain.models import CodeNode, CodeEdge
from src.parsing.identifiers import IdentifierGenerator


class DfgBuilder:
    """Computes intraprocedural reaching definitions, mapping assignments (defs) to usages (uses)."""

    def build(
        self,
        tree: ast.AST,
        file_id: str,
        node_id_mapping: dict[int, CodeNode],
    ) -> list[CodeEdge]:
        """Runs scope-isolated def-use analysis and returns DFG_DEF_USE edges."""
        dfg_edges: list[CodeEdge] = []

        def get_node_id(node: ast.AST) -> str:
            return node_id_mapping[id(node)].node_id

        def add_dfg_edge(source_id: str, target_id: str, variable: str, scope: str) -> None:
            # Deterministic role based on target node ID to ensure stable edge ID
            role = f"dfg|{variable}"
            edge_id = IdentifierGenerator.generate_edge_id(source_id, "DFG_DEF_USE", target_id, role)
            dfg_edges.append(
                CodeEdge(
                    edge_id=edge_id,
                    file_id=file_id,
                    source_id=source_id,
                    target_id=target_id,
                    edge_type="DFG_DEF_USE",
                    properties={
                        "variable": variable,
                        "scope": scope,
                        "resolution": "REACHING_DEFINITION",
                    },
                )
            )

        def analyze_scope(scope_node: ast.AST, scope_name: str) -> None:
            # Reaching definitions tracking map: variable -> list of source node IDs
            last_defs: dict[str, list[str]] = {}

            # Add function arguments as definitions
            if isinstance(scope_node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for arg in scope_node.args.args:
                    arg_id = get_node_id(arg)
                    last_defs[arg.arg] = [arg_id]

            def merge_states(state_a: dict[str, list[str]], state_b: dict[str, list[str]]) -> dict[str, list[str]]:
                merged = {}
                keys = set(state_a.keys()).union(state_b.keys())
                for k in keys:
                    defs_a = state_a.get(k, [])
                    defs_b = state_b.get(k, [])
                    # Union and deduplicate definition source node IDs
                    merged[k] = list(set(defs_a + defs_b))
                return merged

            def visit_node(node: ast.AST, current_state: dict[str, list[str]]) -> dict[str, list[str]]:
                # Make a shallow copy of the state
                state = {k: list(v) for k, v in current_state.items()}

                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    # Do not descend into sub-scopes; they are processed independently
                    # But they define their own names in the parent scope
                    node_id = get_node_id(node)
                    state[node.name] = [node_id]
                    return state

                # If assignment, evaluate RHS (Load) before LHS (Store)
                if isinstance(node, ast.Assign):
                    # Process RHS first
                    state = visit_node(node.value, state)
                    # Process LHS
                    for target in node.targets:
                        state = visit_node(target, state)
                    return state

                if isinstance(node, (ast.AnnAssign, ast.AugAssign)):
                    # AugAssign uses the variable first, then defines it
                    if isinstance(node, ast.AugAssign):
                        state = visit_node(node.target, state)
                    if node.value:
                        state = visit_node(node.value, state)
                    state = visit_node(node.target, state)
                    return state

                if isinstance(node, ast.Name):
                    node_id = get_node_id(node)
                    if isinstance(node.ctx, ast.Store):
                        state[node.id] = [node_id]
                    elif isinstance(node.ctx, ast.Load):
                        # Create edges from all active definition nodes
                        active_defs = state.get(node.id, [])
                        for d_id in active_defs:
                            add_dfg_edge(d_id, node_id, node.id, scope_name)
                    return state

                if isinstance(node, ast.Return):
                    if node.value:
                        state = visit_node(node.value, state)
                    return state

                if isinstance(node, ast.If):
                    # 1. Condition
                    state = visit_node(node.test, state)
                    # 2. Branches
                    state_then = state
                    for stmt in node.body:
                        state_then = visit_node(stmt, state_then)

                    state_else = state
                    for stmt in node.orelse:
                        state_else = visit_node(stmt, state_else)

                    # 3. Merge branches
                    return merge_states(state_then, state_else)

                if isinstance(node, (ast.While, ast.For)):
                    # To handle loop back-edges, we simulate 2 passes over the loop body
                    # Pass 1: Propagate definitions through loop
                    state_pass1 = state
                    if isinstance(node, ast.For):
                        state_pass1 = visit_node(node.target, state_pass1)
                        state_pass1 = visit_node(node.iter, state_pass1)
                    else:
                        state_pass1 = visit_node(node.test, state_pass1)

                    for stmt in node.body:
                        state_pass1 = visit_node(stmt, state_pass1)

                    # Merge loop body defs with loop entry state
                    merged_entry = merge_states(state, state_pass1)

                    # Pass 2: Actually resolve def-uses with merged entry definitions
                    state_pass2 = merged_entry
                    if isinstance(node, ast.For):
                        state_pass2 = visit_node(node.target, state_pass2)
                        state_pass2 = visit_node(node.iter, state_pass2)
                    else:
                        state_pass2 = visit_node(node.test, state_pass2)

                    for stmt in node.body:
                        state_pass2 = visit_node(stmt, state_pass2)

                    # Return loop state merged with entry state (to support loop exit branching)
                    return merge_states(state, state_pass2)

                # Default traversal for other node types
                for child in ast.iter_child_nodes(node):
                    state = visit_node(child, state)

                return state

            # Execute sequential flow for top level of scope
            current = last_defs
            if isinstance(scope_node, ast.Module):
                for stmt in scope_node.body:
                    current = visit_node(stmt, current)
            else:
                for stmt in getattr(scope_node, "body", []):
                    current = visit_node(stmt, current)

        # Walk AST to find all scopes
        for n in ast.walk(tree):
            if isinstance(n, ast.Module):
                analyze_scope(n, "Module")
            elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                mapped_node = node_id_mapping.get(id(n))
                scope_name = mapped_node.qualified_name if mapped_node else n.name
                analyze_scope(n, scope_name)

        return dfg_edges
