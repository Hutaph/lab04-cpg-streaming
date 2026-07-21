"""Builds statement-level control-flow graphs (CFG) for Python scopes."""

import ast
from domain.models import CodeNode, CodeEdge
from parsing.identifiers import IdentifierGenerator


class CfgBuilder:
    """Constructs statement-level control flow graphs, handling conditional branches and loops."""

    def build(
        self,
        tree: ast.AST,
        file_id: str,
        node_id_mapping: dict[int, CodeNode],
    ) -> tuple[list[CodeNode], list[CodeEdge]]:
        """Constructs CFG nodes and edges for all scopes in the AST."""
        synthetic_nodes: list[CodeNode] = []
        cfg_edges: list[CodeEdge] = []

        def get_node_id(node: ast.AST) -> str:
            return node_id_mapping[id(node)].node_id

        def add_edge(source_id: str, target_id: str, edge_type: str, role: str = "") -> None:
            edge_id = IdentifierGenerator.generate_edge_id(source_id, edge_type, target_id, role)
            cfg_edges.append(
                CodeEdge(
                    edge_id=edge_id,
                    file_id=file_id,
                    source_id=source_id,
                    target_id=target_id,
                    edge_type=edge_type,
                    properties={"role": role} if role else {},
                )
            )

        def process_scope(
            body: list[ast.stmt],
            scope_name: str,
            ast_path: str,
        ) -> None:
            if not body:
                return

            # Create synthetic ScopeEntry and ScopeExit nodes
            entry_id = IdentifierGenerator.generate_node_id(
                file_id=file_id,
                node_type="ScopeEntry",
                qualified_scope=scope_name,
                semantic_key="",
                ast_path=f"{ast_path}.entry",
            )
            exit_id = IdentifierGenerator.generate_node_id(
                file_id=file_id,
                node_type="ScopeExit",
                qualified_scope=scope_name,
                semantic_key="",
                ast_path=f"{ast_path}.exit",
            )

            synthetic_nodes.append(
                CodeNode(
                    node_id=entry_id,
                    file_id=file_id,
                    node_type="ScopeEntry",
                    ast_path=f"{ast_path}.entry",
                    name="entry",
                    qualified_name=f"{scope_name}.entry",
                    line_start=None,
                    column_start=None,
                    line_end=None,
                    column_end=None,
                )
            )
            synthetic_nodes.append(
                CodeNode(
                    node_id=exit_id,
                    file_id=file_id,
                    node_type="ScopeExit",
                    ast_path=f"{ast_path}.exit",
                    name="exit",
                    qualified_name=f"{scope_name}.exit",
                    line_start=None,
                    column_start=None,
                    line_end=None,
                    column_end=None,
                )
            )

            def build_block_cfg(
                stmts: list[ast.stmt],
                next_ids: list[str],
                break_ids: list[str],
                continue_ids: list[str],
            ) -> list[str]:
                """Connects statements in a block and returns entry point node IDs of this block."""
                if not stmts:
                    return next_ids

                # Process from back to front to easily chain targets
                current_next = next_ids
                for i in range(len(stmts) - 1, -1, -1):
                    stmt = stmts[i]
                    stmt_id = get_node_id(stmt)

                    if isinstance(stmt, ast.If):
                        # True branch
                        true_targets = build_block_cfg(stmt.body, current_next, break_ids, continue_ids)
                        for t in true_targets:
                            add_edge(stmt_id, t, "CFG_TRUE")

                        # False branch
                        false_targets = build_block_cfg(stmt.orelse, current_next, break_ids, continue_ids)
                        for f in false_targets:
                            add_edge(stmt_id, f, "CFG_FALSE")

                        # If statement node is the entry to this conditional block
                        current_next = [stmt_id]

                    elif isinstance(stmt, (ast.While, ast.For)):
                        # Loop Body entry
                        # continue jumps back to the loop test node
                        body_targets = build_block_cfg(stmt.body, [stmt_id], current_next, continue_ids=current_next)
                        for b in body_targets:
                            add_edge(stmt_id, b, "CFG_LOOP_BODY")

                        # Loop Exit
                        for n in current_next:
                            add_edge(stmt_id, n, "CFG_LOOP_EXIT")

                        # Connect end of loop body back
                        if stmt.body:
                            last_body_stmt = stmt.body[-1]
                            # Only connect back if it doesn't return or break
                            if not isinstance(last_body_stmt, (ast.Return, ast.Break, ast.Continue)):
                                add_edge(get_node_id(last_body_stmt), stmt_id, "CFG_LOOP_BACK")

                        current_next = [stmt_id]

                    elif isinstance(stmt, ast.Return):
                        add_edge(stmt_id, exit_id, "CFG_RETURN")
                        current_next = []  # dead code after return

                    elif isinstance(stmt, ast.Break):
                        for b in break_ids:
                            add_edge(stmt_id, b, "CFG_LOOP_EXIT")
                        current_next = []

                    elif isinstance(stmt, ast.Continue):
                        for c in continue_ids:
                            add_edge(stmt_id, c, "CFG_LOOP_BACK")
                        current_next = []

                    else:
                        # Simple sequential statements (Expr, Assign, Pass, etc.)
                        for n in current_next:
                            add_edge(stmt_id, n, "CFG_NEXT")
                        current_next = [stmt_id]

                return current_next

            # Connect synthetic ScopeEntry to block entry points
            block_entries = build_block_cfg(body, [exit_id], [], [])
            for entry in block_entries:
                add_edge(entry_id, entry, "CFG_NEXT")

        # Walk AST to find all scopes
        for node in ast.walk(tree):
            if isinstance(node, ast.Module):
                # Resolve scope name
                process_scope(node.body, "Module", "Module")
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                # Locate parents to compute qualified scope
                mapped_node = node_id_mapping.get(id(node))
                scope_name = mapped_node.qualified_name if (mapped_node and mapped_node.qualified_name) else node.name
                ast_path = mapped_node.ast_path if (mapped_node and mapped_node.ast_path) else node.name
                process_scope(node.body, scope_name, ast_path)

        return synthetic_nodes, cfg_edges
