"""Builds AST nodes and parent-child edges from Python ast module objects."""

import ast
from typing import Any
from domain.models import CodeNode, CodeEdge
from parsing.identifiers import IdentifierGenerator


class AstBuilder:
    """Traverses Python AST to construct CodeNode and CPG AST child edges."""

    def build(self, tree: ast.AST, file_id: str) -> tuple[list[CodeNode], list[CodeEdge], dict[int, CodeNode]]:
        """Parses the AST tree and returns lists of CodeNodes, CodeEdges, and a mapping of object_id to node_id."""
        nodes: dict[str, CodeNode] = {}
        edges: dict[str, CodeEdge] = {}
        node_id_mapping: dict[int, CodeNode] = {}  # Map Python object id(node) -> CodeNode

        def get_semantic_key(node: ast.AST) -> str:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                return node.name
            elif isinstance(node, ast.Name):
                return node.id
            elif isinstance(node, ast.arg):
                return node.arg
            elif isinstance(node, ast.Attribute):
                return node.attr
            elif isinstance(node, ast.Constant):
                return type(node.value).__name__
            return ""

        def traverse(node: ast.AST, parent_path: str, qualified_scope: str, parent_id: str | None) -> None:
            node_type = type(node).__name__
            semantic_key = get_semantic_key(node)
            ast_path = parent_path if parent_path else node_type

            # Generate stable node ID
            node_id = IdentifierGenerator.generate_node_id(
                file_id=file_id,
                node_type=node_type,
                qualified_scope=qualified_scope,
                semantic_key=semantic_key,
                ast_path=ast_path,
            )

            # Node properties extraction
            properties: dict[str, Any] = {}
            if isinstance(node, ast.Constant):
                properties["value_type"] = type(node.value).__name__
                properties["has_value"] = node.value is not None

            line_start = getattr(node, "lineno", None)
            column_start = getattr(node, "col_offset", None)
            line_end = getattr(node, "end_lineno", None)
            column_end = getattr(node, "end_col_offset", None)

            name = (
                getattr(node, "name", None)
                or getattr(node, "id", None)
                or getattr(node, "arg", None)
                or getattr(node, "attr", None)
            )
            if isinstance(node, ast.Module):
                name = "Module"
            if name is not None:
                name = str(name)

            qualified_name = None
            if name:
                if isinstance(node, ast.Module):
                    qualified_name = "Module"
                else:
                    qualified_name = f"{qualified_scope}.{name}" if qualified_scope else name

            # Deduplicate nodes by node_id
            if node_id not in nodes:
                nodes[node_id] = CodeNode(
                    node_id=node_id,
                    file_id=file_id,
                    node_type=node_type,
                    ast_path=ast_path,
                    name=name,
                    qualified_name=qualified_name,
                    line_start=line_start,
                    column_start=column_start,
                    line_end=line_end,
                    column_end=column_end,
                    properties=properties,
                )

            node_id_mapping[id(node)] = nodes[node_id]

            # Determine next qualified scope scope
            next_scope = qualified_scope
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                next_scope = f"{qualified_scope}.{node.name}" if qualified_scope else node.name

            # Traverse child fields
            for field_name, value in ast.iter_fields(node):
                if isinstance(value, ast.AST):
                    child_path = f"{ast_path}.{field_name}"
                    child_id = IdentifierGenerator.generate_node_id(
                        file_id=file_id,
                        node_type=type(value).__name__,
                        qualified_scope=next_scope,
                        semantic_key=get_semantic_key(value),
                        ast_path=child_path,
                    )

                    # Create AST_CHILD edge
                    role = f"{field_name}|0"
                    edge_id = IdentifierGenerator.generate_edge_id(node_id, "AST_CHILD", child_id, role)
                    if edge_id not in edges:
                        edges[edge_id] = CodeEdge(
                            edge_id=edge_id,
                            file_id=file_id,
                            source_id=node_id,
                            target_id=child_id,
                            edge_type="AST_CHILD",
                            properties={"field": field_name, "child_index": 0},
                        )
                    traverse(value, child_path, next_scope, node_id)

                elif isinstance(value, list):
                    for index, child_node in enumerate(value):
                        if isinstance(child_node, ast.AST):
                            child_path = f"{ast_path}.{field_name}[{index}]"
                            child_id = IdentifierGenerator.generate_node_id(
                                file_id=file_id,
                                node_type=type(child_node).__name__,
                                qualified_scope=next_scope,
                                semantic_key=get_semantic_key(child_node),
                                ast_path=child_path,
                            )

                            role = f"{field_name}|{index}"
                            edge_id = IdentifierGenerator.generate_edge_id(node_id, "AST_CHILD", child_id, role)
                            if edge_id not in edges:
                                edges[edge_id] = CodeEdge(
                                    edge_id=edge_id,
                                    file_id=file_id,
                                    source_id=node_id,
                                    target_id=child_id,
                                    edge_type="AST_CHILD",
                                    properties={"field": field_name, "child_index": index},
                                )
                            traverse(child_node, child_path, next_scope, node_id)

        traverse(tree, "Module", "Module", None)
        return list(nodes.values()), list(edges.values()), node_id_mapping
