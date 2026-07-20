"""Resolves call targets and builds call-graph connections (CALLS edges)."""

import ast
from domain.models import CodeNode, CodeEdge
from parsing.identifiers import IdentifierGenerator


class CallBuilder:
    """Builds call graphs resolving local functions, imports, and dynamic external symbols."""

    def build(
        self,
        tree: ast.AST,
        file_id: str,
        node_id_mapping: dict[int, CodeNode],
    ) -> tuple[list[CodeNode], list[CodeEdge]]:
        """Scans imports and calls, and constructs CALLS edges and ExternalSymbol nodes."""
        external_nodes: dict[str, CodeNode] = {}
        call_edges: list[CodeEdge] = []

        # 1. First Pass: Scan local symbols (functions, classes) and imports
        local_symbols: dict[str, str] = {}  # short_name -> node_id
        imported_symbols: dict[str, tuple[str, str]] = {}  # alias -> (qualified_name, resolution_type)

        # Locate module level node_id for fallback caller
        module_node_id = ""
        for n in ast.walk(tree):
            if isinstance(n, ast.Module):
                module_node_id = node_id_mapping[id(n)].node_id
                break

        # Register local functions and classes defined at module level
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                node_id = node_id_mapping[id(node)].node_id
                local_symbols[node.name] = node_id

        # Scan imports
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    local_name = alias.asname or alias.name
                    imported_symbols[local_name] = (alias.name, "IMPORTED_SYMBOL")
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for alias in node.names:
                    local_name = alias.asname or alias.name
                    qualified_name = f"{module}.{alias.name}" if module else alias.name
                    # Distinguish between module attribute or direct symbol import
                    resolution = "IMPORTED_MODULE_ATTRIBUTE" if node.level > 0 else "IMPORTED_SYMBOL"
                    imported_symbols[local_name] = (qualified_name, resolution)

        # Helper to extract dotted function call names
        def get_call_name(node: ast.Call) -> str | None:
            if isinstance(node.func, ast.Name):
                return node.func.id
            elif isinstance(node.func, ast.Attribute):
                parts = []
                current: ast.AST | None = node.func
                while isinstance(current, ast.Attribute):
                    parts.append(current.attr)
                    current = current.value
                if isinstance(current, ast.Name):
                    parts.append(current.id)
                elif isinstance(current, ast.Call):
                    # For chained calls like foo().bar()
                    return None
                return ".".join(reversed(parts))
            return None

        # 2. Second Pass: Find calls and create CALLS edges
        # We need to trace the current containing function/method scope during traversal
        def traverse(node: ast.AST, current_caller_id: str) -> None:
            caller_id = current_caller_id
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                caller_id = node_id_mapping[id(node)].node_id

            if isinstance(node, ast.Call):
                call_site_id = node_id_mapping[id(node)].node_id
                call_text = get_call_name(node)

                if call_text:
                    target_id = None
                    resolution_type = "UNRESOLVED_DYNAMIC_CALL"
                    qualified_name = call_text

                    # Try to resolve target
                    # Class 1: Local function definition
                    if call_text in local_symbols:
                        target_id = local_symbols[call_text]
                        resolution_type = "LOCAL_FUNCTION"

                    # Class 2: Imported symbol
                    elif call_text in imported_symbols:
                        qualified_name, resolution_type = imported_symbols[call_text]
                    elif "." in call_text:
                        # Handles math.sqrt where math is imported
                        first_part = call_text.split(".")[0]
                        rest = ".".join(call_text.split(".")[1:])
                        if first_part in imported_symbols:
                            module_qname, _ = imported_symbols[first_part]
                            qualified_name = f"{module_qname}.{rest}"
                            resolution_type = "IMPORTED_MODULE_ATTRIBUTE"

                    # If not local, create ExternalSymbol node
                    if target_id is None:
                        # Generate deterministic ID for the external target
                        target_id = IdentifierGenerator.generate_node_id(
                            file_id=file_id,
                            node_type="ExternalSymbol",
                            qualified_scope="External",
                            semantic_key=qualified_name,
                            ast_path=f"External.{qualified_name}",
                        )
                        if target_id not in external_nodes:
                            external_nodes[target_id] = CodeNode(
                                node_id=target_id,
                                file_id=file_id,
                                node_type="ExternalSymbol",
                                ast_path=f"External.{qualified_name}",
                                name=call_text.split(".")[-1],
                                qualified_name=qualified_name,
                                line_start=None,
                                column_start=None,
                                line_end=None,
                                column_end=None,
                                properties={"resolution_type": resolution_type},
                            )

                    # Create CALLS edge from containing function/module node to target
                    role = f"call|{call_site_id}"
                    edge_id = IdentifierGenerator.generate_edge_id(caller_id, "CALLS", target_id, role)
                    call_edges.append(
                        CodeEdge(
                            edge_id=edge_id,
                            file_id=file_id,
                            source_id=caller_id,
                            target_id=target_id,
                            edge_type="CALLS",
                            properties={
                                "call_site_node_id": call_site_id,
                                "call_text": call_text,
                                "resolution_type": resolution_type,
                            },
                        )
                    )

            for child in ast.iter_child_nodes(node):
                traverse(child, caller_id)

        traverse(tree, module_node_id)
        return list(external_nodes.values()), call_edges
