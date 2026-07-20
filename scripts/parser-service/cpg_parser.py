import ast
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from stable_id import file_sha256, stable_id


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_commit(repo_path: Path) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repo_path), "rev-parse", "HEAD"],
        text=True,
    ).strip()


def call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        parts = []
        current: ast.AST | None = node.func
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            parts.append(current.id)
        return ".".join(reversed(parts))
    return None


def iter_child_nodes_with_field(node: ast.AST) -> Iterator[tuple[str, int | None, ast.AST]]:
    for field_name, value in ast.iter_fields(node):
        if isinstance(value, ast.AST):
            yield field_name, None, value
        elif isinstance(value, list):
            for index, item in enumerate(value):
                if isinstance(item, ast.AST):
                    yield field_name, index, item


@dataclass
class ParseResult:
    nodes: list[dict[str, Any]] = field(default_factory=list)
    edges: list[dict[str, Any]] = field(default_factory=list)
    metadata: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        return {
            "nodes": len(self.nodes),
            "edges": len(self.edges),
            "metadata": len(self.metadata),
            "errors": len(self.errors),
        }


class CpgParser:
    def __init__(self, repo_path: Path, source_root: Path):
        self.repo_path = repo_path.resolve()
        self.source_root = source_root.resolve()
        self.commit_hash = read_commit(self.repo_path)

    def parse_file(self, file_path: Path) -> ParseResult:
        file_path = file_path.resolve()
        rel_path = file_path.relative_to(self.repo_path).as_posix()
        source = file_path.read_text(encoding="utf-8", errors="replace")
        content_hash = file_sha256(source)
        result = ParseResult()

        try:
            tree = ast.parse(source, filename=rel_path)
        except SyntaxError as exc:
            result.errors.append(
                self._base_event(
                    "parser_error",
                    rel_path,
                    content_hash,
                    {
                        "id": stable_id("error", rel_path, content_hash),
                        "error_type": type(exc).__name__,
                        "message": str(exc),
                        "line": exc.lineno,
                        "column": exc.offset,
                    },
                )
            )
            return result

        result.metadata.append(
            self._base_event(
                "source_metadata",
                rel_path,
                content_hash,
                {
                    "id": stable_id("metadata", rel_path),
                    "size_bytes": len(source.encode("utf-8")),
                    "line_count": source.count("\n") + 1,
                    "parser": "python.ast",
                },
            )
        )

        node_ids: dict[int, str] = {}
        self._collect_ast(tree, rel_path, content_hash, result, node_ids)
        self._collect_cfg(tree, rel_path, content_hash, result, node_ids)
        self._collect_dfg(tree, rel_path, content_hash, result, node_ids)
        self._collect_calls(tree, rel_path, content_hash, result, node_ids)
        return result

    def _base_event(
        self,
        event_type: str,
        rel_path: str,
        content_hash: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "event_type": event_type,
            "timestamp": utc_now(),
            "repo": self.repo_path.name,
            "commit_hash": self.commit_hash,
            "file_path": rel_path,
            "file_hash": content_hash,
            **payload,
        }

    def _node_event(
        self,
        rel_path: str,
        content_hash: str,
        node: ast.AST,
        node_id: str,
        ast_path: str,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "id": node_id,
            "label": type(node).__name__,
            "ast_path": ast_path,
            "line": getattr(node, "lineno", None),
            "column": getattr(node, "col_offset", None),
            "end_line": getattr(node, "end_lineno", None),
            "end_column": getattr(node, "end_col_offset", None),
        }

        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            payload["name"] = node.name
        elif isinstance(node, ast.Name):
            payload["name"] = node.id
            payload["context"] = type(node.ctx).__name__
        elif isinstance(node, ast.arg):
            payload["name"] = node.arg
        elif isinstance(node, ast.Call):
            payload["call_name"] = call_name(node)
        elif isinstance(node, ast.Constant):
            payload["value_type"] = type(node.value).__name__

        return self._base_event("cpg_node", rel_path, content_hash, payload)

    def _edge_event(
        self,
        rel_path: str,
        content_hash: str,
        source_id: str,
        target_id: str,
        edge_type: str,
        **payload: Any,
    ) -> dict[str, Any]:
        edge_id = stable_id(edge_type, source_id, target_id, payload.get("field"), payload.get("index"))
        return self._base_event(
            "cpg_edge",
            rel_path,
            content_hash,
            {
                "id": edge_id,
                "source": source_id,
                "target": target_id,
                "edge_type": edge_type,
                **payload,
            },
        )

    def _collect_ast(
        self,
        tree: ast.AST,
        rel_path: str,
        content_hash: str,
        result: ParseResult,
        node_ids: dict[int, str],
    ) -> None:
        def visit(
            node: ast.AST,
            ast_path: str,
            parent_id: str | None = None,
            field_name: str | None = None,
            index: int | None = None,
        ) -> None:
            node_id = stable_id(rel_path, content_hash, ast_path, type(node).__name__)
            node_ids[id(node)] = node_id
            result.nodes.append(self._node_event(rel_path, content_hash, node, node_id, ast_path))

            if parent_id is not None:
                result.edges.append(
                    self._edge_event(
                        rel_path,
                        content_hash,
                        parent_id,
                        node_id,
                        "AST_CHILD",
                        field=field_name,
                        index=index,
                    )
                )

            for child_number, (child_field, child_index, child) in enumerate(iter_child_nodes_with_field(node)):
                visit(child, f"{ast_path}.{child_number}", node_id, child_field, child_index)

        visit(tree, "0")

    def _collect_cfg(
        self,
        tree: ast.AST,
        rel_path: str,
        content_hash: str,
        result: ParseResult,
        node_ids: dict[int, str],
    ) -> None:
        for node in ast.walk(tree):
            body = getattr(node, "body", None)
            if not isinstance(body, list):
                continue
            statements = [item for item in body if isinstance(item, ast.stmt)]
            for source_stmt, target_stmt in zip(statements, statements[1:]):
                result.edges.append(
                    self._edge_event(
                        rel_path,
                        content_hash,
                        node_ids[id(source_stmt)],
                        node_ids[id(target_stmt)],
                        "CFG_NEXT",
                    )
                )

    def _collect_dfg(
        self,
        tree: ast.AST,
        rel_path: str,
        content_hash: str,
        result: ParseResult,
        node_ids: dict[int, str],
    ) -> None:
        last_store_by_name: dict[str, str] = {}
        nodes_by_line = sorted(
            ast.walk(tree),
            key=lambda item: (getattr(item, "lineno", -1), getattr(item, "col_offset", -1)),
        )

        for node in nodes_by_line:
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
                last_store_by_name[node.id] = node_ids[id(node)]
            elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                source_id = last_store_by_name.get(node.id)
                if source_id is not None:
                    result.edges.append(
                        self._edge_event(
                            rel_path,
                            content_hash,
                            source_id,
                            node_ids[id(node)],
                            "DFG_REACHES",
                            variable=node.id,
                        )
                    )

    def _collect_calls(
        self,
        tree: ast.AST,
        rel_path: str,
        content_hash: str,
        result: ParseResult,
        node_ids: dict[int, str],
    ) -> None:
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = call_name(node)
            if not name:
                continue
            call_target_id = stable_id("call_target", name)
            result.nodes.append(
                self._base_event(
                    "cpg_node",
                    rel_path,
                    content_hash,
                    {
                        "id": call_target_id,
                        "label": "CallTarget",
                        "name": name,
                        "line": None,
                        "column": None,
                    },
                )
            )
            result.edges.append(
                self._edge_event(
                    rel_path,
                    content_hash,
                    node_ids[id(node)],
                    call_target_id,
                    "CALLS",
                    call_name=name,
                )
            )
