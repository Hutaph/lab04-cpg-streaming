"""Builds AST nodes and parent-child edges from Python ast module objects."""

import ast
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class AstBuildResult:
    """AST build result wrapper."""

    nodes: list[dict] = field(default_factory=list)
    edges: list[dict] = field(default_factory=list)


class AstBuilder:
    """TODO: Implement tree traversal to extract AST nodes and parent-child relationships."""

    def build(self, tree: ast.AST, file_path: str, content_hash: str) -> AstBuildResult:
        """Translates Python AST structures to domain CPG nodes/edges."""
        raise NotImplementedError("AST builder will be implemented in Phase 2")
