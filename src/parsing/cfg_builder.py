"""Builds statement-level control-flow edges for Python scopes."""

import ast
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class CfgBuildResult:
    """Placeholder result contract for the future CFG builder."""

    edges: list[dict] = field(default_factory=list)


class CfgBuilder:
    """TODO: Implement statement-level control flow graph construction."""

    def build(self, tree: ast.AST, node_ids: dict[int, str]) -> CfgBuildResult:
        """Finds control flow paths and returns CFG_NEXT edges."""
        raise NotImplementedError("CFG builder will be implemented in Phase 3")
