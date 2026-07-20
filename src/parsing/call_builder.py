"""Resolves call targets and builds call-graph connections (CALLS edges)."""

import ast
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class CallBuildResult:
    """Placeholder result contract for the future call graph builder."""

    nodes: list[dict] = field(default_factory=list)
    edges: list[dict] = field(default_factory=list)


class CallBuilder:
    """TODO: Implement identifier-based call resolution."""

    def build(self, tree: ast.AST, node_ids: dict[int, str]) -> CallBuildResult:
        """Finds function call expressions and returns CALLS edges linked to stable targets."""
        raise NotImplementedError("Call builder will be implemented in Phase 5")
