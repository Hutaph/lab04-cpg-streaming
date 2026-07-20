"""Domain-level enumerations for Code Property Graph (CPG) streaming."""

from enum import Enum


class NodeType(str, Enum):
    """Placeholder enumeration for CPG Node Types."""

    AST_NODE = "AstNode"
    CALL_TARGET = "CallTarget"
    ERROR = "Error"
    # TODO: Define specific AST node types (e.g., FunctionDef, Name, etc.) in Phase 2.


class EdgeType(str, Enum):
    """Placeholder enumeration for CPG Edge Types."""

    AST_CHILD = "AST_CHILD"
    CFG_NEXT = "CFG_NEXT"
    DFG_REACHES = "DFG_REACHES"
    CALLS = "CALLS"


class ParseStatus(str, Enum):
    """Status of a file parsing action."""

    SUCCESS = "success"
    FAILED = "failed"
