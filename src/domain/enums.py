"""Domain-level enumerations for Code Property Graph (CPG) streaming."""

from enum import Enum


class EventType(str, Enum):
    """Kafka CPG Event Types."""

    NODE_UPSERT = "NODE_UPSERT"
    NODE_DELETE = "NODE_DELETE"
    EDGE_UPSERT = "EDGE_UPSERT"
    EDGE_DELETE = "EDGE_DELETE"
    FILE_METADATA_UPSERT = "FILE_METADATA_UPSERT"
    PARSER_ERROR = "PARSER_ERROR"


class EdgeType(str, Enum):
    """Official CPG Edge Types."""

    AST_CHILD = "AST_CHILD"
    CFG_NEXT = "CFG_NEXT"
    CFG_TRUE = "CFG_TRUE"
    CFG_FALSE = "CFG_FALSE"
    CFG_LOOP_BODY = "CFG_LOOP_BODY"
    CFG_LOOP_BACK = "CFG_LOOP_BACK"
    CFG_LOOP_EXIT = "CFG_LOOP_EXIT"
    CFG_RETURN = "CFG_RETURN"
    DFG_DEF_USE = "DFG_DEF_USE"
    CALLS = "CALLS"


class ParseStatus(str, Enum):
    """Status of a file parsing action."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED_UNCHANGED = "SKIPPED_UNCHANGED"
