"""Coordinates AST, CFG, DFG, and Call builders to compile a unified CPG graph."""

import ast
import time
from pathlib import Path
from domain.models import ParsedFileGraph, SourceFile
from domain.enums import ParseStatus
from domain.errors import ParsingError
from parsing.ast_builder import AstBuilder
from parsing.cfg_builder import CfgBuilder
from parsing.dfg_builder import DfgBuilder
from parsing.call_builder import CallBuilder
from parsing.metadata import MetadataExtractor
from parsing.identifiers import IdentifierGenerator


class CpgParser:
    """Orchestrates CPG subgraph builders for a single file source code."""

    def __init__(self, repository_id: str):
        self.repository_id = repository_id
        self.ast_builder = AstBuilder()
        self.cfg_builder = CfgBuilder()
        self.dfg_builder = DfgBuilder()
        self.call_builder = CallBuilder()
        self.metadata_extractor = MetadataExtractor()

    def parse_file(self, relative_path: Path, source_code: bytes, commit_sha: str) -> ParsedFileGraph:
        """Parses source text into a CpgGraph.

        Raises ParsingError if a SyntaxError occurs.
        """
        source_text = source_code.decode("utf-8", errors="replace")
        file_id = IdentifierGenerator.generate_file_id(self.repository_id, relative_path)
        content_hash = IdentifierGenerator.generate_content_hash(source_code)

        start_time = time.perf_counter()

        try:
            tree = ast.parse(source_text, filename=str(relative_path))
        except SyntaxError as exc:
            raise ParsingError(
                f"SyntaxError in {relative_path}: {exc.msg} at line {exc.lineno}, col {exc.offset}"
            ) from exc

        # 1. AST Construction
        ast_nodes, ast_edges, node_id_mapping = self.ast_builder.build(tree, file_id)

        # 2. CFG Construction (appends synthetic nodes to list)
        synthetic_nodes, cfg_edges = self.cfg_builder.build(tree, file_id, node_id_mapping)
        all_nodes = ast_nodes + synthetic_nodes

        # 3. DFG Construction
        dfg_edges = self.dfg_builder.build(tree, file_id, node_id_mapping)

        # 4. Call Graph Construction
        external_nodes, call_edges = self.call_builder.build(tree, file_id, node_id_mapping)
        all_nodes += external_nodes

        # Merge nodes and edges
        all_edges = ast_edges + cfg_edges + dfg_edges + call_edges

        # Deduplicate nodes by node_id (keeping first match)
        unique_nodes = {}
        for node in all_nodes:
            if node.node_id not in unique_nodes:
                unique_nodes[node.node_id] = node

        # Deduplicate edges by edge_id
        unique_edges = {}
        for edge in all_edges:
            if edge.edge_id not in unique_edges:
                unique_edges[edge.edge_id] = edge

        duration_ms = int((time.perf_counter() - start_time) * 1000)

        # 5. Extract Metadata
        metadata = self.metadata_extractor.extract(
            source_code=source_code,
            file_id=file_id,
            repository_id=self.repository_id,
            file_path=str(relative_path),
            content_hash=content_hash,
            node_count=len(unique_nodes),
            edge_count=len(unique_edges),
            parse_duration_ms=duration_ms,
            status=ParseStatus.SUCCESS,
        )

        source_file = SourceFile(
            repository_id=self.repository_id,
            repository_root="",
            relative_path=str(relative_path),
            commit_sha=commit_sha,
            size_bytes=len(source_code),
        )

        return ParsedFileGraph(
            source_file=source_file,
            file_id=file_id,
            content_hash=content_hash,
            nodes=list(unique_nodes.values()),
            edges=list(unique_edges.values()),
            metadata=metadata,
        )
