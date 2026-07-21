"""Extracts metadata statistics from Python source code files using AST analysis."""

import ast
from domain.models import FileMetadata
from domain.enums import ParseStatus


class MetadataExtractor:
    """Traverses Python AST to count classes, functions, line statistics, and import symbols."""

    def extract(
        self,
        source_code: bytes,
        file_id: str,
        repository_id: str,
        file_path: str,
        content_hash: str,
        node_count: int,
        edge_count: int,
        parse_duration_ms: int,
        status: ParseStatus,
    ) -> FileMetadata:
        """Parses the text and returns a populated FileMetadata object."""
        size_bytes = len(source_code)

        # Calculate line count properly
        source_text = source_code.decode("utf-8", errors="replace")
        if not source_text:
            line_count = 0
        else:
            line_count = len(source_text.splitlines())

        function_count = 0
        class_count = 0
        import_count = 0

        try:
            tree = ast.parse(source_text)
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    function_count += 1
                elif isinstance(node, ast.ClassDef):
                    class_count += 1
                elif isinstance(node, (ast.Import, ast.ImportFrom)):
                    import_count += len(node.names)
        except SyntaxError:
            # If there was a syntax error, we calculate what we can
            pass

        return FileMetadata(
            file_id=file_id,
            repository_id=repository_id,
            file_path=file_path,
            content_hash=content_hash,
            size_bytes=size_bytes,
            line_count=line_count,
            function_count=function_count,
            class_count=class_count,
            import_count=import_count,
            node_count=node_count,
            edge_count=edge_count,
            parse_duration_ms=parse_duration_ms,
            parse_status=status,
            parser="python.ast",
        )
