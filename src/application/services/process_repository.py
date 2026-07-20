"""Service to run incremental parse across the entire source repository."""

import time
from typing import Any
from src.application.services.discover_repository import DiscoverRepositoryService
from src.application.services.process_file import ProcessFileService
from src.domain.enums import ParseStatus
from src.infrastructure.observability.logging import setup_logger

logger = setup_logger("process_repository")


class ProcessRepositoryService:
    """Orchestrates discover, filter, and parsing of all files in a repository loop."""

    def __init__(
        self,
        discover_service: DiscoverRepositoryService,
        process_file_service: ProcessFileService,
    ):
        self.discover_service = discover_service
        self.process_file_service = process_file_service

    def execute(
        self,
        limit: int | None = None,
        fail_fast: bool = False,
    ) -> dict[str, Any]:
        """Runs repository scan and incremental parser execution loop."""
        start_time = time.perf_counter()

        logger.info("Discovering files in target repository...")
        source_files = self.discover_service.execute()

        total_discovered = len(source_files)
        # Limit files if smoke scope or parameter limit set
        if limit is not None and limit > 0:
            logger.info(f"Limiting execution run to {limit} files.")
            source_files = source_files[:limit]

        eligible_count = len(source_files)

        processed = 0
        skipped = 0
        failed = 0
        node_events = 0
        edge_events = 0
        metadata_events = 0
        error_events = 0

        logger.info(f"Starting parsing loop for {eligible_count} eligible files...")
        
        for sf in source_files:
            file_path = sf.relative_path
            try:
                res = self.process_file_service.execute(sf)
                if res.status == ParseStatus.SKIPPED_UNCHANGED:
                    skipped += 1
                    logger.info(f"Skipped (unchanged): {file_path}")
                elif res.status == ParseStatus.SUCCESS:
                    processed += 1
                    node_events += res.emitted_event_counts.get(self.process_file_service.topic_nodes, 0)
                    edge_events += res.emitted_event_counts.get(self.process_file_service.topic_edges, 0)
                    metadata_events += res.emitted_event_counts.get(self.process_file_service.topic_metadata, 0)
                    logger.info(
                        f"Parsed successfully: {file_path} "
                        f"(nodes={res.node_count}, edges={res.edge_count})"
                    )
                else:
                    failed += 1
                    error_events += 1
                    logger.error(f"Failed parsing file: {file_path}. Error: {res.error}")
                    if fail_fast:
                        logger.error("Fail-fast triggered. Aborting loop.")
                        break

            except Exception as exc:
                failed += 1
                error_events += 1
                logger.exception(f"Unhandled exception while processing {file_path}: {exc}")
                if fail_fast:
                    logger.error("Fail-fast triggered. Aborting loop.")
                    raise exc

        duration_ms = int((time.perf_counter() - start_time) * 1000)

        summary = {
            "discovered": total_discovered,
            "eligible": eligible_count,
            "processed": processed,
            "skipped_unchanged": skipped,
            "failed": failed,
            "node_events": node_events,
            "edge_events": edge_events,
            "metadata_events": metadata_events,
            "error_events": error_events,
            "duration_ms": duration_ms,
        }

        logger.info(f"Run completed in {duration_ms} ms. Summary: {summary}")
        return summary
