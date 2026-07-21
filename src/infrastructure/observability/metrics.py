"""Tracks parser throughput, file processing counts, and error rates."""

from dataclasses import dataclass, field
from time import perf_counter


@dataclass
class ParseMetrics:
    """Stores performance and throughput statistics of a parser session."""

    total_files: int = 0
    successful_files: int = 0
    failed_files: int = 0
    total_nodes: int = 0
    total_edges: int = 0
    start_time: float = field(default_factory=perf_counter)

    @property
    def duration(self) -> float:
        """Calculates total elapsed time since start."""
        return perf_counter() - self.start_time
