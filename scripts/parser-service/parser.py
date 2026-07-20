import argparse
from collections import defaultdict
from pathlib import Path

from cpg_parser import CpgParser
from event_writer import EventWriter


def selected_python_files(source_root: Path, limit: int | None) -> list[Path]:
    exclude_dirs = {".git", "__pycache__", ".pytest_cache"}
    exclude_files = {"setup.py", "conftest.py"}
    files = [
        file
        for file in source_root.rglob("*.py")
        if not any(part in exclude_dirs for part in file.parts)
        and file.name not in exclude_files
    ]
    return files[:limit] if limit else files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incremental Python CPG Parser Service")
    parser.add_argument("--repo", default="transformers-pr-agent", help="Path to the cloned repository")
    parser.add_argument("--source-root", default=None, help="Path to Python source root. Defaults to <repo>/src")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of files for a demo run")
    parser.add_argument("--dry-run", action="store_true", help="Write JSONL events locally instead of Kafka")
    parser.add_argument("--out-dir", default="parser-output", help="Output directory for dry-run JSONL files")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_path = Path(args.repo)
    source_root = Path(args.source_root) if args.source_root else repo_path / "src"
    parser = CpgParser(repo_path, source_root)
    writer = EventWriter(args.dry_run, Path(args.out_dir), args.bootstrap_servers)
    totals = defaultdict(int)

    for file_path in selected_python_files(source_root, args.limit):
        result = parser.parse_file(file_path)
        writer.write_many("nodes", result.nodes)
        writer.write_many("edges", result.edges)
        writer.write_many("metadata", result.metadata)
        writer.write_many("errors", result.errors)

        counts = result.counts()
        for key, value in counts.items():
            totals[key] += value
        print(f"parsed {file_path}: {counts}")

    writer.flush()
    print("total", dict(totals))


if __name__ == "__main__":
    main()
