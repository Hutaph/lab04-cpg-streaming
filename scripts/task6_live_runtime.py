# ruff: noqa: E402
"""Run the Task 6 live replay scenario and emit a runtime summary as JSON."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from application.services.process_file import ProcessFileService
from application.services.replay_file import ReplayFileService
from domain.enums import ParseStatus
from domain.models import SourceFile
from infrastructure.config.mongodb import build_mongodb_uri
from infrastructure.filesystem.git_source_repository import GitSourceRepository
from infrastructure.messaging.event_validator import EventValidator
from infrastructure.messaging.kafka_producer import KafkaEventProducer
from infrastructure.state.sqlite_state_store import SqliteStateStore
from infrastructure.verification.kafka_connect import (
    wait_for_zero_lag,
)
from parsing.cpg_parser import CpgParser


TARGET_FILE = Path(".github/scripts/assign_reviewers.py")
SOURCE_REPO = PROJECT_ROOT / "workspace" / "source" / "transformers-pr-agent"
CHECKPOINT_BASE = PROJECT_ROOT / "workspace" / "checkpoints" / "task6-live"
LIVE_BASE = PROJECT_ROOT / "workspace" / "tmp" / "task6-live"
KAFKA_CONNECTORS = ("connect-neo4j-nodes-sink", "connect-neo4j-edges-sink")
NEO4J_CONTAINER = "cpg-neo4j"
KAFKA_CONTAINER = "cpg-kafka"
MONGODB_CONTAINER_DEFAULT = "cpg-mongodb-metadata"


@dataclass(slots=True)
class SimpleResult:
    status: str
    file_id: str
    file_path: str
    content_hash: str
    node_count: int
    edge_count: int
    emitted_event_counts: dict[str, int]
    error: str | None = None


def load_env() -> dict[str, str]:
    """Load `.env` values into the current environment without printing secrets."""
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return dict(os.environ)


def ensure_stack() -> None:
    """Bring up the lab stack required for Task 6 live verification."""
    subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            ".env",
            "-f",
            "infra/docker-compose.yml",
            "-f",
            "infra/docker-compose.neo4j.yml",
            "-f",
            "infra/docker-compose.mongodb-metadata.yml",
            "up",
            "-d",
            "kafka",
            "neo4j",
            "kafka-connect",
            "mongodb-metadata",
        ],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def load_clean_env() -> None:
    """Reuse the same parsing contract as the notebooks."""
    load_env()


def run_cypher_query(query: str, password: str) -> list[list[str]]:
    """Run a Cypher query inside the Neo4j container and return CSV rows."""
    result = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            NEO4J_CONTAINER,
            "cypher-shell",
            "-u",
            "neo4j",
            "-p",
            password,
            "--format",
            "plain",
        ],
        input=query,
        capture_output=True,
        text=True,
        check=True,
    )
    reader = csv.reader(io.StringIO(result.stdout.strip()), skipinitialspace=True)
    rows: list[list[str]] = []
    for row in reader:
        if row:
            rows.append([value.lower() if value in ("TRUE", "FALSE") else value for value in row])
    return rows


def count_query(query: str, password: str) -> int:
    """Return a single integer count from a Cypher query."""
    rows = run_cypher_query(query, password)
    if len(rows) < 2:
        return 0
    return int(rows[1][0])


def snapshot_topic_offsets(topic: str) -> dict[int, int]:
    """Capture end offsets for all partitions in a Kafka topic."""
    result = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            KAFKA_CONTAINER,
            "kafka-run-class",
            "kafka.tools.GetOffsetShell",
            "--broker-list",
            "kafka:29092",
            "--topic",
            topic,
            "--time",
            "-1",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    offsets: dict[int, int] = {}
    for line in result.stdout.strip().splitlines():
        if not line:
            continue
        _, partition, offset = line.split(":")
        offsets[int(partition)] = int(offset)
    return offsets


def offset_delta(before: dict[int, int], after: dict[int, int]) -> int:
    """Calculate the delta between two offset snapshots."""
    delta = 0
    for partition in sorted(set(before) | set(after)):
        delta += max(0, after.get(partition, 0) - before.get(partition, 0))
    return delta


def count_topic_records_containing(
    topic: str,
    before: dict[int, int],
    after: dict[int, int],
    needle: str,
) -> int:
    """Count records in an offset delta window whose value contains a specific token."""
    total = 0
    for partition in sorted(set(before) | set(after)):
        start = before.get(partition, 0)
        end = after.get(partition, start)
        if end <= start:
            continue
        max_messages = end - start
        result = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                KAFKA_CONTAINER,
                "kafka-console-consumer",
                "--bootstrap-server",
                "kafka:29092",
                "--topic",
                topic,
                "--partition",
                str(partition),
                "--offset",
                str(start),
                "--max-messages",
                str(max_messages),
                "--timeout-ms",
                "10000",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        total += sum(1 for line in result.stdout.splitlines() if needle in line)
    return total


def run_spark(
    starting_offsets: dict[str, dict[str, int]] | None, checkpoint_dir: Path, mongodb_uri: str, env: dict[str, str]
) -> tuple[int, str, list[int]]:
    """Run the metadata Spark job and return exit code, logs and parsed input rows."""
    network = subprocess.check_output(
        [
            "docker",
            "inspect",
            "-f",
            "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}",
            KAFKA_CONTAINER,
        ],
        text=True,
    ).strip()
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.chmod(0o777)
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        network,
        "-v",
        f"{PROJECT_ROOT}:/opt/project",
        "-w",
        "/opt/project",
        "apache/spark-py:v3.3.0",
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "--conf",
        "spark.jars.ivy=/tmp/ivy",
        "--packages",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "spark_jobs/metadata_to_mongodb.py",
        "--bootstrap-servers",
        "kafka:29092",
        "--mongodb-uri",
        mongodb_uri,
        "--checkpoint-dir",
        checkpoint_dir.relative_to(PROJECT_ROOT).as_posix(),
        "--app-name",
        f"cpg-task6-{env['TASK6_NOTEBOOK_RUN_ID']}",
        "--available-now",
    ]
    if starting_offsets is not None:
        command[-1:-1] = ["--starting-offsets", json.dumps(starting_offsets)]
    result = subprocess.run(command, capture_output=True, text=True, timeout=600)
    logs = result.stdout + result.stderr
    logs = logs.replace(env["MONGO_ROOT_PASSWORD"], "***")
    logs = re.sub(r"mongodb://([^:]+):[^@]+@", r"mongodb://\\1:***@", logs)
    input_rows = [int(value) for value in re.findall(r'"numInputRows"\s*:\s*(\d+)', logs)]
    return result.returncode, logs, input_rows


def mongo_json(javascript: str, container_name: str, uri: str) -> dict[str, Any] | list[Any]:
    """Execute mongosh against MongoDB and parse the JSON output."""
    last_error = ""
    for _ in range(5):
        result = subprocess.run(
            [
                "docker",
                "exec",
                container_name,
                "mongosh",
                "--quiet",
                "--username",
                os.environ["MONGO_ROOT_USERNAME"],
                "--password",
                os.environ["MONGO_ROOT_PASSWORD"],
                "--authenticationDatabase",
                "admin",
                "--eval",
                javascript,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return json.loads(result.stdout.strip().splitlines()[-1])
        last_error = (result.stderr or result.stdout).strip()
        time.sleep(2.0)
    raise RuntimeError(f"Mongo query failed after retries: {last_error}")


def mongo_document_state(
    container_name: str, database: str, collection: str, file_id: str, repository_id: str, file_path: str
) -> dict[str, Any]:
    """Return document counts and indexed fields for the metadata collection."""
    javascript = (
        f"const dbh=db.getSiblingDB('{database}'); "
        f"const d=dbh.{collection}.findOne({{file_id:'{file_id}'}}); "
        f"const indexes=dbh.{collection}.getIndexes().map(i=>i.name); "
        "const asNumber=(v)=>v && typeof v.toNumber==='function' ? v.toNumber() : v; "
        f"print(JSON.stringify({{document_count:dbh.{collection}.countDocuments({{file_id:'{file_id}'}}), "
        f"repository_path_count:dbh.{collection}.countDocuments({{repository_id:'{repository_id}', file_path:'{file_path}'}}), "
        "content_hash:d ? d.content_hash : null, "
        "repository_id:d ? d.repository_id : null, "
        "file_path:d ? d.file_path : null, "
        "node_count:d ? asNumber(d.node_count) : null, "
        "edge_count:d ? asNumber(d.edge_count) : null, "
        "function_count:d ? asNumber(d.function_count) : null, "
        "class_count:d ? asNumber(d.class_count) : null, "
        "indexes:indexes}));"
    )
    return dict(mongo_json(javascript, container_name, ""))  # type: ignore[arg-type]


def build_source_file(repository_id: str, source_root: Path, target_file: Path, source_path: Path) -> SourceFile:
    """Create the SourceFile contract for the parser services."""
    return SourceFile(
        repository_id=repository_id,
        repository_root=str(source_root),
        relative_path=target_file.as_posix(),
        commit_sha=GitSourceRepository(source_root, "", None).get_commit_hash(),
        size_bytes=source_path.stat().st_size,
    )


def create_runtime_summary(run_id: str) -> dict[str, Any]:
    """Execute the Task 6 live scenario and return runtime evidence."""
    load_clean_env()
    ensure_stack()

    env = dict(os.environ)
    repo_id = f"notebook/task6/{run_id}"
    live_root = PROJECT_ROOT / "workspace" / "tmp" / "task6-live" / run_id
    source_root = live_root / "source"
    state_db = live_root / "state.sqlite3"
    checkpoint_dir = PROJECT_ROOT / "workspace" / "checkpoints" / "task6-live" / run_id
    isolated_repo = source_root

    if live_root.exists():
        raise RuntimeError(f"Live runtime directory already exists: {live_root}")
    live_root.mkdir(parents=True, exist_ok=False)
    CHECKPOINT_BASE.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(SOURCE_REPO),
            "worktree",
            "add",
            "--detach",
            str(isolated_repo),
            "HEAD",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    try:
        target_file = TARGET_FILE
        source_path = isolated_repo / target_file
        if not source_path.exists():
            raise FileNotFoundError(f"Target file does not exist in live worktree: {source_path}")

        initial_source_bytes = source_path.read_text(encoding="utf-8")
        modified_source = initial_source_bytes + (
            "\n\n\ndef task6_runtime_marker(value):\n    if value:\n        return value.strip()\n    return value\n"
        )

        validator = EventValidator(PROJECT_ROOT / "schemas")
        state_store = SqliteStateStore(state_db, repository_id=repo_id)
        producer = KafkaEventProducer(bootstrap_servers=env["KAFKA_BOOTSTRAP_SERVERS"])
        parser = CpgParser(repository_id=repo_id)
        process_service = ProcessFileService(
            GitSourceRepository(isolated_repo, "", None),
            parser,
            state_store,
            validator,
            producer,
            topic_nodes=env["TOPIC_NODES"],
            topic_edges=env["TOPIC_EDGES"],
            topic_metadata=env["TOPIC_METADATA"],
            topic_errors=env["TOPIC_ERRORS"],
        )
        replay_service = ReplayFileService(
            GitSourceRepository(isolated_repo, "", None),
            parser,
            state_store,
            process_service,
            repo_id,
        )

        baseline_topic_offsets = {
            topic: snapshot_topic_offsets(topic)
            for topic in ("cpg.nodes", "cpg.edges", "source.metadata", "parser.errors", "connector.errors")
        }

        source_file = build_source_file(repo_id, isolated_repo, target_file, source_path)
        baseline_result = process_service.execute(source_file)
        baseline_file_id = baseline_result.file_id
        baseline_content_hash = baseline_result.content_hash
        wait_for_zero_lag("connect-neo4j-nodes-sink", timeout=180)
        wait_for_zero_lag("connect-neo4j-edges-sink", timeout=180)

        after_baseline_topic_offsets = {
            topic: snapshot_topic_offsets(topic)
            for topic in ("cpg.nodes", "cpg.edges", "source.metadata", "parser.errors", "connector.errors")
        }
        baseline_neo4j_counts = {
            "nodes": count_query(
                f"MATCH (n:CPGNode {{file_id: '{baseline_file_id}'}}) RETURN count(n);",
                env["NEO4J_PASSWORD"],
            ),
            "edges": count_query(
                f"MATCH ()-[r:CPG_EDGE]->() WHERE r.file_id = '{baseline_file_id}' RETURN count(r);",
                env["NEO4J_PASSWORD"],
            ),
        }
        baseline_spark_exit, baseline_spark_logs, baseline_input_rows = run_spark(
            {"source.metadata": {"0": baseline_topic_offsets["source.metadata"].get(0, 0)}},
            checkpoint_dir,
            build_mongodb_uri(
                env["MONGO_ROOT_USERNAME"],
                env["MONGO_ROOT_PASSWORD"],
                env["MONGODB_CONTAINER_HOST"],
                int(env["MONGODB_CONTAINER_PORT"]),
            ),
            env,
        )
        if baseline_spark_exit != 0:
            raise RuntimeError(f"Spark baseline failed:\n{baseline_spark_logs[-4000:]}")
        baseline_mongo = mongo_document_state(
            env["MONGODB_CONTAINER_NAME"],
            env["MONGODB_DATABASE"],
            env["MONGODB_COLLECTION"],
            baseline_file_id,
            repo_id,
            target_file.as_posix(),
        )

        source_path.write_text(modified_source, encoding="utf-8")
        before_replay_topic_offsets = {
            topic: snapshot_topic_offsets(topic)
            for topic in ("cpg.nodes", "cpg.edges", "source.metadata", "parser.errors", "connector.errors")
        }
        replay_result = replay_service.execute(target_file)
        after_replay_topic_offsets = {
            topic: snapshot_topic_offsets(topic)
            for topic in ("cpg.nodes", "cpg.edges", "source.metadata", "parser.errors", "connector.errors")
        }
        wait_for_zero_lag("connect-neo4j-nodes-sink", timeout=180)
        wait_for_zero_lag("connect-neo4j-edges-sink", timeout=180)

        neo4j_integrity = {
            "baseline_nodes": baseline_neo4j_counts["nodes"],
            "baseline_edges": baseline_neo4j_counts["edges"],
            "replay_nodes": count_query(
                f"MATCH (n:CPGNode {{file_id: '{baseline_file_id}'}}) RETURN count(n);",
                env["NEO4J_PASSWORD"],
            ),
            "replay_edges": count_query(
                f"MATCH ()-[r:CPG_EDGE]->() WHERE r.file_id = '{baseline_file_id}' RETURN count(r);",
                env["NEO4J_PASSWORD"],
            ),
            "duplicate_nodes": count_query(
                "MATCH (n:CPGNode) WITH n.id AS id, count(n) AS count WHERE count > 1 RETURN count(id);",
                env["NEO4J_PASSWORD"],
            ),
            "duplicate_edges": count_query(
                "MATCH ()-[r:CPG_EDGE]->() WITH r.edge_id AS id, count(r) AS count WHERE count > 1 RETURN count(id);",
                env["NEO4J_PASSWORD"],
            ),
            "placeholders": count_query(
                f"MATCH (n:CPGNode {{file_id: '{baseline_file_id}', placeholder: true}}) RETURN count(n);",
                env["NEO4J_PASSWORD"],
            ),
            "duplicate_node_tombstones": count_query(
                "MATCH (t:CPGNodeTombstone) WITH t.id AS id, t.generation_id AS gen, count(t) AS c WHERE c > 1 RETURN count(id);",
                env["NEO4J_PASSWORD"],
            ),
            "duplicate_edge_tombstones": count_query(
                "MATCH (t:CPGEdgeTombstone) WITH t.id AS id, t.generation_id AS gen, count(t) AS c WHERE c > 1 RETURN count(id);",
                env["NEO4J_PASSWORD"],
            ),
            "malformed_tombstones": count_query(
                "MATCH (t:CPGNodeTombstone) WHERE t.file_id IS NULL OR t.generation_id IS NULL RETURN count(t);",
                env["NEO4J_PASSWORD"],
            )
            + count_query(
                "MATCH (t:CPGEdgeTombstone) WHERE t.file_id IS NULL OR t.generation_id IS NULL RETURN count(t);",
                env["NEO4J_PASSWORD"],
            ),
        }

        replay_spark_exit, replay_spark_logs, replay_input_rows = run_spark(
            None,
            checkpoint_dir,
            build_mongodb_uri(
                env["MONGO_ROOT_USERNAME"],
                env["MONGO_ROOT_PASSWORD"],
                env["MONGODB_CONTAINER_HOST"],
                int(env["MONGODB_CONTAINER_PORT"]),
            ),
            env,
        )
        if replay_spark_exit != 0:
            raise RuntimeError(f"Spark replay failed:\n{replay_spark_logs[-4000:]}")
        mongo_after_replay = mongo_document_state(
            env["MONGODB_CONTAINER_NAME"],
            env["MONGODB_DATABASE"],
            env["MONGODB_COLLECTION"],
            baseline_file_id,
            repo_id,
            target_file.as_posix(),
        )

        unchanged_before_offsets = {
            topic: snapshot_topic_offsets(topic)
            for topic in ("cpg.nodes", "cpg.edges", "source.metadata", "parser.errors", "connector.errors")
        }
        unchanged_result = process_service.execute(build_source_file(repo_id, isolated_repo, target_file, source_path))
        unchanged_after_offsets = {
            topic: snapshot_topic_offsets(topic)
            for topic in ("cpg.nodes", "cpg.edges", "source.metadata", "parser.errors", "connector.errors")
        }
        if unchanged_result.status != ParseStatus.SKIPPED_UNCHANGED:
            raise RuntimeError(f"Expected SKIPPED_UNCHANGED, got {unchanged_result.status.value}")
        unchanged_spark_exit, unchanged_spark_logs, unchanged_input_rows = run_spark(
            None,
            checkpoint_dir,
            build_mongodb_uri(
                env["MONGO_ROOT_USERNAME"],
                env["MONGO_ROOT_PASSWORD"],
                env["MONGODB_CONTAINER_HOST"],
                int(env["MONGODB_CONTAINER_PORT"]),
            ),
            env,
        )
        if unchanged_spark_exit != 0:
            raise RuntimeError(f"Spark unchanged rerun failed:\n{unchanged_spark_logs[-4000:]}")

        baseline_deltas = {
            topic: offset_delta(baseline_topic_offsets[topic], after_baseline_topic_offsets[topic])
            for topic in ("cpg.nodes", "cpg.edges", "source.metadata", "parser.errors", "connector.errors")
        }
        replay_deltas = {
            topic: offset_delta(before_replay_topic_offsets[topic], after_replay_topic_offsets[topic])
            for topic in ("cpg.nodes", "cpg.edges", "source.metadata", "parser.errors", "connector.errors")
        }
        unchanged_deltas = {
            topic: offset_delta(unchanged_before_offsets[topic], unchanged_after_offsets[topic])
            for topic in ("cpg.nodes", "cpg.edges", "source.metadata", "parser.errors", "connector.errors")
        }
        baseline_deltas["connector.errors_raw"] = baseline_deltas["connector.errors"]
        replay_deltas["connector.errors_raw"] = replay_deltas["connector.errors"]
        unchanged_deltas["connector.errors_raw"] = unchanged_deltas["connector.errors"]
        baseline_deltas["connector.errors"] = count_topic_records_containing(
            "connector.errors",
            baseline_topic_offsets["connector.errors"],
            after_baseline_topic_offsets["connector.errors"],
            baseline_file_id,
        )
        replay_deltas["connector.errors"] = count_topic_records_containing(
            "connector.errors",
            before_replay_topic_offsets["connector.errors"],
            after_replay_topic_offsets["connector.errors"],
            baseline_file_id,
        )
        unchanged_deltas["connector.errors"] = count_topic_records_containing(
            "connector.errors",
            unchanged_before_offsets["connector.errors"],
            unchanged_after_offsets["connector.errors"],
            baseline_file_id,
        )

        return {
            "run_id": run_id,
            "repository_id": repo_id,
            "target_file": target_file.as_posix(),
            "baseline_parser_result": {
                "status": baseline_result.status.value,
                "file_id": baseline_result.file_id,
                "file_path": baseline_result.file_path,
                "content_hash": baseline_result.content_hash,
                "node_count": baseline_result.node_count,
                "edge_count": baseline_result.edge_count,
                "emitted_event_counts": baseline_result.emitted_event_counts,
            },
            "replay_result": replay_result,
            "unchanged_result": {
                "status": unchanged_result.status.value,
                "file_id": unchanged_result.file_id,
                "file_path": unchanged_result.file_path,
                "content_hash": unchanged_result.content_hash,
                "node_count": unchanged_result.node_count,
                "edge_count": unchanged_result.edge_count,
                "emitted_event_counts": unchanged_result.emitted_event_counts,
            },
            "kafka_offset_deltas": {
                "baseline": baseline_deltas,
                "replay": replay_deltas,
                "unchanged": unchanged_deltas,
            },
            "neo4j_integrity": neo4j_integrity,
            "spark_progress": {
                "baseline": {
                    "exit_code": baseline_spark_exit,
                    "numInputRows": baseline_input_rows[-1] if baseline_input_rows else 0,
                },
                "replay": {
                    "exit_code": replay_spark_exit,
                    "numInputRows": replay_input_rows[-1] if replay_input_rows else 0,
                },
                "unchanged": {
                    "exit_code": unchanged_spark_exit,
                    "numInputRows": unchanged_input_rows[-1] if unchanged_input_rows else 0,
                },
            },
            "mongo_document_state": {
                "baseline": baseline_mongo,
                "replay": mongo_after_replay,
            },
            "hash_chain": {
                "baseline_content_hash": baseline_content_hash,
                "replay_old_content_hash": replay_result["old_content_hash"],
                "replay_new_content_hash": replay_result["new_content_hash"],
            },
        }
    finally:
        subprocess.run(
            ["git", "-C", str(SOURCE_REPO), "worktree", "remove", "--force", str(isolated_repo)],
            capture_output=True,
            text=True,
            check=False,
        )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=os.environ.get("TASK6_NOTEBOOK_RUN_ID") or f"replay-{uuid.uuid4().hex[:8]}")
    return parser


def main() -> None:
    args = build_argument_parser().parse_args()
    summary = create_runtime_summary(args.run_id)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
