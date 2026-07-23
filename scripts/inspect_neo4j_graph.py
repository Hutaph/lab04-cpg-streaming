import os
import sys
import subprocess
import json
import csv
import io
from pathlib import Path
from typing import Any


def load_env() -> dict[str, str]:
    env = {}
    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                env[key.strip()] = val.strip()
    for k, v in os.environ.items():
        env[k] = v
    return env


def run_cypher(query: str, password: str) -> list[list[str]]:
    """Runs a cypher query using cypher-shell --format csv via docker exec and parses it."""
    try:
        res = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "cpg-neo4j",
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
        )
        if res.returncode != 0:
            print(f"Cypher error: {res.stderr.strip()}", file=sys.stderr)
            return []

        reader = csv.reader(io.StringIO(res.stdout.strip()), skipinitialspace=True)
        return list(reader)
    except Exception as exc:
        print(f"Execution error: {exc}", file=sys.stderr)
        return []


def cast_value(val: str) -> Any:
    if val == "" or val.lower() == "null":
        return None
    if val.isdigit():
        return int(val)
    try:
        return float(val)
    except ValueError:
        return val


def main() -> None:
    env = load_env()
    password = env.get("NEO4J_PASSWORD", "CHANGE_ME_NEO4J_PASSWORD")

    # Define canonical queries
    queries = {
        "node_count_by_file": "MATCH (n:CPGNode) RETURN n.file_id AS file_id, count(n) AS node_count ORDER BY node_count DESC LIMIT 10;",
        "relationship_count_by_file": "MATCH (:CPGNode)-[r:CPG_EDGE]->(:CPGNode) RETURN r.file_id AS file_id, count(r) AS edge_count ORDER BY edge_count DESC LIMIT 10;",
        "duplicate_nodes": "MATCH (n:CPGNode) WITH n.id AS id, count(n) AS count WHERE count > 1 RETURN id, count;",
        "duplicate_edges": "MATCH ()-[r:CPG_EDGE]->() WITH r.edge_id AS id, count(r) AS count WHERE count > 1 RETURN id, count;",
        "placeholders": "MATCH (n:CPGNode {placeholder: true}) RETURN n.id AS node_id, n.file_id AS file_id LIMIT 10;",
        "null_file_id_nodes": "MATCH (n:CPGNode) WHERE n.file_id IS NULL RETURN n.id AS node_id;",
        "null_file_id_edges": "MATCH ()-[r:CPG_EDGE]->() WHERE r.file_id IS NULL RETURN r.edge_id AS edge_id;",
        "null_generation_id_entities": "MATCH (n) WHERE (n:CPGNode OR n:CPGNodeTombstone) AND n.generation_id IS NULL RETURN id(n) AS entity_id;",
        "tombstones": "MATCH (t:CPGNodeTombstone) RETURN t.id AS id, t.generation_id AS generation_id, t.file_id AS file_id LIMIT 10;",
        "edge_tombstones": "MATCH (t:CPGEdgeTombstone) RETURN t.id AS id, t.generation_id AS generation_id, t.file_id AS file_id LIMIT 10;",
        "duplicate_edge_tombstones": "MATCH (t:CPGEdgeTombstone) WITH t.id AS id, t.generation_id AS gen, count(t) AS c WHERE c > 1 RETURN id, gen, c;",
        "null_file_id_edge_tombstones": "MATCH (t:CPGEdgeTombstone) WHERE t.file_id IS NULL RETURN t.id AS id, t.generation_id AS generation_id;",
        "null_generation_id_edge_tombstones": "MATCH (t:CPGEdgeTombstone) WHERE t.generation_id IS NULL RETURN t.id AS id, t.file_id AS file_id;",
    }

    report = {}
    for title, cypher in queries.items():
        rows = run_cypher(cypher, password)
        if rows:
            headers = rows[0]
            records = []
            for row in rows[1:]:
                record = {}
                for h, val in zip(headers, row):
                    record[h] = cast_value(val)
                records.append(record)
            report[title] = records
        else:
            report[title] = []

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
