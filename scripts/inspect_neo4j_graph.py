import os
import sys
import subprocess
import json
from pathlib import Path


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
    """Runs a cypher query using cypher-shell via docker exec and parses plain tab-separated output."""
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

        lines = res.stdout.strip().splitlines()
        results = []
        for line in lines:
            if line:
                results.append(line.split("\t"))
        return results
    except Exception as exc:
        print(f"Execution error: {exc}", file=sys.stderr)
        return []


def main() -> None:
    env = load_env()
    password = env.get("NEO4J_PASSWORD", "CHANGE_ME_NEO4J_PASSWORD")

    # Define queries
    queries = {
        "node_count_by_file": "MATCH (n:CPGNode) RETURN n.file_id AS file_id, count(n) AS node_count ORDER BY node_count DESC LIMIT 10;",
        "relationship_count_by_file": "MATCH (:CPGNode)-[r:CPG_EDGE]->(:CPGNode) RETURN r.file_id AS file_id, count(r) AS edge_count ORDER BY edge_count DESC LIMIT 10;",
        "duplicate_nodes": "MATCH (n:CPGNode) WITH n.id AS id, count(n) AS count WHERE count > 1 RETURN id, count;",
        "duplicate_edges": "MATCH ()-[r:CPG_EDGE]->() WITH r.edge_id AS id, count(r) AS count WHERE count > 1 RETURN id, count;",
        "placeholders": "MATCH (n:CPGNode {placeholder: true}) RETURN n.id AS node_id, n.file_id AS file_id LIMIT 10;",
        "content_generations": "MATCH (n:CPGNode) RETURN DISTINCT n.file_id AS file_id, n.content_hash AS content_hash LIMIT 10;",
        "stale_entities": "MATCH (n:CPGNode) WITH n.file_id AS file_id, count(DISTINCT n.content_hash) AS hash_count WHERE hash_count > 1 RETURN file_id, hash_count;",
        "sample_relationships": "MATCH (src:CPGNode)-[r:CPG_EDGE]->(dst:CPGNode) RETURN src.id AS source, r.edge_type AS edge_type, dst.id AS target LIMIT 5;",
    }

    report = {}
    for title, cypher in queries.items():
        rows = run_cypher(cypher, password)
        # Separate headers and records if rows exist
        if rows:
            headers = rows[0]
            records = [dict(zip(headers, row)) for row in rows[1:]]
            report[title] = records
        else:
            report[title] = []

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
