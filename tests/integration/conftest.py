import csv
import subprocess
import sys
import pytest
from pathlib import Path
from confluent_kafka import Producer

# Add scripts directory to path
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.append(str(scripts_dir))

import deploy_connectors  # noqa: E402


def run_cypher_query(query: str, password: str) -> list[list[str]]:
    """Shared helper: run a Cypher query inside cpg-neo4j container and return parsed rows."""
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
            check=True,
        )
        lines = res.stdout.strip().splitlines()
        results = []
        reader = csv.reader(lines, skipinitialspace=True)
        for row in reader:
            if row:
                normalized_row = [val.lower() if val in ("TRUE", "FALSE") else val for val in row]
                results.append(normalized_row)
        return results
    except Exception as exc:
        print(f"Failed to run cypher: {exc}", file=sys.stderr)
        return []


@pytest.fixture(scope="module")
def env_vars() -> dict[str, str]:
    return deploy_connectors.load_env()


@pytest.fixture(scope="module")
def neo4j_password(env_vars: dict[str, str]) -> str:
    return env_vars.get("NEO4J_PASSWORD", "CHANGE_ME_NEO4J_PASSWORD")


@pytest.fixture(scope="module")
def kafka_producer(env_vars: dict[str, str]) -> Producer:
    bootstrap_servers = env_vars.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",
        "retries": 3,
    }
    return Producer(conf)
