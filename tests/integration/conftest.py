import sys
import pytest
from pathlib import Path
from confluent_kafka import Producer

# Add scripts directory to path
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.append(str(scripts_dir))

import deploy_connectors  # noqa: E402


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
