import os
import sys
import json
import time
import subprocess
import pytest
from confluent_kafka import Consumer, Producer
from pathlib import Path

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


def run_cypher_query(query: str, password: str) -> list[list[str]]:
    """Runs a Cypher query inside cpg-neo4j container and returns split tab-separated lines."""
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
        import csv

        results = []
        reader = csv.reader(lines, skipinitialspace=True)
        for row in reader:
            if row:
                # Normalize boolean strings to match assertions (e.g. FALSE -> false)
                normalized_row = [val.lower() if val in ("TRUE", "FALSE") else val for val in row]
                results.append(normalized_row)
        return results
    except Exception as exc:
        print(f"Failed to run cypher: {exc}", file=sys.stderr)
        return []


@pytest.mark.neo4j
@pytest.mark.kafka_connect
@pytest.mark.kafka
def test_infrastructure_health_and_plugin(env_vars: dict[str, str]):
    """Verify Neo4j, Kafka Connect and Neo4j Sink Plugin health."""
    # 1. Neo4j connection check
    password = env_vars.get("NEO4J_PASSWORD", "CHANGE_ME_NEO4J_PASSWORD")
    res = run_cypher_query("RETURN 1;", password)
    assert len(res) > 1
    assert res[1][0] == "1"

    # 2. Connector API status check
    code, info = deploy_connectors.make_request(f"{deploy_connectors.CONNECT_URL}/")
    assert code == 200
    assert "version" in info

    # 3. Neo4j connector plugin loaded check
    code, plugins = deploy_connectors.make_request(f"{deploy_connectors.CONNECT_URL}/connector-plugins")
    assert code == 200
    plugin_classes = [p.get("class") for p in plugins]
    assert "streams.kafka.connect.sink.Neo4jSinkConnector" in plugin_classes


@pytest.mark.neo4j
def test_schema_bootstrap_twice(env_vars: dict[str, str]):
    """Verify that create_neo4j_schema script is idempotent and can run twice without error."""
    password = env_vars.get("NEO4J_PASSWORD", "CHANGE_ME_NEO4J_PASSWORD")

    # First Run
    res1 = subprocess.run([sys.executable, "scripts/create_neo4j_schema.py"], capture_output=True, text=True)
    assert res1.returncode == 0, f"schema setup run 1 failed:\nSTDERR:\n{res1.stderr}\nSTDOUT:\n{res1.stdout}"

    # Second Run
    res2 = subprocess.run([sys.executable, "scripts/create_neo4j_schema.py"], capture_output=True, text=True)
    assert res2.returncode == 0, f"schema setup run 2 failed:\nSTDERR:\n{res2.stderr}\nSTDOUT:\n{res2.stdout}"

    # Verify constraint exists
    checks = run_cypher_query("SHOW CONSTRAINTS;", password)
    assert any("cpg_node_id_unique" in col for row in checks for col in row)


@pytest.mark.kafka_connect
def test_connector_deployment_twice():
    """Verify that deploy_connectors script is idempotent, handles drift, and can run twice without creating duplicate connectors."""
    # First Run
    res1 = subprocess.run([sys.executable, "scripts/deploy_connectors.py"], capture_output=True, text=True)
    assert res1.returncode == 0, f"connector deploy run 1 failed:\nSTDERR:\n{res1.stderr}\nSTDOUT:\n{res1.stdout}"

    # Second Run (should run with 'No changes made' or 'matches target')
    res2 = subprocess.run([sys.executable, "scripts/deploy_connectors.py"], capture_output=True, text=True)
    assert res2.returncode == 0, f"connector deploy run 2 failed:\nSTDERR:\n{res2.stderr}\nSTDOUT:\n{res2.stdout}"
    assert "matches target" in res2.stdout or "configuration is up-to-date" in res2.stdout


@pytest.mark.neo4j
@pytest.mark.kafka
def test_node_ingestion_scenarios(kafka_producer: Producer, neo4j_password: str):
    """Verify node upsert, exact replay, update, delete, and delete replay scenarios."""
    node_topic = "cpg.nodes"
    file_id = "test_file_id_node_ingest"
    node_id = "test_node_id_1"

    # Prepare events
    upsert_evt = {
        "schema_version": "1.0",
        "event_id": "evt_n_up_1",
        "event_type": "NODE_UPSERT",
        "event_time": "2026-07-22T10:00:00Z",
        "repository_id": "test_repo",
        "commit_sha": "sha_1",
        "file_id": file_id,
        "file_path": "a.py",
        "content_hash": "hash_version_1",
        "parser_version": "1.0.0",
        "node": {
            "node_id": node_id,
            "node_type": "Assign",
            "name": "x",
            "qualified_name": "x",
            "ast_path": "Module.body[0]",
            "line_start": 1,
            "column_start": 0,
            "line_end": 1,
            "column_end": 5,
            "properties": {"value": "42"},
        },
    }

    # 1. NODE_UPSERT
    kafka_producer.produce(node_topic, key=file_id, value=json.dumps(upsert_evt))
    kafka_producer.flush()
    time.sleep(3.5)  # Wait for ingestion

    res = run_cypher_query(
        f"MATCH (n:CPGNode {{id: '{node_id}'}}) RETURN n.node_type, n.name, n.placeholder, n.content_hash;",
        neo4j_password,
    )
    assert len(res) == 2  # header + row
    assert res[1][0] == "Assign"
    assert res[1][1] == "x"
    assert res[1][2] == "false"
    assert res[1][3] == "hash_version_1"

    # 2. Exact Replay (counts and hashes shouldn't change)
    kafka_producer.produce(node_topic, key=file_id, value=json.dumps(upsert_evt))
    kafka_producer.flush()
    time.sleep(3.0)

    res = run_cypher_query(f"MATCH (n:CPGNode {{id: '{node_id}'}}) RETURN count(n) as count;", neo4j_password)
    assert res[1][0] == "1"

    # 3. Update Properties (new generation)
    update_evt = dict(upsert_evt)
    update_evt["content_hash"] = "hash_version_2"
    update_evt["node"] = dict(upsert_evt["node"])
    update_evt["node"]["name"] = "x_updated"
    kafka_producer.produce(node_topic, key=file_id, value=json.dumps(update_evt))
    kafka_producer.flush()
    time.sleep(3.0)

    res = run_cypher_query(f"MATCH (n:CPGNode {{id: '{node_id}'}}) RETURN n.name, n.content_hash;", neo4j_password)
    assert res[1][0] == "x_updated"
    assert res[1][1] == "hash_version_2"

    # 4. NODE_DELETE (with generation guard matching content_hash)
    delete_evt = {
        "schema_version": "1.0",
        "event_id": "evt_n_del_1",
        "event_type": "NODE_DELETE",
        "event_time": "2026-07-22T10:05:00Z",
        "repository_id": "test_repo",
        "commit_sha": "sha_1",
        "file_id": file_id,
        "file_path": "a.py",
        "content_hash": "hash_version_2",
        "parser_version": "1.0.0",
        "node": {"node_id": node_id},
    }
    kafka_producer.produce(node_topic, key=file_id, value=json.dumps(delete_evt))
    kafka_producer.flush()
    time.sleep(3.0)

    res = run_cypher_query(f"MATCH (n:CPGNode {{id: '{node_id}'}}) RETURN count(n);", neo4j_password)
    assert res[1][0] == "0"

    # 5. Replay Delete (verify no failure and stays deleted)
    kafka_producer.produce(node_topic, key=file_id, value=json.dumps(delete_evt))
    kafka_producer.flush()
    time.sleep(3.0)

    res = run_cypher_query(f"MATCH (n:CPGNode {{id: '{node_id}'}}) RETURN count(n);", neo4j_password)
    assert res[1][0] == "0"


@pytest.mark.neo4j
@pytest.mark.kafka
def test_edge_ingestion_and_placeholder_scenarios(kafka_producer: Producer, neo4j_password: str):
    """Verify edge upsert, replay, delete, and edge-before-node placeholder pattern."""
    edge_topic = "cpg.edges"
    node_topic = "cpg.nodes"
    file_id = "test_file_id_edge_ingest"
    edge_id = "test_edge_id_1"
    src_id = "test_src_node_id"
    dst_id = "test_dst_node_id"

    # 1. Edge-before-node pattern
    # Ingest edge where source and target nodes do not exist yet
    edge_evt = {
        "schema_version": "1.0",
        "event_id": "evt_e_up_1",
        "event_type": "EDGE_UPSERT",
        "event_time": "2026-07-22T10:00:00Z",
        "repository_id": "test_repo",
        "commit_sha": "sha_1",
        "file_id": file_id,
        "file_path": "a.py",
        "content_hash": "edge_generation_1",
        "parser_version": "1.0.0",
        "edge": {
            "edge_id": edge_id,
            "source_id": src_id,
            "target_id": dst_id,
            "edge_type": "AST_CHILD",
            "properties": {},
        },
    }

    kafka_producer.produce(edge_topic, key=file_id, value=json.dumps(edge_evt))
    kafka_producer.flush()
    time.sleep(3.5)

    # Verify that source and target nodes were created as placeholders
    res_nodes = run_cypher_query(
        f"MATCH (n:CPGNode) WHERE n.id IN ['{src_id}', '{dst_id}'] RETURN n.id, n.placeholder, n.file_id;",
        neo4j_password,
    )
    assert len(res_nodes) == 3  # header + 2 rows
    for row in res_nodes[1:]:
        assert row[1] == "true"
        assert row[2] == file_id

    # Verify the relationship is established
    res_edges = run_cypher_query(
        f"MATCH (s)-[r:CPG_EDGE {{edge_id: '{edge_id}'}}]->(d) RETURN s.id, r.edge_type, d.id;",
        neo4j_password,
    )
    assert len(res_edges) == 2
    assert res_edges[1][0] == src_id
    assert res_edges[1][1] == "AST_CHILD"
    assert res_edges[1][2] == dst_id

    # Replay Edge (verify counts)
    kafka_producer.produce(edge_topic, key=file_id, value=json.dumps(edge_evt))
    kafka_producer.flush()
    time.sleep(3.0)

    res_edge_count = run_cypher_query(
        f"MATCH ()-[r:CPG_EDGE {{edge_id: '{edge_id}'}}]->() RETURN count(r);", neo4j_password
    )
    assert res_edge_count[1][0] == "1"

    # Hydrate source node with a real node upsert
    src_node_evt = {
        "schema_version": "1.0",
        "event_id": "evt_src_up_1",
        "event_type": "NODE_UPSERT",
        "event_time": "2026-07-22T10:02:00Z",
        "repository_id": "test_repo",
        "commit_sha": "sha_1",
        "file_id": file_id,
        "file_path": "a.py",
        "content_hash": "edge_generation_1",
        "parser_version": "1.0.0",
        "node": {
            "node_id": src_id,
            "node_type": "Name",
            "name": "source_node_real",
            "qualified_name": "source_node_real",
            "ast_path": "Module.body[0]",
            "line_start": 2,
            "column_start": 4,
            "line_end": 2,
            "column_end": 8,
            "properties": {},
        },
    }
    kafka_producer.produce(node_topic, key=file_id, value=json.dumps(src_node_evt))
    kafka_producer.flush()
    time.sleep(3.0)

    # Verify that source node is hydrated (placeholder=false), and target is still placeholder=true
    res_hydration = run_cypher_query(
        f"MATCH (n:CPGNode) WHERE n.id IN ['{src_id}', '{dst_id}'] RETURN n.id, n.placeholder, n.name;",
        neo4j_password,
    )
    rows = {r[0]: (r[1], r[2]) for r in res_hydration[1:]}
    assert rows[src_id][0] == "false"
    assert rows[src_id][1] == "source_node_real"
    assert rows[dst_id][0] == "true"

    # Clean up edge using EDGE_DELETE
    edge_del_evt = {
        "schema_version": "1.0",
        "event_id": "evt_e_del_1",
        "event_type": "EDGE_DELETE",
        "event_time": "2026-07-22T10:05:00Z",
        "repository_id": "test_repo",
        "commit_sha": "sha_1",
        "file_id": file_id,
        "file_path": "a.py",
        "content_hash": "edge_generation_1",
        "parser_version": "1.0.0",
        "edge": {"edge_id": edge_id},
    }
    kafka_producer.produce(edge_topic, key=file_id, value=json.dumps(edge_del_evt))
    kafka_producer.flush()
    time.sleep(3.0)

    res_edge_count = run_cypher_query(
        f"MATCH ()-[r:CPG_EDGE {{edge_id: '{edge_id}'}}]->() RETURN count(r);", neo4j_password
    )
    assert res_edge_count[1][0] == "0"

    # Replay Edge Delete
    kafka_producer.produce(edge_topic, key=file_id, value=json.dumps(edge_del_evt))
    kafka_producer.flush()
    time.sleep(3.0)
    res_edge_count = run_cypher_query(
        f"MATCH ()-[r:CPG_EDGE {{edge_id: '{edge_id}'}}]->() RETURN count(r);", neo4j_password
    )
    assert res_edge_count[1][0] == "0"


@pytest.mark.neo4j
@pytest.mark.kafka
def test_generation_guarded_stale_delete(kafka_producer: Producer, neo4j_password: str):
    """Verify that a stale delete (with an old content_hash) does not remove a node created/updated by a newer generation."""
    node_topic = "cpg.nodes"
    file_id = "test_file_id_stale_del"
    node_id = "test_stale_node_1"

    # 1. Upsert a node in a new generation (generation 2)
    new_upsert = {
        "schema_version": "1.0",
        "event_id": "evt_stale_up_2",
        "event_type": "NODE_UPSERT",
        "event_time": "2026-07-22T10:00:00Z",
        "repository_id": "test_repo",
        "commit_sha": "sha_1",
        "file_id": file_id,
        "file_path": "a.py",
        "content_hash": "generation_new",
        "parser_version": "1.0.0",
        "node": {
            "node_id": node_id,
            "node_type": "Constant",
            "name": "y",
            "qualified_name": "y",
            "ast_path": "Module.body[0]",
            "line_start": 1,
            "column_start": 0,
            "line_end": 1,
            "column_end": 1,
            "properties": {},
        },
    }
    kafka_producer.produce(node_topic, key=file_id, value=json.dumps(new_upsert))
    kafka_producer.flush()
    time.sleep(3.5)

    # Assert node exists
    res = run_cypher_query(f"MATCH (n:CPGNode {{id: '{node_id}'}}) RETURN n.content_hash;", neo4j_password)
    assert res[1][0] == "generation_new"

    # 2. Publish a delete event from an old generation (generation 1)
    stale_del = {
        "schema_version": "1.0",
        "event_id": "evt_stale_del_1",
        "event_type": "NODE_DELETE",
        "event_time": "2026-07-22T10:05:00Z",
        "repository_id": "test_repo",
        "commit_sha": "sha_1",
        "file_id": file_id,
        "file_path": "a.py",
        "content_hash": "generation_old_stale",
        "parser_version": "1.0.0",
        "node": {"node_id": node_id},
    }
    kafka_producer.produce(node_topic, key=file_id, value=json.dumps(stale_del))
    kafka_producer.flush()
    time.sleep(3.0)

    # Assert that the node remains in the database (deletion was guarded and ignored)
    res = run_cypher_query(f"MATCH (n:CPGNode {{id: '{node_id}'}}) RETURN count(n);", neo4j_password)
    assert res[1][0] == "1"


@pytest.mark.kafka
@pytest.mark.kafka_connect
def test_dead_letter_queue_handling(kafka_producer: Producer, env_vars: dict[str, str]):
    """Verify that sending a malformed payload to cpg.nodes does not crash the connector and routes the error record to connector.errors."""
    bootstrap_servers = env_vars.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    dlq_topic = "connector.errors"
    node_topic = "cpg.nodes"

    # Create consumer for DLQ
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"test-dlq-validation-group-{int(time.time())}",
        "auto.offset.reset": "earliest",  # Read from beginning to prevent partition assignment race
        "enable.auto.commit": "false",
    }
    consumer = Consumer(conf)
    consumer.subscribe([dlq_topic])

    # Let consumer assign partition
    consumer.poll(1.0)

    # 1. Publish malformed JSON message (invalid structure/JSON parse error)
    malformed_msg = '{"event_id": "dlq_fail_1", "event_type": "NODE_UPSERT", '  # incomplete JSON
    kafka_producer.produce(node_topic, key="dlq_test_key", value=malformed_msg)
    kafka_producer.flush()
    time.sleep(3.0)

    # 2. Verify connector health (stays RUNNING)
    code, status = deploy_connectors.make_request(f"{deploy_connectors.CONNECT_URL}/connectors/neo4j-nodes-sink/status")
    assert code == 200
    assert status.get("connector", {}).get("state") == "RUNNING"
    assert status.get("tasks", [{}])[0].get("state") == "RUNNING"

    # 3. Read from DLQ topic and verify message is captured
    dlq_msg = consumer.poll(timeout=10.0)
    assert dlq_msg is not None, "Malformed message did not reach DLQ topic connector.errors"
    assert dlq_msg.error() is None

    # Assert header context details
    headers = dlq_msg.headers()
    assert headers is not None
    header_keys = [h[0] for h in headers]
    assert "deadletterqueue.error.class" in header_keys or any("error" in k for k in header_keys)

    # 4. Verify a subsequent valid message works correctly
    valid_id = "test_valid_post_dlq"
    valid_evt = {
        "schema_version": "1.0",
        "event_id": "evt_dlq_valid_1",
        "event_type": "NODE_UPSERT",
        "event_time": "2026-07-22T11:00:00Z",
        "repository_id": "test_repo",
        "commit_sha": "sha_1",
        "file_id": "dlq_test_key",
        "file_path": "a.py",
        "content_hash": "dlq_verif",
        "parser_version": "1.0.0",
        "node": {
            "node_id": valid_id,
            "node_type": "Constant",
            "name": "z",
            "qualified_name": "z",
            "ast_path": "Module.body[0]",
            "line_start": 1,
            "column_start": 0,
            "line_end": 1,
            "column_end": 1,
            "properties": {},
        },
    }
    kafka_producer.produce(node_topic, key="dlq_test_key", value=json.dumps(valid_evt))
    kafka_producer.flush()
    time.sleep(3.0)

    # Check node is ingested successfully
    password = env_vars.get("NEO4J_PASSWORD", "CHANGE_ME_NEO4J_PASSWORD")
    res = run_cypher_query(f"MATCH (n:CPGNode {{id: '{valid_id}'}}) RETURN count(n);", password)
    assert res[1][0] == "1"

    consumer.close()


def test_base_compose_without_neo4j_secret():
    """Verify that base docker-compose config command succeeds when NEO4J_PASSWORD is not set."""
    # Run config with NEO4J_PASSWORD unset using clean environment
    env = dict(os.environ)
    if "NEO4J_PASSWORD" in env:
        del env["NEO4J_PASSWORD"]

    res = subprocess.run(
        ["docker", "compose", "-f", "infra/docker-compose.yml", "config"],
        capture_output=True,
        text=True,
        env=env,
    )
    assert res.returncode == 0, f"Base compose configuration check failed: {res.stderr}"
