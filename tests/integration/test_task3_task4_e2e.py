import time
import json
import sqlite3
import subprocess
import pytest
from pathlib import Path
from confluent_kafka import Consumer

from infrastructure.messaging.event_validator import EventValidator
from infrastructure.messaging.kafka_producer import KafkaEventProducer
from infrastructure.state.sqlite_state_store import SqliteStateStore
from parsing.cpg_parser import CpgParser
from application.services.process_file import ProcessFileService
from infrastructure.filesystem.git_source_repository import GitSourceRepository
from domain.models import SourceFile
from domain.enums import ParseStatus
from conftest import run_cypher_query


def wait_for_neo4j_count(query: str, expected_count: int, password: str, timeout: float = 15.0) -> list[list[str]]:
    """Polls Neo4j until the count matches the expected value or timeout is reached."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        res = run_cypher_query(query, password)
        if len(res) >= 2 and res[1][0] == str(expected_count):
            return res
        time.sleep(0.5)
    return run_cypher_query(query, password)


@pytest.fixture
def e2e_temp_git_repo(tmp_path):
    """Sets up an isolated, real git repository containing standard Python code."""
    repo_dir = tmp_path / "e2e_git_repo"
    repo_dir.mkdir()
    subprocess.run(["git", "init"], cwd=str(repo_dir), check=True)
    subprocess.run(["git", "config", "user.name", "E2E Tester"], cwd=str(repo_dir), check=True)
    subprocess.run(["git", "config", "user.email", "e2e@example.com"], cwd=str(repo_dir), check=True)

    py_file = repo_dir / "maths.py"
    py_file.write_text("a = 10\nb = a + 5\n", encoding="utf-8")

    subprocess.run(["git", "add", "."], cwd=str(repo_dir), check=True)
    subprocess.run(["git", "commit", "-m", "initial maths module"], cwd=str(repo_dir), check=True)

    return repo_dir


@pytest.mark.neo4j
@pytest.mark.kafka
def test_task3_task4_end_to_end_flow(e2e_temp_git_repo, env_vars: dict[str, str]):
    password = env_vars.get("NEO4J_PASSWORD", "CHANGE_ME_NEO4J_PASSWORD")
    bootstrap_servers = env_vars.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # Clean up any potential leftover entities for e2e repo prefix
    run_cypher_query(
        "MATCH (n:CPGNode) WHERE n.repository_id = 'e2e_repo' WITH collect(n.file_id) AS file_ids OPTIONAL MATCH (t1:CPGNodeTombstone) WHERE t1.file_id IN file_ids OPTIONAL MATCH (t2:CPGEdgeTombstone) WHERE t2.file_id IN file_ids DETACH DELETE t1, t2;",
        password,
    )
    run_cypher_query("MATCH (n:CPGNode) WHERE n.repository_id = 'e2e_repo' DETACH DELETE n;", password)

    # 1. Initialize Pipeline Components
    repo_adapter = GitSourceRepository(repo_path=e2e_temp_git_repo, clone_url="")
    commit_sha = repo_adapter.get_commit_hash()

    db_path = e2e_temp_git_repo.parent / "e2e_state.sqlite3"
    state_store = SqliteStateStore(db_path=db_path, repository_id="e2e_repo")

    validator = EventValidator(schemas_dir=Path("schemas"))
    writer = KafkaEventProducer(bootstrap_servers=bootstrap_servers)
    parser = CpgParser(repository_id="e2e_repo")

    service = ProcessFileService(
        repo_adapter=repo_adapter,
        parser=parser,
        state_store=state_store,
        validator=validator,
        writer=writer,
    )

    sf = SourceFile(
        repository_id="e2e_repo",
        repository_root=str(e2e_temp_git_repo),
        relative_path="maths.py",
        commit_sha=commit_sha,
        size_bytes=100,
    )

    # ==========================================
    # SCENARIO A: Fresh Parse
    # ==========================================
    res_fresh = service.execute(sf)
    assert res_fresh.status == ParseStatus.SUCCESS
    file_id = res_fresh.file_id
    node_count = res_fresh.node_count
    edge_count = res_fresh.edge_count
    assert node_count > 0
    assert edge_count > 0

    # Verify counts in Neo4j with active polling
    nodes_res = wait_for_neo4j_count(
        f"MATCH (n:CPGNode) WHERE n.file_id = '{file_id}' RETURN count(n);",
        node_count,
        password,
    )
    assert nodes_res[1][0] == str(node_count)

    edges_res = wait_for_neo4j_count(
        f"MATCH ()-[r:CPG_EDGE]->() WHERE r.file_id = '{file_id}' RETURN count(r);",
        edge_count,
        password,
    )
    assert edges_res[1][0] == str(edge_count)

    # ==========================================
    # SCENARIO B: Unchanged Rerun
    # ==========================================
    res_rerun = service.execute(sf)
    assert res_rerun.status == ParseStatus.SKIPPED_UNCHANGED

    # Ensure Neo4j counts remain the same
    nodes_res = wait_for_neo4j_count(
        f"MATCH (n:CPGNode) WHERE n.file_id = '{file_id}' RETURN count(n);",
        node_count,
        password,
    )
    assert nodes_res[1][0] == str(node_count)

    # ==========================================
    # SCENARIO C: Exact Kafka Replay
    # ==========================================
    # Just publishing the exact same content will cause upsert replay (skipped hash match)
    res_replay = service.execute(sf)
    assert res_replay.status == ParseStatus.SKIPPED_UNCHANGED

    # ==========================================
    # SCENARIO D: File Modification
    # ==========================================
    # Change the maths module to contain different statements, deleting old nodes
    py_file = e2e_temp_git_repo / "maths.py"
    # Old file: a = 10 \n b = a + 5
    # New file: z = 99
    py_file.write_text("z = 99\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=str(e2e_temp_git_repo), check=True)
    subprocess.run(["git", "commit", "-m", "modify maths module"], cwd=str(e2e_temp_git_repo), check=True)

    new_commit = repo_adapter.get_commit_hash()
    sf_modified = SourceFile(
        repository_id="e2e_repo",
        repository_root=str(e2e_temp_git_repo),
        relative_path="maths.py",
        commit_sha=new_commit,
        size_bytes=100,
    )

    res_modified = service.execute(sf_modified)
    assert res_modified.status == ParseStatus.SUCCESS
    node_count_mod = res_modified.node_count
    edge_count_mod = res_modified.edge_count
    assert node_count_mod > 0

    # In Neo4j, old nodes/edges must be deleted, and new node created
    nodes_res = wait_for_neo4j_count(
        f"MATCH (n:CPGNode) WHERE n.file_id = '{file_id}' RETURN count(n);",
        node_count_mod,
        password,
    )
    assert nodes_res[1][0] == str(node_count_mod)

    edges_res = wait_for_neo4j_count(
        f"MATCH ()-[r:CPG_EDGE]->() WHERE r.file_id = '{file_id}' RETURN count(r);",
        edge_count_mod,
        password,
    )
    assert edges_res[1][0] == str(edge_count_mod)

    # Confirm Tombstones are created for deleted nodes
    tomb_res = run_cypher_query(
        f"MATCH (t:CPGNodeTombstone) WHERE t.file_id = '{file_id}' RETURN count(t);",
        password,
    )
    assert int(tomb_res[1][0]) > 0

    # ==========================================
    # SCENARIO E: Parser/Schema Version Refresh
    # ==========================================
    # Seed old parser version in SQLite to trigger forced full republish
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("UPDATE file_state SET parser_version = '0.9.0' WHERE file_id = ?", (file_id,))
        conn.commit()

    res_refresh = service.execute(sf_modified)
    assert res_refresh.status == ParseStatus.SUCCESS  # Forces republish

    # Graph remains correct and duplicate-free
    nodes_res = wait_for_neo4j_count(
        f"MATCH (n:CPGNode) WHERE n.file_id = '{file_id}' RETURN count(n);",
        node_count_mod,
        password,
    )
    assert nodes_res[1][0] == str(node_count_mod)

    # ==========================================
    # SCENARIO F: Parser failure
    # ==========================================
    # Put broken syntax in maths.py
    py_file.write_text("if not:\n    broken syntax\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=str(e2e_temp_git_repo), check=True)
    subprocess.run(["git", "commit", "-m", "broken syntax commit"], cwd=str(e2e_temp_git_repo), check=True)

    new_broken_commit = repo_adapter.get_commit_hash()
    sf_broken = SourceFile(
        repository_id="e2e_repo",
        repository_root=str(e2e_temp_git_repo),
        relative_path="maths.py",
        commit_sha=new_broken_commit,
        size_bytes=100,
    )

    # Listen to parser.errors topic
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"test-e2e-parser-errors-group-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    }
    consumer = Consumer(conf)
    consumer.subscribe(["parser.errors"])
    consumer.poll(1.0)

    # Run processing - it should fail parsing and emit error event
    res_broken = service.execute(sf_broken)
    assert res_broken.status == ParseStatus.FAILED

    # Wait and check that the parser.errors topic received this event
    msg = consumer.poll(timeout=10.0)
    assert msg is not None, "Error event did not reach parser.errors topic"
    val = json.loads(msg.value().decode("utf-8"))
    assert val["event_type"] == "PARSER_ERROR"
    assert val["file_id"] == file_id

    # Graph counts remain unchanged in Neo4j
    nodes_res = wait_for_neo4j_count(
        f"MATCH (n:CPGNode) WHERE n.file_id = '{file_id}' RETURN count(n);",
        node_count_mod,
        password,
    )
    assert nodes_res[1][0] == str(node_count_mod)

    consumer.close()

    # Final E2E cleanup
    run_cypher_query("MATCH (n:CPGNode) WHERE n.file_id STARTS WITH 'e2e_repo:' DETACH DELETE n;", password)
    run_cypher_query("MATCH (t:CPGNodeTombstone) WHERE t.file_id STARTS WITH 'e2e_repo:' DETACH DELETE t;", password)
