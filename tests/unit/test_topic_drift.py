import sys
from pathlib import Path

# Add infra/kafka to path to import create_topics
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT / "infra/kafka"))

from create_topics import TOPICS_YAML, check_drift, load_desired_topics  # noqa: E402


def test_check_drift_matching():
    desired = [{"name": "topic.a", "partitions": 3, "replication_factor": 1}]
    actual = {"topic.a": {"partitions": 3, "replication_factor": 1}}
    drift, logs = check_drift(desired, actual)
    assert not drift
    assert any("[OK] Topic 'topic.a' matches" in log for log in logs)


def test_check_drift_missing():
    desired = [{"name": "topic.b", "partitions": 3, "replication_factor": 1}]
    actual = {}
    drift, logs = check_drift(desired, actual)
    assert not drift
    assert any("MISSING:topic.b,3,1" in log for log in logs)


def test_check_drift_partition_mismatch():
    desired = [{"name": "topic.c", "partitions": 3, "replication_factor": 1}]
    actual = {"topic.c": {"partitions": 1, "replication_factor": 1}}
    drift, logs = check_drift(desired, actual)
    assert drift
    assert any("Partitions mismatch: desired 3, actual 1" in log for log in logs)


def test_check_drift_replication_mismatch():
    desired = [{"name": "topic.d", "partitions": 3, "replication_factor": 2}]
    actual = {"topic.d": {"partitions": 3, "replication_factor": 1}}
    drift, logs = check_drift(desired, actual)
    assert drift
    assert any("Replication mismatch: desired 2, actual 1" in log for log in logs)


def test_topic_contract_uses_configured_partitions():
    desired = {topic["name"]: topic for topic in load_desired_topics(TOPICS_YAML)}

    assert desired["cpg.nodes"]["partitions"] == 3
    assert desired["cpg.edges"]["partitions"] == 3
    assert desired["source.metadata"]["partitions"] == 1
    assert desired["parser.errors"]["partitions"] == 1
    assert desired["connector.errors"]["partitions"] == 1
    assert desired["connector.errors"]["purpose"].lower().find("dead-letter") >= 0
