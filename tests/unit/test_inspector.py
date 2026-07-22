import sys
import json
from pathlib import Path
from unittest.mock import Mock, patch
import pytest

# Add scripts directory to path to import inspect_kafka_events
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT / "scripts"))

import inspect_kafka_events  # noqa: E402


@patch("inspect_kafka_events.Consumer")
@patch("inspect_kafka_events.argparse.ArgumentParser.parse_args")
def test_inspector_unbounded(mock_parse_args, mock_consumer_cls):
    mock_args = Mock()
    mock_args.start_offsets = None
    mock_args.end_offsets = None
    mock_args.expected_file_id = None
    mock_args.expected_error_file_id = None
    mock_parse_args.return_value = mock_args

    mock_consumer = Mock()
    mock_consumer_cls.return_value = mock_consumer
    mock_consumer.poll.return_value = None

    with pytest.raises(SystemExit) as exc_info:
        inspect_kafka_events.main()
    assert exc_info.value.code == 0

    mock_consumer.subscribe.assert_called_once()
    mock_consumer.assign.assert_not_called()


@patch("inspect_kafka_events.Consumer")
@patch("inspect_kafka_events.argparse.ArgumentParser.parse_args")
def test_inspector_bounded(mock_parse_args, mock_consumer_cls, tmp_path):
    start_file = tmp_path / "start.json"
    end_file = tmp_path / "end.json"

    with open(start_file, "w") as f:
        json.dump({"cpg.nodes": {"0": 10}}, f)
    with open(end_file, "w") as f:
        json.dump({"cpg.nodes": {"0": 15}}, f)

    mock_args = Mock()
    mock_args.start_offsets = str(start_file)
    mock_args.end_offsets = str(end_file)
    mock_args.expected_file_id = "test_file_id"
    mock_args.expected_error_file_id = None
    mock_parse_args.return_value = mock_args

    mock_consumer = Mock()
    mock_consumer_cls.return_value = mock_consumer

    mock_msg = Mock()
    mock_msg.error.return_value = None
    mock_msg.topic.return_value = "cpg.nodes"
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 11
    mock_msg.key.return_value = b"test_file_id"
    payload = {
        "schema_version": "1.0",
        "event_id": "evt_1",
        "event_type": "NODE_UPSERT",
        "event_time": "2026-07-22T03:00:00Z",
        "repository_id": "test_repo",
        "commit_sha": "sha_abc",
        "file_id": "test_file_id",
        "file_path": "foo.py",
        "content_hash": "hash_abc",
        "parser_version": "1.0.0",
        "node": {"node_id": "node_1", "node_type": "Module", "ast_path": "0"},
    }
    mock_msg.value.return_value = json.dumps(payload).encode("utf-8")

    mock_consumer.poll.side_effect = [mock_msg, None, None, None, None, None]

    with pytest.raises(SystemExit) as exc_info:
        inspect_kafka_events.main()
    assert exc_info.value.code == 0

    mock_consumer.assign.assert_called_once()
    mock_consumer.seek.assert_called_once()
