"""Kafka Connect and Kafka cluster verification utilities.

Adheres strictly to the Code Language Policy: 100% English code, docstrings,
comments, and log messages.
"""

import json
import subprocess
import time
import urllib.request
from typing import Any, cast


def make_request(url: str, method: str = "GET", data: Any = None) -> tuple[int, Any]:
    """Helper to perform HTTP requests to Kafka Connect REST API."""
    try:
        req = urllib.request.Request(url, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
            json_data = json.dumps(data).encode("utf-8")
            req.data = json_data

        with urllib.request.urlopen(req, timeout=10) as response:
            body = response.read().decode("utf-8")
            if response.status in (200, 201):
                return response.status, json.loads(body) if body else {}
            return response.status, body
    except urllib.error.HTTPError as err:
        try:
            body = err.read().decode("utf-8")
            return err.code, json.loads(body) if body else {}
        except Exception:
            return err.code, str(err)
    except Exception as exc:
        return 500, str(exc)


def redact_connector_config(config: dict[str, Any]) -> dict[str, Any]:
    """Returns a copy of the configuration dictionary with sensitive fields redacted."""
    redacted = dict(config)
    sensitive_fragments = ("password", "secret", "token", "connection.uri")
    for key, value in config.items():
        normalized_key = key.lower()
        if any(fragment in normalized_key for fragment in sensitive_fragments):
            redacted[key] = f"REDACTED (len={len(str(value))})"
    return redacted


def get_connector_status(connector_name: str) -> dict[str, Any]:
    """Retrieves status of a specific connector and its tasks from Kafka Connect."""
    code, res = make_request(f"http://localhost:8083/connectors/{connector_name}/status")
    if code != 200:
        raise RuntimeError(f"Failed to query status of connector '{connector_name}': {res}")
    if not isinstance(res, dict):
        raise RuntimeError(f"Connector '{connector_name}' returned a non-object status payload: {res}")
    return cast(dict[str, Any], res)


def assert_connector_running(connector_name: str) -> None:
    """Asserts that the connector and all of its tasks are in the RUNNING state."""
    status = get_connector_status(connector_name)
    conn_state = status.get("connector", {}).get("state", "UNKNOWN")
    if conn_state != "RUNNING":
        raise AssertionError(f"Connector '{connector_name}' is in state '{conn_state}' (expected 'RUNNING')")

    tasks = status.get("tasks", [])
    if not tasks:
        raise AssertionError(f"No tasks found for connector '{connector_name}'")
    for task in tasks:
        task_id = task.get("id")
        task_state = task.get("state")
        if task_state != "RUNNING":
            raise AssertionError(
                f"Task {task_id} of connector '{connector_name}' is in state '{task_state}' (expected 'RUNNING')"
            )


def get_connector_lag(group_id: str) -> dict[int, int]:
    """Queries kafka-consumer-groups tool to obtain current lag per partition."""
    try:
        res = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "cpg-kafka",
                "kafka-consumer-groups",
                "--bootstrap-server",
                "localhost:9092",
                "--group",
                group_id,
                "--describe",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        lines = res.stdout.strip().splitlines()
        lags: dict[int, int] = {}
        if not lines:
            return lags

        # Parse CSV/whitespace separated columns:
        # GROUP, TOPIC, PARTITION, CURRENT-OFFSET, LOG-END-OFFSET, LAG, CONSUMER-ID, HOST, CLIENT-ID
        headers = [h.strip() for h in lines[0].split() if h.strip()]
        if "PARTITION" not in headers or "LAG" not in headers:
            return lags

        part_idx = headers.index("PARTITION")
        lag_idx = headers.index("LAG")

        for line in lines[1:]:
            parts = line.split()
            if len(parts) > max(part_idx, lag_idx):
                part_str = parts[part_idx].strip()
                lag_str = parts[lag_idx].strip()
                if part_str.isdigit() and (lag_str.isdigit() or lag_str == "-"):
                    partition = int(part_str)
                    lag_val = 0 if lag_str == "-" else int(lag_str)
                    lags[partition] = lag_val
        return lags
    except Exception as exc:
        raise RuntimeError(f"Failed to retrieve consumer lag for group '{group_id}': {exc}")


def wait_for_zero_lag(group_id: str, timeout: int = 120) -> None:
    """Blocks and polls consumer lag until it reaches 0 for all partitions, or times out."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        lags = get_connector_lag(group_id)
        if lags and sum(lags.values()) == 0:
            return
        time.sleep(2)
    raise TimeoutError(f"Lag for group '{group_id}' did not reach 0 within {timeout} seconds")


def get_topic_end_offsets(bootstrap_servers: str, topic: str) -> dict[int, int]:
    """Queries kafka-run-class tool to get end offsets (high watermarks) for all partitions."""
    try:
        # Run kafka-run-class.sh kafka.tools.GetOffsetShell
        res = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "cpg-kafka",
                "kafka-run-class",
                "kafka.tools.GetOffsetShell",
                "--bootstrap-server",
                bootstrap_servers,
                "--topic",
                topic,
                "--time",
                "-1",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        lines = res.stdout.strip().splitlines()
        offsets: dict[int, int] = {}
        for line in lines:
            parts = line.split(":")
            if len(parts) == 3:
                # Format is topic:partition:offset
                partition = int(parts[1])
                offset = int(parts[2])
                offsets[partition] = offset
        return offsets
    except Exception as exc:
        raise RuntimeError(f"Failed to query offsets for topic '{topic}': {exc}")


def calculate_dlq_delta(before: dict[int, int], after: dict[int, int]) -> int:
    """Calculates the sum of differences in partition offsets between two measurements."""
    delta = 0
    all_partitions = set(before.keys()).union(after.keys())
    for p in all_partitions:
        b_val = before.get(p, 0)
        a_val = after.get(p, 0)
        delta += max(0, a_val - b_val)
    return delta
