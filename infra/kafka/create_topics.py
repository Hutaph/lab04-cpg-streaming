import sys
import yaml
import subprocess
import re
import time
from pathlib import Path

COMPOSE_FILE = Path("infra/docker-compose.yml")
TOPICS_YAML = Path("config/topics.yaml")


def run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def load_desired_topics(config_path: Path = TOPICS_YAML) -> list[dict]:
    """Load desired Kafka topics from the project topic contract."""
    with open(config_path, "r", encoding="utf-8") as f:
        config_data = yaml.safe_load(f) or {}
    desired_topics = config_data.get("topics", [])
    if not isinstance(desired_topics, list):
        raise ValueError("config/topics.yaml must define topics as a list.")
    return desired_topics


def wait_for_kafka():
    print("Waiting for Kafka broker to be healthy...")
    max_retries = 30
    for i in range(1, max_retries + 1):
        try:
            res = run_cmd(
                [
                    "docker",
                    "compose",
                    "-f",
                    str(COMPOSE_FILE),
                    "exec",
                    "-T",
                    "kafka",
                    "kafka-topics",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--list",
                ],
                check=False,
            )
            if res.returncode == 0:
                print("Kafka broker is healthy.")
                return
        except Exception:
            pass
        print(f"Kafka is not ready yet. Retrying in 2 seconds... ({i}/{max_retries})")
        time.sleep(2)
    print("Error: Kafka broker did not become healthy in time.")
    sys.exit(1)


def check_drift(desired_topics: list[dict], actual_topics: dict) -> tuple[bool, list[str]]:
    drift_detected = False
    log_messages = []
    for dt in desired_topics:
        name = dt["name"]
        desired_parts = int(dt["partitions"])
        desired_rep = int(dt["replication_factor"])

        if name not in actual_topics:
            log_messages.append(f"MISSING:{name},{desired_parts},{desired_rep}")
        else:
            actual_parts = actual_topics[name]["partitions"]
            actual_rep = actual_topics[name]["replication_factor"]

            mismatch = False
            if actual_parts != desired_parts:
                log_messages.append(
                    f"[ERROR] Topic configuration drift detected for topic '{name}'. Partitions mismatch: desired {desired_parts}, actual {actual_parts}"
                )
                mismatch = True
            if actual_rep != desired_rep:
                log_messages.append(
                    f"[ERROR] Topic configuration drift detected for topic '{name}'. Replication mismatch: desired {desired_rep}, actual {actual_rep}"
                )
                mismatch = True

            if mismatch:
                drift_detected = True
            else:
                log_messages.append(f"[OK] Topic '{name}' matches desired configuration.")
    return drift_detected, log_messages


def main():
    wait_for_kafka()

    desired_topics = load_desired_topics(TOPICS_YAML)

    # Get actual topics describe output
    describe_res = run_cmd(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "exec",
            "-T",
            "kafka",
            "kafka-topics",
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
        ]
    )
    describe_output = describe_res.stdout

    # Parse actual topic configurations
    actual_topics = {}
    for line in describe_output.splitlines():
        if line.startswith("Topic:") and "PartitionCount:" in line:
            match_name = re.search(r"Topic:\s*(\S+)", line)
            match_partitions = re.search(r"PartitionCount:\s*(\d+)", line)
            match_replication = re.search(r"ReplicationFactor:\s*(\d+)", line)
            if match_name and match_partitions and match_replication:
                name = match_name.group(1)
                partitions = int(match_partitions.group(1))
                replication = int(match_replication.group(1))
                actual_topics[name] = {"partitions": partitions, "replication_factor": replication}

    drift_detected, log_messages = check_drift(desired_topics, actual_topics)

    print("Creating and validating topics...")
    for msg in log_messages:
        if msg.startswith("MISSING:"):
            name, parts, rep = msg.split(":")[-1].split(",")
            print(f"Creating topic '{name}' (partitions: {parts}, replication factor: {rep})...")
            create_res = run_cmd(
                [
                    "docker",
                    "compose",
                    "-f",
                    str(COMPOSE_FILE),
                    "exec",
                    "-T",
                    "kafka",
                    "kafka-topics",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--create",
                    "--topic",
                    name,
                    "--partitions",
                    str(parts),
                    "--replication-factor",
                    str(rep),
                ],
                check=False,
            )
            if create_res.returncode != 0:
                print(f"[ERROR] Failed to create topic '{name}': {create_res.stderr.strip()}")
                drift_detected = True
        else:
            print(msg)

    # List all topics at the end
    print("\n=== Existing Topics ===")
    list_res = run_cmd(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "exec",
            "-T",
            "kafka",
            "kafka-topics",
            "--bootstrap-server",
            "localhost:9092",
            "--list",
        ],
        check=False,
    )
    print(list_res.stdout)

    if drift_detected:
        print("\n[FAILED] Configuration drift detected. Setup failed.")
        sys.exit(1)
    else:
        print("\n[SUCCESS] All topics match desired configurations.")
        sys.exit(0)


if __name__ == "__main__":
    main()
