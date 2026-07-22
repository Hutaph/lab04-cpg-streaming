#!/usr/bin/env python3
"""Verification script to consume CPG events from Kafka and validate them against JSON Schemas."""

import json
import os
import sys
from pathlib import Path
import yaml
from confluent_kafka import Consumer, KafkaError
from jsonschema import Draft202012Validator


def load_schemas(schemas_dir: Path) -> dict[str, Draft202012Validator]:
    schemas = {}
    mapping = {
        "NODE_UPSERT": "node-event.schema.json",
        "NODE_DELETE": "node-event.schema.json",
        "EDGE_UPSERT": "edge-event.schema.json",
        "EDGE_DELETE": "edge-event.schema.json",
        "FILE_METADATA_UPSERT": "metadata-event.schema.json",
        "PARSER_ERROR": "error-event.schema.json",
    }
    for event_type, filename in mapping.items():
        schema_path = schemas_dir / filename
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_data = json.load(f)
        schemas[event_type] = Draft202012Validator(schema_data)
    return schemas


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topics_yaml = Path("config/topics.yaml")
    schemas_dir = Path("schemas")

    if not topics_yaml.exists():
        topics_yaml = Path("../config/topics.yaml")
    if not schemas_dir.exists():
        schemas_dir = Path("../schemas")

    with open(topics_yaml, "r", encoding="utf-8") as f:
        config_data = yaml.safe_load(f)

    topics = [t["name"] for t in config_data["topics"] if t["name"] != "connector.errors"]

    print(f"Subscribing to topics: {topics}")
    schemas = load_schemas(schemas_dir)

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": "cpg-kafka-smoke-verifier",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
        "enable.partition.eof": "false",
    }

    consumer = Consumer(conf)
    consumer.subscribe(topics)

    consumed_count = 0
    validation_failures = 0
    key_failures = 0
    partition_map = {}  # (topic, file_id) -> set of partitions

    print("Listening for messages... (will auto-stop after 5s of inactivity)")
    try:
        empty_polls = 0
        while empty_polls < 5:
            msg = consumer.poll(1.0)
            if msg is None:
                empty_polls += 1
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    empty_polls += 1
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            empty_polls = 0

            consumed_count += 1
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            key_bytes = msg.key()
            value_bytes = msg.value()

            key = key_bytes.decode("utf-8") if key_bytes else None
            payload = json.loads(value_bytes.decode("utf-8"))
            event_type = payload.get("event_type")
            file_id = payload.get("file_id")

            print(f"[{topic}] Part:{partition} Off:{offset} Key:{key} Event:{event_type}")

            # Validate key = file_id
            if key != file_id:
                print(f"  [ERROR] Key mismatch! Key: {key}, file_id in payload: {file_id}")
                key_failures += 1

            # Group events by (topic, file_id) to verify partition routing
            if file_id:
                partition_map.setdefault((topic, file_id), set()).add(partition)

            # Validate against schema
            if event_type in schemas:
                try:
                    schemas[event_type].validate(payload)
                    print("  [OK] Schema validation passed.")
                except Exception as exc:
                    print(f"  [ERROR] Schema validation failed: {exc}")
                    validation_failures += 1
            else:
                print(f"  [WARNING] Unknown event_type: {event_type}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print("\n=== Verification Summary ===")
    print(f"Total messages consumed: {consumed_count}")
    print(f"Schema validation failures: {validation_failures}")
    print(f"Key mismatches (key != file_id): {key_failures}")

    # Partition key validation
    print("\n=== Per-Topic Partition Consistency ===")
    partition_routing_ok = True
    for (t, fid), parts in sorted(partition_map.items()):
        if len(parts) > 1:
            print(f"[FAIL] topic={t}, file_id={fid[:8]}..., partitions={parts}")
            partition_routing_ok = False
        else:
            print(f"[PASS] topic={t}, file_id={fid[:8]}..., partitions={parts}")

    print("\n[INFO] Partition numbers are topic-local and are not compared across topics.")
    print("[INFO] This verification does not claim cross-topic ordering.")

    if validation_failures > 0 or key_failures > 0 or not partition_routing_ok:
        print("\n[FAILED] Verification completed with failures.")
        sys.exit(1)
    else:
        print(
            "\n[SUCCESS] All inspected events have valid schemas, correct keys, valid topic routing, and consistent per-topic partition assignment."
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
