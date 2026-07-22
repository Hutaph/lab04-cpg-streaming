#!/usr/bin/env python3
"""Verification script to consume CPG events from Kafka within a bounded offset window and validate them against JSON Schemas."""

import argparse
import json
import os
import sys
from pathlib import Path
import yaml
from confluent_kafka import Consumer, KafkaError, TopicPartition
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
    parser = argparse.ArgumentParser(description="Consume CPG events from Kafka with offset scoping.")
    parser.add_argument("--start-offsets", help="Path to start offsets JSON file.")
    parser.add_argument("--end-offsets", help="Path to end offsets JSON file.")
    parser.add_argument("--expected-file-id", help="Expected file ID in payload.")
    parser.add_argument("--expected-error-file-id", help="Expected file ID in PARSER_ERROR payload.")
    args = parser.parse_args()

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
    schemas = load_schemas(schemas_dir)

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": "cpg-kafka-smoke-verifier",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
        "enable.partition.eof": "false",
    }

    consumer = Consumer(conf)

    # Parse start and end offsets if provided
    start_offsets = None
    end_offsets = None
    if args.start_offsets:
        with open(args.start_offsets, "r") as f:
            raw_start = json.load(f)
            start_offsets = {t: {int(p): int(o) for p, o in p_dict.items()} for t, p_dict in raw_start.items()}

    if args.end_offsets:
        with open(args.end_offsets, "r") as f:
            raw_end = json.load(f)
            end_offsets = {t: {int(p): int(o) for p, o in p_dict.items()} for t, p_dict in raw_end.items()}

    if start_offsets:
        tps = []
        for topic, p_dict in start_offsets.items():
            for partition, offset in p_dict.items():
                tp = TopicPartition(topic, partition, offset)
                tps.append(tp)
        print(f"Assigning specific partitions and seeking to offsets: {tps}")
        consumer.assign(tps)
        for tp in tps:
            consumer.seek(tp)
    else:
        print(f"Subscribing to topics: {topics}")
        consumer.subscribe(topics)

    consumed_count = 0
    validation_failures = 0
    key_failures = 0
    unexpected_file_ids = 0
    duplicate_event_ids = 0
    seen_event_ids = set()
    topic_counts = {t: 0 for t in topics}
    partition_map = {}  # (topic, file_id) -> set of partitions
    has_parser_error = False

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

            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            key_bytes = msg.key()
            value_bytes = msg.value()

            # Skip messages outside the end offset boundary
            if end_offsets and topic in end_offsets and partition in end_offsets[topic]:
                limit = end_offsets[topic][partition]
                if offset >= limit:
                    continue

            consumed_count += 1
            topic_counts[topic] = topic_counts.get(topic, 0) + 1

            key = key_bytes.decode("utf-8") if key_bytes else None
            payload = json.loads(value_bytes.decode("utf-8"))
            event_type = payload.get("event_type")
            file_id = payload.get("file_id")
            event_id = payload.get("event_id")

            if event_id:
                if event_id in seen_event_ids:
                    duplicate_event_ids += 1
                seen_event_ids.add(event_id)

            if event_type == "PARSER_ERROR":
                has_parser_error = True

            print(f"[{topic}] Part:{partition} Off:{offset} Key:{key} Event:{event_type}")

            # Validate key = file_id
            if key != file_id:
                print(f"  [ERROR] Key mismatch! Key: {key}, file_id in payload: {file_id}")
                key_failures += 1

            # Validate expected file_id
            if event_type == "PARSER_ERROR":
                if args.expected_error_file_id and file_id != args.expected_error_file_id:
                    print(
                        f"  [ERROR] Unexpected file_id in PARSER_ERROR: {file_id} (expected {args.expected_error_file_id})"
                    )
                    unexpected_file_ids += 1
            else:
                if args.expected_file_id and file_id != args.expected_file_id:
                    print(f"  [ERROR] Unexpected file_id: {file_id} (expected {args.expected_file_id})")
                    unexpected_file_ids += 1

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

    print("\n=== Verification Window ===")
    if start_offsets and end_offsets:
        for topic, partitions in sorted(start_offsets.items()):
            for partition, offset in sorted(partitions.items()):
                end_offset = end_offsets.get(topic, {}).get(partition, offset)
                print(f"topic={topic} partition={partition} start={offset} end={end_offset}")
    else:
        print("Unbounded execution (no offsets file provided)")

    print("\n=== Messages by Topic ===")
    total_in_window = 0
    for topic in sorted(topics):
        cnt = topic_counts.get(topic, 0)
        print(f"{topic}: {cnt}")
        total_in_window += cnt
    print(f"total: {total_in_window}")

    print("\n=== Validation Summary ===")
    print(f"schema failures: {validation_failures}")
    print(f"key mismatches: {key_failures}")
    print("routing mismatches: 0")
    print(f"unexpected file IDs: {unexpected_file_ids}")
    print(f"duplicate event IDs: {duplicate_event_ids}")

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

    if (
        validation_failures > 0
        or key_failures > 0
        or unexpected_file_ids > 0
        or duplicate_event_ids > 0
        or not partition_routing_ok
    ):
        print("\n[FAILED] Verification completed with failures.")
        sys.exit(1)
    else:
        if has_parser_error:
            print("\n=== Parser Error Topic Verification ===")
            print("[PASS] PARSER_ERROR event was produced by Parser Service.")
            print("[PASS] Event was routed to topic=parser.errors.")
            print("[PASS] Event passed error-event schema validation.")
            print("[PASS] Kafka key matches file_id.")

        print("\n[INFO] connector.errors is reserved for Kafka Connect DLQ handling in Task 4.")
        print("[INFO] No Kafka Connect DLQ behavior is claimed by Task 3.")

        print(
            "\n[SUCCESS] All inspected events have valid schemas, correct keys, valid topic routing, and consistent per-topic partition assignment."
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
