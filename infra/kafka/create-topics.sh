#!/usr/bin/env bash
set -euo pipefail

# This script runs from the repository root (since scripts/create_topics.sh wraps it).
COMPOSE_FILE="infra/docker-compose.yml"
TOPICS_YAML="config/topics.yaml"

echo "Waiting for Kafka broker to be healthy..."
max_retries=30
retry_count=0
until docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
    retry_count=$((retry_count + 1))
    if [ "$retry_count" -ge "$max_retries" ]; then
        echo "Error: Kafka broker did not become healthy in time."
        exit 1
    fi
    echo "Kafka is not ready yet. Retrying in 2 seconds... ($retry_count/$max_retries)"
    sleep 2
done
echo "Kafka broker is healthy."

# Parse topics from topics.yaml using python
python3_cmd="import yaml; print('\n'.join([f\"{t['name']},{t['partitions']},{t['replication_factor']}\" for t in yaml.safe_load(open('$TOPICS_YAML'))['topics']]))"
topics_list=$(uv run python -c "$python3_cmd")

# Get list of existing topics
existing_topics=$(docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || echo "")

echo "Creating topics idempotently..."
for item in $topics_list; do
    IFS=',' read -r name partitions replication <<< "$item"
    if echo "$existing_topics" | grep -Fqx "$name"; then
        echo "Topic '$name' already exists."
    else
        echo "Creating topic '$name' (partitions: $partitions, replication factor: $replication)..."
        docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-topics \
            --bootstrap-server localhost:9092 \
            --create \
            --topic "$name" \
            --partitions "$partitions" \
            --replication-factor "$replication" </dev/null
    fi
done

echo -e "\n=== Existing Topics ==="
docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list

echo -e "\n=== Topic Details ==="
docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --describe
