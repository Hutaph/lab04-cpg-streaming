#!/usr/bin/env bash

# Shell script to create topics on startup

echo "Creating Kafka Topics..."
kafka-topics --bootstrap-server localhost:9092 --create --topic cpg.nodes --partitions 3 --replication-factor 1 || true
kafka-topics --bootstrap-server localhost:9092 --create --topic cpg.edges --partitions 3 --replication-factor 1 || true
kafka-topics --bootstrap-server localhost:9092 --create --topic source.metadata --partitions 1 --replication-factor 1 || true
kafka-topics --bootstrap-server localhost:9092 --create --topic parser.errors --partitions 1 --replication-factor 1 || true
kafka-topics --bootstrap-server localhost:9092 --create --topic connector.errors --partitions 1 --replication-factor 1 || true
