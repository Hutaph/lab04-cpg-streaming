# Testing Layout for CPG Ingestion System

This directory houses tests for validating parsing correctness, deterministic stable IDs, topic event compatibility, and data streaming integration.

## Directory Structure

- **`fixtures/`**: Static Python scripts containing typical control flow constructs (loops, calls, broken syntax) used as parser input sources.
- **`unit/`**: Verifies builders, identifier generator, event serialization, schema validation, and graph diffing.
- **`integration/`**: Verifies Kafka producers and local sqlite state transactions.
- **`e2e/`**: Verifies end-to-end flow from git repository scan to Neo4j/MongoDB assertions.
