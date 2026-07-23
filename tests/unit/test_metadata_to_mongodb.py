"""Unit tests for Spark metadata ingestion configuration."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))

from spark_jobs.metadata_to_mongodb import (  # noqa: E402
    JobConfig,
    build_mongodb_writer,
    parse_args,
)


def test_parse_args_uses_task5_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify defaults match the repository's Kafka, MongoDB, and checkpoint config."""
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.delenv("METADATA_TOPIC", raising=False)
    monkeypatch.delenv("MONGODB_URI", raising=False)
    monkeypatch.delenv("MONGODB_DATABASE", raising=False)
    monkeypatch.delenv("MONGODB_COLLECTION", raising=False)
    monkeypatch.delenv("SPARK_CHECKPOINT_PATH", raising=False)
    monkeypatch.delenv("KAFKA_STARTING_OFFSETS", raising=False)

    config = parse_args([])

    assert config.bootstrap_servers == "localhost:9092"
    assert config.metadata_topic == "source.metadata"
    assert config.mongodb_uri == "mongodb://localhost:27017"
    assert config.mongodb_database == "cpg_metadata"
    assert config.mongodb_collection == "file_statistics"
    assert config.checkpoint_dir == Path("workspace/checkpoints/spark")
    assert config.starting_offsets == "earliest"


def test_resolved_config_uses_absolute_checkpoint_path(tmp_path: Path) -> None:
    """Verify Spark receives an absolute checkpoint directory."""
    config = JobConfig(
        bootstrap_servers="kafka:29092",
        metadata_topic="source.metadata",
        mongodb_uri="mongodb://mongodb:27017",
        mongodb_database="cpg_metadata",
        mongodb_collection="file_statistics",
        checkpoint_dir=tmp_path / "checkpoints",
    )

    assert config.resolved().checkpoint_dir == (tmp_path / "checkpoints").resolve()


def test_parse_args_rejects_conflicting_trigger_options() -> None:
    """Verify the job does not configure two mutually exclusive triggers."""
    with pytest.raises(ValueError, match="cannot be used together"):
        parse_args(["--processing-time", "10 seconds", "--available-now"])


def test_mongodb_writer_configures_checkpoint_and_upsert() -> None:
    """Verify the writer options implement checkpointed file-level upserts."""

    class FakeWriter:
        def __init__(self) -> None:
            self.options: dict[str, str] = {}
            self.output_mode: str | None = None

        def format(self, name: str) -> "FakeWriter":
            self.options["format"] = name
            return self

        def option(self, name: str, value: str) -> "FakeWriter":
            self.options[name] = value
            return self

        def outputMode(self, mode: str) -> "FakeWriter":
            self.output_mode = mode
            return self

    class FakeStream:
        def __init__(self) -> None:
            self.writeStream = FakeWriter()

    config = JobConfig(
        bootstrap_servers="kafka:29092",
        metadata_topic="source.metadata",
        mongodb_uri="mongodb://mongodb:27017",
        mongodb_database="cpg_metadata",
        mongodb_collection="file_statistics",
        checkpoint_dir=Path("workspace/checkpoints/spark"),
    )
    writer = build_mongodb_writer(FakeStream(), config)

    assert writer.options["format"] == "mongodb"
    assert writer.options["checkpointLocation"] == str(config.checkpoint_dir)
    assert writer.options["spark.mongodb.operationType"] == "replace"
    assert writer.options["spark.mongodb.idFieldList"] == "file_id"
    assert writer.options["spark.mongodb.upsertDocument"] == "true"
    assert writer.output_mode == "append"
