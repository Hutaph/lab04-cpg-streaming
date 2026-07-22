"""Stream source metadata events from Kafka into MongoDB with Spark."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


DEFAULT_METADATA_TOPIC = "source.metadata"
DEFAULT_CHECKPOINT_DIR = "workspace/checkpoints/spark"
DEFAULT_APP_NAME = "cpg-metadata-to-mongodb"


@dataclass(frozen=True)
class JobConfig:
    """Runtime configuration for the metadata ingestion query."""

    bootstrap_servers: str
    metadata_topic: str
    mongodb_uri: str
    mongodb_database: str
    mongodb_collection: str
    checkpoint_dir: Path
    spark_master: str | None = "local[*]"
    starting_offsets: str = "earliest"
    processing_time: str | None = None
    available_now: bool = False
    app_name: str = DEFAULT_APP_NAME

    def resolved(self) -> "JobConfig":
        """Return a copy with an absolute checkpoint path."""
        return JobConfig(
            bootstrap_servers=self.bootstrap_servers,
            metadata_topic=self.metadata_topic,
            mongodb_uri=self.mongodb_uri,
            mongodb_database=self.mongodb_database,
            mongodb_collection=self.mongodb_collection,
            checkpoint_dir=self.checkpoint_dir.expanduser().resolve(),
            spark_master=self.spark_master,
            starting_offsets=self.starting_offsets,
            processing_time=self.processing_time,
            available_now=self.available_now,
            app_name=self.app_name,
        )


def metadata_event_schema() -> Any:
    """Build the explicit schema for the metadata Kafka event contract."""
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    metadata_schema = StructType(
        [
            StructField("size_bytes", LongType(), nullable=False),
            StructField("line_count", LongType(), nullable=False),
            StructField("function_count", LongType(), nullable=False),
            StructField("class_count", LongType(), nullable=False),
            StructField("import_count", LongType(), nullable=False),
            StructField("node_count", LongType(), nullable=False),
            StructField("edge_count", LongType(), nullable=False),
            StructField("parse_duration_ms", LongType(), nullable=False),
            StructField("parse_status", StringType(), nullable=False),
            StructField("parser", StringType(), nullable=False),
        ]
    )
    return StructType(
        [
            StructField("schema_version", StringType(), nullable=False),
            StructField("event_id", StringType(), nullable=False),
            StructField("event_type", StringType(), nullable=False),
            StructField("event_time", StringType(), nullable=False),
            StructField("repository_id", StringType(), nullable=False),
            StructField("commit_sha", StringType(), nullable=False),
            StructField("file_id", StringType(), nullable=False),
            StructField("file_path", StringType(), nullable=False),
            StructField("content_hash", StringType(), nullable=False),
            StructField("parser_version", StringType(), nullable=False),
            StructField("metadata", metadata_schema, nullable=False),
        ]
    )


def build_metadata_stream(spark: Any, config: JobConfig) -> Any:
    """Read, validate, and flatten metadata events from the Kafka topic."""
    from pyspark.sql.functions import col, from_json

    raw_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.metadata_topic)
        .option("startingOffsets", config.starting_offsets)
        .load()
    )

    parsed_events = raw_events.select(from_json(col("value").cast("string"), metadata_event_schema()).alias("event"))
    valid_events = parsed_events.where(col("event").isNotNull() & (col("event.event_type") == "FILE_METADATA_UPSERT"))

    event_fields = [
        "schema_version",
        "event_id",
        "event_time",
        "repository_id",
        "commit_sha",
        "file_id",
        "file_path",
        "content_hash",
        "parser_version",
    ]
    metadata_fields = [
        "size_bytes",
        "line_count",
        "function_count",
        "class_count",
        "import_count",
        "node_count",
        "edge_count",
        "parse_duration_ms",
        "parse_status",
        "parser",
    ]
    return valid_events.select(
        *[col(f"event.{field}").alias(field) for field in event_fields],
        *[col(f"event.metadata.{field}").alias(field) for field in metadata_fields],
    )


def build_mongodb_writer(metadata_stream: Any, config: JobConfig) -> Any:
    """Create the MongoDB streaming writer with checkpointed idempotent upserts."""
    writer = (
        metadata_stream.writeStream.format("mongodb")
        .option("checkpointLocation", str(config.checkpoint_dir))
        .option("spark.mongodb.connection.uri", config.mongodb_uri)
        .option("spark.mongodb.database", config.mongodb_database)
        .option("spark.mongodb.collection", config.mongodb_collection)
        .option("spark.mongodb.operationType", "replace")
        .option("spark.mongodb.idFieldList", "file_id")
        .option("spark.mongodb.upsertDocument", "true")
        .outputMode("append")
    )
    if config.processing_time:
        writer = writer.trigger(processingTime=config.processing_time)
    elif config.available_now:
        writer = writer.trigger(availableNow=True)
    return writer


def create_spark_session(config: JobConfig) -> Any:
    """Create a Spark session without importing PySpark during module import."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(config.app_name)
    if config.spark_master:
        builder = builder.master(config.spark_master)
    return builder.getOrCreate()


def run(config: JobConfig) -> None:
    """Start the streaming query and wait until Spark stops it."""
    resolved_config = config.resolved()
    resolved_config.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    spark = create_spark_session(resolved_config)
    try:
        metadata_stream = build_metadata_stream(spark, resolved_config)
        query = build_mongodb_writer(metadata_stream, resolved_config).start()
        query.awaitTermination()
    finally:
        spark.stop()


def _env(name: str, fallback: str) -> str:
    """Return an environment variable value or its configured fallback."""
    return os.environ.get(name, fallback)


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the command-line parser used by spark-submit."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap server list.",
    )
    parser.add_argument(
        "--metadata-topic",
        default=_env("TOPIC_METADATA", DEFAULT_METADATA_TOPIC),
        help="Kafka topic containing FILE_METADATA_UPSERT events.",
    )
    parser.add_argument(
        "--mongodb-uri",
        default=_env("MONGODB_URI", "mongodb://localhost:27017"),
        help="MongoDB connection URI.",
    )
    parser.add_argument(
        "--mongodb-database",
        default=_env("MONGODB_DATABASE", "cpg_metadata"),
        help="MongoDB database name.",
    )
    parser.add_argument(
        "--mongodb-collection",
        default=_env("MONGODB_COLLECTION", "file_statistics"),
        help="MongoDB collection name.",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default=_env("SPARK_CHECKPOINT_PATH", DEFAULT_CHECKPOINT_DIR),
        help="Persistent Spark checkpoint directory.",
    )
    parser.add_argument(
        "--spark-master",
        default=_env("SPARK_MASTER", "local[*]"),
        help="Spark master URL; pass an empty value for cluster defaults.",
    )
    parser.add_argument(
        "--starting-offsets",
        default=_env("KAFKA_STARTING_OFFSETS", "earliest"),
        help="Kafka starting offsets used only when no checkpoint exists.",
    )
    parser.add_argument(
        "--processing-time",
        default=os.environ.get("SPARK_PROCESSING_TIME"),
        help="Optional Spark processing-time trigger, for example '10 seconds'.",
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process currently available Kafka data and then stop.",
    )
    parser.add_argument("--app-name", default=_env("SPARK_APP_NAME", DEFAULT_APP_NAME))
    return parser


def parse_args(argv: Sequence[str] | None = None) -> JobConfig:
    """Parse CLI arguments into a validated job configuration."""
    args = build_argument_parser().parse_args(argv)
    if args.processing_time and args.available_now:
        raise ValueError("--processing-time and --available-now cannot be used together")
    return JobConfig(
        bootstrap_servers=args.bootstrap_servers,
        metadata_topic=args.metadata_topic,
        mongodb_uri=args.mongodb_uri,
        mongodb_database=args.mongodb_database,
        mongodb_collection=args.mongodb_collection,
        checkpoint_dir=Path(args.checkpoint_dir),
        spark_master=args.spark_master or None,
        starting_offsets=args.starting_offsets,
        processing_time=args.processing_time,
        available_now=args.available_now,
        app_name=args.app_name,
    )


def main(argv: Sequence[str] | None = None) -> None:
    """Run the Kafka-to-MongoDB Spark Structured Streaming job."""
    run(parse_args(argv))


if __name__ == "__main__":
    main()
