"""Application settings loader utilizing Pydantic Settings and PyYAML."""

import os
from pathlib import Path
from typing import Any
import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseModel):
    """General application settings."""

    name: str = "cpg-streaming-service"
    environment: str = "development"
    log_level: str = "INFO"


class RepoConfig(BaseModel):
    """Source code repository target config."""

    url: str = "https://github.com/huggingface/transformers-pr-agent.git"
    path: Path = Path("workspace/source/transformers-pr-agent")
    commit: str = "458c957fa1e8851825cd799f5d030876f0644194"


class ParserConfig(BaseModel):
    """CPG parsing logic settings."""

    version: str = "1.0.0"
    schema_version: str = "1.0"
    type: str = "python.ast"


class KafkaConfig(BaseModel):
    """Apache Kafka Connection parameters."""

    bootstrap_servers: str = "localhost:9092"


class Neo4jConfig(BaseModel):
    """Neo4j Database connection parameters."""

    uri: str = "bolt://localhost:7687"
    username: str = "neo4j"
    password: str = "TODO_NEO4J_PASSWORD"


class MongodbConfig(BaseModel):
    """MongoDB Database connection parameters."""

    uri: str = "mongodb://localhost:27017"
    database: str = "cpg_metadata"
    collection: str = "file_statistics"


class SparkConfig(BaseModel):
    """Apache Spark structured streaming configurations."""

    master: str = "local[*]"
    checkpoint_dir: str = "workspace/checkpoints/spark"


class Settings(BaseSettings):
    """Root settings configuration class merging environment variables and YAML profiles."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    # Sub-configs nested to match application.yaml format
    application: AppConfig = Field(default_factory=AppConfig)
    source_repository: RepoConfig = Field(default_factory=RepoConfig)
    parser: ParserConfig = Field(default_factory=ParserConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    neo4j: Neo4jConfig = Field(default_factory=Neo4jConfig)
    mongodb: MongodbConfig = Field(default_factory=MongodbConfig)
    spark: SparkConfig = Field(default_factory=SparkConfig)

    # SQLite state database path
    state_db_path: Path = Path("workspace/state/parser_state.sqlite3")


def load_settings(config_path: Path | None = None) -> Settings:
    """Loads configuration by reading application.yaml and binding environment overrides."""
    if config_path is None:
        # Search application.yaml from standard directories
        possible_paths = [
            Path("config/application.yaml"),
            Path("../config/application.yaml"),
            Path("application.yaml"),
        ]
        for p in possible_paths:
            if p.exists():
                config_path = p
                break

    yaml_data: dict[str, Any] = {}
    if config_path and config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f) or {}

    # Map flat environment variables to nested structure if set
    # e.g., KAFKA_BOOTSTRAP_SERVERS -> kafka: {bootstrap_servers: ...}
    kafka_srv = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if kafka_srv:
        yaml_data.setdefault("kafka", {})["bootstrap_servers"] = kafka_srv

    neo4j_uri = os.getenv("NEO4J_URI")
    if neo4j_uri:
        yaml_data.setdefault("neo4j", {})["uri"] = neo4j_uri

    neo4j_user = os.getenv("NEO4J_USERNAME")
    if neo4j_user:
        yaml_data.setdefault("neo4j", {})["username"] = neo4j_user

    neo4j_pass = os.getenv("NEO4J_PASSWORD")
    if neo4j_pass:
        yaml_data.setdefault("neo4j", {})["password"] = neo4j_pass

    mongo_uri = os.getenv("MONGODB_URI")
    if mongo_uri:
        yaml_data.setdefault("mongodb", {})["uri"] = mongo_uri

    mongo_db = os.getenv("MONGODB_DATABASE")
    if mongo_db:
        yaml_data.setdefault("mongodb", {})["database"] = mongo_db

    mongo_coll = os.getenv("MONGODB_COLLECTION")
    if mongo_coll:
        yaml_data.setdefault("mongodb", {})["collection"] = mongo_coll

    spark_cp = os.getenv("SPARK_CHECKPOINT_PATH")
    if spark_cp:
        yaml_data.setdefault("spark", {})["checkpoint_dir"] = spark_cp

    state_db = os.getenv("PARSER_STATE_DB")
    if state_db:
        yaml_data["state_db_path"] = state_db

    repo_url = os.getenv("SOURCE_REPOSITORY_URL")
    if repo_url:
        yaml_data.setdefault("source_repository", {})["url"] = repo_url

    repo_path = os.getenv("SOURCE_REPOSITORY_PATH")
    if repo_path:
        yaml_data.setdefault("source_repository", {})["path"] = repo_path

    return Settings(**yaml_data)
