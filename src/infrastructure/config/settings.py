"""Application configuration schema loaded from environments and YAML files."""

from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Placeholder application configuration schema."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Repository Configurations
    source_repository_url: str = "https://github.com/huggingface/transformers-pr-agent.git"
    source_repository_path: Path = Path("workspace/source/transformers-pr-agent")

    # Kafka Configurations
    kafka_bootstrap_servers: str = "localhost:9092"
    topic_nodes: str = "cpg.nodes"
    topic_edges: str = "cpg.edges"
    topic_metadata: str = "cpg.metadata"
    topic_errors: str = "cpg.errors"

    # SQLite State Store
    state_db_path: Path = Path("workspace/state/parser_state.db")

    # Version information
    schema_version: int = 1
    parser_version: str = "1.0.0"


def load_settings() -> Settings:
    """Helper method to instantiate current configuration settings."""
    return Settings()
