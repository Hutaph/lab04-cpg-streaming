"""Command-Line Interface for CPG streaming application using Typer."""

import os
from pathlib import Path
from typing import Any, Optional
import typer
from infrastructure.config.settings import load_settings
from infrastructure.filesystem.git_source_repository import GitSourceRepository
from infrastructure.filesystem.manifest_writer import ManifestWriter
from infrastructure.messaging.event_validator import EventValidator
from infrastructure.messaging.jsonl_event_writer import JsonlEventWriter
from infrastructure.messaging.kafka_producer import KafkaEventProducer
from infrastructure.state.sqlite_state_store import SqliteStateStore
from parsing.cpg_parser import CpgParser
from application.services.discover_repository import DiscoverRepositoryService
from application.services.process_file import ProcessFileService
from application.services.process_repository import ProcessRepositoryService
from application.services.replay_file import ReplayFileService
from application.ports import EventWriterPort, EventPublisherPort
from domain.models import SourceFile

app = typer.Typer(help="CPG Stream Ingestion Pipeline CLI Commands")


def get_adapters(
    config_path: Optional[Path],
    dry_run: bool,
    out_dir: Optional[Path],
    bootstrap_servers: Optional[str],
    clean_output: bool,
) -> tuple[GitSourceRepository, SqliteStateStore, EventValidator, Any, str]:
    """Helper to initialize concrete adapters based on CLI flags."""
    settings = load_settings(config_path)

    # Overwrite settings dynamically via CLI flags
    if bootstrap_servers:
        settings.kafka.bootstrap_servers = bootstrap_servers

    repository_id = "huggingface/transformers-pr-agent"

    repo_adapter = GitSourceRepository(
        repo_path=settings.source_repository.path,
        clone_url=settings.source_repository.url,
        target_commit=settings.source_repository.commit,
    )

    state_store = SqliteStateStore(
        db_path=settings.state_db_path,
        repository_id=repository_id,
    )

    # schemas directory path resolve
    schemas_dir = Path("schemas")
    if not schemas_dir.exists():
        schemas_dir = Path("../schemas")
    validator = EventValidator(schemas_dir=schemas_dir)

    writer: EventWriterPort | EventPublisherPort
    if dry_run:
        target_out = out_dir if out_dir else Path("workspace/tmp/parser-output")
        writer = JsonlEventWriter(output_dir=target_out)
        if clean_output:
            writer.clean()
    else:
        writer = KafkaEventProducer(bootstrap_servers=settings.kafka.bootstrap_servers)

    return repo_adapter, state_store, validator, writer, repository_id


@app.command()
def clone_source(
    config: Optional[Path] = typer.Option(None, help="Path to config YAML file"),
) -> None:
    """Clones the target HuggingFace source repository shallowly."""
    typer.echo("Cloning target source repository...")
    settings = load_settings(config)
    repo_adapter = GitSourceRepository(
        repo_path=settings.source_repository.path,
        clone_url=settings.source_repository.url,
        target_commit=settings.source_repository.commit,
    )
    repo_adapter.clone_repository()
    typer.echo(f"Cloned successfully to: {settings.source_repository.path}")
    typer.echo(f"Commit SHA: {repo_adapter.get_commit_hash()}")


@app.command()
def discover(
    scope: str = typer.Option("final", help="File filter scope: raw, eligible, final, or smoke"),
    manifest: Path = typer.Option(
        Path("artifacts/manifests/source-files.jsonl"), help="Path to write manifest audit JSONL"
    ),
    config: Optional[Path] = typer.Option(None, help="Path to config YAML file"),
) -> None:
    """Scans repository files and logs files manifest JSONL."""
    typer.echo(f"Executing discovery phase (scope={scope}, manifest={manifest})...")
    os.environ["PARSER_SCOPE"] = scope
    settings = load_settings(config)

    repo_adapter = GitSourceRepository(
        repo_path=settings.source_repository.path,
        clone_url=settings.source_repository.url,
        target_commit=settings.source_repository.commit,
    )
    manifest_writer = ManifestWriter(manifest_file_path=manifest)

    service = DiscoverRepositoryService(
        repo_adapter=repo_adapter,
        manifest_writer=manifest_writer,
        repository_id="huggingface/transformers-pr-agent",
    )
    source_files = service.execute()

    discovered_count = len(source_files)
    typer.echo(f"Discovery phase completed. Eligible files: {discovered_count}")


@app.command()
def parse_file(
    file: Path = typer.Option(..., help="Path to target python file to parse relative to repo root"),
    dry_run: bool = typer.Option(True, help="Run in dry-run mode (write local JSONL)"),
    out_dir: Optional[Path] = typer.Option(None, help="Output directory for JSONL events"),
    bootstrap_servers: Optional[str] = typer.Option(None, help="Kafka bootstrap servers override"),
    clean_output: bool = typer.Option(False, help="Clean output directory before run"),
    config: Optional[Path] = typer.Option(None, help="Path to config YAML file"),
) -> None:
    """Parses a single file and pushes event notifications."""
    typer.echo(f"Parsing single file: {file}")
    repo_adapter, state_store, validator, writer, repo_id = get_adapters(
        config, dry_run, out_dir, bootstrap_servers, clean_output
    )

    parser = CpgParser(repository_id=repo_id)
    process_service = ProcessFileService(
        repo_adapter=repo_adapter,
        parser=parser,
        state_store=state_store,
        validator=validator,
        writer=writer,
    )

    abs_file = repo_adapter.resolve_path(file)
    if not abs_file.exists():
        typer.echo(f"Error: Target file {abs_file} does not exist.")
        raise typer.Exit(code=1)

    source_file = SourceFile(
        repository_id=repo_id,
        repository_root=str(repo_adapter.resolve_path(Path(""))),
        relative_path=file.as_posix(),
        commit_sha=repo_adapter.get_commit_hash(),
        size_bytes=abs_file.stat().st_size,
    )

    res = process_service.execute(source_file)
    typer.echo(f"File processed. Status: {res.status.value}, content_hash: {res.content_hash}")
    if res.error:
        typer.echo(f"Error details: {res.error}")
        raise typer.Exit(code=1)


@app.command()
def parse_repository(
    scope: str = typer.Option("smoke", help="Filter scope (smoke, eligible, or final)"),
    limit: Optional[int] = typer.Option(None, help="Limit maximum files to parse for demos"),
    dry_run: bool = typer.Option(True, help="Run in dry-run mode (write local JSONL)"),
    out_dir: Optional[Path] = typer.Option(None, help="Output directory for JSONL events"),
    bootstrap_servers: Optional[str] = typer.Option(None, help="Kafka bootstrap servers override"),
    clean_output: bool = typer.Option(False, help="Clean output directory before run"),
    config: Optional[Path] = typer.Option(None, help="Path to config YAML file"),
) -> None:
    """Scans and parses target repository source files incrementally."""
    typer.echo(f"Parsing repository (scope={scope}, limit={limit}, dry_run={dry_run})...")
    os.environ["PARSER_SCOPE"] = scope

    repo_adapter, state_store, validator, writer, repo_id = get_adapters(
        config, dry_run, out_dir, bootstrap_servers, clean_output
    )

    parser = CpgParser(repository_id=repo_id)
    process_file_service = ProcessFileService(
        repo_adapter=repo_adapter,
        parser=parser,
        state_store=state_store,
        validator=validator,
        writer=writer,
    )

    manifest_path = (
        Path("artifacts/manifests/source-files.jsonl")
        if scope in {"eligible", "final"}
        else Path(f"artifacts/manifests/source-files-{scope}.jsonl")
    )
    manifest_writer = ManifestWriter(manifest_path)
    discover_service = DiscoverRepositoryService(
        repo_adapter=repo_adapter,
        manifest_writer=manifest_writer,
        repository_id=repo_id,
    )

    repo_service = ProcessRepositoryService(
        discover_service=discover_service,
        process_file_service=process_file_service,
    )

    summary = repo_service.execute(limit=limit)
    typer.echo("Repository run completed. Summary:")
    typer.echo(summary)


@app.command()
def replay_file(
    file: Path = typer.Option(..., help="Path to replayed python file relative to repo root"),
    dry_run: bool = typer.Option(True, help="Run in dry-run mode (write local JSONL)"),
    out_dir: Optional[Path] = typer.Option(None, help="Output directory for JSONL events"),
    bootstrap_servers: Optional[str] = typer.Option(None, help="Kafka bootstrap servers override"),
    clean_output: bool = typer.Option(False, help="Clean output directory before run"),
    config: Optional[Path] = typer.Option(None, help="Path to config YAML file"),
) -> None:
    """Triggers replay logic on a single modified source file, producing DELETE/UPSERT events."""
    typer.echo(f"Replaying file updates: {file}")
    repo_adapter, state_store, validator, writer, repo_id = get_adapters(
        config, dry_run, out_dir, bootstrap_servers, clean_output
    )

    parser = CpgParser(repository_id=repo_id)
    process_file_service = ProcessFileService(
        repo_adapter=repo_adapter,
        parser=parser,
        state_store=state_store,
        validator=validator,
        writer=writer,
    )

    replay_service = ReplayFileService(
        repo_adapter=repo_adapter,
        parser=parser,
        state_store=state_store,
        process_file_service=process_file_service,
        repository_id=repo_id,
    )

    res = replay_service.execute(file)
    typer.echo("Replay file run completed. Results:")
    typer.echo(res)


if __name__ == "__main__":
    app()
