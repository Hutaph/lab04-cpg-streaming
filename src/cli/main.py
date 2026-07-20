"""Command-Line Interface entrypoint using Typer."""

import typer

app = typer.Typer(help="CPG Stream Ingestion command line parser")


@app.command()
def scan(
    repo: str = typer.Option("transformers-pr-agent", help="Path to repository"),
    dry_run: bool = typer.Option(True, help="Dry run and write JSONL files locally"),
) -> None:
    """Scan repository incrementally and publish AST/CFG/DFG to Kafka/JSONL."""
    typer.echo(f"Scanning repository: {repo} (dry_run={dry_run})")
    typer.echo("TODO: Complete implementation in Phase 11")


@app.command()
def replay(
    file: str = typer.Option(..., help="Path to modified Python file"),
) -> None:
    """Replay a single modified file, updating Neo4j and MongoDB graph states."""
    typer.echo(f"Replaying file: {file}")
    typer.echo("TODO: Complete implementation in Phase 13")


if __name__ == "__main__":
    app()
