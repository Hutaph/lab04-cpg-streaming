"""Preflight MongoDB authentication and metadata collection readiness."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from shutil import which
from urllib.parse import parse_qs, urlparse

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from infrastructure.config.mongodb import build_mongodb_uri


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_INDEXES = {"file_id_1", "repository_id_1_file_path_1"}


@dataclass(frozen=True)
class MongoConfig:
    """MongoDB runtime configuration without exposing secrets."""

    username: str
    password: str
    host: str
    container_host: str
    container_name: str
    auth_source: str
    database: str
    collection: str

    @property
    def masked_username(self) -> str:
        if len(self.username) <= 2:
            return self.username
        return f"{self.username[0]}***{self.username[-1]}"

    def host_uri(self) -> str:
        host, port = self.host.split(":", maxsplit=1)
        return build_mongodb_uri(self.username, self.password, host, int(port), self.auth_source)

    def container_uri(self) -> str:
        host, port = self.container_host.rsplit(":", maxsplit=1)
        return build_mongodb_uri(self.username, self.password, host, int(port), self.auth_source)


def load_env_file(path: Path) -> None:
    """Load .env values into the current process without printing them."""
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        os.environ.setdefault(name.strip(), value.strip().strip('"').strip("'"))


def load_config(args: argparse.Namespace | None = None) -> MongoConfig:
    """Resolve MongoDB config and verify that the URI matches root credentials."""
    load_env_file(PROJECT_ROOT / ".env")
    username = os.environ.get("MONGO_ROOT_USERNAME", "root")
    password = os.environ.get("MONGO_ROOT_PASSWORD", "")
    uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/?authSource=admin")
    parsed = urlparse(uri)
    host = parsed.netloc.rsplit("@", maxsplit=1)[-1] or "localhost:27018"
    if args and args.host_port:
        host = f"localhost:{args.host_port}"
    elif os.environ.get("MONGODB_HOST_PORT"):
        host = f"localhost:{os.environ['MONGODB_HOST_PORT']}"
    container_host = os.environ.get("MONGODB_CONTAINER_HOST", "cpg-mongodb-metadata")
    container_port = os.environ.get("MONGODB_CONTAINER_PORT", "27017")
    container_name = os.environ.get("MONGODB_CONTAINER_NAME", "cpg-mongodb-metadata")
    if args and args.container_host:
        container_host = args.container_host
    if args and args.container_port:
        container_port = str(args.container_port)
    if args and args.container_name:
        container_name = args.container_name
    query = parse_qs(parsed.query)
    auth_source = query.get("authSource", [""])[0]
    uri_username = parsed.username or ""
    database = os.environ.get("MONGODB_DATABASE", "cpg_metadata")
    collection = os.environ.get("MONGODB_COLLECTION", "file_statistics")

    if not password:
        raise ValueError("MONGO_ROOT_PASSWORD is not set.")
    if uri_username and uri_username != username:
        raise ValueError("MONGODB_URI username does not match MONGO_ROOT_USERNAME.")
    if auth_source != "admin":
        raise ValueError("MONGODB_URI must include authSource=admin.")
    return MongoConfig(
        username,
        password,
        host,
        f"{container_host}:{container_port}",
        container_name,
        auth_source,
        database,
        collection,
    )


def run_command(command: list[str], timeout: int = 30) -> subprocess.CompletedProcess[str]:
    """Run a command and capture output for PASS/FAIL decisions."""
    try:
        return subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as exc:
        return subprocess.CompletedProcess(command, 124, exc.stdout or "", exc.stderr or "Command timed out.")


def container_running(name: str) -> bool:
    """Return whether a Docker container is running."""
    result = run_command(["docker", "inspect", "-f", "{{.State.Running}}", name])
    return result.returncode == 0 and result.stdout.strip() == "true"


def docker_network_name(anchor_container: str = "cpg-kafka") -> str:
    """Resolve the Docker network used by the lab stack."""
    result = run_command(
        [
            "docker",
            "inspect",
            "-f",
            "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}",
            anchor_container,
        ]
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return "infra_default"


def mongosh_eval(container_name: str, uri: str, javascript: str) -> tuple[bool, str]:
    """Run mongosh from the selected MongoDB container without exposing credentials."""
    result = run_command(
        [
            "docker",
            "exec",
            container_name,
            "mongosh",
            uri,
            "--quiet",
            "--eval",
            javascript,
        ]
    )
    return result.returncode == 0, result.stdout.strip() or result.stderr.strip()


def host_mongosh_eval(uri: str, javascript: str) -> tuple[bool, str]:
    """Run mongosh against the host endpoint without exposing credentials."""
    if which("mongosh"):
        result = run_command(["mongosh", uri, "--quiet", "--eval", javascript])
    else:
        result = run_command(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "host",
                "mongo:6.0.4",
                "mongosh",
                uri,
                "--quiet",
                "--eval",
                javascript,
            ],
            timeout=60,
        )
    return result.returncode == 0, result.stdout.strip() or result.stderr.strip()


def check_indexes(config: MongoConfig) -> tuple[bool, set[str]]:
    """Verify that the metadata collection has the required unique indexes."""
    ok, output = mongosh_eval(
        config.container_name,
        config.container_uri(),
        (
            f"const dbh=db.getSiblingDB('{config.database}');"
            f"print(JSON.stringify(dbh.{config.collection}.getIndexes().map(i => i.name)));"
        ),
    )
    if not ok:
        return False, set()
    try:
        names = set(json.loads(output.splitlines()[-1]))
    except json.JSONDecodeError:
        return False, set()
    return REQUIRED_INDEXES.issubset(names), names


def print_status(name: str, passed: bool) -> None:
    """Print one stable status line."""
    status = "PASS" if passed else "FAIL"
    print(f"{name}: {status}")


def run_preflight() -> int:
    """Run all MongoDB preflight checks."""
    args = build_argument_parser().parse_args()
    try:
        config = load_config(args)
    except ValueError as exc:
        print(f"config: FAIL ({exc})")
        return 1

    print(f"host={config.host}")
    print(f"container_host={config.container_host}")
    print(f"container_name={config.container_name}")
    print(f"database={config.database}")
    print(f"collection={config.collection}")
    print(f"authSource={config.auth_source}")
    print(f"username={config.masked_username}")

    running = container_running(config.container_name)
    print_status("container_running", running)
    if not running:
        return 1

    host_auth, _ = host_mongosh_eval(config.host_uri(), "db.adminCommand({ ping: 1 });")
    print_status("host_authentication", host_auth)

    database_access, _ = host_mongosh_eval(config.host_uri(), f"db.getSiblingDB('{config.database}').stats();")
    print_status("database_accessible", database_access)

    collection_access, _ = host_mongosh_eval(
        config.host_uri(),
        f"db.getSiblingDB('{config.database}').{config.collection}.countDocuments({{}});",
    )
    print_status("collection_accessible", collection_access)

    indexes_ok, _ = check_indexes(config)
    print_status("required_indexes", indexes_ok)

    container_auth, _ = mongosh_eval(config.container_name, config.container_uri(), "db.adminCommand({ ping: 1 });")
    print_status("container_network_authentication", container_auth)

    spark_resolve = run_command(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            docker_network_name(),
            "busybox:1.36",
            "nslookup",
            config.container_host.split(":", maxsplit=1)[0],
        ],
        timeout=60,
    )
    print_status("spark_can_resolve_mongodb_host", spark_resolve.returncode == 0)

    checks = [host_auth, database_access, collection_access, indexes_ok, container_auth, spark_resolve.returncode == 0]
    return 0 if all(checks) else 1


def build_argument_parser() -> argparse.ArgumentParser:
    """Build CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host-port", help="Host-side MongoDB port, for example 27018.")
    parser.add_argument("--container-host", help="Docker-network MongoDB hostname.")
    parser.add_argument("--container-port", default=None, help="Docker-network MongoDB port.")
    parser.add_argument("--container-name", help="Docker container name used for mongosh.")
    return parser


def main() -> None:
    """Execute preflight checks."""
    sys.exit(run_preflight())


if __name__ == "__main__":
    main()
