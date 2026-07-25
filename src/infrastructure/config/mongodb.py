"""MongoDB runtime endpoint helpers."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote


@dataclass(frozen=True, slots=True)
class MongoRuntimeEndpoint:
    """Canonical MongoDB endpoint description for host and container use."""

    host: str
    port: int

    @property
    def host_port(self) -> str:
        return f"{self.host}:{self.port}"


def build_mongodb_uri(
    username: str,
    password: str,
    host: str,
    port: int,
    auth_source: str = "admin",
) -> str:
    """Build a MongoDB URI with an encoded password and explicit host/port."""
    encoded_username = quote(username, safe="")
    encoded_password = quote(password, safe="")
    return f"mongodb://{encoded_username}:{encoded_password}@{host}:{port}/?authSource={auth_source}"


def mask_mongodb_uri(username: str, host: str, port: int, auth_source: str = "admin") -> str:
    """Build a redacted MongoDB URI for logs and notebook prints."""
    encoded_username = quote(username, safe="")
    return f"mongodb://{encoded_username}:***@{host}:{port}/?authSource={auth_source}"
