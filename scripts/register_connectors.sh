#!/usr/bin/env bash
set -euo pipefail

exec uv run python "$(dirname "$0")/deploy_connectors.py" "$@"

