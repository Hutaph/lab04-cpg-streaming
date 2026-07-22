#!/usr/bin/env bash
set -euo pipefail

# Delegate execution to python drift detector script
exec uv run python "$(dirname "$0")/create_topics.py" "$@"
