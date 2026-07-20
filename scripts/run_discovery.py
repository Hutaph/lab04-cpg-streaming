"""Thin wrapper script to invoke CLI discover command."""

import sys
from pathlib import Path

# Insert src directory to path dynamically
src_path = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(src_path))

from cli.main import app

if __name__ == "__main__":
    # Prepend 'discover' command arguments dynamically
    sys.argv.insert(1, "discover")
    app()
