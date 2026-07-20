# Makefile for Lab 04 scaffolding commands

.PHONY: tree compile lint test docs-check

tree:
	@tree -a -I '.git|__pycache__|.ipynb_checkpoints|.pytest_cache'

compile:
	@echo "Compiling Python source codes..."
	python3 -m compileall -q src spark_jobs

lint:
	@echo "Running lint checks via ruff..."
	@if command -v ruff >/dev/null 2>&1; then \
		ruff check src spark_jobs; \
	else \
		echo "WARNING: ruff is not installed in the current environment. Please install dev dependencies."; \
	fi

test:
	@echo "Running unit tests..."
	@if command -v pytest >/dev/null 2>&1; then \
		pytest tests/unit; \
	else \
		echo "WARNING: pytest is not installed. Test suites skipped."; \
	fi

docs-check:
	@echo "Verifying Jupyter Book configuration..."
	@if command -v jupyter-book >/dev/null 2>&1; then \
		jupyter-book build lab04-book --dry-run; \
	else \
		echo "WARNING: jupyter-book is not installed. Cannot verify book build locally."; \
	fi
