# Project Consistency Audit

## 1. Executive Summary

- **Verdict**: **Consistent**
- **Summary**: Following the Clean Rewrite of Task 1 and Task 2, all core implementation logic, configurations, schemas, and documents are fully aligned with the canonical specifications. All 5 findings (including import path issues, static type warnings, and configuration inconsistencies) have been resolved. The test suite is fully functional with 100% pass rates, and the Jupyter Book builds successfully without structural errors.

## 2. Environment

- **Branch**: `chore/project-architecture-scaffold`
- **HEAD Commit**: `a571dae feat: complete clean rewrite of Task 1 and Task 2`
- **Working Tree Status**: Modified with minor consistency fixes (uncommitted as per audit instructions).
- **Python Version**: `3.10.12`
- **uv Version**: `0.11.2`
- **uv.lock**: Yes, generated and synchronized (`uv.lock` now exists at project root).
- **Source Repository Path**: `workspace/source/transformers-pr-agent`
- **Source Repository Commit SHA**: `458c957fa1e8851825cd799f5d030876f0644194`
- **Number of Tests**: 17 unit tests, all passing.
- **Jupyter Book Build Status**: Successful compilation (9 pages generated under `lab04-book/_build/html`).

## 3. Canonical Decisions

| Aspect / Decision | Configured / Implemented Value | Source of Truth |
|---|---|---|
| **Project Root** | `~/AI_Project/lab04-cpg-streaming` | System layout |
| **Source Repository Path** | `workspace/source/transformers-pr-agent` | `config/application.yaml` |
| **Source Root for Parse** | `workspace/source/transformers-pr-agent/src` | `config/application.yaml` |
| **Repository ID** | `huggingface/transformers-pr-agent` | `src/cli/main.py` |
| **Source Commit SHA** | `458c957fa1e8851825cd799f5d030876f0644194` | `config/application.yaml` |
| **Package Layout** | `src/` (domain, application, parsing, infrastructure, cli) | `pyproject.toml` |
| **Kafka Topics** | `cpg.nodes`, `cpg.edges`, `source.metadata`, `parser.errors`, `connector.errors` | `config/topics.yaml` |
| **Kafka Message Key** | `file_id` | `src/infrastructure/messaging/kafka_producer.py` |
| **Schema Version** | `"1.0"` (string) | `schemas/*.schema.json` |
| **Timestamp Field** | `event_time` (ISO 8601 UTC string) | `schemas/` and `domain/events.py` |
| **File ID** | Stable SHA256 of normalized path | `src/parsing/identifiers.py` |
| **Parser State DB** | `workspace/state/parser_state.sqlite3` | `config/application.yaml` |
| **Dry-run Directory** | `workspace/tmp/parser-output/` | `src/cli/main.py` |
| **Spark Checkpoint** | `workspace/checkpoints/` | `config/application.yaml` |
| **Moodle Submission** | `https://<github-username>.github.io/lab04-cpg-streaming/` | Jupyter Book index / README |

## 4. Findings

### FND-01: Packaging Import Path Mismatch
- **Severity**: Medium
- **Component**: Packaging & Imports
- **Evidence**: Python modules inside `src/` used absolute imports prefixed with `from src.xxx` (e.g. `from src.domain.enums import EventType`). Because `pyproject.toml` defines package discovery search as `where = ["src"]`, Setuptools installs `domain`, `application`, etc. at the root level of `site-packages`, making the `src` module namespace unreachable. Running the console wrapper `lab04` crashed with `ModuleNotFoundError: No module named 'src'`.
- **Impact**: Broke the CLI executable wrapper script inside the virtual environment.
- **Resolution**: Stripped the `src.` prefix from all import statements in both `src/` and `tests/` directories, aligning with python path structures when running through the CLI or specifying `PYTHONPATH=src`.
- **Status**: **Fixed**

### FND-02: Missing uv.lock File
- **Severity**: Low
- **Component**: Environment
- **Evidence**: Project lacked a lockfile (`uv.lock`) despite being configured with `pyproject.toml` dependencies.
- **Impact**: Missing reproducible lock constraints for installed dependencies.
- **Resolution**: Generated `uv.lock` by executing `uv lock` at the project root.
- **Status**: **Fixed**

### FND-03: Mypy and Ruff Quality Warnings
- **Severity**: Medium
- **Component**: Static Code Quality
- **Evidence**: Mypy reported 14 errors under strict mode (un-typed `dict` declarations, scope name nullability, signature mismatches in `AstBuilder`, and unused type ignore comments). Ruff flagged unused imports and dynamic script loading E402 warnings.
- **Impact**: Quality checks and type verification checks failed.
- **Resolution**:
  - Typed properties dictionaries to `dict[str, Any]`.
  - Updated `AstBuilder.build` signature to return `dict[int, CodeNode]` mappings.
  - Resolved nullable type errors for scope and AST path arguments inside DFG and CFG builders.
  - Removed unused type ignores and added `# noqa: E402` inline comments in scripts wrapper.
  - Excluded the syntax-broken test fixture file (`tests/fixtures/broken_syntax.py`) from Ruff linting using `pyproject.toml`.
- **Status**: **Fixed**

### FND-04: Outdated Topics and Config Duplications
- **Severity**: Low
- **Component**: Topics Configuration & Infrastructure
- **Evidence**: Outdated topics (`code.events.*`) and fields (`event_timestamp`) were documented inside `docs/AGENTS.md`. Duplicate shell scripts to create Kafka topics existed in `scripts/create_topics.sh` and `infra/kafka/create-topics.sh`.
- **Impact**: Confusing documentation specs and script redundancy.
- **Resolution**: Updated `docs/AGENTS.md` to reference the canonical topics and `event_time`. Made `scripts/create_topics.sh` a thin wrapper script delegating to the main implementation in `infra/kafka/create-topics.sh`.
- **Status**: **Fixed**

### FND-05: Inaccurate Legacy References in Docs and Book
- **Severity**: Low
- **Component**: Documentation & Notebooks
- **Evidence**: Outdated documentation text claiming prototype code is preserved in `scripts/parser-service/` was present in `lab04-book/task2_parser_service.ipynb`, `README.md`, `docs/project_structure.md`, and `docs/refactor_mapping.md`.
- **Impact**: Inaccurate and misleading file tree descriptions.
- **Resolution**: Corrected the texts to document the successful migration and cleanup of the old scripts.
- **Status**: **Fixed**

---

## 5. Code and Architecture Consistency
- **Domain Independence**: Fully verified. No domain modules depend on application, parsing, or infrastructure layers.
- **Adapter Decoupling**: Fully verified. Port structures in `src/application/ports.py` abstract Kafka connection and State Store operations. CLI behaves as the composition root.
- **Spark & Ingestion isolation**: Ingestion connectors and spark streaming configuration modules are scaffolded separately inside `infra/` and `spark_jobs/`.

## 6. Configuration Consistency
All variables including repository paths (`workspace/source/transformers-pr-agent`), schema versions (`"1.0"`), parser state sqlite file, and Kafka connection defaults match exactly across `config/application.yaml`, `.env.example`, `.env`, and `src/infrastructure/config/settings.py`.

## 7. Schema and Topic Consistency
Schemas defined under `schemas/` use string type `"1.0"` for `schema_version` and ISO 8601 UTC `event_time`. Topics mapped in code match those in `config/topics.yaml`:
- `cpg.nodes` (key: `file_id`)
- `cpg.edges` (key: `file_id`)
- `source.metadata` (key: `file_id`)
- `parser.errors` (key: `file_id`)
- `connector.errors` (key: `file_id`)

## 8. Task 1 Verification
Scanning and cloning target repository are functional. The command `uv run lab04 discover --scope final --manifest artifacts/manifests/source-files.jsonl` correctly filters the project and lists exactly **2779** eligible files for the target checkout version.

## 9. Task 2 Verification
Parsing AST, statement-level CFG sequence/branches, variables reaches definitions DFG, and local/import call graph resolution are fully verified through independent unit tests.

## 10. Test and Quality Results
- **Unit Tests**: 17 collected, 17 passed.
- **Ruff Linter**: Clean ("All checks passed!").
- **Ruff Formatter**: Clean (Formatting checked & applied).
- **Mypy strict check**: Success ("no issues found in 39 source files").

## 11. Jupyter Book Verification
MyST Book theme successfully compiles all 9 pages including task descriptions, architecture chapter, index, and reflections under `lab04-book/_build/html`.

## 12. GitHub Pages Verification
The deployment workflow `.github/workflows/deploy.yml` correctly targets building `lab04-book/` directory via `myst build --html --force` and uploads the built artifacts from `lab04-book/_build/html` to pages environment.

## 13. Submission Readiness
- Moodle link correctly targets `https://<github-username>.github.io/lab04-cpg-streaming/` (the published Jupyter Book site).
- Git repository contains all source code and book contents under a clean single public branch history.

## 14. Remaining Risks
- **No Neo4j/Kafka Live Connections**: The ingestion tests are validated via dry-run and mock ports. Real network integration latency or Cypher syntax changes may affect downstream streaming in subsequent tasks (Task 3 & 4).

## 15. Final Recommendation
- **Verdict**: **Ready to start Task 3**
- **Action**: All inconsistencies are resolved. The scaffolding and Task 1/2 implementation are complete, statically verified, and compile cleanly.
