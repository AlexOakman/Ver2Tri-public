# Ver2Tri

Ver2Tri is a local migration pipeline for converting Vertica SQL into Trino SQL with DSPy and a stage-based workflow runner. The project is designed for iterative translation, diagnostics, and repair in a local development environment.

This public version is sanitized for open-source sharing:

- training examples in `golden_dataset/` are synthetic
- local workflow outputs and checkpoints stay outside Git
- all environment defaults use localhost or neutral placeholders
- richer private examples can be supplied through a local override path

## What It Does

For each SQL file in `workflow/in_queue/`, the pipeline can:

1. split a large Vertica script into ordered parts
2. translate each part with a compiled DSPy module
3. detect forbidden Vertica-specific patterns
4. format and assemble final Trino SQL
5. optionally run a generic SQL validation service
6. optionally run runtime checks against Trino
7. optionally compare produced output with a reference dataset
8. write diagnostics, reports, and retry metadata under `workflow/`

The default runtime is local-safe. Validation and runtime execution are opt-in.

## Repository Layout

- `main.py` - CLI entrypoint and workflow lifecycle
- `dashboard.py` - Streamlit dashboard for workflow inspection
- `core/` - pipeline stages, translation helpers, validation, runtime testing
- `dspy_modules/` - DSPy signatures and compiler
- `golden_dataset/` - public-safe synthetic examples, rule prompts, forbidden patterns
- `tests/` - regression and sanitization tests
- `doc/` - project notes and structure overview
- `run.md` - common local commands

## Requirements

- Python 3.10+
- dependencies from `pyproject.toml`
- an OpenAI-compatible endpoint for translation or repair flows
- `sqlfluff` available if SQL formatting is enabled
- optional Trino access for runtime checks
- optional HTTP endpoint for SQL validation

## Environment

Main runtime settings live in `config.py` and can be overridden through `.env`.

Common variables:

- `LLM_BASE_URL`
- `LLM_MODEL`
- `LLM_API_KEY`
- `LLM_TIMEOUT`
- `LLM_MAX_TOKENS`
- `ENABLE_SQL_FORMATTER`
- `ENABLE_API_VALIDATION`
- `API_VALIDATOR_URL`
- `ENABLE_TRINO_TEST`
- `ENABLE_COMPARE`
- `TRINO_HOST`
- `TRINO_PORT`
- `TRINO_USER`
- `TRINO_PASSWORD`
- `TRINO_CATALOG`
- `TRINO_SCHEMA`
- `TRINO_TEST_SCHEMA`
- `WORKFLOW_BASE_PATH`
- `CHECKPOINT_PATH`
- `PRIVATE_GOLDEN_DATASET_PATH`

`PRIVATE_GOLDEN_DATASET_PATH` is optional. When set, the compiler and runtime helpers will read examples from that local path instead of the public `golden_dataset/` directory. This path is meant for private experiments and should remain ignored by Git.

If `.env` is missing, defaults from `config.py` are used.

## Installation

```bash
python -m venv .ver2tri
source .ver2tri/bin/activate
pip install -e .
```

## Compile The DSPy Module

The translation flow expects a compiled DSPy module in `checkpoint/`.

```bash
python -m dspy_modules.compiler
```

Forced recompilation:

```bash
python -m dspy_modules.compiler --force -n 30 -c 20 --bootstrapped-demos 6 --labeled-demos 6 --minibatch-size 22
```

Expected outputs:

- `checkpoint/compiled_module.pkl`
- `checkpoint/compiled_module.json`
- optionally `checkpoint/compiled_module_full/`

## Run The Migrator

```bash
python main.py
```

Useful options:

- `python main.py --list`
- `python main.py --file sample_query`
- `python main.py --retry sample_query`
- `python main.py --retry sample_query --retry-from pattern_guard`
- `python main.py --reset sample_query`
- `python main.py --reset-in-progress-states`
- `python main.py --no-dashboard`

See [run.md](run.md) for a compact command list.

## Workflow Artifacts

For each query, `workflow/in_progress/<query_name>/` can contain:

- `metadata.json`
- copied source SQL
- `vertica_parts/`
- `trino_parts/`
- `final/<query_name>_final.sql`
- `logs/`
- `reports/`

Final outputs are copied into `workflow/done/` or `workflow/review/`.

These directories are local working data and are ignored by Git.

## Public Dataset Policy

`golden_dataset/` in this repository is intentionally synthetic. It preserves SQL transformation patterns without shipping private business vocabulary or internal warehouse examples.

If you need richer local examples:

1. create a private dataset outside the public fixture set
2. point `PRIVATE_GOLDEN_DATASET_PATH` to that directory
3. keep that directory ignored by Git

## Sanitization Guardrails

This repository includes a sanitization test suite that checks for:

- home-directory paths
- private per-user schema naming patterns
- tracked editor or OS junk
- explicit corporate host or product markers

Run the guard together with the normal test suite before publishing.

## Current Limitations

- translation quality depends on the compiled DSPy module and the synthetic example set
- SQL splitting is statement-based and may still require review for tightly coupled scripts
- API validation and runtime checks are optional and environment-dependent
- the public synthetic examples are safer to share, but may be weaker than a richer private corpus
