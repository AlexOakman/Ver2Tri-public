# File Function Notes

## `config.py`

Centralized settings loaded from `.env` with safe local defaults.

## `main.py`

Owns CLI commands, workflow bootstrap, retry entrypoints, and dashboard startup.

## `core/state_manager.py`

Persists `metadata.json`, stage diagnostics, and per-part state under `workflow/in_progress/<query_name>/`.

## `core/translator.py`

Loads rule text from `golden_dataset/`, resolves context for a SQL part, and invokes the compiled DSPy program.

## `core/pattern_guard.py`

Detects forbidden Vertica patterns and records repair context in metadata.

## `core/trino_tester.py`

Runs optional Trino runtime checks, gathers evidence, and saves guarded fixes.

## `dspy_modules/compiler.py`

Loads the public or private example dataset, compiles the DSPy module, and writes checkpoint artifacts.
