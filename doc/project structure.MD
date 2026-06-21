# Project Structure

## Main Code

- `main.py` - CLI workflow entrypoint
- `dashboard.py` - dashboard for local workflow inspection
- `config.py` - environment-backed settings

## Runtime Modules

- `core/pipeline.py` - runner contract
- `core/pipeline_stages.py` - concrete stage implementations
- `core/translator.py` - DSPy translation flow
- `core/pattern_guard.py` - forbidden pattern detection and repair loop
- `core/trino_tester.py` - optional runtime execution and repair tools
- `core/api_validation_*` - optional validation client and fixer

## Training and Rules

- `dspy_modules/` - DSPy signatures and compiler
- `golden_dataset/examples.json` - public-safe synthetic examples
- `golden_dataset/forbidden_patterns.json` - forbidden Vertica constructs
- `golden_dataset/*.MD` - prompt and rule files used by translation and repair

## Local Outputs

- `workflow/` - queue, progress, reports, final artifacts
- `checkpoint/` - compiled DSPy assets

These output directories are local-only and should not be committed.
