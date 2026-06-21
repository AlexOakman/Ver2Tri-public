# Project Description

Ver2Tri translates Vertica SQL into Trino SQL through a staged local workflow.

Core ideas:

- split complex SQL into smaller parts
- translate parts with DSPy
- catch forbidden Vertica-only patterns
- assemble a final Trino artifact
- keep detailed metadata for retries and diagnostics

The public repository uses a synthetic `golden_dataset/examples.json` so the compiler and repair helpers remain usable without shipping private warehouse examples.

Generated runtime artifacts are written under `workflow/` and compiled assets under `checkpoint/`. Both are local working directories and are not intended for Git history.
