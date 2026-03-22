# Task
Describe the task in one line.

## Goal
Describe the desired outcome in plain language.

## Constraints
- Add explicit limits here
- Mention any files or APIs that must not change
- Mention time or scope constraints if they matter

## Done
- Concrete completion criteria
- External checks should pass
- `result.json` should be written
- `result.json` must validate against the schema below
- `outputs/run_manifest.json` should report `overall_pass: true`

## Eval
```bash
# One command per non-comment line
# Use plain argv-style commands only.
# Wrapper forms like `bash -c`, `sh -c`, `python -c`, and `env ... python3 -c` require dangerous-eval opt-in.
# Avoid pipes, redirects, shell chaining, or networked commands unless you opt in explicitly.
# Example:
# python3 -m pytest tests/test_runner_e2e.py -q
```

## Required Artifacts
# Required artifacts must be relative paths that stay inside this run directory.
- result.json
- outputs/run_manifest.json

## Notes
Optional context, pointers, or hypotheses.

## Retrieval Quality Rubric
- `summary`: write 1-3 outcome-focused sentences with concrete identifiers, outputs, or checks; do not just restate the task title.
- `claims`: keep each claim atomic and specific; include only supported outcomes and cite evidence paths or exact verification commands.
- `artifacts[].description`: describe what the artifact proves or contains, not just the filename.

## Result JSON schema (source of truth)

`new-task.sh` injects this section from `result.schema.json` when creating a run.

Agents and reviewers should follow this schema exactly and write valid raw JSON only.

`result.template.json` is generated per run and can be copied as a quick scaffold for the final `result.json`.
