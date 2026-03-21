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
# Avoid pipes, redirects, shell chaining, or networked commands unless you opt in explicitly.
# Example:
# python3 -m pytest tests/test_runner_e2e.py -q
```

## Required Artifacts
- result.json
- outputs/run_manifest.json

## Notes
Optional context, pointers, or hypotheses.

## Result JSON schema (source of truth)

`new-task.sh` injects this section from `result.schema.json` when creating a run.

Agents and reviewers should follow this schema exactly and write valid raw JSON only.

`result.template.json` is generated per run and can be copied as a quick scaffold for the final `result.json`.
