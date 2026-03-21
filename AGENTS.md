# Harness Contract

You are operating in a repository with a minimal pi-based harness.

## Workspace
- The task contract lives in `runs/<run-id>/task.md`.
- Working notes live in `runs/<run-id>/RUN.md`.
- Durable artifacts live in `runs/<run-id>/outputs/`.
- Final model report lives in `runs/<run-id>/result.json`.
- Harness-authored metadata lives in `runs/<run-id>/outputs/run_manifest.json` and `runs/<run-id>/score.json`.

## Default tool model
- Use only the default pi tools unless explicitly told otherwise: `read`, `write`, `edit`, `bash`.
- Keep working notes in `RUN.md` and write durable outputs under `runs/<run-id>/outputs/`.
- Run checks with `bash` commands provided by the task or from repo workflow.
- Write `runs/<run-id>/result.json` before finishing.
- `result.json` must conform exactly to the schema embedded in `task.md`; include `x-interface-version: "v1"`.
- Preserve state in files only; do not invent hidden orchestrator state.

## Boundaries
- Do not use sub-agents, planner/critic pipelines, or custom skills/extensions/themes.
- Do not rely on hidden memory, manager prompts, or workflow graphs.
- Do not introduce dangerous eval commands, network access, or wider HOME copies unless the task explicitly opts in.
- Do not change public contracts unless the task explicitly requires it.
- Prefer direct edits and external verification.
