# Harness Contract

This repository is operated through a minimal pi-based harness.

## Workspace
- The task contract lives in `runs/<run-id>/task.md`
- Working notes live in `runs/<run-id>/RUN.md`
- Durable artifacts live in `runs/<run-id>/outputs/`
- Final machine-readable summary lives in `runs/<run-id>/result.json`
- Harness metadata lives in `runs/<run-id>/outputs/run_manifest.json` and `runs/<run-id>/score.json`

## Working style
- Use direct reasoning and direct tool use
- Prefer the default pi tools only: `read`, `write`, `edit`, `bash`
- Use `bash` to run tests and checks
- Keep notes in `RUN.md`
- Save any durable output in `outputs/`
- Write `result.json` before finishing
- Keep `result.json` schema-valid, raw JSON only, with `x-interface-version: "v1"`

## Boundaries
- Do not rely on hidden memory or hidden workflow state
- Do not create manager agents, critic agents, or elaborate workflows unless the task explicitly requires them
- Sub-agents are forbidden unless the run uses the v3 capability manifest path and the resolved `context/capability-manifest.json` explicitly allows the profile you want to use
- Even when sub-agents are allowed, use the manifest as a capability registry only; do not treat it as a workflow script
- Do not introduce new tools, skills, extensions, or prompts as part of normal task execution
- Do not add dangerous eval commands or networked behavior unless the task explicitly allows it
- Prefer simple file edits and external verification
