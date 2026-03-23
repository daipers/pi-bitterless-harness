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
- Do not use sub-agents, planner/critic pipelines, or custom skills/extensions/themes by default.
- Sub-agents are only allowed when the active `run.contract.json` explicitly opts into the v3 capability manifest and the resolved `context/capability-manifest.json` lists the profile being used.
- When sub-agents are allowed, treat the capability manifest as the source of truth; do not invent new roles, workflows, or helper pipelines.
- Do not rely on hidden memory, manager prompts, or workflow graphs.
- Do not introduce dangerous eval commands, network access, or wider HOME copies unless the task explicitly opts in.
- Do not change public contracts unless the task explicitly requires it.
- Prefer direct edits and external verification.

## Runtime Governance
- Treat new execution profiles and guardrail hooks as expensive; add them only when replay, benchmark, or canary evidence shows a material behavior change.
- Prefer typed policy or config changes over new runtime branching when a change is only a limit, threshold, or cap.
- Keep failure taxonomy entries aligned with `starter/governance/runtime-governance-v1.json`; do not add a new code unless operator action or promotion logic changes materially.
- Temporary runtime special cases must include an owner, removal condition, and evidence check in the governance registry and PR description.
