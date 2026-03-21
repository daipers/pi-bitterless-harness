# Bitterless Harness on pi.dev
Version: 0.1
Status: buildable MVP

## 1. Objective

Build a minimal harness on top of pi that keeps the harness stupid and the model responsible for reasoning.

The harness must:
- provide a workspace
- expose only a tiny, general tool surface through pi
- persist tasks, working notes, transcripts, patches, eval logs, and scores as plain files
- run external evaluation commands outside the model
- remain usable through raw CLI first

The harness must not:
- impose planner/executor/critic pipelines
- depend on custom skills, manager agents, or workflow graphs
- hide state in databases or internal controller memory
- require RPC or SDK to achieve the core flow

## 2. Chosen pi integration surface

MVP uses the raw pi CLI in JSON event stream mode.

Why:
1. It preserves the raw terminal/file workflow.
2. It gives us streaming transcripts as JSONL.
3. It avoids building transport or session-control logic we do not need yet.

Later:
- use RPC mode only when we need follow-up steering or non-Node process control
- use the SDK only when we need in-process embedding

## 3. Core design

### 3.1 Single-run lifecycle

1. Create a run directory under `runs/<run-id>/`
2. Write `task.md`
3. Initialize `RUN.md`
4. Create `outputs/`
5. Launch pi from repo root with:
   - `AGENTS.md` in scope
   - isolated HOME to suppress hidden global instructions/packages
   - `--mode json`
   - `--no-extensions --no-skills --no-prompt-templates --no-themes`
6. Capture stdout JSONL to `transcript.jsonl`
7. Capture stderr to `pi.stderr.log`
8. After pi exits:
   - save `git status`
   - save `git diff --binary`
   - run eval commands from `task.md`
   - verify required artifacts
   - write `score.json`
9. The model writes `result.json` as its own final artifact
10. A human or another external evaluator inspects `result.json`, `score.json`, and artifacts

### 3.2 Run directory contract

```
runs/<run-id>/
├── task.md              # human task contract
├── RUN.md               # model scratchpad / run-local notes
├── result.json          # model-written final result summary
├── score.json           # harness-written external scoreboard
├── transcript.jsonl     # pi --mode json stdout
├── pi.stderr.log        # stderr from pi
├── git.status.txt       # repo status after run
├── patch.diff           # git diff --binary after run
├── outputs/             # durable artifacts from the model
├── home/                # isolated HOME for pi
└── session/             # pi session store for this run
```

## 4. File contracts

### 4.1 `AGENTS.md`
Stable project contract. It should include:
- repo conventions
- allowed tools and safety boundaries
- common commands
- required artifact locations
- eval expectations
- how to use `RUN.md` and `outputs/`

It must not contain:
- giant domain playbooks
- manager logic
- forced reasoning choreography
- required subagents
- hidden escalation policies

### 4.2 `task.md`
Plain markdown. No hidden database state.

Required sections:
- `# Task`
- `## Goal`
- `## Constraints`
- `## Done`
- `## Eval` with one fenced `bash` block; one command per non-comment line
- `## Required Artifacts`
- `## Notes` (optional)

Example:

```md
# Task
Fix flaky login integration test

## Goal
Make the login integration test pass reliably.

## Constraints
- Do not change public API shape
- Keep changes under 5 files unless necessary
- Save any investigation notes in RUN.md

## Done
- Root cause fixed
- Tests pass
- result.json written

## Eval
```bash
npm test -- login.integration.test.ts
npm run lint
```

## Required Artifacts
- result.json
- outputs/root-cause.md

## Notes
The failure appears only on CI-like timing.
```

### 4.3 `RUN.md`
Run-local working notes. Human-readable. Not machine-governed beyond existing as a file.

Suggested sections:
- status
- hypotheses
- files touched
- commands run
- unresolved risks
- final check list

### 4.4 `result.json`
Model-written final report.

Minimal schema:
```json
{
  "status": "success | partial | failed",
  "summary": "string",
  "artifacts": [
    { "path": "runs/<run-id>/outputs/file.ext", "description": "string" }
  ],
  "claims": [
    { "claim": "string", "evidence": ["path or command"] }
  ],
  "remaining_risks": ["string"]
}
```

### 4.5 `score.json`
Harness-written scoreboard.

Fields:
- `pi_exit_code`
- `result_json_present`
- `result_json_valid_minimal`
- `evaluations[]` with command, exit_code, duration, stdout_path, stderr_path
- `required_artifacts[]` with path and exists boolean
- `overall_pass`

## 5. Execution model

### 5.1 Harness prompt
The wrapper sends one task-oriented prompt to pi, for example:

> Complete the task in @runs/<run-id>/task.md. Use runs/<run-id>/RUN.md as your working notes. Save durable outputs under runs/<run-id>/outputs/. Run repo checks through bash before declaring success. Write runs/<run-id>/result.json before finishing.

That is all.
No planner role.
No critic role.
No manager role.

### 5.2 Tool model
Default pi tools only:
- read
- write
- edit
- bash

No custom tools in MVP.
No skills in MVP.
No extensions in MVP.
No prompt templates in MVP.
No themes in automation mode.

### 5.3 State model
The only durable run state is:
- repo files
- `runs/<run-id>/...`
- pi session JSONL files
- shell command outputs

There is no hidden orchestrator memory.

## 6. Isolation and reproducibility

### 6.1 Isolated HOME
Run pi with `HOME=runs/<run-id>/home`.

Reason:
- suppress accidental loading of global `AGENTS.md`
- suppress project-unrelated extensions, skills, prompts, and themes
- keep auth/config explicit

Auth choices:
1. preferred: API keys via env vars
2. optional: copy a specific `auth.json` into `home/.pi/agent/auth.json`

Do not copy full global settings.

### 6.2 Working directory
Run pi from repo root so project `AGENTS.md` and repo files are naturally available.

### 6.3 Session persistence
Use `--session-dir runs/<run-id>/session` so every run has local pi session files.

## 7. Evaluation model

### 7.1 External evals
The harness, not the model, runs eval commands from `task.md`.

Rules:
- run commands sequentially
- capture stdout/stderr per command
- record exit codes and durations
- do not let a failing command abort score generation

### 7.2 Artifact verification
The harness also checks every bullet under `## Required Artifacts`.

### 7.3 Human review
Humans inspect:
- `result.json`
- `score.json`
- `patch.diff`
- artifacts in `outputs/`

## 8. Interface

### 8.1 New task
```bash
bin/new-task.sh "short task title"
```

Creates:
- `runs/<timestamp>-<slug>/task.md`
- `runs/<timestamp>-<slug>/RUN.md`
- `runs/<timestamp>-<slug>/outputs/`

### 8.2 Execute task
```bash
bin/run-task.sh runs/<run-id> [model-pattern]
```

### 8.3 Outputs
On success or failure, the run directory is complete enough for inspection.

## 9. Non-goals for MVP

Do not add:
- RPC clients
- SDK embedding
- custom extensions
- custom skills
- permission popups
- sub-agents
- plan mode
- background job manager
- database-backed memory
- automatic retries beyond shell-level transient handling
- task router
- cross-run learning layer

## 10. Upgrade path

### Phase 0: raw CLI
Ship the exact MVP in this spec.

### Phase 1: thin viewer
Optional TUI/web viewer that reads `runs/*` files only.
No new control logic.

### Phase 2: RPC transport swap
If you need steering messages, replace `--mode json` with `--mode rpc`.
Keep the same file contracts and scoreboard.

### Phase 3: SDK transport swap
If you need in-process embedding in Node, replace subprocess launch with `createAgentSession`.
Keep the same contracts.

The invariant:
transport may change; file contracts and external evals stay stable.

## 11. Review checklist

A PR against this harness should be rejected unless:
- the new feature exposes new external capability rather than a reasoning policy
- the feature keeps raw CLI usage first-class
- the run remains explainable from files alone
- the feature helps stronger models, not just weaker ones
- the feature earns its keep on shared evals
- the feature does not smuggle orchestration through skills/extensions

## 12. Acceptance tests

1. **Cold start**
   - fresh repo
   - pi installed
   - API key present
   - `bin/new-task.sh` and `bin/run-task.sh` succeed

2. **Transcript durability**
   - `transcript.jsonl` is written
   - `pi.stderr.log` is written even on failure

3. **Artifact durability**
   - `patch.diff` and `git.status.txt` exist after every run

4. **Eval independence**
   - if the model claims success but tests fail, `score.json.overall_pass` is false

5. **Global-state isolation**
   - a user-global pi extension/skill does not affect the run because the harness uses isolated HOME and `--no-*` resource flags

6. **Transport replaceability**
   - `task.md`, `RUN.md`, `result.json`, `score.json`, and `outputs/` remain identical when switching to RPC or SDK

## 13. Suggested `AGENTS.md` contents for this harness

```md
# Harness Contract

You are operating in a repository with a minimal harness.

Rules:
- Use only the default pi tools unless explicitly told otherwise.
- Use `runs/<run-id>/RUN.md` for working notes.
- Save durable outputs under `runs/<run-id>/outputs/`.
- Run repo checks through `bash`.
- Before finishing, write `runs/<run-id>/result.json`.
- Do not invent hidden task state.
- Prefer simple edits and direct verification.
- Do not create sub-plans, manager agents, or helper workflows unless the task explicitly requires them.
```

## 14. Why this stays Bitter-Lesson-proof

The harness owns:
- setup
- files
- launch
- logs
- scorekeeping

Pi and the model own:
- interpretation
- planning
- search
- editing
- testing
- adaptation

That keeps intelligence in the model and verification in the world.
