# Bitterless Harness v1

This is a deterministic, file-based harness for running model-assisted tasks through raw `pi` CLI JSON mode while enforcing versioned contracts, machine-readable manifests, security gates, and CI ship checks.

## Prerequisites

- `pi` installed and on `PATH`
- Python 3.12 and `bash`
- Python tooling from `requirements-dev.txt` for the full ship gate
- `shellcheck`, `jq`, and `trivy` for the full local ship gate
- run from the repository root (or any path with `pi-bitterless-harness/starter` as a child)
- API keys exported in environment variables, or:
  - `HARNESS_PI_AUTH_JSON=/absolute/path/to/auth.json` to copy auth into the isolated run HOME

Install pi:

```bash
npm install -g @mariozechner/pi-coding-agent
```

Install local dev/test tooling:

```bash
starter/bin/setup-dev-env.sh
```

This creates `.venv/` and installs the Python tooling that `starter/bin/preflight.sh`
expects. The ship gate automatically prefers `.venv/bin` when it exists, so you do not
need to activate the virtual environment first.

CI installs the full ship-gate toolchain, including `shellcheck`, `jq`, and `trivy`, before running `starter/bin/preflight.sh`.

## Required contract files

- `/AGENTS.md` (root harness contract)
- `task.template.md`
- `RUN.template.md`
- `result.schema.json`
- `contracts/run-contract-v1.schema.json`
- `bin/check-run-contract.sh` (must be executable)

## Quick start

1. Install/prepare dependencies.
2. Create a task:

```bash
bin/new-task.sh "fix flaky login test"
```

3. Edit `runs/<run-id>/task.md`.
4. Gate before launch:

```bash
bin/check-run-contract.sh runs/<run-id>
```

5. Run the task:

```bash
bin/run-task.sh runs/<run-id>
```

The gate command must pass before launching `run-task.sh`.

Optional model override:

```bash
bin/run-task.sh runs/<run-id> anthropic/claude-sonnet-4
```

## Run directory contract

Each run creates:

- `task.md` (human contract)
- `RUN.md` (working notes)
- `run.contract.json` (versioned run contract snapshot with `run_contract_version: "v1"`)
- `result.json` (model-authored final result)
- `score.json` (harness-authored score)
- `run-events.jsonl` (structured lifecycle events)
- `transcript.jsonl` (pi JSON event stream)
- `pi.stderr.log`
- `git.status.txt`
- `patch.diff`
- `pi.exit_code.txt`
- `result.template.json` (schema-compliant scaffold for copy/paste)
- `outputs/` (durable artifacts)
  - `outputs/run_manifest.json` (machine-readable manifest and audit trail)
- `score/` (eval artifacts)
  - `score/eval-<n>.stdout.log`
  - `score/eval-<n>.stderr.log`
- `home/` (isolated run HOME)
- `session/` (pi session state)

## Task format

Use `task.template.md` and keep these required sections:

- `## Eval` with a fenced ` ```bash ` block containing one command per non-comment line
- `## Eval` commands must be plain argv-style commands by default; shell chaining, redirects, blocked programs, and networked commands require explicit opt-in via env flags
- `## Required Artifacts` bullet list
- `## Result JSON schema (source of truth)` section (auto-injected from `result.schema.json`)
- `result.template.json` is generated for each run from the same schema

Everything else is for humans and the model.

`parse_task.py` is the canonical parser and returns structured errors plus normalized eval command metadata.

## Evaluation behavior

`bin/run-task.sh` always:

- Resolves run state as `new`, `running`, `partial`, or `complete`
- Probes dependencies (`pi`, `bash`, `python3`, `git`, `cat`) and validates write access before launch
- Validates `run.contract.json`, `task.md`, and `result.schema.json`
- Emits structured lifecycle events to `run-events.jsonl`
- Captures `transcript.jsonl` and `pi.stderr.log`
- Writes `pi.exit_code.txt`
- Captures `git.status.txt` and `patch.diff`
- Runs eval commands from `task.md` sequentially and records pass/fail, duration, and log paths
- Validates `result.json` against `result.schema.json`, canonicalizes JSON formatting, and records validation findings
- Scans run artifacts for likely secrets
- Captures schema snapshot metadata (`result_json_schema` and `result_schema_path|sha256|available`) in `score.json`
- Emits `outputs/run_manifest.json` with timings, dependency hashes, git SHA, invariants, audit flags, and failure classifications
- Writes `score.json` with `overall_pass`

### Result JSON examples

Passing example:

```json
{
  "x-interface-version": "v1",
  "status": "success",
  "summary": "Implemented requested change and added checks.",
  "artifacts": [
    {
      "path": "outputs/claim.txt",
      "description": "Proof of completed work."
    }
  ],
  "claims": [
    {
      "claim": "Added schema-valid result validation checks.",
      "evidence": ["score/eval-1.stdout.log"]
    }
  ],
  "remaining_risks": []
}
```

Failing example:

```json
{
  "x-interface-version": "v1",
  "status": "done",
  "summary": "",
  "artifacts": "outputs/claim.txt",
  "claims": [
    {
      "claim": "",
      "evidence": "score/eval-1.stdout.log"
    }
  ],
  "remaining_risks": "none"
}
```

The failing example fails status enum checks, summary requirement, `artifacts` array requirement, `claims[0].evidence` array requirement, and `remaining_risks` array requirement.

## Result contract

The model writes `result.json`.
The harness writes `score.json`.
Do not merge them.

## Auth file policy

This harness intentionally avoids copying full global pi settings. It creates an isolated `HOME` and only copies a specific file when `HARNESS_PI_AUTH_JSON` is set.

## Feature Flags

- `HARNESS_STRICT_MODE=1` keeps strict contract and backpressure checks on. This is the default.
- `HARNESS_ALLOW_DANGEROUS_EVAL=1` allows eval commands that would otherwise be blocked by the default allowlist policy.
- `HARNESS_ALLOW_NETWORK_TASKS=1` allows eval commands that access the network.
- `HARNESS_FORCE_RERUN=1` reruns a `complete` run directory instead of returning its prior state.
- `HARNESS_MODEL_TIMEOUT_SECONDS=900` changes the bounded `pi` runtime timeout.

## Release Gate

Use the ship gate before tagging a release:

```bash
starter/bin/preflight.sh
python3 -m pytest
starter/bin/build-release-artifacts.sh
```

CI enforces:

- lint + static checks
- contract validation
- unit, property, integration, and e2e scenarios
- security scans
- release artifact validation
- changelog/version consistency

This README is the canonical release readiness document for v1.

## Script permissions

Before first use, ensure shell scripts are executable:

```bash
chmod +x bin/new-task.sh bin/run-task.sh bin/check-run-contract.sh
``` 
