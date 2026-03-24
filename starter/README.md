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
npm install -g @mariozechner/pi-coding-agent@$(cat PI_VERSION)
```

Install local dev/test tooling:

```bash
starter/bin/setup-dev-env.sh
```

This creates `.venv/` and installs the Python tooling that `starter/bin/preflight.sh`
expects. The ship gate automatically prefers `.venv/bin` when it exists, so you do not
need to activate the virtual environment first. `starter/bin/setup-dev-env.sh` now
fails fast if the supported Python `3.12.x` runtime is not available, rather than
creating a mismatched environment.

Supported local readiness workflow:

```bash
starter/bin/setup-dev-env.sh
starter/bin/check-supported-runtime.sh
.venv/bin/python -m pytest
starter/bin/preflight.sh
```

CI installs the full ship-gate toolchain, including `shellcheck`, `jq`, `trivy`, and the pinned `pi` version from `PI_VERSION`, before running `starter/bin/preflight.sh`.

## Architecture planes

The harness keeps the runtime path thin and moves most new capability into typed evidence and promotion checks:

- Runtime plane: execution, scoring, guardrails, retries, secret scanning, and artifact validation
- Evidence plane: run artifacts, replay corpora, canary summaries, provenance, and promotion bundles
- Learning plane: retrieval indexing, profile sweeps, hard-negative mining, and benchmark reports
- Promotion plane: runtime verification, benchmark thresholds, canary freshness, and release gates

## Runtime Governance

Runtime governance is codified, not implied:

- `governance/runtime-governance-v1.json` is the canonical registry for execution profiles, guardrail hooks, and the failure taxonomy.
- `contracts/runtime-governance-v1.schema.json` defines the typed contract for that registry.
- New profiles or hooks must come with benchmark, replay, or canary evidence showing a material effect.
- New failure codes must map to a distinct operator action or promotion impact; otherwise they should fold into an existing code.
- Temporary special cases must carry an owner, removal condition, and evidence check in the governance registry and PR template.

## Supported runtime policy

- Supported Python runtime for release candidates: `3.12.x` (`.python-version` pins CI to `3.12.9`)
- Supported `pi` CLI runtime for release candidates: [`PI_VERSION`](../PI_VERSION) currently `0.61.1`
- Node.js `22` is used in CI to install the supported `pi` CLI package

`starter/bin/check-supported-runtime.sh` is the source-of-truth verifier for the supported runtime policy.

## Required contract files

- `/AGENTS.md` (root harness contract)
- `task.template.md`
- `RUN.template.md`
- `result.schema.json`
- `contracts/run-contract-v1.schema.json`
- `contracts/run-contract-v2.schema.json`
- `contracts/run-contract-v3.schema.json`
- `contracts/run-manifest-v1.schema.json`
- `contracts/score-v1.schema.json`
- `contracts/context-manifest-v1.schema.json`
- `contracts/benchmark-report-v1.schema.json`
- `contracts/release-gate-v1.schema.json`
- `contracts/trajectory-record-v1.schema.json`
- `contracts/retrieval-example-v1.schema.json`
- `contracts/retrieval-document-v1.schema.json`
- `contracts/policy-example-v1.schema.json`
- `contracts/model-example-v1.schema.json`
- `contracts/candidate-manifest-v1.schema.json`
- `contracts/candidate-report-v1.schema.json`
- `contracts/capability-library-v1.schema.json`
- `contracts/runtime-governance-v1.schema.json`
- `governance/runtime-governance-v1.json`
- `policies/strict.json`
- `policies/capability.json`
- `library.yaml`
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

Optional profile selection:

```bash
bin/new-task.sh --profile capability "investigate prior successful runs"
bin/run-task.sh --profile capability runs/<run-id>
```

V2 defaults to the `strict` profile. `capability` keeps the same eval/network policy in this release, but adds explicit retrieval context materialization from prior successful runs.

## Command Center

Use the local command center when you want one terminal cockpit across multiple
Bitterless harness repos:

```bash
python3 starter/bin/control_center.py --check
python3 starter/bin/control_center.py
```

It reads the existing run files and queue JSONL logs, owns one orchestrator
process per configured repo, and exposes safe operator actions like start, stop,
restart, canary, runtime checks, cancel, enqueue, rerun, archive, and restore.

Configuration lives at `~/.config/bitterless/control-center.toml` by default.
Start from [`control-center.example.toml`](./control-center.example.toml) and see
[`docs/control-center.md`](./docs/control-center.md) for keybindings, sort/follow
controls, and command palette usage.

## Run directory contract

Each run creates:

- `task.md` (human contract)
- `RUN.md` (working notes)
- `run.contract.json` (versioned run contract snapshot with `run_contract_version: "v1"`)
- `result.json` (model-authored final result)
- `score.json` (harness-authored score)
- `run-events.jsonl` (structured lifecycle events conforming to `contracts/run-event-v1.schema.json`)
- `transcript.jsonl` (pi JSON event stream)
- `pi.stderr.log`
- `git.status.txt`
- `patch.diff`
- `pi.exit_code.txt`
- `result.template.json` (schema-compliant scaffold for copy/paste)
- `outputs/` (durable artifacts)
  - `outputs/run_manifest.json` (machine-readable manifest and audit trail)
  - `outputs/subagent-usage-validation.json` (pre-score capability audit for subagent-capable runs)
- `score/` (eval artifacts)
  - `score/eval-<n>.stdout.log`
  - `score/eval-<n>.stderr.log`
- `home/` (isolated run HOME)
- `session/` (pi session state)
- `context/` (capability-profile retrieval context, only when enabled)
  - `context/retrieval-manifest.json`
  - `context/retrieval-summary.md`
  - `context/capability-manifest.json` for v3 capability-manifest runs
  - `context/source-runs/<run-id>/retrieval-view.md`
  - `context/source-runs/<run-id>/outputs/...` for copied evidence files only

## Task format

Use `task.template.md` and keep these required sections:

- `## Eval` with a fenced ` ```bash ` block containing one command per non-comment line
- `## Eval` commands must be plain argv-style commands by default; wrapper forms like `bash -c`, `sh -c`, `python -c`, and `env ... python3 -c`, plus shell chaining, redirects, blocked programs, and networked commands, require explicit opt-in via env flags
- `## Required Artifacts` bullet list using relative paths that resolve inside the run directory
- `## Result JSON schema (source of truth)` section (auto-injected from `result.schema.json`)
- `result.template.json` is generated for each run from the same schema

Everything else is for humans and the model.

`parse_task.py` is the canonical parser and returns structured errors plus normalized eval command metadata.

## Execution profiles

- `strict` is the default profile for new V2 runs.
- `capability` adds explicit retrieval context under `context/` before launch.
- Existing V1 and V2 run directories still execute unchanged; the runner detects the contract version from `run.contract.json`.
- V3 adds an opt-in capability manifest and RPC transport selection for subagent-capable runs.

Profile precedence at runtime:

1. CLI `--profile`
2. `run.contract.json.execution_profile`
3. default `strict`

## Evaluation behavior

`bin/run-task.sh` always:

- Resolves run state as `new`, `running`, `partial`, or `complete`
- Probes dependencies (`pi`, `bash`, `python3`, `git`, `cat`) and validates write access before launch
- Validates `run.contract.json`, `task.md`, and `result.schema.json`
- Resolves the active execution profile and policy file
- Materializes retrieval context for V2 capability runs under `context/`
  - Reuses a derived retrieval index under `runs/.index/retrieval-v4/` when available
- Emits structured lifecycle events to `run-events.jsonl`
- Uses `trace_id == run_id` for every new event and `outputs/run_manifest.json.trace_id`
- Captures `transcript.jsonl` and `pi.stderr.log`
- Writes `pi.exit_code.txt`
- Captures `git.status.txt` and `patch.diff`
- Runs eval commands from `task.md` sequentially and records pass/fail, duration, and log paths
- Validates `result.json` against `result.schema.json`, canonicalizes JSON formatting, and records validation findings
- Rejects required artifact declarations that point outside the run directory
- Scans current-run evidence roots plus archived recovery evidence for likely secrets
- Captures schema snapshot metadata (`result_json_schema` and `result_schema_path|sha256|available`) in `score.json`
- Emits `outputs/run_manifest.json` with timings, dependency hashes, git SHA, invariants, audit flags, and failure classifications
- Writes `score.json` with `overall_pass`
- Records execution profile, policy path, and retrieval provenance in `score.json` and `outputs/run_manifest.json`

`run-events.jsonl` is append-only JSONL. Each line follows `contracts/run-event-v1.schema.json`; the required correlation fields are `ts`, `trace_id`, `run_id`, `phase`, and `message`.

`trace_id` is the stable per-run correlation key and is equal to the run directory name (`run_id`) for newly emitted telemetry. When `failure_classification` is present, emitters also preserve the legacy `failure_class` alias for compatibility.

Failure classifications currently emitted in `score.json`:

- `contract_invalid`
- `eval_failed`
- `model_invocation_failed`
- `result_invalid`

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

For broad production-readiness checks, point `HARNESS_PI_AUTH_JSON` at a minimal auth file used only for this harness or CI canary job.

## Feature Flags

- `HARNESS_STRICT_MODE=1` keeps strict contract and backpressure checks on. This is the default.
- `HARNESS_ALLOW_DANGEROUS_EVAL=1` allows eval commands that would otherwise be blocked by the default allowlist policy.
- `HARNESS_ALLOW_NETWORK_TASKS=1` allows eval commands that access the network, including wrapper payloads with literal network indicators.
- `HARNESS_FORCE_RERUN=1` reruns a `complete` run directory instead of returning its prior state.
- `HARNESS_MODEL_TIMEOUT_SECONDS=900` changes the bounded `pi` runtime timeout.
- `HARNESS_PI_RETRY_COUNT=2` changes the model startup retry budget.
- `HARNESS_EVAL_TIMEOUT_SECONDS=300` changes the timeout for each eval command.

## Retrieval Workflow

Rebuild the derived retrieval index at any time without modifying run artifacts:

```bash
python3 starter/bin/rebuild_retrieval_index.py
```

Run the retrieval benchmark corpus with the checked-in active profile:

```bash
python3 starter/bin/benchmark_harness.py --mode retrieval
```

Mine harder benchmark proposals from real passing runs into the review queue:

```bash
python3 starter/bin/mine_harder_retrieval_benchmarks.py
```

Sweep candidate retrieval profiles offline and optionally write the best profile:

```bash
python3 starter/bin/sweep_retrieval_profiles.py --write-best starter/retrieval/active_profile.json
```

Analyze benchmark snapshots, append history rows, and emit pruning suggestions:

```bash
python3 starter/bin/benchmark_harness.py --mode retrieval --out starter/runs/retrieval-latest.json
python3 starter/bin/analyze_retrieval_benchmarks.py starter/runs/retrieval-latest.json --history-dir starter/runs
```

Train a retrieval candidate from learning examples:

```bash
python3 starter/bin/train_retrieval_candidate.py \
  --examples starter/learning/latest/retrieval-examples.jsonl \
  --out starter/candidates/retrieval/active.json
```

Evaluate the retrieval candidate against the baseline and optionally promote it:

```bash
python3 starter/bin/evaluate_retrieval_candidate.py \
  --candidate starter/candidates/retrieval/active.json \
  --harness-root starter \
  --out starter/runs/retrieval-candidate-report.json \
  --promote-if-passed
```

Train a policy candidate from learning examples:

```bash
python3 starter/bin/train_policy_candidate.py \
  --examples starter/learning/latest/policy-examples.jsonl \
  --out starter/candidates/policy/active.json
```

Evaluate the policy candidate against replay and canary evidence and optionally promote it:

```bash
python3 starter/bin/evaluate_policy_candidate.py \
  --candidate starter/candidates/policy/active.json \
  --baseline-replay-report starter/runs/replay-baseline.json \
  --candidate-replay-report starter/runs/replay-candidate.json \
  --baseline-canary-summary-glob "starter/runs/real-canary-baseline-*.summary.json" \
  --candidate-canary-summary-glob "starter/runs/real-canary-policy-*.summary.json" \
  --out starter/runs/policy-candidate-report.json \
  --promote-if-passed
```

Build immutable learning datasets from run evidence:

```bash
python3 starter/bin/build_learning_datasets.py --runs-root starter/runs --out-root starter/learning/latest
```

This emits:

- `trajectory-records.jsonl`
- `retrieval-examples.jsonl`
- `retrieval-documents.jsonl`
- `policy-examples.jsonl`
- `model-examples.jsonl`

Convert a candidate manifest plus benchmark output into a typed promotion artifact:

```bash
python3 starter/bin/build_candidate_report.py \
  --candidate-type retrieval \
  --candidate starter/candidates/retrieval/active.json \
  --benchmark-report starter/runs/retrieval-latest.json \
  --out starter/runs/retrieval-candidate-latest.json
```

## Real `pi` coverage and canaries

Operator-controlled production readiness should be justified primarily by recent real-`pi` canary history and replayable production-like evidence, not fixture-only coverage.

Optional real-`pi` pytest coverage:

```bash
export HARNESS_RUN_REAL_PI_TESTS=1
export HARNESS_PI_AUTH_JSON=/absolute/path/to/auth.json
export HARNESS_REAL_PI_MODEL=anthropic/claude-sonnet-4
.venv/bin/python -m pytest tests/test_real_pi_integration.py -q
```

Lifecycle canary with the real CLI:

```bash
export HARNESS_PI_AUTH_JSON=/absolute/path/to/auth.json
export HARNESS_REAL_PI_MODEL=anthropic/claude-sonnet-4
python3 starter/bin/run_real_canary.py
```

The canary covers success, forced invalid `result.json`, timeout, interruption, retry, and partial-run recovery. Evidence is written under `starter/runs/`, and each canary summary now includes scenario rollups, `PI_VERSION`, model, commit SHA, and referenced run directories.

For a real policy-candidate promotion check, use [`.github/workflows/real-policy-candidate-canary.yml`](../../.github/workflows/real-policy-candidate-canary.yml). It runs a baseline real-`pi` canary, captures a replay corpus, reruns the canary and replay benchmark with a temporarily activated policy candidate manifest, and emits a typed `policy-candidate-report.json`.

Build a sanitized replay corpus from real run evidence:

```bash
python3 starter/bin/build_replay_corpus.py --runs-root starter/runs --out starter/benchmarks/replay-corpus.json
```

Run replay/load and generated fault-injection benchmarks:

```bash
python3 starter/bin/benchmark_harness.py --mode replay --replay-corpus starter/benchmarks/replay-corpus.json --history-dir starter/runs --out starter/runs/replay-latest.json
python3 starter/bin/benchmark_harness.py --mode fault-injection --fault-samples 6 --fault-seed 7 --fault-corpus-out starter/runs/fault-novel.json --out starter/runs/fault-latest.json
```

`benchmark_harness.py` now emits the `benchmark-report-v1` sidecar contract with threshold results and an `overall_pass` promotion signal.

## Release gate evidence

Verify canary freshness, benchmark thresholds, and release provenance as one typed artifact:

```bash
python3 starter/bin/verify_release_evidence.py \
  --summary-glob "starter/runs/real-canary-*.summary.json" \
  --benchmark-report starter/runs/retrieval-latest.json \
  --replay-report starter/runs/replay-latest.json \
  --fault-report starter/runs/fault-latest.json \
  --provenance-file dist/pi-bitterless-harness-$(cat VERSION).provenance.json \
  --min-runs 2 \
  --freshness-hours 36 \
  --out dist/pi-bitterless-harness-$(cat VERSION).release-gate.json
```

If `HARNESS_BENCHMARK_REPORT` and `HARNESS_CANARY_SUMMARY_GLOB` are set, `starter/bin/build-release-artifacts.sh` also emits:

- `dist/<artifact>.release-gate.json`
- `dist/<artifact>.promotion-bundle.json`

Set `HARNESS_REPLAY_REPORT` and `HARNESS_FAULT_REPORT` as well when building a promotion-ready bundle so replay and fault-injection evidence are attached to the same release package.
Set `HARNESS_POLICY_CANDIDATE_REPORT` as well when an active policy candidate is part of the promotion proof.

For automation-facing failure handling, use `outputs/run_manifest.json.primary_error_code` and `outputs/run_manifest.json.failure_classifications`. The legacy aggregate `error_code` remains for compatibility, and the raw evidence in `score.json`, `run-events.jsonl`, `transcript.jsonl`, and `pi.stderr.log` remains the richer source of truth.

## Release Gate

Use the ship gate before tagging a release:

```bash
starter/bin/setup-dev-env.sh
starter/bin/check-supported-runtime.sh
.venv/bin/python -m pytest
starter/bin/preflight.sh
HARNESS_BENCHMARK_REPORT=starter/runs/retrieval-latest.json \
HARNESS_REPLAY_REPORT=starter/runs/replay-latest.json \
HARNESS_FAULT_REPORT=starter/runs/fault-latest.json \
HARNESS_POLICY_CANDIDATE_REPORT=starter/runs/policy-candidate-report.json \
HARNESS_CANARY_SUMMARY_GLOB="starter/runs/real-canary-*.summary.json" \
starter/bin/build-release-artifacts.sh
```

CI enforces:

- lint + static checks
- contract validation
- unit, property, integration, and e2e scenarios
- security scans
- release artifact validation
- changelog/version consistency
- supported Python + `pi` runtime verification
- trusted `main` pushes must pass auth-backed real-`pi` tests plus a fresh canary summary
- `v*` tag promotion must pass `starter/bin/verify_release_evidence.py` against recent successful `main` canary artifacts

See the operator-facing runbook at [docs/operator-runbook.md](./docs/operator-runbook.md) for install, auth, retention, recovery, and canary procedures.

This README is the canonical release readiness document for v1.

## Script permissions

Before first use, ensure shell scripts are executable:

```bash
chmod +x bin/new-task.sh bin/run-task.sh bin/check-run-contract.sh
``` 
