# Bitterless Harness Operator Runbook

This runbook is the operator-facing contract for broad production use of Bitterless Harness.

## Supported versions

- Python release-candidate runtime: `3.12.x`
- CI lock file: [`.python-version`](../../.python-version)
- Supported `pi` CLI release-candidate runtime: [`PI_VERSION`](../../PI_VERSION)
- CI install command: `npm install -g @mariozechner/pi-coding-agent@$(cat PI_VERSION)`

Verify the runtime before a release candidate:

```bash
starter/bin/check-supported-runtime.sh
```

## Install and auth

1. Install the pinned `pi` version from `PI_VERSION`.
2. Run `starter/bin/setup-dev-env.sh`.
3. Provide auth through `HARNESS_PI_AUTH_JSON=/absolute/path/to/auth.json`.
4. Keep the auth file scoped to this harness or CI canary job. Do not copy an entire home directory into runs.

## Release-candidate gate

Run these commands from the repository root:

```bash
starter/bin/setup-dev-env.sh
starter/bin/check-supported-runtime.sh
.venv/bin/python -m pytest
starter/bin/preflight.sh
starter/bin/build-release-artifacts.sh
```

For operator-controlled production readiness, also run:

```bash
export HARNESS_PI_AUTH_JSON=/absolute/path/to/auth.json
export HARNESS_REAL_PI_MODEL=anthropic/claude-sonnet-4
.venv/bin/python -m pytest tests/test_real_pi_integration.py -q
python3 starter/bin/run_real_canary.py
```

Treat the recent canary history as the primary promotion signal. Before tagging a release, verify recent successful canary artifacts from `main`:

```bash
python3 starter/bin/verify_release_evidence.py --summary-glob "starter/runs/real-canary-*.summary.json" --min-runs 2 --freshness-hours 36
```

Promotion-ready release evidence should also include the benchmark report and provenance file:

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

Build the release bundle from the supported runtime only, and attach the full evidence set:

```bash
HARNESS_BENCHMARK_REPORT=starter/runs/retrieval-latest.json \
HARNESS_REPLAY_REPORT=starter/runs/replay-latest.json \
HARNESS_FAULT_REPORT=starter/runs/fault-latest.json \
HARNESS_CANARY_SUMMARY_GLOB="starter/runs/real-canary-*.summary.json" \
starter/bin/build-release-artifacts.sh
```

## Operational defaults

- `HARNESS_STRICT_MODE=1`
- `HARNESS_MODEL_TIMEOUT_SECONDS=900`
- `HARNESS_PI_RETRY_COUNT=2`
- `HARNESS_EVAL_TIMEOUT_SECONDS=300`
- `HARNESS_ALLOW_DANGEROUS_EVAL=0`
- `HARNESS_ALLOW_NETWORK_TASKS=0`

Wrapper-style eval commands such as `bash -c`, `sh -c`, `python -c`, and `env ... python3 -c`
are blocked by default and require `HARNESS_ALLOW_DANGEROUS_EVAL=1`. Wrapper payloads with
literal network indicators still require `HARNESS_ALLOW_NETWORK_TASKS=1`.

## Failure classes

- `contract_invalid`: the task or run contract is malformed, blocked by policy, or missing required files
- `eval_failed`: eval commands, artifact checks, secret scans, cancellations, or post-run checks failed
- `model_invocation_failed`: the `pi` invocation exited non-zero or timed out
- `result_invalid`: `result.json` is missing, unparsable, non-object, or schema-invalid

Automation should read:

- `outputs/run_manifest.json.primary_error_code`
- `outputs/run_manifest.json.failure_classifications`

The legacy `outputs/run_manifest.json.error_code` remains for compatibility. Use `score.json`, `run-events.jsonl`, `transcript.jsonl`, and `pi.stderr.log` when diagnosis needs richer context.

## Evidence retention and recovery

- Keep run evidence in `starter/runs/<run-id>/`
- Derived retrieval cache lives under `starter/runs/.index/retrieval-v4/` and may be rebuilt anytime
- Archive evidence with `starter/bin/archive-run-evidence.sh`
- Rebuild retrieval cache with `python3 starter/bin/rebuild_retrieval_index.py`
- Mine harder benchmark proposals with `python3 starter/bin/mine_harder_retrieval_benchmarks.py`
- Sweep offline retrieval profiles with `python3 starter/bin/sweep_retrieval_profiles.py`
- Analyze benchmark snapshots with `python3 starter/bin/analyze_retrieval_benchmarks.py`
- Build learning datasets with `python3 starter/bin/build_learning_datasets.py`
- Train retrieval candidates with `python3 starter/bin/train_retrieval_candidate.py`
- Evaluate and promote retrieval candidates with `python3 starter/bin/evaluate_retrieval_candidate.py`
- Build candidate reports with `python3 starter/bin/build_candidate_report.py`
- Restore an archived run with `starter/bin/restore-run-evidence.sh`
- Partial reruns are automatically moved into `runs/<run-id>/recovery/<timestamp>/`
- Recovery evidence is included in secret scanning; reruns fail if archived recovery artifacts
  still contain likely secrets
- Imported retrieval payloads under `context/source-runs/` are treated as historical references and
  are excluded from current-run secret scanning
- Review `outputs/run_manifest.json`, `score.json`, `run-events.jsonl`, `transcript.jsonl`, and `pi.stderr.log` before signing off a canary
- Treat missing fresh canary summaries, retrieval benchmark output, replay benchmark output, or fault-injection benchmark output as a release blocker

## Real canary expectations

`starter/bin/run_real_canary.py` exercises:

- success
- forced invalid `result.json`
- timeout
- interrupted run
- retry path
- partial-run recovery

It writes a summary JSON file under `starter/runs/real-canary-*.summary.json`.

Each summary includes:

- scenario pass/fail rollups
- `PI_VERSION`
- model
- commit SHA
- timestamps
- referenced run directories

Use those summaries to build replay corpora and trend evidence rather than relying only on handcrafted fixtures.

Generate a sanitized replay corpus from recent run evidence:

```bash
python3 starter/bin/build_replay_corpus.py --runs-root starter/runs --out starter/benchmarks/replay-corpus.json
```

Run replay/load and generated fault-injection benchmarks:

```bash
python3 starter/bin/benchmark_harness.py --mode replay --replay-corpus starter/benchmarks/replay-corpus.json --history-dir starter/runs --out starter/runs/replay-latest.json
python3 starter/bin/benchmark_harness.py --mode fault-injection --fault-samples 6 --fault-seed 7 --fault-corpus-out starter/runs/fault-novel.json --out starter/runs/fault-latest.json
```

`prepare-context.py` now emits the `context-manifest-v1` sidecar, `benchmark_harness.py` emits the `benchmark-report-v1` contract, and `verify_release_evidence.py` emits the `release-gate-v1` artifact for promotion decisions.

## Security posture

- The harness is designed for file-based local execution, not a hosted service
- Preflight scans the repository for secrets and dependency issues
- Trivy is only a required release gate when a `Dockerfile` or other image surface exists
- If the deployment surface expands beyond local file-based execution, add image or platform scanning before calling the system production-ready
