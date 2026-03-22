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

For broad production-readiness, also run:

```bash
export HARNESS_PI_AUTH_JSON=/absolute/path/to/auth.json
export HARNESS_REAL_PI_MODEL=anthropic/claude-sonnet-4
.venv/bin/python -m pytest tests/test_real_pi_integration.py -q
python3 starter/bin/run_real_canary.py
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

## Evidence retention and recovery

- Keep run evidence in `starter/runs/<run-id>/`
- Derived retrieval cache lives under `starter/runs/.index/retrieval-v1/` and may be rebuilt anytime
- Archive evidence with `starter/bin/archive-run-evidence.sh`
- Rebuild retrieval cache with `python3 starter/bin/rebuild_retrieval_index.py`
- Restore an archived run with `starter/bin/restore-run-evidence.sh`
- Partial reruns are automatically moved into `runs/<run-id>/recovery/<timestamp>/`
- Recovery evidence is included in secret scanning; reruns fail if archived recovery artifacts
  still contain likely secrets
- Imported retrieval payloads under `context/source-runs/` are treated as historical references and
  are excluded from current-run secret scanning
- Review `outputs/run_manifest.json`, `score.json`, `run-events.jsonl`, `transcript.jsonl`, and `pi.stderr.log` before signing off a canary

## Real canary expectations

`starter/bin/run_real_canary.py` exercises:

- success
- forced invalid `result.json`
- timeout
- interrupted run
- retry path
- partial-run recovery

It writes a summary JSON file under `starter/runs/real-canary-*.summary.json`.

## Security posture

- The harness is designed for file-based local execution, not a hosted service
- Preflight scans the repository for secrets and dependency issues
- Trivy is only a required release gate when a `Dockerfile` or other image surface exists
- If the deployment surface expands beyond local file-based execution, add image or platform scanning before calling the system production-ready
