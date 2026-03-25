# pi-bitterless-harness

`pi-bitterless-harness` is a deterministic, file-based harness for running model-assisted tasks through the `pi` CLI while keeping the control plane simple and auditable.

It is built around a "keep the harness stupid" idea:

- the model does the reasoning
- the harness manages contracts, artifacts, scoring, and evidence
- every important run artifact lives on disk as plain files

## What This Repo Is

This repo provides a thin execution wrapper around `pi` with:

- repeatable runs with versioned contracts
- isolated per-run workspaces
- structured transcripts, manifests, and score artifacts
- external evaluation commands
- retrieval-assisted context for capability runs
- replay, fault-injection, and retrieval benchmarks
- typed release-gate evidence for production readiness

The goal is to make model runs easier to inspect, test, benchmark, and ship without hiding the workflow behind a large orchestration system.

## Why It Exists

Most agent harnesses grow into complicated controller layers. This repo takes the opposite approach:

- keep the runtime path thin
- store state in files instead of hidden services
- evaluate outcomes with external checks
- promote releases using evidence, not vibes

If you want the fuller design rationale, start with [pi-bitterless_harness_spec.md](./pi-bitterless_harness_spec.md).

## Multi-Agent Hardening Scorecard

- `Green` Opt-in gating: subagents are disabled by default and only allowed through the v3 capability path with explicit allowed profiles and `rpc` transport. See [AGENTS.md](./AGENTS.md), [starter/bin/harnesslib.py](./starter/bin/harnesslib.py), and [tests/test_runner_e2e.py](./tests/test_runner_e2e.py).
- `Green` Anti-choreography guardrails: the capability library rejects manager-style fields such as `workflow`, `pipeline`, `route`, and `handoff`, so the manifest stays a capability registry rather than a hidden workflow script. See [pi-bitterless_harness_spec.md](./pi-bitterless_harness_spec.md) and [starter/bin/capabilitylib.py](./starter/bin/capabilitylib.py).
- `Green` Capability containment: allowed tools, read/write scopes, network access, spawn count, token budget, and runtime budget are constrained per profile and validated against recorded usage. See [starter/bin/capabilitylib.py](./starter/bin/capabilitylib.py) and [tests/test_capabilitylib.py](./tests/test_capabilitylib.py).
- `Green` Auditability and evidence: capability manifests, subagent usage, run manifests, transcripts, and score artifacts keep the control plane file-native and inspectable. See [starter/bin/run_task.py](./starter/bin/run_task.py), [starter/bin/score_run.py](./starter/bin/score_run.py), and [starter/docs/operator-runbook.md](./starter/docs/operator-runbook.md).
- `Yellow` Runtime enforcement model: opt-in `managed_rpc` runs fail contract validation unless the configured peer implements `--managed-rpc-probe` for pre-execution interception, while legacy `rpc` runs still rely on pre-score audit plus score-time validation. This is a contract-enforcement stopgap, not proof of real-`pi` managed-runtime coverage. See [starter/bin/run_task.py](./starter/bin/run_task.py), [starter/bin/score_run.py](./starter/bin/score_run.py), and [tests/test_runner_e2e.py](./tests/test_runner_e2e.py).
- `Yellow` Future complexity risk: the command-center and orchestrator layers are still operational wrappers today, but they are the most likely place for workflow creep to re-enter later. See [starter/docs/operator-runbook.md](./starter/docs/operator-runbook.md) and [tests/test_orchestrator_async_retention.py](./tests/test_orchestrator_async_retention.py).

No current `Red` areas were found based on checked repo evidence.

## Repo Guide

- [starter/README.md](./starter/README.md): full operator and developer guide
- [starter/docs/operator-runbook.md](./starter/docs/operator-runbook.md): release and canary workflow
- [CHANGELOG.md](./CHANGELOG.md): release history
- [AGENTS.md](./AGENTS.md): project contract for agent-driven runs

## Quick Start

Install the supported Python tooling:

```bash
starter/bin/setup-dev-env.sh
```

Create a run:

```bash
starter/bin/new-task.sh "fix flaky login test"
```

Validate the run contract:

```bash
starter/bin/check-run-contract.sh starter/runs/<run-id>
```

Execute the run:

```bash
starter/bin/run-task.sh starter/runs/<run-id>
```

Run the full local verification gate:

```bash
starter/bin/preflight.sh
```

## What Gets Produced

Each run is designed to leave behind inspectable artifacts such as:

- `task.md`
- `RUN.md`
- `result.json`
- `score.json`
- `run-events.jsonl`
- `transcript.jsonl`
- `outputs/run_manifest.json`
- `patch.diff`

That makes the harness useful both for local iteration and for evidence-driven CI and release workflows.

## Status

The current repo already includes:

- ship-gate CI
- real-`pi` canaries
- retrieval, replay, and fault-injection benchmarks
- learning dataset and candidate-manifest tooling
- release-gate evidence verification

For the detailed runtime policy, supported versions, and contract files, use [starter/README.md](./starter/README.md).
