# pi-bitterless-harness

Run `pi` like an engineer, not like a black box.

`pi-bitterless-harness` is a deterministic, file-based execution harness for teams who want agent runs to be inspectable, benchmarkable, and releaseable.

Instead of burying everything inside an orchestration layer, it keeps the workflow simple:

- the model does the reasoning
- the harness enforces contracts and captures evidence
- every important artifact stays on disk in plain files

## Why People Use It

Most agent wrappers get complicated fast. This repo is for teams who want:

- repeatable runs with versioned contracts
- transcripts, manifests, scores, and diffs they can actually inspect
- external evaluation instead of self-grading
- retrieval, replay, and fault-injection benchmarks
- release gates based on fresh evidence, not intuition

In short: it helps turn raw agent execution into something you can test, audit, compare, and ship.

## What It Includes

This repo provides a thin execution wrapper around `pi` with:

- isolated per-run workspaces
- structured transcripts, manifests, and score artifacts
- retrieval-assisted context for capability runs
- learning datasets and candidate manifests for retrieval improvements
- replay, fault-injection, and retrieval benchmark tooling
- typed release-gate evidence for production readiness

If you want the fuller design rationale, start with [pi-bitterless_harness_spec.md](./pi-bitterless_harness_spec.md).

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
