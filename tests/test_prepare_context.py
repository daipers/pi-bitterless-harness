from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess
import sys

import numpy
from harnesslib import default_run_contract
from learninglib import build_candidate_manifest, write_candidate_manifest


def write_capability_run(run_dir: pathlib.Path, *, task_body: str) -> None:
    (run_dir / "outputs").mkdir(parents=True, exist_ok=True)
    (run_dir / "score").mkdir(exist_ok=True)
    (run_dir / "home").mkdir(exist_ok=True)
    (run_dir / "session").mkdir(exist_ok=True)
    (run_dir / "task.md").write_text(task_body, encoding="utf-8")
    (run_dir / "RUN.md").write_text("# Run\n", encoding="utf-8")
    (run_dir / "result.template.json").write_text("{}", encoding="utf-8")
    (run_dir / "run.contract.json").write_text(
        json.dumps(default_run_contract(version="v2", execution_profile="capability"), indent=2)
        + "\n",
        encoding="utf-8",
    )


def write_dense_candidate(
    starter: pathlib.Path,
    *,
    candidate_id: str,
    mode: str,
    activation_approved: bool,
) -> pathlib.Path:
    candidate_path = starter / "candidates" / "retrieval" / "active.json"
    artifacts_dir = starter / "candidates" / "retrieval" / f"{candidate_id}.artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    weights_path = artifacts_dir / "feature_weights.npy"
    numpy.save(weights_path, numpy.ones(8192, dtype=numpy.float32))
    config_path = artifacts_dir / "encoder-config.json"
    config_path.write_text(
        json.dumps(
            {
                "retriever_type": "dense-v1",
                "hash_dim": 8192,
                "embedding_dim": 256,
                "projection_seed": 7,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    write_candidate_manifest(
        candidate_path,
        build_candidate_manifest(
            candidate_type="retrieval",
            candidate_id=candidate_id,
            mode=mode,
            runtime={
                "retriever_version": "dense-hashed-shared-encoder-v1",
                "retriever": {
                    "retriever_type": "dense-v1",
                    "hash_dim": 8192,
                    "embedding_dim": 256,
                    "projection_seed": 7,
                    "artifact_fingerprint": f"{candidate_id}-fingerprint",
                    "feature_weights_path": str(weights_path),
                    "config_path": str(config_path),
                },
                "reranker_version": "learned-linear-reranker-v1",
                "abstention_model_version": "threshold-calibrator-v1",
                "selection": {"max_selected_sources": 1, "stage1_k": 10},
                "reranker": {
                    "bias": -1.0,
                    "feature_weights": {
                        "claim_overlap": 0.4,
                        "quality_prior": 0.6,
                        "stage1_score": 0.1,
                    },
                },
                "abstention": {"probability_threshold": 0.5},
            },
            promotion={
                "activation_approved": activation_approved,
                "approved_at": "2026-03-22T12:00:00Z" if activation_approved else None,
                "approval_reason": "test fixture"
                if activation_approved
                else "pending benchmark",
            },
        ),
    )
    return candidate_path


def test_prepare_context_selects_successful_runs_and_copies_only_safe_text_artifacts(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Improve harness retrieval

## Goal
Produce a passing score for harness retrieval.

## Constraints
- Stay local.

## Done
- Score is written.

## Eval
```bash
python3 ../tests/fixtures/pass_eval.py
```

## Required Artifacts
- result.json
- outputs/run_manifest.json

## Result JSON schema (source of truth)
```json
{schema_text.strip()}
```
"""

    current_run = starter / "runs" / "20260321-000000-current"
    write_capability_run(current_run, task_body=task_body)
    (current_run / "result.schema.json").write_text(schema_text, encoding="utf-8")

    good_run = starter / "runs" / "20260320-000000-good"
    write_capability_run(good_run, task_body=task_body)
    (good_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (good_run / "outputs" / "text.txt").write_text("retrieval proof\n", encoding="utf-8")
    (good_run / "outputs" / "big.txt").write_text("x" * 70000, encoding="utf-8")
    (good_run / "outputs" / "binary.bin").write_bytes(b"\xff\x00\xff")
    outside_path = good_run.parent / "outside.txt"
    outside_path.write_text("outside\n", encoding="utf-8")
    (good_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "retrieval scoring success",
                "artifacts": [
                    {"path": "outputs/text.txt", "description": "safe text artifact"},
                    {"path": "../outside.txt", "description": "out of scope"},
                    {"path": "outputs/big.txt", "description": "too large"},
                    {"path": "outputs/binary.bin", "description": "binary"},
                    {"path": "outputs/missing.txt", "description": "missing"},
                ],
                "claims": [{"claim": "retrieval helped", "evidence": ["outputs/text.txt"]}],
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (good_run / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )
    (good_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )

    bad_run = starter / "runs" / "20260319-000000-bad"
    write_capability_run(bad_run, task_body=task_body)
    (bad_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (bad_run / "result.json").write_text("{broken\n", encoding="utf-8")
    (bad_run / "score.json").write_text(
        json.dumps({"overall_pass": False, "failure_classifications": ["eval_failed"]}, indent=2)
        + "\n",
        encoding="utf-8",
    )

    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    completed = subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "prepare-context.py"),
            str(current_run),
            "policies/capability.json",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    summary = (current_run / "context" / "retrieval-summary.md").read_text(encoding="utf-8")

    assert str(current_run / "context" / "retrieval-manifest.json") in completed.stdout
    assert manifest["context_manifest_version"] == "v1"
    assert manifest["index_version"] == "retrieval-v4"
    assert manifest["index_mode"] == "cold_build"
    assert manifest["selection_strategy"] == "hybrid_v1"
    assert manifest["retrieval_profile_id"] == "retrieval-v4-default"
    assert manifest["retrieval_profile_fingerprint"]
    assert manifest["candidate_run_count"] == 2
    assert manifest["eligible_run_count"] == 1
    assert manifest["selected_count"] == 1
    assert manifest["selected_source_count"] == 1
    assert manifest["empty_context"] is False
    assert manifest["abstained"] is False
    assert manifest["abstention_reason"] is None
    assert manifest["query_token_count"] > 0
    assert manifest["ranking_latency_ms"] >= 0
    assert manifest["refreshed_run_count"] == 2
    assert manifest["evicted_run_count"] == 0
    assert manifest["artifact_bytes_copied"] > 0
    assert "20260320-000000-good" in manifest["selected_source_run_ids"]
    assert "20260319-000000-bad" not in manifest["selected_source_run_ids"]
    assert manifest["skipped_sources_count"] >= 1
    assert manifest["top_candidates"][0]["run_id"] == "20260320-000000-good"
    assert manifest["top_candidates"][0]["selected"] is True
    assert "20260320-000000-good" in summary
    assert "retrieval-view.md" in summary
    assert "outputs/text.txt" in summary
    assert "safe text artifact" in summary
    selected_run_dir = current_run / "context" / "source-runs" / "20260320-000000-good"
    assert (selected_run_dir / "retrieval-view.md").exists()
    assert (selected_run_dir / "outputs" / "text.txt").exists()
    assert not (selected_run_dir / "task.md").exists()
    assert not (selected_run_dir / "result.json").exists()
    assert not (selected_run_dir / "score.json").exists()
    assert not (selected_run_dir / "outputs" / "big.txt").exists()
    assert not (selected_run_dir / "outputs" / "binary.bin").exists()
    copied_files = manifest["selected_sources"][0]["copied_files"]
    assert copied_files[0]["copy_reason"] == "retrieval_view"
    assert copied_files[-1]["copy_reason"] == "claim_evidence"
    assert manifest["selected_sources"][0]["copy_summary"]["claim_evidence_copy_count"] == 1
    assert manifest["selected_sources"][0]["view_path"].endswith("retrieval-view.md")
    assert (current_run.parent / ".index" / "retrieval-v4" / "20260320-000000-good.json").exists()


def test_prepare_context_can_abstain_on_low_confidence_profile(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Improve harness retrieval

## Goal
Produce a passing score for harness retrieval.

## Constraints
- Stay local.

## Done
- Score is written.

## Eval
```bash
python3 ../tests/fixtures/pass_eval.py
```

## Required Artifacts
- result.json
- outputs/run_manifest.json

## Result JSON schema (source of truth)
```json
{schema_text.strip()}
```
"""

    current_run = starter / "runs" / "20260321-000000-current"
    write_capability_run(current_run, task_body=task_body)
    (current_run / "result.schema.json").write_text(schema_text, encoding="utf-8")

    good_run = starter / "runs" / "20260320-000000-good"
    write_capability_run(good_run, task_body=task_body)
    (good_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (good_run / "outputs" / "text.txt").write_text("retrieval proof\n", encoding="utf-8")
    (good_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (good_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "retrieval scoring success with evidence",
                "artifacts": [{"path": "outputs/text.txt", "description": "safe text artifact"}],
                "claims": [{"claim": "retrieval helped", "evidence": ["outputs/text.txt"]}],
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (good_run / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )

    profile_path = starter / "retrieval" / "abstain-profile.json"
    profile_path.write_text(
        json.dumps(
            {
                "profile_id": "retrieval-v4-abstain",
                "abstention": {
                    "enabled": True,
                    "min_top_score": 1000,
                    "min_score_margin": 2,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    env = os.environ | {
        "PYTHONPATH": str(starter / "bin"),
        "HARNESS_RETRIEVAL_PROFILE_PATH": str(profile_path),
    }
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "prepare-context.py"),
            str(current_run),
            "policies/capability.json",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )

    assert manifest["abstained"] is True
    assert manifest["abstention_reason"] == "low_top_score"
    assert manifest["selected_count"] == 0
    assert manifest["selected_source_run_ids"] == []


def test_prepare_context_records_retrieval_candidate_metadata(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Improve harness retrieval

## Goal
Produce a passing score for harness retrieval.

## Constraints
- Stay local.

## Done
- Score is written.

## Eval
```bash
python3 ../tests/fixtures/pass_eval.py
```

## Required Artifacts
- result.json
- outputs/run_manifest.json

## Result JSON schema (source of truth)
```json
{schema_text.strip()}
```
"""

    current_run = starter / "runs" / "20260321-000000-current"
    write_capability_run(current_run, task_body=task_body)
    (current_run / "result.schema.json").write_text(schema_text, encoding="utf-8")

    good_run = starter / "runs" / "20260320-000000-good"
    write_capability_run(good_run, task_body=task_body)
    (good_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (good_run / "outputs" / "text.txt").write_text("retrieval proof\n", encoding="utf-8")
    (good_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (good_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "retrieval scoring success with evidence",
                "artifacts": [{"path": "outputs/text.txt", "description": "safe text artifact"}],
                "claims": [{"claim": "retrieval helped", "evidence": ["outputs/text.txt"]}],
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (good_run / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )

    candidate_path = starter / "candidates" / "retrieval" / "active.json"
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    write_candidate_manifest(
        candidate_path,
        build_candidate_manifest(
            candidate_type="retrieval",
            candidate_id="retrieval-linear-1",
            mode="active",
            runtime={
                "retriever_version": "embedding-ann-v1",
                "reranker_version": "linear-reranker-v1",
                "abstention_model_version": "logistic-v1",
                "selection": {"max_selected_sources": 1, "stage1_k": 10},
                "reranker": {
                    "bias": -1.0,
                    "feature_weights": {
                        "claim_overlap": 0.4,
                        "quality_prior": 0.6,
                        "stage1_score": 0.1
                    }
                },
                "abstention": {"probability_threshold": 0.5}
            },
            promotion={
                "activation_approved": True,
                "approved_at": "2026-03-22T12:00:00Z",
                "approval_reason": "test fixture"
            },
        ),
    )

    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "prepare-context.py"),
            str(current_run),
            "policies/capability.json",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["retrieval_candidate_id"] == "retrieval-linear-1"
    assert manifest["retrieval_candidate_mode"] == "active"
    assert manifest["retriever_version"] == "embedding-ann-v1"
    assert manifest["reranker_version"] == "linear-reranker-v1"
    assert manifest["abstention_model_version"] == "logistic-v1"
    assert manifest["selection_source"] == "candidate"
    assert manifest["top_candidates"][0]["usefulness_probability"] is not None


def test_prepare_context_treats_unapproved_active_candidate_as_shadow(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Improve harness retrieval

## Goal
Produce a passing score for harness retrieval.

## Constraints
- Stay local.

## Done
- Score is written.

## Eval
```bash
python3 ../tests/fixtures/pass_eval.py
```

## Required Artifacts
- result.json
- outputs/run_manifest.json

## Result JSON schema (source of truth)
```json
{schema_text.strip()}
```
"""

    current_run = starter / "runs" / "20260321-000000-current"
    write_capability_run(current_run, task_body=task_body)
    (current_run / "result.schema.json").write_text(schema_text, encoding="utf-8")

    good_run = starter / "runs" / "20260320-000000-good"
    write_capability_run(good_run, task_body=task_body)
    (good_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (good_run / "outputs" / "text.txt").write_text("retrieval proof\n", encoding="utf-8")
    (good_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (good_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "retrieval scoring success with evidence",
                "artifacts": [{"path": "outputs/text.txt", "description": "safe text artifact"}],
                "claims": [{"claim": "retrieval helped", "evidence": ["outputs/text.txt"]}],
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (good_run / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )

    candidate_path = starter / "candidates" / "retrieval" / "active.json"
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    write_candidate_manifest(
        candidate_path,
        build_candidate_manifest(
            candidate_type="retrieval",
            candidate_id="retrieval-shadowed-1",
            mode="active",
            runtime={
                "retriever_version": "embedding-ann-v1",
                "reranker_version": "linear-reranker-v1",
                "abstention_model_version": "logistic-v1",
                "selection": {"max_selected_sources": 1, "stage1_k": 10},
                "reranker": {
                    "bias": -1.0,
                    "feature_weights": {"claim_overlap": 0.4, "quality_prior": 0.6},
                },
                "abstention": {"probability_threshold": 0.5},
            },
            promotion={
                "activation_approved": False,
                "approved_at": None,
                "approval_reason": "pending benchmark"
            },
        ),
    )

    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "prepare-context.py"),
            str(current_run),
            "policies/capability.json",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["retrieval_candidate_mode"] == "shadow"
    assert manifest["selection_source"] == "legacy"
    assert manifest["candidate_shadow"]["candidate_id"] == "retrieval-shadowed-1"


def test_prepare_context_uses_dense_stage1_for_active_candidate(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Recover delta forge proof

## Goal
Recover delta-forge proof evidence for shard ledger scoring.

## Constraints
- Stay local.

## Done
- Score is written.

## Eval
```bash
python3 ../tests/fixtures/pass_eval.py
```

## Required Artifacts
- result.json
- outputs/run_manifest.json

## Result JSON schema (source of truth)
```json
{schema_text.strip()}
```
"""

    current_run = starter / "runs" / "20260321-000000-current"
    write_capability_run(current_run, task_body=task_body)
    (current_run / "result.schema.json").write_text(schema_text, encoding="utf-8")

    gold_run = starter / "runs" / "20260320-000000-gold"
    write_capability_run(gold_run, task_body=task_body)
    (gold_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (gold_run / "outputs" / "delta-proof.txt").write_text("delta proof\n", encoding="utf-8")
    (gold_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (gold_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "Recovered delta forge proof evidence for shard ledger scoring.",
                "artifacts": [
                    {
                        "path": "outputs/delta-proof.txt",
                        "description": "delta forge proof artifact",
                    }
                ],
                "claims": [
                    {
                        "claim": "delta proof restored",
                        "evidence": ["outputs/delta-proof.txt"],
                    }
                ],
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (gold_run / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )

    confuser_run = starter / "runs" / "20260320-000001-confuser"
    write_capability_run(confuser_run, task_body=task_body)
    (confuser_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (confuser_run / "outputs" / "generic.txt").write_text("generic note\n", encoding="utf-8")
    (confuser_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (confuser_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "Generic shard ledger note without the relevant proof text.",
                "artifacts": [
                    {
                        "path": "outputs/generic.txt",
                        "description": "generic note",
                    }
                ],
                "claims": [
                    {
                        "claim": "generic note preserved",
                        "evidence": ["outputs/generic.txt"],
                    }
                ],
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (confuser_run / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )

    write_dense_candidate(
        starter,
        candidate_id="retrieval-dense-active",
        mode="active",
        activation_approved=True,
    )

    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "prepare-context.py"),
            str(current_run),
            "policies/capability.json",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["stage1_source"] == "dense_candidate"
    assert manifest["dense_candidate_id"] == "retrieval-dense-active"
    assert manifest["dense_candidate_mode"] == "active"
    assert manifest["dense_fallback_reason"] is None
    assert manifest["selection_source"] == "candidate"
    assert manifest["top_candidates"][0]["dense_stage1_score"] is not None


def test_prepare_context_records_dense_shadow_without_changing_live_selection(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Recover delta forge proof

## Goal
Recover delta-forge proof evidence for shard ledger scoring.

## Constraints
- Stay local.

## Done
- Score is written.

## Eval
```bash
python3 ../tests/fixtures/pass_eval.py
```

## Required Artifacts
- result.json
- outputs/run_manifest.json

## Result JSON schema (source of truth)
```json
{schema_text.strip()}
```
"""

    current_run = starter / "runs" / "20260321-000000-current"
    write_capability_run(current_run, task_body=task_body)
    (current_run / "result.schema.json").write_text(schema_text, encoding="utf-8")

    good_run = starter / "runs" / "20260320-000000-good"
    write_capability_run(good_run, task_body=task_body)
    (good_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (good_run / "outputs" / "proof.txt").write_text("delta proof\n", encoding="utf-8")
    (good_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (good_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "Recovered delta forge proof evidence.",
                "artifacts": [{"path": "outputs/proof.txt", "description": "delta proof artifact"}],
                "claims": [{"claim": "delta proof restored", "evidence": ["outputs/proof.txt"]}],
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (good_run / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )

    write_dense_candidate(
        starter,
        candidate_id="retrieval-dense-shadow",
        mode="shadow",
        activation_approved=False,
    )

    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "prepare-context.py"),
            str(current_run),
            "policies/capability.json",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["stage1_source"] == "lexical"
    assert manifest["selection_source"] == "legacy"
    assert manifest["candidate_shadow"]["dense_stage1_enabled"] is True
    assert manifest["candidate_shadow"]["dense_stage1_run_ids"]


def test_prepare_context_falls_back_to_lexical_when_dense_artifacts_are_missing(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Recover delta forge proof

## Goal
Recover delta-forge proof evidence for shard ledger scoring.

## Constraints
- Stay local.

## Done
- Score is written.

## Eval
```bash
python3 ../tests/fixtures/pass_eval.py
```

## Required Artifacts
- result.json
- outputs/run_manifest.json

## Result JSON schema (source of truth)
```json
{schema_text.strip()}
```
"""

    current_run = starter / "runs" / "20260321-000000-current"
    write_capability_run(current_run, task_body=task_body)
    (current_run / "result.schema.json").write_text(schema_text, encoding="utf-8")

    good_run = starter / "runs" / "20260320-000000-good"
    write_capability_run(good_run, task_body=task_body)
    (good_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (good_run / "outputs" / "proof.txt").write_text("delta proof\n", encoding="utf-8")
    (good_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (good_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "Recovered delta forge proof evidence.",
                "artifacts": [{"path": "outputs/proof.txt", "description": "delta proof artifact"}],
                "claims": [{"claim": "delta proof restored", "evidence": ["outputs/proof.txt"]}],
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (good_run / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )

    candidate_path = write_dense_candidate(
        starter,
        candidate_id="retrieval-dense-broken",
        mode="active",
        activation_approved=True,
    )
    candidate_payload = json.loads(candidate_path.read_text(encoding="utf-8"))
    pathlib.Path(
        candidate_payload["runtime"]["retriever"]["feature_weights_path"]
    ).unlink()

    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "prepare-context.py"),
            str(current_run),
            "policies/capability.json",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["stage1_source"] == "lexical"
    assert manifest["dense_candidate_id"] == "retrieval-dense-broken"
    assert "dense feature weights missing" in manifest["dense_fallback_reason"]


def test_prepare_context_reuses_refreshes_and_evicts_index_entries(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Improve harness retrieval

## Goal
Produce a passing score for harness retrieval with nebula-vector retrieval anchor.

## Constraints
- Stay local.

## Done
- Score is written.

## Eval
```bash
python3 ../tests/fixtures/pass_eval.py
```

## Required Artifacts
- result.json
- outputs/run_manifest.json

## Result JSON schema (source of truth)
```json
{schema_text.strip()}
```
"""

    current_run = starter / "runs" / "20260321-000000-current"
    write_capability_run(current_run, task_body=task_body)
    (current_run / "result.schema.json").write_text(schema_text, encoding="utf-8")

    good_run = starter / "runs" / "20260320-000000-good"
    write_capability_run(good_run, task_body=task_body)
    (good_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (good_run / "outputs" / "text.txt").write_text("retrieval proof\n", encoding="utf-8")
    (good_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (good_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "retrieval scoring success",
                "artifacts": [{"path": "outputs/text.txt", "description": "safe text artifact"}],
                "claims": [{"claim": "retrieval helped", "evidence": ["outputs/text.txt"]}],
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (good_run / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )
    write_dense_candidate(
        starter,
        candidate_id="retrieval-dense-cache",
        mode="active",
        activation_approved=True,
    )

    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    command = [
        sys.executable,
        str(starter / "bin" / "prepare-context.py"),
        str(current_run),
        "policies/capability.json",
    ]

    subprocess.run(
        command,
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    warm_completed = subprocess.run(
        command,
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    warm_manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    assert warm_completed.returncode == 0
    assert warm_manifest["index_mode"] == "warm_reuse"
    assert warm_manifest["refreshed_run_count"] == 0
    assert warm_manifest["evicted_run_count"] == 0
    dense_cache_path = (
        starter
        / "runs"
        / ".index"
        / "retrieval-v4"
        / "dense-stage1-cache"
        / "retrieval-dense-cache"
        / "20260320-000000-good.json"
    )
    first_cache_payload = json.loads(dense_cache_path.read_text(encoding="utf-8"))

    refreshed_payload = json.loads((good_run / "result.json").read_text(encoding="utf-8"))
    refreshed_payload["summary"] = "retrieval scoring success updated"
    (good_run / "result.json").write_text(
        json.dumps(refreshed_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    subprocess.run(
        command,
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    refreshed_manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    assert refreshed_manifest["index_mode"] == "incremental_refresh"
    assert refreshed_manifest["refreshed_run_count"] == 1
    assert refreshed_manifest["evicted_run_count"] == 0
    assert (
        refreshed_manifest["selected_sources"][0]["summary"] == "retrieval scoring success updated"
    )
    refreshed_cache_payload = json.loads(dense_cache_path.read_text(encoding="utf-8"))
    assert (
        refreshed_cache_payload["source_snapshot_fingerprint"]
        != first_cache_payload["source_snapshot_fingerprint"]
    )

    shutil.rmtree(good_run)
    subprocess.run(
        command,
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    evicted_manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    assert evicted_manifest["index_mode"] == "incremental_refresh"
    assert evicted_manifest["evicted_run_count"] == 1
    assert evicted_manifest["selected_source_run_ids"] == []


def test_rebuild_retrieval_index_rebuilds_cache_without_touching_runs(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Improve harness retrieval

## Goal
Produce a passing score for harness retrieval.

## Constraints
- Stay local.

## Done
- Score is written.

## Eval
```bash
python3 ../tests/fixtures/pass_eval.py
```

## Required Artifacts
- result.json
- outputs/run_manifest.json

## Result JSON schema (source of truth)
```json
{schema_text.strip()}
```
"""

    good_run = starter / "runs" / "20260320-000000-good"
    write_capability_run(good_run, task_body=task_body)
    (good_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (good_run / "outputs" / "text.txt").write_text("retrieval proof\n", encoding="utf-8")
    (good_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (good_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "retrieval scoring success",
                "artifacts": [{"path": "outputs/text.txt", "description": "safe text artifact"}],
                "claims": [{"claim": "retrieval helped", "evidence": ["outputs/text.txt"]}],
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (good_run / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )

    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    completed = subprocess.run(
        [sys.executable, str(starter / "bin" / "rebuild_retrieval_index.py")],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    payload = json.loads(completed.stdout)
    assert payload["index_mode"] == "cold_build"
    assert payload["candidate_run_count"] == 1
    assert payload["eligible_run_count"] == 1
    assert (starter / "runs" / ".index" / "retrieval-v4" / "20260320-000000-good.json").exists()


def test_prepare_context_prefers_exact_phrase_claim_evidence_and_evidence_first_copy(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Improve harness retrieval

## Goal
Produce a passing score for harness retrieval with nebula-vector retrieval anchor.

## Constraints
- Stay local.

## Done
- Score is written.

## Eval
```bash
python3 ../tests/fixtures/pass_eval.py
```

## Required Artifacts
- result.json
- outputs/run_manifest.json

## Result JSON schema (source of truth)
```json
{schema_text.strip()}
```
"""

    current_run = starter / "runs" / "20260321-000000-current"
    write_capability_run(current_run, task_body=task_body)
    (current_run / "result.schema.json").write_text(schema_text, encoding="utf-8")

    confuser_run = starter / "runs" / "20260320-000000-confuser"
    write_capability_run(confuser_run, task_body=task_body)
    (confuser_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (confuser_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (confuser_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "Produce a passing score for harness retrieval quickly",
                "artifacts": [],
                "claims": [],
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (confuser_run / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )

    good_run = starter / "runs" / "20260320-000001-good"
    write_capability_run(good_run, task_body=task_body)
    (good_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (good_run / "outputs" / "anchor.txt").write_text(
        "nebula-vector retrieval anchor proof\n",
        encoding="utf-8",
    )
    (good_run / "outputs" / "generic.txt").write_text("generic helper text\n", encoding="utf-8")
    (good_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (good_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "retrieval scoring success",
                "artifacts": [
                    {
                        "path": "outputs/generic.txt",
                        "description": "generic retrieval notes",
                    },
                    {
                        "path": "outputs/anchor.txt",
                        "description": "nebula-vector retrieval anchor evidence",
                    },
                ],
                "claims": [
                    {
                        "claim": "nebula-vector retrieval anchor proof was preserved",
                        "evidence": ["outputs/anchor.txt"],
                    }
                ],
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (good_run / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )

    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "prepare-context.py"),
            str(current_run),
            "policies/capability.json",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["selected_source_run_ids"][0] == "20260320-000001-good"
    assert manifest["top_candidates"][0]["run_id"] == "20260320-000001-good"
    assert manifest["top_candidates"][0]["score_breakdown"]["phrase_bonus"] >= 6
    copied_files = manifest["selected_sources"][0]["copied_files"]
    artifact_copy_reasons = [
        entry["copy_reason"]
        for entry in copied_files
        if entry["source_path"].startswith("outputs/")
    ]
    assert artifact_copy_reasons[0] == "claim_evidence"
