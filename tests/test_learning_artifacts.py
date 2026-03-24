from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys

import numpy
from learninglib import build_candidate_manifest, write_candidate_manifest


def write_run(
    run_dir: pathlib.Path,
    *,
    task_text: str,
    score_payload: dict,
    result_payload: dict,
    context_manifest: dict | None = None,
) -> None:
    (run_dir / "outputs").mkdir(parents=True, exist_ok=True)
    (run_dir / "score").mkdir(exist_ok=True)
    (run_dir / "home").mkdir(exist_ok=True)
    (run_dir / "session").mkdir(exist_ok=True)
    (run_dir / "task.md").write_text(task_text, encoding="utf-8")
    (run_dir / "RUN.md").write_text("# Run\n", encoding="utf-8")
    (run_dir / "result.schema.json").write_text('{"type":"object"}\n', encoding="utf-8")
    (run_dir / "result.template.json").write_text("{}\n", encoding="utf-8")
    (run_dir / "run.contract.json").write_text("{}\n", encoding="utf-8")
    (run_dir / "outputs" / "run_manifest.json").write_text(
        json.dumps(
            {
                "snapshots": {
                    "task_sha256": "task-sha",
                    "run_contract_sha256": "contract-sha",
                    "result_schema_sha256": "schema-sha",
                },
                "timings": {"run_duration_ms": 12},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "score.json").write_text(
        json.dumps(score_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    (run_dir / "result.json").write_text(
        json.dumps(result_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    (run_dir / "run-events.jsonl").write_text(
        json.dumps({"phase": "run", "message": "done"}) + "\n",
        encoding="utf-8",
    )
    (run_dir / "transcript.jsonl").write_text(
        json.dumps({"type": "assistant", "text": "ok"}) + "\n",
        encoding="utf-8",
    )
    (run_dir / "patch.diff").write_text("diff --git a/x b/x\n+one\n-two\n", encoding="utf-8")
    if context_manifest is not None:
        (run_dir / "context").mkdir(exist_ok=True)
        (run_dir / "context" / "retrieval-manifest.json").write_text(
            json.dumps(context_manifest, indent=2) + "\n",
            encoding="utf-8",
        )
        (run_dir / "context" / "retrieval-summary.md").write_text(
            "# Retrieved Context\n",
            encoding="utf-8",
        )


def test_build_learning_datasets_emits_all_learning_corpora(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    runs_root = starter / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)
    task_text = """# Task
Learning dataset task

## Goal
Ship a learning dataset.

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

## Result JSON schema (source of truth)
```json
{"type":"object"}
```
"""
    write_run(
        runs_root / "20260322-000001-good",
        task_text=task_text,
        score_payload={
            "overall_pass": True,
            "result_json_valid_schema": True,
            "secret_scan": {"findings": []},
            "execution_profile": "capability",
            "policy_path": "policies/capability.json",
            "failure_classifications": [],
            "benchmark_eligibility": {"eligible": True},
            "retrieval": {"selected_source_count": 1, "candidate_run_count": 2, "abstained": False},
        },
        result_payload={
            "x-interface-version": "v1",
            "status": "success",
            "summary": "good run",
            "artifacts": [{"path": "outputs/claim.txt", "description": "claim"}],
            "claims": [{"claim": "done", "evidence": ["outputs/claim.txt"]}],
            "remaining_risks": [],
        },
        context_manifest={
            "context_manifest_version": "v1",
            "selected_source_run_ids": ["seed-a"],
            "top_candidates": [{"run_id": "seed-a", "selected": True}],
            "empty_context": False,
            "abstained": False,
        },
    )
    write_run(
        runs_root / "20260322-000002-secret",
        task_text=task_text,
        score_payload={
            "overall_pass": False,
            "result_json_valid_schema": True,
            "secret_scan": {"findings": [{"type": "token"}]},
            "failure_classifications": ["eval_failed"],
        },
        result_payload={"status": "failed"},
    )

    out_root = starter / "learning" / "latest"
    completed = subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "build_learning_datasets.py"),
            "--runs-root",
            str(runs_root),
            "--out-root",
            str(out_root),
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ | {"PYTHONPATH": str(starter / "bin")},
    )

    manifest = json.loads(
        (out_root / "learning-datasets.manifest.json").read_text(encoding="utf-8")
    )
    trajectory_rows = (
        out_root / "trajectory-records.jsonl"
    ).read_text(encoding="utf-8").splitlines()
    retrieval_rows = (
        out_root / "retrieval-examples.jsonl"
    ).read_text(encoding="utf-8").splitlines()
    retrieval_document_rows = (
        out_root / "retrieval-documents.jsonl"
    ).read_text(encoding="utf-8").splitlines()
    policy_rows = (out_root / "policy-examples.jsonl").read_text(encoding="utf-8").splitlines()
    model_rows = (out_root / "model-examples.jsonl").read_text(encoding="utf-8").splitlines()

    assert json.loads(completed.stdout)["learning_dataset_manifest_version"] == "v1"
    assert manifest["datasets"]["trajectory_records"]["row_count"] == 1
    assert len(trajectory_rows) == 1
    assert len(retrieval_rows) == 1
    assert len(retrieval_document_rows) == 1
    assert len(policy_rows) == 1
    assert len(model_rows) == 1
    trajectory_payload = json.loads(trajectory_rows[0])
    retrieval_document_payload = json.loads(retrieval_document_rows[0])
    assert trajectory_payload["trajectory_record_version"] == "v1"
    assert trajectory_payload["context"]["source_run_ids"] == ["seed-a"]
    assert retrieval_document_payload["retrieval_document_version"] == "v1"
    assert retrieval_document_payload["claims"] == ["done"]
    assert retrieval_document_payload["source_snapshot_fingerprint"]


def test_build_candidate_report_emits_candidate_report_v1(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    candidate_path = starter / "candidates" / "retrieval" / "active.json"
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    write_candidate_manifest(
        candidate_path,
        build_candidate_manifest(
            candidate_type="retrieval",
            candidate_id="retrieval-candidate-1",
            mode="active",
            runtime={
                "retriever_version": "embedding-ann-v1",
                "reranker_version": "linear-reranker-v1",
                "abstention_model_version": "logistic-v1",
            },
            training_dataset_fingerprints={"retrieval_examples": "train-sha"},
            evaluation_dataset_fingerprints={"retrieval_benchmark": "eval-sha"},
            bundle_id="bundle-1",
        ),
    )
    benchmark_path = starter / "runs" / "retrieval-benchmark.json"
    benchmark_path.parent.mkdir(parents=True, exist_ok=True)
    benchmark_path.write_text(
        json.dumps(
            {
                "benchmark_report_version": "v1",
                "generated_at": "2026-03-22T10:00:00Z",
                "overall_pass": True,
                "promotion_summary": {
                    "bundle_id": "bundle-1",
                    "gated_sections": ["retrieval"],
                    "threshold_results": {"retrieval": {"top_1_hit_rate": True}},
                    "candidate_types": {"retrieval": "retrieval-candidate-1"},
                },
                "retrieval": {"top_1_hit_rate": 0.8},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    out_path = starter / "runs" / "candidate-report.json"
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "build_candidate_report.py"),
            "--candidate-type",
            "retrieval",
            "--candidate",
            str(candidate_path),
            "--benchmark-report",
            str(benchmark_path),
            "--out",
            str(out_path),
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ | {"PYTHONPATH": str(starter / "bin")},
    )

    report = json.loads(out_path.read_text(encoding="utf-8"))
    assert report["candidate_report_version"] == "v1"
    assert report["candidate_id"] == "retrieval-candidate-1"
    assert report["overall_pass"] is True
    assert report["promotion_summary"]["bundle_id"] == "bundle-1"


def test_train_retrieval_candidate_emits_shadow_candidate_with_learned_weights(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    examples_path = starter / "learning" / "latest" / "retrieval-examples.jsonl"
    examples_path.parent.mkdir(parents=True, exist_ok=True)
    examples_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "retrieval_example_version": "v1",
                        "example_id": "ex-1",
                        "query": {"goal": "goal"},
                        "candidate_set": [
                            {
                                "run_id": "gold-1",
                                "total_score": 8,
                                "score_breakdown": {
                                    "claim_overlap": 4,
                                    "quality_prior": 2,
                                    "summary_overlap": 2,
                                },
                                "selected": True,
                            },
                            {
                                "run_id": "neg-1",
                                "total_score": 2,
                                "score_breakdown": {
                                    "claim_overlap": 0,
                                    "quality_prior": 0,
                                    "summary_overlap": 2,
                                },
                                "selected": False,
                            },
                        ],
                        "gold_source_run_ids": ["gold-1"],
                        "hard_negative_run_ids": ["neg-1"],
                        "abstention_label": False,
                        "usefulness_label": True,
                    }
                ),
                json.dumps(
                    {
                        "retrieval_example_version": "v1",
                        "example_id": "ex-2",
                        "query": {"goal": "goal"},
                        "candidate_set": [
                            {
                                "run_id": "neg-2",
                                "total_score": 1,
                                "score_breakdown": {"claim_overlap": 0, "quality_prior": 0},
                                "selected": False,
                            }
                        ],
                        "gold_source_run_ids": [],
                        "hard_negative_run_ids": ["neg-2"],
                        "abstention_label": True,
                        "usefulness_label": False,
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    out_path = starter / "candidates" / "retrieval" / "trained.json"
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "train_retrieval_candidate.py"),
            "--examples",
            str(examples_path),
            "--out",
            str(out_path),
            "--candidate-id",
            "retrieval-trained-1",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ | {"PYTHONPATH": str(starter / "bin")},
    )

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["candidate_manifest_version"] == "v1"
    assert payload["candidate_id"] == "retrieval-trained-1"
    assert payload["mode"] == "shadow"
    assert payload["promotion"]["activation_approved"] is False
    assert payload["runtime"]["retriever_version"] == "lexical-stage1-v2"
    assert payload["runtime"]["reranker_version"] == "text-pair-hashed-reranker-v2"
    assert payload["runtime"]["reranker"]["artifact_paths"]["feature_weights_path"]
    assert payload["runtime"]["abstention"]["probability_threshold"] > 0


def test_train_retrieval_candidate_dense_v1_writes_retriever_artifacts(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    examples_path = starter / "learning" / "latest" / "retrieval-examples.jsonl"
    documents_path = starter / "learning" / "latest" / "retrieval-documents.jsonl"
    examples_path.parent.mkdir(parents=True, exist_ok=True)
    examples_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "retrieval_example_version": "v1",
                        "example_id": "ex-1",
                        "query": {
                            "task_title": "Recover delta proof",
                            "goal": "Recover delta proof evidence",
                            "constraints": "Stay local",
                            "done": "Proof restored",
                        },
                        "candidate_set": [
                            {
                                "run_id": "gold-1",
                                "total_score": 8,
                                "score_breakdown": {
                                    "claim_overlap": 4,
                                    "quality_prior": 2,
                                    "summary_overlap": 2,
                                },
                                "selected": True,
                            },
                            {
                                "run_id": "neg-1",
                                "total_score": 1,
                                "score_breakdown": {
                                    "claim_overlap": 0,
                                    "quality_prior": 0,
                                },
                                "selected": False,
                            },
                        ],
                        "gold_source_run_ids": ["gold-1"],
                        "hard_negative_run_ids": ["neg-1"],
                        "abstention_label": False,
                        "usefulness_label": True,
                    }
                )
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    documents_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "retrieval_document_version": "v1",
                        "run_id": "gold-1",
                        "text": "recover delta proof artifact evidence claim",
                        "query_sections": {
                            "task_title": "Recover delta proof",
                            "goal": "Recover delta proof evidence",
                            "constraints": "Stay local",
                            "done": "Proof restored",
                        },
                        "summary": "good",
                        "claims": ["delta proof restored"],
                        "evidence_paths": ["outputs/delta-proof.txt"],
                        "artifact_records": [],
                        "quality": {"evidence_backed_claim_count": 1},
                        "source_snapshot_fingerprint": "gold-fp",
                    }
                ),
                json.dumps(
                    {
                        "retrieval_document_version": "v1",
                        "run_id": "neg-1",
                        "text": "generic shard note with no proof",
                        "query_sections": {
                            "task_title": "Recover delta proof",
                            "goal": "Recover delta proof evidence",
                            "constraints": "Stay local",
                            "done": "Proof restored",
                        },
                        "summary": "bad",
                        "claims": ["generic note"],
                        "evidence_paths": [],
                        "artifact_records": [],
                        "quality": {"evidence_backed_claim_count": 0},
                        "source_snapshot_fingerprint": "neg-fp",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    out_path = starter / "candidates" / "retrieval" / "dense.json"
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "train_retrieval_candidate.py"),
            "--examples",
            str(examples_path),
            "--documents",
            str(documents_path),
            "--out",
            str(out_path),
            "--candidate-id",
            "retrieval-dense-1",
            "--retriever-type",
            "dense-v1",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ | {"PYTHONPATH": str(starter / "bin")},
    )

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    retriever = payload["runtime"]["retriever"]
    assert payload["runtime"]["retriever_version"] == "dense-hashed-shared-encoder-v1"
    assert payload["runtime"]["reranker_version"] == "text-pair-hashed-reranker-v2"
    assert retriever["retriever_type"] == "dense-v1"
    assert pathlib.Path(retriever["feature_weights_path"]).is_file()
    assert pathlib.Path(retriever["config_path"]).is_file()
    weights = numpy.load(retriever["feature_weights_path"])
    assert weights.shape == (8192,)
    assert numpy.any(weights != 1.0)
    assert payload["training_dataset_fingerprints"]["retrieval_documents"]


def test_evaluate_retrieval_candidate_can_promote_manifest_from_reports(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    candidate_path = starter / "candidates" / "retrieval" / "trained.json"
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    write_candidate_manifest(
        candidate_path,
        build_candidate_manifest(
            candidate_type="retrieval",
            candidate_id="retrieval-trained-1",
            mode="shadow",
            runtime={
                "retriever_version": "lexical-stage1-v1",
                "reranker_version": "learned-linear-reranker-v1",
                "abstention_model_version": "threshold-calibrator-v1",
            },
            promotion={
                "activation_approved": False,
                "approved_at": None,
                "approval_reason": "pending benchmark",
            },
        ),
    )
    baseline_path = starter / "runs" / "baseline.json"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text(
        json.dumps(
            {
                "benchmark_report_version": "v1",
                "generated_at": "2026-03-22T10:00:00Z",
                "overall_pass": True,
                "retrieval": {
                    "hard_negative_win_rate": 0.75,
                    "top_1_hit_rate": 0.5,
                    "empty_context_precision": 0.75,
                    "hallucinated_evidence_rate": 0.2,
                    "warm_reuse_ms": 100.0,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    candidate_benchmark_path = starter / "runs" / "candidate-benchmark.json"
    candidate_benchmark_path.write_text(
        json.dumps(
            {
                "benchmark_report_version": "v1",
                "generated_at": "2026-03-22T11:00:00Z",
                "overall_pass": True,
                "retrieval": {
                    "hard_negative_win_rate": 0.88,
                    "top_1_hit_rate": 0.63,
                    "empty_context_precision": 0.8,
                    "hallucinated_evidence_rate": 0.1,
                    "warm_reuse_ms": 110.0,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    report_path = starter / "runs" / "retrieval-candidate-report.json"
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "evaluate_retrieval_candidate.py"),
            "--candidate",
            str(candidate_path),
            "--baseline-report",
            str(baseline_path),
            "--candidate-report",
            str(candidate_benchmark_path),
            "--out",
            str(report_path),
            "--promote-if-passed",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ | {"PYTHONPATH": str(starter / "bin")},
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    promoted_manifest = json.loads(candidate_path.read_text(encoding="utf-8"))
    assert report["candidate_report_version"] == "v1"
    assert report["comparison"]["activation_approved"] is True
    assert promoted_manifest["promotion"]["activation_approved"] is True
    assert promoted_manifest["mode"] == "active"


def test_evaluate_retrieval_candidate_blocks_dense_active_promotion_without_replay(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    weights_path = starter / "candidates" / "retrieval" / "dense.artifacts" / "feature_weights.npy"
    weights_path.parent.mkdir(parents=True, exist_ok=True)
    numpy.save(weights_path, numpy.ones(8192, dtype=numpy.float32))
    candidate_path = starter / "candidates" / "retrieval" / "dense.json"
    write_candidate_manifest(
        candidate_path,
        build_candidate_manifest(
            candidate_type="retrieval",
            candidate_id="retrieval-dense-1",
            mode="shadow",
            runtime={
                "retriever_version": "dense-hashed-shared-encoder-v1",
                "retriever": {
                    "retriever_type": "dense-v1",
                    "hash_dim": 8192,
                    "embedding_dim": 256,
                    "projection_seed": 7,
                    "artifact_fingerprint": "dense-artifacts",
                    "feature_weights_path": str(weights_path),
                    "config_path": str(weights_path.parent / "encoder-config.json"),
                },
                "reranker_version": "learned-linear-reranker-v1",
                "abstention_model_version": "threshold-calibrator-v1",
            },
            promotion={
                "activation_approved": False,
                "approved_at": None,
                "approval_reason": "pending benchmark",
            },
        ),
    )
    baseline_path = starter / "runs" / "baseline.json"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text(
        json.dumps(
            {
                "benchmark_report_version": "v1",
                "generated_at": "2026-03-22T10:00:00Z",
                "overall_pass": True,
                "retrieval": {
                    "hard_negative_win_rate": 0.75,
                    "top_1_hit_rate": 0.5,
                    "empty_context_precision": 0.75,
                    "hallucinated_evidence_rate": 0.2,
                    "warm_reuse_ms": 100.0,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    candidate_benchmark_path = starter / "runs" / "candidate-benchmark.json"
    candidate_benchmark_path.write_text(
        json.dumps(
            {
                "benchmark_report_version": "v1",
                "generated_at": "2026-03-22T11:00:00Z",
                "overall_pass": True,
                "retrieval": {
                    "hard_negative_win_rate": 0.88,
                    "top_1_hit_rate": 0.63,
                    "empty_context_precision": 0.8,
                    "hallucinated_evidence_rate": 0.1,
                    "warm_reuse_ms": 110.0,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    report_path = starter / "runs" / "retrieval-dense-report.json"
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "evaluate_retrieval_candidate.py"),
            "--candidate",
            str(candidate_path),
            "--baseline-report",
            str(baseline_path),
            "--candidate-report",
            str(candidate_benchmark_path),
            "--out",
            str(report_path),
            "--promote-if-passed",
            "--promote-mode",
            "active",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ | {"PYTHONPATH": str(starter / "bin")},
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    promoted_manifest = json.loads(candidate_path.read_text(encoding="utf-8"))
    assert report["comparison"]["activation_approved"] is False
    assert (
        report["comparison"]["threshold_results"]["replay_available_for_dense_active_promotion"]
        is False
    )
    assert promoted_manifest["promotion"]["activation_approved"] is False
    assert promoted_manifest["mode"] == "shadow"


def test_train_policy_candidate_emits_shadow_candidate_with_learned_recommendations(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    examples_path = starter / "learning" / "latest" / "policy-examples.jsonl"
    examples_path.parent.mkdir(parents=True, exist_ok=True)
    examples_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "policy_example_version": "v1",
                        "example_id": "policy-1",
                        "features": {
                            "execution_profile": "capability",
                            "selected_source_count": 2,
                            "candidate_run_count": 6,
                        },
                        "labels": {
                            "overall_pass": True,
                            "execution_profile": "capability",
                            "retry_recommended": True,
                            "benchmark_eligible": True,
                            "retrieval_budget": {
                                "selected_source_count": 2,
                                "candidate_run_count": 6,
                            },
                        },
                    }
                ),
                json.dumps(
                    {
                        "policy_example_version": "v1",
                        "example_id": "policy-2",
                        "features": {
                            "execution_profile": "capability",
                            "selected_source_count": 3,
                            "candidate_run_count": 8,
                        },
                        "labels": {
                            "overall_pass": True,
                            "execution_profile": "capability",
                            "retry_recommended": False,
                            "benchmark_eligible": True,
                            "retrieval_budget": {
                                "selected_source_count": 3,
                                "candidate_run_count": 8,
                            },
                        },
                    }
                ),
                json.dumps(
                    {
                        "policy_example_version": "v1",
                        "example_id": "policy-3",
                        "features": {
                            "execution_profile": "strict",
                            "selected_source_count": 1,
                            "candidate_run_count": 2,
                        },
                        "labels": {
                            "overall_pass": False,
                            "execution_profile": "strict",
                            "retry_recommended": False,
                            "benchmark_eligible": False,
                            "retrieval_budget": {
                                "selected_source_count": 1,
                                "candidate_run_count": 2,
                            },
                        },
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    out_path = starter / "candidates" / "policy" / "trained.json"
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "train_policy_candidate.py"),
            "--examples",
            str(examples_path),
            "--out",
            str(out_path),
            "--candidate-id",
            "policy-trained-1",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ | {"PYTHONPATH": str(starter / "bin")},
    )

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["candidate_manifest_version"] == "v1"
    assert payload["candidate_id"] == "policy-trained-1"
    assert payload["mode"] == "shadow"
    assert payload["runtime"]["policy_model_version"] == "contextual-policy-v2"
    assert payload["runtime"]["model"]["artifact_paths"]["model_path"]
    assert "execution_profile" in payload["runtime"]["heads"]
    assert payload["runtime"]["defaults"]["execution_profile"] == "strict"
    assert payload["runtime"]["recommendations"]["retrieval_budget"]["value"] == payload["runtime"][
        "defaults"
    ]["retrieval_budget"]
    assert payload["promotion"]["activation_approved"] is False


def test_evaluate_policy_candidate_can_promote_manifest_from_replay_and_canaries(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    candidate_path = starter / "candidates" / "policy" / "trained.json"
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    write_candidate_manifest(
        candidate_path,
        build_candidate_manifest(
            candidate_type="policy",
            candidate_id="policy-trained-1",
            mode="shadow",
            runtime={
                "policy_model_version": "aggregate-policy-v1",
                "activation_threshold": 0.6,
                "recommendations": {
                    "execution_profile": {"value": "capability", "confidence": 0.8},
                    "retry_limit": {"value": 3, "confidence": 0.7},
                    "retrieval_budget": {
                        "value": {"max_source_runs": 2, "max_candidates": 6},
                        "confidence": 0.75,
                    },
                    "benchmark_eligible": {"value": True, "confidence": 0.8},
                },
            },
            promotion={
                "activation_approved": False,
                "approved_at": None,
                "approval_reason": "pending replay/canary benchmark",
            },
        ),
    )
    baseline_replay_path = starter / "runs" / "baseline-replay.json"
    baseline_replay_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_replay_path.write_text(
        json.dumps(
            {
                "benchmark_report_version": "v1",
                "generated_at": "2026-03-22T10:00:00Z",
                "overall_pass": True,
                "replay": {
                    "workload_metrics": [
                        {"concurrency": 1, "pass_rate_percent": 66.7, "retry_recovery_rate": 0.5}
                    ]
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    candidate_replay_path = starter / "runs" / "candidate-replay.json"
    candidate_replay_path.write_text(
        json.dumps(
            {
                "benchmark_report_version": "v1",
                "generated_at": "2026-03-22T11:00:00Z",
                "overall_pass": True,
                "replay": {
                    "workload_metrics": [
                        {"concurrency": 1, "pass_rate_percent": 100.0, "retry_recovery_rate": 1.0}
                    ]
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    baseline_canary_dir = starter / "runs" / "baseline-canaries"
    candidate_canary_dir = starter / "runs" / "candidate-canaries"
    baseline_canary_dir.mkdir(parents=True, exist_ok=True)
    candidate_canary_dir.mkdir(parents=True, exist_ok=True)
    for path, timestamp in [
        (baseline_canary_dir / "one.summary.json", "2026-03-24T10:00:00Z"),
        (baseline_canary_dir / "two.summary.json", "2026-03-24T09:00:00Z"),
        (candidate_canary_dir / "one.summary.json", "2026-03-24T10:30:00Z"),
        (candidate_canary_dir / "two.summary.json", "2026-03-24T09:30:00Z"),
    ]:
        path.write_text(
            json.dumps(
                {
                    "generated_at": timestamp,
                    "overall_ok": True,
                    "supported_pi_version": "0.61.1",
                    "git_sha": "abc123",
                    "scenario_totals": {"total": 6, "passed": 6, "failed": 0},
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    report_path = starter / "runs" / "policy-candidate-report.json"
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "evaluate_policy_candidate.py"),
            "--candidate",
            str(candidate_path),
            "--baseline-replay-report",
            str(baseline_replay_path),
            "--candidate-replay-report",
            str(candidate_replay_path),
            "--baseline-canary-summary-glob",
            str(baseline_canary_dir / "*.summary.json"),
            "--candidate-canary-summary-glob",
            str(candidate_canary_dir / "*.summary.json"),
            "--out",
            str(report_path),
            "--promote-if-passed",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ | {"PYTHONPATH": str(starter / "bin")},
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    promoted_manifest = json.loads(candidate_path.read_text(encoding="utf-8"))
    assert report["candidate_report_version"] == "v1"
    assert report["comparison"]["activation_approved"] is True
    assert report["promotion_summary"]["candidate_types"]["policy"] == "policy-trained-1"
    assert promoted_manifest["promotion"]["activation_approved"] is True
    assert promoted_manifest["mode"] == "active"


def test_evaluate_policy_candidate_blocks_promotion_when_candidate_canaries_regress(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    candidate_path = starter / "candidates" / "policy" / "trained.json"
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    write_candidate_manifest(
        candidate_path,
        build_candidate_manifest(
            candidate_type="policy",
            candidate_id="policy-trained-1",
            mode="shadow",
            runtime={"policy_model_version": "aggregate-policy-v1", "recommendations": {}},
            promotion={
                "activation_approved": False,
                "approved_at": None,
                "approval_reason": "pending replay/canary benchmark",
            },
        ),
    )
    baseline_replay_path = starter / "runs" / "baseline-replay.json"
    baseline_replay_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_replay_path.write_text(
        json.dumps(
            {
                "benchmark_report_version": "v1",
                "generated_at": "2026-03-22T10:00:00Z",
                "overall_pass": True,
                "replay": {
                    "workload_metrics": [
                        {"concurrency": 1, "pass_rate_percent": 80.0, "retry_recovery_rate": 1.0}
                    ]
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    candidate_replay_path = starter / "runs" / "candidate-replay.json"
    candidate_replay_path.write_text(
        json.dumps(
            {
                "benchmark_report_version": "v1",
                "generated_at": "2026-03-22T11:00:00Z",
                "overall_pass": True,
                "replay": {
                    "workload_metrics": [
                        {"concurrency": 1, "pass_rate_percent": 90.0, "retry_recovery_rate": 1.0}
                    ]
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    baseline_canary_dir = starter / "runs" / "baseline-canaries"
    candidate_canary_dir = starter / "runs" / "candidate-canaries"
    baseline_canary_dir.mkdir(parents=True, exist_ok=True)
    candidate_canary_dir.mkdir(parents=True, exist_ok=True)
    for path, ok in [
        (baseline_canary_dir / "one.summary.json", True),
        (baseline_canary_dir / "two.summary.json", True),
        (candidate_canary_dir / "one.summary.json", True),
        (candidate_canary_dir / "two.summary.json", False),
    ]:
        path.write_text(
            json.dumps(
                {
                    "generated_at": "2026-03-22T10:00:00Z",
                    "overall_ok": ok,
                    "supported_pi_version": "0.61.1",
                    "git_sha": "abc123",
                    "scenario_totals": {"total": 6, "passed": 6 if ok else 5, "failed": 0 if ok else 1},
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    report_path = starter / "runs" / "policy-candidate-report.json"
    subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "evaluate_policy_candidate.py"),
            "--candidate",
            str(candidate_path),
            "--baseline-replay-report",
            str(baseline_replay_path),
            "--candidate-replay-report",
            str(candidate_replay_path),
            "--baseline-canary-summary-glob",
            str(baseline_canary_dir / "*.summary.json"),
            "--candidate-canary-summary-glob",
            str(candidate_canary_dir / "*.summary.json"),
            "--out",
            str(report_path),
            "--promote-if-passed",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ | {"PYTHONPATH": str(starter / "bin")},
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    promoted_manifest = json.loads(candidate_path.read_text(encoding="utf-8"))
    assert report["comparison"]["activation_approved"] is False
    assert report["comparison"]["threshold_results"]["candidate_canary_pass"] is False
    assert promoted_manifest["promotion"]["activation_approved"] is False
    assert promoted_manifest["mode"] == "shadow"
