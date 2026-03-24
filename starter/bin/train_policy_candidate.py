#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

from harnesslib import DEFAULT_RETRIEVAL_CONFIG, now_utc, sha256_file
from learninglib import build_candidate_manifest, write_candidate_manifest
from policylib import predict_policy_heads, train_contextual_policy_model

DEFAULT_ACTIVATION_THRESHOLD = 0.6


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train a policy candidate from learning data")
    parser.add_argument("--examples", required=True, help="policy-examples.jsonl path")
    parser.add_argument("--out", required=True, help="candidate manifest output path")
    parser.add_argument("--candidate-id", help="optional candidate id override")
    parser.add_argument("--bundle-id", help="optional bundle id")
    parser.add_argument("--activation-threshold", type=float, default=DEFAULT_ACTIVATION_THRESHOLD)
    parser.add_argument("--mode", choices=["off", "shadow", "active"], default="shadow")
    return parser.parse_args(argv)


def read_jsonl(path: pathlib.Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        payload = json.loads(raw_line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def candidate_id_for_path(out_path: pathlib.Path, *, examples_path: pathlib.Path) -> str:
    fingerprint = sha256_file(examples_path) or "unknown"
    stem = out_path.stem.replace("_", "-")
    return f"{stem}-{fingerprint[:8]}"


def policy_rows(examples: list[dict[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for example in examples:
        features = example.get("features", {})
        labels = example.get("labels", {})
        if isinstance(features, dict) and isinstance(labels, dict):
            rows.append((features, labels))
    return rows


def training_summary(rows: list[tuple[dict[str, Any], dict[str, Any]]], *, model: dict[str, Any]) -> dict[str, Any]:
    predictions = predict_policy_heads(
        model,
        {
            "task_text": "",
            "execution_profile": "",
            "policy_path": "",
            "eval_command_count": 0,
            "required_artifact_count": 0,
            "selected_source_count": 0,
            "candidate_run_count": 0,
            "duration_ms": 0,
            "failure_classification_count": 0,
            "top_candidate_score": 0,
            "ranking_latency_ms": 0,
            "abstained": False,
            "context_empty": True,
        },
    )
    successful = [labels for _features, labels in rows if bool(labels.get("overall_pass", False))]
    return {
        "example_count": len(rows),
        "success_count": len(successful),
        "trained_at": now_utc(),
        "head_count": len(model.get("heads", {})),
        "default_predictions": predictions,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    examples_path = pathlib.Path(args.examples).resolve()
    out_path = pathlib.Path(args.out).resolve()
    examples = read_jsonl(examples_path)
    rows = policy_rows(examples)
    if not rows:
        raise SystemExit("no policy examples found")

    defaults = {
        "execution_profile": "strict",
        "retrieval_budget": {
            "max_source_runs": int(DEFAULT_RETRIEVAL_CONFIG["max_source_runs"]),
            "max_candidates": int(DEFAULT_RETRIEVAL_CONFIG["max_candidates"]),
        },
        "retry_policy": {"retry_limit": 2},
        "benchmark_eligibility": False,
        "capability_profile": None,
    }
    model = train_contextual_policy_model(rows, defaults=defaults)
    artifacts_dir = pathlib.Path(f"{out_path}.artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    model_path = artifacts_dir / "policy-model.json"
    model_path.write_text(json.dumps(model, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    candidate_payload = build_candidate_manifest(
        candidate_type="policy",
        candidate_id=args.candidate_id
        or candidate_id_for_path(out_path, examples_path=examples_path),
        mode=args.mode,
        runtime={
            "policy_model_version": "contextual-policy-v2",
            "activation_threshold": max(0.0, min(1.0, float(args.activation_threshold))),
            "model": {
                "model_version": "contextual-policy-v2",
                "artifact_paths": {"model_path": str(model_path.resolve())},
                "vector_dim": int(model.get("vector_dim", 128)),
            },
            "heads": {
                "execution_profile": {"default": defaults["execution_profile"]},
                "retrieval_budget": {"default": defaults["retrieval_budget"]},
                "retry_policy": {"default": defaults["retry_policy"]},
                "benchmark_eligibility": {"default": defaults["benchmark_eligibility"]},
                "capability_profile": {"default": defaults["capability_profile"]},
            },
            "defaults": defaults,
            "recommendations": {
                "execution_profile": {"value": defaults["execution_profile"], "confidence": 0.0},
                "retry_limit": {"value": defaults["retry_policy"]["retry_limit"], "confidence": 0.0},
                "context_budget": {"value": defaults["retrieval_budget"], "confidence": 0.0},
                "benchmark_eligible": {
                    "value": defaults["benchmark_eligibility"],
                    "confidence": 0.0,
                },
            },
            "training_summary": training_summary(rows, model=model),
        },
        training_dataset_fingerprints={"policy_examples": sha256_file(examples_path) or ""},
        evaluation_dataset_fingerprints={},
        bundle_id=args.bundle_id,
        description="Contextual policy recommendations learned from task and runtime features.",
        promotion={
            "activation_approved": False,
            "approved_at": None,
            "approval_reason": "candidate not yet replay/canary evaluated",
        },
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_candidate_manifest(out_path, candidate_payload)
    print(json.dumps(candidate_payload, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
