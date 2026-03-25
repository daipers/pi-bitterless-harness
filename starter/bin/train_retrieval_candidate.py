#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import statistics
from typing import Any

from dense_retrieval import (
    DEFAULT_EMBEDDING_DIM,
    DEFAULT_HASH_DIM,
    DEFAULT_PROJECTION_SEED,
    load_text_encoder_runtime,
    score_pair,
    train_dense_feature_weights,
    write_dense_retriever_artifacts,
)
from harnesslib import now_utc, sha256_file, sha256_text
from learninglib import build_candidate_manifest, sigmoid, write_candidate_manifest

DEFAULT_RETRIEVER_TYPE = "local-hashed-v2"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train a retrieval candidate from learning data")
    parser.add_argument("--examples", required=True, help="retrieval-examples.jsonl path")
    parser.add_argument(
        "--documents",
        help="retrieval-documents.jsonl path; optional but recommended for broader text coverage",
    )
    parser.add_argument("--out", required=True, help="candidate manifest output path")
    parser.add_argument("--candidate-id", help="optional candidate id override")
    parser.add_argument("--bundle-id", help="optional bundle id")
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--margin", type=float, default=0.1)
    parser.add_argument("--stage1-k", type=int, default=25)
    parser.add_argument("--max-selected-sources", type=int, default=3)
    parser.add_argument(
        "--retriever-type",
        choices=["lexical-v1", "dense-v1"],
        default="lexical-v1",
    )
    parser.add_argument("--hash-dim", type=int, default=DEFAULT_HASH_DIM)
    parser.add_argument("--embedding-dim", type=int, default=DEFAULT_EMBEDDING_DIM)
    parser.add_argument("--projection-seed", type=int, default=DEFAULT_PROJECTION_SEED)
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


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _query_payload(example: dict[str, Any]) -> dict[str, Any]:
    payload = example.get("query", {})
    return dict(payload) if isinstance(payload, dict) else {}


def query_text(example: dict[str, Any]) -> str:
    query = _query_payload(example)
    explicit = str(query.get("text", "")).strip()
    if explicit:
        return explicit
    return "\n".join(
        [
            str(query.get("task_title", "")),
            str(query.get("goal", "")),
            str(query.get("constraints", "")),
            str(query.get("done", "")),
        ]
    ).strip()


def candidate_document_text(candidate: dict[str, Any]) -> str:
    document = candidate.get("document", {})
    if isinstance(document, dict):
        explicit = str(document.get("text", "")).strip()
        if explicit:
            return explicit
    explicit = str(candidate.get("document_text", "")).strip()
    if explicit:
        return explicit
    parts = [str(candidate.get("summary", "")).strip()]
    claims = candidate.get("claims", [])
    if isinstance(claims, list):
        parts.extend(str(item).strip() for item in claims if str(item).strip())
    evidence_paths = candidate.get("evidence_paths", [])
    if isinstance(evidence_paths, list):
        parts.extend(str(item).strip() for item in evidence_paths if str(item).strip())
    for artifact in candidate.get("artifact_records", []):
        if not isinstance(artifact, dict):
            continue
        parts.append(str(artifact.get("description", "")).strip())
        parts.append(str(artifact.get("excerpt", "")).strip())
    text = "\n".join(part for part in parts if part).strip()
    if text:
        return text
    score_breakdown = candidate.get("score_breakdown", {})
    if isinstance(score_breakdown, dict) and score_breakdown:
        return " ".join(
            [str(candidate.get("run_id", ""))]
            + [
                f"{key}:{value}"
                for key, value in sorted(score_breakdown.items())
                if safe_float(value) != 0.0
            ]
        ).strip()
    return str(candidate.get("run_id", "")).strip()


def dense_documents_by_run_id(
    path: pathlib.Path | None,
    examples: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    documents_by_run_id: dict[str, dict[str, Any]] = {}
    fingerprints: dict[str, str] = {}
    if path is not None and path.exists():
        rows = read_jsonl(path)
        for row in rows:
            run_id = str(row.get("run_id", "")).strip()
            if not run_id:
                continue
            text = str(row.get("text", "")).strip()
            if not text:
                continue
            documents_by_run_id[run_id] = row
        fingerprints["retrieval_documents"] = sha256_file(path) or ""

    for example in examples:
        for candidate in example.get("candidate_set", []):
            if not isinstance(candidate, dict):
                continue
            run_id = str(candidate.get("run_id", "")).strip()
            text = candidate_document_text(candidate)
            if not run_id or not text or run_id in documents_by_run_id:
                continue
            documents_by_run_id[run_id] = {
                "run_id": run_id,
                "text": text,
                "source_snapshot_fingerprint": str(
                    candidate.get("source_snapshot_fingerprint", "")
                ),
            }

    payload_fingerprint = sha256_text(
        json.dumps(
            [
                {
                    "run_id": run_id,
                    "source_snapshot_fingerprint": document.get("source_snapshot_fingerprint"),
                    "text": document.get("text"),
                }
                for run_id, document in sorted(documents_by_run_id.items())
            ],
            sort_keys=True,
            ensure_ascii=False,
        )
    )
    fingerprints["retrieval_document_payload"] = payload_fingerprint
    return documents_by_run_id, fingerprints


def candidate_scores(
    example: dict[str, Any],
    *,
    runtime: dict[str, Any],
    document_text_by_run_id: dict[str, str],
) -> list[dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    query_value = query_text(example)
    for candidate in example.get("candidate_set", []):
        if not isinstance(candidate, dict):
            continue
        run_id = str(candidate.get("run_id", "")).strip()
        document_text = document_text_by_run_id.get(run_id) or candidate_document_text(candidate)
        score = score_pair(query_value, document_text, runtime=runtime) + float(
            runtime.get("bias", 0.0)
        )
        scored.append(
            {
                **candidate,
                "run_id": run_id,
                "document_text": document_text,
                "reranker_score": round(float(score), 6),
                "usefulness_probability": round(sigmoid(score), 6),
            }
        )
    scored.sort(
        key=lambda item: (-float(item["usefulness_probability"]), str(item.get("run_id", "")))
    )
    return scored


def calibrate_bias(
    examples: list[dict[str, Any]],
    *,
    runtime: dict[str, Any],
    document_text_by_run_id: dict[str, str],
) -> float:
    positive_scores: list[float] = []
    negative_scores: list[float] = []
    for example in examples:
        positive_ids = {str(item) for item in example.get("gold_source_run_ids", []) if str(item)}
        for candidate in candidate_scores(
            example,
            runtime=runtime,
            document_text_by_run_id=document_text_by_run_id,
        ):
            score = float(candidate.get("reranker_score", 0.0))
            if str(candidate.get("run_id")) in positive_ids:
                positive_scores.append(score)
            else:
                negative_scores.append(score)
    if positive_scores and negative_scores:
        return -(
            (statistics.median(positive_scores) + statistics.median(negative_scores)) / 2.0
        )
    if positive_scores:
        return -statistics.median(positive_scores)
    if negative_scores:
        return -statistics.median(negative_scores)
    return 0.0


def select_candidates(
    example: dict[str, Any],
    *,
    runtime: dict[str, Any],
    probability_threshold: float,
    max_selected_sources: int,
    document_text_by_run_id: dict[str, str],
) -> tuple[list[dict[str, Any]], float]:
    scored = candidate_scores(
        example,
        runtime=runtime,
        document_text_by_run_id=document_text_by_run_id,
    )
    selected = [
        item for item in scored if float(item["usefulness_probability"]) >= probability_threshold
    ][:max(1, max_selected_sources)]
    top_probability = float(scored[0]["usefulness_probability"]) if scored else 0.0
    return selected, top_probability


def threshold_objective(
    examples: list[dict[str, Any]],
    *,
    runtime: dict[str, Any],
    threshold: float,
    max_selected_sources: int,
    document_text_by_run_id: dict[str, str],
) -> float:
    score = 0.0
    for example in examples:
        selected, top_probability = select_candidates(
            example,
            runtime=runtime,
            probability_threshold=threshold,
            max_selected_sources=max_selected_sources,
            document_text_by_run_id=document_text_by_run_id,
        )
        selected_ids = {str(item.get("run_id")) for item in selected}
        gold_ids = {str(item) for item in example.get("gold_source_run_ids", []) if str(item)}
        expected_abstain = bool(example.get("abstention_label", False))
        predicted_abstain = top_probability < threshold or not selected
        if predicted_abstain == expected_abstain:
            score += 1.0
        if not expected_abstain:
            if gold_ids and selected_ids.intersection(gold_ids):
                score += 1.0
            elif predicted_abstain:
                score -= 0.25
        elif predicted_abstain:
            score += 0.25
    return score


def fit_probability_threshold(
    examples: list[dict[str, Any]],
    *,
    runtime: dict[str, Any],
    max_selected_sources: int,
    document_text_by_run_id: dict[str, str],
) -> float:
    best_threshold = 0.5
    best_score = float("-inf")
    for threshold_basis in range(10, 91, 5):
        threshold = threshold_basis / 100.0
        objective = threshold_objective(
            examples,
            runtime=runtime,
            threshold=threshold,
            max_selected_sources=max_selected_sources,
            document_text_by_run_id=document_text_by_run_id,
        )
        if objective > best_score:
            best_score = objective
            best_threshold = threshold
    return round(best_threshold, 4)


def training_summary(
    examples: list[dict[str, Any]],
    *,
    runtime: dict[str, Any],
    probability_threshold: float,
    max_selected_sources: int,
    document_text_by_run_id: dict[str, str],
) -> dict[str, Any]:
    useful_hits = 0
    abstain_hits = 0
    abstain_total = 0
    useful_total = 0
    for example in examples:
        selected, top_probability = select_candidates(
            example,
            runtime=runtime,
            probability_threshold=probability_threshold,
            max_selected_sources=max_selected_sources,
            document_text_by_run_id=document_text_by_run_id,
        )
        selected_ids = {str(item.get("run_id")) for item in selected}
        gold_ids = {str(item) for item in example.get("gold_source_run_ids", []) if str(item)}
        expected_abstain = bool(example.get("abstention_label", False))
        if expected_abstain:
            abstain_total += 1
            if top_probability < probability_threshold or not selected:
                abstain_hits += 1
        else:
            useful_total += 1
            if selected_ids.intersection(gold_ids):
                useful_hits += 1
    return {
        "example_count": len(examples),
        "useful_hit_rate": round(useful_hits / useful_total, 2) if useful_total else 0.0,
        "abstention_hit_rate": round(abstain_hits / abstain_total, 2) if abstain_total else 0.0,
        "probability_threshold": probability_threshold,
        "max_selected_sources": max_selected_sources,
        "trained_at": now_utc(),
    }


def candidate_id_for_path(out_path: pathlib.Path, *, examples_path: pathlib.Path) -> str:
    fingerprint = sha256_file(examples_path) or "unknown"
    stem = out_path.stem.replace("_", "-")
    return f"{stem}-{fingerprint[:8]}"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    examples_path = pathlib.Path(args.examples).resolve()
    out_path = pathlib.Path(args.out).resolve()
    examples = read_jsonl(examples_path)
    if not examples:
        raise SystemExit("no retrieval examples found")

    documents_path = pathlib.Path(args.documents).resolve() if args.documents else None
    documents_by_run_id, dataset_fingerprints = dense_documents_by_run_id(documents_path, examples)
    if not documents_by_run_id:
        raise SystemExit("no retrieval documents found")

    dense_weights, stats = train_dense_feature_weights(
        examples,
        documents_by_run_id,
        hash_dim=max(1, int(args.hash_dim)),
        embedding_dim=max(1, int(args.embedding_dim)),
        projection_seed=int(args.projection_seed),
        epochs=max(1, args.epochs),
        learning_rate=max(0.0001, args.learning_rate),
        margin=max(0.0, args.margin),
    )
    artifacts_dir = pathlib.Path(f"{out_path}.artifacts")
    encoder_runtime = write_dense_retriever_artifacts(
        artifacts_dir,
        feature_weights=dense_weights,
        hash_dim=max(1, int(args.hash_dim)),
        embedding_dim=max(1, int(args.embedding_dim)),
        projection_seed=int(args.projection_seed),
        document_fingerprint=dataset_fingerprints["retrieval_document_payload"],
    )
    runtime_encoder = load_text_encoder_runtime(encoder_runtime)
    document_text_by_run_id = {
        run_id: str(document.get("text", ""))
        for run_id, document in documents_by_run_id.items()
        if str(document.get("text", "")).strip()
    }
    bias = round(
        calibrate_bias(
            examples,
            runtime=runtime_encoder,
            document_text_by_run_id=document_text_by_run_id,
        ),
        6,
    )
    calibrated_runtime = dict(runtime_encoder)
    calibrated_runtime["bias"] = bias
    probability_threshold = fit_probability_threshold(
        examples,
        runtime=calibrated_runtime,
        max_selected_sources=max(1, args.max_selected_sources),
        document_text_by_run_id=document_text_by_run_id,
    )
    summary = training_summary(
        examples,
        runtime=calibrated_runtime,
        probability_threshold=probability_threshold,
        max_selected_sources=max(1, args.max_selected_sources),
        document_text_by_run_id=document_text_by_run_id,
    )

    training_dataset_fingerprints = {
        "retrieval_examples": sha256_file(examples_path) or "",
        **dataset_fingerprints,
    }
    candidate_payload = build_candidate_manifest(
        candidate_type="retrieval",
        candidate_id=args.candidate_id
        or candidate_id_for_path(out_path, examples_path=examples_path),
        mode=args.mode,
        runtime={
            "retriever_version": (
                "dense-hashed-shared-encoder-v1"
                if args.retriever_type == "dense-v1"
                else "lexical-stage1-v2"
            ),
            "reranker_version": "text-pair-hashed-reranker-v2",
            "abstention_model_version": "threshold-calibrator-v2",
            "stage1": {
                "type": "lexical-v1" if args.retriever_type == "lexical-v1" else "dense-v1",
                "max_candidates": max(1, args.stage1_k),
                "max_source_runs": max(1, args.max_selected_sources),
            },
            "selection": {
                "stage1_k": max(1, args.stage1_k),
                "max_selected_sources": max(1, args.max_selected_sources),
            },
            "reranker": {
                "model_version": "text-pair-hashed-reranker-v2",
                "input_schema_version": "retrieval-example-v2",
                "score_mode": "cosine",
                "bias": bias,
                "artifact_paths": {
                    "feature_weights_path": encoder_runtime["feature_weights_path"],
                    "config_path": encoder_runtime["config_path"],
                },
                "encoder": encoder_runtime,
            },
            "abstention": {
                "model_version": "threshold-calibrator-v2",
                "threshold": probability_threshold,
                "artifact_paths": {
                    "config_path": encoder_runtime["config_path"],
                },
                "probability_threshold": probability_threshold,
            },
            "training_summary": {
                **summary,
                **stats,
                "retriever_type": DEFAULT_RETRIEVER_TYPE,
                "document_count": len(documents_by_run_id),
            },
            "retriever": encoder_runtime,
        },
        training_dataset_fingerprints=training_dataset_fingerprints,
        evaluation_dataset_fingerprints={},
        bundle_id=args.bundle_id,
        description="Local lightweight retrieval candidate with lexical stage1 and learned text-pair reranker.",
        promotion={
            "activation_approved": False,
            "approved_at": None,
            "approval_reason": "candidate not yet benchmarked",
        },
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_candidate_manifest(out_path, candidate_payload)
    print(json.dumps(candidate_payload, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
