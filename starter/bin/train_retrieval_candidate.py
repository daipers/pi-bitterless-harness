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
    train_dense_feature_weights,
    write_dense_retriever_artifacts,
)
from harnesslib import now_utc, sha256_file, sha256_text
from learninglib import build_candidate_manifest, sigmoid, write_candidate_manifest

DEFAULT_FEATURE_KEYS = (
    "task_title_overlap",
    "goal_overlap",
    "constraints_overlap",
    "done_overlap",
    "summary_overlap",
    "claim_overlap",
    "artifact_overlap",
    "evidence_path_overlap",
    "phrase_bonus",
    "quality_prior",
    "total_score",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train a retrieval candidate from learning data")
    parser.add_argument("--examples", required=True, help="retrieval-examples.jsonl path")
    parser.add_argument(
        "--documents",
        help="retrieval-documents.jsonl path; required for dense-v1 retrievers",
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
        choices=["dense-v1", "lexical-v1"],
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


def features_for_candidate(candidate: dict[str, Any]) -> dict[str, float]:
    score_breakdown = dict(candidate.get("score_breakdown", {}))
    features = {key: float(score_breakdown.get(key, 0.0)) for key in DEFAULT_FEATURE_KEYS}
    features["total_score"] = float(candidate.get("total_score", 0.0))
    return features


def dot(weights: dict[str, float], features: dict[str, float], *, bias: float = 0.0) -> float:
    score = bias
    for key, value in features.items():
        score += float(weights.get(key, 0.0)) * float(value)
    return score


def train_weights(
    examples: list[dict[str, Any]],
    *,
    epochs: int,
    learning_rate: float,
    margin: float,
) -> tuple[dict[str, float], dict[str, int]]:
    weights = {key: 0.0 for key in DEFAULT_FEATURE_KEYS}
    stats = {"pair_count": 0, "update_count": 0, "positive_examples": 0, "abstain_examples": 0}
    for example in examples:
        if example.get("abstention_label") is True:
            stats["abstain_examples"] += 1
        candidate_set = list(example.get("candidate_set", []))
        positive_ids = {
            str(item) for item in example.get("gold_source_run_ids", []) if str(item)
        }
        if positive_ids and candidate_set:
            stats["positive_examples"] += 1
    for _ in range(max(1, epochs)):
        for example in examples:
            candidate_set = list(example.get("candidate_set", []))
            positive_ids = {
                str(item) for item in example.get("gold_source_run_ids", []) if str(item)
            }
            if not positive_ids:
                continue
            positives = [item for item in candidate_set if str(item.get("run_id")) in positive_ids]
            negatives = [
                item for item in candidate_set if str(item.get("run_id")) not in positive_ids
            ]
            if not positives or not negatives:
                continue
            for positive in positives:
                pos_features = features_for_candidate(positive)
                pos_score = dot(weights, pos_features)
                for negative in negatives:
                    neg_features = features_for_candidate(negative)
                    neg_score = dot(weights, neg_features)
                    stats["pair_count"] += 1
                    if pos_score > neg_score + margin:
                        continue
                    for key in DEFAULT_FEATURE_KEYS:
                        weights[key] += learning_rate * (pos_features[key] - neg_features[key])
                    stats["update_count"] += 1
    return weights, stats


def calibrate_bias(
    examples: list[dict[str, Any]],
    weights: dict[str, float],
) -> float:
    positive_scores: list[float] = []
    negative_scores: list[float] = []
    for example in examples:
        positive_ids = {str(item) for item in example.get("gold_source_run_ids", []) if str(item)}
        for candidate in example.get("candidate_set", []):
            score = dot(weights, features_for_candidate(candidate))
            run_id = str(candidate.get("run_id"))
            if run_id in positive_ids:
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
    weights: dict[str, float],
    *,
    bias: float,
    probability_threshold: float,
    max_selected_sources: int,
) -> tuple[list[dict[str, Any]], float]:
    scored: list[dict[str, Any]] = []
    for candidate in example.get("candidate_set", []):
        probability = sigmoid(dot(weights, features_for_candidate(candidate), bias=bias))
        scored.append({**candidate, "usefulness_probability": probability})
    scored.sort(
        key=lambda item: (-float(item["usefulness_probability"]), str(item.get("run_id", "")))
    )
    selected = [
        item for item in scored if float(item["usefulness_probability"]) >= probability_threshold
    ][:max(1, max_selected_sources)]
    top_probability = float(scored[0]["usefulness_probability"]) if scored else 0.0
    return selected, top_probability


def threshold_objective(
    examples: list[dict[str, Any]],
    weights: dict[str, float],
    *,
    bias: float,
    threshold: float,
    max_selected_sources: int,
) -> float:
    score = 0.0
    for example in examples:
        selected, top_probability = select_candidates(
            example,
            weights,
            bias=bias,
            probability_threshold=threshold,
            max_selected_sources=max_selected_sources,
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
    weights: dict[str, float],
    *,
    bias: float,
    max_selected_sources: int,
) -> float:
    best_threshold = 0.5
    best_score = float("-inf")
    for threshold_basis in range(10, 91, 5):
        threshold = threshold_basis / 100.0
        objective = threshold_objective(
            examples,
            weights,
            bias=bias,
            threshold=threshold,
            max_selected_sources=max_selected_sources,
        )
        if objective > best_score:
            best_score = objective
            best_threshold = threshold
    return round(best_threshold, 4)


def training_summary(
    examples: list[dict[str, Any]],
    weights: dict[str, float],
    *,
    bias: float,
    probability_threshold: float,
    max_selected_sources: int,
) -> dict[str, Any]:
    useful_hits = 0
    abstain_hits = 0
    abstain_total = 0
    useful_total = 0
    for example in examples:
        selected, top_probability = select_candidates(
            example,
            weights,
            bias=bias,
            probability_threshold=probability_threshold,
            max_selected_sources=max_selected_sources,
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


def dense_documents_by_run_id(path: pathlib.Path) -> tuple[dict[str, dict[str, Any]], str]:
    rows = read_jsonl(path)
    documents_by_run_id = {
        str(row.get("run_id")): row for row in rows if str(row.get("run_id", "")).strip()
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
    return documents_by_run_id, payload_fingerprint


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    examples_path = pathlib.Path(args.examples).resolve()
    out_path = pathlib.Path(args.out).resolve()
    examples = read_jsonl(examples_path)
    if not examples:
        raise SystemExit("no retrieval examples found")

    weights, stats = train_weights(
        examples,
        epochs=max(1, args.epochs),
        learning_rate=max(0.0001, args.learning_rate),
        margin=max(0.0, args.margin),
    )
    bias = round(calibrate_bias(examples, weights), 6)
    probability_threshold = fit_probability_threshold(
        examples,
        weights,
        bias=bias,
        max_selected_sources=max(1, args.max_selected_sources),
    )
    summary = training_summary(
        examples,
        weights,
        bias=bias,
        probability_threshold=probability_threshold,
        max_selected_sources=max(1, args.max_selected_sources),
    )

    retriever_type = str(args.retriever_type)
    retriever_runtime: dict[str, Any] = {"retriever_type": retriever_type}
    retriever_version = "lexical-stage1-v1"
    training_dataset_fingerprints = {"retrieval_examples": sha256_file(examples_path) or ""}
    description = "Learned linear reranker trained from retrieval examples."

    dense_stats: dict[str, Any] = {}
    if retriever_type == "dense-v1":
        if not args.documents:
            raise SystemExit("--documents is required for dense-v1 retrievers")
        documents_path = pathlib.Path(args.documents).resolve()
        documents_by_run_id, document_fingerprint = dense_documents_by_run_id(documents_path)
        if not documents_by_run_id:
            raise SystemExit("no retrieval documents found")
        dense_weights, dense_stats = train_dense_feature_weights(
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
        retriever_runtime = write_dense_retriever_artifacts(
            artifacts_dir,
            feature_weights=dense_weights,
            hash_dim=max(1, int(args.hash_dim)),
            embedding_dim=max(1, int(args.embedding_dim)),
            projection_seed=int(args.projection_seed),
            document_fingerprint=document_fingerprint,
        )
        retriever_version = "dense-hashed-shared-encoder-v1"
        training_dataset_fingerprints["retrieval_documents"] = sha256_file(documents_path) or ""
        summary = {
            **summary,
            "dense_document_count": len(documents_by_run_id),
            "dense_pair_count": dense_stats.get("pair_count", 0),
            "dense_update_count": dense_stats.get("update_count", 0),
            "dense_positive_examples": dense_stats.get("positive_examples", 0),
            "dense_abstain_examples": dense_stats.get("abstain_examples", 0),
        }
        description = "Dense hashed shared-encoder retriever with learned linear reranker."

    candidate_payload = build_candidate_manifest(
        candidate_type="retrieval",
        candidate_id=args.candidate_id
        or candidate_id_for_path(out_path, examples_path=examples_path),
        mode=args.mode,
        runtime={
            "retriever_version": retriever_version,
            "retriever": retriever_runtime,
            "reranker_version": "learned-linear-reranker-v1",
            "abstention_model_version": "threshold-calibrator-v1",
            "selection": {
                "stage1_k": max(1, args.stage1_k),
                "max_selected_sources": max(1, args.max_selected_sources),
            },
            "reranker": {
                "bias": bias,
                "feature_weights": {key: round(value, 6) for key, value in weights.items()},
                "margin": max(0.0, args.margin),
                "epochs": max(1, args.epochs),
                "learning_rate": max(0.0001, args.learning_rate),
            },
            "abstention": {"probability_threshold": probability_threshold},
            "training_summary": {**summary, **stats, "retriever_type": retriever_type},
        },
        training_dataset_fingerprints=training_dataset_fingerprints,
        evaluation_dataset_fingerprints={},
        bundle_id=args.bundle_id,
        description=description,
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
