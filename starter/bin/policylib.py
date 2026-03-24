#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import math
from typing import Any

import numpy

DEFAULT_POLICY_VECTOR_DIM = 128
NUMERIC_FEATURES = (
    "eval_command_count",
    "required_artifact_count",
    "selected_source_count",
    "candidate_run_count",
    "duration_ms",
    "failure_classification_count",
    "top_candidate_score",
    "ranking_latency_ms",
)


def _stable_hash(token: str, *, dim: int) -> int:
    digest = hashlib.sha256(token.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False) % max(1, dim)


def _tokenize(text: str) -> list[str]:
    return __import__("re").findall(r"[a-z0-9_./-]+", text.lower())


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def policy_feature_payload(example: dict[str, Any]) -> dict[str, Any]:
    features = example.get("features", {}) if isinstance(example.get("features"), dict) else {}
    observed = example.get("observed", {}) if isinstance(example.get("observed"), dict) else {}
    task = observed.get("task", {}) if isinstance(observed.get("task"), dict) else {}
    task_text = str(features.get("task_text", "")).strip()
    if not task_text:
        task_text = "\n".join(
            [
                str(task.get("title", "")),
                str(task.get("goal", "")),
                str(task.get("constraints", "")),
                str(task.get("done", "")),
            ]
        ).strip()
    return {
        "task_text": task_text,
        "execution_profile": str(features.get("execution_profile", "")),
        "policy_path": str(features.get("policy_path", "")),
        "eval_command_count": safe_float(features.get("eval_command_count")),
        "required_artifact_count": safe_float(features.get("required_artifact_count")),
        "selected_source_count": safe_float(features.get("selected_source_count")),
        "candidate_run_count": safe_float(features.get("candidate_run_count")),
        "duration_ms": safe_float(features.get("duration_ms")),
        "failure_classification_count": safe_float(features.get("failure_classification_count")),
        "top_candidate_score": safe_float(features.get("top_candidate_score")),
        "ranking_latency_ms": safe_float(features.get("ranking_latency_ms")),
        "abstained": bool(features.get("abstained", False)),
        "context_empty": bool(features.get("context_empty", False)),
    }


def vectorize_policy_payload(
    payload: dict[str, Any],
    *,
    dim: int = DEFAULT_POLICY_VECTOR_DIM,
) -> numpy.ndarray:
    vector = numpy.zeros(dim + len(NUMERIC_FEATURES) + 2, dtype=numpy.float32)
    text = "\n".join(
        [
            str(payload.get("task_text", "")),
            str(payload.get("execution_profile", "")),
            str(payload.get("policy_path", "")),
        ]
    )
    for token in _tokenize(text):
        vector[_stable_hash(token, dim=dim)] += 1.0
    offset = dim
    for index, feature_name in enumerate(NUMERIC_FEATURES):
        value = safe_float(payload.get(feature_name))
        vector[offset + index] = math.log1p(max(0.0, value))
    vector[offset + len(NUMERIC_FEATURES)] = 1.0 if bool(payload.get("abstained", False)) else 0.0
    vector[offset + len(NUMERIC_FEATURES) + 1] = (
        1.0 if bool(payload.get("context_empty", False)) else 0.0
    )
    norm = float(numpy.linalg.norm(vector))
    return vector if norm <= 0 else (vector / norm).astype(numpy.float32)


def _mean_vector(vectors: list[numpy.ndarray]) -> numpy.ndarray:
    if not vectors:
        return numpy.zeros(1, dtype=numpy.float32)
    mean = numpy.mean(numpy.stack(vectors), axis=0)
    norm = float(numpy.linalg.norm(mean))
    return mean if norm <= 0 else (mean / norm).astype(numpy.float32)


def _value_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def train_contextual_policy_model(
    rows: list[tuple[dict[str, Any], dict[str, Any]]],
    *,
    defaults: dict[str, Any],
    dim: int = DEFAULT_POLICY_VECTOR_DIM,
) -> dict[str, Any]:
    features = [vectorize_policy_payload(policy_feature_payload({"features": row[0]}), dim=dim) for row in rows]
    head_buckets: dict[str, dict[str, dict[str, Any]]] = {
        "execution_profile": {},
        "retrieval_budget": {},
        "retry_policy": {},
        "benchmark_eligibility": {},
        "capability_profile": {},
    }
    for index, (_feature_payload, labels) in enumerate(rows):
        vector = features[index]
        execution_profile = str(labels.get("execution_profile", "")).strip()
        if execution_profile:
            key = _value_key(execution_profile)
            bucket = head_buckets["execution_profile"].setdefault(
                key, {"value": execution_profile, "vectors": []}
            )
            bucket["vectors"].append(vector)

        context_budget = labels.get("context_budget")
        if isinstance(context_budget, dict):
            budget_value = {
                "max_source_runs": max(1, int(context_budget.get("selected_source_count", 1) or 1)),
                "max_candidates": max(
                    1,
                    int(
                        context_budget.get("candidate_run_count")
                        or context_budget.get("selected_source_count")
                        or 1
                    ),
                ),
            }
            key = _value_key(budget_value)
            bucket = head_buckets["retrieval_budget"].setdefault(
                key, {"value": budget_value, "vectors": []}
            )
            bucket["vectors"].append(vector)

        retry_limit = 3 if bool(labels.get("retry_recommended", False)) else 2
        retry_value = {"retry_limit": retry_limit}
        retry_bucket = head_buckets["retry_policy"].setdefault(
            _value_key(retry_value), {"value": retry_value, "vectors": []}
        )
        retry_bucket["vectors"].append(vector)

        benchmark_value = bool(labels.get("benchmark_eligible", False))
        benchmark_bucket = head_buckets["benchmark_eligibility"].setdefault(
            _value_key(benchmark_value), {"value": benchmark_value, "vectors": []}
        )
        benchmark_bucket["vectors"].append(vector)

        capability_profile = str(labels.get("capability_profile", "") or "").strip()
        if capability_profile:
            capability_bucket = head_buckets["capability_profile"].setdefault(
                _value_key(capability_profile), {"value": capability_profile, "vectors": []}
            )
            capability_bucket["vectors"].append(vector)

    heads: dict[str, Any] = {}
    for head_name, buckets in head_buckets.items():
        labels_payload: dict[str, Any] = {}
        for key, bucket in buckets.items():
            vectors = list(bucket["vectors"])
            if not vectors:
                continue
            centroid = _mean_vector(vectors)
            labels_payload[key] = {
                "value": bucket["value"],
                "support": len(vectors),
                "prototype": [round(float(value), 8) for value in centroid.tolist()],
            }
        if labels_payload:
            heads[head_name] = {"labels": labels_payload}
    return {
        "model_version": "contextual-policy-v2",
        "vector_dim": dim,
        "numeric_features": list(NUMERIC_FEATURES),
        "defaults": defaults,
        "heads": heads,
    }


def predict_policy_heads(model: dict[str, Any], payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    dim = int(model.get("vector_dim", DEFAULT_POLICY_VECTOR_DIM))
    query_vector = vectorize_policy_payload(payload, dim=dim)
    predictions: dict[str, dict[str, Any]] = {}
    for head_name, head_payload in dict(model.get("heads", {})).items():
        labels = dict(head_payload.get("labels", {}))
        best_label: dict[str, Any] | None = None
        best_score = float("-inf")
        for label_payload in labels.values():
            prototype = numpy.asarray(label_payload.get("prototype", []), dtype=numpy.float32)
            if prototype.shape != query_vector.shape:
                continue
            score = float(numpy.dot(query_vector, prototype))
            if score > best_score:
                best_score = score
                best_label = label_payload
        if best_label is None:
            continue
        predictions[head_name] = {
            "value": best_label.get("value"),
            "confidence": round(max(0.0, min(1.0, (best_score + 1.0) / 2.0)), 4),
            "support": int(best_label.get("support", 0)),
        }
    return predictions
