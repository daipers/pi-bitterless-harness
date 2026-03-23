#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import pathlib
from typing import Any

from harnesslib import now_utc, sha256_file, sha256_text, write_json

CANDIDATE_TYPES = {"retrieval", "policy", "model", "bundle"}
CANDIDATE_MODES = {"off", "shadow", "active"}
CANDIDATE_ENV_VARS = {
    "retrieval": "HARNESS_RETRIEVAL_CANDIDATE_PATH",
    "policy": "HARNESS_POLICY_CANDIDATE_PATH",
    "model": "HARNESS_MODEL_CANDIDATE_PATH",
    "bundle": "HARNESS_BUNDLE_CANDIDATE_PATH",
}
DEFAULT_CANDIDATE_PATHS = {
    "retrieval": "candidates/retrieval/active.json",
    "policy": "candidates/policy/active.json",
    "model": "candidates/model/active.json",
    "bundle": "candidates/bundle/active.json",
}


def script_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def _coerce_path(path: str | pathlib.Path, *, repo_root: pathlib.Path | None) -> pathlib.Path:
    candidate = pathlib.Path(path)
    if candidate.is_absolute():
        return candidate
    return (repo_root or script_root()) / candidate


def resolve_candidate_manifest_path(
    candidate_type: str,
    candidate_path: str | pathlib.Path | None = None,
    *,
    repo_root: pathlib.Path | None = None,
) -> pathlib.Path | None:
    if candidate_type not in CANDIDATE_TYPES:
        raise ValueError(f"unsupported candidate type: {candidate_type}")
    if candidate_path is None:
        env_value = os.environ.get(CANDIDATE_ENV_VARS[candidate_type], "").strip()
        if env_value:
            candidate_path = env_value
    if candidate_path is not None:
        return _coerce_path(candidate_path, repo_root=repo_root)
    default_path = (repo_root or script_root()) / DEFAULT_CANDIDATE_PATHS[candidate_type]
    return default_path if default_path.exists() else None


def validate_candidate_manifest(payload: dict[str, Any], *, candidate_type: str) -> list[str]:
    errors: list[str] = []
    if payload.get("candidate_manifest_version") != "v1":
        errors.append("candidate_manifest_version must be v1")
    if payload.get("candidate_type") != candidate_type:
        errors.append(f"candidate_type must be {candidate_type!r}")
    if not isinstance(payload.get("candidate_id"), str) or not payload["candidate_id"]:
        errors.append("candidate_id must be a non-empty string")
    if payload.get("mode") not in CANDIDATE_MODES:
        errors.append("mode must be one of: off, shadow, active")
    if not isinstance(payload.get("created_at"), str) or not payload["created_at"]:
        errors.append("created_at must be a non-empty string")
    if (
        not isinstance(payload.get("artifact_fingerprint"), str)
        or not payload["artifact_fingerprint"]
    ):
        errors.append("artifact_fingerprint must be a non-empty string")
    if not isinstance(payload.get("training_dataset_fingerprints"), dict):
        errors.append("training_dataset_fingerprints must be an object")
    if not isinstance(payload.get("evaluation_dataset_fingerprints"), dict):
        errors.append("evaluation_dataset_fingerprints must be an object")
    if not isinstance(payload.get("runtime"), dict):
        errors.append("runtime must be an object")
    if not isinstance(payload.get("promotion"), dict):
        errors.append("promotion must be an object")
    return errors


def load_candidate_manifest(
    candidate_type: str,
    candidate_path: str | pathlib.Path | None = None,
    *,
    repo_root: pathlib.Path | None = None,
) -> dict[str, Any] | None:
    path = resolve_candidate_manifest_path(candidate_type, candidate_path, repo_root=repo_root)
    if path is None or not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("candidate manifest must be a JSON object")
    errors = validate_candidate_manifest(payload, candidate_type=candidate_type)
    if errors:
        raise ValueError("; ".join(errors))
    loaded = dict(payload)
    loaded["path"] = str(path)
    return loaded


def candidate_mode(candidate: dict[str, Any] | None) -> str:
    if not candidate:
        return "off"
    mode = str(candidate.get("mode", "off"))
    return mode if mode in CANDIDATE_MODES else "off"


def effective_candidate_mode(candidate: dict[str, Any] | None) -> str:
    mode = candidate_mode(candidate)
    if mode != "active":
        return mode
    promotion = candidate.get("promotion", {}) if isinstance(candidate, dict) else {}
    if not isinstance(promotion, dict):
        return "shadow"
    return "active" if promotion.get("activation_approved") is True else "shadow"


def candidate_runtime(candidate: dict[str, Any] | None) -> dict[str, Any]:
    payload = candidate or {}
    runtime = payload.get("runtime", {})
    return dict(runtime) if isinstance(runtime, dict) else {}


def candidate_summary(candidate: dict[str, Any] | None) -> dict[str, Any]:
    if not candidate:
        return {
            "configured": False,
            "candidate_type": None,
            "candidate_id": None,
            "mode": "off",
            "path": None,
        }
    runtime = candidate_runtime(candidate)
    return {
        "configured": True,
        "candidate_type": candidate.get("candidate_type"),
        "candidate_id": candidate.get("candidate_id"),
        "bundle_id": candidate.get("bundle_id"),
        "mode": candidate_mode(candidate),
        "effective_mode": effective_candidate_mode(candidate),
        "path": candidate.get("path"),
        "artifact_fingerprint": candidate.get("artifact_fingerprint"),
        "training_dataset_fingerprints": dict(candidate.get("training_dataset_fingerprints", {})),
        "evaluation_dataset_fingerprints": dict(
            candidate.get("evaluation_dataset_fingerprints", {})
        ),
        "promotion": dict(candidate.get("promotion", {})),
        "runtime_versions": {
            "retriever_version": runtime.get("retriever_version"),
            "reranker_version": runtime.get("reranker_version"),
            "abstention_model_version": runtime.get("abstention_model_version"),
            "model_version": runtime.get("model_version"),
            "policy_model_version": runtime.get("policy_model_version"),
        },
    }


def sigmoid(value: float) -> float:
    clamped = max(-60.0, min(60.0, value))
    return 1.0 / (1.0 + math.exp(-clamped))


def _feature_payload_fingerprint(payload: dict[str, Any]) -> str:
    return sha256_text(json.dumps(payload, sort_keys=True, ensure_ascii=False))


def build_candidate_manifest(
    *,
    candidate_type: str,
    candidate_id: str,
    mode: str,
    runtime: dict[str, Any],
    training_dataset_fingerprints: dict[str, str] | None = None,
    evaluation_dataset_fingerprints: dict[str, str] | None = None,
    bundle_id: str | None = None,
    description: str = "",
    promotion: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "candidate_manifest_version": "v1",
        "candidate_type": candidate_type,
        "candidate_id": candidate_id,
        "bundle_id": bundle_id,
        "mode": mode,
        "created_at": now_utc(),
        "description": description,
        "artifact_fingerprint": _feature_payload_fingerprint(runtime),
        "training_dataset_fingerprints": dict(training_dataset_fingerprints or {}),
        "evaluation_dataset_fingerprints": dict(evaluation_dataset_fingerprints or {}),
        "runtime": dict(runtime),
        "promotion": dict(promotion or {}),
    }
    errors = validate_candidate_manifest(payload, candidate_type=candidate_type)
    if errors:
        raise ValueError("; ".join(errors))
    return payload


def write_candidate_manifest(path: pathlib.Path, payload: dict[str, Any]) -> None:
    write_json(path, payload, sort_keys=False)


def build_candidate_report(
    *,
    candidate: dict[str, Any],
    benchmark_report: dict[str, Any] | None = None,
    baseline_report: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
    overall_pass: bool,
) -> dict[str, Any]:
    summary = candidate_summary(candidate)
    benchmark_payload = benchmark_report or {}
    baseline_payload = baseline_report or {}
    return {
        "candidate_report_version": "v1",
        "generated_at": now_utc(),
        "candidate_type": summary["candidate_type"],
        "candidate_id": summary["candidate_id"],
        "bundle_id": summary.get("bundle_id"),
        "mode": summary["mode"],
        "artifact_fingerprint": summary.get("artifact_fingerprint"),
        "candidate_manifest_path": summary.get("path"),
        "training_dataset_fingerprints": summary.get("training_dataset_fingerprints", {}),
        "evaluation_dataset_fingerprints": summary.get("evaluation_dataset_fingerprints", {}),
        "benchmark_report_path": benchmark_payload.get("_path"),
        "baseline_report_path": baseline_payload.get("_path"),
        "promotion_summary": dict(benchmark_payload.get("promotion_summary", {})),
        "metrics": dict(metrics or {}),
        "baseline_metrics": dict(baseline_payload.get("retrieval", {}))
        if isinstance(baseline_payload.get("retrieval"), dict)
        else {},
        "overall_pass": bool(overall_pass),
    }


def write_jsonl(path: pathlib.Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n")


def dataset_manifest_entry(path: pathlib.Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size if path.exists() else 0,
    }
