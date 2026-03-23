#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import pathlib
from typing import Any

from harnesslib import now_utc, sha256_file
from learninglib import build_candidate_manifest, write_candidate_manifest

DEFAULT_RETRY_LIMIT = 2
RECOMMENDED_RETRY_LIMIT = 3
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


def wilson_lower_bound(successes: int, total: int, *, z: float = 1.0) -> float:
    if total <= 0:
        return 0.0
    proportion = successes / total
    denominator = 1.0 + (z * z) / total
    center = proportion + (z * z) / (2.0 * total)
    margin = z * math.sqrt((proportion * (1.0 - proportion) + (z * z) / (4.0 * total)) / total)
    return max(0.0, min(1.0, (center - margin) / denominator))


def percentile(values: list[int], fraction: float) -> int:
    if not values:
        return 0
    ordered = sorted(int(value) for value in values)
    index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1))
    return ordered[index]


def policy_rows(examples: list[dict[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for example in examples:
        features = example.get("features", {})
        labels = example.get("labels", {})
        if isinstance(features, dict) and isinstance(labels, dict):
            rows.append((features, labels))
    return rows


def learn_execution_profile(rows: list[tuple[dict[str, Any], dict[str, Any]]]) -> tuple[str, dict[str, Any]]:
    profile_stats: dict[str, dict[str, int]] = {}
    for _features, labels in rows:
        profile = str(labels.get("execution_profile", "")).strip()
        if not profile:
            continue
        stats = profile_stats.setdefault(profile, {"total": 0, "success": 0})
        stats["total"] += 1
        if bool(labels.get("overall_pass", False)):
            stats["success"] += 1

    if not profile_stats:
        return "strict", {"value": "strict", "confidence": 0.0, "support": 0}

    ranked = []
    for profile, stats in profile_stats.items():
        support = int(stats["total"])
        successes = int(stats["success"])
        pass_rate = successes / support if support else 0.0
        lower_bound = wilson_lower_bound(successes, support)
        ranked.append((lower_bound, pass_rate, support, profile))
    ranked.sort(reverse=True)
    lower_bound, pass_rate, support, profile = ranked[0]
    return profile, {
        "value": profile,
        "confidence": round(lower_bound, 4),
        "support": support,
        "success_count": int(profile_stats[profile]["success"]),
        "pass_rate": round(pass_rate, 4),
        "lower_bound": round(lower_bound, 4),
    }


def learn_retry_limit(rows: list[tuple[dict[str, Any], dict[str, Any]]]) -> tuple[int, dict[str, Any]]:
    total = len(rows)
    retry_true = sum(1 for _features, labels in rows if bool(labels.get("retry_recommended", False)))
    retry_false = max(0, total - retry_true)
    positive_rate = retry_true / total if total else 0.0
    positive_lower_bound = wilson_lower_bound(retry_true, total)
    negative_lower_bound = wilson_lower_bound(retry_false, total)
    if retry_true > 0 and (positive_rate >= 0.25 or positive_lower_bound >= 0.15):
        return RECOMMENDED_RETRY_LIMIT, {
            "value": RECOMMENDED_RETRY_LIMIT,
            "confidence": round(max(positive_lower_bound, positive_rate), 4),
            "support": total,
            "positive_count": retry_true,
            "positive_rate": round(positive_rate, 4),
            "lower_bound": round(positive_lower_bound, 4),
        }
    return DEFAULT_RETRY_LIMIT, {
        "value": DEFAULT_RETRY_LIMIT,
        "confidence": round(max(negative_lower_bound, retry_false / total if total else 0.0), 4),
        "support": total,
        "negative_count": retry_false,
        "negative_rate": round(retry_false / total if total else 0.0, 4),
        "lower_bound": round(negative_lower_bound, 4),
    }


def learn_context_budget(rows: list[tuple[dict[str, Any], dict[str, Any]]]) -> tuple[dict[str, int], dict[str, Any]]:
    successful = [labels for _features, labels in rows if bool(labels.get("overall_pass", False))]
    selected_source_counts: list[int] = []
    candidate_run_counts: list[int] = []
    for labels in successful:
        context_budget = labels.get("context_budget", {})
        if not isinstance(context_budget, dict):
            continue
        selected_source_count = int(context_budget.get("selected_source_count", 0) or 0)
        candidate_run_count = int(context_budget.get("candidate_run_count", 0) or 0)
        if selected_source_count > 0:
            selected_source_counts.append(selected_source_count)
        if candidate_run_count > 0:
            candidate_run_counts.append(candidate_run_count)

    max_source_runs = max(1, percentile(selected_source_counts, 0.75) or 1)
    max_candidates = max(max_source_runs, percentile(candidate_run_counts, 0.75) or max_source_runs)
    support = max(len(selected_source_counts), len(candidate_run_counts))
    confidence = min(1.0, support / 8.0) if support else 0.0
    return {
        "max_source_runs": max_source_runs,
        "max_candidates": max_candidates,
    }, {
        "value": {
            "max_source_runs": max_source_runs,
            "max_candidates": max_candidates,
        },
        "confidence": round(confidence, 4),
        "support": support,
        "selected_source_count_p75": max_source_runs,
        "candidate_run_count_p75": max_candidates,
    }


def learn_benchmark_eligibility(
    rows: list[tuple[dict[str, Any], dict[str, Any]]],
) -> tuple[bool, dict[str, Any]]:
    total = len(rows)
    eligible_count = sum(1 for _features, labels in rows if bool(labels.get("benchmark_eligible", False)))
    ineligible_count = max(0, total - eligible_count)
    eligible_rate = eligible_count / total if total else 0.0
    eligible_lower_bound = wilson_lower_bound(eligible_count, total)
    ineligible_lower_bound = wilson_lower_bound(ineligible_count, total)
    if eligible_count >= ineligible_count:
        return True, {
            "value": True,
            "confidence": round(max(eligible_lower_bound, eligible_rate), 4),
            "support": total,
            "positive_count": eligible_count,
            "positive_rate": round(eligible_rate, 4),
            "lower_bound": round(eligible_lower_bound, 4),
        }
    return False, {
        "value": False,
        "confidence": round(
            max(ineligible_lower_bound, ineligible_count / total if total else 0.0),
            4,
        ),
        "support": total,
        "negative_count": ineligible_count,
        "negative_rate": round(ineligible_count / total if total else 0.0, 4),
        "lower_bound": round(ineligible_lower_bound, 4),
    }


def training_summary(
    rows: list[tuple[dict[str, Any], dict[str, Any]]],
    *,
    execution_profile: str,
    retry_limit: int,
    context_budget: dict[str, int],
    benchmark_eligible: bool,
) -> dict[str, Any]:
    successful = [labels for _features, labels in rows if bool(labels.get("overall_pass", False))]
    capability_rows = [
        labels for _features, labels in rows if str(labels.get("execution_profile", "")) == "capability"
    ]
    return {
        "example_count": len(rows),
        "success_count": len(successful),
        "recommended_execution_profile": execution_profile,
        "recommended_retry_limit": retry_limit,
        "recommended_context_budget": dict(context_budget),
        "recommended_benchmark_eligible": benchmark_eligible,
        "capability_example_count": len(capability_rows),
        "trained_at": now_utc(),
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    examples_path = pathlib.Path(args.examples).resolve()
    out_path = pathlib.Path(args.out).resolve()
    examples = read_jsonl(examples_path)
    rows = policy_rows(examples)
    if not rows:
        raise SystemExit("no policy examples found")

    execution_profile, execution_profile_payload = learn_execution_profile(rows)
    retry_limit, retry_payload = learn_retry_limit(rows)
    context_budget, context_budget_payload = learn_context_budget(rows)
    benchmark_eligible, benchmark_payload = learn_benchmark_eligibility(rows)

    candidate_payload = build_candidate_manifest(
        candidate_type="policy",
        candidate_id=args.candidate_id
        or candidate_id_for_path(out_path, examples_path=examples_path),
        mode=args.mode,
        runtime={
            "policy_model_version": "aggregate-policy-v1",
            "activation_threshold": max(0.0, min(1.0, float(args.activation_threshold))),
            "recommendations": {
                "execution_profile": execution_profile_payload,
                "retry_limit": retry_payload,
                "context_budget": context_budget_payload,
                "benchmark_eligible": benchmark_payload,
            },
            "training_summary": training_summary(
                rows,
                execution_profile=execution_profile,
                retry_limit=retry_limit,
                context_budget=context_budget,
                benchmark_eligible=benchmark_eligible,
            ),
        },
        training_dataset_fingerprints={"policy_examples": sha256_file(examples_path) or ""},
        evaluation_dataset_fingerprints={},
        bundle_id=args.bundle_id,
        description="Offline-learned policy recommendations aggregated from policy examples.",
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
