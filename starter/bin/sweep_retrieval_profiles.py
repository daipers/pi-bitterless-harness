#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import tempfile
import time
from collections.abc import Iterable
from itertools import islice, product
from typing import Any

from benchmark_harness import benchmark_retrieval
from retrieval_index import load_retrieval_profile


def clone_profile(profile: dict[str, Any]) -> dict[str, Any]:
    return json.loads(
        json.dumps(
            {
                key: value
                for key, value in profile.items()
                if key not in {"path", "profile_fingerprint"}
            }
        )
    )


def candidate_profiles(base_profile: dict[str, Any]) -> Iterable[dict[str, Any]]:
    goal_base = int(base_profile["field_weights"]["goal_overlap"])
    claim_base = int(base_profile["field_weights"]["claim_overlap"])
    artifact_base = int(base_profile["field_weights"]["artifact_overlap"])
    evidence_base = int(base_profile["field_weights"]["evidence_path_overlap"])
    phrase_base = int(base_profile["phrase_bonus_per_field"])
    cutoff_base = int(base_profile["stage1_candidate_cutoff"])

    axes = product(
        [goal_base, goal_base + 1],
        [claim_base, claim_base + 1],
        [artifact_base, artifact_base + 1],
        [evidence_base, evidence_base + 1],
        [phrase_base, phrase_base + 2],
        sorted({max(5, cutoff_base // 2), cutoff_base}),
        ["evidence_first", "descriptive_first"],
    )

    for index, (
        goal_weight,
        claim_weight,
        artifact_weight,
        evidence_weight,
        phrase_bonus,
        cutoff,
        selection,
    ) in enumerate(axes, start=1):
        candidate = clone_profile(base_profile)
        candidate["profile_id"] = f"sweep-{index:03d}"
        candidate["field_weights"]["goal_overlap"] = goal_weight
        candidate["field_weights"]["claim_overlap"] = claim_weight
        candidate["field_weights"]["artifact_overlap"] = artifact_weight
        candidate["field_weights"]["evidence_path_overlap"] = evidence_weight
        candidate["phrase_bonus_per_field"] = phrase_bonus
        candidate["stage1_candidate_cutoff"] = cutoff
        candidate["view_artifact_selection"] = selection
        yield candidate


def candidate_sort_key(result: dict[str, Any]) -> tuple[Any, ...]:
    metrics = result["metrics"]
    return (
        -float(metrics["hard_negative_win_rate"]),
        -float(metrics["top_1_hit_rate"]),
        -float(metrics["abstention_hit_rate"]),
        -float(metrics["top_3_hit_rate"]),
        float(metrics["mean_selected_source_count"]),
        result["profile"]["profile_id"],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sweep retrieval profiles against the benchmark corpus")
    parser.add_argument(
        "--harness-root",
        default=str(pathlib.Path(__file__).resolve().parents[1]),
        help="path to the starter harness root",
    )
    parser.add_argument(
        "--base-profile",
        help="optional base retrieval profile path",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="optional maximum number of profiles to evaluate",
    )
    parser.add_argument(
        "--write-best",
        help="optional path to write the best profile JSON",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    harness_root = pathlib.Path(args.harness_root).resolve()
    base_profile = load_retrieval_profile(
        args.base_profile,
        repo_root=harness_root,
    )
    candidates = candidate_profiles(base_profile)
    if args.limit is not None:
        candidates = islice(candidates, args.limit)

    results: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="retrieval-sweep-") as tmp_dir_name:
        temp_root = pathlib.Path(tmp_dir_name)
        harness_copy = temp_root / "starter"
        shutil.copytree(
            harness_root,
            harness_copy,
            ignore=shutil.ignore_patterns("runs"),
        )
        profiles_dir = temp_root / "profiles"
        profiles_dir.mkdir(parents=True, exist_ok=True)

        for candidate in candidates:
            candidate_path = profiles_dir / f"{candidate['profile_id']}.json"
            candidate_path.write_text(json.dumps(candidate, indent=2) + "\n", encoding="utf-8")
            started = time.perf_counter()
            metrics = benchmark_retrieval(harness_copy, profile_path=candidate_path)
            results.append(
                {
                    "profile": candidate,
                    "metrics": metrics,
                    "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                }
            )

    if not results:
        raise SystemExit("no retrieval profiles were evaluated")

    results.sort(key=candidate_sort_key)
    best = results[0]
    if args.write_best:
        destination = pathlib.Path(args.write_best).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(best["profile"], indent=2) + "\n", encoding="utf-8")

    payload = {
        "candidate_count": len(results),
        "best_profile": best["profile"],
        "best_metrics": best["metrics"],
        "results": [
            {
                "profile_id": item["profile"]["profile_id"],
                "duration_ms": item["duration_ms"],
                "metrics": {
                    key: item["metrics"][key]
                    for key in [
                        "hard_negative_win_rate",
                        "top_1_hit_rate",
                        "abstention_hit_rate",
                        "top_3_hit_rate",
                        "mean_selected_source_count",
                    ]
                },
            }
            for item in results
        ],
    }
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
