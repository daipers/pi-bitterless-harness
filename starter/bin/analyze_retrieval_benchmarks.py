#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import time
from typing import Any

SUMMARY_METRIC_KEYS = (
    "top_1_hit_rate",
    "top_3_hit_rate",
    "abstention_hit_rate",
    "empty_context_rate",
    "empty_context_precision",
    "mean_selected_source_count",
    "mean_selected_score",
    "hard_negative_win_rate",
    "copied_artifact_usefulness_rate",
    "hallucinated_evidence_rate",
)


def load_payload(path: pathlib.Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("retrieval"), dict):
        raise ValueError(f"benchmark payload missing retrieval block: {path}")
    return payload


def history_row_from_payload(
    payload: dict[str, Any], *, source_path: pathlib.Path
) -> dict[str, Any]:
    retrieval = payload["retrieval"]
    row = {
        "generated_at": payload.get("generated_at")
        or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source_path": str(source_path),
        "retrieval_profile_id": retrieval.get("retrieval_profile_id"),
        "scenario_count": retrieval.get("scenario_count"),
    }
    for key in SUMMARY_METRIC_KEYS:
        row[key] = retrieval.get(key)
    return row


def compute_trend(history_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not history_rows:
        return None
    latest = history_rows[-1]
    previous = history_rows[-2] if len(history_rows) >= 2 else None
    latest_metrics = {key: latest.get(key) for key in SUMMARY_METRIC_KEYS}
    delta_vs_previous = {
        key: (
            round(float(latest[key]) - float(previous[key]), 2)
            if previous is not None
            and latest.get(key) is not None
            and previous.get(key) is not None
            else None
        )
        for key in SUMMARY_METRIC_KEYS
    }
    return {
        "history_length": len(history_rows),
        "latest_metrics": latest_metrics,
        "delta_vs_previous": delta_vs_previous,
    }


def analyze_latest_scenarios(payload: dict[str, Any]) -> dict[str, Any]:
    retrieval = payload["retrieval"]
    scenario_results = list(retrieval.get("scenario_results", []))
    failing_hard_negatives = [
        item["scenario_id"]
        for item in scenario_results
        if item.get("expected_empty_context") is False and not item.get("hard_negative_pass")
    ]
    failing_top_1 = [
        item["scenario_id"]
        for item in scenario_results
        if item.get("expected_empty_context") is False and not item.get("top_1_hit")
    ]
    prune_candidates = [
        item["scenario_id"]
        for item in scenario_results
        if item.get("expected_empty_context") is False
        and item.get("top_1_hit")
        and item.get("hard_negative_pass")
        and float(item.get("duration_ms", 0.0)) < 50.0
    ]
    return {
        "failing_hard_negatives": failing_hard_negatives,
        "failing_top_1": failing_top_1,
        "prune_candidates": prune_candidates,
    }


def record_history(
    history_dir: pathlib.Path, payloads: list[dict[str, Any]], paths: list[pathlib.Path]
) -> tuple[pathlib.Path, list[pathlib.Path]]:
    history_dir.mkdir(parents=True, exist_ok=True)
    history_path = history_dir / "retrieval-benchmark.history.jsonl"
    snapshot_paths: list[pathlib.Path] = []

    rows = [
        history_row_from_payload(payload, source_path=path)
        for payload, path in zip(payloads, paths)
    ]
    with history_path.open("a", encoding="utf-8") as handle:
        for row, payload in zip(rows, payloads):
            handle.write(json.dumps(row, sort_keys=True) + "\n")
            snapshot_stem = f"retrieval-benchmark-{time.strftime('%Y%m%d-%H%M%S')}"
            snapshot_path = history_dir / f"{snapshot_stem}-{len(snapshot_paths) + 1}.summary.json"
            snapshot_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
            snapshot_paths.append(snapshot_path)
    return history_path, snapshot_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze retrieval benchmark snapshots")
    parser.add_argument(
        "inputs", nargs="+", help="benchmark JSON files produced by benchmark_harness.py"
    )
    parser.add_argument(
        "--history-dir",
        help="optional directory to append history rows and snapshot copies",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_paths = [pathlib.Path(item).resolve() for item in args.inputs]
    payloads = [load_payload(path) for path in input_paths]
    history_rows = [
        history_row_from_payload(payload, source_path=path)
        for payload, path in zip(payloads, input_paths)
    ]
    trend = compute_trend(history_rows)
    latest_analysis = analyze_latest_scenarios(payloads[-1])

    history_path = None
    snapshot_paths: list[str] = []
    if args.history_dir:
        recorded_history_path, recorded_snapshots = record_history(
            pathlib.Path(args.history_dir).resolve(),
            payloads,
            input_paths,
        )
        history_path = str(recorded_history_path)
        snapshot_paths = [str(path) for path in recorded_snapshots]

    payload = {
        "history_length": len(history_rows),
        "trend": trend,
        "latest_analysis": latest_analysis,
        "history_path": history_path,
        "snapshot_paths": snapshot_paths,
    }
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
