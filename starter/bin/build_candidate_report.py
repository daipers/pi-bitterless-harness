#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

from learninglib import build_candidate_report, load_candidate_manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a typed candidate report")
    parser.add_argument(
        "--candidate-type",
        required=True,
        choices=["retrieval", "policy", "model", "bundle"],
    )
    parser.add_argument("--candidate", required=True, help="path to a candidate manifest")
    parser.add_argument("--benchmark-report", help="optional benchmark report JSON path")
    parser.add_argument("--baseline-report", help="optional baseline benchmark report JSON path")
    parser.add_argument(
        "--metrics-key",
        help="optional metrics section key to copy from the benchmark report",
    )
    parser.add_argument("--out", required=True, help="output path")
    return parser.parse_args(argv)


def read_json(path: pathlib.Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    payload["_path"] = str(path)
    return payload


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    candidate = load_candidate_manifest(
        args.candidate_type,
        pathlib.Path(args.candidate).resolve(),
    )
    if candidate is None:
        raise SystemExit("candidate manifest not found")

    benchmark_report = (
        read_json(pathlib.Path(args.benchmark_report).resolve()) if args.benchmark_report else {}
    )
    baseline_report = (
        read_json(pathlib.Path(args.baseline_report).resolve()) if args.baseline_report else {}
    )
    metrics_key = args.metrics_key or args.candidate_type
    metrics = benchmark_report.get(metrics_key, {})
    report = build_candidate_report(
        candidate=candidate,
        benchmark_report=benchmark_report,
        baseline_report=baseline_report,
        metrics=metrics if isinstance(metrics, dict) else {},
        overall_pass=bool(benchmark_report.get("overall_pass", False)),
    )

    out_path = pathlib.Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
