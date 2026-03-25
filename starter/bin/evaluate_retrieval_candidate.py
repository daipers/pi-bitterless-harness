#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import pathlib
import subprocess
import sys
from typing import Any

from harnesslib import now_utc
from learninglib import load_candidate_manifest, write_candidate_manifest

TARGET_METRICS = (
    "hard_negative_win_rate",
    "top_1_hit_rate",
    "empty_context_precision",
)
REPLAY_TARGET_METRICS = ("pass_rate_percent", "retry_recovery_rate")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate and optionally promote a retrieval candidate"
    )
    parser.add_argument("--candidate", required=True, help="candidate manifest path")
    parser.add_argument("--out", required=True, help="candidate report output path")
    parser.add_argument(
        "--harness-root",
        help="starter harness root; required when reports are not supplied",
    )
    parser.add_argument("--baseline-report", help="existing baseline benchmark JSON")
    parser.add_argument("--candidate-report", help="existing candidate benchmark JSON")
    parser.add_argument("--max-latency-regression-ratio", type=float, default=1.25)
    parser.add_argument("--promote-if-passed", action="store_true")
    parser.add_argument("--promote-mode", choices=["shadow", "active"], default="active")
    return parser.parse_args(argv)


def read_json(path: pathlib.Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def retriever_requires_replay(candidate_manifest: dict[str, Any]) -> bool:
    runtime = candidate_manifest.get("runtime", {}) if isinstance(candidate_manifest, dict) else {}
    return (
        str(runtime.get("retriever_version", ""))
        in {
            "dense-hashed-shared-encoder-v1",
            "lexical-stage1-v2",
        }
        or str(runtime.get("reranker_version", "")) == "text-pair-hashed-reranker-v2"
    )


def benchmark_command(
    harness_root: pathlib.Path,
    *,
    mode: str,
    output_path: pathlib.Path,
    candidate_path: pathlib.Path | None,
    bundle_id: str,
    replay_corpus_path: pathlib.Path | None = None,
) -> list[str]:
    command = [
        sys.executable,
        str(harness_root / "bin" / "benchmark_harness.py"),
        "--mode",
        mode,
        "--candidate-bundle-id",
        bundle_id,
        "--out",
        str(output_path),
    ]
    if candidate_path is not None:
        command.extend(["--retrieval-candidate", str(candidate_path)])
    if replay_corpus_path is not None:
        command.extend(
            [
                "--replay-corpus",
                str(replay_corpus_path),
                "--history-dir",
                str(output_path.parent / "history"),
            ]
        )
    return command


def run_benchmark(
    harness_root: pathlib.Path,
    *,
    mode: str,
    candidate_path: pathlib.Path | None,
    output_path: pathlib.Path,
    bundle_id: str,
    replay_corpus_path: pathlib.Path | None = None,
) -> dict[str, Any]:
    subprocess.run(
        benchmark_command(
            harness_root,
            mode=mode,
            output_path=output_path,
            candidate_path=candidate_path,
            bundle_id=bundle_id,
            replay_corpus_path=replay_corpus_path,
        ),
        cwd=str(harness_root),
        check=True,
        capture_output=True,
        text=True,
        env=os.environ | {"PYTHONPATH": str(harness_root / "bin")},
    )
    return read_json(output_path)


def latest_replay_metrics(report: dict[str, Any]) -> dict[str, Any]:
    replay_payload = report.get("replay", {}) if isinstance(report.get("replay"), dict) else {}
    workload_metrics = replay_payload.get("workload_metrics", [])
    if not isinstance(workload_metrics, list) or not workload_metrics:
        return {}
    latest = workload_metrics[-1]
    return dict(latest) if isinstance(latest, dict) else {}


def compare_retrieval_reports(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    candidate_manifest: dict[str, Any],
    max_latency_regression_ratio: float,
) -> dict[str, Any]:
    baseline_metrics = dict(baseline.get("retrieval", {}))
    candidate_metrics = dict(candidate.get("retrieval", {}))
    baseline_replay_metrics = latest_replay_metrics(baseline)
    candidate_replay_metrics = latest_replay_metrics(candidate)
    deltas = {
        key: round(
            float(candidate_metrics.get(key, 0.0)) - float(baseline_metrics.get(key, 0.0)),
            4,
        )
        for key in {
            *TARGET_METRICS,
            "hallucinated_evidence_rate",
            "warm_reuse_ms",
        }
    }
    if baseline_replay_metrics and candidate_replay_metrics:
        for key in REPLAY_TARGET_METRICS:
            deltas[f"replay_{key}"] = round(
                float(candidate_replay_metrics.get(key, 0.0))
                - float(baseline_replay_metrics.get(key, 0.0)),
                4,
            )

    baseline_latency = float(baseline_metrics.get("warm_reuse_ms", 0.0) or 0.0)
    candidate_latency = float(candidate_metrics.get("warm_reuse_ms", 0.0) or 0.0)
    latency_allowed = (
        True
        if baseline_latency <= 0
        else candidate_latency <= baseline_latency * max(1.0, max_latency_regression_ratio)
    )
    threshold_results = {
        "hard_negative_win_rate_non_regression": (
            float(candidate_metrics.get("hard_negative_win_rate", 0.0))
            >= float(baseline_metrics.get("hard_negative_win_rate", 0.0))
        ),
        "top_1_hit_rate_non_regression": (
            float(candidate_metrics.get("top_1_hit_rate", 0.0))
            >= float(baseline_metrics.get("top_1_hit_rate", 0.0))
        ),
        "empty_context_precision_non_regression": (
            float(candidate_metrics.get("empty_context_precision", 0.0))
            >= float(baseline_metrics.get("empty_context_precision", 0.0))
        ),
        "hallucinated_evidence_rate_non_regression": (
            float(candidate_metrics.get("hallucinated_evidence_rate", 1.0))
            <= float(baseline_metrics.get("hallucinated_evidence_rate", 1.0))
        ),
        "latency_within_tolerance": latency_allowed,
        "candidate_benchmark_pass": bool(candidate.get("overall_pass", False)),
    }

    replay_available = bool(baseline_replay_metrics and candidate_replay_metrics)
    if replay_available:
        threshold_results["replay_pass_rate_percent_non_regression"] = float(
            candidate_replay_metrics.get("pass_rate_percent", 0.0)
        ) >= float(baseline_replay_metrics.get("pass_rate_percent", 0.0))
        threshold_results["replay_retry_recovery_rate_non_regression"] = float(
            candidate_replay_metrics.get("retry_recovery_rate", 0.0)
        ) >= float(baseline_replay_metrics.get("retry_recovery_rate", 0.0))

    approved = all(threshold_results.values())
    approval_reason = "candidate beats or matches baseline within tolerance"
    if not approved and retriever_requires_replay(candidate_manifest) and not replay_available:
        approval_reason = "dense retrieval candidate requires replay evidence for active promotion"
    elif not approved:
        approval_reason = "candidate failed retrieval promotion checks"

    return {
        "baseline_metrics": {
            "retrieval": baseline_metrics,
            "replay": baseline_replay_metrics,
        },
        "candidate_metrics": {
            "retrieval": candidate_metrics,
            "replay": candidate_replay_metrics,
        },
        "deltas": deltas,
        "threshold_results": threshold_results,
        "activation_approved": approved,
        "approval_reason": approval_reason,
        "replay_available": replay_available,
    }


def build_candidate_report_payload(
    *,
    candidate_manifest: dict[str, Any],
    comparison: dict[str, Any],
    baseline_report_path: pathlib.Path,
    candidate_report_path: pathlib.Path,
) -> dict[str, Any]:
    return {
        "candidate_report_version": "v1",
        "generated_at": now_utc(),
        "candidate_type": "retrieval",
        "candidate_id": candidate_manifest.get("candidate_id"),
        "bundle_id": candidate_manifest.get("bundle_id"),
        "mode": candidate_manifest.get("mode"),
        "candidate_manifest_path": candidate_manifest.get("path"),
        "artifact_fingerprint": candidate_manifest.get("artifact_fingerprint"),
        "benchmark_report_path": str(candidate_report_path),
        "baseline_report_path": str(baseline_report_path),
        "training_dataset_fingerprints": dict(
            candidate_manifest.get("training_dataset_fingerprints", {})
        ),
        "evaluation_dataset_fingerprints": dict(
            candidate_manifest.get("evaluation_dataset_fingerprints", {})
        ),
        "comparison": comparison,
        "promotion_summary": {
            "bundle_id": candidate_manifest.get("bundle_id"),
            "candidate_types": {"retrieval": candidate_manifest.get("candidate_id")},
            "threshold_results": comparison["threshold_results"],
            "activation_approved": comparison["activation_approved"],
            "approval_reason": comparison["approval_reason"],
        },
        "metrics": dict(comparison["candidate_metrics"]),
        "baseline_metrics": dict(comparison["baseline_metrics"]),
        "overall_pass": bool(comparison["activation_approved"]),
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    candidate_path = pathlib.Path(args.candidate).resolve()
    candidate_manifest = load_candidate_manifest("retrieval", candidate_path)
    if candidate_manifest is None:
        raise SystemExit("candidate manifest not found")

    if args.baseline_report and args.candidate_report:
        baseline_report_path = pathlib.Path(args.baseline_report).resolve()
        candidate_report_path = pathlib.Path(args.candidate_report).resolve()
        baseline_report = read_json(baseline_report_path)
        candidate_benchmark_report = read_json(candidate_report_path)
    else:
        if not args.harness_root:
            raise SystemExit("--harness-root is required when benchmark reports are not supplied")
        harness_root = pathlib.Path(args.harness_root).resolve()
        output_root = pathlib.Path(args.out).resolve().parent
        output_root.mkdir(parents=True, exist_ok=True)
        baseline_report_path = output_root / "retrieval-baseline-benchmark.json"
        candidate_report_path = output_root / "retrieval-candidate-benchmark.json"
        bundle_id = str(
            candidate_manifest.get("bundle_id") or candidate_manifest.get("candidate_id")
        )
        baseline_report = run_benchmark(
            harness_root,
            mode="retrieval",
            candidate_path=None,
            output_path=baseline_report_path,
            bundle_id=f"{bundle_id}-baseline",
        )
        candidate_benchmark_report = run_benchmark(
            harness_root,
            mode="retrieval",
            candidate_path=candidate_path,
            output_path=candidate_report_path,
            bundle_id=bundle_id,
        )

        replay_corpus_path = harness_root / "benchmarks" / "replay-corpus.json"
        if replay_corpus_path.exists():
            baseline_replay_path = output_root / "replay-baseline-benchmark.json"
            candidate_replay_path = output_root / "replay-candidate-benchmark.json"
            baseline_report["replay"] = run_benchmark(
                harness_root,
                mode="replay",
                candidate_path=None,
                output_path=baseline_replay_path,
                bundle_id=f"{bundle_id}-baseline",
                replay_corpus_path=replay_corpus_path,
            ).get("replay", {})
            candidate_benchmark_report["replay"] = run_benchmark(
                harness_root,
                mode="replay",
                candidate_path=candidate_path,
                output_path=candidate_replay_path,
                bundle_id=bundle_id,
                replay_corpus_path=replay_corpus_path,
            ).get("replay", {})

    comparison = compare_retrieval_reports(
        baseline_report,
        candidate_benchmark_report,
        candidate_manifest=candidate_manifest,
        max_latency_regression_ratio=max(1.0, args.max_latency_regression_ratio),
    )

    if (
        args.promote_mode == "active"
        and retriever_requires_replay(candidate_manifest)
        and not comparison["replay_available"]
    ):
        comparison["activation_approved"] = False
        comparison["threshold_results"]["replay_available_for_dense_active_promotion"] = False
        comparison["approval_reason"] = (
            "dense retrieval candidate requires replay evidence for active promotion"
        )

    report_payload = build_candidate_report_payload(
        candidate_manifest=candidate_manifest,
        comparison=comparison,
        baseline_report_path=baseline_report_path,
        candidate_report_path=candidate_report_path,
    )

    if args.promote_if_passed:
        updated_manifest = dict(candidate_manifest)
        updated_manifest["promotion"] = {
            **dict(candidate_manifest.get("promotion", {})),
            "activation_approved": bool(comparison["activation_approved"]),
            "approved_at": (
                report_payload["generated_at"] if comparison["activation_approved"] else None
            ),
            "approval_reason": comparison["approval_reason"],
            "baseline_report_path": str(baseline_report_path),
            "candidate_report_path": str(candidate_report_path),
            "comparison": comparison,
        }
        if comparison["activation_approved"]:
            updated_manifest["mode"] = args.promote_mode
        write_candidate_manifest(candidate_path, updated_manifest)
        candidate_manifest = (
            load_candidate_manifest("retrieval", candidate_path) or updated_manifest
        )
        report_payload["mode"] = candidate_manifest.get("mode")
        report_payload["candidate_manifest_path"] = candidate_manifest.get("path")
        report_payload["promotion_summary"]["activation_approved"] = comparison[
            "activation_approved"
        ]

    out_path = pathlib.Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(report_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report_payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
