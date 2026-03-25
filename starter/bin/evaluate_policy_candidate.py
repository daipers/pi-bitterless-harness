#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

from harnesslib import now_utc
from learninglib import load_candidate_manifest, write_candidate_manifest
from verify_release_evidence import (
    expected_pi_version,
    load_local_summaries,
    parse_args as parse_release_args,
    validate_summaries,
)

REPLAY_TARGET_METRICS = ("pass_rate_percent", "retry_recovery_rate")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate and optionally promote a policy candidate"
    )
    parser.add_argument("--candidate", required=True, help="policy candidate manifest path")
    parser.add_argument("--baseline-replay-report", required=True, help="baseline replay report path")
    parser.add_argument("--candidate-replay-report", required=True, help="candidate replay report path")
    parser.add_argument(
        "--baseline-canary-summary-glob",
        required=True,
        help="glob for baseline canary summaries",
    )
    parser.add_argument(
        "--candidate-canary-summary-glob",
        required=True,
        help="glob for candidate canary summaries",
    )
    parser.add_argument(
        "--baseline-canary-kind",
        choices=["real_cli", "real_managed_rpc"],
        default=None,
        help="optional baseline canary kind filter",
    )
    parser.add_argument(
        "--candidate-canary-kind",
        choices=["real_cli", "real_managed_rpc"],
        default=None,
        help="optional candidate canary kind filter",
    )
    parser.add_argument("--out", required=True, help="candidate report output path")
    parser.add_argument("--promote-if-passed", action="store_true")
    parser.add_argument("--promote-mode", choices=["shadow", "active"], default="active")
    parser.add_argument("--min-runs", type=int, default=2)
    parser.add_argument("--freshness-hours", type=int, default=36)
    parser.add_argument("--min-pass-rate", type=float, default=1.0)
    parser.add_argument("--expected-pi-version", default=None)
    return parser.parse_args(argv)


def read_json(path: pathlib.Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    payload["_path"] = str(path)
    return payload


def latest_replay_metrics(report: dict[str, Any]) -> dict[str, Any]:
    replay_payload = report.get("replay", {}) if isinstance(report.get("replay"), dict) else {}
    workload_metrics = replay_payload.get("workload_metrics", [])
    if not isinstance(workload_metrics, list) or not workload_metrics:
        return {}
    latest = workload_metrics[-1]
    return dict(latest) if isinstance(latest, dict) else {}


def compare_replay_reports(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, bool]]:
    baseline_metrics = latest_replay_metrics(baseline)
    candidate_metrics = latest_replay_metrics(candidate)
    deltas = {
        f"replay_{key}": round(
            float(candidate_metrics.get(key, 0.0)) - float(baseline_metrics.get(key, 0.0)),
            4,
        )
        for key in REPLAY_TARGET_METRICS
    }
    threshold_results = {
        "candidate_replay_available": bool(candidate_metrics),
        "baseline_replay_available": bool(baseline_metrics),
        "candidate_replay_pass": bool(candidate.get("overall_pass", False)),
        "replay_pass_rate_percent_non_regression": float(
            candidate_metrics.get("pass_rate_percent", 0.0)
        )
        >= float(baseline_metrics.get("pass_rate_percent", 0.0)),
        "replay_retry_recovery_rate_non_regression": float(
            candidate_metrics.get("retry_recovery_rate", 0.0)
        )
        >= float(baseline_metrics.get("retry_recovery_rate", 0.0)),
    }
    return {
        "baseline": baseline_metrics,
        "candidate": candidate_metrics,
        "deltas": deltas,
    }, threshold_results


def canary_validation_report(
    pattern: str,
    *,
    min_runs: int,
    freshness_hours: int,
    min_pass_rate: float,
    expected_pi: str,
    canary_kind: str | None = None,
) -> dict[str, Any]:
    try:
        summary = validate_summaries(
            load_local_summaries(pattern),
            min_runs=max(1, min_runs),
            freshness_hours=max(1, freshness_hours),
            min_pass_rate=max(0.0, min(1.0, min_pass_rate)),
            expected_pi=expected_pi,
            canary_kind=canary_kind,
        )
        return {"passed": True, **summary, "pattern": pattern, "requested_canary_kind": canary_kind}
    except SystemExit as exc:
        return {
            "passed": False,
            "pattern": pattern,
            "reason": str(exc),
            "requested_canary_kind": canary_kind,
        }


def compare_canary_reports(
    *,
    baseline_pattern: str,
    candidate_pattern: str,
    min_runs: int,
    freshness_hours: int,
    min_pass_rate: float,
    expected_pi: str,
    baseline_canary_kind: str | None = None,
    candidate_canary_kind: str | None = None,
) -> tuple[dict[str, Any], dict[str, bool]]:
    baseline_report = canary_validation_report(
        baseline_pattern,
        min_runs=min_runs,
        freshness_hours=freshness_hours,
        min_pass_rate=min_pass_rate,
        expected_pi=expected_pi,
        canary_kind=baseline_canary_kind,
    )
    candidate_report = canary_validation_report(
        candidate_pattern,
        min_runs=min_runs,
        freshness_hours=freshness_hours,
        min_pass_rate=min_pass_rate,
        expected_pi=expected_pi,
        canary_kind=candidate_canary_kind,
    )
    baseline_pass_rate = float(baseline_report.get("pass_rate", 0.0) or 0.0)
    candidate_pass_rate = float(candidate_report.get("pass_rate", 0.0) or 0.0)
    threshold_results = {
        "baseline_canary_available": bool(baseline_report.get("passed")),
        "candidate_canary_pass": bool(candidate_report.get("passed")),
        "canary_pass_rate_non_regression": candidate_pass_rate >= baseline_pass_rate,
    }
    return {
        "baseline": baseline_report,
        "candidate": candidate_report,
        "deltas": {
            "canary_pass_rate": round(candidate_pass_rate - baseline_pass_rate, 4),
            "canary_selected_runs": int(candidate_report.get("selected_runs", 0) or 0)
            - int(baseline_report.get("selected_runs", 0) or 0),
        },
    }, threshold_results


def build_candidate_report_payload(
    *,
    candidate_manifest: dict[str, Any],
    replay_comparison: dict[str, Any],
    canary_comparison: dict[str, Any],
    threshold_results: dict[str, Any],
    activation_approved: bool,
    approval_reason: str,
    baseline_replay_report_path: pathlib.Path,
    candidate_replay_report_path: pathlib.Path,
) -> dict[str, Any]:
    return {
        "candidate_report_version": "v1",
        "generated_at": now_utc(),
        "candidate_type": "policy",
        "candidate_id": candidate_manifest.get("candidate_id"),
        "bundle_id": candidate_manifest.get("bundle_id"),
        "mode": candidate_manifest.get("mode"),
        "candidate_manifest_path": candidate_manifest.get("path"),
        "artifact_fingerprint": candidate_manifest.get("artifact_fingerprint"),
        "benchmark_report_path": str(candidate_replay_report_path),
        "baseline_report_path": str(baseline_replay_report_path),
        "training_dataset_fingerprints": dict(
            candidate_manifest.get("training_dataset_fingerprints", {})
        ),
        "evaluation_dataset_fingerprints": dict(
            candidate_manifest.get("evaluation_dataset_fingerprints", {})
        ),
        "comparison": {
            "baseline_metrics": {
                "replay": replay_comparison["baseline"],
                "canary": canary_comparison["baseline"],
            },
            "candidate_metrics": {
                "replay": replay_comparison["candidate"],
                "canary": canary_comparison["candidate"],
            },
            "deltas": {**replay_comparison["deltas"], **canary_comparison["deltas"]},
            "threshold_results": threshold_results,
            "activation_approved": activation_approved,
            "approval_reason": approval_reason,
        },
        "promotion_summary": {
            "bundle_id": candidate_manifest.get("bundle_id"),
            "candidate_types": {"policy": candidate_manifest.get("candidate_id")},
            "threshold_results": threshold_results,
            "activation_approved": activation_approved,
            "approval_reason": approval_reason,
        },
        "metrics": {
            "replay": replay_comparison["candidate"],
            "canary": canary_comparison["candidate"],
        },
        "baseline_metrics": {
            "replay": replay_comparison["baseline"],
            "canary": canary_comparison["baseline"],
        },
        "overall_pass": bool(activation_approved),
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    candidate_path = pathlib.Path(args.candidate).resolve()
    candidate_manifest = load_candidate_manifest("policy", candidate_path)
    if candidate_manifest is None:
        raise SystemExit("candidate manifest not found")

    baseline_replay_report_path = pathlib.Path(args.baseline_replay_report).resolve()
    candidate_replay_report_path = pathlib.Path(args.candidate_replay_report).resolve()
    baseline_replay_report = read_json(baseline_replay_report_path)
    candidate_replay_report = read_json(candidate_replay_report_path)
    expected_pi = expected_pi_version(
        parse_release_args(
            ["--summary-glob", args.candidate_canary_summary_glob, "--expected-pi-version", args.expected_pi_version]
            if args.expected_pi_version
            else ["--summary-glob", args.candidate_canary_summary_glob]
        )
    )

    replay_comparison, replay_thresholds = compare_replay_reports(
        baseline_replay_report,
        candidate_replay_report,
    )
    canary_comparison, canary_thresholds = compare_canary_reports(
        baseline_pattern=args.baseline_canary_summary_glob,
        candidate_pattern=args.candidate_canary_summary_glob,
        min_runs=max(1, args.min_runs),
        freshness_hours=max(1, args.freshness_hours),
        min_pass_rate=max(0.0, min(1.0, args.min_pass_rate)),
        expected_pi=expected_pi,
        baseline_canary_kind=args.baseline_canary_kind,
        candidate_canary_kind=args.candidate_canary_kind,
    )

    threshold_results = {**replay_thresholds, **canary_thresholds}
    activation_approved = all(bool(value) for value in threshold_results.values())
    approval_reason = "candidate beats or matches baseline replay/canary evidence"
    if not replay_thresholds["candidate_replay_available"] or not replay_thresholds["baseline_replay_available"]:
        approval_reason = "policy candidate requires baseline and candidate replay evidence"
    elif not canary_thresholds["baseline_canary_available"] or not canary_thresholds["candidate_canary_pass"]:
        approval_reason = "policy candidate requires passing baseline and candidate canary evidence"
    elif not activation_approved:
        approval_reason = "candidate failed replay/canary promotion checks"

    report = build_candidate_report_payload(
        candidate_manifest=candidate_manifest,
        replay_comparison=replay_comparison,
        canary_comparison=canary_comparison,
        threshold_results=threshold_results,
        activation_approved=activation_approved,
        approval_reason=approval_reason,
        baseline_replay_report_path=baseline_replay_report_path,
        candidate_replay_report_path=candidate_replay_report_path,
    )

    out_path = pathlib.Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    if args.promote_if_passed:
        promotion = dict(candidate_manifest.get("promotion", {}))
        promotion.update(
            {
                "activation_approved": bool(activation_approved),
                "approved_at": now_utc() if activation_approved else None,
                "approval_reason": approval_reason,
                "evidence": {
                    "baseline_replay_report": str(baseline_replay_report_path),
                    "candidate_replay_report": str(candidate_replay_report_path),
                    "baseline_canary_summary_glob": args.baseline_canary_summary_glob,
                    "candidate_canary_summary_glob": args.candidate_canary_summary_glob,
                    "baseline_canary_kind": args.baseline_canary_kind,
                    "candidate_canary_kind": args.candidate_canary_kind,
                },
            }
        )
        candidate_manifest["promotion"] = promotion
        if activation_approved:
            candidate_manifest["mode"] = args.promote_mode
        write_candidate_manifest(candidate_path, candidate_manifest)

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
