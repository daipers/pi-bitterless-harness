from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
import verify_release_evidence


def make_summary(
    *,
    generated_at: str,
    overall_ok: bool = True,
    failed: int = 0,
    supported_pi_version: str = "0.61.1",
) -> dict[str, object]:
    return {
        "generated_at": generated_at,
        "overall_ok": overall_ok,
        "supported_pi_version": supported_pi_version,
        "git_sha": "abc123",
        "scenario_totals": {
            "total": 6,
            "passed": 6 - failed,
            "failed": failed,
        },
    }


def make_benchmark_report(*, overall_pass: bool = True) -> dict[str, object]:
    return {
        "benchmark_report_version": "v1",
        "generated_at": "2026-03-22T11:00:00Z",
        "overall_pass": overall_pass,
        "promotion_summary": {
            "bundle_id": "bundle-1",
            "gated_sections": ["retrieval"],
            "threshold_results": {"retrieval": {"top_1_hit_rate": overall_pass}},
            "candidate_types": {"retrieval": "retrieval-candidate-1"},
        },
    }


def make_provenance(*, supported_pi_version: str = "0.61.1") -> dict[str, object]:
    return {
        "version": "1.0.0",
        "artifact": "pi-bitterless-harness-1.0.0.tar.gz",
        "checksum_file": "pi-bitterless-harness-1.0.0.tar.gz.sha256",
        "created_at": "2026-03-22T11:30:00Z",
        "git_sha": "abc123",
        "python_version": "Python 3.12.9",
        "supported_pi_version": supported_pi_version,
    }


def make_policy_candidate_report(*, overall_pass: bool = True) -> dict[str, object]:
    return {
        "candidate_report_version": "v1",
        "generated_at": "2026-03-22T11:15:00Z",
        "candidate_type": "policy",
        "candidate_id": "policy-candidate-1",
        "overall_pass": overall_pass,
        "promotion_summary": {
            "bundle_id": "bundle-1",
            "candidate_types": {"policy": "policy-candidate-1"},
            "threshold_results": {"candidate_canary_pass": overall_pass},
        },
    }


def make_bundle_candidate_report(*, overall_pass: bool = True) -> dict[str, object]:
    return {
        "candidate_report_version": "v1",
        "generated_at": "2026-03-22T11:20:00Z",
        "candidate_type": "bundle",
        "candidate_id": "bundle-candidate-1",
        "overall_pass": overall_pass,
        "promotion_summary": {
            "bundle_id": "bundle-1",
            "candidate_types": {"bundle": "bundle-candidate-1"},
            "threshold_results": {"bundle_canary_pass": overall_pass},
        },
    }


def test_validate_summaries_accepts_fresh_matching_history(monkeypatch) -> None:
    monkeypatch.setattr(
        verify_release_evidence,
        "now_utc",
        lambda: datetime(2026, 3, 22, 12, 0, tzinfo=UTC),
    )
    report = verify_release_evidence.validate_summaries(
        [
            make_summary(generated_at="2026-03-22T10:00:00Z"),
            make_summary(generated_at="2026-03-22T08:00:00Z"),
            make_summary(generated_at="2026-03-21T01:00:00Z"),
        ],
        min_runs=2,
        freshness_hours=36,
        min_pass_rate=1.0,
        expected_pi="0.61.1",
    )

    assert report["selected_runs"] == 2
    assert report["pass_rate"] == 1.0
    assert report["supported_pi_version"] == "0.61.1"
    assert report["passed"] is True


def test_validate_summaries_rejects_version_drift(monkeypatch) -> None:
    monkeypatch.setattr(
        verify_release_evidence,
        "now_utc",
        lambda: datetime(2026, 3, 22, 12, 0, tzinfo=UTC),
    )
    with pytest.raises(SystemExit, match="PI_VERSION drift"):
        verify_release_evidence.validate_summaries(
            [
                make_summary(generated_at="2026-03-22T10:00:00Z", supported_pi_version="0.60.0"),
                make_summary(generated_at="2026-03-22T08:00:00Z"),
            ],
            min_runs=2,
            freshness_hours=36,
            min_pass_rate=1.0,
            expected_pi="0.61.1",
        )


def test_validate_summaries_rejects_failed_scenarios(monkeypatch) -> None:
    monkeypatch.setattr(
        verify_release_evidence,
        "now_utc",
        lambda: datetime(2026, 3, 22, 12, 0, tzinfo=UTC),
    )
    with pytest.raises(SystemExit, match="failed scenarios"):
        verify_release_evidence.validate_summaries(
            [
                make_summary(generated_at="2026-03-22T10:00:00Z", failed=1),
                make_summary(generated_at="2026-03-22T08:00:00Z"),
            ],
            min_runs=2,
            freshness_hours=36,
            min_pass_rate=1.0,
            expected_pi="0.61.1",
        )


def test_build_release_gate_report_accepts_benchmark_and_provenance(
    tmp_path,
    monkeypatch,
) -> None:
    summary_path = tmp_path / "canary.summary.json"
    benchmark_path = tmp_path / "benchmark.json"
    policy_candidate_path = tmp_path / "policy-candidate.json"
    bundle_candidate_path = tmp_path / "bundle-candidate.json"
    provenance_path = tmp_path / "provenance.json"
    summary_path.write_text(
        json.dumps(make_summary(generated_at="2026-03-22T10:00:00Z"), indent=2) + "\n",
        encoding="utf-8",
    )
    second = tmp_path / "canary-2.summary.json"
    second.write_text(
        json.dumps(make_summary(generated_at="2026-03-22T09:00:00Z"), indent=2) + "\n",
        encoding="utf-8",
    )
    benchmark_path.write_text(
        json.dumps(make_benchmark_report(), indent=2) + "\n",
        encoding="utf-8",
    )
    policy_candidate_path.write_text(
        json.dumps(make_policy_candidate_report(), indent=2) + "\n",
        encoding="utf-8",
    )
    bundle_candidate_path.write_text(
        json.dumps(make_bundle_candidate_report(), indent=2) + "\n",
        encoding="utf-8",
    )
    provenance_path.write_text(
        json.dumps(make_provenance(), indent=2) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        verify_release_evidence,
        "now_utc",
        lambda: datetime(2026, 3, 22, 12, 0, tzinfo=UTC),
    )
    args = verify_release_evidence.parse_args(
        [
            "--summary-glob",
            str(summary_path.parent / "*.summary.json"),
            "--benchmark-report",
            str(benchmark_path),
            "--policy-candidate-report",
            str(policy_candidate_path),
            "--bundle-candidate-report",
            str(bundle_candidate_path),
            "--provenance-file",
            str(provenance_path),
        ]
    )

    report = verify_release_evidence.build_release_gate_report(args)

    assert report["release_gate_version"] == "v1"
    assert report["overall_pass"] is True
    assert report["checks"]["canary"]["selected_runs"] == 2
    assert report["checks"]["benchmark"]["passed"] is True
    assert report["checks"]["benchmark"]["bundle_id"] == "bundle-1"
    assert report["checks"]["benchmark"]["candidate_types"]["retrieval"] == "retrieval-candidate-1"
    assert report["checks"]["policy_candidate"]["passed"] is True
    assert report["checks"]["policy_candidate"]["candidate_id"] == "policy-candidate-1"
    assert report["checks"]["bundle_candidate"]["passed"] is True
    assert report["checks"]["bundle_candidate"]["candidate_id"] == "bundle-candidate-1"
    assert report["checks"]["provenance"]["passed"] is True
    assert report["checks"]["runtime"]["passed"] is True
    assert report["checks"]["runtime"]["expected_python_policy"] == "3.12.x"


def test_validate_candidate_report_rejects_failed_thresholds(tmp_path) -> None:
    candidate_report_path = tmp_path / "policy-candidate.json"
    candidate_report_path.write_text(
        json.dumps(make_policy_candidate_report(overall_pass=False), indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit, match="candidate report did not pass promotion thresholds"):
        verify_release_evidence.validate_candidate_report(
            candidate_report_path,
            expected_candidate_type="policy",
        )


def test_build_release_gate_report_allows_canary_only_verification(
    tmp_path,
    monkeypatch,
) -> None:
    summary_path = tmp_path / "canary.summary.json"
    second = tmp_path / "canary-2.summary.json"
    summary_path.write_text(
        json.dumps(make_summary(generated_at="2026-03-22T10:00:00Z"), indent=2) + "\n",
        encoding="utf-8",
    )
    second.write_text(
        json.dumps(make_summary(generated_at="2026-03-22T09:00:00Z"), indent=2) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        verify_release_evidence,
        "now_utc",
        lambda: datetime(2026, 3, 22, 12, 0, tzinfo=UTC),
    )

    report = verify_release_evidence.build_release_gate_report(
        verify_release_evidence.parse_args(
            [
                "--summary-glob",
                str(summary_path.parent / "*.summary.json"),
            ]
        )
    )

    assert report["overall_pass"] is True
    assert report["checks"]["benchmark"]["skipped"] is True
    assert report["checks"]["runtime"]["skipped"] is True


def test_build_release_gate_report_rejects_python_runtime_drift(
    tmp_path,
    monkeypatch,
) -> None:
    summary_path = tmp_path / "canary.summary.json"
    second = tmp_path / "canary-2.summary.json"
    benchmark_path = tmp_path / "benchmark.json"
    provenance_path = tmp_path / "provenance.json"
    summary_path.write_text(
        json.dumps(make_summary(generated_at="2026-03-22T10:00:00Z"), indent=2) + "\n",
        encoding="utf-8",
    )
    second.write_text(
        json.dumps(make_summary(generated_at="2026-03-22T09:00:00Z"), indent=2) + "\n",
        encoding="utf-8",
    )
    benchmark_path.write_text(
        json.dumps(make_benchmark_report(), indent=2) + "\n",
        encoding="utf-8",
    )
    provenance = make_provenance()
    provenance["python_version"] = "Python 3.14.3"
    provenance_path.write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        verify_release_evidence,
        "now_utc",
        lambda: datetime(2026, 3, 22, 12, 0, tzinfo=UTC),
    )

    report = verify_release_evidence.build_release_gate_report(
        verify_release_evidence.parse_args(
            [
                "--summary-glob",
                str(summary_path.parent / "*.summary.json"),
                "--benchmark-report",
                str(benchmark_path),
                "--provenance-file",
                str(provenance_path),
            ]
        )
    )

    assert report["overall_pass"] is False
    assert report["checks"]["runtime"]["passed"] is False
    assert report["checks"]["runtime"]["provenance_python_minor"] == "3.14"


def test_validate_benchmark_report_rejects_failed_thresholds(tmp_path) -> None:
    benchmark_path = tmp_path / "benchmark.json"
    benchmark_path.write_text(
        json.dumps(make_benchmark_report(overall_pass=False), indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit, match="did not pass promotion thresholds"):
        verify_release_evidence.validate_benchmark_report(benchmark_path)
