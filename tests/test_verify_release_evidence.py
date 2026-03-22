from __future__ import annotations

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
