from __future__ import annotations

import json
import pathlib

import run_real_canary


def test_candidate_metadata_reads_candidate_manifest(tmp_path: pathlib.Path) -> None:
    candidate_path = tmp_path / "policy.json"
    candidate_path.write_text(
        json.dumps(
            {
                "candidate_id": "policy-candidate-1",
                "candidate_type": "policy",
                "mode": "active",
                "promotion": {"activation_approved": True},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = run_real_canary.candidate_metadata(str(candidate_path))

    assert payload == {
        "path": str(candidate_path.resolve()),
        "configured": True,
        "candidate_id": "policy-candidate-1",
        "candidate_type": "policy",
        "mode": "active",
        "activation_approved": True,
    }


def test_candidate_metadata_reports_missing_candidate(tmp_path: pathlib.Path) -> None:
    payload = run_real_canary.candidate_metadata(str(tmp_path / "missing.json"))

    assert payload["configured"] is False
    assert payload["error"] == "candidate manifest not found"


def test_parse_args_accepts_label_and_policy_candidate(tmp_path: pathlib.Path) -> None:
    args = run_real_canary.parse_args(
        [
            "--label",
            "policy-candidate",
            "--policy-candidate",
            str(tmp_path / "policy.json"),
            "--summary-path",
            str(tmp_path / "summary.json"),
        ]
    )

    assert args.label == "policy-candidate"
    assert args.policy_candidate == str(tmp_path / "policy.json")
    assert args.summary_path == str(tmp_path / "summary.json")
