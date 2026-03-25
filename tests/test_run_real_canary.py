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


def test_main_writes_v3_cli_summary(tmp_path: pathlib.Path, monkeypatch) -> None:
    starter_root = tmp_path / "starter"
    starter_root.mkdir(parents=True, exist_ok=True)
    (tmp_path / "PI_VERSION").write_text("0.61.1\n", encoding="utf-8")
    summary_path = tmp_path / "summary.json"

    monkeypatch.setattr(run_real_canary, "STARTER_ROOT", starter_root)
    monkeypatch.setattr(run_real_canary, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(run_real_canary, "base_env", lambda **_: {})
    monkeypatch.setattr(run_real_canary, "git_sha", lambda: "abc123")
    monkeypatch.setattr(run_real_canary, "scenario_success", lambda env, model: {"run_dir": "success"})
    monkeypatch.setattr(
        run_real_canary, "scenario_corrupt_result", lambda env, model: {"run_dir": "corrupt"}
    )
    monkeypatch.setattr(run_real_canary, "scenario_timeout", lambda env, model: {"run_dir": "timeout"})
    monkeypatch.setattr(
        run_real_canary, "scenario_interrupted", lambda env, model: {"run_dir": "interrupted"}
    )
    monkeypatch.setattr(run_real_canary, "scenario_retry", lambda env, model: {"run_dir": "retry"})
    monkeypatch.setattr(
        run_real_canary, "scenario_partial_recovery", lambda env, model: {"run_dir": "recovery"}
    )

    exit_code = run_real_canary.main(["--summary-path", str(summary_path), "--label", "default"])

    assert exit_code == 0
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["summary_version"] == "v3"
    assert summary["canary_kind"] == "real_cli"
    assert summary["transport_mode"] == "cli_json"
    assert summary["interception_proven"] is False
    assert summary["scenario_totals"]["total"] == 6
    assert summary["overall_ok"] is True
