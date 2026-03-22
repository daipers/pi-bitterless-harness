from __future__ import annotations

import json
import os
import pathlib
import runpy
import signal
import subprocess
import sys

import pytest
import score_run
from harnesslib import default_run_contract
from score_run import ScoreContext

TASK_TEXT = """# Task
Check score generation.

## Goal
Produce a passing score.

## Constraints
- Stay local.

## Done
- Score is written.

## Eval
```bash
python3 ../tests/fixtures/pass_eval.py
```

## Required Artifacts
- result.json
- outputs/claim.txt

## Result JSON schema (source of truth)
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://bitterless-harness.dev/contracts/result.schema.json",
  "type": "object",
  "required": [
    "x-interface-version",
    "status",
    "summary",
    "artifacts",
    "claims",
    "remaining_risks"
  ],
  "properties": {
    "x-interface-version": {"type": "string", "const": "v1"},
    "status": {"type": "string", "enum": ["success", "partial", "failed"]},
    "summary": {"type": "string", "minLength": 1},
    "artifacts": {"type": "array"},
    "claims": {"type": "array"},
    "remaining_risks": {"type": "array"}
  },
  "additionalProperties": false
}
```
"""


def make_run_dir(
    isolated_repo: pathlib.Path,
    *,
    contract_version: str = "v2",
    execution_profile: str = "strict",
) -> pathlib.Path:
    run_dir = isolated_repo / "starter" / "runs" / "score-test"
    (run_dir / "outputs").mkdir(parents=True)
    (run_dir / "score").mkdir()
    (run_dir / "home").mkdir()
    (run_dir / "session").mkdir()
    (run_dir / "task.md").write_text(TASK_TEXT, encoding="utf-8")
    (run_dir / "RUN.md").write_text("# Run Log\n", encoding="utf-8")
    schema_text = (isolated_repo / "starter" / "result.schema.json").read_text(encoding="utf-8")
    (run_dir / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (run_dir / "result.template.json").write_text("{}", encoding="utf-8")
    (run_dir / "run.contract.json").write_text(
        json.dumps(
            default_run_contract(version=contract_version, execution_profile=execution_profile),
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return run_dir


def make_context(
    run_dir: pathlib.Path,
    *,
    isolated_repo: pathlib.Path,
    task_path: pathlib.Path | None = None,
    exit_code_path: pathlib.Path | None = None,
    out_path: pathlib.Path | None = None,
    schema_path: pathlib.Path | None = None,
    event_log_path: pathlib.Path | None = None,
) -> ScoreContext:
    return ScoreContext(
        task_path=(task_path or (run_dir / "task.md")).resolve(),
        run_dir=run_dir.resolve(),
        exit_code_path=(exit_code_path or (run_dir / "pi.exit_code.txt")).resolve(),
        out_path=(out_path or (run_dir / "score.json")).resolve(),
        schema_path=(schema_path or (run_dir / "result.schema.json")).resolve(),
        event_log_path=(event_log_path or (run_dir / "run-events.jsonl")).resolve(),
        repo_root=(isolated_repo / "starter").resolve(),
    )


def test_score_run_helpers_cover_blocked_and_timeout_paths(
    isolated_repo: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    (tmp_path / "score").mkdir()
    context = make_context(tmp_path, isolated_repo=isolated_repo)

    blocked = score_run.run_evaluation(
        context,
        {
            "raw": "curl https://example.com",
            "argv": ["curl", "https://example.com"],
            "requires_opt_in": True,
            "network_access": True,
            "dangerous_reasons": ["network"],
        },
        tmp_path / "score",
        1,
        eval_timeout_seconds=1,
        allow_dangerous_eval=False,
        allow_network_tasks=False,
    )
    assert blocked["blocked"] is True
    assert blocked["failure_classification"] == "contract_invalid"

    network_only = score_run.run_evaluation(
        context,
        {
            "raw": "python3 ../tests/fixtures/pass_eval.py",
            "argv": ["python3", "../tests/fixtures/pass_eval.py"],
            "requires_opt_in": False,
            "network_access": True,
            "dangerous_reasons": ["network"],
        },
        tmp_path / "score",
        3,
        eval_timeout_seconds=1,
        allow_dangerous_eval=True,
        allow_network_tasks=False,
    )
    assert network_only["blocked"] is True

    timeout = score_run.run_evaluation(
        context,
        {
            "raw": "python3 ../tests/fixtures/sleep_eval.py",
            "argv": ["python3", "../tests/fixtures/sleep_eval.py"],
            "requires_opt_in": False,
            "network_access": False,
            "dangerous_reasons": [],
        },
        tmp_path / "score",
        2,
        eval_timeout_seconds=1,
        allow_dangerous_eval=True,
        allow_network_tasks=True,
    )
    assert timeout["exit_code"] == 124


def test_score_run_main_usage_and_signal() -> None:
    assert score_run.main([]) == 2
    try:
        score_run.handle_signal(signal.SIGTERM, None)
    except RuntimeError as exc:
        assert "SIGTERM" in str(exc)


def test_build_score_payload_covers_failure_branches(
    isolated_repo: pathlib.Path,
    tmp_path: pathlib.Path,
    monkeypatch,
) -> None:
    run_dir = tmp_path / "runs" / "failure"
    (run_dir / "score").mkdir(parents=True)
    (run_dir / "outputs").mkdir()
    (run_dir / "home").mkdir()
    (run_dir / "session").mkdir()
    (run_dir / "task.md").write_text(
        """# Task
Broken task

## Goal
Goal

## Constraints
- None

## Done
- Done

## Eval
```bash
python3 ../tests/fixtures/pass_eval.py
```

## Required Artifacts
- outputs/missing.txt

## Result JSON schema (source of truth)
```json
{"type":"object"}
```
""",
        encoding="utf-8",
    )
    (run_dir / "RUN.md").write_text("# Run\n", encoding="utf-8")
    (run_dir / "pi.exit_code.txt").write_text("nope\n", encoding="utf-8")
    (run_dir / "result.json").write_text("[]\n", encoding="utf-8")
    monkeypatch.setenv("HARNESS_ALLOW_DANGEROUS_EVAL", "0")

    payload = score_run.build_score_payload(
        make_context(
            run_dir,
            isolated_repo=isolated_repo,
            schema_path=run_dir / "missing.schema.json",
        ),
        cancelled=True,
    )

    assert "model_invocation_failed" in payload["failure_classifications"]
    assert "result_invalid" in payload["failure_classifications"]
    assert "eval_failed" in payload["failure_classifications"]


def test_build_context_derives_repo_root_from_run_directory(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = isolated_repo / "starter" / "runs" / "score-test"
    context = score_run.build_context(
        [
            str(run_dir / "task.md"),
            str(run_dir),
            str(run_dir / "pi.exit_code.txt"),
            str(run_dir / "score.json"),
        ]
    )

    assert context.repo_root == (isolated_repo / "starter").resolve()
    assert context.schema_path == (run_dir / "result.schema.json").resolve()


def test_resolve_repo_root_for_non_runs_directory(tmp_path: pathlib.Path) -> None:
    run_dir = tmp_path / "custom-run"
    run_dir.mkdir()

    assert score_run.resolve_repo_root(run_dir) == tmp_path.resolve()


def test_score_run_metadata_helpers_cover_remaining_path_and_env_branches(
    isolated_repo: pathlib.Path,
    monkeypatch,
) -> None:
    run_dir = make_run_dir(isolated_repo, execution_profile="capability")
    context = make_context(run_dir, isolated_repo=isolated_repo)
    outside_path = (isolated_repo / "outside.txt").resolve()

    assert score_run._relative_to_run_dir(context, None) is None
    assert score_run._relative_to_run_dir(context, outside_path) == str(outside_path)

    monkeypatch.setenv("HARNESS_CONTEXT_SOURCE_RUN_IDS", "alpha,beta")
    metadata = score_run._load_execution_metadata(context)

    assert metadata["context_source_run_ids"] == ["alpha", "beta"]


def test_collect_evaluations_marks_invalid_task_without_commands(
    isolated_repo: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    context = make_context(tmp_path, isolated_repo=isolated_repo)
    (tmp_path / "score").mkdir()

    result = score_run._collect_evaluations(
        context,
        {"ok": False, "eval_command_details": []},
        eval_timeout_seconds=1,
        allow_dangerous_eval=True,
        allow_network_tasks=True,
    )

    assert result.evaluations == ()
    assert result.failure_classifications == frozenset({"contract_invalid"})


def test_check_required_artifacts_rejects_out_of_scope_paths(
    isolated_repo: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "outputs").mkdir(parents=True)
    (run_dir / "score").mkdir()
    (run_dir / "home").mkdir()
    (run_dir / "session").mkdir()

    outside = tmp_path / "outside.txt"
    outside.write_text("outside\n", encoding="utf-8")
    (run_dir / "outside-link.txt").symlink_to(outside)

    result = score_run._check_required_artifacts(
        make_context(run_dir, isolated_repo=isolated_repo),
        [str(outside), "../outside.txt", "outside-link.txt"],
    )

    assert result.failure_classifications == frozenset({"contract_invalid"})
    assert {item["status"] for item in result.required_artifacts} == {"invalid_out_of_run_scope"}
    assert all(item["exists"] is False for item in result.required_artifacts)


def test_collect_evaluations_defaults_failed_eval_classification(
    isolated_repo: pathlib.Path,
    tmp_path: pathlib.Path,
    monkeypatch,
) -> None:
    context = make_context(tmp_path, isolated_repo=isolated_repo)
    (tmp_path / "score").mkdir()

    monkeypatch.setattr(
        score_run,
        "run_evaluation",
        lambda *args, **kwargs: {"passed": False},
    )

    result = score_run._collect_evaluations(
        context,
        {"ok": True, "eval_command_details": [{"argv": ["python3"], "raw": "python3"}]},
        eval_timeout_seconds=1,
        allow_dangerous_eval=True,
        allow_network_tasks=True,
    )

    assert result.failure_classifications == frozenset({"eval_failed"})
    assert result.evaluations == ({"passed": False},)


def test_score_run_happy_path(isolated_repo: pathlib.Path) -> None:
    run_dir = make_run_dir(isolated_repo)
    (run_dir / "outputs" / "claim.txt").write_text("ok\n", encoding="utf-8")
    (run_dir / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "all good",
                "artifacts": [{"path": "outputs/claim.txt", "description": "proof"}],
                "claims": [{"claim": "ok", "evidence": ["outputs/claim.txt"]}],
                "remaining_risks": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "pi.exit_code.txt").write_text("0\n", encoding="utf-8")

    previous_cwd = pathlib.Path.cwd()
    os.chdir(isolated_repo)
    try:
        exit_code = score_run.main(
            [
                str(run_dir / "task.md"),
                str(run_dir),
                str(run_dir / "pi.exit_code.txt"),
                str(run_dir / "score.json"),
                str(run_dir / "result.schema.json"),
            ]
        )
    finally:
        os.chdir(previous_cwd)

    assert exit_code == 0

    payload = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    assert payload["overall_pass"] is True
    assert payload["failure_classifications"] == []
    assert payload["execution_profile"] == "strict"
    assert payload["policy_path"] == "policies/strict.json"
    assert payload["retrieval"]["enabled"] is False


def test_score_run_persists_guardrail_artifacts_and_merges_existing_decisions(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = make_run_dir(isolated_repo)
    (run_dir / "outputs" / "claim.txt").write_text("ok\n", encoding="utf-8")
    (run_dir / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "all good",
                "artifacts": [{"path": "outputs/claim.txt", "description": "proof"}],
                "claims": [{"claim": "ok", "evidence": ["outputs/claim.txt"]}],
                "remaining_risks": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "pi.exit_code.txt").write_text("0\n", encoding="utf-8")

    contract = json.loads((run_dir / "run.contract.json").read_text(encoding="utf-8"))
    policy_path = isolated_repo / "starter" / "policies" / "test-deny-tool-use.json"
    policy_path.write_text(
        json.dumps(
            {
                "opt_in_env": "HARNESS_ALLOW_DANGEROUS_EVAL",
                "allow_network_env": "HARNESS_ALLOW_NETWORK_TASKS",
                "allowed_programs": [],
                "blocked_programs": [],
                "network_programs": [],
                "guardrails": {
                    "hooks": {
                        "pre_tool_use": {
                            "enabled": True,
                            "allow": False,
                            "allow_network_tools": True,
                            "allow_dangerous_commands": True,
                        }
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    contract["policy_path"] = "policies/test-deny-tool-use.json"
    (run_dir / "run.contract.json").write_text(
        json.dumps(contract) + "\n",
        encoding="utf-8",
    )
    (run_dir / "outputs" / "guardrails.json").write_text(
        json.dumps(
            {
                "selected_profile_id": "strict",
                "policy_path": "policies/strict.json",
                "decisions": [
                    {
                        "hook": "pre_run",
                        "allowed": True,
                        "violations": [],
                        "policy_version_source": "seed",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = score_run.build_score_payload(make_context(run_dir, isolated_repo=isolated_repo))
    persisted_guardrails = json.loads(
        (run_dir / "outputs" / "guardrails.json").read_text(encoding="utf-8")
    )
    persisted_hooks = {item["hook"] for item in persisted_guardrails["decisions"]}

    assert payload["overall_pass"] is False
    assert payload["failure_classifications"] == ["guardrail_policy_violation"]
    assert payload["guardrails"]["decisions"] == persisted_guardrails["decisions"]
    assert persisted_hooks == {"pre_run", "pre_tool_use"}
    assert persisted_guardrails["score_overall_pass"] is False
    assert persisted_guardrails["score_overall_error_code"] == "guardrail_policy_violation"


@pytest.mark.parametrize(
    "contract_version,contract_profile,env_profile,expected_profile,expected_policy,expected_retrieval_enabled",
    [
        ("v2", "strict", None, "strict", "policies/strict.json", False),
        ("v2", "capability", None, "capability", "policies/capability.json", True),
        ("v2", "heavy_tools", None, "heavy_tools", "policies/heavy_tools.json", True),
        ("v1", "strict", None, "strict", "policies/strict.json", False),
        ("v1", "strict", "networked", "networked", "policies/networked.json", False),
    ],
)
def test_score_run_contract_matrix_controls_execution_profile_and_policy(
    isolated_repo: pathlib.Path,
    monkeypatch,
    contract_version: str,
    contract_profile: str,
    env_profile: str | None,
    expected_profile: str,
    expected_policy: str,
    expected_retrieval_enabled: bool,
) -> None:
    run_dir = make_run_dir(
        isolated_repo,
        contract_version=contract_version,
        execution_profile=contract_profile,
    )
    (run_dir / "outputs" / "claim.txt").write_text("ok\n", encoding="utf-8")
    (run_dir / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "all good",
                "artifacts": [{"path": "outputs/claim.txt", "description": "proof"}],
                "claims": [{"claim": "ok", "evidence": ["outputs/claim.txt"]}],
                "remaining_risks": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    if env_profile is None:
        monkeypatch.delenv("HARNESS_EXECUTION_PROFILE", raising=False)
    else:
        monkeypatch.setenv("HARNESS_EXECUTION_PROFILE", env_profile)
    (run_dir / "pi.exit_code.txt").write_text("0\n", encoding="utf-8")

    payload = score_run.build_score_payload(make_context(run_dir, isolated_repo=isolated_repo))
    assert payload["execution_profile"] == expected_profile
    assert payload["policy_path"] == expected_policy
    assert payload["retrieval"]["enabled"] is expected_retrieval_enabled


def test_score_run_includes_selected_source_count_and_empty_context(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = make_run_dir(isolated_repo, execution_profile="capability")
    (run_dir / "outputs" / "claim.txt").write_text("ok\n", encoding="utf-8")
    (run_dir / "context").mkdir(exist_ok=True)
    (run_dir / "context" / "retrieval-manifest.json").write_text(
        json.dumps(
            {
                "retrieval_profile_id": "retrieval-v4-default",
                "selected_count": 1,
                "selected_source_count": 1,
                "empty_context": False,
                "candidate_run_count": 3,
                "eligible_run_count": 2,
                "ranking_latency_ms": 4.5,
                "artifact_bytes_copied": 42,
                "selected_source_run_ids": ["prior-run-1"],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "all good",
                "artifacts": [{"path": "outputs/claim.txt", "description": "proof"}],
                "claims": [{"claim": "ok", "evidence": ["outputs/claim.txt"]}],
                "remaining_risks": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "pi.exit_code.txt").write_text("0\n", encoding="utf-8")

    payload = score_run.build_score_payload(make_context(run_dir, isolated_repo=isolated_repo))

    assert payload["retrieval"]["enabled"] is True
    assert payload["retrieval"]["retrieval_profile_id"] == "retrieval-v4-default"
    assert payload["retrieval"]["selected_count"] == 1
    assert payload["retrieval"]["selected_source_count"] == 1
    assert payload["retrieval"]["empty_context"] is False


def test_score_run_reports_invalid_result(isolated_repo: pathlib.Path) -> None:
    run_dir = make_run_dir(isolated_repo)
    (run_dir / "outputs" / "claim.txt").write_text("ok\n", encoding="utf-8")
    (run_dir / "result.json").write_text("{broken\n", encoding="utf-8")
    (run_dir / "pi.exit_code.txt").write_text("0\n", encoding="utf-8")

    env = os.environ | {"PYTHONPATH": str(isolated_repo / "starter" / "bin")}
    subprocess.run(
        [
            sys.executable,
            str(isolated_repo / "starter" / "bin" / "score_run.py"),
            str(run_dir / "task.md"),
            str(run_dir),
            str(run_dir / "pi.exit_code.txt"),
            str(run_dir / "score.json"),
            str(run_dir / "result.schema.json"),
        ],
        cwd=isolated_repo,
        check=True,
        env=env,
        capture_output=True,
        text=True,
    )

    payload = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    assert "result_invalid" in payload["failure_classifications"]


def test_validate_result_json_reports_parse_errors_in_process(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = make_run_dir(isolated_repo)
    (run_dir / "result.json").write_text("{broken\n", encoding="utf-8")

    validation = score_run._validate_result_json(make_context(run_dir, isolated_repo=isolated_repo))

    assert validation.result_json_present is True
    assert validation.result_json_valid_schema is False
    assert "json parse error:" in validation.result_json_validations[0]["observed"]


def test_score_run_reports_missing_result_json(isolated_repo: pathlib.Path) -> None:
    run_dir = make_run_dir(isolated_repo)
    (run_dir / "outputs" / "claim.txt").write_text("ok\n", encoding="utf-8")
    (run_dir / "pi.exit_code.txt").write_text("0\n", encoding="utf-8")

    payload = score_run.build_score_payload(make_context(run_dir, isolated_repo=isolated_repo))

    assert payload["result_json_present"] is False
    assert payload["result_json_valid_schema"] is False
    assert "result_invalid" in payload["failure_classifications"]
    assert payload["result_json_validations"][0]["observed"] == "missing file"


def test_score_run_reports_non_object_result_json(isolated_repo: pathlib.Path) -> None:
    run_dir = make_run_dir(isolated_repo)
    (run_dir / "outputs" / "claim.txt").write_text("ok\n", encoding="utf-8")
    (run_dir / "result.json").write_text("[]\n", encoding="utf-8")
    (run_dir / "pi.exit_code.txt").write_text("0\n", encoding="utf-8")

    payload = score_run.build_score_payload(make_context(run_dir, isolated_repo=isolated_repo))

    assert payload["result_json_present"] is True
    assert payload["result_json_valid_schema"] is False
    assert payload["result_json_validations"][0]["observed"] == "list"


def test_score_run_secret_findings_fail_score(isolated_repo: pathlib.Path) -> None:
    run_dir = make_run_dir(isolated_repo)
    (run_dir / "outputs" / "claim.txt").write_text("ok\n", encoding="utf-8")
    (run_dir / "outputs" / "secret.txt").write_text(
        "OPENAI_API_KEY=" + ("sk-" + "abcdefghijklmnopqrstuvwxyz1234"),
        encoding="utf-8",
    )
    (run_dir / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "all good",
                "artifacts": [{"path": "outputs/claim.txt", "description": "proof"}],
                "claims": [{"claim": "ok", "evidence": ["outputs/claim.txt"]}],
                "remaining_risks": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "pi.exit_code.txt").write_text("0\n", encoding="utf-8")

    payload = score_run.build_score_payload(make_context(run_dir, isolated_repo=isolated_repo))

    assert payload["overall_pass"] is False
    assert "eval_failed" in payload["failure_classifications"]
    assert payload["secret_scan"]["findings"]


def test_discover_secret_scan_paths_excludes_isolated_state(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = make_run_dir(isolated_repo)
    visible = run_dir / "outputs" / "claim.txt"
    visible.write_text("ok\n", encoding="utf-8")
    hidden_home = run_dir / "home" / "secret.txt"
    hidden_home.write_text("ignore\n", encoding="utf-8")
    hidden_session = run_dir / "session" / "session.txt"
    hidden_session.write_text("ignore\n", encoding="utf-8")
    recovery_file = run_dir / "recovery" / "recovery.txt"
    recovery_file.parent.mkdir()
    recovery_file.write_text("scan me\n", encoding="utf-8")
    imported_context = run_dir / "context" / "source-runs" / "old-run" / "artifact.txt"
    imported_context.parent.mkdir(parents=True)
    imported_context.write_text("historical\n", encoding="utf-8")
    retrieval_summary = run_dir / "context" / "retrieval-summary.md"
    retrieval_summary.parent.mkdir(exist_ok=True)
    retrieval_summary.write_text("summary\n", encoding="utf-8")

    candidates = score_run.discover_secret_scan_paths(
        make_context(run_dir, isolated_repo=isolated_repo)
    )

    assert visible in candidates
    assert hidden_home not in candidates
    assert hidden_session not in candidates
    assert recovery_file in candidates
    assert imported_context not in candidates
    assert retrieval_summary in candidates


def test_score_run_recovery_secret_findings_fail_score(isolated_repo: pathlib.Path) -> None:
    run_dir = make_run_dir(isolated_repo)
    (run_dir / "outputs" / "claim.txt").write_text("ok\n", encoding="utf-8")
    recovery_secret = run_dir / "recovery" / "secret.txt"
    recovery_secret.parent.mkdir()
    recovery_secret.write_text(
        "OPENAI_API_KEY=" + ("sk-" + "abcdefghijklmnopqrstuvwxyz1234"),
        encoding="utf-8",
    )
    (run_dir / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "all good",
                "artifacts": [{"path": "outputs/claim.txt", "description": "proof"}],
                "claims": [{"claim": "ok", "evidence": ["outputs/claim.txt"]}],
                "remaining_risks": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "pi.exit_code.txt").write_text("0\n", encoding="utf-8")

    payload = score_run.build_score_payload(make_context(run_dir, isolated_repo=isolated_repo))

    assert payload["overall_pass"] is False
    assert "eval_failed" in payload["failure_classifications"]
    assert any(
        "recovery/secret.txt" in finding["path"] for finding in payload["secret_scan"]["findings"]
    )


def test_score_run_records_secret_scan_stats_and_skipped_roots(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = make_run_dir(isolated_repo)
    (run_dir / "outputs" / "claim.txt").write_text("ok\n", encoding="utf-8")
    (run_dir / "context").mkdir()
    (run_dir / "context" / "retrieval-manifest.json").write_text("{}\n", encoding="utf-8")
    (run_dir / "context" / "retrieval-summary.md").write_text("summary\n", encoding="utf-8")
    imported_context = run_dir / "context" / "source-runs" / "old-run" / "artifact.txt"
    imported_context.parent.mkdir(parents=True)
    imported_context.write_text(
        "OPENAI_API_KEY=" + ("sk-" + "abcdefghijklmnopqrstuvwxyz1234"),
        encoding="utf-8",
    )
    (run_dir / "home" / "secret.txt").write_text("ignore\n", encoding="utf-8")
    (run_dir / "session" / "session.txt").write_text("ignore\n", encoding="utf-8")
    (run_dir / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "all good",
                "artifacts": [{"path": "outputs/claim.txt", "description": "proof"}],
                "claims": [{"claim": "ok", "evidence": ["outputs/claim.txt"]}],
                "remaining_risks": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "pi.exit_code.txt").write_text("0\n", encoding="utf-8")

    payload = score_run.build_score_payload(make_context(run_dir, isolated_repo=isolated_repo))

    assert payload["overall_pass"] is True
    assert payload["secret_scan"]["scanned_path_count"] >= 4
    assert payload["secret_scan"]["skipped_path_count"] == 3
    assert payload["secret_scan"]["skipped_reason_counts"] == {
        "context/source-runs": 1,
        "home": 1,
        "session": 1,
    }
    assert payload["secret_scan"]["findings"] == []


def test_main_writes_partial_payload_when_interrupted(
    tmp_path: pathlib.Path,
    monkeypatch,
    capsys,
) -> None:
    context = ScoreContext(
        task_path=tmp_path / "task.md",
        run_dir=tmp_path / "run",
        exit_code_path=tmp_path / "pi.exit_code.txt",
        out_path=tmp_path / "score.json",
        schema_path=tmp_path / "result.schema.json",
        event_log_path=tmp_path / "run-events.jsonl",
        repo_root=tmp_path,
    )
    context.run_dir.mkdir()

    calls: list[bool] = []

    def fake_build_context(_argv: list[str]) -> ScoreContext:
        return context

    def fake_build_score_payload(
        _context: ScoreContext,
        *,
        cancelled: bool = False,
    ) -> dict[str, object]:
        calls.append(cancelled)
        if not cancelled:
            raise RuntimeError("boom")
        return {
            "overall_pass": False,
            "overall_error_code": "eval_failed",
            "failure_classifications": ["eval_failed"],
            "cancelled": True,
        }

    monkeypatch.setattr(score_run, "build_context", fake_build_context)
    monkeypatch.setattr(score_run, "build_score_payload", fake_build_score_payload)
    monkeypatch.setattr(score_run, "append_event", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="boom"):
        score_run.main(["task.md", "run", "pi.exit_code.txt", "score.json"])

    payload = json.loads(context.out_path.read_text(encoding="utf-8"))
    captured = capsys.readouterr()
    assert calls == [False, True]
    assert payload["interruption"] == "boom"
    assert str(context.out_path) in captured.out


def test_score_run_script_main_entrypoint(
    isolated_repo: pathlib.Path,
    monkeypatch,
    capsys,
) -> None:
    run_dir = make_run_dir(isolated_repo)
    (run_dir / "outputs" / "claim.txt").write_text("ok\n", encoding="utf-8")
    (run_dir / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "all good",
                "artifacts": [{"path": "outputs/claim.txt", "description": "proof"}],
                "claims": [{"claim": "ok", "evidence": ["outputs/claim.txt"]}],
                "remaining_risks": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "pi.exit_code.txt").write_text("0\n", encoding="utf-8")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(pathlib.Path(score_run.__file__).resolve()),
            str(run_dir / "task.md"),
            str(run_dir),
            str(run_dir / "pi.exit_code.txt"),
            str(run_dir / "score.json"),
            str(run_dir / "result.schema.json"),
        ],
    )

    with pytest.raises(SystemExit) as excinfo:
        runpy.run_path(str(pathlib.Path(score_run.__file__).resolve()), run_name="__main__")

    captured = capsys.readouterr()
    assert excinfo.value.code == 0
    assert str(run_dir / "score.json") in captured.out


def test_score_run_records_retrieval_provenance(
    isolated_repo: pathlib.Path,
    monkeypatch,
) -> None:
    run_dir = make_run_dir(isolated_repo, execution_profile="capability")
    (run_dir / "outputs" / "claim.txt").write_text("ok\n", encoding="utf-8")
    (run_dir / "context").mkdir()
    (run_dir / "context" / "retrieval-manifest.json").write_text(
        json.dumps(
            {
                "selected_source_run_ids": ["old-run-1", "old-run-2"],
                "index_mode": "warm_reuse",
                "candidate_run_count": 5,
                "eligible_run_count": 4,
                "selected_count": 2,
                "ranking_latency_ms": 1.25,
                "artifact_bytes_copied": 99,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "all good",
                "artifacts": [{"path": "outputs/claim.txt", "description": "proof"}],
                "claims": [{"claim": "ok", "evidence": ["outputs/claim.txt"]}],
                "remaining_risks": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "pi.exit_code.txt").write_text("0\n", encoding="utf-8")
    monkeypatch.setenv("HARNESS_EXECUTION_PROFILE", "capability")
    monkeypatch.setenv("HARNESS_POLICY_PATH", "policies/capability.json")
    monkeypatch.setenv("HARNESS_CONTEXT_ENABLED", "1")
    monkeypatch.setenv("HARNESS_CONTEXT_MANIFEST_PATH", "context/retrieval-manifest.json")

    payload = score_run.build_score_payload(make_context(run_dir, isolated_repo=isolated_repo))

    assert payload["execution_profile"] == "capability"
    assert payload["policy_path"] == "policies/capability.json"
    assert payload["retrieval"]["enabled"] is True
    assert payload["retrieval"]["source_run_ids"] == ["old-run-1", "old-run-2"]
    assert payload["retrieval"]["context_manifest_path"] == "context/retrieval-manifest.json"
    assert payload["retrieval"]["index_mode"] == "warm_reuse"
    assert payload["retrieval"]["candidate_run_count"] == 5
    assert payload["retrieval"]["eligible_run_count"] == 4
    assert payload["retrieval"]["selected_count"] == 2
    assert payload["retrieval"]["ranking_latency_ms"] == 1.25
    assert payload["retrieval"]["artifact_bytes_copied"] == 99
