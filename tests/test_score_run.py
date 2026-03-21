from __future__ import annotations

import json
import os
import pathlib
import signal
import subprocess
import sys

import score_run
from harnesslib import default_run_contract

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


def make_run_dir(isolated_repo: pathlib.Path) -> pathlib.Path:
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
        json.dumps(default_run_contract(), indent=2) + "\n",
        encoding="utf-8",
    )
    return run_dir


def test_score_run_helpers_cover_blocked_and_timeout_paths(
    isolated_repo: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    score_run.run_dir = tmp_path
    score_run.repo_root = isolated_repo / "starter"
    (tmp_path / "score").mkdir()

    blocked = score_run.run_evaluation(
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
    score_run.task_path = run_dir / "task.md"
    score_run.run_dir = run_dir
    score_run.exit_code_path = run_dir / "pi.exit_code.txt"
    score_run.out_path = run_dir / "score.json"
    score_run.schema_path = run_dir / "missing.schema.json"
    score_run.event_log_path = run_dir / "run-events.jsonl"
    score_run.repo_root = isolated_repo / "starter"
    monkeypatch.setenv("HARNESS_ALLOW_DANGEROUS_EVAL", "0")

    payload = score_run.build_score_payload(cancelled=True)

    assert "model_invocation_failed" in payload["failure_classifications"]
    assert "result_invalid" in payload["failure_classifications"]
    assert "eval_failed" in payload["failure_classifications"]


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
