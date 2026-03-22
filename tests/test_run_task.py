from __future__ import annotations

import io
import json
import os
import pathlib
import signal
from contextlib import redirect_stdout

import pytest
import run_task


def make_run_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    run_dir = tmp_path / "runs" / "state-test"
    (run_dir / "outputs").mkdir(parents=True)
    return run_dir


def test_resolve_initial_state_reports_running_locked(tmp_path: pathlib.Path) -> None:
    run_dir = make_run_dir(tmp_path)
    runner = run_task.RunTaskRunner([str(run_dir)])
    runner.lock_dir.mkdir()
    (runner.lock_dir / "pid").write_text(f"{os.getpid()}\n", encoding="utf-8")

    buffer = io.StringIO()
    with redirect_stdout(buffer):
        state = runner._resolve_initial_state()

    assert state == "running"
    assert buffer.getvalue().strip() == "run state: running"


def test_resolve_initial_state_treats_stale_lock_as_partial(tmp_path: pathlib.Path) -> None:
    run_dir = make_run_dir(tmp_path)
    runner = run_task.RunTaskRunner([str(run_dir)])
    runner.lock_dir.mkdir()
    (runner.lock_dir / "pid").write_text("999999999\n", encoding="utf-8")

    assert runner._resolve_initial_state() == "partial"


def test_resolve_initial_state_reports_complete_and_partial_profiles(
    tmp_path: pathlib.Path,
) -> None:
    run_dir = make_run_dir(tmp_path)
    payload_path = run_dir / "outputs" / "run_manifest.json"
    payload_path.write_text(json.dumps({"state": "complete"}) + "\n", encoding="utf-8")

    runner = run_task.RunTaskRunner([str(run_dir)])
    assert runner._resolve_initial_state() == "complete"

    runner = run_task.RunTaskRunner([str(run_dir)], config_env={"HARNESS_FORCE_RERUN": "1"})
    assert runner._resolve_initial_state() == "partial"


def test_resolve_initial_state_treats_partial_manifest_as_partial(tmp_path: pathlib.Path) -> None:
    run_dir = make_run_dir(tmp_path)
    payload_path = run_dir / "outputs" / "run_manifest.json"
    payload_path.write_text(json.dumps({"state": "cancelled"}) + "\n", encoding="utf-8")

    runner = run_task.RunTaskRunner([str(run_dir)])
    assert runner._resolve_initial_state() == "partial"


def test_pi_retry_boundary_retries_startup_failure_when_transcript_is_empty(
    tmp_path: pathlib.Path,
) -> None:
    run_dir = make_run_dir(tmp_path)
    (run_dir / "task.md").write_text("task", encoding="utf-8")
    (run_dir / "RUN.md").write_text("run", encoding="utf-8")
    runner = run_task.RunTaskRunner(
        [str(run_dir)],
        config_env={
            "HARNESS_PI_RETRY_COUNT": "3",
            "PYTHONPATH": str(tmp_path),
        },
    )

    attempts: list[int] = []

    def fail_startup() -> int:
        attempts.append(1)
        return 75

    runner._invoke_pi = fail_startup
    runner._sleep = lambda _: None

    runner._run_pi_loop()
    assert attempts == [1, 1, 1]
    assert (run_dir / "pi.exit_code.txt").read_text(encoding="utf-8").strip() == "75"


def test_pi_retry_boundary_stops_when_transcript_is_not_empty(tmp_path: pathlib.Path) -> None:
    run_dir = make_run_dir(tmp_path)
    (run_dir / "task.md").write_text("task", encoding="utf-8")
    (run_dir / "RUN.md").write_text("run", encoding="utf-8")
    runner = run_task.RunTaskRunner(
        [str(run_dir)],
        config_env={
            "HARNESS_PI_RETRY_COUNT": "3",
            "PYTHONPATH": str(tmp_path),
        },
    )

    attempts: list[int] = []

    def fail_with_output() -> int:
        attempts.append(1)
        (run_dir / "transcript.jsonl").write_text("started\n", encoding="utf-8")
        return 75

    runner._invoke_pi = fail_with_output
    runner._sleep = lambda _: None

    runner._run_pi_loop()
    assert attempts == [1]
    assert (run_dir / "pi.exit_code.txt").read_text(encoding="utf-8").strip() == "75"


def test_score_retry_boundary_executes_two_attempts(tmp_path: pathlib.Path) -> None:
    run_dir = make_run_dir(tmp_path)
    (run_dir / "task.md").write_text("task", encoding="utf-8")
    (run_dir / "RUN.md").write_text("run", encoding="utf-8")
    runner = run_task.RunTaskRunner([str(run_dir)], config_env={"PYTHONPATH": str(tmp_path)})

    attempts: list[int] = []

    def fake_score() -> int:
        attempts.append(1)
        return 1 if len(attempts) < 2 else 0

    runner._invoke_score = fake_score
    runner._sleep = lambda _: None

    runner._run_score_loop()
    assert attempts == [1, 1]


def test_signal_path_marks_state_as_cancelled(tmp_path: pathlib.Path) -> None:
    run_dir = make_run_dir(tmp_path)
    (run_dir / "task.md").write_text("task", encoding="utf-8")
    runner = run_task.RunTaskRunner([str(run_dir)], config_env={"PYTHONPATH": str(tmp_path)})

    with pytest.raises(SystemExit) as exc:
        runner._handle_signal(signal.SIGINT, None)

    assert exc.value.code == 130
    assert (run_dir / "run.state").read_text(encoding="utf-8").strip() == "cancelled"
    manifest = json.loads((run_dir / "outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["state"] == "cancelled"
    assert manifest["phase"] == "cancelled"
    assert manifest["error_code"] == "cancelled"
