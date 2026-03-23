from __future__ import annotations

import asyncio
import json
import pathlib
import time

import control_centerlib as cclib
import harvester
import pytest


def _make_repo_root(tmp_path: pathlib.Path, name: str = "repo") -> pathlib.Path:
    root = tmp_path / name
    (root / "starter" / "bin").mkdir(parents=True)
    (root / "starter" / "runs").mkdir()
    for rel in ("run-task.sh", "orchestrator.py", "run_real_canary.py"):
        path = root / "starter" / "bin" / rel
        path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        path.chmod(0o755)
    return root


def _write_run(
    runs_root: pathlib.Path,
    run_id: str,
    *,
    state: str = "complete",
    overall_pass: bool | None = True,
    profile: str = "strict",
    primary_error_code: str = "",
) -> pathlib.Path:
    run_dir = runs_root / run_id
    now_ms = int(time.time() * 1000)
    (run_dir / "outputs").mkdir(parents=True)
    (run_dir / "run-events.jsonl").write_text('{"message":"event"}\n', encoding="utf-8")
    (run_dir / "transcript.jsonl").write_text('{"type":"message"}\n', encoding="utf-8")
    (run_dir / "patch.diff").write_text("diff --git a/a b/a\n", encoding="utf-8")
    (run_dir / "result.json").write_text('{"status":"success"}\n', encoding="utf-8")
    (run_dir / "RUN.md").write_text("# Run\n", encoding="utf-8")
    (run_dir / "task.md").write_text("# Task\n", encoding="utf-8")
    (run_dir / "run.state").write_text(f"{state}\n", encoding="utf-8")
    (run_dir / "run.contract.json").write_text(
        json.dumps({"execution_profile": profile}) + "\n",
        encoding="utf-8",
    )
    manifest = {
        "state": state,
        "primary_error_code": primary_error_code or None,
        "failure_classifications": [] if not primary_error_code else [primary_error_code],
        "execution": {"profile": profile},
        "orchestration": {
            "worker_id": "worker-1",
            "queue_wait_ms": 120,
            "score_wait_ms": 40,
        },
        "timings": {
            "run_started_epoch_ms": now_ms - 2000,
            "run_finished_epoch_ms": now_ms,
            "run_duration_ms": 2000,
        },
    }
    (run_dir / "outputs" / "run_manifest.json").write_text(
        json.dumps(manifest) + "\n",
        encoding="utf-8",
    )
    score = {"execution_profile": profile, "failure_classifications": []}
    if overall_pass is not None:
        score["overall_pass"] = overall_pass
    if primary_error_code:
        score["overall_error_code"] = primary_error_code
    (run_dir / "score.json").write_text(json.dumps(score) + "\n", encoding="utf-8")
    return run_dir


def test_load_control_center_config_derives_runs_root(tmp_path: pathlib.Path) -> None:
    repo_root = _make_repo_root(tmp_path, "alpha")
    config_path = tmp_path / "control-center.toml"
    config_path.write_text(
        "\n".join(
            [
                "[ui]",
                "refresh_interval_seconds = 2.5",
                "window_days = 10",
                "",
                "[[repo]]",
                'id = "alpha"',
                'name = "Alpha"',
                f'root = "{repo_root}"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    config = cclib.load_control_center_config(config_path)

    assert config.ui.refresh_interval_seconds == 2.5
    assert config.ui.window_days == 10
    assert config.repos[0].runs_root == (repo_root / "starter" / "runs").resolve()


def test_repo_supervisor_start_stop_restart_and_backoff(
    tmp_path: pathlib.Path,
    monkeypatch,
) -> None:
    repo_root = _make_repo_root(tmp_path, "alpha")
    repo = cclib.RepoConfig(
        id="alpha",
        name="Alpha",
        root=repo_root,
        runs_root=(repo_root / "starter" / "runs").resolve(),
        auto_start=False,
        max_model_workers=3,
        max_score_workers=4,
        orchestrator_poll_seconds=1.5,
    )
    popen_calls: list[dict[str, object]] = []

    class FakeProcess:
        next_pid = 500

        def __init__(self, command: list[str], **kwargs):
            self.command = command
            self.kwargs = kwargs
            self.returncode = None
            self.pid = FakeProcess.next_pid
            FakeProcess.next_pid += 1
            popen_calls.append({"command": command, "kwargs": kwargs, "process": self})

        def poll(self) -> int | None:
            return self.returncode

        def terminate(self) -> None:
            self.returncode = 0

        def wait(self, timeout: float | None = None) -> int:
            if self.returncode is None:
                self.returncode = 0
            return self.returncode

        def kill(self) -> None:
            self.returncode = -9

    monotonic_value = {"value": 1.0}
    monkeypatch.setattr(cclib.subprocess, "Popen", FakeProcess)
    monkeypatch.setattr(cclib, "_monotonic", lambda: monotonic_value["value"])

    supervisor = cclib.RepoSupervisor(repo)

    assert supervisor.start() == "orchestrator started"
    assert popen_calls[0]["command"] == [
        "python3",
        str(repo_root / "starter" / "bin" / "orchestrator.py"),
        "--runs-root",
        str(repo.runs_root),
        "--max-model-workers",
        "3",
        "--max-score-workers",
        "4",
    ]
    assert popen_calls[0]["kwargs"]["env"]["HARNESS_ORCHESTRATOR_POLL_SECONDS"] == "1.5"

    first_process = popen_calls[0]["process"]
    first_process.returncode = 9
    supervisor.poll()

    assert supervisor.snapshot().state == "backoff"
    assert supervisor.last_exit_code == 9

    monotonic_value["value"] = 2.3
    supervisor.poll()
    assert len(popen_calls) == 2
    assert supervisor.snapshot().state == "running"
    assert supervisor.restart_failures == 1

    assert supervisor.restart() == "orchestrator restarted"
    assert len(popen_calls) == 3
    assert supervisor.stop() == "orchestrator stopped"
    assert supervisor.snapshot().state == "stopped"


def test_control_center_service_collects_rows_and_safe_actions(
    tmp_path: pathlib.Path,
    monkeypatch,
) -> None:
    repo_root = _make_repo_root(tmp_path, "alpha")
    runs_root = repo_root / "starter" / "runs"
    queued_run = _write_run(runs_root, "run-queued", state="partial", overall_pass=None)
    _write_run(
        runs_root,
        "run-failed",
        state="failed",
        overall_pass=False,
        primary_error_code="eval_failed",
    )
    orchestrator_root = runs_root / ".orchestrator"
    orchestrator_root.mkdir(parents=True)
    (orchestrator_root / "run_queue.jsonl").write_text(
        json.dumps(
            {
                "type": "run",
                "kind": "run",
                "run_id": "run-queued",
                "run_dir": str(queued_run),
                "attempt": 2,
                "state": "queued",
                "worker_id": "queue-worker",
                "ts_ms": 1,
                "queued_at_ms": 1,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    config = cclib.ControlCenterConfig(
        ui=cclib.UIConfig(),
        repos=(
            cclib.RepoConfig(
                id="alpha",
                name="Alpha",
                root=repo_root,
                runs_root=runs_root,
                auto_start=False,
            ),
        ),
        source_path=None,
    )
    service = cclib.ControlCenterService(config)
    snapshot = service.refresh()

    assert snapshot.repos[0].queue_depth == 1
    assert snapshot.repos[0].runs[0].artifact_paths["manifest"].name == "run_manifest.json"
    assert any(run.run_id == "run-failed" for run in snapshot.repos[0].runs)

    assert service.cancel_run("alpha", "run-queued") == "cancellation requested for run-queued"
    assert (queued_run / ".orchestrator-cancel").exists()

    (queued_run / ".orchestrator-cancel").unlink()
    (orchestrator_root / "run_queue.jsonl").write_text("", encoding="utf-8")
    service.refresh()
    assert service.enqueue_run("alpha", "run-queued") == "enqueued run-queued"
    queue_entries = harvester.read_queue_entries(orchestrator_root / "run_queue.jsonl")
    assert queue_entries["run-queued"]["worker_id"] == "control-center"
    assert (queued_run / "run.state").read_text(encoding="utf-8").strip() == "queued"

    popen_calls: list[dict[str, object]] = []

    class FakeProcess:
        def __init__(self, command: list[str], **kwargs):
            self.args = command
            self.kwargs = kwargs
            self.returncode = None
            self.pid = 700 + len(popen_calls)
            popen_calls.append({"command": command, "kwargs": kwargs, "process": self})

        def poll(self) -> int | None:
            return self.returncode

        def terminate(self) -> None:
            self.returncode = 0

        def wait(self, timeout: float | None = None) -> int:
            if self.returncode is None:
                self.returncode = 0
            return self.returncode

        def kill(self) -> None:
            self.returncode = -9

    monkeypatch.setattr(cclib.subprocess, "Popen", FakeProcess)
    assert service.rerun_run("alpha", "run-failed") == "rerun run-failed started"
    supervisor = service.supervisors["alpha"]
    assert "rerun:run-failed" in supervisor.active_commands
    managed = supervisor.active_commands["rerun:run-failed"]
    assert managed.process.args[0].endswith("run-task.sh")


def test_control_center_app_filters_and_command_palette(tmp_path: pathlib.Path) -> None:
    pytest.importorskip("textual")
    from control_center import ControlCenterApp
    from textual.widgets import DataTable

    repo_one = _make_repo_root(tmp_path, "alpha")
    repo_two = _make_repo_root(tmp_path, "beta")
    _write_run(repo_one / "starter" / "runs", "run-success", state="complete", overall_pass=True)
    _write_run(
        repo_two / "starter" / "runs",
        "run-failure",
        state="failed",
        overall_pass=False,
        primary_error_code="eval_failed",
    )

    service = cclib.ControlCenterService(
        cclib.ControlCenterConfig(
            ui=cclib.UIConfig(refresh_interval_seconds=60.0),
            repos=(
                cclib.RepoConfig(
                    id="alpha",
                    name="Alpha",
                    root=repo_one,
                    runs_root=repo_one / "starter" / "runs",
                    auto_start=False,
                ),
                cclib.RepoConfig(
                    id="beta",
                    name="Beta",
                    root=repo_two,
                    runs_root=repo_two / "starter" / "runs",
                    auto_start=False,
                ),
            ),
            source_path=None,
        )
    )

    service.start_repo = lambda repo_id: f"started {repo_id}"  # type: ignore[method-assign]

    async def runner() -> None:
        app = ControlCenterApp(service)
        async with app.run_test() as pilot:
            await pilot.pause()
            repo_table = app.query_one("#repo-table", DataTable)
            run_table = app.query_one("#run-table", DataTable)
            assert repo_table.row_count == 2
            assert run_table.row_count == 1

            await pilot.press("j")
            await pilot.pause()
            assert app.selected_repo_id == "beta"

            await pilot.press("/")
            await pilot.press(
                "s",
                "t",
                "a",
                "t",
                "e",
                ":",
                "f",
                "a",
                "i",
                "l",
                "e",
                "d",
                "enter",
            )
            await pilot.pause()
            assert run_table.row_count == 1
            assert app.selected_run_id == "run-failure"

            await pilot.press(":")
            for key in "open transcript":
                await pilot.press("space" if key == " " else key)
            await pilot.press("enter")
            await pilot.pause()
            assert app.query_one("#detail-tabs").active == "tab-transcript"

            await pilot.press(":")
            for key in "repo start":
                await pilot.press("space" if key == " " else key)
            await pilot.press("enter")
            await pilot.pause()
            assert "started beta" in str(app.query_one("#status-line").renderable)

    asyncio.run(runner())
