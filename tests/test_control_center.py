from __future__ import annotations

import asyncio
import json
import os
import pathlib
import shutil
import time

import control_center
import control_centerlib as cclib
import harvester
import pytest


def _make_repo_root(tmp_path: pathlib.Path, name: str = "repo") -> pathlib.Path:
    root = tmp_path / name
    (root / "starter" / "bin").mkdir(parents=True)
    (root / "starter" / "runs").mkdir()
    script_payloads = {
        "run-task.sh": "#!/bin/sh\nexit 0\n",
        "orchestrator.py": "#!/bin/sh\nexit 0\n",
        "run_real_canary.py": "#!/bin/sh\nexit 0\n",
        "archive-run-evidence.sh": (
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "run_dir=\"$1\"\n"
            "archive_path=\"${2:-${run_dir%/}.tgz}\"\n"
            "tar -czf \"$archive_path\""
            " -C \"$(dirname \"$run_dir\")\""
            " \"$(basename \"$run_dir\")\"\n"
            "echo \"$archive_path\"\n"
        ),
        "restore-run-evidence.sh": (
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "archive_path=\"$1\"\n"
            "destination_dir=\"$2\"\n"
            "mkdir -p \"$destination_dir\"\n"
            "tar -xzf \"$archive_path\" -C \"$destination_dir\"\n"
            "echo \"$destination_dir\"\n"
        ),
        "check-supported-runtime.sh": (
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "if [[ -f .runtime-check-fail ]]; then\n"
            "  cat .runtime-check-fail >&2\n"
            "  exit 1\n"
            "fi\n"
            "echo \"supported runtime check passed: python 3.12.9, pi 0.61.1\"\n"
        ),
    }
    for rel, contents in script_payloads.items():
        path = root / "starter" / "bin" / rel
        path.write_text(contents, encoding="utf-8")
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


def _write_canary_summary(
    runs_root: pathlib.Path,
    *,
    suffix: str = "latest",
    finished_at: str = "2026-03-23T12:00:00Z",
    overall_ok: bool = True,
) -> pathlib.Path:
    path = runs_root / f"real-canary-{suffix}.summary.json"
    path.write_text(
        json.dumps(
            {
                "summary_version": "v2",
                "generated_at": finished_at,
                "finished_at": finished_at,
                "overall_ok": overall_ok,
                "scenario_totals": {
                    "total": 6,
                    "passed": 6 if overall_ok else 5,
                    "failed": 0 if overall_ok else 1,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _poll_supervisor_until_idle(
    supervisor: cclib.RepoSupervisor,
    *,
    timeout_seconds: float = 2.0,
) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        supervisor.poll()
        if not supervisor.active_commands:
            return
        time.sleep(0.02)
    raise AssertionError("managed command did not finish before timeout")


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


def test_startup_preflight_passes_for_valid_repo(tmp_path: pathlib.Path, monkeypatch) -> None:
    repo_root = _make_repo_root(tmp_path, "alpha")
    config_path = tmp_path / "control-center.toml"
    config_path.write_text(
        "\n".join(
            [
                "[[repo]]",
                'id = "alpha"',
                'name = "Alpha"',
                f'root = "{repo_root}"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(cclib, "_preflight_python_issue", lambda repo_root: None)
    report = cclib.run_startup_preflight(config_path)

    assert report.ok is True
    assert report.config is not None
    assert report.repo_reports[0].ok is True


def test_startup_preflight_fails_for_missing_repo_root(tmp_path: pathlib.Path) -> None:
    config_path = tmp_path / "control-center.toml"
    config_path.write_text(
        "\n".join(
            [
                "[[repo]]",
                'id = "alpha"',
                'name = "Alpha"',
                f'root = "{tmp_path / "missing"}"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    report = cclib.run_startup_preflight(config_path)

    assert report.ok is False
    assert "repo root does not exist" in report.repo_reports[0].issues[0].message


def test_startup_preflight_fails_for_missing_required_script(
    tmp_path: pathlib.Path,
    monkeypatch,
) -> None:
    repo_root = _make_repo_root(tmp_path, "alpha")
    (repo_root / "starter" / "bin" / "run_real_canary.py").unlink()
    config_path = tmp_path / "control-center.toml"
    config_path.write_text(
        "\n".join(
            [
                "[[repo]]",
                'id = "alpha"',
                'name = "Alpha"',
                f'root = "{repo_root}"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(cclib, "_preflight_python_issue", lambda repo_root: None)
    report = cclib.run_startup_preflight(config_path)

    assert report.ok is False
    assert any(
        "required script is missing" in issue.message
        for issue in report.repo_reports[0].issues
    )


def test_startup_preflight_blocks_live_supervisor_pid(tmp_path: pathlib.Path, monkeypatch) -> None:
    repo_root = _make_repo_root(tmp_path, "alpha")
    status_path = repo_root / "starter" / "runs" / ".orchestrator" / "supervisor-status.json"
    status_path.parent.mkdir(parents=True)
    status_path.write_text(
        json.dumps({"pid": 4242, "state": "running", "desired_running": True}) + "\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "control-center.toml"
    config_path.write_text(
        "\n".join(
            [
                "[[repo]]",
                'id = "alpha"',
                'name = "Alpha"',
                f'root = "{repo_root}"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(cclib, "_preflight_python_issue", lambda repo_root: None)
    monkeypatch.setattr(cclib, "_pid_is_alive", lambda pid: pid == 4242)

    report = cclib.run_startup_preflight(config_path)

    assert report.ok is False
    assert "live orchestrator pid=4242" in report.repo_reports[0].issues[0].message


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
    status_payload = json.loads(supervisor.status_path.read_text(encoding="utf-8"))
    assert status_payload["state"] == "running"
    assert status_payload["pid"] == 500

    first_process = popen_calls[0]["process"]
    first_process.returncode = 9
    supervisor.poll()

    assert supervisor.snapshot().state == "backoff"
    assert supervisor.last_exit_code == 9
    status_payload = json.loads(supervisor.status_path.read_text(encoding="utf-8"))
    assert status_payload["state"] == "backoff"
    assert status_payload["last_exit_code"] == 9

    monotonic_value["value"] = 2.3
    supervisor.poll()
    assert len(popen_calls) == 2
    assert supervisor.snapshot().state == "running"
    assert supervisor.restart_failures == 1

    assert supervisor.restart() == "orchestrator restarted"
    assert len(popen_calls) == 3
    assert supervisor.stop() == "orchestrator stopped"
    assert supervisor.snapshot().state == "stopped"
    status_payload = json.loads(supervisor.status_path.read_text(encoding="utf-8"))
    assert status_payload["state"] == "stopped"


def test_repo_supervisor_records_launch_failure_and_crash_loop(
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
    )
    monotonic_value = {"value": 1.0}
    now_value = {"value": 1_000}

    def fake_popen(*args, **kwargs):
        raise OSError("boom")

    monkeypatch.setattr(cclib.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(cclib, "_monotonic", lambda: monotonic_value["value"])
    monkeypatch.setattr(cclib, "_now_ms", lambda: now_value["value"])

    supervisor = cclib.RepoSupervisor(repo)

    message = supervisor.start()
    assert "launch failed" in message
    assert supervisor.last_error == "launch failed: OSError: boom"
    assert supervisor.snapshot().state == "backoff"

    for _ in range(4):
        monotonic_value["value"] += 10.0
        now_value["value"] += 1_000
        supervisor.poll()

    assert supervisor.snapshot().state == "crash_loop"
    status_payload = json.loads(supervisor.status_path.read_text(encoding="utf-8"))
    assert status_payload["state"] == "crash_loop"
    assert status_payload["last_error"] == "launch failed: OSError: boom"


def test_repo_supervisor_rotates_large_logs_before_launch(
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
    )

    class FakeProcess:
        def __init__(self, *args, **kwargs):
            self.returncode = None
            self.pid = 999

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

    supervisor = cclib.RepoSupervisor(repo)
    supervisor.orchestrator_dir.mkdir(parents=True, exist_ok=True)
    supervisor.stdout_path.write_bytes(b"x" * (cclib.ORCHESTRATOR_LOG_ROTATE_BYTES + 10))
    supervisor.stderr_path.write_bytes(b"y" * (cclib.ORCHESTRATOR_LOG_ROTATE_BYTES + 10))
    monkeypatch.setattr(cclib.subprocess, "Popen", FakeProcess)

    assert supervisor.start() == "orchestrator started"
    assert supervisor.stdout_path.with_name("orchestrator.stdout.log.1").exists()
    assert supervisor.stderr_path.with_name("orchestrator.stderr.log.1").exists()


def test_control_center_service_collects_rows_and_safe_actions(
    tmp_path: pathlib.Path,
    monkeypatch,
) -> None:
    repo_root = _make_repo_root(tmp_path, "alpha")
    runs_root = repo_root / "starter" / "runs"
    queued_run = _write_run(runs_root, "run-queued", state="queued", overall_pass=None)
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
    assert snapshot.repos[0].queued_count == 1
    assert snapshot.repos[0].runs[0].artifact_paths["manifest"].name == "run_manifest.json"
    assert any(run.run_id == "run-failed" for run in snapshot.repos[0].runs)
    assert snapshot.totals["queued"] == 1
    assert snapshot.totals["in_flight"] == 1
    assert "Top failure causes" in service.repo_health_text("alpha")

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


def test_control_center_service_archive_restore_round_trip(tmp_path: pathlib.Path) -> None:
    repo_root = _make_repo_root(tmp_path, "alpha")
    runs_root = repo_root / "starter" / "runs"
    run_dir = _write_run(runs_root, "run-archive", state="failed", overall_pass=False)
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
    service.refresh()

    assert service.archive_run("alpha", "run-archive") == "archive run-archive started"
    _poll_supervisor_until_idle(service.supervisors["alpha"])
    service.refresh()
    archive_path = service.archive_path("alpha", "run-archive")
    assert archive_path.exists()
    assert "archive run-archive" in service.repo_health_text("alpha")

    shutil.rmtree(run_dir)
    assert service.restore_evidence("alpha", "run-archive") == "restore run-archive started"
    _poll_supervisor_until_idle(service.supervisors["alpha"])
    assert run_dir.exists()
    assert (run_dir / "score.json").exists()


def test_runtime_check_failure_surfaces_in_repo_health(tmp_path: pathlib.Path) -> None:
    repo_root = _make_repo_root(tmp_path, "alpha")
    runs_root = repo_root / "starter" / "runs"
    _write_run(runs_root, "run-pass", state="complete", overall_pass=True)
    (repo_root / ".runtime-check-fail").write_text(
        "unsupported python runtime: expected 3.12.x, got 3.11.9\n",
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
    service.refresh()

    assert service.runtime_check("alpha") == "runtime check started"
    _poll_supervisor_until_idle(service.supervisors["alpha"])
    snapshot = service.refresh()

    assert snapshot.repos[0].runtime_check_ok is False
    assert snapshot.totals["repos_runtime_failing"] == 1
    assert "unsupported python runtime" in service.repo_health_text("alpha")


def test_harvester_emits_operator_summary_signals(tmp_path: pathlib.Path, monkeypatch) -> None:
    repo_root = _make_repo_root(tmp_path, "alpha")
    runs_root = repo_root / "starter" / "runs"
    old_run = _write_run(runs_root, "run-stale", state="partial", overall_pass=None)
    _write_run(runs_root, "run-pass", state="complete", overall_pass=True)
    _write_run(
        runs_root,
        "run-fail",
        state="failed",
        overall_pass=False,
        primary_error_code="eval_failed",
    )
    _write_canary_summary(
        runs_root,
        suffix="current",
        finished_at="2026-03-23T12:00:00Z",
        overall_ok=True,
    )
    stale_mtime = time.time() - (20 * 60)
    old_run.touch()
    os.utime(old_run, (stale_mtime, stale_mtime))
    monkeypatch.setattr(harvester, "now_ms", lambda: int(time.time() * 1000))

    payload = harvester.harvest(runs_root)

    assert payload["queue_wait_ms"]["p95"] == 120
    assert payload["score_wait_ms"]["p95"] == 40
    assert payload["activity"]["stale_non_terminal_count"] == 1
    assert payload["activity"]["oldest_non_terminal_age_ms"] >= 20 * 60 * 1000
    assert payload["top_failure_causes"][0]["code"] == "eval_failed"
    assert payload["canary_status"]["latest_summary_path"].endswith(".summary.json")
    assert payload["canary_status"]["all_passed"] is True


def test_control_center_main_check_mode_returns_exit_codes(
    tmp_path: pathlib.Path,
    capsys,
    monkeypatch,
) -> None:
    repo_root = _make_repo_root(tmp_path, "alpha")
    config_path = tmp_path / "control-center.toml"
    config_path.write_text(
        "\n".join(
            [
                "[[repo]]",
                'id = "alpha"',
                'name = "Alpha"',
                f'root = "{repo_root}"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(cclib, "_preflight_python_issue", lambda repo_root: None)
    monkeypatch.setattr(control_center, "TEXTUAL_IMPORT_ERROR", None)

    assert control_center.main(["--check", "--config", str(config_path)]) == 0
    assert "Status: ok" in capsys.readouterr().out

    (repo_root / "starter" / "bin" / "orchestrator.py").unlink()
    assert control_center.main(["--check", "--config", str(config_path)]) == 2
    assert "blockers found" in capsys.readouterr().out


def test_control_center_app_filters_and_command_palette(tmp_path: pathlib.Path) -> None:
    pytest.importorskip("textual")
    from control_center import ControlCenterApp
    from textual.widgets import DataTable

    repo_one = _make_repo_root(tmp_path, "alpha")
    repo_two = _make_repo_root(tmp_path, "beta")
    _write_run(repo_one / "starter" / "runs", "run-success", state="complete", overall_pass=True)
    _write_run(repo_one / "starter" / "runs", "run-pending", state="queued", overall_pass=None)
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
    service.runtime_check = lambda repo_id: f"runtime {repo_id}"  # type: ignore[method-assign]
    service.archive_run = lambda repo_id, run_id: f"archive {run_id}"  # type: ignore[method-assign]
    service.restore_evidence = (  # type: ignore[method-assign]
        lambda repo_id, run_id, archive_path="", force=False: (
            f"restore {run_id} {archive_path} {force}"
        )
    )

    async def runner() -> None:
        app = ControlCenterApp(service)
        async with app.run_test() as pilot:
            await pilot.pause()
            repo_table = app.query_one("#repo-table", DataTable)
            run_table = app.query_one("#run-table", DataTable)
            assert repo_table.row_count == 2
            assert run_table.row_count == 2
            assert "InFlight" in str(app.query_one("#summary-bar").renderable)

            await pilot.press("s")
            await pilot.pause()
            assert "repo sort: orchestrator" in str(app.query_one("#status-line").renderable)

            await pilot.press("r")
            await pilot.pause()
            assert "repo sort: orchestrator" in str(app.query_one("#status-line").renderable)

            await pilot.press("k")
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
            for key in "open health":
                await pilot.press("space" if key == " " else key)
            await pilot.press("enter")
            await pilot.pause()
            assert app.query_one("#detail-tabs").active == "tab-health"
            assert "Top failure causes" in str(app.query_one("#health-text").renderable)

            await pilot.press(":")
            for key in "open transcript":
                await pilot.press("space" if key == " " else key)
            await pilot.press("enter")
            await pilot.pause()
            assert app.query_one("#detail-tabs").active == "tab-transcript"

            await pilot.press("f")
            transcript_path = repo_two / "starter" / "runs" / "run-failure" / "transcript.jsonl"
            transcript_path.write_text(
                '{"type":"message"}\n{"type":"message","text":"tail"}\n',
                encoding="utf-8",
            )
            app.refresh_data()
            await pilot.pause()
            assert "tail" in str(app.query_one("#transcript-text").renderable)

            await pilot.press(":")
            for key in "open patch":
                await pilot.press("space" if key == " " else key)
            await pilot.press("enter")
            await pilot.pause()
            assert app.query_one("#detail-tabs").active == "tab-patch"

            (repo_two / "starter" / "runs" / "run-failure" / "patch.diff").unlink()
            app.refresh_data()
            await pilot.pause()
            assert app.query_one("#detail-tabs").active == "tab-overview"

            await pilot.press("tab")
            await pilot.pause()
            await pilot.press("tab")
            await pilot.pause()
            await pilot.press("s")
            await pilot.pause()
            assert "run sort: state" in str(app.query_one("#status-line").renderable)

            await pilot.press(":")
            for key in "repo start":
                await pilot.press("space" if key == " " else key)
            await pilot.press("enter")
            await pilot.pause()
            assert "started beta" in str(app.query_one("#status-line").renderable)

            await pilot.press(":")
            for key in "runtime-check":
                await pilot.press("space" if key == " " else key)
            await pilot.press("enter")
            await pilot.pause()
            assert "runtime beta" in str(app.query_one("#status-line").renderable)

            await pilot.press(":")
            for key in "archive-run":
                await pilot.press("space" if key == " " else key)
            await pilot.press("enter")
            await pilot.pause()
            assert "archive run-failure" in str(app.query_one("#status-line").renderable)

            await pilot.press(":")
            for key in "restore-evidence run-failure /tmp/archive.tgz --force":
                await pilot.press("space" if key == " " else key)
            await pilot.press("enter")
            await pilot.pause()
            assert "restore run-failure /tmp/archive.tgz True" in str(
                app.query_one("#status-line").renderable
            )

    asyncio.run(runner())
