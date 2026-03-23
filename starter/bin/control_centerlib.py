#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import pathlib
import subprocess
import time
import tomllib
from dataclasses import dataclass
from typing import Any

import harvester


def _now_ms() -> int:
    return int(time.time() * 1000)


def _monotonic() -> float:
    return time.monotonic()


def _to_positive_int(value: Any, *, default: int | None = None) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    if parsed < 0:
        return default
    return parsed


def _to_positive_float(value: Any, *, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return parsed if parsed >= 0 else default


def _append_jsonl(path: pathlib.Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _read_text(path: pathlib.Path, *, limit: int = 40_000, tail_lines: int | None = None) -> str:
    if not path.exists():
        return f"{path.name} is not available.\n"
    text = path.read_text(encoding="utf-8", errors="replace")
    if tail_lines is not None:
        text = "\n".join(text.splitlines()[-tail_lines:])
    if len(text) > limit:
        text = text[-limit:]
        text = f"... truncated to last {limit} bytes ...\n{text}"
    return text


def _is_terminal_state(state: str) -> bool:
    return state in {"complete", "failed", "cancelled"}


def _run_is_locked(run_dir: pathlib.Path) -> bool:
    lock_dir = run_dir / ".run-lock"
    if not lock_dir.is_dir():
        return False
    pid_file = lock_dir / "pid"
    if not pid_file.is_file():
        return False
    try:
        pid = int(pid_file.read_text(encoding="utf-8").strip())
    except Exception:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


@dataclass(frozen=True)
class UIConfig:
    refresh_interval_seconds: float = 1.0
    window_days: int = 30


@dataclass(frozen=True)
class RepoConfig:
    id: str
    name: str
    root: pathlib.Path
    runs_root: pathlib.Path
    auto_start: bool = True
    default_profile: str = ""
    max_model_workers: int | None = None
    max_score_workers: int | None = None
    orchestrator_poll_seconds: float | None = None


@dataclass(frozen=True)
class ControlCenterConfig:
    ui: UIConfig
    repos: tuple[RepoConfig, ...]
    source_path: pathlib.Path | None = None


@dataclass(frozen=True)
class ManagedCommandSnapshot:
    command_id: str
    label: str
    state: str
    exit_code: int | None
    stdout_path: pathlib.Path
    stderr_path: pathlib.Path
    started_epoch_ms: int
    completed_epoch_ms: int | None


@dataclass(frozen=True)
class OrchestratorSnapshot:
    state: str
    pid: int | None
    exit_code: int | None
    next_restart_seconds: float
    stdout_path: pathlib.Path
    stderr_path: pathlib.Path


@dataclass(frozen=True)
class RunRow:
    repo_id: str
    run_id: str
    run_dir: pathlib.Path
    state: str
    overall_pass: bool | None
    primary_error_code: str
    failure_classifications: tuple[str, ...]
    execution_profile: str
    duration_ms: int
    queue_wait_ms: int
    score_wait_ms: int
    worker_id: str
    run_queue_state: str
    score_queue_state: str
    artifact_paths: dict[str, pathlib.Path]
    updated_epoch_ms: int


@dataclass(frozen=True)
class RepoSnapshot:
    repo: RepoConfig
    orchestrator: OrchestratorSnapshot
    runs: tuple[RunRow, ...]
    summary: dict[str, Any]
    queue_depth: int
    in_flight_count: int
    active_commands: tuple[ManagedCommandSnapshot, ...]
    recent_messages: tuple[str, ...]


@dataclass(frozen=True)
class FleetSnapshot:
    repos: tuple[RepoSnapshot, ...]
    totals: dict[str, int]
    pass_rate_percent: float


@dataclass
class _ManagedCommand:
    command_id: str
    label: str
    process: subprocess.Popen[Any]
    stdout_path: pathlib.Path
    stderr_path: pathlib.Path
    started_epoch_ms: int
    completed_epoch_ms: int | None = None
    exit_code: int | None = None

    @property
    def state(self) -> str:
        if self.exit_code is None:
            return "running"
        return "complete" if self.exit_code == 0 else "failed"

    def snapshot(self) -> ManagedCommandSnapshot:
        return ManagedCommandSnapshot(
            command_id=self.command_id,
            label=self.label,
            state=self.state,
            exit_code=self.exit_code,
            stdout_path=self.stdout_path,
            stderr_path=self.stderr_path,
            started_epoch_ms=self.started_epoch_ms,
            completed_epoch_ms=self.completed_epoch_ms,
        )


def default_config_path() -> pathlib.Path:
    return pathlib.Path("~/.config/bitterless/control-center.toml").expanduser()


def _repo_config_from_payload(payload: dict[str, Any]) -> RepoConfig:
    repo_id = str(payload.get("id", "")).strip()
    name = str(payload.get("name", "")).strip()
    root_raw = str(payload.get("root", "")).strip()
    if not repo_id or not name or not root_raw:
        raise ValueError("each [[repo]] entry requires id, name, and root")

    root = pathlib.Path(root_raw).expanduser().resolve()
    runs_root_raw = str(payload.get("runs_root", "")).strip()
    runs_root = (
        pathlib.Path(runs_root_raw).expanduser().resolve()
        if runs_root_raw
        else (root / "starter" / "runs").resolve()
    )
    return RepoConfig(
        id=repo_id,
        name=name,
        root=root,
        runs_root=runs_root,
        auto_start=bool(payload.get("auto_start", True)),
        default_profile=str(payload.get("default_profile", "")).strip(),
        max_model_workers=_to_positive_int(payload.get("max_model_workers"), default=None),
        max_score_workers=_to_positive_int(payload.get("max_score_workers"), default=None),
        orchestrator_poll_seconds=(
            _to_positive_float(payload.get("orchestrator_poll_seconds"), default=1.0)
            if payload.get("orchestrator_poll_seconds") is not None
            else None
        ),
    )


def load_control_center_config(path: pathlib.Path | None = None) -> ControlCenterConfig:
    config_path = path.resolve() if path else default_config_path()
    if config_path.exists():
        payload = tomllib.loads(config_path.read_text(encoding="utf-8"))
        ui_payload = payload.get("ui", {}) if isinstance(payload.get("ui"), dict) else {}
        repos_payload = payload.get("repo", [])
        if not isinstance(repos_payload, list):
            raise ValueError("[[repo]] entries must be an array of tables")
        repos = tuple(_repo_config_from_payload(item) for item in repos_payload)
        if not repos:
            raise ValueError("control center config requires at least one [[repo]] entry")
        return ControlCenterConfig(
            ui=UIConfig(
                refresh_interval_seconds=_to_positive_float(
                    ui_payload.get("refresh_interval_seconds"), default=1.0
                ),
                window_days=_to_positive_int(ui_payload.get("window_days"), default=30) or 30,
            ),
            repos=repos,
            source_path=config_path,
        )

    if path is not None:
        raise FileNotFoundError(f"control center config not found: {config_path}")

    repo_root = pathlib.Path(__file__).resolve().parents[2]
    return ControlCenterConfig(
        ui=UIConfig(),
        repos=(
            RepoConfig(
                id="local",
                name=repo_root.name,
                root=repo_root,
                runs_root=(repo_root / "starter" / "runs").resolve(),
                auto_start=True,
            ),
        ),
        source_path=None,
    )


class RepoSupervisor:
    def __init__(self, repo: RepoConfig):
        self.repo = repo
        self.desired_running = repo.auto_start
        self.process: subprocess.Popen[Any] | None = None
        self._stdout_handle: Any | None = None
        self._stderr_handle: Any | None = None
        self.last_exit_code: int | None = None
        self.restart_failures = 0
        self.next_restart_at = 0.0
        self.active_commands: dict[str, _ManagedCommand] = {}
        self.completed_commands: list[ManagedCommandSnapshot] = []
        self.recent_messages: list[str] = []

    @property
    def orchestrator_dir(self) -> pathlib.Path:
        return self.repo.runs_root / ".orchestrator"

    @property
    def stdout_path(self) -> pathlib.Path:
        return self.orchestrator_dir / "orchestrator.stdout.log"

    @property
    def stderr_path(self) -> pathlib.Path:
        return self.orchestrator_dir / "orchestrator.stderr.log"

    @property
    def run_queue_path(self) -> pathlib.Path:
        return self.orchestrator_dir / "run_queue.jsonl"

    @property
    def score_queue_path(self) -> pathlib.Path:
        return self.orchestrator_dir / "score_queue.jsonl"

    def _record_message(self, message: str) -> None:
        self.recent_messages.append(message)
        self.recent_messages = self.recent_messages[-8:]

    def _build_orchestrator_command(self) -> list[str]:
        command = [
            "python3",
            str(self.repo.root / "starter" / "bin" / "orchestrator.py"),
            "--runs-root",
            str(self.repo.runs_root),
        ]
        if self.repo.max_model_workers is not None:
            command.extend(["--max-model-workers", str(self.repo.max_model_workers)])
        if self.repo.max_score_workers is not None:
            command.extend(["--max-score-workers", str(self.repo.max_score_workers)])
        return command

    def _build_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(self.repo.root / "starter" / "bin")
        if self.repo.orchestrator_poll_seconds is not None:
            env["HARNESS_ORCHESTRATOR_POLL_SECONDS"] = str(self.repo.orchestrator_poll_seconds)
        return env

    def _open_orchestrator_logs(self) -> tuple[Any, Any]:
        self.orchestrator_dir.mkdir(parents=True, exist_ok=True)
        stdout_handle = self.stdout_path.open("ab")
        stderr_handle = self.stderr_path.open("ab")
        return stdout_handle, stderr_handle

    def _launch_orchestrator(self) -> None:
        self.orchestrator_dir.mkdir(parents=True, exist_ok=True)
        stdout_handle, stderr_handle = self._open_orchestrator_logs()
        try:
            self.process = subprocess.Popen(
                self._build_orchestrator_command(),
                cwd=self.repo.root,
                stdout=stdout_handle,
                stderr=stderr_handle,
                env=self._build_env(),
            )
        except Exception:
            stdout_handle.close()
            stderr_handle.close()
            raise
        self._stdout_handle = stdout_handle
        self._stderr_handle = stderr_handle
        self.last_exit_code = None
        self._record_message(f"started orchestrator pid={self.process.pid}")

    def _close_orchestrator_logs(self) -> None:
        if self._stdout_handle is not None:
            self._stdout_handle.close()
            self._stdout_handle = None
        if self._stderr_handle is not None:
            self._stderr_handle.close()
            self._stderr_handle = None

    def _schedule_restart(self) -> None:
        self.restart_failures += 1
        backoff = min(8.0, 2 ** max(0, self.restart_failures - 1))
        self.next_restart_at = _monotonic() + backoff
        self._record_message(f"orchestrator exited, retrying in {backoff:.1f}s")

    def start(self) -> str:
        self.desired_running = True
        if self.process is not None and self.process.poll() is None:
            return "orchestrator already running"
        self.restart_failures = 0
        self.next_restart_at = 0.0
        self._launch_orchestrator()
        return "orchestrator started"

    def stop(self) -> str:
        self.desired_running = False
        self.next_restart_at = 0.0
        if self.process is None:
            return "orchestrator already stopped"
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.last_exit_code = self.process.returncode
        self.process = None
        self._close_orchestrator_logs()
        self._record_message("stopped orchestrator")
        return "orchestrator stopped"

    def restart(self) -> str:
        self.stop()
        self.desired_running = True
        self._launch_orchestrator()
        return "orchestrator restarted"

    def _poll_orchestrator(self) -> None:
        if self.process is not None:
            exit_code = self.process.poll()
            if exit_code is not None:
                self.last_exit_code = exit_code
                self.process = None
                self._close_orchestrator_logs()
                if self.desired_running:
                    self._schedule_restart()
                else:
                    self._record_message(f"orchestrator exited code={exit_code}")
        if self.desired_running and self.process is None and _monotonic() >= self.next_restart_at:
            self._launch_orchestrator()

    def _command_log_paths(self, label: str) -> tuple[pathlib.Path, pathlib.Path]:
        stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
        root = self.orchestrator_dir
        root.mkdir(parents=True, exist_ok=True)
        return (
            root / f"{stamp}-{label}.stdout.log",
            root / f"{stamp}-{label}.stderr.log",
        )

    def _launch_managed_command(
        self,
        command_id: str,
        label: str,
        command: list[str],
        *,
        cwd: pathlib.Path,
        env: dict[str, str] | None = None,
    ) -> str:
        active = self.active_commands.get(command_id)
        if active is not None and active.exit_code is None:
            return f"{label} is already running"
        stdout_path, stderr_path = self._command_log_paths(label.replace(" ", "-"))
        stdout_handle = stdout_path.open("ab")
        stderr_handle = stderr_path.open("ab")
        try:
            process = subprocess.Popen(
                command,
                cwd=cwd,
                env=(os.environ.copy() | (env or {})),
                stdout=stdout_handle,
                stderr=stderr_handle,
            )
        finally:
            stdout_handle.close()
            stderr_handle.close()
        self.active_commands[command_id] = _ManagedCommand(
            command_id=command_id,
            label=label,
            process=process,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            started_epoch_ms=_now_ms(),
        )
        self._record_message(f"started {label}")
        return f"{label} started"

    def launch_canary(self) -> str:
        return self._launch_managed_command(
            "canary",
            "real canary",
            ["python3", str(self.repo.root / "starter" / "bin" / "run_real_canary.py")],
            cwd=self.repo.root,
            env={"PYTHONPATH": str(self.repo.root / "starter" / "bin")},
        )

    def launch_rerun(self, run_dir: pathlib.Path) -> str:
        command_id = f"rerun:{run_dir.name}"
        return self._launch_managed_command(
            command_id,
            f"rerun {run_dir.name}",
            [str(self.repo.root / "starter" / "bin" / "run-task.sh"), str(run_dir)],
            cwd=self.repo.root / "starter",
            env={
                "PYTHONPATH": str(self.repo.root / "starter" / "bin"),
                "HARNESS_FORCE_RERUN": "1",
            },
        )

    def _poll_commands(self) -> None:
        finished: list[str] = []
        for command_id, managed in self.active_commands.items():
            exit_code = managed.process.poll()
            if exit_code is None:
                continue
            managed.exit_code = exit_code
            managed.completed_epoch_ms = _now_ms()
            snapshot = managed.snapshot()
            self.completed_commands = ([snapshot] + self.completed_commands)[:10]
            self._record_message(f"{managed.label} completed with exit code {exit_code}")
            finished.append(command_id)
        for command_id in finished:
            del self.active_commands[command_id]

    def poll(self) -> None:
        self._poll_orchestrator()
        self._poll_commands()

    def snapshot(self) -> OrchestratorSnapshot:
        state = "stopped"
        pid = None
        next_restart_seconds = 0.0
        if self.process is not None and self.process.poll() is None:
            state = "running"
            pid = self.process.pid
        elif self.desired_running and self.next_restart_at > _monotonic():
            state = "backoff"
            next_restart_seconds = max(0.0, self.next_restart_at - _monotonic())
        elif self.desired_running:
            state = "starting"
        return OrchestratorSnapshot(
            state=state,
            pid=pid,
            exit_code=self.last_exit_code,
            next_restart_seconds=next_restart_seconds,
            stdout_path=self.stdout_path,
            stderr_path=self.stderr_path,
        )

    def shutdown(self) -> None:
        self.stop()
        for managed in self.active_commands.values():
            if managed.process.poll() is None:
                managed.process.terminate()
        self.active_commands.clear()


class ControlCenterService:
    def __init__(self, config: ControlCenterConfig):
        self.config = config
        self.supervisors = {repo.id: RepoSupervisor(repo) for repo in config.repos}
        self._last_snapshot = FleetSnapshot(repos=(), totals={}, pass_rate_percent=0.0)

    def close(self) -> None:
        for supervisor in self.supervisors.values():
            supervisor.shutdown()

    def _queue_depth(self, runs: tuple[RunRow, ...]) -> int:
        return sum(
            1
            for run in runs
            if run.run_queue_state in {"queued", "claimed", "model_running"}
            or run.score_queue_state in {"queued", "claimed", "scoring"}
            or run.state in {"queued", "claimed", "model_running", "scoring", "score_pending"}
        )

    def _in_flight_count(self, runs: tuple[RunRow, ...]) -> int:
        return sum(
            1
            for run in runs
            if run.state in {"queued", "claimed", "model_running", "scoring", "score_pending"}
        )

    def refresh(self) -> FleetSnapshot:
        snapshots: list[RepoSnapshot] = []
        total_runs = 0
        total_pass = 0
        total_complete = 0

        for repo in self.config.repos:
            supervisor = self.supervisors[repo.id]
            supervisor.poll()
            harvested = harvester.harvest_repo(
                repo.runs_root,
                window_days=self.config.ui.window_days,
            )
            runs = tuple(
                RunRow(
                    repo_id=repo.id,
                    run_id=row["run_id"],
                    run_dir=pathlib.Path(row["run_dir"]),
                    state=str(row["state"]),
                    overall_pass=row["overall_pass"],
                    primary_error_code=str(row["primary_error_code"]),
                    failure_classifications=tuple(row["failure_classifications"]),
                    execution_profile=str(row["execution_profile"]),
                    duration_ms=int(row["duration_ms"]),
                    queue_wait_ms=int(row["queue_wait_ms"]),
                    score_wait_ms=int(row["score_wait_ms"]),
                    worker_id=str(row["worker_id"]),
                    run_queue_state=str(row["run_queue_state"]),
                    score_queue_state=str(row["score_queue_state"]),
                    artifact_paths={
                        key: pathlib.Path(value) for key, value in row["artifact_paths"].items()
                    },
                    updated_epoch_ms=int(row["updated_epoch_ms"]),
                )
                for row in harvested["runs"]
            )
            summary = harvested["summary"]
            totals = summary.get("totals", {})
            total_runs += int(totals.get("total_runs", 0))
            total_pass += int(totals.get("complete_pass", 0))
            total_complete += int(totals.get("complete", 0))
            snapshots.append(
                RepoSnapshot(
                    repo=repo,
                    orchestrator=supervisor.snapshot(),
                    runs=runs,
                    summary=summary,
                    queue_depth=self._queue_depth(runs),
                    in_flight_count=self._in_flight_count(runs),
                    active_commands=tuple(
                        [command.snapshot() for command in supervisor.active_commands.values()]
                        + supervisor.completed_commands[:5]
                    ),
                    recent_messages=tuple(supervisor.recent_messages),
                )
            )

        pass_rate_percent = (
            round((total_pass / total_complete) * 100.0, 2) if total_complete else 0.0
        )
        self._last_snapshot = FleetSnapshot(
            repos=tuple(snapshots),
            totals={
                "total_runs": total_runs,
                "complete_pass": total_pass,
                "complete": total_complete,
            },
            pass_rate_percent=pass_rate_percent,
        )
        return self._last_snapshot

    @property
    def last_snapshot(self) -> FleetSnapshot:
        return self._last_snapshot

    def _repo_snapshot(self, repo_id: str) -> RepoSnapshot:
        for repo in self._last_snapshot.repos:
            if repo.repo.id == repo_id:
                return repo
        return next(repo for repo in self.refresh().repos if repo.repo.id == repo_id)

    def _run_row(self, repo_id: str, run_id: str) -> RunRow:
        repo = self._repo_snapshot(repo_id)
        for row in repo.runs:
            if row.run_id == run_id:
                return row
        raise KeyError(f"run not found: {repo_id}/{run_id}")

    def start_repo(self, repo_id: str) -> str:
        return self.supervisors[repo_id].start()

    def stop_repo(self, repo_id: str) -> str:
        return self.supervisors[repo_id].stop()

    def restart_repo(self, repo_id: str) -> str:
        return self.supervisors[repo_id].restart()

    def run_canary(self, repo_id: str) -> str:
        return self.supervisors[repo_id].launch_canary()

    def cancel_run(self, repo_id: str, run_id: str) -> str:
        row = self._run_row(repo_id, run_id)
        if _is_terminal_state(row.state):
            return f"{run_id} is already terminal"
        cancel_path = row.run_dir / ".orchestrator-cancel"
        cancel_path.write_text("cancelled\n", encoding="utf-8")
        return f"cancellation requested for {run_id}"

    def enqueue_run(self, repo_id: str, run_id: str) -> str:
        row = self._run_row(repo_id, run_id)
        if _is_terminal_state(row.state):
            return f"{run_id} is terminal and cannot be enqueued"
        if _run_is_locked(row.run_dir):
            return f"{run_id} is locked and cannot be enqueued"
        if row.run_queue_state in {"queued", "claimed", "model_running"}:
            return f"{run_id} is already queued or running"
        supervisor = self.supervisors[repo_id]
        queue_entries = harvester.read_queue_entries(supervisor.run_queue_path)
        prior_attempt = (
            _to_positive_int(queue_entries.get(run_id, {}).get("attempt"), default=1) or 1
        )
        _append_jsonl(
            supervisor.run_queue_path,
            {
                "type": "run",
                "kind": "run",
                "run_id": run_id,
                "run_dir": str(row.run_dir.resolve()),
                "attempt": prior_attempt,
                "state": "queued",
                "worker_id": "control-center",
                "ts_ms": _now_ms(),
                "queued_at_ms": _now_ms(),
                "orchestration_state": "queued",
            },
        )
        (row.run_dir / "run.state").write_text("queued\n", encoding="utf-8")
        return f"enqueued {run_id}"

    def rerun_run(self, repo_id: str, run_id: str) -> str:
        row = self._run_row(repo_id, run_id)
        if not _is_terminal_state(row.state):
            return f"{run_id} is not terminal and cannot be rerun"
        cancel_path = row.run_dir / ".orchestrator-cancel"
        if cancel_path.exists():
            cancel_path.unlink()
        return self.supervisors[repo_id].launch_rerun(row.run_dir)

    def read_artifact(self, repo_id: str, run_id: str, kind: str) -> str:
        row = self._run_row(repo_id, run_id)
        if kind == "events":
            return _read_text(row.artifact_paths["events"], tail_lines=200)
        if kind == "transcript":
            return _read_text(row.artifact_paths["transcript"], tail_lines=200)
        if kind == "score":
            return _read_text(row.artifact_paths["score"])
        if kind == "patch":
            return _read_text(row.artifact_paths["patch"])
        return _read_text(row.artifact_paths["manifest"])

    def overview_text(self, repo_id: str, run_id: str, *, preview: str = "manifest") -> str:
        row = self._run_row(repo_id, run_id)
        lines = [
            f"Run: {row.run_id}",
            f"State: {row.state}",
            f"Pass: {row.overall_pass}",
            f"Profile: {row.execution_profile or '-'}",
            f"Primary error: {row.primary_error_code or '-'}",
            f"Failures: {', '.join(row.failure_classifications) or '-'}",
            f"Duration: {row.duration_ms} ms",
            f"Queue wait: {row.queue_wait_ms} ms",
            f"Score wait: {row.score_wait_ms} ms",
            f"Worker: {row.worker_id or '-'}",
            "",
        ]
        if preview == "patch":
            lines.append("Patch preview")
            lines.append("=" * 12)
            lines.append(self.read_artifact(repo_id, run_id, "patch"))
        else:
            lines.append("Manifest preview")
            lines.append("=" * 15)
            lines.append(self.read_artifact(repo_id, run_id, "manifest"))
        return "\n".join(lines)


def render_duration_ms(value: int) -> str:
    if value <= 0:
        return "-"
    if value < 1000:
        return f"{value}ms"
    return f"{value / 1000:.1f}s"


def build_example_config_text(repo_root: pathlib.Path) -> str:
    sibling = repo_root.parent / "another-bitterless-harness"
    return "\n".join(
        [
            "[ui]",
            "refresh_interval_seconds = 1.0",
            "window_days = 30",
            "",
            "[[repo]]",
            'id = "main"',
            f'name = "{repo_root.name}"',
            f'root = "{repo_root}"',
            "auto_start = true",
            "max_model_workers = 2",
            "max_score_workers = 2",
            "",
            "[[repo]]",
            'id = "secondary"',
            'name = "Another Harness"',
            f'root = "{sibling}"',
            "auto_start = false",
        ]
    ) + "\n"
