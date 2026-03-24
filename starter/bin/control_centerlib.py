#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import pathlib
import re
import shlex
import subprocess
import sys
import tarfile
import time
import tomllib
from dataclasses import dataclass
from typing import Any

import harvester

REQUIRED_REPO_SCRIPTS = (
    "starter/bin/orchestrator.py",
    "starter/bin/run-task.sh",
    "starter/bin/run_real_canary.py",
)
ORCHESTRATOR_LOG_ROTATE_BYTES = 5 * 1024 * 1024
ORCHESTRATOR_LOG_ROTATE_KEEP = 3
SUPERVISOR_FAILURE_WINDOW_SECONDS = 10 * 60
SUPERVISOR_CRASH_LOOP_FAILURES = 5
CANARY_STALE_HOURS = 24.0
REPO_SORT_KEYS = ("name", "orchestrator", "queue", "in_flight", "pass", "p95")
RUN_SORT_KEYS = ("updated", "state", "pass", "duration", "queue_wait", "score_wait", "profile")


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


def _last_nonempty_line(path: pathlib.Path) -> str:
    if not path.exists():
        return ""
    try:
        for line in reversed(path.read_text(encoding="utf-8", errors="replace").splitlines()):
            stripped = line.strip()
            if stripped:
                return stripped
    except OSError:
        return ""
    return ""


def _is_terminal_state(state: str) -> bool:
    return state in {"complete", "failed", "cancelled"}


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


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
    return _pid_is_alive(pid)


def _format_exception(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def _archive_root_name(archive_path: pathlib.Path) -> str:
    if not archive_path.exists():
        return ""
    try:
        with tarfile.open(archive_path, "r:gz") as handle:
            for member in handle.getmembers():
                name = member.name.strip().strip("/")
                if not name:
                    continue
                root = pathlib.PurePosixPath(name).parts[0]
                if root.startswith("._") or root == "__MACOSX":
                    continue
                return root
    except (tarfile.TarError, OSError):
        return ""
    return ""


def _repo_runtime_version_file(repo_root: pathlib.Path) -> pathlib.Path:
    return repo_root / ".python-version"


def _supported_python_minor(repo_root: pathlib.Path) -> str:
    version_file = _repo_runtime_version_file(repo_root)
    if not version_file.exists():
        return ""
    expected = version_file.read_text(encoding="utf-8").strip()
    return ".".join(expected.split(".")[:2])


def _severity_rank(severity: str) -> int:
    return {
        "critical": 0,
        "warning": 1,
        "info": 2,
        "success": 3,
    }.get(severity, 4)


def _render_relative_age(epoch_ms: int | None) -> str:
    if not epoch_ms:
        return "-"
    delta_seconds = max(0, int((_now_ms() - epoch_ms) / 1000))
    if delta_seconds < 60:
        return f"{delta_seconds}s ago"
    if delta_seconds < 3600:
        return f"{delta_seconds // 60}m ago"
    if delta_seconds < 86400:
        return f"{delta_seconds // 3600}h ago"
    return f"{delta_seconds // 86400}d ago"


def _serialize_ui_action(action: UIAction) -> dict[str, Any]:
    return {
        "id": action.id,
        "label": action.label,
        "kind": action.kind,
        "scope": action.scope,
        "command_text": action.command_text,
        "requires_confirmation": action.requires_confirmation,
        "enabled": action.enabled,
        "disabled_reason": action.disabled_reason,
        "open_tab": action.open_tab,
        "aliases": list(action.aliases),
    }


def _deserialize_ui_action(payload: dict[str, Any]) -> UIAction:
    return UIAction(
        id=str(payload.get("id", "")),
        label=str(payload.get("label", "")),
        kind=str(payload.get("kind", "")),
        scope=str(payload.get("scope", "")),
        command_text=str(payload.get("command_text", "")),
        requires_confirmation=bool(payload.get("requires_confirmation", False)),
        enabled=bool(payload.get("enabled", True)),
        disabled_reason=str(payload.get("disabled_reason", "")),
        open_tab=str(payload.get("open_tab", "")),
        aliases=tuple(str(item) for item in payload.get("aliases", []) if str(item).strip()),
    )


def _read_jsonl_events(path: pathlib.Path) -> list[dict[str, Any]]:
    try:
        return [
            payload
            for payload in harvester._read_jsonl(path)  # type: ignore[attr-defined]
            if isinstance(payload, dict)
        ]
    except Exception:
        return []


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
    default_model: str = ""
    max_model_workers: int | None = None
    max_score_workers: int | None = None
    orchestrator_poll_seconds: float | None = None


@dataclass(frozen=True)
class ControlCenterConfig:
    ui: UIConfig
    repos: tuple[RepoConfig, ...]
    source_path: pathlib.Path | None = None


@dataclass(frozen=True)
class SortState:
    key: str
    reverse: bool = False


@dataclass(frozen=True)
class RunFilterState:
    failed_only: bool = False
    queued_only: bool = False
    capability_only: bool = False
    last_24h_only: bool = False
    text: str = ""


@dataclass(frozen=True)
class AlertBadge:
    severity: str
    label: str
    detail: str = ""


@dataclass(frozen=True)
class TimelineStep:
    key: str
    label: str
    status: str


@dataclass(frozen=True)
class UIAction:
    id: str
    label: str
    kind: str
    scope: str
    command_text: str
    requires_confirmation: bool = False
    enabled: bool = True
    disabled_reason: str = ""
    open_tab: str = ""
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class TargetSummary:
    repo_id: str
    repo_name: str
    run_id: str
    run_state: str
    pass_label: str
    profile: str
    age_text: str
    alerts: tuple[AlertBadge, ...]
    recommended_actions: tuple[UIAction, ...]
    recommended_tab: str


@dataclass(frozen=True)
class RepoViewState:
    active_tab: str = "tab-chat"
    overview_preview: str = "manifest"
    follow_streams: tuple[str, ...] = ()


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
    restart_failures: int
    last_error: str
    last_started_epoch_ms: int | None
    last_exited_epoch_ms: int | None
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
    queued_count: int
    in_flight_count: int
    stale_run_count: int
    runtime_check_ok: bool | None
    runtime_check_message: str
    runtime_check_completed_epoch_ms: int | None
    canary_failing: bool
    canary_stale: bool
    last_action_result: str
    active_commands: tuple[ManagedCommandSnapshot, ...]
    recent_messages: tuple[str, ...]


@dataclass(frozen=True)
class FleetSnapshot:
    repos: tuple[RepoSnapshot, ...]
    totals: dict[str, int]
    pass_rate_percent: float


@dataclass(frozen=True)
class PreflightIssue:
    severity: str
    message: str


@dataclass(frozen=True)
class RepoPreflightReport:
    repo: RepoConfig
    issues: tuple[PreflightIssue, ...]

    @property
    def ok(self) -> bool:
        return not any(issue.severity == "blocker" for issue in self.issues)


@dataclass(frozen=True)
class StartupPreflightReport:
    config: ControlCenterConfig | None
    config_path: pathlib.Path | None
    app_issues: tuple[PreflightIssue, ...]
    repo_reports: tuple[RepoPreflightReport, ...]

    @property
    def ok(self) -> bool:
        return not self.app_issues and all(report.ok for report in self.repo_reports)


@dataclass(frozen=True)
class ChatMessage:
    ts_ms: int
    repo_id: str
    role: str
    message_type: str
    content: str
    run_id: str = ""
    action_name: str = ""
    resulting_run_id: str = ""
    follow_up_actions: tuple[UIAction, ...] = ()


@dataclass(frozen=True)
class ChatPendingAction:
    action_type: str
    label: str
    prompt: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class ChatSubmissionResult:
    reply: str
    focus_run_id: str = ""
    open_tab: str = "tab-chat"
    follow_up_actions: tuple[UIAction, ...] = ()


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


def _supervisor_status_path(runs_root: pathlib.Path) -> pathlib.Path:
    return runs_root / ".orchestrator" / "supervisor-status.json"


def _read_supervisor_status(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_json_dict(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _slugify_title(value: str) -> str:
    title = re.sub(r"\s+", " ", value.strip())
    return title[:120].strip() or "operator-request"


def _derive_run_title(prompt: str) -> str:
    cleaned = re.sub(r"\s+", " ", prompt.strip())
    cleaned = re.sub(r"^(create|start|run|launch|ask)\s+(a\s+)?(new\s+)?(run|task)\s+(to\s+)?", "", cleaned, flags=re.IGNORECASE)
    words = cleaned.split()
    if not words:
        return "operator-request"
    return _slugify_title(" ".join(words[:10]))


def _task_schema_section(task_text: str) -> str:
    match = re.search(
        r"^## Result JSON schema \(source of truth\)\n(?:.*\n)*?(?=^## |\Z)",
        task_text,
        flags=re.MULTILINE,
    )
    return match.group(0).rstrip() if match else ""


def _build_chat_task_text(existing_task: str, *, title: str, prompt: str) -> str:
    schema_section = _task_schema_section(existing_task)
    body = "\n".join(
        [
            "# Task",
            title,
            "",
            "## Goal",
            prompt,
            "",
            "## Constraints",
            "- Stay within the repository and harness guardrails.",
            "- Save durable outputs under `outputs/`.",
            "- Keep `result.json` schema-compliant.",
            "",
            "## Done",
            "- The operator request is completed.",
            "- Durable artifacts are written under `outputs/` when needed.",
            "- `result.json` describes the outcome and remaining risks.",
            "",
            "## Eval",
            "```bash",
            "# Add or refine verification commands before launch if needed.",
            "```",
            "",
            "## Required Artifacts",
            "- result.json",
            "",
            "## Notes",
            "- Requested through the command-center chat panel.",
            f"- Original operator request: {prompt}",
        ]
    )
    if schema_section:
        body = f"{body}\n\n{schema_section}"
    return body.rstrip() + "\n"


def _preflight_python_issue(repo_root: pathlib.Path) -> PreflightIssue | None:
    expected_minor = _supported_python_minor(repo_root)
    if not expected_minor:
        return None
    current_minor = f"{sys.version_info.major}.{sys.version_info.minor}"
    if current_minor == expected_minor:
        return None
    return PreflightIssue(
        severity="blocker",
        message=(
            f"python {expected_minor}.x is required, but command center is running on "
            f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        ),
    )


def _repo_preflight(repo: RepoConfig) -> RepoPreflightReport:
    issues: list[PreflightIssue] = []
    if not repo.root.exists():
        issues.append(
            PreflightIssue(severity="blocker", message=f"repo root does not exist: {repo.root}")
        )
        return RepoPreflightReport(repo=repo, issues=tuple(issues))

    for rel_path in REQUIRED_REPO_SCRIPTS:
        required = repo.root / rel_path
        if not required.exists():
            issues.append(
                PreflightIssue(
                    severity="blocker",
                    message=f"required script is missing: {required}",
                )
            )

    if not repo.runs_root.exists():
        parent = repo.runs_root.parent
        if not parent.exists() or not os.access(parent, os.W_OK):
            issues.append(
                PreflightIssue(
                    severity="blocker",
                    message=(
                        f"runs_root does not exist and parent is not writable: {repo.runs_root}"
                    ),
                )
            )

    status_path = _supervisor_status_path(repo.runs_root)
    status = _read_supervisor_status(status_path)
    pid = _to_positive_int(status.get("pid"), default=None)
    if pid is not None and _pid_is_alive(pid):
        issues.append(
            PreflightIssue(
                severity="blocker",
                message=(
                    f"supervisor status reports a live orchestrator pid={pid} at {status_path}; "
                    "stop that process before launching the command center"
                ),
            )
        )

    return RepoPreflightReport(repo=repo, issues=tuple(issues))


def run_startup_preflight(
    path: pathlib.Path | None = None,
    *,
    textual_import_error: BaseException | None = None,
) -> StartupPreflightReport:
    app_issues: list[PreflightIssue] = []
    repo_reports: list[RepoPreflightReport] = []
    config: ControlCenterConfig | None = None
    config_path = path.resolve() if path else default_config_path()

    try:
        config = load_control_center_config(path)
    except Exception as exc:
        app_issues.append(
            PreflightIssue(
                severity="blocker",
                message=f"failed to load config: {_format_exception(exc)}",
            )
        )
    else:
        runtime_issue = _preflight_python_issue(pathlib.Path(__file__).resolve().parents[2])
        if runtime_issue is not None:
            app_issues.append(runtime_issue)
        if textual_import_error is not None:
            app_issues.append(
                PreflightIssue(
                    severity="blocker",
                    message=(
                        "textual is required for the command center: "
                        f"{_format_exception(textual_import_error)}"
                    ),
                )
            )
        repo_reports = [_repo_preflight(repo) for repo in config.repos]
        config_path = config.source_path or config_path

    return StartupPreflightReport(
        config=config,
        config_path=config_path,
        app_issues=tuple(app_issues),
        repo_reports=tuple(repo_reports),
    )


def render_startup_preflight(report: StartupPreflightReport) -> str:
    lines = [
        "Bitterless command center preflight",
        f"Config: {report.config_path or '(default local config)'}",
        f"Status: {'ok' if report.ok else 'blockers found'}",
    ]
    if report.app_issues:
        lines.append("")
        lines.append("Application")
        for issue in report.app_issues:
            lines.append(f"- {issue.severity}: {issue.message}")
    for repo_report in report.repo_reports:
        lines.append("")
        lines.append(f"Repo {repo_report.repo.id} ({repo_report.repo.name})")
        if repo_report.ok:
            lines.append("- ok")
            continue
        for issue in repo_report.issues:
            lines.append(f"- {issue.severity}: {issue.message}")
    return "\n".join(lines) + "\n"


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
        default_model=str(payload.get("default_model", "")).strip(),
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
                default_model="",
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
        self.last_error = ""
        self.last_started_epoch_ms: int | None = None
        self.last_exited_epoch_ms: int | None = None
        self.failure_history_ms: list[int] = []
        self.crash_loop = False
        self.active_commands: dict[str, _ManagedCommand] = {}
        self.completed_commands: list[ManagedCommandSnapshot] = []
        self.recent_messages: list[str] = []
        self.last_action_result = ""
        self.last_runtime_check_ok: bool | None = None
        self.last_runtime_check_message = ""
        self.last_runtime_check_completed_epoch_ms: int | None = None
        self.archive_paths: dict[str, pathlib.Path] = {}
        self._persist_status("starting" if self.desired_running else "stopped")

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

    @property
    def status_path(self) -> pathlib.Path:
        return _supervisor_status_path(self.repo.runs_root)

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

    def _status_payload(self, state: str) -> dict[str, Any]:
        pid = None
        if self.process is not None and self.process.poll() is None:
            pid = self.process.pid
        return {
            "repo_id": self.repo.id,
            "desired_running": self.desired_running,
            "state": state,
            "pid": pid,
            "last_exit_code": self.last_exit_code,
            "restart_failures": self.restart_failures,
            "next_restart_epoch_ms": (
                _now_ms() + int(max(0.0, self.next_restart_at - _monotonic()) * 1000.0)
                if self.next_restart_at > _monotonic()
                else None
            ),
            "last_started_epoch_ms": self.last_started_epoch_ms,
            "last_exited_epoch_ms": self.last_exited_epoch_ms,
            "last_error": self.last_error,
            "recent_messages": list(self.recent_messages),
        }

    def _persist_status(self, state: str) -> None:
        self.status_path.parent.mkdir(parents=True, exist_ok=True)
        self.status_path.write_text(
            json.dumps(self._status_payload(state), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def _clear_failure_latch(self) -> None:
        self.restart_failures = 0
        self.next_restart_at = 0.0
        self.failure_history_ms = []
        self.crash_loop = False
        self.last_error = ""

    def _record_failure(self, message: str) -> None:
        now_ms = _now_ms()
        cutoff_ms = now_ms - (SUPERVISOR_FAILURE_WINDOW_SECONDS * 1000)
        self.failure_history_ms = [
            failure_ms for failure_ms in self.failure_history_ms if failure_ms >= cutoff_ms
        ]
        self.failure_history_ms.append(now_ms)
        self.restart_failures += 1
        self.last_error = message
        if len(self.failure_history_ms) >= SUPERVISOR_CRASH_LOOP_FAILURES:
            self.crash_loop = True
            self.next_restart_at = 0.0
            self._record_message("orchestrator entered crash loop; manual restart required")
            self._persist_status("crash_loop")
            return
        backoff = min(8.0, 2 ** max(0, self.restart_failures - 1))
        self.next_restart_at = _monotonic() + backoff
        self._record_message(f"{message}; retrying in {backoff:.1f}s")
        self._persist_status("backoff")

    def _rotate_log_if_needed(self, path: pathlib.Path) -> None:
        if not path.exists():
            return
        try:
            if path.stat().st_size <= ORCHESTRATOR_LOG_ROTATE_BYTES:
                return
        except OSError:
            return
        oldest = path.with_name(f"{path.name}.{ORCHESTRATOR_LOG_ROTATE_KEEP}")
        if oldest.exists():
            oldest.unlink()
        for index in range(ORCHESTRATOR_LOG_ROTATE_KEEP - 1, 0, -1):
            current = path.with_name(f"{path.name}.{index}")
            if current.exists():
                current.replace(path.with_name(f"{path.name}.{index + 1}"))
        path.replace(path.with_name(f"{path.name}.1"))

    def _open_orchestrator_logs(self) -> tuple[Any, Any]:
        self.orchestrator_dir.mkdir(parents=True, exist_ok=True)
        self._rotate_log_if_needed(self.stdout_path)
        self._rotate_log_if_needed(self.stderr_path)
        stdout_handle = self.stdout_path.open("ab")
        stderr_handle = self.stderr_path.open("ab")
        return stdout_handle, stderr_handle

    def _launch_orchestrator(self) -> bool:
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
        except Exception as exc:
            stdout_handle.close()
            stderr_handle.close()
            self.process = None
            self._persist_status("launch_failed")
            self._record_failure(f"launch failed: {_format_exception(exc)}")
            return False
        self._stdout_handle = stdout_handle
        self._stderr_handle = stderr_handle
        self.last_exit_code = None
        self.last_started_epoch_ms = _now_ms()
        self.last_error = ""
        self._record_message(f"started orchestrator pid={self.process.pid}")
        self._persist_status("running")
        return True

    def _close_orchestrator_logs(self) -> None:
        if self._stdout_handle is not None:
            self._stdout_handle.close()
            self._stdout_handle = None
        if self._stderr_handle is not None:
            self._stderr_handle.close()
            self._stderr_handle = None

    def start(self) -> str:
        self.desired_running = True
        if self.process is not None and self.process.poll() is None:
            return "orchestrator already running"
        self._clear_failure_latch()
        if self._launch_orchestrator():
            return "orchestrator started"
        return (
            "orchestrator entered crash loop"
            if self.crash_loop
            else f"orchestrator launch failed: {self.last_error}"
        )

    def stop(self) -> str:
        self.desired_running = False
        self.next_restart_at = 0.0
        if self.process is None:
            self._persist_status("stopped")
            return "orchestrator already stopped"
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.last_exit_code = self.process.returncode
        self.last_exited_epoch_ms = _now_ms()
        self.process = None
        self._close_orchestrator_logs()
        self._record_message("stopped orchestrator")
        self._persist_status("stopped")
        return "orchestrator stopped"

    def restart(self) -> str:
        self.stop()
        self.desired_running = True
        self._clear_failure_latch()
        if self._launch_orchestrator():
            return "orchestrator restarted"
        return (
            "orchestrator entered crash loop"
            if self.crash_loop
            else f"orchestrator launch failed: {self.last_error}"
        )

    def _poll_orchestrator(self) -> None:
        if self.process is not None:
            exit_code = self.process.poll()
            if exit_code is not None:
                self.last_exit_code = exit_code
                self.last_exited_epoch_ms = _now_ms()
                self.process = None
                self._close_orchestrator_logs()
                if self.desired_running:
                    self._record_failure(f"orchestrator exited code={exit_code}")
                else:
                    self._record_message(f"orchestrator exited code={exit_code}")
                    self._persist_status("stopped")
        if (
            self.desired_running
            and self.process is None
            and not self.crash_loop
            and _monotonic() >= self.next_restart_at
        ):
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
        except Exception as exc:
            message = f"{label} failed to start: {_format_exception(exc)}"
            self.last_action_result = message
            self._record_message(message)
            return message
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

    def launch_run(self, run_dir: pathlib.Path, *, model: str = "") -> str:
        command_id = f"run:{run_dir.name}"
        command = [str(self.repo.root / "starter" / "bin" / "run-task.sh"), str(run_dir)]
        if model:
            command.append(model)
        return self._launch_managed_command(
            command_id,
            f"run {run_dir.name}",
            command,
            cwd=self.repo.root / "starter",
            env={"PYTHONPATH": str(self.repo.root / "starter" / "bin")},
        )

    def launch_archive(self, run_dir: pathlib.Path, archive_path: pathlib.Path) -> str:
        command_id = f"archive:{run_dir.name}"
        return self._launch_managed_command(
            command_id,
            f"archive {run_dir.name}",
            [
                str(self.repo.root / "starter" / "bin" / "archive-run-evidence.sh"),
                str(run_dir),
                str(archive_path),
            ],
            cwd=self.repo.root,
            env={"PYTHONPATH": str(self.repo.root / "starter" / "bin")},
        )

    def launch_restore(
        self,
        archive_path: pathlib.Path,
        destination_root: pathlib.Path,
        *,
        archive_root: str,
    ) -> str:
        command_id = f"restore:{archive_root}"
        return self._launch_managed_command(
            command_id,
            f"restore {archive_root}",
            [
                str(self.repo.root / "starter" / "bin" / "restore-run-evidence.sh"),
                str(archive_path),
                str(destination_root),
            ],
            cwd=self.repo.root,
            env={"PYTHONPATH": str(self.repo.root / "starter" / "bin")},
        )

    def launch_runtime_check(self) -> str:
        return self._launch_managed_command(
            "runtime-check",
            "runtime check",
            [str(self.repo.root / "starter" / "bin" / "check-supported-runtime.sh")],
            cwd=self.repo.root,
            env={"PYTHONPATH": str(self.repo.root / "starter" / "bin")},
        )

    def _record_command_result(self, command_id: str, managed: _ManagedCommand) -> None:
        stdout_line = _last_nonempty_line(managed.stdout_path)
        stderr_line = _last_nonempty_line(managed.stderr_path)
        if managed.exit_code == 0:
            summary_line = stdout_line or stderr_line or "completed successfully"
        else:
            summary_line = (
                stderr_line
                or stdout_line
                or f"failed with exit code {managed.exit_code}"
            )
        self.last_action_result = f"{managed.label}: {summary_line}"
        self._record_message(self.last_action_result)
        if command_id == "runtime-check":
            self.last_runtime_check_ok = managed.exit_code == 0
            self.last_runtime_check_message = summary_line
            self.last_runtime_check_completed_epoch_ms = managed.completed_epoch_ms
        elif command_id.startswith("archive:") and managed.exit_code == 0 and stdout_line:
            run_id = command_id.split(":", 1)[1]
            self.archive_paths[run_id] = pathlib.Path(stdout_line).expanduser().resolve()

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
            self._record_command_result(command_id, managed)
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
        elif self.crash_loop:
            state = "crash_loop"
        elif self.desired_running and self.next_restart_at > _monotonic():
            state = "backoff"
            next_restart_seconds = max(0.0, self.next_restart_at - _monotonic())
        elif self.desired_running and self.last_error:
            state = "launch_failed"
        elif self.desired_running:
            state = "starting"
        return OrchestratorSnapshot(
            state=state,
            pid=pid,
            exit_code=self.last_exit_code,
            next_restart_seconds=next_restart_seconds,
            restart_failures=self.restart_failures,
            last_error=self.last_error,
            last_started_epoch_ms=self.last_started_epoch_ms,
            last_exited_epoch_ms=self.last_exited_epoch_ms,
            stdout_path=self.stdout_path,
            stderr_path=self.stderr_path,
        )

    def shutdown(self) -> None:
        self.stop()
        for managed in self.active_commands.values():
            if managed.process.poll() is None:
                managed.process.terminate()
        self.active_commands.clear()
        self._persist_status("stopped")


class ControlCenterService:
    def __init__(self, config: ControlCenterConfig):
        self.config = config
        self.supervisors = {repo.id: RepoSupervisor(repo) for repo in config.repos}
        self._last_snapshot = FleetSnapshot(repos=(), totals={}, pass_rate_percent=0.0)

    def close(self) -> None:
        for supervisor in self.supervisors.values():
            supervisor.shutdown()

    def build_filter_state(
        self,
        *,
        failed_only: bool = False,
        queued_only: bool = False,
        capability_only: bool = False,
        last_24h_only: bool = False,
        text: str = "",
    ) -> RunFilterState:
        return RunFilterState(
            failed_only=failed_only,
            queued_only=queued_only,
            capability_only=capability_only,
            last_24h_only=last_24h_only,
            text=text.strip(),
        )

    def _filter_runs_legacy(self, runs: tuple[RunRow, ...], filter_text: str) -> list[RunRow]:
        filtered = list(runs)
        tokens = [token for token in filter_text.split() if token.strip()]
        cutoff_ms = None
        for token in tokens:
            if ":" not in token:
                continue
            key, value = token.split(":", 1)
            if key in {"age", "window"}:
                days = _to_positive_int(value, default=None)
                if days is None:
                    continue
                cutoff_ms = _now_ms() - (days * 24 * 60 * 60 * 1000)
        if cutoff_ms is not None:
            filtered = [run for run in filtered if run.updated_epoch_ms >= cutoff_ms]

        for token in tokens:
            if ":" not in token:
                continue
            key, value = token.split(":", 1)
            if key == "state":
                filtered = [run for run in filtered if run.state == value]
            elif key == "failure":
                filtered = [
                    run
                    for run in filtered
                    if value in run.failure_classifications or run.primary_error_code == value
                ]
            elif key == "profile":
                filtered = [run for run in filtered if run.execution_profile == value]

        plain_terms = [token.lower() for token in tokens if ":" not in token]
        if plain_terms:
            filtered = [
                run
                for run in filtered
                if all(
                    term
                    in " ".join(
                        [
                            run.run_id.lower(),
                            run.state.lower(),
                            run.primary_error_code.lower(),
                            run.execution_profile.lower(),
                            " ".join(run.failure_classifications).lower(),
                        ]
                    )
                    for term in plain_terms
                )
            ]
        return filtered

    def filter_runs(
        self,
        runs: tuple[RunRow, ...],
        filter_state: RunFilterState | str,
    ) -> list[RunRow]:
        if isinstance(filter_state, str):
            return self._filter_runs_legacy(runs, filter_state)

        filtered = list(runs)
        if filter_state.failed_only:
            filtered = [
                run
                for run in filtered
                if run.state in {"failed", "cancelled"} or run.overall_pass is False
            ]
        if filter_state.queued_only:
            filtered = [
                run
                for run in filtered
                if run.state in {"queued", "claimed", "model_running", "scoring", "score_pending"}
                or run.run_queue_state in {"queued", "claimed", "model_running"}
                or run.score_queue_state in {"queued", "claimed", "scoring"}
            ]
        if filter_state.capability_only:
            filtered = [run for run in filtered if run.execution_profile == "capability"]
        if filter_state.last_24h_only:
            cutoff_ms = _now_ms() - (24 * 60 * 60 * 1000)
            filtered = [run for run in filtered if run.updated_epoch_ms >= cutoff_ms]

        plain_terms = [token.lower() for token in filter_state.text.split() if token.strip()]
        if plain_terms:
            filtered = [
                run
                for run in filtered
                if all(
                    term
                    in " ".join(
                        [
                            run.run_id.lower(),
                            run.state.lower(),
                            run.primary_error_code.lower(),
                            run.execution_profile.lower(),
                            " ".join(run.failure_classifications).lower(),
                        ]
                    )
                    for term in plain_terms
                )
            ]
        return filtered

    def sort_repos(
        self,
        repos: tuple[RepoSnapshot, ...],
        sort_state: SortState,
    ) -> list[RepoSnapshot]:
        def key(repo: RepoSnapshot) -> tuple[Any, ...]:
            summary = repo.summary
            if sort_state.key == "orchestrator":
                return (repo.orchestrator.state, repo.repo.name.lower())
            if sort_state.key == "queue":
                return (repo.queue_depth, repo.repo.name.lower())
            if sort_state.key == "in_flight":
                return (repo.in_flight_count, repo.repo.name.lower())
            if sort_state.key == "pass":
                return (float(summary.get("pass_rate_percent", 0.0)), repo.repo.name.lower())
            if sort_state.key == "p95":
                return (
                    int(summary.get("duration_ms", {}).get("p95", 0)),
                    repo.repo.name.lower(),
                )
            return (repo.repo.name.lower(),)

        return sorted(repos, key=key, reverse=sort_state.reverse)

    def sort_runs(self, runs: list[RunRow], sort_state: SortState) -> list[RunRow]:
        def pass_rank(row: RunRow) -> int:
            if row.overall_pass is True:
                return 2
            if row.overall_pass is False:
                return 1
            return 0

        def key(row: RunRow) -> tuple[Any, ...]:
            if sort_state.key == "state":
                return (row.state, row.run_id.lower())
            if sort_state.key == "pass":
                return (pass_rank(row), row.run_id.lower())
            if sort_state.key == "duration":
                return (row.duration_ms, row.run_id.lower())
            if sort_state.key == "queue_wait":
                return (row.queue_wait_ms, row.run_id.lower())
            if sort_state.key == "score_wait":
                return (row.score_wait_ms, row.run_id.lower())
            if sort_state.key == "profile":
                return (row.execution_profile.lower(), row.run_id.lower())
            return (row.updated_epoch_ms, row.run_id.lower())

        return sorted(runs, key=key, reverse=sort_state.reverse)

    def _queue_depth(self, runs: tuple[RunRow, ...]) -> int:
        return sum(
            1
            for run in runs
            if run.run_queue_state in {"queued", "claimed", "model_running"}
            or run.score_queue_state in {"queued", "claimed", "scoring"}
            or run.state in {"queued", "claimed", "model_running", "scoring", "score_pending"}
        )

    def _queued_count(self, runs: tuple[RunRow, ...]) -> int:
        return sum(
            1
            for run in runs
            if run.run_queue_state == "queued"
            or run.score_queue_state == "queued"
            or run.state == "queued"
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
        total_in_flight = 0
        total_queued = 0
        total_stale_runs = 0
        repos_runtime_failing = 0
        repos_canary_bad = 0

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
            activity = summary.get("activity", {})
            canary_status = summary.get("canary_status", {})
            queue_depth = self._queue_depth(runs)
            queued_count = self._queued_count(runs)
            in_flight_count = self._in_flight_count(runs)
            stale_run_count = int(activity.get("stale_non_terminal_count", 0))
            canary_failing = canary_status.get("all_passed") is False
            freshness_hours = canary_status.get("freshness_hours")
            canary_stale = (
                not canary_status.get("latest_summary_path")
                or not isinstance(freshness_hours, int | float)
                or float(freshness_hours) > CANARY_STALE_HOURS
            )
            total_runs += int(totals.get("total_runs", 0))
            total_pass += int(totals.get("complete_pass", 0))
            total_complete += int(totals.get("complete", 0))
            total_in_flight += in_flight_count
            total_queued += queued_count
            total_stale_runs += stale_run_count
            if supervisor.last_runtime_check_ok is False:
                repos_runtime_failing += 1
            if canary_failing or canary_stale:
                repos_canary_bad += 1
            snapshots.append(
                RepoSnapshot(
                    repo=repo,
                    orchestrator=supervisor.snapshot(),
                    runs=runs,
                    summary=summary,
                    queue_depth=queue_depth,
                    queued_count=queued_count,
                    in_flight_count=in_flight_count,
                    stale_run_count=stale_run_count,
                    runtime_check_ok=supervisor.last_runtime_check_ok,
                    runtime_check_message=supervisor.last_runtime_check_message,
                    runtime_check_completed_epoch_ms=supervisor.last_runtime_check_completed_epoch_ms,
                    canary_failing=canary_failing,
                    canary_stale=canary_stale,
                    last_action_result=supervisor.last_action_result,
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
                "queued": total_queued,
                "in_flight": total_in_flight,
                "stale_runs": total_stale_runs,
                "repos_runtime_failing": repos_runtime_failing,
                "repos_canary_bad": repos_canary_bad,
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

    def _repo_config(self, repo_id: str) -> RepoConfig:
        return next(repo for repo in self.config.repos if repo.id == repo_id)

    def newest_failed_run_id(self, repo_id: str) -> str:
        repo = self._repo_snapshot(repo_id)
        for row in repo.runs:
            if row.state in {"failed", "cancelled"} or row.overall_pass is False:
                return row.run_id
        return ""

    def _run_events(self, repo_id: str, run_id: str) -> list[dict[str, Any]]:
        row = self._run_row(repo_id, run_id)
        return _read_jsonl_events(row.artifact_paths["events"])

    def recommended_artifact_tab(self, repo_id: str, run_id: str) -> str:
        row = self._run_row(repo_id, run_id)
        if row.state in {"failed", "cancelled"} or row.overall_pass is False:
            if row.artifact_paths["score"].exists():
                return "tab-score"
            if row.artifact_paths["transcript"].exists():
                return "tab-transcript"
            return "tab-events"
        if row.state == "complete" and row.overall_pass is True:
            return "tab-patch" if self.run_has_patch(repo_id, run_id) else "tab-overview"
        if row.state in {"queued", "claimed", "model_running", "scoring", "score_pending"}:
            return "tab-events"
        return "tab-overview"

    def _repo_alerts(self, repo: RepoSnapshot) -> list[AlertBadge]:
        alerts: list[AlertBadge] = []
        queue_wait_p95 = int(repo.summary.get("queue_wait_ms", {}).get("p95", 0) or 0)
        saturation_last_24h = int(
            repo.summary.get("queue_saturation", {}).get("events_last_24h", 0) or 0
        )
        if repo.runtime_check_ok is False:
            alerts.append(
                AlertBadge("critical", "Runtime check failed", repo.runtime_check_message or "")
            )
        elif repo.runtime_check_ok is True:
            alerts.append(AlertBadge("success", "Runtime OK"))
        if repo.canary_failing:
            alerts.append(AlertBadge("critical", "Canary failed"))
        elif repo.canary_stale:
            alerts.append(AlertBadge("warning", "Canary stale"))
        if queue_wait_p95 >= 300_000 or saturation_last_24h > 0:
            alerts.append(
                AlertBadge(
                    "critical",
                    "Queue backing up",
                    f"queue wait p95 {render_duration_ms(queue_wait_p95)}",
                )
            )
        elif repo.queued_count >= 3 or queue_wait_p95 >= 60_000:
            alerts.append(
                AlertBadge(
                    "warning",
                    "Queue pressure rising",
                    f"queued {repo.queued_count}, p95 {render_duration_ms(queue_wait_p95)}",
                )
            )
        if repo.stale_run_count > 0:
            alerts.append(
                AlertBadge(
                    "warning",
                    "Stale runs need attention",
                    f"{repo.stale_run_count} stale non-terminal",
                )
            )
        if repo.in_flight_count > 0:
            alerts.append(
                AlertBadge("info", "Runs in flight", f"{repo.in_flight_count} active")
            )
        alerts.sort(key=lambda item: (_severity_rank(item.severity), item.label.lower()))
        return alerts

    def build_repo_alerts(self, repo_id: str) -> tuple[AlertBadge, ...]:
        return tuple(self._repo_alerts(self._repo_snapshot(repo_id)))

    def _run_retry_alerts(self, events: list[dict[str, Any]]) -> list[AlertBadge]:
        alerts: list[AlertBadge] = []
        retry_event: dict[str, Any] | None = None
        retry_ceiling_event: dict[str, Any] | None = None
        for event in events:
            phase = str(event.get("phase", "")).lower()
            message = str(event.get("message", "")).lower()
            failure = str(event.get("failure_classification") or event.get("failure_class") or "")
            heartbeat = str(event.get("heartbeat_reason", ""))
            if phase in {"model_retry", "score_retry"} or failure in {
                "orchestrator_worker_retry",
                "orchestrator_score_retry",
                "score_backpressure",
            }:
                retry_event = event
            if heartbeat == "resource_cap_exceeded":
                retry_event = event
            if phase in {"model_failed", "score_failed"} and (
                "retry ceiling" in message
                or "exhaust" in message
                or failure in {"model_runtime_failure", "orchestrator_worker_exhausted"}
            ):
                retry_ceiling_event = event
        if retry_ceiling_event is not None:
            alerts.append(
                AlertBadge(
                    "critical",
                    "Retry ceiling hit",
                    str(retry_ceiling_event.get("message", "")).strip(),
                )
            )
        elif retry_event is not None:
            alerts.append(
                AlertBadge(
                    "warning",
                    "Retry or backpressure in progress",
                    str(retry_event.get("message", "")).strip(),
                )
            )
        return alerts

    def build_run_alerts(self, repo_id: str, run_id: str) -> tuple[AlertBadge, ...]:
        row = self._run_row(repo_id, run_id)
        alerts: list[AlertBadge] = []
        if row.state in {"failed", "cancelled"}:
            alerts.append(AlertBadge("critical", row.state.title(), row.primary_error_code or ""))
        elif row.overall_pass is False:
            alerts.append(AlertBadge("critical", "Evaluation failed", row.primary_error_code or ""))
        elif row.state == "complete" and row.overall_pass is True:
            alerts.append(AlertBadge("success", "Passing complete run"))
        elif row.state in {"queued", "claimed", "model_running", "scoring", "score_pending"}:
            alerts.append(AlertBadge("info", "Run in progress", row.state))
        alerts.extend(self._run_retry_alerts(self._run_events(repo_id, run_id)))
        alerts.sort(key=lambda item: (_severity_rank(item.severity), item.label.lower()))
        return tuple(alerts)

    def build_run_timeline(self, repo_id: str, run_id: str) -> tuple[TimelineStep, ...]:
        row = self._run_row(repo_id, run_id)
        events = self._run_events(repo_id, run_id)
        seen_claimed = row.run_queue_state == "claimed"
        seen_model_running = row.run_queue_state == "model_running" or row.state == "model_running"
        seen_scoring = row.score_queue_state in {"claimed", "scoring"} or row.state in {
            "scoring",
            "score_pending",
            "model_complete",
        }
        for event in events:
            state_after = str(event.get("state_after", ""))
            phase = str(event.get("phase", ""))
            if state_after == "claimed":
                seen_claimed = True
            if state_after == "model_running" or phase == "model_dispatch":
                seen_model_running = True
            if state_after == "scoring" or phase.startswith("score_"):
                seen_scoring = True

        final_label = "Complete"
        final_status = "upcoming"
        if row.state == "failed" or row.overall_pass is False:
            final_label = "Failed"
            final_status = "problem"
        elif row.state == "cancelled":
            final_label = "Cancelled"
            final_status = "problem"
        elif row.state == "complete":
            final_status = "done"

        if row.state in {"failed", "cancelled", "complete"}:
            current_index = 4
        elif row.state in {"scoring", "score_pending", "model_complete"} or seen_scoring:
            current_index = 3
        elif row.state == "model_running" or seen_model_running:
            current_index = 2
        elif row.run_queue_state == "claimed" or seen_claimed:
            current_index = 1
        else:
            current_index = 0

        labels = [
            ("queued", "Queued"),
            ("claimed", "Claimed"),
            ("model_running", "Model Running"),
            ("scoring", "Scoring"),
            ("complete", final_label),
        ]
        steps: list[TimelineStep] = []
        for index, (key, label) in enumerate(labels):
            if index < current_index:
                status = "done"
            elif index == current_index:
                status = final_status if index == 4 else "current"
            else:
                status = "upcoming"
            if index == 4 and final_status == "problem":
                status = "problem"
            steps.append(TimelineStep(key=key, label=label, status=status))
        return tuple(steps)

    def build_context_actions(self, repo_id: str, run_id: str) -> tuple[UIAction, ...]:
        repo = self._repo_snapshot(repo_id)
        row = self._run_row(repo_id, run_id)
        repo_state = repo.orchestrator.state
        actions = [
            UIAction(
                id="open-best-artifact",
                label="Open Best Artifact",
                kind="open",
                scope="run",
                command_text="open-best-artifact",
                open_tab=self.recommended_artifact_tab(repo_id, run_id),
                aliases=("best", "recommended", "artifact"),
            ),
            UIAction(
                id="open-transcript",
                label="Open Transcript",
                kind="open",
                scope="run",
                command_text="open transcript",
                open_tab="tab-transcript",
                aliases=("transcript", "logs"),
            ),
            UIAction(
                id="open-score",
                label="Open Score",
                kind="open",
                scope="run",
                command_text="open score",
                enabled=row.artifact_paths["score"].exists(),
                disabled_reason="score artifact missing",
                open_tab="tab-score",
                aliases=("score", "evaluation"),
            ),
            UIAction(
                id="runtime-check",
                label="Runtime Check",
                kind="service",
                scope="repo",
                command_text=f"runtime-check {repo_id}",
                aliases=("runtime", "python"),
            ),
            UIAction(
                id="archive-run",
                label="Archive",
                kind="service",
                scope="run",
                command_text=f"archive-run {run_id}",
                aliases=("archive", "evidence"),
            ),
            UIAction(
                id="cancel-run",
                label="Cancel",
                kind="service",
                scope="run",
                command_text=f"run cancel {run_id}",
                requires_confirmation=True,
                enabled=not _is_terminal_state(row.state),
                disabled_reason="run already terminal" if _is_terminal_state(row.state) else "",
                aliases=("cancel", "stop run"),
            ),
            UIAction(
                id="enqueue-run",
                label="Enqueue",
                kind="service",
                scope="run",
                command_text=f"run enqueue {run_id}",
                enabled=(
                    not _is_terminal_state(row.state)
                    and not _run_is_locked(row.run_dir)
                    and row.run_queue_state not in {"queued", "claimed", "model_running"}
                ),
                disabled_reason=(
                    "run not eligible"
                    if _is_terminal_state(row.state)
                    or _run_is_locked(row.run_dir)
                    or row.run_queue_state in {"queued", "claimed", "model_running"}
                    else ""
                ),
                aliases=("enqueue", "queue"),
            ),
            UIAction(
                id="rerun-run",
                label="Rerun",
                kind="service",
                scope="run",
                command_text=f"run rerun {run_id}",
                requires_confirmation=True,
                enabled=_is_terminal_state(row.state),
                disabled_reason="run is not terminal" if not _is_terminal_state(row.state) else "",
                aliases=("rerun", "retry"),
            ),
            UIAction(
                id="repo-start",
                label="Start Repo",
                kind="service",
                scope="repo",
                command_text=f"repo start {repo_id}",
                enabled=repo_state != "running",
                disabled_reason="repo already running" if repo_state == "running" else "",
                aliases=("start repo",),
            ),
            UIAction(
                id="repo-stop",
                label="Stop Repo",
                kind="service",
                scope="repo",
                command_text=f"repo stop {repo_id}",
                requires_confirmation=True,
                enabled=repo_state != "stopped",
                disabled_reason="repo already stopped" if repo_state == "stopped" else "",
                aliases=("stop repo",),
            ),
            UIAction(
                id="repo-restart",
                label="Restart Repo",
                kind="service",
                scope="repo",
                command_text=f"repo restart {repo_id}",
                requires_confirmation=True,
                enabled=repo_state != "stopped",
                disabled_reason="repo is stopped" if repo_state == "stopped" else "",
                aliases=("restart repo",),
            ),
            UIAction(
                id="repo-canary",
                label="Run Canary",
                kind="service",
                scope="repo",
                command_text=f"repo canary {repo_id}",
                aliases=("canary",),
            ),
        ]
        return tuple(actions)

    def build_target_summary(self, repo_id: str, run_id: str) -> TargetSummary:
        repo = self._repo_snapshot(repo_id)
        row = self._run_row(repo_id, run_id)
        actions = self.build_context_actions(repo_id, run_id)
        safe_actions = tuple(
            action for action in actions if action.enabled and not action.requires_confirmation
        )[:3]
        alerts = tuple(self._repo_alerts(repo) + list(self.build_run_alerts(repo_id, run_id)))
        pass_label = (
            "pass" if row.overall_pass is True else "fail" if row.overall_pass is False else "pending"
        )
        return TargetSummary(
            repo_id=repo.repo.id,
            repo_name=repo.repo.name,
            run_id=row.run_id,
            run_state=row.state,
            pass_label=pass_label,
            profile=row.execution_profile or "-",
            age_text=_render_relative_age(row.updated_epoch_ms),
            alerts=alerts,
            recommended_actions=safe_actions,
            recommended_tab=self.recommended_artifact_tab(repo_id, run_id),
        )

    def _chat_log_path(self, repo_id: str) -> pathlib.Path:
        return self._repo_config(repo_id).runs_root / ".orchestrator" / "chat-log.jsonl"

    def _chat_state_path(self, repo_id: str) -> pathlib.Path:
        return self._repo_config(repo_id).runs_root / ".orchestrator" / "chat-state.json"

    def _chat_append(
        self,
        repo_id: str,
        *,
        role: str,
        message_type: str,
        content: str,
        run_id: str = "",
        action_name: str = "",
        resulting_run_id: str = "",
        follow_up_actions: tuple[UIAction, ...] = (),
    ) -> None:
        _append_jsonl(
            self._chat_log_path(repo_id),
            {
                "ts_ms": _now_ms(),
                "repo_id": repo_id,
                "role": role,
                "message_type": message_type,
                "content": content,
                "run_id": run_id,
                "action_name": action_name,
                "resulting_run_id": resulting_run_id,
                "follow_up_actions": [
                    _serialize_ui_action(action) for action in follow_up_actions
                ],
            },
        )

    def _load_chat_messages(self, repo_id: str) -> tuple[ChatMessage, ...]:
        messages: list[ChatMessage] = []
        for payload in harvester._read_jsonl(self._chat_log_path(repo_id)):  # type: ignore[attr-defined]
            messages.append(
                ChatMessage(
                    ts_ms=int(payload.get("ts_ms", 0) or 0),
                    repo_id=str(payload.get("repo_id", repo_id)),
                    role=str(payload.get("role", "assistant")),
                    message_type=str(payload.get("message_type", "reply")),
                    content=str(payload.get("content", "")),
                    run_id=str(payload.get("run_id", "")),
                    action_name=str(payload.get("action_name", "")),
                    resulting_run_id=str(payload.get("resulting_run_id", "")),
                    follow_up_actions=tuple(
                        _deserialize_ui_action(item)
                        for item in payload.get("follow_up_actions", [])
                        if isinstance(item, dict)
                    ),
                )
            )
        return tuple(messages)

    def _load_pending_action(self, repo_id: str) -> ChatPendingAction | None:
        payload = _read_json_dict(self._chat_state_path(repo_id))
        pending = payload.get("pending_action")
        if not isinstance(pending, dict):
            return None
        return ChatPendingAction(
            action_type=str(pending.get("action_type", "")),
            label=str(pending.get("label", "")),
            prompt=str(pending.get("prompt", "")),
            payload=dict(pending.get("payload", {}))
            if isinstance(pending.get("payload"), dict)
            else {},
        )

    def _save_pending_action(self, repo_id: str, pending: ChatPendingAction | None) -> None:
        path = self._chat_state_path(repo_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {}
        if pending is not None:
            payload["pending_action"] = {
                "action_type": pending.action_type,
                "label": pending.label,
                "prompt": pending.prompt,
                "payload": pending.payload,
            }
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def chat_history_text(self, repo_id: str) -> str:
        messages = self._load_chat_messages(repo_id)
        if not messages:
            return (
                "Operator chat is ready.\n\n"
                "Examples:\n"
                "- show failed runs\n"
                "- restart repo\n"
                "- rerun run-20260324-123456\n"
                "- /new --profile capability --model openai/gpt-5.4 fix flaky login\n"
            )
        lines: list[str] = []
        for message in messages[-40:]:
            stamp = format_timestamp_ms(message.ts_ms)
            label = {
                "operator": "Operator",
                "assistant": "Assistant",
                "system": "System",
            }.get(message.role, message.role.title())
            lines.append(f"[{stamp}] {label}:")
            lines.append(message.content)
            lines.append("")
        return "\n".join(lines).rstrip()

    def chat_follow_up_actions(self, repo_id: str) -> tuple[UIAction, ...]:
        messages = self._load_chat_messages(repo_id)
        for message in reversed(messages):
            if message.role in {"assistant", "system"} and message.follow_up_actions:
                return message.follow_up_actions
        return ()

    def chat_banner_text(self, repo_id: str) -> str:
        pending = self._load_pending_action(repo_id)
        if pending is None:
            return "No pending action. Reads execute immediately; mutating actions require confirmation."
        return f"Pending confirmation: {pending.label}. Type `confirm` to proceed or `cancel` to discard."

    def _failed_runs_summary(self, repo_id: str) -> str:
        repo = self._repo_snapshot(repo_id)
        failed = [run for run in repo.runs if run.state in {"failed", "cancelled"} or run.overall_pass is False]
        if not failed:
            return "No failed runs are visible in the current window."
        lines = ["Failed runs:"]
        for run in failed[:10]:
            lines.append(
                f"- {run.run_id} | state={run.state} | error={run.primary_error_code or '-'}"
            )
        return "\n".join(lines)

    def _failed_runs_follow_ups(self, repo_id: str) -> tuple[UIAction, ...]:
        newest_failed = self.newest_failed_run_id(repo_id)
        return tuple(
            action
            for action in (
                UIAction(
                    id="focus-newest-failed-run",
                    label="Focus Newest Failed Run",
                    kind="chat",
                    scope="repo",
                    command_text="focus newest-failed",
                    enabled=bool(newest_failed),
                    disabled_reason="no failed run available" if not newest_failed else "",
                ),
                UIAction(
                    id="filter-failed",
                    label="Filter Failed",
                    kind="filter",
                    scope="repo",
                    command_text="filter failed",
                ),
                UIAction(
                    id="open-score-newest-failed",
                    label="Open Score for Newest Failed",
                    kind="chat",
                    scope="repo",
                    command_text="open score newest-failed",
                    enabled=bool(newest_failed),
                    disabled_reason="no failed run available" if not newest_failed else "",
                ),
            )
        )

    def _run_status_summary(self, repo_id: str, run_id: str) -> str:
        row = self._run_row(repo_id, run_id)
        return "\n".join(
            [
                f"Selected run: {row.run_id}",
                f"State: {row.state}",
                f"Pass: {row.overall_pass}",
                f"Profile: {row.execution_profile or '-'}",
                f"Primary error: {row.primary_error_code or '-'}",
                f"Queue wait: {row.queue_wait_ms} ms",
                f"Score wait: {row.score_wait_ms} ms",
            ]
        )

    def _queue_summary(self, repo_id: str) -> str:
        repo = self._repo_snapshot(repo_id)
        return "\n".join(
            [
                f"Queue depth: {repo.queue_depth}",
                f"Queued: {repo.queued_count}",
                f"In flight: {repo.in_flight_count}",
                f"Stale runs: {repo.stale_run_count}",
            ]
        )

    def _queue_follow_ups(self, repo_id: str) -> tuple[UIAction, ...]:
        return (
            UIAction(
                id="filter-queued",
                label="Filter Queued",
                kind="filter",
                scope="repo",
                command_text="filter queued",
            ),
            UIAction(
                id="open-health",
                label="Open Health",
                kind="open",
                scope="repo",
                command_text="open health",
                open_tab="tab-health",
            ),
            UIAction(
                id="runtime-check-follow-up",
                label="Runtime Check",
                kind="service",
                scope="repo",
                command_text=f"runtime-check {repo_id}",
            ),
        )

    def _canary_summary(self, repo_id: str) -> str:
        repo = self._repo_snapshot(repo_id)
        canary = repo.summary.get("canary_status", {})
        return "\n".join(
            [
                f"Canary summary: {canary.get('latest_summary_path') or '-'}",
                f"Finished: {format_timestamp_ms(canary.get('completed_epoch_ms'))}",
                f"Freshness hours: {canary.get('freshness_hours', '-')}",
                f"All passed: {canary.get('all_passed')}",
                f"Stale: {repo.canary_stale}",
            ]
        )

    def _canary_follow_ups(self, repo_id: str) -> tuple[UIAction, ...]:
        return (
            UIAction(
                id="open-health-canary",
                label="Open Health",
                kind="open",
                scope="repo",
                command_text="open health",
                open_tab="tab-health",
            ),
            UIAction(
                id="run-canary-follow-up",
                label="Run Canary",
                kind="service",
                scope="repo",
                command_text=f"repo canary {repo_id}",
            ),
        )

    def _current_run_follow_ups(self, repo_id: str, run_id: str) -> tuple[UIAction, ...]:
        row = self._run_row(repo_id, run_id)
        actions = [
            UIAction(
                id="open-best-artifact-follow-up",
                label="Open Best Artifact",
                kind="open",
                scope="run",
                command_text="open-best-artifact",
                open_tab=self.recommended_artifact_tab(repo_id, run_id),
            ),
            UIAction(
                id="open-transcript-follow-up",
                label="Open Transcript",
                kind="open",
                scope="run",
                command_text="open transcript",
                open_tab="tab-transcript",
            ),
        ]
        if row.state in {"failed", "cancelled"} or row.overall_pass is False:
            actions.append(
                UIAction(
                    id="rerun-follow-up",
                    label="Rerun",
                    kind="service",
                    scope="run",
                    command_text=f"run rerun {run_id}",
                    requires_confirmation=True,
                    enabled=_is_terminal_state(row.state),
                )
            )
        return tuple(actions)

    def _draft_new_run(
        self,
        repo_id: str,
        prompt: str,
        *,
        profile: str = "",
        model: str = "",
        launch_mode: str = "launch",
    ) -> ChatSubmissionResult:
        repo = self._repo_config(repo_id)
        title = _derive_run_title(prompt)
        resolved_profile = profile or repo.default_profile or "strict"
        resolved_model = model or repo.default_model
        preview_lines = [
            "Pending run draft",
            f"Title: {title}",
            f"Profile: {resolved_profile}",
            f"Model: {resolved_model or '(harness default)'}",
            f"Launch mode: {launch_mode}",
            "",
            "Task preview",
            "=" * 12,
            _build_chat_task_text("", title=title, prompt=prompt).split("## Result JSON schema")[0].rstrip(),
            "",
            "Type `confirm` to proceed or `cancel` to discard.",
        ]
        pending = ChatPendingAction(
            action_type="new_run",
            label=f"create + {launch_mode} run `{title}`",
            prompt=prompt,
            payload={
                "title": title,
                "profile": resolved_profile,
                "model": resolved_model,
                "launch_mode": launch_mode,
            },
        )
        self._save_pending_action(repo_id, pending)
        reply = "\n".join(preview_lines)
        self._chat_append(
            repo_id,
            role="assistant",
            message_type="pending_action",
            content=reply,
            action_name="new_run",
            follow_up_actions=(),
        )
        return ChatSubmissionResult(reply=reply, follow_up_actions=())

    def _create_chat_run(self, repo_id: str, pending: ChatPendingAction) -> ChatSubmissionResult:
        repo = self._repo_config(repo_id)
        profile = str(pending.payload.get("profile", "")).strip() or repo.default_profile or "strict"
        model = str(pending.payload.get("model", "")).strip()
        launch_mode = str(pending.payload.get("launch_mode", "launch")).strip() or "launch"
        title = str(pending.payload.get("title", "")).strip() or _derive_run_title(pending.prompt)
        command = [str(repo.root / "starter" / "bin" / "new-task.sh"), "--profile", profile, title]
        completed = subprocess.run(
            command,
            cwd=repo.root / "starter",
            capture_output=True,
            text=True,
            check=False,
            env=os.environ.copy() | {"PYTHONPATH": str(repo.root / "starter" / "bin")},
        )
        if completed.returncode != 0:
            reply = f"run creation failed: {(completed.stderr or completed.stdout).strip() or completed.returncode}"
            self._chat_append(repo_id, role="system", message_type="action_result", content=reply, action_name="new_run")
            return ChatSubmissionResult(reply=reply)

        run_ref = ""
        for line in completed.stdout.splitlines():
            candidate = line.strip()
            if candidate.startswith("runs/"):
                run_ref = candidate
        run_dir = (repo.root / "starter" / run_ref).resolve() if run_ref else None
        if run_dir is None or not run_dir.exists():
            reply = "run creation failed: could not resolve the created run directory"
            self._chat_append(repo_id, role="system", message_type="action_result", content=reply, action_name="new_run")
            return ChatSubmissionResult(reply=reply)

        task_path = run_dir / "task.md"
        task_text = task_path.read_text(encoding="utf-8")
        task_path.write_text(
            _build_chat_task_text(task_text, title=title, prompt=pending.prompt),
            encoding="utf-8",
        )
        self.refresh()
        run_id = run_dir.name
        if launch_mode == "queue":
            action_result = self.enqueue_run(repo_id, run_id)
        else:
            action_result = self.supervisors[repo_id].launch_run(run_dir, model=model)
        reply = (
            f"Created run `{run_id}`.\n"
            f"Profile: {profile}\n"
            f"Model: {model or '(harness default)'}\n"
            f"Action: {action_result}"
        )
        self._chat_append(
            repo_id,
            role="system",
            message_type="action_result",
            content=reply,
            action_name="new_run",
            resulting_run_id=run_id,
            follow_up_actions=(),
        )
        self._save_pending_action(repo_id, None)
        return ChatSubmissionResult(reply=reply, focus_run_id=run_id, follow_up_actions=())

    def _execute_pending_action(self, repo_id: str, pending: ChatPendingAction) -> ChatSubmissionResult:
        if pending.action_type == "new_run":
            return self._create_chat_run(repo_id, pending)

        action_name = str(pending.payload.get("action_name", ""))
        target_repo_id = str(pending.payload.get("repo_id", repo_id))
        run_id = str(pending.payload.get("run_id", ""))
        archive_path = str(pending.payload.get("archive_path", ""))
        force = bool(pending.payload.get("force", False))
        if action_name == "repo_restart":
            reply = self.restart_repo(target_repo_id)
        elif action_name == "repo_start":
            reply = self.start_repo(target_repo_id)
        elif action_name == "repo_stop":
            reply = self.stop_repo(target_repo_id)
        elif action_name == "repo_canary":
            reply = self.run_canary(target_repo_id)
        elif action_name == "runtime_check":
            reply = self.runtime_check(target_repo_id)
        elif action_name == "run_cancel":
            reply = self.cancel_run(target_repo_id, run_id)
        elif action_name == "run_enqueue":
            reply = self.enqueue_run(target_repo_id, run_id)
        elif action_name == "run_rerun":
            reply = self.rerun_run(target_repo_id, run_id)
        elif action_name == "archive_run":
            reply = self.archive_run(target_repo_id, run_id)
        elif action_name == "restore_evidence":
            reply = self.restore_evidence(
                target_repo_id,
                run_id,
                archive_path=archive_path,
                force=force,
            )
        else:
            reply = f"unsupported pending action: {action_name or pending.action_type}"
        self._chat_append(
            repo_id,
            role="system",
            message_type="action_result",
            content=reply,
            run_id=run_id,
            action_name=action_name,
            follow_up_actions=(),
        )
        self._save_pending_action(repo_id, None)
        return ChatSubmissionResult(reply=reply, focus_run_id=run_id, follow_up_actions=())

    def _pending_simple_action(
        self,
        repo_id: str,
        *,
        action_name: str,
        label: str,
        run_id: str = "",
        archive_path: str = "",
        force: bool = False,
    ) -> ChatSubmissionResult:
        pending = ChatPendingAction(
            action_type="service_action",
            label=label,
            prompt=label,
            payload={
                "action_name": action_name,
                "repo_id": repo_id,
                "run_id": run_id,
                "archive_path": archive_path,
                "force": force,
            },
        )
        self._save_pending_action(repo_id, pending)
        reply = f"{label}\n\nType `confirm` to proceed or `cancel` to discard."
        self._chat_append(
            repo_id,
            role="assistant",
            message_type="pending_action",
            content=reply,
            run_id=run_id,
            action_name=action_name,
            follow_up_actions=(),
        )
        return ChatSubmissionResult(reply=reply, focus_run_id=run_id, follow_up_actions=())

    def _command_tokens_to_chat(
        self,
        repo_id: str,
        selected_run_id: str,
        tokens: list[str],
    ) -> ChatSubmissionResult:
        repo = self._repo_snapshot(repo_id)
        if tokens[0] in {"/new", "new"}:
            profile = ""
            model = ""
            launch_mode = "launch"
            prompt_parts: list[str] = []
            index = 1
            while index < len(tokens):
                token = tokens[index]
                if token == "--profile" and index + 1 < len(tokens):
                    profile = tokens[index + 1]
                    index += 2
                    continue
                if token.startswith("--profile="):
                    profile = token.split("=", 1)[1]
                    index += 1
                    continue
                if token == "--model" and index + 1 < len(tokens):
                    model = tokens[index + 1]
                    index += 2
                    continue
                if token.startswith("--model="):
                    model = token.split("=", 1)[1]
                    index += 1
                    continue
                if token in {"--queue", "--enqueue"}:
                    launch_mode = "queue"
                    index += 1
                    continue
                prompt_parts.extend(tokens[index:])
                break
            prompt = " ".join(prompt_parts).strip()
            if not prompt:
                reply = "Usage: /new [--profile PROFILE] [--model MODEL] [--queue] <operator request>"
                self._chat_append(repo_id, role="assistant", message_type="reply", content=reply)
                return ChatSubmissionResult(reply=reply)
            return self._draft_new_run(
                repo_id,
                prompt,
                profile=profile,
                model=model,
                launch_mode=launch_mode,
            )
        if tokens[0] == "repo":
            action = tokens[1] if len(tokens) > 1 else ""
            target_repo_id = tokens[2] if len(tokens) > 2 else repo_id
            if action == "start":
                return self._pending_simple_action(target_repo_id, action_name="repo_start", label=f"Start repo `{target_repo_id}`")
            if action == "stop":
                return self._pending_simple_action(target_repo_id, action_name="repo_stop", label=f"Stop repo `{target_repo_id}`")
            if action == "restart":
                return self._pending_simple_action(target_repo_id, action_name="repo_restart", label=f"Restart repo `{target_repo_id}`")
            if action == "canary":
                return self._pending_simple_action(target_repo_id, action_name="repo_canary", label=f"Run canary for repo `{target_repo_id}`")
        if tokens[0] == "run":
            action = tokens[1] if len(tokens) > 1 else ""
            run_id = tokens[2] if len(tokens) > 2 else selected_run_id
            if not run_id:
                reply = "Select a run or specify a run id."
                self._chat_append(repo_id, role="assistant", message_type="reply", content=reply)
                return ChatSubmissionResult(reply=reply)
            if action == "cancel":
                return self._pending_simple_action(repo_id, action_name="run_cancel", label=f"Cancel run `{run_id}`", run_id=run_id)
            if action == "enqueue":
                return self._pending_simple_action(repo_id, action_name="run_enqueue", label=f"Enqueue run `{run_id}`", run_id=run_id)
            if action == "rerun":
                return self._pending_simple_action(repo_id, action_name="run_rerun", label=f"Rerun run `{run_id}`", run_id=run_id)
        if tokens[0] == "runtime-check":
            return self._pending_simple_action(repo_id, action_name="runtime_check", label=f"Run runtime check for repo `{repo_id}`")
        if tokens[0] == "archive-run":
            run_id = tokens[1] if len(tokens) > 1 else selected_run_id
            return self._pending_simple_action(repo_id, action_name="archive_run", label=f"Archive run `{run_id}`", run_id=run_id)
        if tokens[0] == "restore-evidence":
            run_id = tokens[1] if len(tokens) > 1 and not tokens[1].startswith("/") else selected_run_id
            archive_path = ""
            if len(tokens) > 1 and tokens[1].startswith("/"):
                archive_path = tokens[1]
            elif len(tokens) > 2:
                archive_path = tokens[2]
            force = "--force" in tokens[1:]
            return self._pending_simple_action(
                repo_id,
                action_name="restore_evidence",
                label=f"Restore evidence for run `{run_id}`",
                run_id=run_id,
                archive_path=archive_path,
                force=force,
            )
        if tokens[0] == "open":
            target = tokens[1] if len(tokens) > 1 else "overview"
            tab_map = {
                "health": "tab-health",
                "overview": "tab-overview",
                "manifest": "tab-overview",
                "events": "tab-events",
                "transcript": "tab-transcript",
                "score": "tab-score",
                "patch": "tab-patch",
            }
            open_tab = tab_map.get(target)
            if open_tab:
                reply = f"Opened {target}."
                self._chat_append(repo_id, role="assistant", message_type="reply", content=reply, run_id=selected_run_id)
                return ChatSubmissionResult(reply=reply, focus_run_id=selected_run_id, open_tab=open_tab)
        reply = f"Unsupported chat command: {' '.join(tokens)}"
        self._chat_append(repo_id, role="assistant", message_type="reply", content=reply)
        return ChatSubmissionResult(reply=reply)

    def submit_chat_message(
        self,
        repo_id: str,
        message: str,
        *,
        selected_run_id: str = "",
    ) -> ChatSubmissionResult:
        message = message.strip()
        if not message:
            reply = "Enter a chat request."
            return ChatSubmissionResult(reply=reply)
        self._chat_append(
            repo_id,
            role="operator",
            message_type="query",
            content=message,
            run_id=selected_run_id,
        )
        pending = self._load_pending_action(repo_id)
        normalized = message.lower().strip()
        if pending is not None:
            if normalized in {"confirm", "yes", "y", "launch"}:
                return self._execute_pending_action(repo_id, pending)
            if normalized in {"cancel", "no", "discard"}:
                self._save_pending_action(repo_id, None)
                reply = f"Cancelled pending action: {pending.label}"
                self._chat_append(repo_id, role="system", message_type="action_result", content=reply)
                return ChatSubmissionResult(reply=reply)
            reply = "A pending action is waiting. Type `confirm` to proceed or `cancel` to discard."
            self._chat_append(repo_id, role="assistant", message_type="reply", content=reply)
            return ChatSubmissionResult(reply=reply)

        try:
            tokens = shlex.split(message)
        except ValueError as exc:
            reply = f"Chat parse error: {exc}"
            self._chat_append(repo_id, role="assistant", message_type="reply", content=reply)
            return ChatSubmissionResult(reply=reply)

        if tokens and (tokens[0].startswith("/") or tokens[0] in {"repo", "run", "runtime-check", "archive-run", "restore-evidence", "open", "new"}):
            return self._command_tokens_to_chat(repo_id, selected_run_id, tokens)

        if "failed run" in normalized or normalized.startswith("show failed"):
            reply = self._failed_runs_summary(repo_id)
            follow_up_actions = self._failed_runs_follow_ups(repo_id)
            self._chat_append(
                repo_id,
                role="assistant",
                message_type="reply",
                content=reply,
                follow_up_actions=follow_up_actions,
            )
            return ChatSubmissionResult(reply=reply, follow_up_actions=follow_up_actions)
        if "queue depth" in normalized or "in flight" in normalized:
            reply = self._queue_summary(repo_id)
            follow_up_actions = self._queue_follow_ups(repo_id)
            self._chat_append(
                repo_id,
                role="assistant",
                message_type="reply",
                content=reply,
                follow_up_actions=follow_up_actions,
            )
            return ChatSubmissionResult(reply=reply, follow_up_actions=follow_up_actions)
        if "canary" in normalized:
            reply = self._canary_summary(repo_id)
            follow_up_actions = self._canary_follow_ups(repo_id)
            self._chat_append(
                repo_id,
                role="assistant",
                message_type="reply",
                content=reply,
                follow_up_actions=follow_up_actions,
            )
            return ChatSubmissionResult(reply=reply, follow_up_actions=follow_up_actions)
        if ("selected run" in normalized or "current run" in normalized or "run status" in normalized) and selected_run_id:
            reply = self._run_status_summary(repo_id, selected_run_id)
            follow_up_actions = self._current_run_follow_ups(repo_id, selected_run_id)
            self._chat_append(
                repo_id,
                role="assistant",
                message_type="reply",
                content=reply,
                run_id=selected_run_id,
                follow_up_actions=follow_up_actions,
            )
            return ChatSubmissionResult(
                reply=reply,
                focus_run_id=selected_run_id,
                open_tab="tab-overview",
                follow_up_actions=follow_up_actions,
            )
        if normalized.startswith("restart repo"):
            return self._pending_simple_action(repo_id, action_name="repo_restart", label=f"Restart repo `{repo_id}`")
        if normalized.startswith("start repo"):
            return self._pending_simple_action(repo_id, action_name="repo_start", label=f"Start repo `{repo_id}`")
        if normalized.startswith("stop repo"):
            return self._pending_simple_action(repo_id, action_name="repo_stop", label=f"Stop repo `{repo_id}`")
        if normalized.startswith("run canary") or normalized.startswith("canary"):
            return self._pending_simple_action(repo_id, action_name="repo_canary", label=f"Run canary for repo `{repo_id}`")
        if normalized.startswith("runtime check"):
            return self._pending_simple_action(repo_id, action_name="runtime_check", label=f"Run runtime check for repo `{repo_id}`")
        if normalized.startswith("rerun run "):
            run_id = normalized.split("rerun run ", 1)[1].strip()
            return self._pending_simple_action(repo_id, action_name="run_rerun", label=f"Rerun run `{run_id}`", run_id=run_id)
        if normalized.startswith("cancel run "):
            run_id = normalized.split("cancel run ", 1)[1].strip()
            return self._pending_simple_action(repo_id, action_name="run_cancel", label=f"Cancel run `{run_id}`", run_id=run_id)
        if normalized.startswith("enqueue run "):
            run_id = normalized.split("enqueue run ", 1)[1].strip()
            return self._pending_simple_action(repo_id, action_name="run_enqueue", label=f"Enqueue run `{run_id}`", run_id=run_id)
        if normalized.startswith("archive run "):
            run_id = normalized.split("archive run ", 1)[1].strip()
            return self._pending_simple_action(repo_id, action_name="archive_run", label=f"Archive run `{run_id}`", run_id=run_id)

        if normalized.endswith("?"):
            reply = (
                "Supported chat actions are repo/run control, health/status queries, and "
                "creating new runs with `/new ...` or an explicit operator request."
            )
            self._chat_append(repo_id, role="assistant", message_type="reply", content=reply)
            return ChatSubmissionResult(reply=reply)

        return self._draft_new_run(repo_id, message)

    def start_repo(self, repo_id: str) -> str:
        return self.supervisors[repo_id].start()

    def stop_repo(self, repo_id: str) -> str:
        return self.supervisors[repo_id].stop()

    def restart_repo(self, repo_id: str) -> str:
        return self.supervisors[repo_id].restart()

    def run_canary(self, repo_id: str) -> str:
        return self.supervisors[repo_id].launch_canary()

    def runtime_check(self, repo_id: str) -> str:
        return self.supervisors[repo_id].launch_runtime_check()

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

    def archive_path(self, repo_id: str, run_id: str) -> pathlib.Path:
        supervisor = self.supervisors[repo_id]
        if run_id in supervisor.archive_paths:
            return supervisor.archive_paths[run_id]
        row = self._run_row(repo_id, run_id)
        return row.run_dir.with_suffix(".tgz")

    def archive_run(self, repo_id: str, run_id: str) -> str:
        row = self._run_row(repo_id, run_id)
        if not row.run_dir.exists():
            return f"run directory is missing for {run_id}"
        archive_path = self.archive_path(repo_id, run_id)
        return self.supervisors[repo_id].launch_archive(row.run_dir, archive_path)

    def restore_evidence(
        self,
        repo_id: str,
        run_id: str,
        *,
        archive_path: str = "",
        force: bool = False,
    ) -> str:
        repo = next(item for item in self.config.repos if item.id == repo_id)
        if archive_path:
            resolved_archive = pathlib.Path(archive_path).expanduser().resolve()
        else:
            resolved_archive = self.archive_path(repo_id, run_id).resolve()
        if not resolved_archive.exists():
            return f"archive not found: {resolved_archive}"
        archive_root = _archive_root_name(resolved_archive)
        if not archive_root:
            return f"could not inspect archive: {resolved_archive}"
        destination_dir = repo.runs_root / archive_root
        if destination_dir.exists() and not force:
            return f"restore target exists: {destination_dir} (rerun with --force)"
        return self.supervisors[repo_id].launch_restore(
            resolved_archive,
            repo.runs_root,
            archive_root=archive_root,
        )

    def run_has_patch(self, repo_id: str, run_id: str) -> bool:
        row = self._run_row(repo_id, run_id)
        patch_path = row.artifact_paths.get("patch")
        if patch_path is None or not patch_path.exists():
            return False
        try:
            return patch_path.stat().st_size > 0
        except OSError:
            return False

    def open_run_path(self, repo_id: str, run_id: str) -> str:
        return str(self._run_row(repo_id, run_id).run_dir)

    def open_archive_path(self, repo_id: str, run_id: str) -> str:
        archive_path = self.archive_path(repo_id, run_id).resolve()
        return (
            f"{archive_path}"
            if archive_path.exists()
            else f"{archive_path} (missing)"
        )

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
            "Artifacts",
            "=" * 9,
            f"Run dir: {row.run_dir}",
            f"Manifest: {row.artifact_paths['manifest']}",
            f"Events: {row.artifact_paths['events']}",
            f"Transcript: {row.artifact_paths['transcript']}",
            f"Score: {row.artifact_paths['score']}",
            f"Patch: {row.artifact_paths['patch']}",
            f"Result: {row.artifact_paths['result']}",
            f"Archive: {self.open_archive_path(repo_id, run_id)}",
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

    def repo_health_text(self, repo_id: str) -> str:
        repo = self._repo_snapshot(repo_id)
        orchestrator = repo.orchestrator
        summary = repo.summary
        canary_status = summary.get("canary_status", {})
        activity = summary.get("activity", {})
        top_failure_causes = summary.get("top_failure_causes", [])

        lines = [
            f"Repo: {repo.repo.name} ({repo.repo.id})",
            f"Root: {repo.repo.root}",
            f"Runs root: {repo.repo.runs_root}",
            "",
            f"Orchestrator: {orchestrator.state}",
            f"PID: {orchestrator.pid or '-'}",
            f"Restart failures: {orchestrator.restart_failures}",
            f"Next restart: {orchestrator.next_restart_seconds:.1f}s"
            if orchestrator.next_restart_seconds > 0
            else "Next restart: -",
            f"Last error: {orchestrator.last_error or '-'}",
            f"Last started: {format_timestamp_ms(orchestrator.last_started_epoch_ms)}",
            f"Last exited: {format_timestamp_ms(orchestrator.last_exited_epoch_ms)}",
            "",
            f"Queue depth: {repo.queue_depth}",
            f"Queued: {repo.queued_count}",
            f"In flight: {repo.in_flight_count}",
            f"Stale runs: {repo.stale_run_count}",
            f"Pass rate: {summary.get('pass_rate_percent', 0.0):.1f}%",
            f"Duration p50/p95/p99: "
            f"{render_duration_ms(int(summary.get('duration_ms', {}).get('p50', 0)))} / "
            f"{render_duration_ms(int(summary.get('duration_ms', {}).get('p95', 0)))} / "
            f"{render_duration_ms(int(summary.get('duration_ms', {}).get('p99', 0)))}",
            f"Queue wait p95: "
            f"{render_duration_ms(int(summary.get('queue_wait_ms', {}).get('p95', 0)))}",
            f"Score wait p95: "
            f"{render_duration_ms(int(summary.get('score_wait_ms', {}).get('p95', 0)))}",
            f"Stale non-terminal: {int(activity.get('stale_non_terminal_count', 0))}",
            f"Oldest non-terminal age: "
            f"{render_duration_ms(int(activity.get('oldest_non_terminal_age_ms', 0)))}",
            "",
            f"Canary summary: {canary_status.get('latest_summary_path') or '-'}",
            f"Canary finished: {format_timestamp_ms(canary_status.get('completed_epoch_ms'))}",
            f"Canary freshness hours: {canary_status.get('freshness_hours', '-')}",
            f"Canary all passed: {canary_status.get('all_passed')}",
            f"Canary stale: {repo.canary_stale}",
            "",
            f"Runtime check passed: {repo.runtime_check_ok}",
            f"Runtime check finished: {format_timestamp_ms(repo.runtime_check_completed_epoch_ms)}",
            f"Runtime check message: {repo.runtime_check_message or '-'}",
            f"Last action result: {repo.last_action_result or '-'}",
            "",
            "Top failure causes",
            "=" * 18,
        ]
        if top_failure_causes:
            for item in top_failure_causes:
                lines.append(f"{item.get('code', '-')}: {item.get('count', 0)}")
        else:
            lines.append("none")
        lines.append("")
        lines.append("Active commands")
        lines.append("=" * 15)
        if repo.active_commands:
            for command in repo.active_commands:
                lines.append(f"{command.label}: {command.state}")
        else:
            lines.append("none")
        lines.append("")
        lines.append("Recent supervisor messages")
        lines.append("=" * 26)
        if repo.recent_messages:
            lines.extend(repo.recent_messages)
        else:
            lines.append("none")
        return "\n".join(lines)


def render_duration_ms(value: int) -> str:
    if value <= 0:
        return "-"
    if value < 1000:
        return f"{value}ms"
    return f"{value / 1000:.1f}s"


def format_timestamp_ms(value: int | float | None) -> str:
    if not value:
        return "-"
    seconds = float(value) / 1000.0
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(seconds))


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
            'default_model = "openai/gpt-5.4-mini"',
            "max_model_workers = 2",
            "max_score_workers = 2",
            "",
            "[[repo]]",
            'id = "secondary"',
            'name = "Another Harness"',
            f'root = "{sibling}"',
            "auto_start = false",
            'default_model = "anthropic/claude-sonnet-4"',
        ]
    ) + "\n"
