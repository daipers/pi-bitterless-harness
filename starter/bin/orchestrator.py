#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any

from harnesslib import (
    default_policy_path,
    load_policy,
    load_run_contract,
    resolve_execution_settings,
    resolve_retention_policy,
)

STATE_QUEUED = "queued"
STATE_CLAIMED = "claimed"
STATE_MODEL_RUNNING = "model_running"
STATE_MODEL_COMPLETE = "model_complete"
STATE_SCORE_PENDING = "score_pending"
STATE_SCORING = "scoring"
STATE_COMPLETE = "complete"
STATE_DONE = STATE_COMPLETE
STATE_FAILED = "failed"
STATE_CANCELLED = "cancelled"
TERMINAL_STATES = {STATE_COMPLETE, STATE_FAILED, STATE_CANCELLED}

HEARTBEAT_INTERVAL_MS = 2000
PENDING_STATES = {"running", STATE_MODEL_RUNNING, STATE_SCORING, STATE_CLAIMED}


PROFILE_LIMITS: dict[str, dict[str, int]] = {
    "strict": {
        "max_eval_commands": 100,
        "eval_command_timeout_seconds": 180,
        "model_timeout_seconds": 900,
        "score_timeout_seconds": 300,
        "max_run_wall_clock_seconds": 0,
        "max_model_workers": 0,
        "max_score_workers": 0,
        "max_transcript_bytes": 5_242_880,
        "max_pi_stderr_bytes": 1_048_576,
    },
    "offline": {
        "max_eval_commands": 80,
        "eval_command_timeout_seconds": 180,
        "model_timeout_seconds": 900,
        "score_timeout_seconds": 300,
        "max_run_wall_clock_seconds": 0,
        "max_model_workers": 0,
        "max_score_workers": 0,
        "max_transcript_bytes": 5_242_880,
        "max_pi_stderr_bytes": 1_048_576,
    },
    "networked": {
        "max_eval_commands": 140,
        "eval_command_timeout_seconds": 240,
        "model_timeout_seconds": 1200,
        "score_timeout_seconds": 420,
        "max_run_wall_clock_seconds": 0,
        "max_model_workers": 0,
        "max_score_workers": 0,
        "max_transcript_bytes": 5_242_880,
        "max_pi_stderr_bytes": 1_048_576,
    },
    "heavy_tools": {
        "max_eval_commands": 240,
        "eval_command_timeout_seconds": 420,
        "model_timeout_seconds": 1800,
        "score_timeout_seconds": 600,
        "max_run_wall_clock_seconds": 0,
        "max_model_workers": 1,
        "max_score_workers": 1,
        "max_transcript_bytes": 10_485_760,
        "max_pi_stderr_bytes": 2_097_152,
    },
    "capability": {
        "max_eval_commands": 180,
        "eval_command_timeout_seconds": 300,
        "model_timeout_seconds": 1200,
        "score_timeout_seconds": 420,
        "max_run_wall_clock_seconds": 0,
        "max_model_workers": 0,
        "max_score_workers": 0,
        "max_transcript_bytes": 8_388_608,
        "max_pi_stderr_bytes": 1_572_864,
    },
}


def now_ms() -> int:
    return int(time.time() * 1000)


def _to_positive_int(value: str | int | None, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() not in {"", "0", "false", "no", "off"}


def _normalize_state(value: str | None) -> str:
    value = (value or "").strip().lower()
    if not value:
        return ""
    return "complete" if value == "done" else value


def _now_iso() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _append_jsonl(path: pathlib.Path, payload: dict[str, Any]) -> None:
    payload.setdefault("ts", _now_iso())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _load_jsonl(path: pathlib.Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payloads: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            payloads.append(json.loads(raw_line))
        except json.JSONDecodeError:
            continue
    return payloads


def _run_state(run_dir: pathlib.Path) -> str:
    state_path = run_dir / "run.state"
    if not state_path.exists():
        return ""
    try:
        raw_state = state_path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""
    return _normalize_state(raw_state)


def _run_manifest_state(run_dir: pathlib.Path) -> str:
    manifest_path = run_dir / "outputs" / "run_manifest.json"
    if not manifest_path.exists():
        return ""
    try:
        return _normalize_state(
            json.loads(manifest_path.read_text(encoding="utf-8")).get("state", "")
        )
    except Exception:
        return ""


def _is_terminal(state: str) -> bool:
    return _normalize_state(state) in TERMINAL_STATES


def _is_locked(run_dir: pathlib.Path) -> bool:
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


def _read_json(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _run_state_file_or_manifest(run_dir: pathlib.Path) -> str:
    raw_state = _run_state(run_dir)
    if raw_state == STATE_COMPLETE:
        return STATE_COMPLETE
    if raw_state in PENDING_STATES:
        return raw_state if _is_locked(run_dir) else ""
    if raw_state and not _is_terminal(raw_state):
        return raw_state

    manifest_state = _run_manifest_state(run_dir)
    if manifest_state:
        return manifest_state
    return raw_state or ""


def _write_run_state(run_dir: pathlib.Path, state: str) -> None:
    try:
        run_dir.joinpath("run.state").write_text(f"{_normalize_state(state)}\n", encoding="utf-8")
    except Exception:
        pass


def _path_bytes(path: pathlib.Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _entry_payload_bytes(entry: dict[str, Any]) -> int:
    try:
        return len(json.dumps(entry, sort_keys=True).encode("utf-8"))
    except TypeError:
        return len(json.dumps({}, sort_keys=True).encode("utf-8"))


def _tree_bytes(path: pathlib.Path) -> int:
    total = 0
    if not path.exists():
        return 0
    if path.is_file():
        return _path_bytes(path)
    try:
        for child in path.rglob("*"):
            if child.is_file():
                total += _path_bytes(child)
    except Exception:
        pass
    return total


def _extract_queue_state(value: Any) -> str:
    return _normalize_state(str(value) if value is not None else "")


def _to_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _append_run_event(
    run_dir: pathlib.Path,
    phase: str,
    message: str,
    *,
    state_before: str | None = None,
    state_after: str | None = None,
    worker_id: str = "orchestrator",
    attempt: int = 0,
    queue_wait_ms: int = 0,
    model_wait_ms: int = 0,
    score_wait_ms: int = 0,
    timeout_deadline: int = 0,
    heartbeat_reason: str | None = None,
    error_code: str = "",
    failure_classification: str | None = None,
) -> None:
    payload = {
        "run_id": run_dir.name,
        "trace_id": run_dir.name,
        "phase": phase,
        "duration_ms": None,
        "error_code": error_code,
        "message": message,
        "state_before": state_before,
        "state_after": state_after,
        "worker_id": worker_id,
        "attempt": attempt,
        "timeout_deadline": timeout_deadline,
        "queue_wait_ms": queue_wait_ms,
        "model_wait_ms": model_wait_ms,
        "score_wait_ms": score_wait_ms,
        "heartbeat_reason": heartbeat_reason,
    }
    if failure_classification:
        payload["failure_classification"] = failure_classification
        payload["failure_class"] = failure_classification
    _append_jsonl(run_dir / "run-events.jsonl", payload)


def _queue_payload_type(kind: str) -> str:
    if kind == "run":
        return "run"
    if kind == "score":
        return "score"
    return "service"


def _is_queue_state_terminal(state: str) -> bool:
    normalized = _extract_queue_state(state)
    return normalized in {"complete", "failed", "cancelled", "error", "done"}


def _is_cancel_requested(run_dir: pathlib.Path) -> bool:
    return (run_dir / ".orchestrator-cancel").exists()


@dataclass
class OrchestratorConfig:
    script_dir: pathlib.Path
    runs_root: pathlib.Path
    run_queue_path: pathlib.Path
    score_queue_path: pathlib.Path
    max_model_workers: int
    max_score_workers: int
    model_retries: int
    score_retries: int
    queue_timeout_seconds: int
    poll_interval_seconds: float
    model_backoff_ms: int
    score_backoff_ms: int
    max_run_wall_clock_seconds: int
    retention_maintenance_interval_seconds: float
    retention_manual_safe: bool


@dataclass
class WorkerContext:
    run_dir: pathlib.Path
    process: subprocess.Popen[Any]
    attempt: int
    started_ms: int
    queue_wait_ms: int
    worker_id: str
    deadline_ms: int
    profile: str
    state: str


class Orchestrator:
    def __init__(self, config: OrchestratorConfig, *, max_duration_seconds: int = 0):
        self.config = config
        self.max_duration_ms = max_duration_seconds * 1000
        self.started_ms = now_ms()
        self._running_model: dict[str, WorkerContext] = {}
        self._running_score: dict[str, WorkerContext] = {}
        self._model_attempts: dict[str, int] = {}
        self._score_attempts: dict[str, int] = {}
        self._model_retry_wait_until_ms: dict[str, int] = {}
        self._score_retry_wait_until_ms: dict[str, int] = {}
        self._run_queue_wait_start_ms: dict[str, int] = {}
        self._score_queue_wait_start_ms: dict[str, int] = {}
        self._run_attempt_queue_wait_seen: dict[str, int] = {}
        self._score_attempt_queue_wait_seen: dict[str, int] = {}
        self._run_profile_cache: dict[str, str] = {}
        self._run_retention_cache: dict[str, dict[str, Any]] = {}
        self._global_retention_cache: dict[str, Any] | None = None
        self._policy_cache: dict[str, dict[str, Any]] = {}
        self._run_state_cache: dict[str, str] = {}
        self._last_heartbeat_ms: dict[str, int] = {}
        self._last_retention_ms: int = 0
        self._stop = False
        self._shutdown_reason = ""

        self._run_queue_state_cache: dict[str, int] = {}
        self._score_queue_state_cache: dict[str, int] = {}

    @classmethod
    def from_environment(cls, config: OrchestratorConfig | None = None) -> Orchestrator:
        if config is None:
            script_dir = pathlib.Path(__file__).resolve().parent
            repo_root = script_dir.parent
            runs_root = pathlib.Path(
                os.environ.get("HARNESS_RUN_ROOT", repo_root / "runs")
            ).resolve()
            orchestrator_root = runs_root / ".orchestrator"
            config = OrchestratorConfig(
                script_dir=script_dir,
                runs_root=runs_root,
                run_queue_path=pathlib.Path(
                    os.environ.get(
                        "HARNESS_RUN_QUEUE_PATH", str(orchestrator_root / "run_queue.jsonl")
                    )
                ).resolve(),
                score_queue_path=pathlib.Path(
                    os.environ.get(
                        "HARNESS_SCORE_QUEUE_PATH",
                        str(orchestrator_root / "score_queue.jsonl"),
                    )
                ).resolve(),
                max_model_workers=_to_positive_int(
                    os.environ.get("HARNESS_ORCHESTRATOR_MAX_MODEL_WORKERS", "2"), default=2
                ),
                max_score_workers=_to_positive_int(
                    os.environ.get("HARNESS_ORCHESTRATOR_MAX_SCORE_WORKERS", "2"), default=2
                ),
                model_retries=_to_positive_int(
                    os.environ.get("HARNESS_ORCHESTRATOR_MODEL_RETRIES", "2"), default=2
                ),
                score_retries=_to_positive_int(
                    os.environ.get("HARNESS_ORCHESTRATOR_SCORE_RETRIES", "2"), default=2
                ),
                queue_timeout_seconds=_to_positive_int(
                    os.environ.get("HARNESS_ORCHESTRATOR_QUEUE_TIMEOUT_SECONDS", "0"),
                    default=0,
                ),
                poll_interval_seconds=max(
                    0.25,
                    float(os.environ.get("HARNESS_ORCHESTRATOR_POLL_SECONDS", "1.0")),
                ),
                model_backoff_ms=_to_positive_int(
                    os.environ.get("HARNESS_ORCHESTRATOR_MODEL_BACKOFF_MS", "800"), default=800
                ),
                score_backoff_ms=_to_positive_int(
                    os.environ.get("HARNESS_ORCHESTRATOR_SCORE_BACKOFF_MS", "1200"), default=1200
                ),
                max_run_wall_clock_seconds=_to_positive_int(
                    os.environ.get("HARNESS_ORCHESTRATOR_RUN_WALL_CLOCK_SECONDS", "0"), default=0
                ),
                retention_maintenance_interval_seconds=max(
                    0.25,
                    float(os.environ.get("HARNESS_ORCHESTRATOR_RETENTION_INTERVAL_SECONDS", "10")),
                ),
                retention_manual_safe=_env_flag("HARNESS_ORCHESTRATOR_RETENTION_MANUAL_SAFE"),
            )
        return cls(config)

    @property
    def runs_root(self) -> pathlib.Path:
        return self.config.runs_root

    @property
    def timed_out(self) -> bool:
        if self.max_duration_ms <= 0:
            return False
        return now_ms() - self.started_ms > self.max_duration_ms

    def _run_dir_iter(self) -> list[pathlib.Path]:
        if not self.runs_root.exists():
            return []
        return sorted(
            p for p in self.runs_root.iterdir() if p.is_dir() and not p.name.startswith(".")
        )

    def _resolve_profile_and_caps(self, run_dir: pathlib.Path) -> tuple[str, dict[str, int]]:
        cached = self._run_profile_cache.get(run_dir.name)
        if cached is not None:
            profile = cached
        else:
            profile = "strict"
            try:
                contract = _read_json(run_dir / "run.contract.json")
                if contract:
                    settings = resolve_execution_settings(contract, profile_override=None)
                    profile = settings.get("execution_profile", "strict")
            except Exception:
                try:
                    run_contract = load_run_contract(run_dir / "run.contract.json")
                    settings = resolve_execution_settings(run_contract, profile_override=None)
                    profile = settings.get("execution_profile", "strict")
                except Exception:
                    profile = "strict"
            self._run_profile_cache[run_dir.name] = profile

        caps = dict(PROFILE_LIMITS.get(profile, PROFILE_LIMITS["strict"]))
        caps_env = {
            "max_eval_commands": os.environ.get("HARNESS_ORCHESTRATOR_MAX_EVAL_COMMANDS"),
            "eval_command_timeout_seconds": os.environ.get(
                "HARNESS_ORCHESTRATOR_EVAL_COMMAND_TIMEOUT_SECONDS"
            ),
            "model_timeout_seconds": os.environ.get("HARNESS_ORCHESTRATOR_MODEL_TIMEOUT_SECONDS"),
            "score_timeout_seconds": os.environ.get("HARNESS_ORCHESTRATOR_SCORE_TIMEOUT_SECONDS"),
            "max_run_wall_clock_seconds": os.environ.get(
                "HARNESS_ORCHESTRATOR_MAX_RUN_WALL_CLOCK_SECONDS"
            ),
            "max_model_workers": os.environ.get("HARNESS_ORCHESTRATOR_MAX_MODEL_WORKERS"),
            "max_score_workers": os.environ.get("HARNESS_ORCHESTRATOR_MAX_SCORE_WORKERS"),
            "max_transcript_bytes": os.environ.get("HARNESS_ORCHESTRATOR_MAX_TRANSCRIPT_BYTES"),
            "max_pi_stderr_bytes": os.environ.get("HARNESS_ORCHESTRATOR_MAX_PI_STDERR_BYTES"),
        }
        for key, raw_value in caps_env.items():
            if raw_value is None or raw_value == "":
                continue
            caps[key] = _to_positive_int(raw_value, default=caps[key])

        service_default = self.config.max_run_wall_clock_seconds
        if service_default > 0:
            caps["max_run_wall_clock_seconds"] = service_default

        return profile, caps

    def _resolve_retention_policy_for_run(self, run_dir: pathlib.Path) -> dict[str, Any]:
        cached = self._run_retention_cache.get(run_dir.name)
        if cached is not None:
            return cached

        policy_path = default_policy_path("strict")
        settings: dict[str, Any] | None = None
        contract = _read_json(run_dir / "run.contract.json")
        if contract:
            try:
                settings = resolve_execution_settings(contract, profile_override=None)
            except Exception:
                settings = None
        if settings is None:
            try:
                run_contract = load_run_contract(run_dir / "run.contract.json")
                settings = resolve_execution_settings(run_contract, profile_override=None)
            except Exception:
                settings = None
        if settings and settings.get("policy_path"):
            policy_path = settings["policy_path"]

        policy = self._policy_cache.get(policy_path)
        if policy is None:
            try:
                policy = load_policy(policy_path, repo_root=self.runs_root.parent)
            except Exception:
                try:
                    policy = load_policy(
                        default_policy_path("strict"), repo_root=self.runs_root.parent
                    )
                except Exception:
                    policy = {"retention": {}}
            self._policy_cache[policy_path] = policy

        retention = resolve_retention_policy(
            policy.get("retention"), policy_env_prefix="HARNESS_RETENTION"
        )
        self._run_retention_cache[run_dir.name] = retention
        return retention

    def _resolve_default_retention_policy(self) -> dict[str, Any]:
        if self._global_retention_cache is not None:
            return self._global_retention_cache
        try:
            policy = load_policy(default_policy_path("strict"), repo_root=self.runs_root.parent)
            retention = resolve_retention_policy(
                policy.get("retention"), policy_env_prefix="HARNESS_RETENTION"
            )
        except Exception:
            retention = resolve_retention_policy({}, policy_env_prefix="HARNESS_RETENTION")
        self._global_retention_cache = retention
        return retention

    def _retry_window(self, run_dir: pathlib.Path, score: bool = False) -> bool:
        retry_map = self._score_retry_wait_until_ms if score else self._model_retry_wait_until_ms
        wait_ms = retry_map.get(run_dir.name)
        if wait_ms is None:
            return True
        return now_ms() >= wait_ms

    def _can_retry_model(self, run_dir: pathlib.Path) -> bool:
        return self._model_attempts.get(run_dir.name, 0) < self.config.model_retries

    def _can_retry_score(self, run_dir: pathlib.Path) -> bool:
        return self._score_attempts.get(run_dir.name, 0) < self.config.score_retries

    def _effective_model_cap(self, run_dir: pathlib.Path) -> int:
        _, caps = self._resolve_profile_and_caps(run_dir)
        configured = _to_positive_int(caps.get("max_model_workers"), default=0)
        if configured <= 0:
            return self.config.max_model_workers
        return min(self.config.max_model_workers, configured)

    def _effective_score_cap(self, run_dir: pathlib.Path) -> int:
        _, caps = self._resolve_profile_and_caps(run_dir)
        configured = _to_positive_int(caps.get("max_score_workers"), default=0)
        if configured <= 0:
            return self.config.max_score_workers
        return min(self.config.max_score_workers, configured)

    def _running_count_for_profile(self, bucket: dict[str, WorkerContext], profile: str) -> int:
        return sum(1 for ctx in bucket.values() if ctx.profile == profile)

    def _mark_run_state(self, run_dir: pathlib.Path, state: str) -> None:
        normalized = _normalize_state(state)
        if not normalized:
            return
        self._run_state_cache[run_dir.name] = normalized
        _write_run_state(run_dir, normalized)

    def _queue_wait_ms(self, run_dir: pathlib.Path, *, score: bool = False) -> int:
        key = "score" if score else "run"
        mapping = self._score_queue_wait_start_ms if score else self._run_queue_wait_start_ms
        started = mapping.get(run_dir.name)
        if started is None:
            payload = self._load_queue_state(run_dir.name, kind=key)
            if payload:
                queued_at = _to_int(payload.get("queued_at_ms"), default=0)
                if queued_at > 0:
                    mapping[run_dir.name] = queued_at
                    if score:
                        self._score_attempt_queue_wait_seen[run_dir.name] = _to_int(
                            payload.get("attempt"),
                            default=0,
                        )
                    else:
                        self._run_attempt_queue_wait_seen[run_dir.name] = _to_int(
                            payload.get("attempt"),
                            default=0,
                        )
                    started = queued_at
                    if started is None:
                        return 0
        return max(0, now_ms() - started)

    def _queue_timeout_exceeded(self, run_dir: pathlib.Path, *, score: bool = False) -> bool:
        if self.config.queue_timeout_seconds <= 0:
            return False
        queue_kind = "score" if score else "run"
        payload = self._load_queue_state(run_dir.name, kind=queue_kind)
        if not payload:
            return False
        state = _extract_queue_state(payload.get("state"))
        if _is_queue_state_terminal(state):
            return False
        queued_at = _to_int(payload.get("queued_at_ms"), default=0)
        if queued_at <= 0:
            queued_at = _to_int(payload.get("ts_ms"), default=0)
        if queued_at <= 0:
            return False
        return now_ms() - queued_at >= self.config.queue_timeout_seconds * 1000

    def _mark_queue_retry(
        self, run_dir: pathlib.Path, *, score: bool, attempt: int, worker_id: str, failure: str
    ) -> None:
        kind = "score" if score else "run"
        self._mark_queue_state(
            run_dir,
            kind=kind,
            state=STATE_QUEUED,
            attempt=attempt,
            worker_id=worker_id,
            extra={"failure_classification": failure},
        )

    def _handle_queue_timeout(self, run_dir: pathlib.Path, *, score: bool) -> bool:
        can_retry = self._can_retry_score(run_dir) if score else self._can_retry_model(run_dir)
        if can_retry:
            if score:
                self._score_attempts[run_dir.name] = (
                    max(self._score_attempts.get(run_dir.name, 0), 1) + 1
                )
            else:
                self._model_attempts[run_dir.name] = (
                    max(self._model_attempts.get(run_dir.name, 0), 1) + 1
                )
            attempt = (
                self._score_attempts.get(run_dir.name, 1)
                if score
                else self._model_attempts.get(run_dir.name, 1)
            )
            retry_map = (
                self._score_retry_wait_until_ms if score else self._model_retry_wait_until_ms
            )
            wait_ms = self.config.score_backoff_ms if score else self.config.model_backoff_ms
            retry_map[run_dir.name] = now_ms() + wait_ms
            self._mark_queue_retry(
                run_dir,
                score=score,
                attempt=attempt,
                worker_id="orchestrator-timeout",
                failure="orchestrator_queue_timeout",
            )
            if score:
                self._mark_run_state(run_dir, STATE_SCORE_PENDING)
                _append_run_event(
                    run_dir,
                    "score_queue_timeout",
                    "score queue wait exceeded timeout; scheduling retry",
                    worker_id="orchestrator",
                    attempt=attempt,
                    state_before=STATE_SCORE_PENDING,
                    state_after=STATE_SCORE_PENDING,
                    score_wait_ms=max(0, self._queue_wait_ms(run_dir, score=True)),
                    heartbeat_reason="queue_wait_timeout",
                    failure_classification="orchestrator_queue_timeout",
                    error_code="orchestrator_queue_timeout",
                )
            else:
                self._mark_run_state(run_dir, STATE_QUEUED)
                _append_run_event(
                    run_dir,
                    "run_queue_timeout",
                    "run queue wait exceeded timeout; scheduling retry",
                    worker_id="orchestrator",
                    attempt=attempt,
                    state_before=STATE_QUEUED,
                    state_after=STATE_QUEUED,
                    queue_wait_ms=max(0, self._queue_wait_ms(run_dir, score=False)),
                    heartbeat_reason="queue_wait_timeout",
                    failure_classification="orchestrator_queue_timeout",
                    error_code="orchestrator_queue_timeout",
                )
            return True

        if score:
            attempt = _to_int(
                (self._load_queue_state(run_dir.name, kind="score") or {}).get("attempt"), default=1
            )
            self._mark_queue_state(
                run_dir,
                kind="score",
                state=STATE_FAILED,
                attempt=attempt,
                worker_id="orchestrator-timeout",
                extra={"failure_classification": "orchestrator_queue_timeout"},
            )
            self._mark_run_state(run_dir, STATE_FAILED)
            _append_run_event(
                run_dir,
                "score_queue_timeout",
                "score queue wait exceeded timeout; failing run",
                worker_id="orchestrator",
                attempt=attempt,
                state_before=STATE_SCORE_PENDING,
                state_after=STATE_FAILED,
                score_wait_ms=self._queue_wait_ms(run_dir, score=True),
                heartbeat_reason="queue_wait_timeout",
                failure_classification="orchestrator_queue_timeout",
                error_code="orchestrator_queue_timeout",
            )
        else:
            attempt = _to_int(
                (self._load_queue_state(run_dir.name, kind="run") or {}).get("attempt"), default=1
            )
            self._mark_queue_state(
                run_dir,
                kind="run",
                state=STATE_FAILED,
                attempt=attempt,
                worker_id="orchestrator-timeout",
                extra={"failure_classification": "orchestrator_queue_timeout"},
            )
            self._mark_run_state(run_dir, STATE_FAILED)
            _append_run_event(
                run_dir,
                "run_queue_timeout",
                "run queue wait exceeded timeout; failing run",
                worker_id="orchestrator",
                attempt=attempt,
                state_before=STATE_QUEUED,
                state_after=STATE_FAILED,
                queue_wait_ms=self._queue_wait_ms(run_dir, score=False),
                heartbeat_reason="queue_wait_timeout",
                failure_classification="orchestrator_queue_timeout",
                error_code="orchestrator_queue_timeout",
            )
        return False

    def _clear_run_caches(self, run_id: str) -> None:
        self._run_profile_cache.pop(run_id, None)
        self._run_retention_cache.pop(run_id, None)
        self._run_state_cache.pop(run_id, None)
        self._run_queue_wait_start_ms.pop(run_id, None)
        self._score_queue_wait_start_ms.pop(run_id, None)
        self._run_attempt_queue_wait_seen.pop(run_id, None)
        self._score_attempt_queue_wait_seen.pop(run_id, None)
        self._model_attempts.pop(run_id, None)
        self._score_attempts.pop(run_id, None)
        self._model_retry_wait_until_ms.pop(run_id, None)
        self._score_retry_wait_until_ms.pop(run_id, None)
        self._run_queue_state_cache.pop(run_id, None)
        self._score_queue_state_cache.pop(run_id, None)

    def _should_purge_run_safely(self, run_dir: pathlib.Path) -> bool:
        if self.config.retention_manual_safe:
            if run_dir.name in self._running_model or run_dir.name in self._running_score:
                return False
            if _is_locked(run_dir):
                return False
            return True
        state = self._effective_state(run_dir)
        return _is_terminal(state)

    def _write_retention_service_event(self, payload: dict[str, Any]) -> None:
        event = {
            "type": "orchestrator",
            "kind": "service",
            "event": "retention",
            "ts_ms": now_ms(),
        }
        event.update(payload)
        _append_jsonl(self.config.run_queue_path, event)

    def _purge_queue_file(
        self, kind: str, keep_payloads: dict[str, dict[str, Any]]
    ) -> tuple[int, int]:
        path = self.config.run_queue_path if kind == "run" else self.config.score_queue_path
        if not path.exists():
            return 0, 0
        keep_ids = set(keep_payloads)
        entries = _load_jsonl(path)
        latest_by_run: dict[str, dict[str, Any]] = {}
        service_entries: list[dict[str, Any]] = []
        for entry in entries:
            entry_kind = str(entry.get("kind") or "").strip()
            if entry_kind not in {"run", "score"}:
                service_entries.append(entry)
                continue
            entry_run_id = str(entry.get("run_id", "")).strip()
            if not entry_run_id:
                continue
            latest_by_run[entry_run_id] = entry

        kept_payloads: list[dict[str, Any]] = list(service_entries)
        kept_payloads.extend(
            keep_payloads[run_id] for run_id in sorted(keep_ids) if run_id in keep_payloads
        )
        removed_payloads = [
            payload for run_id, payload in latest_by_run.items() if run_id not in keep_ids
        ]
        removed_bytes = sum(_entry_payload_bytes(payload) for payload in removed_payloads)
        try:
            path.write_text("")
            for payload in sorted(
                kept_payloads,
                key=lambda item: _to_int(item.get("ts_ms"), default=0),
            ):
                _append_jsonl(path, payload)
        except Exception:
            return 0, 0
        return len(removed_payloads), removed_bytes

    def _apply_run_retention(self, *, now_epoch_ms: int, policy: dict[str, Any]) -> dict[str, int]:
        run_scope = policy.get("run", {})
        ttl_days = _to_int(run_scope.get("ttl_days"), default=0)
        max_count = _to_int(run_scope.get("max_count"), default=0)
        max_bytes = _to_int(run_scope.get("max_bytes"), default=0)

        retained_runs = 0
        purged_runs = 0
        candidates: list[tuple[pathlib.Path, int, int, dict[str, Any]]] = []
        run_dirs = self._run_dir_iter()
        for run_dir in run_dirs:
            if not self._should_purge_run_safely(run_dir):
                continue
            retention = self._resolve_retention_policy_for_run(run_dir).get("run", {})
            run_ttl_days = max(0, int(retention.get("ttl_days", ttl_days)))
            candidate_ttl_ms = run_ttl_days * 24 * 60 * 60 * 1000
            ts_ms = self._run_state_timestamp(run_dir)
            if candidate_ttl_ms > 0 and now_epoch_ms - ts_ms > candidate_ttl_ms:
                self._clear_run_caches(run_dir.name)
                try:
                    shutil.rmtree(run_dir)
                except OSError:
                    pass
                purged_runs += 1
                continue
            size_bytes = _tree_bytes(run_dir)
            candidates.append((run_dir, ts_ms, size_bytes, retention))

        if max_count > 0 and len(candidates) > max_count:
            candidates = sorted(candidates, key=lambda item: item[1])
            for run_dir, _, _, _ in candidates[: len(candidates) - max_count]:
                self._clear_run_caches(run_dir.name)
                try:
                    shutil.rmtree(run_dir)
                except OSError:
                    pass
                purged_runs += 1
            candidates = candidates[len(candidates) - max_count :]

        total_bytes = sum(size for _, _, size, _ in candidates)
        if max_bytes > 0 and total_bytes > max_bytes:
            candidates = sorted(candidates, key=lambda item: (item[1], item[0].name))
            while candidates and total_bytes > max_bytes:
                run_dir, _, size, _ = candidates.pop(0)
                total_bytes -= size
                self._clear_run_caches(run_dir.name)
                try:
                    shutil.rmtree(run_dir)
                except OSError:
                    pass
                purged_runs += 1

        retained_runs = len(candidates)
        return {
            "retained_runs": retained_runs,
            "purged_runs": purged_runs,
        }

    def _apply_queue_retention(
        self, *, now_epoch_ms: int, policy: dict[str, Any]
    ) -> dict[str, int]:
        queue_scope = policy.get("queue", {})
        ttl_days = _to_int(queue_scope.get("ttl_days"), default=0)
        ttl_ms = ttl_days * 24 * 60 * 60 * 1000
        max_count = _to_int(queue_scope.get("max_count"), default=0)
        max_bytes = _to_int(queue_scope.get("max_bytes"), default=0)
        purged_queue_items = 0
        purged_queue_bytes = 0
        kept_count = 0

        for kind in ("run", "score"):
            entries = self._load_queue_entries(kind)
            keep_payloads: dict[str, dict[str, Any]] = {}
            candidates: list[tuple[int, str, int, dict[str, Any]]] = []
            for run_id, entry in entries.items():
                state = _extract_queue_state(entry.get("state"))
                raw_run_dir = str(entry.get("run_dir", "")).strip()
                run_dir = pathlib.Path(raw_run_dir) if raw_run_dir else pathlib.Path()
                missing_run = not raw_run_dir or not run_dir.exists()
                terminal = _is_queue_state_terminal(state) or missing_run

                if self.config.retention_manual_safe:
                    safe = self._should_purge_run_safely(run_dir) or missing_run
                    if not safe:
                        keep_payloads[run_id] = entry
                        continue
                else:
                    if not terminal:
                        keep_payloads[run_id] = entry
                        continue

                ts_ms = _to_int(
                    entry.get("queued_at_ms"), default=_to_int(entry.get("ts_ms"), default=0)
                )
                payload_size = _entry_payload_bytes(entry)
                if terminal and ttl_ms > 0 and now_epoch_ms - ts_ms > ttl_ms:
                    continue
                candidates.append((ts_ms, run_id, payload_size, entry))

            if max_count > 0 and len(candidates) > max_count:
                candidates = sorted(candidates, key=lambda item: (item[0], item[1]), reverse=True)
                candidates = candidates[:max_count]

            if max_bytes > 0:
                candidates = sorted(candidates, key=lambda item: (item[0], item[1]), reverse=True)
                total_bytes = sum(size for _, _, size, _ in candidates)
                while total_bytes > max_bytes and candidates:
                    _, _, size, _ = candidates.pop()
                    total_bytes -= size

            for _, run_id, _, payload in candidates:
                keep_payloads[run_id] = payload

            removed, removed_payload_bytes = self._purge_queue_file(kind, keep_payloads)
            purged_queue_items += removed
            purged_queue_bytes += removed_payload_bytes
            kept_count += len(keep_payloads)

        return {
            "retained_queue_items": kept_count,
            "purged_queue_items": purged_queue_items,
            "purged_queue_bytes": purged_queue_bytes,
        }

    def _apply_artifact_retention(
        self, *, now_epoch_ms: int, policy: dict[str, Any]
    ) -> dict[str, int]:
        artifact_scope = policy.get("artifact", {})
        ttl_days = _to_int(artifact_scope.get("ttl_days"), default=0)
        ttl_ms = ttl_days * 24 * 60 * 60 * 1000
        max_count = _to_int(artifact_scope.get("max_count"), default=0)
        max_bytes = _to_int(artifact_scope.get("max_bytes"), default=0)
        candidate_files: list[tuple[pathlib.Path, int, int]] = []
        for run_dir in self._run_dir_iter():
            if not self._should_purge_run_safely(run_dir):
                continue
            score_dir = run_dir / "score"
            if not score_dir.exists():
                continue
            for score_file in score_dir.iterdir():
                if not score_file.is_file():
                    continue
                if not score_file.name.startswith("eval-"):
                    continue
                ts_ms = _to_int(int(score_file.stat().st_mtime * 1000))
                if ttl_ms > 0 and now_epoch_ms - ts_ms > ttl_ms:
                    try:
                        score_file.unlink()
                    except OSError:
                        pass
                    continue
                candidate_files.append((score_file, ts_ms, _path_bytes(score_file)))

        candidate_files = [
            (path, ts, size) for (path, ts, size) in candidate_files if path.exists()
        ]
        candidate_files.sort(key=lambda item: item[1], reverse=True)
        retained_artifacts = len(candidate_files)
        purged_artifacts = 0
        if max_count > 0 and retained_artifacts > max_count:
            for path, _, _ in candidate_files[max_count:]:
                try:
                    path.unlink()
                    purged_artifacts += 1
                except OSError:
                    pass
            candidate_files = candidate_files[:max_count]
            retained_artifacts = len(candidate_files)

        if max_bytes > 0:
            total_bytes = sum(size for _, _, size in candidate_files)
            idx = len(candidate_files) - 1
            while total_bytes > max_bytes and idx >= 0:
                path, _, size = candidate_files[idx]
                try:
                    path.unlink()
                    purged_artifacts += 1
                    total_bytes -= size
                except OSError:
                    pass
                candidate_files.pop(idx)
                idx -= 1
        retained_artifacts = len(candidate_files)
        return {
            "retained_artifacts": retained_artifacts,
            "purged_artifacts": purged_artifacts,
        }

    def _run_retention_maintenance(
        self,
        *,
        reason: str = "periodic",
        force: bool = False,
    ) -> dict[str, int] | None:
        if self.config.retention_maintenance_interval_seconds <= 0:
            return None
        now_epoch_ms = now_ms()
        interval_ms = int(self.config.retention_maintenance_interval_seconds * 1000)
        if (
            not force
            and self._last_retention_ms
            and now_epoch_ms - self._last_retention_ms < interval_ms
        ):
            return None

        self._last_retention_ms = now_epoch_ms
        policy = self._resolve_default_retention_policy()
        run_metrics = self._apply_run_retention(now_epoch_ms=now_epoch_ms, policy=policy)
        queue_metrics = self._apply_queue_retention(now_epoch_ms=now_epoch_ms, policy=policy)
        artifact_metrics = self._apply_artifact_retention(now_epoch_ms=now_epoch_ms, policy=policy)
        retained_score_artifacts = artifact_metrics["retained_artifacts"]
        purged_score_artifacts = artifact_metrics["purged_artifacts"]
        metrics: dict[str, int] = {
            "retained_runs": run_metrics["retained_runs"],
            "purged_runs": run_metrics["purged_runs"],
            "retained_score_artifacts": retained_score_artifacts,
            "purged_score_artifacts": purged_score_artifacts,
            "retained_artifacts": artifact_metrics["retained_artifacts"],
            "purged_artifacts": artifact_metrics["purged_artifacts"],
            "purged_queue_items": queue_metrics["purged_queue_items"],
            "retained_queue_items": queue_metrics["retained_queue_items"],
            "purged_queue_bytes": queue_metrics["purged_queue_bytes"],
        }
        if any(
            value > 0
            for value in (
                metrics["purged_runs"],
                metrics["purged_queue_items"],
                metrics["purged_score_artifacts"],
            )
        ):
            self._write_retention_service_event(
                {
                    "retention_reason": reason,
                    **metrics,
                    "retention_scope": {
                        "run": policy.get("run", {}),
                        "queue": policy.get("queue", {}),
                        "artifact": policy.get("artifact", {}),
                    },
                }
            )
        return metrics

    def _queue_run(self, run_dir: pathlib.Path, *, attempt: int, worker_id: str) -> None:
        self._mark_queue_state(
            run_dir,
            kind="run",
            state=STATE_QUEUED,
            attempt=attempt,
            worker_id=worker_id,
            extra={"orchestration_state": STATE_QUEUED},
        )

    def _queue_score(self, run_dir: pathlib.Path, *, attempt: int, worker_id: str) -> None:
        self._mark_queue_state(
            run_dir,
            kind="score",
            state=STATE_QUEUED,
            attempt=attempt,
            worker_id=worker_id,
            extra={"max_attempts": self.config.score_retries},
        )

    def _run_state_timestamp(self, run_dir: pathlib.Path) -> int:
        marker = run_dir / "run.state"
        if not marker.exists():
            try:
                return int(run_dir.stat().st_mtime * 1000)
            except Exception:
                return now_ms()
        try:
            return int(marker.stat().st_mtime * 1000)
        except Exception:
            return now_ms()

    def _load_queue_entries(self, kind: str) -> dict[str, dict[str, Any]]:
        path = self.config.run_queue_path if kind == "run" else self.config.score_queue_path
        payloads: dict[str, dict[str, Any]] = {}
        for entry in _load_jsonl(path):
            if entry.get("kind") != kind:
                continue
            run_id = str(entry.get("run_id", "")).strip()
            if not run_id:
                continue
            payloads[run_id] = dict(entry)
        return payloads

    def _load_queue_state(self, run_id: str, kind: str) -> dict[str, Any] | None:
        entries = self._load_queue_entries(kind)
        return entries.get(run_id)

    def _queue_state(self, run_id: str, kind: str) -> str:
        payload = self._load_queue_state(run_id, kind)
        if not payload:
            return ""
        return _extract_queue_state(payload.get("state"))

    def _mark_queue_state(
        self,
        run_dir: pathlib.Path,
        *,
        kind: str,
        state: str,
        attempt: int,
        worker_id: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "type": _queue_payload_type(kind),
            "kind": kind,
            "run_id": run_dir.name,
            "run_dir": str(run_dir.resolve()),
            "attempt": attempt,
            "state": state,
            "worker_id": worker_id,
            "ts_ms": now_ms(),
        }
        if state == STATE_QUEUED:
            payload["queued_at_ms"] = now_ms()
        if extra:
            payload.update(extra)
        if kind == "run":
            if self._run_attempt_queue_wait_seen.get(run_dir.name) != attempt:
                self._run_queue_wait_start_ms[run_dir.name] = now_ms()
                self._run_attempt_queue_wait_seen[run_dir.name] = attempt
        else:
            if self._score_attempt_queue_wait_seen.get(run_dir.name) != attempt:
                self._score_queue_wait_start_ms[run_dir.name] = now_ms()
                self._score_attempt_queue_wait_seen[run_dir.name] = attempt
        target = self.config.run_queue_path if kind == "run" else self.config.score_queue_path
        _append_jsonl(target, payload)

    def _effective_state(self, run_dir: pathlib.Path) -> str:
        state = _run_state_file_or_manifest(run_dir)
        if state == STATE_COMPLETE and _run_manifest_state(run_dir) == STATE_COMPLETE:
            return STATE_COMPLETE

        if state == "running":
            return state if _is_locked(run_dir) else "partial"
        if state in PENDING_STATES and not _is_locked(run_dir):
            return "partial"
        return state or _run_manifest_state(run_dir)

    def _discover_model_queue(self) -> list[pathlib.Path]:
        candidates: list[pathlib.Path] = []
        queue_entries = self._load_queue_entries("run")
        queued_run_ids: set[str] = set()
        for run_id, entry in queue_entries.items():
            if self._is_queue_state_terminal(_extract_queue_state(entry.get("state"))):
                continue
            run_dir = pathlib.Path(str(entry.get("run_dir", "")))
            if not run_dir.exists() or not run_dir.is_dir():
                continue
            if not (run_dir / "task.md").exists() or not (run_dir / "RUN.md").exists():
                continue
            if _is_cancel_requested(run_dir):
                continue

            state = self._effective_state(run_dir)
            manifest_state = _run_manifest_state(run_dir)
            if _is_terminal(state) or _is_terminal(manifest_state):
                continue

            queue_state = _extract_queue_state(entry.get("state"))
            if queue_state in {STATE_CLAIMED, STATE_MODEL_RUNNING}:
                if _is_locked(run_dir):
                    continue
                _append_run_event(
                    run_dir,
                    "state_repair",
                    "detected stale in-flight state; marking as queued for recovery",
                    state_before=state or STATE_CLAIMED,
                    state_after=STATE_QUEUED,
                    worker_id="orchestrator",
                    failure_classification="orchestrator_worker_unavailable",
                )
                self._mark_run_state(run_dir, STATE_QUEUED)
                self._mark_queue_state(
                    run_dir,
                    kind="run",
                    state=STATE_QUEUED,
                    attempt=self._model_attempts.get(
                        run_dir.name, _to_int(entry.get("attempt"), default=1)
                    ),
                    worker_id="orchestrator-recover",
                )
                queued_run_ids.add(run_id)
                candidates.append(run_dir)
                continue

            if self._queue_timeout_exceeded(run_dir, score=False):
                self._handle_queue_timeout(run_dir, score=False)
                continue

            if not queue_state or not self._is_queue_state_terminal(queue_state):
                if state in {STATE_SCORE_PENDING, STATE_MODEL_COMPLETE}:
                    continue
                queued_run_ids.add(run_id)
                candidates.append(run_dir)

        for run_dir in self._run_dir_iter():
            if run_dir.name in queued_run_ids:
                continue
            if not (run_dir / "task.md").exists() or not (run_dir / "RUN.md").exists():
                continue
            if _is_cancel_requested(run_dir):
                continue
            state = self._effective_state(run_dir)
            manifest_state = _run_manifest_state(run_dir)
            if _is_terminal(state) or _is_terminal(manifest_state):
                continue
            if state in {STATE_SCORE_PENDING, STATE_MODEL_COMPLETE}:
                continue
            if state in {STATE_SCORING, STATE_MODEL_RUNNING, STATE_CLAIMED}:
                if _is_locked(run_dir):
                    continue
                _append_run_event(
                    run_dir,
                    "state_repair",
                    "detected stale run/task state without queue entry; re-queueing",
                    state_before=state,
                    state_after=STATE_QUEUED,
                    worker_id="orchestrator",
                    failure_classification="orchestrator_worker_unavailable",
                )
            if state in {STATE_QUEUED, "partial", "", STATE_COMPLETE}:
                attempt = max(1, self._model_attempts.get(run_dir.name, 1))
                self._queue_run(run_dir, attempt=attempt, worker_id="orchestrator")
                queued_run_ids.add(run_dir.name)
                candidates.append(run_dir)
        dedup: dict[str, pathlib.Path] = {}
        for run_dir in candidates:
            dedup[run_dir.name] = run_dir
        return list(dedup.values())

    def _discover_score_candidates(self) -> list[pathlib.Path]:
        candidates: list[pathlib.Path] = []
        queue_entries = self._load_queue_entries("score")
        discovered: set[str] = set()
        for run_id, entry in queue_entries.items():
            queue_state = _extract_queue_state(entry.get("state"))
            if _is_queue_state_terminal(queue_state):
                continue
            run_dir = pathlib.Path(str(entry.get("run_dir", "")))
            if not run_dir.exists() or not run_dir.is_dir():
                continue
            if not (run_dir / "task.md").exists() or not (run_dir / "RUN.md").exists():
                continue
            if _is_cancel_requested(run_dir):
                continue

            if queue_state in {STATE_SCORING, STATE_CLAIMED}:
                if _is_locked(run_dir):
                    continue
                _append_run_event(
                    run_dir,
                    "state_repair",
                    "detected stale score worker state without lock; re-queueing score",
                    state_before=STATE_SCORING,
                    state_after=STATE_SCORE_PENDING,
                    worker_id="orchestrator",
                    failure_classification="orchestrator_worker_unavailable",
                )
                self._mark_run_state(run_dir, STATE_SCORE_PENDING)
                self._mark_queue_state(
                    run_dir,
                    kind="score",
                    state=STATE_QUEUED,
                    attempt=self._score_attempts.get(
                        run_dir.name, _to_int(entry.get("attempt"), default=1)
                    ),
                    worker_id="orchestrator-recover",
                )
                run_dir_entry = self._run_state_file_or_manifest(run_dir)
                if run_dir_entry in {STATE_SCORE_PENDING, STATE_MODEL_COMPLETE}:
                    discovered.add(run_id)
                    candidates.append(run_dir)
                continue

            if self._queue_timeout_exceeded(run_dir, score=True):
                self._handle_queue_timeout(run_dir, score=True)
                continue

            state = self._effective_state(run_dir)
            manifest_state = _run_manifest_state(run_dir)
            if _is_terminal(state) or _is_terminal(manifest_state):
                continue
            if state in {STATE_SCORE_PENDING, STATE_MODEL_COMPLETE}:
                discovered.add(run_id)
                candidates.append(run_dir)

        for run_dir in self._run_dir_iter():
            if not (run_dir / "task.md").exists() or not (run_dir / "RUN.md").exists():
                continue
            if run_dir.name in discovered:
                continue
            if _is_cancel_requested(run_dir):
                continue
            state = self._effective_state(run_dir)
            if state not in {STATE_SCORE_PENDING, STATE_MODEL_COMPLETE}:
                continue
            if _is_terminal(state):
                continue
            attempt = max(1, self._score_attempts.get(run_dir.name, 1))
            self._queue_score(run_dir, attempt=attempt, worker_id="orchestrator")
            discovered.add(run_dir.name)
            candidates.append(run_dir)

        dedup: dict[str, pathlib.Path] = {}
        for run_dir in candidates:
            dedup[run_dir.name] = run_dir
        return list(dedup.values())

    def _score_run_error(
        self, run_dir: pathlib.Path, exit_code: int, payload: dict[str, Any]
    ) -> str:
        if payload.get("overall_error_code"):
            return str(payload.get("overall_error_code"))
        if exit_code == 124:
            return "score_timeout"
        if _is_cancel_requested(run_dir):
            return "cancelled"
        return "eval_failed"

    def _model_retry_class(self, run_dir: pathlib.Path, exit_code: int) -> str:
        if _is_cancel_requested(run_dir):
            return "orchestrator_cancelled"
        if exit_code == 124:
            return "deadline_exceeded"
        if _run_state_file_or_manifest(run_dir) == STATE_FAILED:
            return "orchestrator_worker_exhausted"
        return "model_runtime_failure"

    def _worker_cap_reached(self, run_dir: pathlib.Path, score: bool = False) -> bool:
        if score:
            cap = self._effective_score_cap(run_dir)
            profile, _ = self._resolve_profile_and_caps(run_dir)
            return self._running_count_for_profile(self._running_score, profile) >= cap
        profile, _ = self._resolve_profile_and_caps(run_dir)
        cap = self._effective_model_cap(run_dir)
        return self._running_count_for_profile(self._running_model, profile) >= cap

    def _mark_saturation(
        self,
        run_dir: pathlib.Path,
        phase: str,
        profile: str,
        worker: str,
        *,
        failure_classification: str = "resource_cap_exceeded",
    ) -> None:
        _append_run_event(
            run_dir,
            phase,
            "dispatch blocked by resource limits",
            state_before=self._effective_state(run_dir),
            state_after=self._effective_state(run_dir),
            worker_id=worker,
            failure_classification=failure_classification,
            heartbeat_reason="resource_cap_exceeded",
        )

    def _launch_model_worker(
        self, run_dir: pathlib.Path, *, attempt: int, queue_wait_ms: int
    ) -> None:
        profile, caps = self._resolve_profile_and_caps(run_dir)
        worker_id = f"run-worker-{attempt}"
        command = [sys.executable, str(self.config.script_dir / "run_task.py"), str(run_dir)]
        env = dict(os.environ)
        env.update(
            {
                "HARNESS_ASYNC_SCORING": "1",
                "HARNESS_RUN_QUEUE_PATH": str(self.config.run_queue_path),
                "HARNESS_SCORE_QUEUE_PATH": str(self.config.score_queue_path),
                "HARNESS_QUEUE_WAIT_MS": str(queue_wait_ms),
                "HARNESS_WORKER_ID": worker_id,
                "HARNESS_ATTEMPT": str(attempt),
                "HARNESS_EXECUTION_PROFILE": profile,
                "HARNESS_MAX_EVAL_COMMANDS": str(caps["max_eval_commands"]),
                "HARNESS_EVAL_TIMEOUT_SECONDS": str(caps["eval_command_timeout_seconds"]),
                "HARNESS_SCORE_TIMEOUT_SECONDS": str(caps["score_timeout_seconds"]),
                "HARNESS_MODEL_TIMEOUT_SECONDS": str(caps["model_timeout_seconds"]),
                "HARNESS_MAX_TRANSCRIPT_BYTES": str(caps["max_transcript_bytes"]),
                "HARNESS_MAX_PI_STDERR_BYTES": str(caps["max_pi_stderr_bytes"]),
            }
        )
        model_wall_clock = _to_positive_int(caps.get("max_run_wall_clock_seconds"), default=0)
        if model_wall_clock > 0:
            env["HARNESS_MAX_RUN_WALL_CLOCK_SECONDS"] = str(model_wall_clock)
        try:
            process = subprocess.Popen(
                command,
                cwd=str(self.runs_root.parent),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env,
                text=False,
            )
        except OSError as exc:
            _append_run_event(
                run_dir,
                "model_dispatch",
                f"failed to start model worker: {exc}",
                state_before=STATE_QUEUED,
                state_after=STATE_QUEUED,
                worker_id=worker_id,
                attempt=attempt,
                queue_wait_ms=queue_wait_ms,
                failure_classification="orchestrator_worker_unavailable",
                error_code="orchestrator_worker_unavailable",
            )
            raise
        deadline_ms = 0
        if model_wall_clock > 0:
            deadline_ms = now_ms() + model_wall_clock * 1000
        self._running_model[run_dir.name] = WorkerContext(
            run_dir=run_dir,
            process=process,
            attempt=attempt,
            started_ms=now_ms(),
            queue_wait_ms=queue_wait_ms,
            worker_id=worker_id,
            deadline_ms=deadline_ms,
            profile=profile,
            state=STATE_MODEL_RUNNING,
        )

    def _launch_score_worker(
        self, run_dir: pathlib.Path, *, attempt: int, queue_wait_ms: int
    ) -> None:
        profile, caps = self._resolve_profile_and_caps(run_dir)
        worker_id = f"score-worker-{attempt}"
        command = [
            sys.executable,
            str(self.config.script_dir / "run_task.py"),
            "--score-only",
            str(run_dir),
        ]
        env = dict(os.environ)
        env.update(
            {
                "HARNESS_SCORE_TIMEOUT_SECONDS": str(caps["score_timeout_seconds"]),
                "HARNESS_MAX_EVAL_COMMANDS": str(caps["max_eval_commands"]),
                "HARNESS_EVAL_TIMEOUT_SECONDS": str(caps["eval_command_timeout_seconds"]),
                "HARNESS_SCORE_WAIT_MS": str(queue_wait_ms),
                "HARNESS_EXECUTION_PROFILE": profile,
                "HARNESS_WORKER_ID": worker_id,
                "HARNESS_ATTEMPT": str(attempt),
                "HARNESS_ASYNC_SCORING": "0",
            }
        )
        score_wall_clock = _to_positive_int(caps.get("max_run_wall_clock_seconds"), default=0)
        if score_wall_clock > 0:
            env["HARNESS_MAX_RUN_WALL_CLOCK_SECONDS"] = str(score_wall_clock)
        try:
            process = subprocess.Popen(
                command,
                cwd=str(self.runs_root.parent),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env,
                text=False,
            )
        except OSError as exc:
            _append_run_event(
                run_dir,
                "score_dispatch",
                f"failed to start score worker: {exc}",
                state_before=STATE_SCORE_PENDING,
                state_after=STATE_SCORE_PENDING,
                worker_id=worker_id,
                attempt=attempt,
                score_wait_ms=queue_wait_ms,
                failure_classification="orchestrator_worker_unavailable",
                error_code="orchestrator_worker_unavailable",
            )
            raise
        deadline_ms = 0
        if score_wall_clock > 0:
            deadline_ms = now_ms() + score_wall_clock * 1000
        self._running_score[run_dir.name] = WorkerContext(
            run_dir=run_dir,
            process=process,
            attempt=attempt,
            started_ms=now_ms(),
            queue_wait_ms=queue_wait_ms,
            worker_id=worker_id,
            deadline_ms=deadline_ms,
            profile=profile,
            state=STATE_SCORING,
        )

    def _collect_model_results(self) -> None:
        for run_id in list(self._running_model.keys()):
            worker = self._running_model.get(run_id)
            if worker is None:
                continue
            code = worker.process.poll()
            if code is None:
                continue

            del self._running_model[run_id]
            manifest = _read_json(worker.run_dir / "outputs" / "run_manifest.json")
            state = _normalize_state(
                manifest.get("state") or _run_state_file_or_manifest(worker.run_dir)
            )
            if _is_cancel_requested(worker.run_dir):
                self._mark_run_state(worker.run_dir, STATE_CANCELLED)
                self._mark_queue_state(
                    worker.run_dir,
                    kind="run",
                    state=STATE_CANCELLED,
                    attempt=worker.attempt,
                    worker_id="orchestrator-cancel",
                )
                continue

            if code in {0, 124} and state in {STATE_SCORE_PENDING, STATE_MODEL_COMPLETE}:
                self._append_run_event(
                    worker.run_dir,
                    "model_complete",
                    "model run produced score-required output",
                    state_before=STATE_MODEL_RUNNING,
                    state_after=STATE_SCORE_PENDING,
                    worker_id=worker.worker_id,
                    attempt=worker.attempt,
                    queue_wait_ms=worker.queue_wait_ms,
                    model_wait_ms=max(0, now_ms() - worker.started_ms),
                    heartbeat_reason="model_complete",
                )
                self._mark_run_state(worker.run_dir, STATE_SCORE_PENDING)
                self._mark_queue_state(
                    worker.run_dir,
                    kind="run",
                    state=STATE_SCORE_PENDING,
                    attempt=worker.attempt,
                    worker_id=worker.worker_id,
                )
                attempt = self._score_attempts.get(worker.run_dir.name, 1)
                self._queue_score(
                    worker.run_dir, attempt=attempt, worker_id=f"score-queue-{attempt}"
                )
                continue

            if code == 0 and state == STATE_COMPLETE:
                self._append_run_event(
                    worker.run_dir,
                    "model_complete",
                    "model run completed and score already resolved by run task",
                    state_before=STATE_MODEL_RUNNING,
                    state_after=STATE_COMPLETE,
                    worker_id=worker.worker_id,
                    attempt=worker.attempt,
                    queue_wait_ms=worker.queue_wait_ms,
                    model_wait_ms=max(0, now_ms() - worker.started_ms),
                )
                self._mark_run_state(worker.run_dir, STATE_COMPLETE)
                self._mark_queue_state(
                    worker.run_dir,
                    kind="run",
                    state=STATE_COMPLETE,
                    attempt=worker.attempt,
                    worker_id=worker.worker_id,
                )
                continue

            if self._can_retry_model(worker.run_dir):
                self._model_attempts[run_id] = worker.attempt + 1
                backoff_until = now_ms() + self.config.model_backoff_ms
                self._model_retry_wait_until_ms[run_id] = backoff_until
                failure_code = self._model_retry_class(worker.run_dir, code)
                _append_run_event(
                    worker.run_dir,
                    "model_retry",
                    "model run failed, scheduling retry",
                    worker_id=worker.worker_id,
                    attempt=self._model_attempts[run_id],
                    state_before=STATE_MODEL_RUNNING,
                    state_after=STATE_QUEUED,
                    queue_wait_ms=worker.queue_wait_ms,
                    model_wait_ms=max(0, now_ms() - worker.started_ms),
                    heartbeat_reason="retry",
                    error_code=failure_code,
                    failure_classification="orchestrator_worker_retry",
                )
                self._mark_run_state(worker.run_dir, STATE_QUEUED)
                self._mark_queue_retry(
                    worker.run_dir,
                    score=False,
                    attempt=self._model_attempts[run_id],
                    worker_id=f"run-worker-{worker.attempt}",
                    failure="orchestrator_worker_retry",
                )
                continue

            failure_code = self._model_retry_class(worker.run_dir, code)
            self._mark_run_state(worker.run_dir, STATE_FAILED)
            self._mark_queue_state(
                worker.run_dir,
                kind="run",
                state=STATE_FAILED,
                attempt=worker.attempt,
                worker_id=worker.worker_id,
            )
            _append_run_event(
                worker.run_dir,
                "model_failed",
                "model worker hit retry ceiling",
                worker_id=worker.worker_id,
                attempt=worker.attempt,
                state_before=STATE_MODEL_RUNNING,
                state_after=STATE_FAILED,
                queue_wait_ms=worker.queue_wait_ms,
                model_wait_ms=max(0, now_ms() - worker.started_ms),
                error_code=failure_code,
                failure_classification=failure_code or "orchestrator_worker_exhausted",
            )

    def _collect_score_results(self) -> None:
        for run_id in list(self._running_score.keys()):
            worker = self._running_score.get(run_id)
            if worker is None:
                continue
            code = worker.process.poll()
            if code is None:
                continue

            del self._running_score[run_id]
            payload = _read_json(worker.run_dir / "score.json")
            if code == 0 and payload.get("overall_pass", False):
                self._append_run_event(
                    worker.run_dir,
                    "score_complete",
                    "score worker completed successfully",
                    worker_id=worker.worker_id,
                    attempt=worker.attempt,
                    state_before=STATE_SCORING,
                    state_after=STATE_COMPLETE,
                    score_wait_ms=max(0, now_ms() - worker.started_ms),
                    heartbeat_reason="pass",
                )
                self._mark_run_state(worker.run_dir, STATE_COMPLETE)
                self._mark_queue_state(
                    worker.run_dir,
                    kind="score",
                    state=STATE_COMPLETE,
                    attempt=worker.attempt,
                    worker_id=worker.worker_id,
                )
                self._mark_queue_state(
                    worker.run_dir,
                    kind="run",
                    state=STATE_COMPLETE,
                    attempt=worker.attempt,
                    worker_id=worker.worker_id,
                )
                continue

            failure_code = self._score_run_error(worker.run_dir, code, payload)
            if self._can_retry_score(worker.run_dir):
                self._score_attempts[run_id] = worker.attempt + 1
                backoff_until = now_ms() + self.config.score_backoff_ms
                self._score_retry_wait_until_ms[run_id] = backoff_until
                _append_run_event(
                    worker.run_dir,
                    "score_retry",
                    "score worker failed, scheduling retry",
                    worker_id=worker.worker_id,
                    attempt=self._score_attempts[run_id],
                    state_before=STATE_SCORING,
                    state_after=STATE_SCORE_PENDING,
                    score_wait_ms=max(0, now_ms() - worker.started_ms),
                    error_code=failure_code,
                    failure_classification="orchestrator_score_retry",
                    heartbeat_reason="retry",
                )
                self._mark_run_state(worker.run_dir, STATE_SCORE_PENDING)
                self._mark_queue_retry(
                    worker.run_dir,
                    score=True,
                    attempt=self._score_attempts[run_id],
                    worker_id=f"score-worker-{worker.attempt}",
                    failure="orchestrator_score_retry",
                )
                attempt = self._score_attempts.get(worker.run_dir.name, 1)
                self._queue_score(
                    worker.run_dir,
                    attempt=attempt,
                    worker_id=f"score-queue-{worker.worker_id}",
                )
                continue

            self._mark_run_state(worker.run_dir, STATE_FAILED)
            self._mark_queue_state(
                worker.run_dir,
                kind="score",
                state=STATE_FAILED,
                attempt=worker.attempt,
                worker_id=worker.worker_id,
            )
            _append_run_event(
                worker.run_dir,
                "score_failed",
                "score worker hit retry ceiling",
                worker_id=worker.worker_id,
                attempt=worker.attempt,
                state_before=STATE_SCORING,
                state_after=STATE_FAILED,
                score_wait_ms=max(0, now_ms() - worker.started_ms),
                error_code=failure_code,
                failure_classification=failure_code,
            )

    def _drain_stale_workers(self) -> None:
        if self.config.max_run_wall_clock_seconds <= 0:
            return
        cutoff_ms = now_ms()
        for run_id, worker in list(self._running_model.items()):
            if worker.deadline_ms <= 0 or worker.deadline_ms > cutoff_ms:
                continue
            try:
                worker.process.terminate()
            except Exception:
                pass
            self._mark_run_state(worker.run_dir, STATE_FAILED)
            _append_run_event(
                worker.run_dir,
                "model_timeout",
                "model worker terminated by orchestrator wall-clock cap",
                worker_id=worker.worker_id,
                attempt=worker.attempt,
                state_before=STATE_MODEL_RUNNING,
                state_after=STATE_FAILED,
                timeout_deadline=worker.deadline_ms,
                error_code="deadline_exceeded",
                failure_classification="deadline_exceeded",
            )
            self._mark_queue_state(
                worker.run_dir,
                kind="run",
                state=STATE_FAILED,
                attempt=worker.attempt,
                worker_id="orchestrator-timeout",
                extra={"failure_classification": "deadline_exceeded"},
            )
            del self._running_model[run_id]

        for run_id, worker in list(self._running_score.items()):
            if worker.deadline_ms <= 0 or worker.deadline_ms > cutoff_ms:
                continue
            try:
                worker.process.terminate()
            except Exception:
                pass
            self._mark_run_state(worker.run_dir, STATE_FAILED)
            _append_run_event(
                worker.run_dir,
                "score_timeout",
                "score worker terminated by orchestrator wall-clock cap",
                worker_id=worker.worker_id,
                attempt=worker.attempt,
                state_before=STATE_SCORING,
                state_after=STATE_FAILED,
                timeout_deadline=worker.deadline_ms,
                error_code="deadline_exceeded",
                failure_classification="deadline_exceeded",
            )
            self._mark_queue_state(
                worker.run_dir,
                kind="score",
                state=STATE_FAILED,
                attempt=worker.attempt,
                worker_id="orchestrator-timeout",
                extra={"failure_classification": "deadline_exceeded"},
            )
            del self._running_score[run_id]

    def _heartbeat_workers(self) -> None:
        now = now_ms()
        for worker in list(self._running_model.values()) + list(self._running_score.values()):
            if now - self._last_heartbeat_ms.get(worker.run_dir.name, 0) < HEARTBEAT_INTERVAL_MS:
                continue
            self._last_heartbeat_ms[worker.run_dir.name] = now
            if worker.state == STATE_MODEL_RUNNING:
                _append_run_event(
                    worker.run_dir,
                    "heartbeat",
                    "model worker is running",
                    worker_id=worker.worker_id,
                    attempt=worker.attempt,
                    state_before=STATE_MODEL_RUNNING,
                    state_after=STATE_MODEL_RUNNING,
                    model_wait_ms=now - worker.started_ms,
                    queue_wait_ms=worker.queue_wait_ms,
                    heartbeat_reason="model_backlog_heartbeat",
                )
            else:
                _append_run_event(
                    worker.run_dir,
                    "heartbeat",
                    "score worker is running",
                    worker_id=worker.worker_id,
                    attempt=worker.attempt,
                    state_before=STATE_SCORING,
                    state_after=STATE_SCORING,
                    score_wait_ms=now - worker.started_ms,
                    queue_wait_ms=worker.queue_wait_ms,
                    heartbeat_reason="score_backlog_heartbeat",
                )

    def _cancel_runs_if_requested(self) -> None:
        if not self.runs_root.exists():
            return
        for run_dir in self._run_dir_iter():
            if not _is_cancel_requested(run_dir):
                continue
            state = self._effective_state(run_dir)
            if _is_terminal(state):
                continue
            if run_dir.name in self._running_model:
                worker = self._running_model.pop(run_dir.name)
                try:
                    worker.process.terminate()
                except Exception:
                    pass
            if run_dir.name in self._running_score:
                worker = self._running_score.pop(run_dir.name)
                try:
                    worker.process.terminate()
                except Exception:
                    pass
            self._mark_run_state(run_dir, STATE_CANCELLED)
            _append_run_event(
                run_dir,
                "cancel",
                "run cancellation requested via .orchestrator-cancel",
                worker_id="orchestrator",
                state_before=state or STATE_QUEUED,
                state_after=STATE_CANCELLED,
                failure_classification="orchestrator_cancelled",
                error_code="cancelled",
                heartbeat_reason="cancel_requested",
            )
            self._mark_queue_state(
                run_dir,
                kind="run",
                state=STATE_CANCELLED,
                attempt=self._model_attempts.get(run_dir.name, 1),
                worker_id="orchestrator-cancel",
            )
            self._mark_queue_state(
                run_dir,
                kind="score",
                state=STATE_CANCELLED,
                attempt=self._score_attempts.get(run_dir.name, 1),
                worker_id="orchestrator-cancel",
            )

    def _dispatch_model_work(self) -> None:
        for run_dir in self._discover_model_queue():
            if self._stop:
                return
            if run_dir.name in self._running_model:
                continue
            if run_dir.name in self._running_score:
                continue
            if not self._retry_window(run_dir, score=False):
                continue
            if self._queue_timeout_exceeded(run_dir, score=False):
                self._handle_queue_timeout(run_dir, score=False)
                continue
            if _is_cancel_requested(run_dir):
                continue
            if self._worker_cap_reached(run_dir, score=False):
                self._mark_saturation(
                    run_dir,
                    "model_dispatch",
                    self._resolve_profile_and_caps(run_dir)[0],
                    "orchestrator",
                    failure_classification="resource_cap_exceeded",
                )
                continue
            if len(self._running_model) >= self.config.max_model_workers:
                self._mark_saturation(
                    run_dir,
                    "model_dispatch",
                    self._resolve_profile_and_caps(run_dir)[0],
                    "orchestrator",
                    failure_classification="score_backpressure",
                )
                continue

            profile, caps = self._resolve_profile_and_caps(run_dir)
            payload = self._load_queue_state(run_dir.name, kind="run")
            queue_payload_attempt = self._model_attempts.get(
                run_dir.name, _to_int(payload.get("attempt") if payload else None, default=1)
            )
            self._model_attempts[run_dir.name] = max(1, queue_payload_attempt)
            attempt = max(1, self._model_attempts[run_dir.name])
            queue_wait_ms = self._queue_wait_ms(run_dir, score=False)
            if queue_wait_ms < 0:
                queue_wait_ms = 0
            if run_dir.name not in self._run_queue_wait_start_ms:
                self._run_queue_wait_start_ms[run_dir.name] = now_ms() - queue_wait_ms
            if not self._load_queue_state(run_dir.name, kind="run"):
                self._queue_run(run_dir, attempt=attempt, worker_id=f"run-worker-{attempt}")
            self._mark_queue_state(
                run_dir,
                kind="run",
                state=STATE_CLAIMED,
                attempt=attempt,
                worker_id=f"run-worker-{attempt}",
            )
            _append_run_event(
                run_dir,
                "model_dispatch",
                "dispatching model worker",
                worker_id=f"run-worker-{attempt}",
                attempt=attempt,
                state_before=self._effective_state(run_dir),
                state_after=STATE_CLAIMED,
                queue_wait_ms=queue_wait_ms,
                heartbeat_reason="queued",
            )
            self._mark_run_state(run_dir, STATE_CLAIMED)
            try:
                self._launch_model_worker(run_dir, attempt=attempt, queue_wait_ms=queue_wait_ms)
            except Exception:
                self._mark_queue_retry(
                    run_dir,
                    score=False,
                    attempt=attempt + 1,
                    worker_id=f"run-worker-{attempt}",
                    failure="orchestrator_worker_unavailable",
                )
                self._mark_run_state(run_dir, STATE_QUEUED)
                self._model_retry_wait_until_ms[run_dir.name] = (
                    now_ms() + self.config.model_backoff_ms
                )
                _append_run_event(
                    run_dir,
                    "model_dispatch",
                    "worker launch failed; retrying after backoff",
                    worker_id=f"run-worker-{attempt}",
                    attempt=attempt,
                    state_before=STATE_QUEUED,
                    state_after=STATE_QUEUED,
                    queue_wait_ms=queue_wait_ms,
                    failure_classification="orchestrator_worker_unavailable",
                    error_code="orchestrator_worker_unavailable",
                )
                continue
            self._mark_run_state(run_dir, STATE_MODEL_RUNNING)
            self._mark_queue_state(
                run_dir,
                kind="run",
                state=STATE_MODEL_RUNNING,
                attempt=attempt,
                worker_id=f"run-worker-{attempt}",
            )
            _append_run_event(
                run_dir,
                "model_dispatch",
                "model worker started",
                worker_id=f"run-worker-{attempt}",
                attempt=attempt,
                state_before=STATE_CLAIMED,
                state_after=STATE_MODEL_RUNNING,
                queue_wait_ms=queue_wait_ms,
                model_wait_ms=0,
                heartbeat_reason="started",
                timeout_deadline=(
                    now_ms()
                    + _to_positive_int(caps.get("max_run_wall_clock_seconds"), default=0) * 1000
                ),
            )
            if caps.get("max_run_wall_clock_seconds", 0):
                model_deadline_ms = (
                    now_ms()
                    + _to_positive_int(caps.get("max_run_wall_clock_seconds"), default=0) * 1000
                )
                _append_run_event(
                    run_dir,
                    "model_dispatch",
                    "applied run wall-clock cap",
                    worker_id=f"run-worker-{attempt}",
                    attempt=attempt,
                    state_before=STATE_MODEL_RUNNING,
                    state_after=STATE_MODEL_RUNNING,
                    timeout_deadline=model_deadline_ms,
                )

    def _dispatch_score_work(self) -> None:
        for run_dir in self._discover_score_candidates():
            if self._stop:
                return
            if run_dir.name in self._running_score:
                continue
            if run_dir.name in self._running_model:
                continue
            if _is_cancel_requested(run_dir):
                continue
            if not self._retry_window(run_dir, score=True):
                continue
            if self._queue_timeout_exceeded(run_dir, score=True):
                self._handle_queue_timeout(run_dir, score=True)
                continue
            if self._worker_cap_reached(run_dir, score=True):
                self._mark_saturation(
                    run_dir,
                    "score_dispatch",
                    self._resolve_profile_and_caps(run_dir)[0],
                    "orchestrator",
                    failure_classification="resource_cap_exceeded",
                )
                continue
            if len(self._running_score) >= self.config.max_score_workers:
                self._mark_saturation(
                    run_dir,
                    "score_dispatch",
                    self._resolve_profile_and_caps(run_dir)[0],
                    "orchestrator",
                    failure_classification="score_backpressure",
                )
                continue

            _, caps = self._resolve_profile_and_caps(run_dir)
            payload = self._load_queue_state(run_dir.name, kind="score")
            queue_payload_attempt = self._score_attempts.get(
                run_dir.name, _to_int(payload.get("attempt") if payload else None, default=1)
            )
            self._score_attempts[run_dir.name] = max(1, queue_payload_attempt)
            attempt = max(1, self._score_attempts[run_dir.name])
            queue_wait_ms = self._queue_wait_ms(run_dir, score=True)
            if queue_wait_ms < 0:
                queue_wait_ms = 0
            if run_dir.name not in self._score_queue_wait_start_ms:
                self._score_queue_wait_start_ms[run_dir.name] = now_ms() - queue_wait_ms
            if not self._load_queue_state(run_dir.name, kind="score"):
                self._queue_score(run_dir, attempt=attempt, worker_id=f"score-worker-{attempt}")
            self._mark_queue_state(
                run_dir,
                kind="score",
                state=STATE_CLAIMED,
                attempt=attempt,
                worker_id=f"score-worker-{attempt}",
            )
            _append_run_event(
                run_dir,
                "score_dispatch",
                "dispatching score worker",
                worker_id=f"score-worker-{attempt}",
                attempt=attempt,
                state_before=self._effective_state(run_dir),
                state_after=STATE_SCORING,
                score_wait_ms=queue_wait_ms,
                heartbeat_reason="queued",
            )
            self._mark_run_state(run_dir, STATE_SCORING)
            try:
                self._launch_score_worker(run_dir, attempt=attempt, queue_wait_ms=queue_wait_ms)
            except Exception:
                self._mark_queue_retry(
                    run_dir,
                    score=True,
                    attempt=attempt + 1,
                    worker_id=f"score-worker-{attempt}",
                    failure="orchestrator_worker_unavailable",
                )
                self._mark_run_state(run_dir, STATE_SCORE_PENDING)
                self._score_retry_wait_until_ms[run_dir.name] = (
                    now_ms() + self.config.score_backoff_ms
                )
                _append_run_event(
                    run_dir,
                    "score_dispatch",
                    "score worker launch failed; retrying after backoff",
                    worker_id=f"score-worker-{attempt}",
                    attempt=attempt,
                    state_before=STATE_SCORE_PENDING,
                    state_after=STATE_SCORE_PENDING,
                    score_wait_ms=queue_wait_ms,
                    failure_classification="orchestrator_worker_unavailable",
                    error_code="orchestrator_worker_unavailable",
                )
                continue
            self._mark_queue_state(
                run_dir,
                kind="score",
                state=STATE_SCORING,
                attempt=attempt,
                worker_id=f"score-worker-{attempt}",
            )
            if caps.get("max_run_wall_clock_seconds", 0):
                score_deadline_ms = (
                    now_ms()
                    + _to_positive_int(caps.get("max_run_wall_clock_seconds"), default=0) * 1000
                )
                _append_run_event(
                    run_dir,
                    "score_dispatch",
                    "applied score wall-clock cap",
                    worker_id=f"score-worker-{attempt}",
                    attempt=attempt,
                    state_before=STATE_SCORING,
                    state_after=STATE_SCORING,
                    timeout_deadline=score_deadline_ms,
                )

    def _discover_backlog_exists(self) -> bool:
        return any(bool(self._discover_model_queue()) or bool(self._discover_score_candidates()))

    def run(self) -> int:
        if not self.runs_root.exists():
            return 2
        _append_jsonl(
            self.config.run_queue_path,
            {
                "type": "orchestrator",
                "kind": "service",
                "event": "startup",
                "started_at_ms": now_ms(),
                "max_model_workers": self.config.max_model_workers,
                "max_score_workers": self.config.max_score_workers,
                "model_retries": self.config.model_retries,
                "score_retries": self.config.score_retries,
            },
        )

        while not self._stop:
            if self.timed_out:
                _append_jsonl(
                    self.config.run_queue_path,
                    {
                        "type": "orchestrator",
                        "kind": "service",
                        "event": "timeout",
                        "stopped_at_ms": now_ms(),
                        "reason": "orchestrator_wall_clock_exceeded",
                    },
                )
                break

            self._cancel_runs_if_requested()
            self._collect_model_results()
            self._collect_score_results()
            self._drain_stale_workers()
            self._run_retention_maintenance(reason="pre_async_cycle")
            self._heartbeat_workers()
            self._run_retention_maintenance(reason="heartbeat")
            self._dispatch_model_work()
            self._dispatch_score_work()
            self._run_retention_maintenance(reason="post_async_cycle")

            if (
                not self._running_model
                and not self._running_score
                and not self._discover_model_queue()
                and not self._discover_score_candidates()
            ):
                time.sleep(self.config.poll_interval_seconds)
                continue
            time.sleep(self.config.poll_interval_seconds)

            if self._discover_backlog_exists() and not (
                len(self._running_model) >= self.config.max_model_workers
                and len(self._running_score) >= self.config.max_score_workers
            ):
                continue

        for worker in list(self._running_model.values()):
            try:
                worker.process.terminate()
            except Exception:
                pass
        for worker in list(self._running_score.values()):
            try:
                worker.process.terminate()
            except Exception:
                pass
        return 0

    def request_stop(self, reason: str) -> None:
        self._stop = True
        self._shutdown_reason = reason


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="orchestrator.py schedules model + score workers on a file-native queue."
    )
    parser.add_argument("--runs-root", default=None, help="base directory containing run folders")
    parser.add_argument("--run-queue-path", default=None, help="append-only JSONL model queue path")
    parser.add_argument(
        "--score-queue-path", default=None, help="append-only JSONL score queue path"
    )
    parser.add_argument(
        "--max-model-workers", type=int, default=None, help="max concurrent model workers"
    )
    parser.add_argument(
        "--max-score-workers", type=int, default=None, help="max concurrent score workers"
    )
    parser.add_argument(
        "--duration-seconds", type=int, default=0, help="orchestrator wall-clock cap"
    )
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_argument_parser()
    return parser.parse_args(argv)


def _attach_signal_handlers(orchestrator: Orchestrator) -> list[tuple[int, Any]]:
    handlers: list[tuple[int, Any]] = []

    def _stop(_signum: int, _frame: object) -> None:
        orchestrator.request_stop("signal")

    for signame in ("SIGINT", "SIGTERM"):
        signum = getattr(signal, signame)
        handlers.append((signum, signal.getsignal(signum)))
        signal.signal(signum, _stop)
    return handlers


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = Orchestrator.from_environment()
    if args.runs_root:
        config.runs_root = pathlib.Path(args.runs_root).resolve()
    if args.run_queue_path:
        config.run_queue_path = pathlib.Path(args.run_queue_path).resolve()
    if args.score_queue_path:
        config.score_queue_path = pathlib.Path(args.score_queue_path).resolve()
    if args.max_model_workers is not None:
        config.max_model_workers = args.max_model_workers
    if args.max_score_workers is not None:
        config.max_score_workers = args.max_score_workers
    orchestrator = Orchestrator(config, max_duration_seconds=args.duration_seconds)
    handlers = _attach_signal_handlers(orchestrator)
    try:
        return orchestrator.run()
    finally:
        for signum, handler in handlers:
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())
