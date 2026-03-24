#!/usr/bin/env python3
from __future__ import annotations

import atexit
import json
import os
import pathlib
import re
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any

from capabilitylib import (
    DEFAULT_USAGE_PATH,
    build_capability_manifest,
    load_capability_library,
)
from harnesslib import (
    DEFAULT_RETRIEVAL_CONFIG,
    EXECUTION_PROFILES,
    RUNNER_VERSION,
    build_run_event,
    compute_dependencies_hash,
    default_policy_path,
    default_run_contract,
    evaluate_policy_guardrail,
    guardrail_policy_snapshot,
    load_policy,
    load_run_contract,
    make_result_template,
    now_utc,
    parse_task_file,
    resolve_execution_settings,
    write_json,
)
from learninglib import (
    candidate_runtime,
    candidate_summary,
    effective_candidate_mode,
    load_candidate_manifest,
)
from policylib import policy_feature_payload, predict_policy_heads


def now_ms() -> int:
    return int(time.time() * 1000)


def command_output(command: list[str]) -> str:
    try:
        completed = subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=False,
        )
    except Exception as exc:
        return f"unavailable: {exc}"
    return (completed.stdout or completed.stderr or "").strip()


def read_json(path: pathlib.Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


USAGE = (
    "usage: run-task.sh [--profile strict|capability|offline|networked|heavy_tools] "
    "[--skip-score] [--score-only] runs/<run-id> [model-pattern]"
)
REQUIRED_RUN_TOOLS = ("bash", "python3", "cat", "git")
BASE_DIRECTORIES = ("outputs", "home", "session", "score")
PRIMARY_ERROR_CODE_PRECEDENCE = (
    "cancelled",
    "guardrail_pre_run_denied",
    "guardrail_pre_score_dispatch_denied",
    "contract_invalid",
    "orchestrator_queue_timeout",
    "orchestrator_worker_unavailable",
    "deadline_exceeded",
    "model_invocation_failed",
    "result_invalid",
    "eval_failed",
)


def _to_positive_int(value: str, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    if parsed < 1:
        return default
    return parsed


def usage() -> str:
    return USAGE


def split_error_codes(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    raw = str(value).strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def unique_codes(*values: Any) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        for code in split_error_codes(value):
            if code in seen:
                continue
            seen.add(code)
            normalized.append(code)
    return normalized


def primary_error_code_for_codes(codes: list[str]) -> str | None:
    if not codes:
        return None
    for code in PRIMARY_ERROR_CODE_PRECEDENCE:
        if code in codes:
            return code
    return sorted(codes)[0]


def parse_args(
    argv: list[str] | None = None,
) -> tuple[str | None, str, str, bool, bool]:
    args = list(sys.argv[1:] if argv is None else argv)
    profile_override = None
    skip_score = False
    score_only = False

    i = 0
    while i < len(args):
        arg = args[i]
        if arg in {"--help", "-h"}:
            print(USAGE, file=sys.stderr)
            raise SystemExit(0)
        if arg == "--profile":
            if i + 1 >= len(args):
                print(USAGE, file=sys.stderr)
                raise SystemExit(2)
            profile_override = args[i + 1]
            i += 2
            continue
        if arg == "--skip-score":
            skip_score = True
            i += 1
            continue
        if arg == "--score-only":
            score_only = True
            i += 1
            continue
        if arg.startswith("--profile="):
            profile_override = arg.split("=", 1)[1]
            i += 1
            continue
        if arg.startswith("--"):
            print(f"unknown option: {arg}", file=sys.stderr)
            print(USAGE, file=sys.stderr)
            raise SystemExit(2)
        break

    remaining = args[i:]
    if not remaining:
        print(USAGE, file=sys.stderr)
        raise SystemExit(2)

    run_dir = remaining[0]
    model = remaining[1] if len(remaining) > 1 else ""
    if score_only and skip_score:
        print("--score-only and --skip-score are mutually exclusive", file=sys.stderr)
        raise SystemExit(2)
    return profile_override, run_dir, model, skip_score, score_only


def parse_run_dir(raw_run_dir: str, repo_root: pathlib.Path) -> pathlib.Path:
    candidate = pathlib.Path(raw_run_dir).expanduser()
    if not candidate.is_absolute():
        candidate = repo_root / candidate
    return candidate.resolve()


def _normalize_pythonpath(
    script_dir: pathlib.Path,
    base_env: dict[str, str] | None = None,
) -> dict[str, str]:
    env = dict(os.environ if base_env is None else base_env)
    if script_dir is not None:
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = str(script_dir) if not existing else f"{script_dir}:{existing}"
    return env


def _is_executable_path(path: str) -> bool:
    candidate = pathlib.Path(path)
    return candidate.is_file() and os.access(candidate, os.X_OK)


@dataclass
class RunTaskConfig:
    script_dir: pathlib.Path
    repo_root: pathlib.Path
    run_dir: pathlib.Path
    model: str
    profile_override: str | None
    pi_bin: str
    strict_mode: str
    force_rerun: str
    model_timeout_seconds: str
    retry_count: str
    score_retry_count: str
    score_timeout_seconds: str
    eval_timeout_seconds: str
    skip_score: bool
    score_only: bool
    worker_id: str
    attempt: int
    async_scoring: bool
    max_run_wall_clock_seconds: str
    max_eval_commands: str
    run_queue_path: str
    score_queue_path: str
    max_transcript_bytes: str
    max_pi_stderr_bytes: str
    log_max_lines: str
    queue_wait_ms: str
    score_wait_ms: str


class RunTaskRunner:
    def __init__(self, argv: list[str], *, config_env: dict[str, str] | None = None):
        (
            profile_override,
            run_dir,
            model,
            skip_score,
            score_only,
        ) = parse_args(argv)
        self.script_dir = pathlib.Path(__file__).resolve().parent
        self.repo_root = self.script_dir.parent
        self.profile_override = profile_override
        self.run_dir = parse_run_dir(run_dir, self.repo_root)
        self.model = model

        env = os.environ if config_env is None else config_env
        self.pi_bin = env.get("HARNESS_PI_BIN", "pi")
        self.strict_mode = env.get("HARNESS_STRICT_MODE", "1")
        self.force_rerun = env.get("HARNESS_FORCE_RERUN", "0")
        self.model_timeout_seconds = env.get("HARNESS_MODEL_TIMEOUT_SECONDS", "900")
        self.score_timeout_seconds = env.get("HARNESS_SCORE_TIMEOUT_SECONDS", "0")
        self.retry_count = env.get("HARNESS_PI_RETRY_COUNT", "2")
        self.score_retry_count = env.get("HARNESS_SCORE_RETRY_COUNT", "2")
        self.eval_timeout_seconds = env.get("HARNESS_EVAL_TIMEOUT_SECONDS", "")
        self.max_run_wall_clock_seconds = env.get("HARNESS_MAX_RUN_WALL_CLOCK_SECONDS", "0")
        self.max_eval_commands = env.get("HARNESS_MAX_EVAL_COMMANDS", "0")
        self.run_queue_path = env.get("HARNESS_RUN_QUEUE_PATH", "")
        self.score_queue_path = env.get("HARNESS_SCORE_QUEUE_PATH", "")
        self.queue_wait_ms = env.get("HARNESS_QUEUE_WAIT_MS", "0")
        self.score_wait_ms = env.get("HARNESS_SCORE_WAIT_MS", "0")
        self.worker_id = env.get("HARNESS_WORKER_ID", "orchestrator-worker")
        self.attempt = _to_positive_int(env.get("HARNESS_ATTEMPT", "1"), default=1)
        self.skip_score = bool(
            skip_score or env.get("HARNESS_SKIP_SCORE", "0") not in {"", "0", "false", "False"}
        )
        self.score_only = bool(score_only)
        self.async_scoring = env.get("HARNESS_ASYNC_SCORING", "0") not in {
            "",
            "0",
            "false",
            "False",
        }
        self.max_transcript_bytes = env.get("HARNESS_MAX_TRANSCRIPT_BYTES", "5242880")
        self.max_pi_stderr_bytes = env.get("HARNESS_MAX_PI_STDERR_BYTES", "1048576")
        self.log_max_lines = env.get("HARNESS_STREAM_LOG_LINES", "200000")

        self.config = RunTaskConfig(
            script_dir=self.script_dir,
            repo_root=self.repo_root,
            run_dir=self.run_dir,
            model=self.model,
            profile_override=self.profile_override,
            pi_bin=self.pi_bin,
            strict_mode=self.strict_mode,
            force_rerun=self.force_rerun,
            model_timeout_seconds=self.model_timeout_seconds,
            retry_count=self.retry_count,
            score_retry_count=self.score_retry_count,
            score_timeout_seconds=self.score_timeout_seconds,
            eval_timeout_seconds=self.eval_timeout_seconds,
            skip_score=self.skip_score,
            score_only=self.score_only,
            worker_id=self.worker_id,
            attempt=self.attempt,
            async_scoring=self.async_scoring,
            max_run_wall_clock_seconds=self.max_run_wall_clock_seconds,
            max_eval_commands=self.max_eval_commands,
            run_queue_path=self.run_queue_path,
            score_queue_path=self.score_queue_path,
            max_transcript_bytes=self.max_transcript_bytes,
            max_pi_stderr_bytes=self.max_pi_stderr_bytes,
            log_max_lines=self.log_max_lines,
            queue_wait_ms=self.queue_wait_ms,
            score_wait_ms=self.score_wait_ms,
        )

        self.run_id = self.run_dir.name
        self.task_md = self.run_dir / "task.md"
        self.run_md = self.run_dir / "RUN.md"
        self.run_schema_path = self.run_dir / "result.schema.json"
        self.result_template_path = self.run_dir / "result.template.json"
        self.run_contract_path = self.run_dir / "run.contract.json"
        self.manifest_path = self.run_dir / "outputs" / "run_manifest.json"
        self.event_log_path = self.run_dir / "run-events.jsonl"
        self.state_file = self.run_dir / "run.state"
        self.lock_dir = self.run_dir / ".run-lock"
        self.trace_id = self.run_id

        self.phase = "resolve"
        self.error_code = ""
        self.run_started_epoch_ms = now_ms()
        self.model_started_epoch_ms = ""
        self.model_wait_start_ms = ""
        self.pi_started_epoch_ms = ""
        self.pi_finished_epoch_ms = ""
        self.score_started_epoch_ms = ""
        self.score_finished_epoch_ms = ""
        self.run_finished_epoch_ms = ""

        self.run_contract_version = ""
        self.execution_profile = "strict"
        self.policy_path = "policies/strict.json"
        self.policy: dict[str, Any] = {}
        self.context_enabled = "0"
        self.context_manifest_rel = ""
        self.context_summary_rel = ""
        self.context_source_run_ids = ""
        self.context_bootstrap_mode = ""
        self.transport_mode = "cli_json"
        self.capabilities_enabled = False
        self.capability_library_path = ""
        self.capability_manifest_rel = ""
        self.subagents_allowed = False
        self.subagent_max_agents = 0
        self.allowed_subagent_profiles: list[str] = []
        self.capability_library: dict[str, Any] | None = None
        self.guardrail_decisions: list[dict[str, Any]] = []
        self.guardrails_path = self.run_dir / "outputs" / "guardrails.json"
        self._pre_score_dispatch_decision: dict[str, Any] | None = None
        self.retrieval_candidate: dict[str, Any] | None = None
        self.policy_candidate: dict[str, Any] | None = None
        self.model_candidate: dict[str, Any] | None = None
        self.bundle_candidate: dict[str, Any] | None = None
        self.retrieval_candidate_summary = candidate_summary(None)
        self.policy_candidate_summary = candidate_summary(None)
        self.model_candidate_summary = candidate_summary(None)
        self.bundle_candidate_summary = candidate_summary(None)
        self.policy_candidate_recommendations: dict[str, Any] = {}
        self.policy_candidate_applied: list[str] = []
        self.policy_candidate_overrides: list[str] = []
        self.retrieval_budget_overrides: dict[str, int] = {}
        self.retrieval_budget_envelope = {
            "max_source_runs": int(DEFAULT_RETRIEVAL_CONFIG["max_source_runs"]),
            "max_candidates": int(DEFAULT_RETRIEVAL_CONFIG["max_candidates"]),
        }
        self.selected_capability_profile: str | None = None
        self.model_selection_source = "cli_or_default"
        self.model_fallback = ""

        self._orig_sigint = None
        self._orig_sigterm = None
        self._locked = False
        self._registered_atexit = False
        atexit.register(self._cleanup_lock)
        self._registered_atexit = True

    @property
    def retry_limit(self) -> int:
        return int(self.retry_count)

    @property
    def timeout_seconds(self) -> int:
        return int(self.model_timeout_seconds)

    @property
    def score_retry_limit(self) -> int:
        return max(1, int(self.score_retry_count))

    @property
    def max_transcript_bytes_int(self) -> int:
        return max(1024, int(self.max_transcript_bytes))

    @property
    def max_pi_stderr_bytes_int(self) -> int:
        return max(1024, int(self.max_pi_stderr_bytes))

    @property
    def score_timeout_seconds_int(self) -> int | None:
        if not self.score_timeout_seconds:
            return None
        return max(1, int(self.score_timeout_seconds))

    @property
    def max_run_wall_clock_seconds_int(self) -> int:
        try:
            return max(0, int(self.max_run_wall_clock_seconds))
        except ValueError:
            return 0

    @property
    def max_eval_commands_int(self) -> int:
        return max(0, _to_positive_int(self.max_eval_commands, default=0))

    @property
    def queue_wait_ms_int(self) -> int:
        return max(0, _to_positive_int(self.queue_wait_ms, default=0))

    @property
    def score_wait_ms_int(self) -> int:
        return max(0, _to_positive_int(self.score_wait_ms, default=0))

    @property
    def run_deadline_ms(self) -> int:
        if self.max_run_wall_clock_seconds_int <= 0:
            return 0
        return self.run_started_epoch_ms + (self.max_run_wall_clock_seconds_int * 1000)

    def _sleep(self, seconds: int) -> None:
        time.sleep(seconds)

    def _context(self) -> dict[str, str]:
        return dict(os.environ)

    def _with_pythonpath(self, base_env: dict[str, str] | None = None) -> dict[str, str]:
        return _normalize_pythonpath(self.script_dir, base_env or self._context())

    def _run_command(
        self,
        command: list[str],
        *,
        env: dict[str, str] | None = None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd: str | pathlib.Path | None = None,
        text: bool = True,
        check: bool = False,
        timeout: int | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            command,
            env=env,
            stdout=stdout,
            stderr=stderr,
            cwd=cwd if cwd is not None else str(self.repo_root),
            text=text,
            check=check,
            timeout=timeout,
        )

    def _guardrail_policy_snapshot(self) -> dict[str, Any]:
        if not self.policy:
            return {}
        return guardrail_policy_snapshot(self.policy)

    def _append_guardrail_decision(
        self,
        hook: str,
        decision: dict[str, Any],
    ) -> None:
        if not isinstance(decision, dict):
            return
        payload = dict(decision)
        payload["hook"] = hook
        self.guardrail_decisions.append(payload)

    def _evaluate_guardrail(
        self,
        hook: str,
        *,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        decision = evaluate_policy_guardrail(
            self.policy or {},
            hook,
            context=context or {},
        )
        self._append_guardrail_decision(hook, decision)
        return decision

    def _write_guardrails_artifact(self) -> None:
        payload = {
            "selected_profile_id": self.execution_profile,
            "policy_fingerprint": self.policy.get("policy_fingerprint", ""),
            "policy_path": self.policy_path,
            "policy_version_source": self._guardrail_policy_snapshot().get("source_of_truth", ""),
            "policy_snapshot": self._guardrail_policy_snapshot(),
            "decisions": list(self.guardrail_decisions),
            "effective_policy": self.policy,
        }
        write_json(self.guardrails_path, payload)

    def _write_guardrail_block_score_payload(
        self, failure_code: str, violation_messages: list[str]
    ) -> None:
        score_payload = {
            "pi_exit_code": _to_positive_int(self.pi_exit, default=1)
            if hasattr(self, "pi_exit")
            else 1,
            "result_json_present": False,
            "result_json_valid_minimal": False,
            "result_json_valid_schema": False,
            "result_json_validations": [],
            "result_json_schema": {},
            "evaluations": [],
            "required_artifacts": [],
            "max_eval_commands": self.max_eval_commands_int,
            "task_parse": {"ok": True, "errors": [], "dangerous_eval_commands": []},
            "secret_scan": {
                "paths_scanned": 0,
                "scanned_path_count": 0,
                "skipped_path_count": 0,
                "skipped_reason_counts": {},
                "findings": [],
            },
            "execution_profile": self.execution_profile,
            "policy_path": self.policy_path,
            "guardrails": {
                "policy_snapshot": self._guardrail_policy_snapshot(),
                "decisions": list(self.guardrail_decisions),
                "policy_version_source": self._guardrail_policy_snapshot().get(
                    "source_of_truth", ""
                ),
            },
            "retrieval": {},
            "candidates": {
                "retrieval": dict(self.retrieval_candidate_summary),
                "policy": {
                    **dict(self.policy_candidate_summary),
                    "recommendations": dict(self.policy_candidate_recommendations),
                    "applied": list(self.policy_candidate_applied),
                    "overrides": list(self.policy_candidate_overrides),
                },
                "model": {
                    **dict(self.model_candidate_summary),
                    "selected_model": self.model or None,
                    "fallback_model": self.model_fallback or None,
                    "selection_source": self.model_selection_source,
                },
            },
            "failure_classifications": [failure_code],
            "overall_error_code": failure_code,
            "overall_pass": False,
            "cancelled": False,
            "guardrail_violations": violation_messages,
        }
        write_json(self.run_dir / "score.json", score_payload)

    def _write_state(self, value: str) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(f"{value}\n", encoding="utf-8")

    def _is_cancel_requested(self) -> bool:
        return (self.run_dir / ".orchestrator-cancel").exists()

    def _check_cancelled(self, phase: str) -> None:
        if self._is_cancel_requested():
            self.error_code = "cancelled"
            self._log_event(
                phase,
                "orchestrator cancellation requested",
                state_before=self.phase,
                state_after="cancelled",
                extra={"heartbeat_reason": "cancelled"},
            )
            raise SystemExit(130)

    def _append_jsonl(self, path: pathlib.Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")

    def _append_queue_event(self, path: pathlib.Path, payload: dict[str, Any]) -> None:
        normalized = dict(payload)
        normalized.setdefault("ts", now_utc())
        self._append_jsonl(path, normalized)

    def _enqueue_score_job(self) -> None:
        if not self.score_queue_path:
            return
        self._append_queue_event(
            pathlib.Path(self.score_queue_path),
            {
                "type": "score_enqueued",
                "kind": "score",
                "run_id": self.run_id,
                "run_dir": str(self.run_dir.resolve()),
                "attempt": self.attempt,
                "state": "queued",
                "worker_id": self.worker_id,
                "max_attempts": int(self.score_retry_limit),
            },
        )

    def _resolve_initial_state(self) -> str:
        if self.lock_dir.is_dir():
            pid_file = self.lock_dir / "pid"
            if pid_file.is_file():
                try:
                    pid = int(pid_file.read_text(encoding="utf-8").strip())
                    os.kill(pid, 0)
                    print("run state: running")
                    return "running"
                except (ProcessLookupError, OSError, ValueError):
                    pass
            return "partial"

        current_state = "new"
        if self.manifest_path.exists():
            manifest = read_json(self.manifest_path) or {}
            manifest_state = manifest.get("state", "partial")
            if manifest_state in {"model_complete", "score_pending"}:
                return "partial"
            if manifest_state == "complete" and self.force_rerun != "1":
                print("run state: complete")
                return "complete"
            if manifest_state == "complete":
                current_state = "partial"
            elif manifest_state != "complete":
                current_state = "partial"
        return current_state

    def _archive_partial_run(self) -> None:
        stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
        recovery_dir = self.run_dir / "recovery" / stamp
        recovery_dir.mkdir(parents=True, exist_ok=True)
        for rel in [
            "transcript.jsonl",
            "pi.stderr.log",
            "prompt.txt",
            "score.json",
            "git.status.txt",
            "patch.diff",
            "pi.exit_code.txt",
            "run-events.jsonl",
            "score",
            "outputs/run_manifest.json",
            "result.json",
        ]:
            source = self.run_dir / rel
            if source.exists():
                try:
                    shutil.move(source, recovery_dir / source.name)
                except OSError:
                    pass

    def _cleanup_lock(self) -> None:
        if self._locked and self.lock_dir.exists():
            shutil.rmtree(self.lock_dir)
        self._locked = False

    def _fail_contract_check(
        self,
        message: str,
        *,
        error_code: str,
        exit_code: int = 2,
    ) -> None:
        print(message, file=sys.stderr)
        self.error_code = error_code
        self._write_manifest("partial", self.phase, self.error_code, "")
        raise SystemExit(exit_code)

    def _acquire_lock(self) -> None:
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        (self.lock_dir / "pid").write_text(f"{os.getpid()}\n", encoding="utf-8")
        self._write_state("running")
        self._locked = True

    def _log_event(
        self,
        phase_name: str,
        message: str,
        error: str = "",
        *,
        state_before: str | None = None,
        state_after: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        payload = build_run_event(
            self.run_id,
            phase_name,
            message,
            error_code=error,
            extra={
                "state_before": state_before,
                "state_after": state_after,
                "worker_id": self.worker_id,
                "attempt": self.attempt,
                "timeout_deadline": self.run_deadline_ms,
                "queue_wait_ms": self.queue_wait_ms_int,
                "model_wait_ms": 0,
                "score_wait_ms": self.score_wait_ms_int,
                "heartbeat_reason": None,
                **(extra or {}),
            },
        )
        self.event_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.event_log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")

    def _write_manifest(
        self,
        state: str,
        phase_name: str,
        error: str = "",
        run_finished: str = "",
    ) -> None:
        if self.policy:
            self._write_guardrails_artifact()
        score_payload = read_json(self.run_dir / "score.json") or {}
        manifest_failure_classifications = unique_codes(
            score_payload.get("failure_classifications"),
            error,
        )
        primary_error_code = primary_error_code_for_codes(manifest_failure_classifications)
        context_payload = (
            read_json(self.run_dir / self.context_manifest_rel)
            if self.context_enabled == "1" and self.context_manifest_rel
            else {}
        ) or {}
        capability_payload = (
            read_json(self.run_dir / self.capability_manifest_rel)
            if self.capabilities_enabled and self.capability_manifest_rel
            else {}
        ) or {}
        capability_score = score_payload.get("capabilities", {}) or {}
        dependencies = {
            "pi": command_output([self.pi_bin, "--version"]),
            "python3": command_output(["python3", "--version"]),
            "bash": command_output(["bash", "--version"]),
            "git": command_output(["git", "--version"]),
        }
        manifest = {
            "manifest_version": "v1",
            "runner_version": RUNNER_VERSION,
            "run_id": self.run_id,
            "trace_id": self.trace_id,
            "generated_at": now_utc(),
            "state": state,
            "phase": phase_name,
            "error_code": error or None,
            "primary_error_code": primary_error_code,
            "failure_classifications": manifest_failure_classifications,
            "run_contract_version": self.run_contract_version or "v1",
            "paths": {
                "task_md": "task.md",
                "run_md": "RUN.md",
                "run_contract": "run.contract.json",
                "result_schema": "result.schema.json",
                "result_template": "result.template.json",
                "result_json": "result.json",
                "score_json": "score.json",
                "event_log": "run-events.jsonl",
                "manifest": "outputs/run_manifest.json",
            },
            "dependencies": {
                **dependencies,
                "hash": compute_dependencies_hash(dependencies),
            },
            "git": {"sha": self._git_sha()},
            "timings": {
                "run_started_epoch_ms": int(self.run_started_epoch_ms),
                "pi_started_epoch_ms": int(self.pi_started_epoch_ms)
                if self.pi_started_epoch_ms
                else None,
                "pi_finished_epoch_ms": int(self.pi_finished_epoch_ms)
                if self.pi_finished_epoch_ms
                else None,
                "score_started_epoch_ms": int(self.score_started_epoch_ms)
                if self.score_started_epoch_ms
                else None,
                "score_finished_epoch_ms": int(self.score_finished_epoch_ms)
                if self.score_finished_epoch_ms
                else None,
                "run_finished_epoch_ms": int(run_finished) if run_finished else None,
                "pi_duration_ms": self._duration_ms(
                    self.pi_started_epoch_ms, self.pi_finished_epoch_ms
                ),
                "score_duration_ms": self._duration_ms(
                    self.score_started_epoch_ms, self.score_finished_epoch_ms
                ),
                "run_duration_ms": self._duration_ms(self.run_started_epoch_ms, run_finished),
            },
            "snapshots": {
                "task_sha256": self._sha256_file(self.run_dir / "task.md"),
                "run_md_sha256": self._sha256_file(self.run_dir / "RUN.md"),
                "run_contract_sha256": self._sha256_file(self.run_dir / "run.contract.json"),
                "result_schema_sha256": self._sha256_file(self.run_dir / "result.schema.json"),
                "prompt_sha256": self._sha256_file(self.run_dir / "prompt.txt"),
                "result_sha256": self._sha256_file(self.run_dir / "result.json"),
                "score_sha256": self._sha256_file(self.run_dir / "score.json"),
            },
            "invariants": {
                "task_exists": (self.run_dir / "task.md").exists(),
                "run_md_exists": (self.run_dir / "RUN.md").exists(),
                "run_contract_exists": (self.run_dir / "run.contract.json").exists(),
                "result_schema_exists": (self.run_dir / "result.schema.json").exists(),
                "result_template_exists": (self.run_dir / "result.template.json").exists(),
                "event_log_exists": (self.run_dir / "run-events.jsonl").exists(),
                "writeable_outputs_dir": os.access(self.run_dir / "outputs", os.W_OK),
                "score_available": (self.run_dir / "score.json").exists(),
                "overall_pass": score_payload.get("overall_pass"),
            },
            "audit": {
                "strict_mode": self.strict_mode not in {"0", "false", "False"},
                "force_rerun": self.force_rerun not in {"0", "false", "False"},
                "async_scoring": self.async_scoring,
                "allow_dangerous_eval": os.environ.get("HARNESS_ALLOW_DANGEROUS_EVAL")
                not in {None, "", "0", "false", "False"},
                "allow_network_tasks": os.environ.get("HARNESS_ALLOW_NETWORK_TASKS")
                not in {None, "", "0", "false", "False"},
                "model_timeout_seconds": int(self.model_timeout_seconds),
                "retry_count": int(self.retry_count),
                "score_failure_classifications": score_payload.get("failure_classifications", []),
                "secret_scan": {
                    "scanned_path_count": (score_payload.get("secret_scan") or {}).get(
                        "scanned_path_count"
                    ),
                    "skipped_path_count": (score_payload.get("secret_scan") or {}).get(
                        "skipped_path_count"
                    ),
                    "skipped_reason_counts": (score_payload.get("secret_scan") or {}).get(
                        "skipped_reason_counts"
                    ),
                },
            },
            "orchestration": {
                "worker_id": self.worker_id,
                "attempt": self.attempt,
                "max_run_wall_clock_seconds": int(self.max_run_wall_clock_seconds)
                if self.max_run_wall_clock_seconds.isdigit()
                else 0,
                "run_deadline_ms": self.run_deadline_ms or None,
                "queue_wait_ms": self.queue_wait_ms_int,
                "score_wait_ms": self.score_wait_ms_int,
                "max_eval_commands": self.max_eval_commands_int,
                "max_eval_timeout_seconds": int(self.eval_timeout_seconds)
                if self.eval_timeout_seconds.isdigit()
                else 0,
            },
            "execution": {
                "contract_version": self.run_contract_version or None,
                "profile": self.execution_profile,
                "policy_path": self.policy_path,
                "transport_mode": self.transport_mode,
                "selected_model": self.model or None,
                "fallback_model": self.model_fallback or None,
            },
            "contracts": {
                "run_contract_version": self.run_contract_version or None,
                "run_manifest_version": "v1",
                "score_version": "v1",
                "context_manifest_version": context_payload.get("context_manifest_version"),
                "capability_manifest_version": capability_payload.get(
                    "capability_manifest_version"
                ),
                "benchmark_report_contract": "contracts/benchmark-report-v1.schema.json",
                "release_gate_contract": "contracts/release-gate-v1.schema.json",
                "candidate_manifest_contract": "contracts/candidate-manifest-v1.schema.json",
                "candidate_report_contract": "contracts/candidate-report-v1.schema.json",
                "capability_library_contract": "contracts/capability-library-v1.schema.json",
            },
            "context": {
                "enabled": self.context_enabled == "1",
                "manifest_path": self.context_manifest_rel or None,
                "summary_path": self.context_summary_rel or None,
                "source_run_ids": [item for item in self.context_source_run_ids.split(",") if item],
                "bootstrap_mode": self.context_bootstrap_mode or None,
                "retrieval_profile_id": context_payload.get("retrieval_profile_id"),
                "retrieval_profile_fingerprint": context_payload.get(
                    "retrieval_profile_fingerprint"
                ),
                "candidate_run_count": context_payload.get("candidate_run_count"),
                "eligible_run_count": context_payload.get("eligible_run_count"),
                "selected_count": context_payload.get("selected_count"),
                "abstained": context_payload.get("abstained"),
                "abstention_reason": context_payload.get("abstention_reason"),
                "top_candidate_score": context_payload.get("top_candidate_score"),
                "top_candidate_score_margin": context_payload.get("top_candidate_score_margin"),
                "ranking_latency_ms": context_payload.get("ranking_latency_ms"),
                "artifact_bytes_copied": context_payload.get("artifact_bytes_copied"),
                "retrieval_candidate_id": context_payload.get("retrieval_candidate_id"),
                "retrieval_candidate_mode": context_payload.get("retrieval_candidate_mode"),
                "retriever_version": context_payload.get("retriever_version"),
                "reranker_version": context_payload.get("reranker_version"),
                "abstention_model_version": context_payload.get("abstention_model_version"),
                "guardrails_path": "outputs/guardrails.json",
            },
            "capabilities": {
                "enabled": self.capabilities_enabled,
                "library_path": capability_payload.get("library_path")
                or self.capability_library_path
                or None,
                "library_fingerprint": capability_payload.get("library_fingerprint"),
                "manifest_path": self.capability_manifest_rel or None,
                "transport_mode": self.transport_mode,
                "subagents_allowed": self.subagents_allowed,
                "max_agents": self.subagent_max_agents,
                "allowed_profiles": list(self.allowed_subagent_profiles),
                "selected_profile": self.selected_capability_profile,
                "usage_path": capability_score.get("usage_path") or DEFAULT_USAGE_PATH,
                "usage_present": capability_score.get("usage_present"),
                "usage_valid": capability_score.get("usage_valid"),
                "usage_violations": list(capability_score.get("violations", [])),
                "spawned_profile_ids": list(capability_score.get("spawned_profile_ids", [])),
                "agent_count": capability_score.get("agent_count"),
                "total_prompt_tokens": capability_score.get("total_prompt_tokens"),
                "total_runtime_seconds": capability_score.get("total_runtime_seconds"),
            },
            "benchmark_eligibility": score_payload.get("benchmark_eligibility", {}),
            "promotion": score_payload.get("promotion_summary", {}),
            "candidates": {
                "retrieval": dict(self.retrieval_candidate_summary),
                "policy": {
                    **dict(self.policy_candidate_summary),
                    "recommendations": dict(self.policy_candidate_recommendations),
                    "applied": list(self.policy_candidate_applied),
                    "overrides": list(self.policy_candidate_overrides),
                },
                "model": {
                    **dict(self.model_candidate_summary),
                    "selected_model": self.model or None,
                    "fallback_model": self.model_fallback or None,
                    "selection_source": self.model_selection_source,
                },
                "bundle": dict(self.bundle_candidate_summary),
            },
            "planes": {
                "runtime": {
                    "stable_kernel": [
                        "starter/bin/run_task.py",
                        "starter/bin/prepare-context.py",
                        "starter/bin/score_run.py",
                    ]
                },
                "evidence": {
                    "manifest": "outputs/run_manifest.json",
                    "score": "score.json",
                    "transcript": "transcript.jsonl",
                },
                "learning": {
                    "context_manifest": self.context_manifest_rel or None,
                    "benchmark_contract": "contracts/benchmark-report-v1.schema.json",
                },
                "promotion": {
                    "release_gate_contract": "contracts/release-gate-v1.schema.json",
                    "provenance": "dist/*.provenance.json",
                },
            },
            "guardrails": {
                "policy_path": self.policy_path,
                "policy_fingerprint": self.policy.get("policy_fingerprint", ""),
                "policy_version_source": self._guardrail_policy_snapshot().get(
                    "source_of_truth", ""
                ),
                "decisions_recorded": len(self.guardrail_decisions),
            },
        }
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        write_json(self.manifest_path, manifest)

    def _load_runtime_candidates(self) -> None:
        self.retrieval_candidate = load_candidate_manifest("retrieval", repo_root=self.repo_root)
        self.policy_candidate = load_candidate_manifest("policy", repo_root=self.repo_root)
        self.model_candidate = load_candidate_manifest("model", repo_root=self.repo_root)
        self.bundle_candidate = load_candidate_manifest("bundle", repo_root=self.repo_root)
        self.retrieval_candidate_summary = candidate_summary(self.retrieval_candidate)
        self.policy_candidate_summary = candidate_summary(self.policy_candidate)
        self.model_candidate_summary = candidate_summary(self.model_candidate)
        self.bundle_candidate_summary = candidate_summary(self.bundle_candidate)

    def _recommendation_payload(self, name: str) -> tuple[Any, float]:
        recommendations = dict(candidate_runtime(self.policy_candidate).get("recommendations", {}))
        payload = recommendations.get(name)
        if isinstance(payload, dict) and "value" in payload:
            return payload.get("value"), float(payload.get("confidence", 0.0))
        if payload is None:
            return None, 0.0
        return payload, 0.0

    def _policy_model_predictions(self) -> dict[str, dict[str, Any]]:
        if not self.policy_candidate:
            return {}
        runtime = candidate_runtime(self.policy_candidate)
        model = dict(runtime.get("model", {}))
        artifact_paths = dict(model.get("artifact_paths", {}))
        model_path = pathlib.Path(str(artifact_paths.get("model_path", "")))
        if not model_path.is_file():
            return {}
        try:
            model_payload = json.loads(model_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        try:
            task_payload = parse_task_file(self.task_md)
        except Exception:
            return {}
        feature_payload = policy_feature_payload(
            {
                "features": {
                    "task_text": "\n".join(
                        [
                            str(task_payload.get("task_title", "")),
                            str(task_payload.get("sections", {}).get("Goal", "")),
                            str(task_payload.get("sections", {}).get("Constraints", "")),
                            str(task_payload.get("sections", {}).get("Done", "")),
                        ]
                    ).strip(),
                    "execution_profile": self.execution_profile,
                    "policy_path": self.policy_path,
                    "eval_command_count": len(task_payload.get("eval_commands", [])),
                    "required_artifact_count": len(task_payload.get("required_artifacts", [])),
                    "selected_source_count": 0,
                    "candidate_run_count": 0,
                    "duration_ms": 0,
                    "context_empty": True,
                    "failure_classification_count": 0,
                    "top_candidate_score": 0,
                    "ranking_latency_ms": 0,
                    "abstained": False,
                }
            }
        )
        return predict_policy_heads(model_payload, feature_payload)

    def _apply_policy_candidate(self) -> None:
        self.policy_candidate_recommendations = {}
        self.policy_candidate_applied = []
        self.policy_candidate_overrides = []
        if not self.policy_candidate:
            return
        runtime = candidate_runtime(self.policy_candidate)
        activation_threshold = float(runtime.get("activation_threshold", 0.0))
        mode = effective_candidate_mode(self.policy_candidate)
        predictions = self._policy_model_predictions()

        def prediction_payload(*names: str) -> tuple[Any, float]:
            for name in names:
                payload = predictions.get(name)
                if isinstance(payload, dict) and "value" in payload:
                    return payload.get("value"), float(payload.get("confidence", 0.0))
            for name in names:
                value, confidence = self._recommendation_payload(name)
                if value is not None:
                    return value, confidence
            defaults = dict(runtime.get("defaults", {}))
            for name in names:
                if name in defaults:
                    return defaults.get(name), 0.0
            return None, 0.0

        execution_profile, profile_confidence = prediction_payload("execution_profile")
        if execution_profile is not None:
            self.policy_candidate_recommendations["execution_profile"] = {
                "value": execution_profile,
                "confidence": profile_confidence,
            }
            if (
                mode == "active"
                and profile_confidence >= activation_threshold
                and execution_profile in EXECUTION_PROFILES
            ):
                self.execution_profile = str(execution_profile)
                self.policy_path = default_policy_path(self.execution_profile)
                self.policy_candidate_applied.append("execution_profile")

        retry_payload, retry_confidence = prediction_payload("retry_policy", "retry_limit")
        retry_limit = (
            retry_payload.get("retry_limit")
            if isinstance(retry_payload, dict)
            else retry_payload
        )
        if retry_limit is not None:
            self.policy_candidate_recommendations["retry_limit"] = {
                "value": retry_limit,
                "confidence": retry_confidence,
            }
            if mode == "active" and retry_confidence >= activation_threshold:
                self.retry_count = str(max(1, int(retry_limit)))
                self.policy_candidate_applied.append("retry_limit")

        retrieval_budget, retrieval_budget_confidence = prediction_payload("retrieval_budget")
        if isinstance(retrieval_budget, dict):
            clipped_budget = {
                "max_source_runs": min(
                    max(1, int(retrieval_budget.get("max_source_runs", 1))),
                    max(1, int(self.retrieval_budget_envelope.get("max_source_runs", 1))),
                ),
                "max_candidates": min(
                    max(1, int(retrieval_budget.get("max_candidates", 1))),
                    max(1, int(self.retrieval_budget_envelope.get("max_candidates", 1))),
                ),
            }
            self.policy_candidate_recommendations["retrieval_budget"] = {
                "value": dict(clipped_budget),
                "confidence": retrieval_budget_confidence,
            }
            if mode == "active" and retrieval_budget_confidence >= activation_threshold:
                self.retrieval_budget_overrides["max_source_runs"] = int(
                    clipped_budget["max_source_runs"]
                )
                self.retrieval_budget_overrides["max_candidates"] = int(
                    clipped_budget["max_candidates"]
                )
                if clipped_budget != {
                    "max_source_runs": int(retrieval_budget.get("max_source_runs", 1)),
                    "max_candidates": int(retrieval_budget.get("max_candidates", 1)),
                }:
                    self.policy_candidate_overrides.append("retrieval_budget_clipped")
                if self.retrieval_budget_overrides:
                    self.policy_candidate_applied.append("retrieval_budget")

        benchmark_eligible, benchmark_confidence = prediction_payload(
            "benchmark_eligibility", "benchmark_eligible"
        )
        if benchmark_eligible is not None:
            self.policy_candidate_recommendations["benchmark_eligible"] = {
                "value": bool(benchmark_eligible),
                "confidence": benchmark_confidence,
            }

        capability_profile, capability_confidence = prediction_payload("capability_profile")
        if capability_profile is not None:
            self.policy_candidate_recommendations["capability_profile"] = {
                "value": capability_profile,
                "confidence": capability_confidence,
            }
            if (
                mode == "active"
                and capability_confidence >= activation_threshold
                and capability_profile in self.allowed_subagent_profiles
            ):
                self.allowed_subagent_profiles = [str(capability_profile)]
                self.selected_capability_profile = str(capability_profile)
                self.policy_candidate_applied.append("capability_profile")

    def _apply_model_candidate(self) -> None:
        self.model_selection_source = "cli_or_default"
        self.model_fallback = ""
        if not self.model_candidate:
            return
        runtime = candidate_runtime(self.model_candidate)
        activation_threshold = float(runtime.get("activation_threshold", 0.0))
        confidence = float(runtime.get("confidence", 1.0))
        self.model_fallback = str(runtime.get("fallback_model", "")).strip()
        if self.model:
            self.model_selection_source = "explicit_cli_model"
            return
        primary_model = str(runtime.get("primary_model", "")).strip()
        if (
            effective_candidate_mode(self.model_candidate) == "active"
            and primary_model
            and confidence >= activation_threshold
        ):
            self.model = primary_model
            self.model_selection_source = "model_candidate"
        elif primary_model:
            self.model_selection_source = "model_candidate_shadow"

    def _duration_ms(self, start: str | int | None, end: str | int | None) -> int | None:
        if not start or not end:
            return None
        try:
            return max(0, int(end) - int(start))
        except (TypeError, ValueError):
            return None

    def _truncate_file(self, path: pathlib.Path, max_bytes: int) -> None:
        if not path.exists():
            return
        try:
            if path.stat().st_size <= max_bytes:
                return
            with path.open("rb") as handle:
                handle.seek(max(0, path.stat().st_size - max_bytes))
                payload = handle.read()
            path.write_bytes(payload)
        except OSError:
            pass

    def _sha256_file(self, path: pathlib.Path) -> str | None:
        from hashlib import sha256

        try:
            return sha256(path.read_bytes()).hexdigest()
        except FileNotFoundError:
            return None

    def _git_sha(self) -> str | None:
        completed = self._run_command(
            ["git", "-C", str(self.repo_root), "rev-parse", "HEAD"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if completed.returncode != 0:
            return None
        return completed.stdout.strip()

    def _handle_signal(self, signum: int, _frame) -> None:
        signal_name = signal.Signals(signum).name
        self.error_code = "cancelled"
        self.phase = "cancelled"
        self._write_state("cancelled")
        self.run_finished_epoch_ms = now_ms()
        self._log_event(
            self.phase,
            f"received {signal_name}",
            self.error_code,
            state_before="running",
            state_after="cancelled",
            extra={"heartbeat_reason": "signal"},
        )
        self._write_manifest("cancelled", self.phase, self.error_code, self.run_finished_epoch_ms)
        raise SystemExit(130)

    def _restore_signals(self) -> None:
        if self._orig_sigint is not None:
            signal.signal(signal.SIGINT, self._orig_sigint)
        if self._orig_sigterm is not None:
            signal.signal(signal.SIGTERM, self._orig_sigterm)

    def _install_signals(self) -> None:
        self._orig_sigint = signal.getsignal(signal.SIGINT)
        self._orig_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _validate(self) -> None:
        self.phase = "validate"
        self._log_event(self.phase, "starting run validation")

        for required_path in (self.task_md, self.run_md):
            if not required_path.exists():
                self._fail_contract_check(
                    f"missing required file: {required_path}",
                    error_code="contract_invalid",
                    exit_code=2,
                )

        if (self.repo_root / "result.schema.json").exists():
            shutil.copy2(self.repo_root / "result.schema.json", self.run_schema_path)

        if not self.result_template_path.exists():
            write_json(self.result_template_path, make_result_template())

        if not self.run_contract_path.exists():
            write_json(
                self.run_contract_path,
                default_run_contract(version="v2", execution_profile="strict"),
            )

        run_contract = load_run_contract(self.run_contract_path)
        settings = resolve_execution_settings(
            run_contract,
            profile_override=self.profile_override,
        )

        self.run_contract_version = settings["run_contract_version"]
        self.execution_profile = settings["execution_profile"]
        self.policy_path = settings["policy_path"]
        self.retrieval_budget_envelope = {
            "max_source_runs": max(1, int(settings.get("retrieval", {}).get("max_source_runs", 1))),
            "max_candidates": max(1, int(settings.get("retrieval", {}).get("max_candidates", 1))),
        }
        self.transport_mode = settings.get("transport_mode", "cli_json")
        self.capabilities_enabled = bool(settings.get("capabilities_enabled", False))
        self.capability_library_path = str(settings.get("capability_library_path") or "")
        self.capability_manifest_rel = str(settings.get("capability_manifest_path") or "")
        self.subagents_allowed = bool(settings.get("subagents_allowed", False))
        self.subagent_max_agents = int(settings.get("subagent_max_agents", 0) or 0)
        self.allowed_subagent_profiles = list(settings.get("allowed_subagent_profiles", []))
        self._load_runtime_candidates()
        self._apply_policy_candidate()
        self.policy = load_policy(self.policy_path, repo_root=self.repo_root)
        self._apply_model_candidate()
        self.context_enabled = "1" if settings["retrieval_enabled"] else "0"
        self.context_manifest_rel = settings["context_manifest_path"]
        self.context_summary_rel = settings["context_summary_path"]
        self.capability_library = None
        if self.capabilities_enabled:
            try:
                self.capability_library = load_capability_library(
                    self.capability_library_path,
                    repo_root=self.repo_root,
                )
            except ValueError as exc:
                self._fail_contract_check(
                    str(exc),
                    error_code="contract_invalid",
                    exit_code=2,
                )
            unknown_profiles = sorted(
                set(self.allowed_subagent_profiles)
                - set((self.capability_library or {}).get("subagent_profiles", {}))
            )
            if unknown_profiles:
                self._fail_contract_check(
                    "unknown subagent profile(s): " + ", ".join(unknown_profiles),
                    error_code="contract_invalid",
                    exit_code=2,
                )
        if self.subagents_allowed and self.transport_mode != "rpc":
            self._fail_contract_check(
                "subagent-capable runs require transport.mode to be rpc",
                error_code="contract_invalid",
                exit_code=2,
            )

        for exe in REQUIRED_RUN_TOOLS:
            if shutil.which(exe) is None:
                self._fail_contract_check(
                    f"missing required executable: {exe}",
                    error_code="contract_invalid",
                    exit_code=127,
                )

        if "/" in self.pi_bin:
            if not _is_executable_path(self.pi_bin):
                self._fail_contract_check(
                    f"pi executable is not runnable: {self.pi_bin}",
                    error_code="contract_invalid",
                    exit_code=127,
                )
        elif shutil.which(self.pi_bin) is None:
            self._fail_contract_check(
                "pi is not on PATH",
                error_code="contract_invalid",
                exit_code=127,
            )

        with (self.run_dir / "pi.version.txt").open("w", encoding="utf-8") as handle:
            completed = self._run_command(
                [self.pi_bin, "--version"], stdout=handle, stderr=handle, text=False
            )
        if completed.returncode != 0:
            self._fail_contract_check(
                "pi --version probe failed",
                error_code="contract_invalid",
                exit_code=127,
            )

        write_probe = self.run_dir / ".write-probe"
        write_probe.touch()
        write_probe.unlink()

        if self.strict_mode != "0":
            completed = self._run_command(
                [str(self.script_dir / "check-backpressure.sh"), str(self.run_dir)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=False,
            )
            if completed.returncode != 0:
                raise SystemExit(completed.returncode)

        completed = self._run_command(
            [str(self.script_dir / "check-run-contract.sh"), str(self.run_dir)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        if completed.returncode != 0:
            sys.stderr.write(completed.stdout)
            raise SystemExit(completed.returncode)

        auth_json = os.environ.get("HARNESS_PI_AUTH_JSON")
        if auth_json is not None:
            auth_path = pathlib.Path(auth_json)
            if not auth_path.exists():
                self._fail_contract_check(
                    f"HARNESS_PI_AUTH_JSON does not exist: {auth_json}",
                    error_code="contract_invalid",
                    exit_code=2,
                )
            shutil.copy2(
                auth_path,
                self.run_dir / "home" / ".pi" / "agent" / "auth.json",
            )

    def _evaluate_pre_run(self) -> bool:
        if not self.policy:
            self.policy = load_policy(self.policy_path, repo_root=self.repo_root)
        decision = self._evaluate_guardrail(
            "pre_run",
            context={
                "skip_run": False,
                "strict_profile": self.execution_profile == "strict",
                "execution_profile": self.execution_profile,
                "policy_path": self.policy_path,
            },
        )
        if decision["allowed"]:
            return True
        self.error_code = "guardrail_pre_run_denied"
        self._write_guardrails_artifact()
        self._log_event(
            "pre_run",
            "pre_run guardrail denied execution",
            error_code="guardrail_pre_run_denied",
            extra={"violations": decision["violations"]},
        )
        return False

    def _evaluate_pre_score_dispatch(self) -> bool:
        if self._pre_score_dispatch_decision is not None:
            return bool(self._pre_score_dispatch_decision.get("allowed", False))
        decision = self._evaluate_guardrail(
            "pre_score_dispatch",
            context={
                "skip_score": self.skip_score,
                "force_score": not self.skip_score,
                "policy_path": self.policy_path,
            },
        )
        self._pre_score_dispatch_decision = decision
        if decision["allowed"]:
            return True
        self.error_code = "guardrail_pre_score_dispatch_denied"
        self._write_guardrail_block_score_payload(
            "guardrail_pre_score_dispatch_denied",
            decision["violations"],
        )
        self._write_guardrails_artifact()
        return False

    def _prepare_context(self) -> None:
        if self.context_enabled != "1" or self.run_contract_version not in {"v2", "v3"}:
            return

        self.phase = "context"
        self._log_event(self.phase, "preparing retrieval context")

        env = self._with_pythonpath()
        env["HARNESS_EXECUTION_PROFILE"] = self.execution_profile
        if self.retrieval_budget_overrides.get("max_source_runs"):
            env["HARNESS_CONTEXT_MAX_SOURCE_RUNS"] = str(
                self.retrieval_budget_overrides["max_source_runs"]
            )
        if self.retrieval_budget_overrides.get("max_candidates"):
            env["HARNESS_CONTEXT_MAX_CANDIDATES"] = str(
                self.retrieval_budget_overrides["max_candidates"]
            )
        completed = self._run_command(
            [
                sys.executable,
                str(self.script_dir / "prepare-context.py"),
                str(self.run_dir),
                self.policy_path,
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        if completed.returncode != 0:
            raise SystemExit(completed.returncode)

        manifest_path = self.run_dir / self.context_manifest_rel
        if manifest_path.exists():
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.context_source_run_ids = ",".join(payload.get("selected_source_run_ids", []))
            self.context_bootstrap_mode = str(payload.get("index_mode", ""))
            pre_context_decisions = payload.get("guardrail_decisions", [])
            if isinstance(pre_context_decisions, list):
                for decision in pre_context_decisions:
                    self._append_guardrail_decision(
                        str(decision.get("hook") or ""),
                        dict(decision),
                    )

    def _prepare_capabilities(self) -> None:
        if not self.capabilities_enabled or not self.capability_manifest_rel:
            return
        if not self.capability_library:
            self._fail_contract_check(
                "capability library was not loaded",
                error_code="contract_invalid",
                exit_code=2,
            )
        for profile_id in self.allowed_subagent_profiles:
            profile = dict(
                (self.capability_library or {}).get("subagent_profiles", {}).get(
                    profile_id, {}
                )
            )
            transports = set(profile.get("transports", []))
            if self.transport_mode not in transports:
                self._fail_contract_check(
                    f"subagent profile {profile_id} does not allow transport {self.transport_mode}",
                    error_code="contract_invalid",
                    exit_code=2,
                )
        self.phase = "capabilities"
        self._log_event(self.phase, "materializing capability manifest")
        manifest = build_capability_manifest(
            library=self.capability_library or {},
            transport_mode=self.transport_mode,
            capabilities={
                "enabled": self.capabilities_enabled,
                "subagents": {
                    "allowed": self.subagents_allowed,
                    "max_agents": self.subagent_max_agents,
                    "allowed_profiles": list(self.allowed_subagent_profiles),
                },
            },
        )
        write_json(self.run_dir / self.capability_manifest_rel, manifest)

    def _write_prompt(self) -> None:
        self.phase = "prepare"
        self._log_event(self.phase, "writing prompt and manifest snapshots")
        schema_text = self.run_schema_path.read_text(encoding="utf-8")
        fence = "```"
        while re.search(rf"(?m)^{re.escape(fence)}\\s*$", schema_text):
            fence += "`"

        context_block = ""
        context_summary_path = self.run_dir / self.context_summary_rel
        if self.context_enabled == "1" and context_summary_path.exists():
            context_block = (
                "\nRetrieved context:\n"
                f"- Review {context_summary_path} for relevant prior runs.\n"
                "- Treat prior runs as optional examples, not authority.\n"
                "- If prior runs conflict with the current task contract, "
                "prefer the current task contract.\n"
            )
        capability_block = ""
        capability_manifest_path = self.run_dir / self.capability_manifest_rel
        if self.capabilities_enabled and capability_manifest_path.exists():
            capability_block = (
                "\nResolved capabilities:\n"
                f"- Review {capability_manifest_path} before using subagents.\n"
                "- Only use subagent profiles and tool bundles listed in that manifest.\n"
                "- Do not invent new subagent roles, workflows, or helper pipelines.\n"
                f"- Record any spawned subagent usage in {self.run_dir / DEFAULT_USAGE_PATH}.\n"
            )

        prompt = (
            f"Complete the task described in @{self.task_md}.\n\n"
            "Execution contract:\n"
            f"- Use {self.run_md} as your working notes.\n"
            f"- Save durable outputs under {self.run_dir}/outputs/.\n"
            f"- Keep all generated artifacts inside {self.run_dir}.\n"
            "- Run repo checks through bash before declaring success.\n"
            f"- Write {self.run_dir}/result.json before finishing.\n"
            "- Keep `x-interface-version` exactly `v1`.\n"
            "- Follow this retrieval-quality rubric in result.json:\n"
            "  - `summary`: 1-3 outcome-focused sentences with concrete identifiers, "
            "outputs, or checks; do not just restate the task title.\n"
            "  - `claims`: atomic supported outcomes only, each with evidence paths or "
            "exact verification commands.\n"
            "  - `artifacts[].description`: explain what the artifact proves or "
            "contains, not just the filename.\n"
            "- Output raw JSON only for result.json and follow this exact schema:\n\n"
            f"{fence}json\n"
            f"{schema_text.rstrip()}\n"
            f"{fence}\n"
            f"{context_block}"
            f"{capability_block}"
        )
        (self.run_dir / "prompt.txt").write_text(prompt, encoding="utf-8")
        self._write_manifest("running", self.phase, "", "")

    def _invoke_pi(self) -> int:
        timeout_seconds = self.timeout_seconds
        if self.run_deadline_ms > 0:
            remaining_ms = max(0, self.run_deadline_ms - now_ms())
            timeout_seconds = max(1, remaining_ms // 1000) if remaining_ms else 0
        if timeout_seconds <= 0:
            timeout_seconds = None

        command = [
            self.pi_bin,
            "--mode",
            "rpc" if self.transport_mode == "rpc" else "json",
            "--session-dir",
            str(self.run_dir / "session"),
            "--no-extensions",
            "--no-skills",
            "--no-prompt-templates",
            "--no-themes",
        ]
        if self.model:
            command.extend(["--model", self.model])
        command.extend(
            [f"@{self.task_md}", (self.run_dir / "prompt.txt").read_text(encoding="utf-8")]
        )

        with (
            (self.run_dir / "transcript.jsonl").open("ab") as stdout_handle,
            (self.run_dir / "pi.stderr.log").open("ab") as stderr_handle,
        ):
            env = self._context()
            env["HOME"] = str(self.run_dir / "home")
            env["HARNESS_TRANSPORT_MODE"] = self.transport_mode
            env["HARNESS_CAPABILITIES_ENABLED"] = "1" if self.capabilities_enabled else "0"
            env["HARNESS_CAPABILITY_MANIFEST_PATH"] = self.capability_manifest_rel
            env["HARNESS_ALLOWED_SUBAGENT_PROFILES"] = ",".join(self.allowed_subagent_profiles)
            env["HARNESS_SUBAGENT_MAX_AGENTS"] = str(self.subagent_max_agents)
            try:
                completed = self._run_command(
                    command,
                    env=env,
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                    cwd=self.repo_root,
                    text=False,
                    timeout=timeout_seconds,
                )
            except subprocess.TimeoutExpired:
                return 124
        return completed.returncode

    def _run_pi_loop(self) -> None:
        self.phase = "model_running"
        self._write_state("model_running")
        model_started = now_ms()
        self.model_started_epoch_ms = model_started
        self._log_event(
            self.phase,
            "starting pi execution",
            state_before="claimed",
            state_after="model_running",
        )
        attempt = 1
        self.pi_exit = 1
        while attempt <= self.retry_limit:
            if self.run_deadline_ms and now_ms() >= self.run_deadline_ms:
                self._log_event(
                    self.phase,
                    "run deadline exceeded before retry",
                    "deadline_exceeded",
                    state_before="model_running",
                    state_after="failed",
                    extra={"heartbeat_reason": "deadline_exceeded"},
                )
                self.error_code = "deadline_exceeded"
                break
            self._truncate_file(self.run_dir / "transcript.jsonl", self.max_transcript_bytes_int)
            self._truncate_file(self.run_dir / "pi.stderr.log", self.max_pi_stderr_bytes_int)
            self.pi_started_epoch_ms = now_ms()
            self._log_event(
                self.phase,
                "starting model attempt",
                state_before="model_running",
                state_after="model_running",
                extra={
                    "attempt": attempt,
                    "model_wait_ms": max(0, self.pi_started_epoch_ms - model_started),
                },
            )
            self.pi_exit = self._invoke_pi()
            self.pi_finished_epoch_ms = now_ms()
            (self.run_dir / "pi.exit_code.txt").write_text(f"{self.pi_exit}\n", encoding="utf-8")

            if self.pi_exit == 0:
                self._log_event(self.phase, "pi execution succeeded", state_before="model_running")
                break

            transcript = self.run_dir / "transcript.jsonl"
            transcript_empty = not transcript.exists() or transcript.stat().st_size == 0
            if transcript_empty and attempt < self.retry_limit:
                self._log_event(
                    self.phase,
                    "retrying pi startup failure",
                    "model_invocation_failed",
                    state_before="model_running",
                    state_after="model_running",
                    extra={"attempt": attempt},
                )
                self._sleep(attempt)
                attempt += 1
                continue
            break

        if self.pi_exit != 0 and not self.error_code:
            self.error_code = "model_invocation_failed"

    def _collect_git_metadata(self) -> None:
        completed = self._run_command(
            ["git", "-C", str(self.repo_root), "rev-parse", "--is-inside-work-tree"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        if completed.returncode == 0:
            self._run_command(
                ["git", "-C", str(self.repo_root), "status", "--short"],
                stdout=(self.run_dir / "git.status.txt").open("w", encoding="utf-8"),
                stderr=subprocess.DEVNULL,
                text=False,
            )
            self._run_command(
                ["git", "-C", str(self.repo_root), "diff", "--binary"],
                stdout=(self.run_dir / "patch.diff").open("w", encoding="utf-8"),
                stderr=subprocess.DEVNULL,
                text=False,
            )
        else:
            (self.run_dir / "git.status.txt").write_text("not a git repository\n", encoding="utf-8")
            (self.run_dir / "patch.diff").write_text("", encoding="utf-8")

    def _invoke_score(self) -> int:
        env = self._with_pythonpath(self._context())
        env["HARNESS_EXECUTION_PROFILE"] = self.execution_profile
        env["HARNESS_POLICY_PATH"] = self.policy_path
        env["HARNESS_TRANSPORT_MODE"] = self.transport_mode
        env["HARNESS_CONTEXT_ENABLED"] = self.context_enabled
        env["HARNESS_CONTEXT_MANIFEST_PATH"] = self.context_manifest_rel
        env["HARNESS_CONTEXT_SOURCE_RUN_IDS"] = self.context_source_run_ids
        env["HARNESS_CAPABILITIES_ENABLED"] = "1" if self.capabilities_enabled else "0"
        env["HARNESS_CAPABILITY_MANIFEST_PATH"] = self.capability_manifest_rel
        env["HARNESS_RETRIEVAL_CANDIDATE_ID"] = str(
            self.retrieval_candidate_summary.get("candidate_id") or ""
        )
        env["HARNESS_RETRIEVAL_CANDIDATE_MODE"] = str(
            self.retrieval_candidate_summary.get("mode") or "off"
        )
        env["HARNESS_POLICY_CANDIDATE_ID"] = str(
            self.policy_candidate_summary.get("candidate_id") or ""
        )
        env["HARNESS_POLICY_CANDIDATE_MODE"] = str(
            self.policy_candidate_summary.get("mode") or "off"
        )
        env["HARNESS_MODEL_CANDIDATE_ID"] = str(
            self.model_candidate_summary.get("candidate_id") or ""
        )
        env["HARNESS_MODEL_CANDIDATE_MODE"] = str(
            self.model_candidate_summary.get("mode") or "off"
        )
        env["HARNESS_BUNDLE_CANDIDATE_ID"] = str(
            self.bundle_candidate_summary.get("candidate_id") or ""
        )
        env["HARNESS_BUNDLE_CANDIDATE_MODE"] = str(
            self.bundle_candidate_summary.get("mode") or "off"
        )
        env["HARNESS_MODEL_SELECTED_MODEL"] = self.model or ""
        env["HARNESS_MODEL_FALLBACK_MODEL"] = self.model_fallback or ""
        if self.max_eval_commands_int > 0:
            env["HARNESS_MAX_EVAL_COMMANDS"] = str(self.max_eval_commands_int)
        if self.eval_timeout_seconds:
            env["HARNESS_EVAL_TIMEOUT_SECONDS"] = self.eval_timeout_seconds
        env["HARNESS_GUARDRAILS_PATH"] = str(self.guardrails_path)
        env["HARNESS_WORKER_ID"] = self.worker_id
        env["HARNESS_ATTEMPT"] = str(self.attempt)

        completed = self._run_command(
            [
                sys.executable,
                str(self.script_dir / "score_run.py"),
                str(self.task_md),
                str(self.run_dir),
                str(self.run_dir / "pi.exit_code.txt"),
                str(self.run_dir / "score.json"),
                str(self.run_schema_path),
                str(self.event_log_path),
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=self.script_dir,
            text=True,
        )
        return completed.returncode

    def _run_score_loop(self) -> None:
        self.phase = "scoring"
        if not self._evaluate_pre_score_dispatch():
            self.score_started_epoch_ms = now_ms()
            self.score_finished_epoch_ms = now_ms()
            self._log_event(
                self.phase,
                "score dispatch blocked by guardrail policy",
                "guardrail_pre_score_dispatch_denied",
                extra={"violations": self._pre_score_dispatch_decision.get("violations", [])},
            )
            return
        self._write_state("scoring")
        self.score_started_epoch_ms = now_ms()
        self._log_event(
            self.phase,
            "starting scoring",
            state_before="model_complete",
            state_after="scoring",
            extra={
                "retry_limit": self.score_retry_limit,
                "model_wait_ms": max(
                    0,
                    self.score_started_epoch_ms
                    - int(self.pi_finished_epoch_ms or self.run_started_epoch_ms),
                ),
            },
        )
        attempt = 1
        score_exit = 1
        while attempt <= self.score_retry_limit:
            if self.run_deadline_ms and now_ms() >= self.run_deadline_ms:
                self._log_event(
                    self.phase,
                    "run deadline exceeded during scoring",
                    "deadline_exceeded",
                    state_before="scoring",
                    state_after="failed",
                    extra={"heartbeat_reason": "deadline_exceeded"},
                )
                self.error_code = "deadline_exceeded"
                break
            score_exit = self._invoke_score()
            if score_exit == 0:
                self._log_event(
                    self.phase,
                    "scoring succeeded",
                    state_before="scoring",
                    state_after="scoring",
                )
                break
            if attempt >= self.score_retry_limit:
                break
            self._log_event(
                self.phase,
                "retrying score generation",
                "eval_failed",
                state_before="scoring",
                state_after="scoring",
                extra={"attempt": attempt},
            )
            self._sleep(attempt)
            attempt += 1
        self.score_finished_epoch_ms = now_ms()

    def _finalize_model_only(self, *, queue_score: bool) -> None:
        self.phase = "model_complete"
        self.error_code = ""
        if queue_score and not self._evaluate_pre_score_dispatch():
            self._log_event(
                self.phase,
                "score dispatch blocked by guardrail policy",
                error_code="guardrail_pre_score_dispatch_denied",
                extra={"violations": self._pre_score_dispatch_decision.get("violations", [])},
            )
            queue_score = False
        if queue_score:
            self._write_state("score_pending")
        else:
            self._write_state("model_complete")
        self.run_finished_epoch_ms = now_ms()
        if queue_score:
            self._enqueue_score_job()
        self._write_manifest(
            "score_pending" if queue_score else "model_complete",
            self.phase,
            self.error_code,
            self.run_finished_epoch_ms,
        )
        self._log_event(
            self.phase,
            "model execution complete; scoring deferred",
            state_before="model_running",
            state_after="model_complete",
            extra={
                "state": "model_complete",
                "pi_exit_code": int(
                    (self.run_dir / "pi.exit_code.txt").read_text(encoding="utf-8").strip() or 1
                ),
            },
        )
        print("run state: model_complete")
        print(f"run dir: {self.run_dir}")
        print(f"pi exit code: {self.pi_exit}")
        raise SystemExit(0)

    def _final_error_code(self) -> str:
        score_path = self.run_dir / "score.json"
        if not score_path.exists():
            return "eval_failed"

        try:
            payload = json.loads(score_path.read_text(encoding="utf-8"))
        except Exception:
            return "eval_failed"

        pi_exit = int((self.run_dir / "pi.exit_code.txt").read_text(encoding="utf-8").strip() or 1)
        if payload.get("overall_pass") is True:
            return ""
        if payload.get("overall_error_code"):
            return str(payload["overall_error_code"])
        if pi_exit != 0:
            return "model_invocation_failed"
        return "eval_failed"

    def _finalize(self) -> None:
        self.phase = "finalize"
        self.run_finished_epoch_ms = now_ms()
        self.error_code = self._final_error_code()
        self._write_state("complete")
        self._write_manifest("complete", self.phase, self.error_code, self.run_finished_epoch_ms)
        self._log_event(
            self.phase,
            "run complete",
            self.error_code,
            state_before="scoring" if self.score_started_epoch_ms else "model_complete",
            state_after="complete",
        )

        print("run state: complete")
        print(f"run complete: {self.run_dir}")
        print(f"pi exit code: {self.pi_exit}")
        print(f"score: {self.run_dir}/score.json")
        raise SystemExit(0)

    def run(self) -> int:
        self._check_cancelled("startup")
        if not self.run_dir.exists() or not self.run_dir.is_dir():
            print(f"run directory not found: {self.run_dir}", file=sys.stderr)
            raise SystemExit(2)

        if self.profile_override not in {"", None} | EXECUTION_PROFILES:
            print(f"unsupported profile override: {self.profile_override}", file=sys.stderr)
            raise SystemExit(2)

        self._install_signals()
        self._ensure_base_dirs()

        state = self._resolve_initial_state()
        if state == "running":
            return 3
        if state == "complete":
            return 0
        if state == "partial":
            self._archive_partial_run()

        self._acquire_lock()
        try:
            if self.score_only:
                self._log_event(
                    "score_only",
                    "running score-only pass",
                    state_before="queued",
                    state_after="scoring",
                )
                self._write_state("scoring")
                self._validate()
                self._run_score_loop()
                self._finalize()
                return 0

            self._validate()
            if not self._evaluate_pre_run():
                self._log_event(
                    "pre_run",
                    "pre_run guardrail denied execution",
                    error_code="guardrail_pre_run_denied",
                    extra={
                        "violations": self.guardrail_decisions[-1]["violations"]
                        if self.guardrail_decisions
                        else []
                    },
                )
                self._write_guardrails_artifact()
                self._write_manifest("failed", "pre_run", "guardrail_pre_run_denied", now_ms())
                return 2
            self._check_cancelled("prepare")
            self._prepare_context()
            self._prepare_capabilities()
            self._write_prompt()
            self._check_cancelled("model_running")
            self._run_pi_loop()
            self._check_cancelled("collect_git_metadata")
            self._collect_git_metadata()
            if self.skip_score or self.async_scoring:
                self._finalize_model_only(queue_score=self.async_scoring and not self.skip_score)
                return 0

            self._run_score_loop()
            self._finalize()
        finally:
            self._cleanup_lock()
            self._restore_signals()
        return 0

    def _ensure_base_dirs(self) -> None:
        for directory in BASE_DIRECTORIES:
            (self.run_dir / directory).mkdir(parents=True, exist_ok=True)
        (self.run_dir / "home" / ".pi" / "agent").mkdir(parents=True, exist_ok=True)


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    try:
        runner = RunTaskRunner(arguments)
        return runner.run()
    except SystemExit as exc:
        return int(exc.code or 0)
    except Exception as exc:  # pragma: no cover - defensive fallback
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
