#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import pathlib
import signal
import subprocess  # nosec B404 - harness scoring executes argv-based local evaluation commands
import sys
import time
from dataclasses import dataclass
from typing import Any

from harnesslib import (
    canonicalize_json_file,
    env_flag,
    now_utc,
    parse_task_file,
    scan_paths_for_secrets,
    sha256_file,
    validate_result_payload,
    write_json,
)


@dataclass(frozen=True)
class ScoreContext:
    task_path: pathlib.Path
    run_dir: pathlib.Path
    exit_code_path: pathlib.Path
    out_path: pathlib.Path
    schema_path: pathlib.Path
    event_log_path: pathlib.Path
    repo_root: pathlib.Path


def usage() -> None:
    print(
        "usage: score_run.py /path/to/task.md /path/to/run-dir /path/to/pi-exit-code.txt "
        "/path/to/out-score.json [/path/to/result-schema.json] [/path/to/run-events.jsonl]",
        file=sys.stderr,
    )


def resolve_repo_root(run_dir: pathlib.Path) -> pathlib.Path:
    if run_dir.parent.name == "runs":
        return run_dir.parent.parent.resolve()
    return run_dir.parent.resolve()


def build_context(argv: list[str]) -> ScoreContext:
    run_dir = pathlib.Path(argv[1]).resolve()
    return ScoreContext(
        task_path=pathlib.Path(argv[0]).resolve(),
        run_dir=run_dir,
        exit_code_path=pathlib.Path(argv[2]).resolve(),
        out_path=pathlib.Path(argv[3]).resolve(),
        schema_path=pathlib.Path(
            argv[4] if len(argv) >= 5 else (run_dir / "result.schema.json")
        ).resolve(),
        event_log_path=pathlib.Path(
            argv[5] if len(argv) == 6 else (run_dir / "run-events.jsonl")
        ).resolve(),
        repo_root=resolve_repo_root(run_dir),
    )


def append_event(
    context: ScoreContext,
    phase: str,
    message: str,
    *,
    error_code: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    payload = {
        "ts": now_utc(),
        "trace_id": context.run_dir.name,
        "run_id": context.run_dir.name,
        "phase": phase,
        "duration_ms": None,
        "error_code": error_code or None,
        "message": message,
    }
    if extra:
        payload.update(extra)
    context.event_log_path.parent.mkdir(parents=True, exist_ok=True)
    with context.event_log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


cancel_reason: str | None = None


def handle_signal(signum: int, _frame: Any) -> None:
    global cancel_reason
    cancel_reason = signal.Signals(signum).name
    raise RuntimeError(f"received {cancel_reason}")


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def load_json(path: pathlib.Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def discover_secret_scan_paths(context: ScoreContext) -> list[pathlib.Path]:
    candidates: list[pathlib.Path] = []
    for child in context.run_dir.rglob("*"):
        if child.is_dir():
            continue
        if (
            "home" in child.parts
            or "session" in child.parts
            or "recovery" in child.parts
        ):
            continue
        candidates.append(child)
    return candidates


def _result_log_paths(
    score_dir: pathlib.Path,
    index: int,
) -> tuple[pathlib.Path, pathlib.Path]:
    return (
        score_dir / f"eval-{index}.stdout.log",
        score_dir / f"eval-{index}.stderr.log",
    )


def _evaluation_result(
    context: ScoreContext,
    detail: dict[str, Any],
    stdout_path: pathlib.Path,
    stderr_path: pathlib.Path,
    *,
    stdout: str,
    stderr: str,
    exit_code: int | None,
    duration: float,
    passed: bool,
    blocked: bool,
    failure_classification: str | None = None,
) -> dict[str, Any]:
    stdout_path.write_text(stdout, encoding="utf-8")
    stderr_path.write_text(stderr, encoding="utf-8")
    result = {
        "command": detail["raw"],
        "argv": detail["argv"],
        "exit_code": exit_code,
        "duration": duration,
        "stdout_path": str(stdout_path.relative_to(context.run_dir)),
        "stderr_path": str(stderr_path.relative_to(context.run_dir)),
        "passed": passed,
        "blocked": blocked,
    }
    if failure_classification:
        result["failure_classification"] = failure_classification
    return result


def _blocked_evaluation_result(
    context: ScoreContext,
    detail: dict[str, Any],
    score_dir: pathlib.Path,
    index: int,
    *,
    message: str,
    failure_classification: str,
) -> dict[str, Any]:
    stdout_path, stderr_path = _result_log_paths(score_dir, index)
    return _evaluation_result(
        context,
        detail,
        stdout_path,
        stderr_path,
        stdout="",
        stderr=message + "\n",
        exit_code=None,
        duration=0.0,
        passed=False,
        blocked=True,
        failure_classification=failure_classification,
    )


def run_evaluation(
    context: ScoreContext,
    detail: dict[str, Any],
    score_dir: pathlib.Path,
    index: int,
    *,
    eval_timeout_seconds: int,
    allow_dangerous_eval: bool,
    allow_network_tasks: bool,
) -> dict[str, Any]:
    if detail["requires_opt_in"] and not allow_dangerous_eval:
        return _blocked_evaluation_result(
            context,
            detail,
            score_dir,
            index,
            message="blocked by eval policy: " + "; ".join(detail["dangerous_reasons"]),
            failure_classification="contract_invalid",
        )

    if detail["network_access"] and not allow_network_tasks:
        return _blocked_evaluation_result(
            context,
            detail,
            score_dir,
            index,
            message="blocked by network policy: HARNESS_ALLOW_NETWORK_TASKS=1 required",
            failure_classification="contract_invalid",
        )

    stdout_path, stderr_path = _result_log_paths(score_dir, index)
    started = time.monotonic()
    try:
        proc = subprocess.run(  # nosec B603 - eval uses parsed argv with shell disabled
            detail["argv"],
            cwd=str(context.repo_root),
            text=True,
            capture_output=True,
            timeout=eval_timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        return _evaluation_result(
            context,
            detail,
            stdout_path,
            stderr_path,
            stdout=exc.stdout or "",
            stderr=(exc.stderr or "") + f"\neval timed out after {eval_timeout_seconds}s\n",
            exit_code=124,
            duration=round(time.monotonic() - started, 3),
            passed=False,
            blocked=False,
            failure_classification="eval_failed",
        )

    return _evaluation_result(
        context,
        detail,
        stdout_path,
        stderr_path,
        stdout=proc.stdout or "",
        stderr=proc.stderr or "",
        exit_code=proc.returncode,
        duration=round(time.monotonic() - started, 3),
        passed=proc.returncode == 0,
        blocked=False,
    )


def _read_pi_exit_code(exit_code_path: pathlib.Path) -> int:
    try:
        return int(exit_code_path.read_text(encoding="utf-8").strip())
    except Exception:
        return 1


def _required_artifact_status(
    context: ScoreContext, required_paths: list[str]
) -> tuple[list[dict[str, Any]], set[str]]:
    payload: list[dict[str, Any]] = []
    failure_classifications: set[str] = set()
    for rel_path in required_paths:
        candidate = pathlib.Path(rel_path)
        exists = (
            candidate.exists()
            if candidate.is_absolute()
            else (context.run_dir / candidate).exists()
        )
        payload.append({"path": rel_path, "exists": exists})
        if not exists:
            failure_classifications.add("eval_failed")
    return payload, failure_classifications


def _result_validation_payload(
    context: ScoreContext,
) -> tuple[bool, bool, list[dict[str, Any]]]:
    result_json_path = context.run_dir / "result.json"
    result_json_present = result_json_path.exists()
    result_json_validations: list[dict[str, Any]] = []
    result_payload = None
    schema_payload = load_json(context.schema_path) if context.schema_path.exists() else None

    if result_json_present:
        try:
            result_payload = canonicalize_json_file(result_json_path)
        except Exception as exc:
            result_json_validations.append(
                {
                    "field": "result_json",
                    "expected": "valid JSON object",
                    "observed": f"json parse error: {exc}",
                }
            )
        else:
            if not isinstance(result_payload, dict):
                result_json_validations.append(
                    {
                        "field": "result_json",
                        "expected": "JSON object",
                        "observed": str(type(result_payload).__name__),
                    }
                )
            else:
                result_json_validations.extend(
                    validate_result_payload(result_payload, schema_payload)
                )
    else:
        result_json_validations.append(
            {
                "field": "result_json",
                "expected": "result.json must exist",
                "observed": "missing file",
            }
        )

    return result_json_present, len(result_json_validations) == 0, result_json_validations


def build_score_payload(context: ScoreContext, *, cancelled: bool = False) -> dict[str, Any]:
    score_dir = context.run_dir / "score"
    score_dir.mkdir(parents=True, exist_ok=True)

    parsed_task = parse_task_file(context.task_path)
    allow_dangerous_eval = env_flag("HARNESS_ALLOW_DANGEROUS_EVAL", default=False)
    allow_network_tasks = env_flag("HARNESS_ALLOW_NETWORK_TASKS", default=False)
    eval_timeout_seconds = int(os.environ.get("HARNESS_EVAL_TIMEOUT_SECONDS", "300"))

    append_event(
        context,
        "score",
        "starting score generation",
        extra={
            "allow_dangerous_eval": allow_dangerous_eval,
            "allow_network_tasks": allow_network_tasks,
        },
    )

    evaluations: list[dict[str, Any]] = []
    failure_classifications: set[str] = set()
    if not parsed_task["ok"]:
        failure_classifications.add("contract_invalid")

    for index, detail in enumerate(parsed_task["eval_command_details"], start=1):
        evaluation = run_evaluation(
            context,
            detail,
            score_dir,
            index,
            eval_timeout_seconds=eval_timeout_seconds,
            allow_dangerous_eval=allow_dangerous_eval,
            allow_network_tasks=allow_network_tasks,
        )
        evaluations.append(evaluation)
        if not evaluation["passed"]:
            failure_classifications.add(evaluation.get("failure_classification", "eval_failed"))

    required_artifacts, artifact_failures = _required_artifact_status(
        context, parsed_task["required_artifacts"]
    )
    failure_classifications.update(artifact_failures)

    pi_exit_code = _read_pi_exit_code(context.exit_code_path)
    if pi_exit_code != 0:
        failure_classifications.add("model_invocation_failed")

    (
        result_json_present,
        result_json_valid_schema,
        result_json_validations,
    ) = _result_validation_payload(context)
    if not result_json_valid_schema:
        failure_classifications.add("result_invalid")

    secret_scan_paths = discover_secret_scan_paths(context)
    secret_findings = scan_paths_for_secrets(secret_scan_paths)
    if secret_findings:
        failure_classifications.add("eval_failed")

    if cancelled:
        failure_classifications.add("eval_failed")

    overall_pass = len(failure_classifications) == 0
    overall_error_code = "none" if overall_pass else ",".join(sorted(failure_classifications))

    payload = {
        "pi_exit_code": pi_exit_code,
        "result_json_present": result_json_present,
        "result_json_valid_minimal": result_json_valid_schema,
        "result_json_valid_schema": result_json_valid_schema,
        "result_json_validations": result_json_validations,
        "result_json_schema": {
            "schema_path": str(context.schema_path),
            "schema_available": context.schema_path.is_file(),
            "schema_sha256": sha256_file(context.schema_path),
        },
        "evaluations": evaluations,
        "required_artifacts": required_artifacts,
        "task_parse": {
            "ok": parsed_task["ok"],
            "errors": parsed_task["errors"],
            "dangerous_eval_commands": parsed_task["dangerous_eval_commands"],
        },
        "secret_scan": {
            "paths_scanned": len(secret_scan_paths),
            "findings": secret_findings,
        },
        "failure_classifications": sorted(failure_classifications),
        "overall_error_code": overall_error_code,
        "overall_pass": overall_pass,
        "cancelled": cancelled,
    }
    append_event(
        context,
        "score",
        "score generation complete",
        error_code="" if overall_pass else overall_error_code,
        extra={"overall_pass": overall_pass},
    )
    return payload


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) not in (4, 5, 6):
        usage()
        return 2

    context = build_context(args)

    try:
        score_payload = build_score_payload(context, cancelled=False)
    except BaseException as exc:  # noqa: BLE001
        append_event(context, "score", "score generation interrupted", error_code="eval_failed")
        partial_payload = build_score_payload(context, cancelled=True)
        partial_payload["interruption"] = str(exc)
        write_json(context.out_path, partial_payload)
        print(context.out_path)
        raise
    else:
        write_json(context.out_path, score_payload)
        print(context.out_path)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
