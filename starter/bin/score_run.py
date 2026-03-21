#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import pathlib
import signal
import subprocess
import sys
import time
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


def append_event(
    phase: str,
    message: str,
    *,
    error_code: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    payload = {
        "ts": now_utc(),
        "trace_id": run_dir.name,
        "run_id": run_dir.name,
        "phase": phase,
        "duration_ms": None,
        "error_code": error_code or None,
        "message": message,
    }
    if extra:
        payload.update(extra)
    event_log_path.parent.mkdir(parents=True, exist_ok=True)
    with event_log_path.open("a", encoding="utf-8") as handle:
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


def discover_secret_scan_paths() -> list[pathlib.Path]:
    candidates: list[pathlib.Path] = []
    for child in run_dir.rglob("*"):
        if child.is_dir():
            continue
        if "home" in child.parts or "session" in child.parts or "recovery" in child.parts:
            continue
        candidates.append(child)
    return candidates


def run_evaluation(
    detail: dict[str, Any],
    score_dir: pathlib.Path,
    index: int,
    *,
    eval_timeout_seconds: int,
    allow_dangerous_eval: bool,
    allow_network_tasks: bool,
) -> dict[str, Any]:
    stdout_path = score_dir / f"eval-{index}.stdout.log"
    stderr_path = score_dir / f"eval-{index}.stderr.log"

    if detail["requires_opt_in"] and not allow_dangerous_eval:
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text(
            "blocked by eval policy: " + "; ".join(detail["dangerous_reasons"]) + "\n",
            encoding="utf-8",
        )
        return {
            "command": detail["raw"],
            "argv": detail["argv"],
            "exit_code": None,
            "duration": 0.0,
            "stdout_path": str(stdout_path.relative_to(run_dir)),
            "stderr_path": str(stderr_path.relative_to(run_dir)),
            "passed": False,
            "blocked": True,
            "failure_classification": "contract_invalid",
        }

    if detail["network_access"] and not allow_network_tasks:
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text(
            "blocked by network policy: HARNESS_ALLOW_NETWORK_TASKS=1 required\n",
            encoding="utf-8",
        )
        return {
            "command": detail["raw"],
            "argv": detail["argv"],
            "exit_code": None,
            "duration": 0.0,
            "stdout_path": str(stdout_path.relative_to(run_dir)),
            "stderr_path": str(stderr_path.relative_to(run_dir)),
            "passed": False,
            "blocked": True,
            "failure_classification": "contract_invalid",
        }

    started = time.monotonic()
    try:
        proc = subprocess.run(
            detail["argv"],
            cwd=str(repo_root),
            text=True,
            capture_output=True,
            timeout=eval_timeout_seconds,
        )
        duration = round(time.monotonic() - started, 3)
        stdout_path.write_text(proc.stdout or "", encoding="utf-8")
        stderr_path.write_text(proc.stderr or "", encoding="utf-8")
        return {
            "command": detail["raw"],
            "argv": detail["argv"],
            "exit_code": proc.returncode,
            "duration": duration,
            "stdout_path": str(stdout_path.relative_to(run_dir)),
            "stderr_path": str(stderr_path.relative_to(run_dir)),
            "passed": proc.returncode == 0,
            "blocked": False,
        }
    except subprocess.TimeoutExpired as exc:
        duration = round(time.monotonic() - started, 3)
        stdout_path.write_text((exc.stdout or ""), encoding="utf-8")
        stderr_path.write_text(
            ((exc.stderr or "") + f"\neval timed out after {eval_timeout_seconds}s\n"),
            encoding="utf-8",
        )
        return {
            "command": detail["raw"],
            "argv": detail["argv"],
            "exit_code": 124,
            "duration": duration,
            "stdout_path": str(stdout_path.relative_to(run_dir)),
            "stderr_path": str(stderr_path.relative_to(run_dir)),
            "passed": False,
            "blocked": False,
            "failure_classification": "eval_failed",
        }


def build_score_payload(cancelled: bool = False) -> dict[str, Any]:
    score_dir = run_dir / "score"
    score_dir.mkdir(parents=True, exist_ok=True)

    parsed_task = parse_task_file(task_path)
    allow_dangerous_eval = env_flag("HARNESS_ALLOW_DANGEROUS_EVAL", default=False)
    allow_network_tasks = env_flag("HARNESS_ALLOW_NETWORK_TASKS", default=False)
    eval_timeout_seconds = int(os.environ.get("HARNESS_EVAL_TIMEOUT_SECONDS", "300"))

    append_event(
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

    required_artifacts = []
    for rel_path in parsed_task["required_artifacts"]:
        candidate = pathlib.Path(rel_path)
        exists = candidate.exists() if candidate.is_absolute() else (run_dir / candidate).exists()
        required_artifacts.append({"path": rel_path, "exists": exists})
        if not exists:
            failure_classifications.add("eval_failed")

    try:
        pi_exit_code = int(exit_code_path.read_text(encoding="utf-8").strip())
    except Exception:
        pi_exit_code = 1
    if pi_exit_code != 0:
        failure_classifications.add("model_invocation_failed")

    result_json_path = run_dir / "result.json"
    result_json_present = result_json_path.exists()
    result_json_validations: list[dict[str, Any]] = []
    result_payload = None
    schema_payload = None
    if schema_path.exists():
        schema_payload = load_json(schema_path)
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

    result_json_valid_schema = len(result_json_validations) == 0
    if not result_json_valid_schema:
        failure_classifications.add("result_invalid")

    secret_scan_paths = discover_secret_scan_paths()
    secret_findings = scan_paths_for_secrets(secret_scan_paths)
    if secret_findings:
        failure_classifications.add("eval_failed")

    if cancelled:
        failure_classifications.add("eval_failed")

    overall_pass = len(failure_classifications) == 0
    overall_error_code = (
        "none"
        if overall_pass
        else ",".join(sorted(failure_classifications))
    )

    payload = {
        "pi_exit_code": pi_exit_code,
        "result_json_present": result_json_present,
        "result_json_valid_minimal": result_json_valid_schema,
        "result_json_valid_schema": result_json_valid_schema,
        "result_json_validations": result_json_validations,
        "result_json_schema": {
            "schema_path": str(schema_path),
            "schema_available": schema_path.is_file(),
            "schema_sha256": sha256_file(schema_path),
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
        "score",
        "score generation complete",
        error_code="" if overall_pass else overall_error_code,
        extra={"overall_pass": overall_pass},
    )
    return payload


def main(argv: list[str] | None = None) -> int:
    global event_log_path
    global exit_code_path
    global out_path
    global repo_root
    global run_dir
    global schema_path
    global task_path

    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) not in (4, 5, 6):
        usage()
        return 2

    task_path = pathlib.Path(args[0]).resolve()
    run_dir = pathlib.Path(args[1]).resolve()
    exit_code_path = pathlib.Path(args[2]).resolve()
    out_path = pathlib.Path(args[3]).resolve()
    schema_path = pathlib.Path(
        args[4] if len(args) >= 5 else (run_dir / "result.schema.json")
    ).resolve()
    event_log_path = pathlib.Path(
        args[5] if len(args) == 6 else (run_dir / "run-events.jsonl")
    ).resolve()

    if run_dir.parent.name == "runs":
        repo_root = run_dir.parent.parent.resolve()
    else:
        repo_root = run_dir.parent.resolve()

    try:
        score_payload = build_score_payload(cancelled=False)
    except BaseException as exc:  # noqa: BLE001
        append_event("score", "score generation interrupted", error_code="eval_failed")
        partial_payload = build_score_payload(cancelled=True)
        partial_payload["interruption"] = str(exc)
        write_json(out_path, partial_payload)
        print(out_path)
        raise
    else:
        write_json(out_path, score_payload)
        print(out_path)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
