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
    evaluate_required_artifact_path,
    load_policy,
    load_run_contract,
    now_utc,
    parse_task_file,
    resolve_execution_settings,
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
    contract_path: pathlib.Path | None = None


@dataclass(frozen=True)
class EvaluationBatchResult:
    evaluations: tuple[dict[str, Any], ...]
    failure_classifications: frozenset[str]


@dataclass(frozen=True)
class ArtifactCheckResult:
    required_artifacts: tuple[dict[str, Any], ...]
    failure_classifications: frozenset[str]


@dataclass(frozen=True)
class ResultValidationResult:
    result_json_present: bool
    result_json_valid_schema: bool
    result_json_validations: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class SecretScanResult:
    paths_scanned: tuple[pathlib.Path, ...]
    findings: tuple[dict[str, Any], ...]
    scanned_path_count: int
    skipped_path_count: int
    skipped_reason_counts: dict[str, int]
    failure_classifications: frozenset[str]


@dataclass(frozen=True)
class ScoreAssemblyInput:
    parsed_task: dict[str, Any]
    evaluations: EvaluationBatchResult
    artifacts: ArtifactCheckResult
    pi_exit_code: int
    result_validation: ResultValidationResult
    secret_scan: SecretScanResult
    cancelled: bool


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
        contract_path=(run_dir / "run.contract.json").resolve(),
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


def _relative_to_run_dir(context: ScoreContext, path: pathlib.Path | None) -> str | None:
    if path is None:
        return None
    try:
        return str(path.resolve().relative_to(context.run_dir.resolve()))
    except Exception:
        return str(path)


def _load_execution_metadata(context: ScoreContext) -> dict[str, Any]:
    contract_path = context.contract_path or (context.run_dir / "run.contract.json")
    run_contract = (
        load_run_contract(contract_path)
        if contract_path.exists()
        else {"run_contract_version": "v1"}
    )
    profile_override = os.environ.get("HARNESS_EXECUTION_PROFILE") or None
    settings = resolve_execution_settings(run_contract, profile_override=profile_override)
    policy_path = os.environ.get("HARNESS_POLICY_PATH") or settings["policy_path"]
    policy = load_policy(policy_path, repo_root=context.repo_root)

    manifest_path_setting = os.environ.get("HARNESS_CONTEXT_MANIFEST_PATH") or settings[
        "context_manifest_path"
    ]
    manifest_path = context.run_dir / manifest_path_setting
    manifest_payload = load_json(manifest_path) if manifest_path.exists() else None
    source_run_ids: list[str] = []
    if os.environ.get("HARNESS_CONTEXT_SOURCE_RUN_IDS"):
        source_run_ids = [
            item for item in os.environ["HARNESS_CONTEXT_SOURCE_RUN_IDS"].split(",") if item
        ]
    elif isinstance(manifest_payload, dict):
        source_run_ids = list(manifest_payload.get("selected_source_run_ids", []))

    context_enabled_env = os.environ.get("HARNESS_CONTEXT_ENABLED")
    if context_enabled_env is None:
        context_enabled = bool(settings["retrieval_enabled"])
    else:
        context_enabled = context_enabled_env not in {"", "0", "false", "False"}

    return {
        "run_contract_version": settings["run_contract_version"],
        "execution_profile": settings["execution_profile"],
        "policy": policy,
        "policy_path": (
            settings["policy_path"]
            if os.environ.get("HARNESS_POLICY_PATH") is None
            else os.environ["HARNESS_POLICY_PATH"]
        ),
        "context_enabled": context_enabled,
        "context_manifest_path": manifest_path if context_enabled else None,
        "context_summary_path": (
            context.run_dir / settings["context_summary_path"] if context_enabled else None
        ),
        "context_source_run_ids": source_run_ids,
        "context_manifest_payload": manifest_payload if isinstance(manifest_payload, dict) else {},
    }


def discover_secret_scan_paths(context: ScoreContext) -> list[pathlib.Path]:
    paths, _, _ = _discover_secret_scan_selection(context)
    return paths


def _root_secret_scan_files(run_dir: pathlib.Path) -> list[pathlib.Path]:
    return sorted([path for path in run_dir.iterdir() if path.is_file()])


def _walk_secret_scan_dir(root: pathlib.Path) -> list[pathlib.Path]:
    if not root.exists():
        return []
    return sorted([path for path in root.rglob("*") if path.is_file()])


def _discover_secret_scan_selection(
    context: ScoreContext,
) -> tuple[list[pathlib.Path], int, dict[str, int]]:
    candidates: list[pathlib.Path] = []
    skipped_reason_counts: dict[str, int] = {}

    def add_skipped(reason: str, count: int = 1) -> None:
        if count <= 0:
            return
        skipped_reason_counts[reason] = skipped_reason_counts.get(reason, 0) + count

    candidates.extend(_root_secret_scan_files(context.run_dir))
    for rel_dir in ["outputs", "score", "recovery"]:
        candidates.extend(_walk_secret_scan_dir(context.run_dir / rel_dir))

    for rel_file in [
        "context/retrieval-manifest.json",
        "context/retrieval-summary.md",
    ]:
        candidate = context.run_dir / rel_file
        if candidate.is_file():
            candidates.append(candidate)

    for skipped_root in [
        context.run_dir / "home",
        context.run_dir / "session",
        context.run_dir / "context" / "source-runs",
    ]:
        if skipped_root.exists():
            add_skipped(
                str(skipped_root.relative_to(context.run_dir)),
                count=1,
            )

    unique_candidates = sorted({path.resolve(): path for path in candidates}.values())
    return unique_candidates, sum(skipped_reason_counts.values()), skipped_reason_counts


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


def _collect_evaluations(
    context: ScoreContext,
    parsed_task: dict[str, Any],
    *,
    eval_timeout_seconds: int,
    allow_dangerous_eval: bool,
    allow_network_tasks: bool,
) -> EvaluationBatchResult:
    evaluations: list[dict[str, Any]] = []
    failure_classifications: set[str] = set()
    if not parsed_task["ok"]:
        failure_classifications.add("contract_invalid")

    score_dir = context.run_dir / "score"
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

    return EvaluationBatchResult(
        evaluations=tuple(evaluations),
        failure_classifications=frozenset(failure_classifications),
    )


def _check_required_artifacts(
    context: ScoreContext, required_paths: list[str]
) -> ArtifactCheckResult:
    payload: list[dict[str, Any]] = []
    failure_classifications: set[str] = set()
    for rel_path in required_paths:
        validated = evaluate_required_artifact_path(context.run_dir, rel_path)
        if not validated["valid"]:
            payload.append(
                {
                    "path": rel_path,
                    "exists": False,
                    "status": validated["status"],
                    "reason": validated["reason"],
                }
            )
            failure_classifications.add("contract_invalid")
            continue

        exists = (context.run_dir / pathlib.Path(rel_path)).exists()
        payload.append(
            {
                "path": rel_path,
                "exists": exists,
                "status": "present" if exists else "missing",
            }
        )
        if not exists:
            failure_classifications.add("eval_failed")
    return ArtifactCheckResult(
        required_artifacts=tuple(payload),
        failure_classifications=frozenset(failure_classifications),
    )


def _validate_result_json(context: ScoreContext) -> ResultValidationResult:
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

    return ResultValidationResult(
        result_json_present=result_json_present,
        result_json_valid_schema=len(result_json_validations) == 0,
        result_json_validations=tuple(result_json_validations),
    )


def _scan_for_secrets(context: ScoreContext) -> SecretScanResult:
    paths_scanned_list, skipped_path_count, skipped_reason_counts = _discover_secret_scan_selection(
        context
    )
    paths_scanned = tuple(paths_scanned_list)
    findings = tuple(scan_paths_for_secrets(list(paths_scanned)))
    failure_classifications = frozenset({"eval_failed"} if findings else set())
    return SecretScanResult(
        paths_scanned=paths_scanned,
        findings=findings,
        scanned_path_count=len(paths_scanned),
        skipped_path_count=skipped_path_count,
        skipped_reason_counts=skipped_reason_counts,
        failure_classifications=failure_classifications,
    )


def _collect_failure_classifications(inputs: ScoreAssemblyInput) -> set[str]:
    failure_classifications = set(inputs.evaluations.failure_classifications)
    failure_classifications.update(inputs.artifacts.failure_classifications)
    failure_classifications.update(inputs.secret_scan.failure_classifications)

    if inputs.pi_exit_code != 0:
        failure_classifications.add("model_invocation_failed")
    if not inputs.result_validation.result_json_valid_schema:
        failure_classifications.add("result_invalid")
    if inputs.cancelled:
        failure_classifications.add("eval_failed")

    return failure_classifications


def _assemble_score_payload(
    context: ScoreContext,
    inputs: ScoreAssemblyInput,
    *,
    execution_metadata: dict[str, Any],
) -> dict[str, Any]:
    failure_classifications = _collect_failure_classifications(inputs)
    overall_pass = len(failure_classifications) == 0
    overall_error_code = "none" if overall_pass else ",".join(sorted(failure_classifications))

    return {
        "pi_exit_code": inputs.pi_exit_code,
        "result_json_present": inputs.result_validation.result_json_present,
        "result_json_valid_minimal": inputs.result_validation.result_json_valid_schema,
        "result_json_valid_schema": inputs.result_validation.result_json_valid_schema,
        "result_json_validations": list(inputs.result_validation.result_json_validations),
        "result_json_schema": {
            "schema_path": str(context.schema_path),
            "schema_available": context.schema_path.is_file(),
            "schema_sha256": sha256_file(context.schema_path),
        },
        "evaluations": list(inputs.evaluations.evaluations),
        "required_artifacts": list(inputs.artifacts.required_artifacts),
        "task_parse": {
            "ok": inputs.parsed_task["ok"],
            "errors": inputs.parsed_task["errors"],
            "dangerous_eval_commands": inputs.parsed_task["dangerous_eval_commands"],
        },
        "secret_scan": {
            "paths_scanned": len(inputs.secret_scan.paths_scanned),
            "scanned_path_count": inputs.secret_scan.scanned_path_count,
            "skipped_path_count": inputs.secret_scan.skipped_path_count,
            "skipped_reason_counts": dict(sorted(inputs.secret_scan.skipped_reason_counts.items())),
            "findings": list(inputs.secret_scan.findings),
        },
        "execution_profile": execution_metadata["execution_profile"],
        "policy_path": execution_metadata["policy_path"],
        "retrieval": {
            "enabled": execution_metadata["context_enabled"],
            "source_run_ids": list(execution_metadata["context_source_run_ids"]),
            "context_manifest_path": _relative_to_run_dir(
                context, execution_metadata["context_manifest_path"]
            ),
            "index_mode": execution_metadata["context_manifest_payload"].get("index_mode"),
            "candidate_run_count": execution_metadata["context_manifest_payload"].get(
                "candidate_run_count"
            ),
            "eligible_run_count": execution_metadata["context_manifest_payload"].get(
                "eligible_run_count"
            ),
            "selected_count": execution_metadata["context_manifest_payload"].get("selected_count"),
            "ranking_latency_ms": execution_metadata["context_manifest_payload"].get(
                "ranking_latency_ms"
            ),
            "artifact_bytes_copied": execution_metadata["context_manifest_payload"].get(
                "artifact_bytes_copied"
            ),
        },
        "failure_classifications": sorted(failure_classifications),
        "overall_error_code": overall_error_code,
        "overall_pass": overall_pass,
        "cancelled": inputs.cancelled,
    }


def build_score_payload(context: ScoreContext, *, cancelled: bool = False) -> dict[str, Any]:
    score_dir = context.run_dir / "score"
    score_dir.mkdir(parents=True, exist_ok=True)

    execution_metadata = _load_execution_metadata(context)
    parsed_task = parse_task_file(context.task_path, eval_policy=execution_metadata["policy"])
    allow_dangerous_eval = env_flag(execution_metadata["policy"]["opt_in_env"], default=False)
    allow_network_tasks = env_flag(execution_metadata["policy"]["allow_network_env"], default=False)
    eval_timeout_seconds = int(os.environ.get("HARNESS_EVAL_TIMEOUT_SECONDS", "300"))

    append_event(
        context,
        "score",
        "starting score generation",
        extra={
            "allow_dangerous_eval": allow_dangerous_eval,
            "allow_network_tasks": allow_network_tasks,
            "execution_profile": execution_metadata["execution_profile"],
        },
    )

    evaluations = _collect_evaluations(
        context,
        parsed_task,
        eval_timeout_seconds=eval_timeout_seconds,
        allow_dangerous_eval=allow_dangerous_eval,
        allow_network_tasks=allow_network_tasks,
    )
    artifacts = _check_required_artifacts(context, parsed_task["required_artifacts"])
    result_validation = _validate_result_json(context)
    secret_scan = _scan_for_secrets(context)
    payload = _assemble_score_payload(
        context,
        ScoreAssemblyInput(
            parsed_task=parsed_task,
            evaluations=evaluations,
            artifacts=artifacts,
            pi_exit_code=_read_pi_exit_code(context.exit_code_path),
            result_validation=result_validation,
            secret_scan=secret_scan,
            cancelled=cancelled,
        ),
        execution_metadata=execution_metadata,
    )
    append_event(
        context,
        "score",
        "score generation complete",
        error_code="" if payload["overall_pass"] else payload["overall_error_code"],
        extra={"overall_pass": payload["overall_pass"]},
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
