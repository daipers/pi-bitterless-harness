#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import pathlib
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any

from harnesslib import (
    build_run_event,
    canonicalize_json_file,
    env_flag,
    evaluate_policy_guardrail,
    evaluate_required_artifact_path,
    guardrail_policy_snapshot,
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

DEFAULT_RESULT_FILE = "result.json"
DEFAULT_RESULT_SCHEMA_FILE = "result.schema.json"
DEFAULT_RUN_EVENTS_FILE = "run-events.jsonl"

DEFAULT_EVAL_TIMEOUT_SECONDS = "300"
DEFAULT_MAX_EVAL_COMMANDS = "0"

SECRET_SCAN_DIRS = ("outputs", "score", "recovery")
SECRET_SCAN_CONTEXT_FILES = ("context/retrieval-manifest.json", "context/retrieval-summary.md")
SECRET_SCAN_SKIP_DIRS = ("home", "session", "context/source-runs")
ENV_FALSE_VALUES = {"", "0", "false", "False"}
SCORE_FAILURE_CLASSIFICATIONS = frozenset(
    {
        "contract_invalid",
        "eval_command_limit_exceeded",
        "guardrail_policy_violation",
        "eval_failed",
        "model_invocation_failed",
        "result_invalid",
    }
)


def _to_positive_int(value: str, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    if parsed < 1:
        return default
    return parsed


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
    worker_id: str | None = None
    attempt: int | None = None


@dataclass(frozen=True)
class EvaluationBatchResult:
    evaluations: tuple[dict[str, Any], ...]
    failure_classifications: frozenset[str]
    tool_guardrail_decisions: tuple[dict[str, Any], ...]


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
    schema_default = run_dir / DEFAULT_RESULT_SCHEMA_FILE
    event_default = run_dir / DEFAULT_RUN_EVENTS_FILE
    return ScoreContext(
        task_path=pathlib.Path(argv[0]).resolve(),
        run_dir=run_dir,
        exit_code_path=pathlib.Path(argv[2]).resolve(),
        out_path=pathlib.Path(argv[3]).resolve(),
        schema_path=pathlib.Path(argv[4] if len(argv) >= 5 else schema_default).resolve(),
        event_log_path=pathlib.Path(argv[5] if len(argv) == 6 else event_default).resolve(),
        repo_root=resolve_repo_root(run_dir),
        contract_path=(run_dir / "run.contract.json").resolve(),
        worker_id=os.environ.get("HARNESS_WORKER_ID") or None,
        attempt=_read_optional_int(os.environ.get("HARNESS_ATTEMPT")),
    )


def _read_optional_int(value: str | None) -> int | None:
    try:
        return int(value) if value not in {None, ""} else None
    except (TypeError, ValueError):
        return None


def append_event(
    context: ScoreContext,
    phase: str,
    message: str,
    *,
    error_code: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    payload_extra: dict[str, Any] = {}
    if context.worker_id:
        payload_extra["worker_id"] = context.worker_id
    if context.attempt is not None:
        payload_extra["attempt"] = context.attempt
    if extra:
        payload_extra.update(extra)
    payload = build_run_event(
        context.run_dir.name,
        phase,
        message,
        error_code=error_code,
        extra=payload_extra or None,
    )
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


def _read_previous_guardrails(path: pathlib.Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    previous = payload.get("decisions", [])
    if isinstance(previous, list):
        return [dict(item) for item in previous if isinstance(item, dict)]
    return []


def _merge_guardrail_decisions(
    baseline: list[dict[str, Any]],
    additions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    signatures: set[str] = set()
    for source in (baseline, additions):
        for item in source:
            hook = str(item.get("hook", ""))
            violations = tuple(item.get("violations", []))
            allowed = bool(item.get("allowed", False))
            policy_source = str(item.get("policy_version_source", ""))
            key = json.dumps(
                {
                    "hook": hook,
                    "allowed": allowed,
                    "violations": violations,
                    "policy_version_source": policy_source,
                },
                sort_keys=True,
            )
            if key in signatures:
                continue
            signatures.add(key)
            merged.append(item)
    return merged


def _relative_to_run_dir(context: ScoreContext, path: pathlib.Path | None) -> str | None:
    if path is None:
        return None
    try:
        return str(path.resolve().relative_to(context.run_dir.resolve()))
    except Exception:
        return str(path)


def _split_csv(value: str) -> list[str]:
    return [item for item in value.split(",") if item]


def _env_is_true(value: str | None) -> bool:
    return value is not None and value not in ENV_FALSE_VALUES


def _append_validation_issue(
    issues: list[dict[str, Any]],
    *,
    field: str,
    expected: str,
    observed: Any,
) -> None:
    issues.append(
        {
            "field": field,
            "expected": expected,
            "observed": observed,
        }
    )


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

    manifest_path_setting = (
        os.environ.get("HARNESS_CONTEXT_MANIFEST_PATH") or settings["context_manifest_path"]
    )
    manifest_path = context.run_dir / manifest_path_setting
    manifest_payload = load_json(manifest_path) if manifest_path.exists() else None
    source_run_ids: list[str] = []
    if os.environ.get("HARNESS_CONTEXT_SOURCE_RUN_IDS"):
        source_run_ids = _split_csv(os.environ["HARNESS_CONTEXT_SOURCE_RUN_IDS"])
    elif isinstance(manifest_payload, dict):
        source_run_ids = list(manifest_payload.get("selected_source_run_ids", []))

    context_enabled_env = os.environ.get("HARNESS_CONTEXT_ENABLED")
    if context_enabled_env is None:
        context_enabled = bool(settings["retrieval_enabled"])
    else:
        context_enabled = _env_is_true(context_enabled_env)

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
        "retrieval_candidate": {
            "candidate_id": os.environ.get("HARNESS_RETRIEVAL_CANDIDATE_ID") or None,
            "mode": os.environ.get("HARNESS_RETRIEVAL_CANDIDATE_MODE") or "off",
        },
        "policy_candidate": {
            "candidate_id": os.environ.get("HARNESS_POLICY_CANDIDATE_ID") or None,
            "mode": os.environ.get("HARNESS_POLICY_CANDIDATE_MODE") or "off",
        },
        "model_candidate": {
            "candidate_id": os.environ.get("HARNESS_MODEL_CANDIDATE_ID") or None,
            "mode": os.environ.get("HARNESS_MODEL_CANDIDATE_MODE") or "off",
            "selected_model": os.environ.get("HARNESS_MODEL_SELECTED_MODEL") or None,
            "fallback_model": os.environ.get("HARNESS_MODEL_FALLBACK_MODEL") or None,
        },
        "guardrails_artifact_path": (
            context.run_dir / os.environ.get("HARNESS_GUARDRAILS_PATH", "outputs/guardrails.json")
        ),
        "guardrail_snapshot": guardrail_policy_snapshot(policy),
        "previous_guardrail_decisions": _read_previous_guardrails(
            context.run_dir / os.environ.get("HARNESS_GUARDRAILS_PATH", "outputs/guardrails.json")
        ),
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
    for rel_dir in SECRET_SCAN_DIRS:
        candidates.extend(_walk_secret_scan_dir(context.run_dir / rel_dir))

    for rel_file in SECRET_SCAN_CONTEXT_FILES:
        candidate = context.run_dir / rel_file
        if candidate.is_file():
            candidates.append(candidate)

    for skipped_root in [context.run_dir / rel_path for rel_path in SECRET_SCAN_SKIP_DIRS]:
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


def _tool_guardrail_decision(
    context: ScoreContext,
    execution_metadata: dict[str, Any],
    detail: dict[str, Any],
    *,
    allow_dangerous_eval: bool,
    allow_network_tasks: bool,
) -> dict[str, Any]:
    return evaluate_policy_guardrail(
        execution_metadata["policy"],
        "pre_tool_use",
        context={
            "requires_opt_in": bool(detail.get("requires_opt_in")),
            "network_access": bool(detail.get("network_access")),
            "blocked_reasons": detail.get("blocked_reasons", []),
            "allow_dangerous_eval": allow_dangerous_eval,
            "allow_network_tasks": allow_network_tasks,
            "policy_path": execution_metadata["policy_path"],
        },
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
        proc = subprocess.run(  # nosec B603
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
    execution_metadata: dict[str, Any] | None = None,
    *,
    max_eval_commands: int = 0,
    eval_timeout_seconds: int,
    allow_dangerous_eval: bool,
    allow_network_tasks: bool,
) -> EvaluationBatchResult:
    if execution_metadata is None:
        execution_metadata = _load_execution_metadata(context)
    evaluations: list[dict[str, Any]] = []
    failure_classifications: set[str] = set()
    tool_guardrail_decisions: list[dict[str, Any]] = []
    if not parsed_task["ok"]:
        failure_classifications.add("contract_invalid")

    score_dir = context.run_dir / "score"
    for index, detail in enumerate(parsed_task["eval_command_details"], start=1):
        if max_eval_commands > 0 and index > max_eval_commands:
            stdout_path, stderr_path = _result_log_paths(score_dir, index)
            evaluations.append(
                _evaluation_result(
                    context,
                    detail,
                    stdout_path,
                    stderr_path,
                    stdout="",
                    stderr=f"max eval command limit exceeded ({max_eval_commands})\n",
                    exit_code=None,
                    duration=0.0,
                    passed=False,
                    blocked=True,
                    failure_classification="eval_command_limit_exceeded",
                )
            )
            failure_classifications.add("eval_command_limit_exceeded")
            break
        tool_decision = _tool_guardrail_decision(
            context,
            execution_metadata,
            detail,
            allow_dangerous_eval=allow_dangerous_eval,
            allow_network_tasks=allow_network_tasks,
        )
        tool_guardrail_decisions.append(tool_decision)
        if not tool_decision["allowed"]:
            stdout_path, stderr_path = _result_log_paths(score_dir, index)
            evaluations.append(
                _evaluation_result(
                    context,
                    detail,
                    stdout_path,
                    stderr_path,
                    stdout="",
                    stderr="; ".join(tool_decision["violations"]) + "\n",
                    exit_code=None,
                    duration=0.0,
                    passed=False,
                    blocked=True,
                    failure_classification="guardrail_policy_violation",
                )
            )
            failure_classifications.add("guardrail_policy_violation")
            continue
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
        tool_guardrail_decisions=tuple(tool_guardrail_decisions),
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
    result_json_path = context.run_dir / DEFAULT_RESULT_FILE
    result_json_present = result_json_path.exists()
    result_json_validations: list[dict[str, Any]] = []
    result_payload = None
    schema_payload = load_json(context.schema_path) if context.schema_path.exists() else None

    if result_json_present:
        try:
            result_payload = canonicalize_json_file(result_json_path)
        except Exception as exc:
            _append_validation_issue(
                result_json_validations,
                field="result_json",
                expected="valid JSON object",
                observed=f"json parse error: {exc}",
            )
        else:
            if not isinstance(result_payload, dict):
                _append_validation_issue(
                    result_json_validations,
                    field="result_json",
                    expected="JSON object",
                    observed=str(type(result_payload).__name__),
                )
            else:
                result_json_validations.extend(
                    validate_result_payload(result_payload, schema_payload)
                )
    else:
        _append_validation_issue(
            result_json_validations,
            field="result_json",
            expected=f"{DEFAULT_RESULT_FILE} must exist",
            observed="missing file",
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


def _build_retrieval_metadata(
    context: ScoreContext,
    execution_metadata: dict[str, Any],
) -> dict[str, Any]:
    manifest_payload = execution_metadata["context_manifest_payload"]
    return {
        "enabled": execution_metadata["context_enabled"],
        "source_run_ids": list(execution_metadata["context_source_run_ids"]),
        "context_manifest_path": _relative_to_run_dir(
            context, execution_metadata["context_manifest_path"]
        ),
        "context_manifest_version": manifest_payload.get("context_manifest_version"),
        "retrieval_profile_id": manifest_payload.get("retrieval_profile_id"),
        "retrieval_profile_fingerprint": manifest_payload.get("retrieval_profile_fingerprint"),
        "retrieval_candidate_id": manifest_payload.get("retrieval_candidate_id"),
        "retrieval_candidate_mode": manifest_payload.get("retrieval_candidate_mode"),
        "retriever_version": manifest_payload.get("retriever_version"),
        "reranker_version": manifest_payload.get("reranker_version"),
        "abstention_model_version": manifest_payload.get("abstention_model_version"),
        "selection_source": manifest_payload.get("selection_source"),
        "index_mode": manifest_payload.get("index_mode"),
        "index_provenance_token": manifest_payload.get("index_provenance_token"),
        "candidate_run_count": manifest_payload.get("candidate_run_count"),
        "eligible_run_count": manifest_payload.get("eligible_run_count"),
        "selected_count": manifest_payload.get("selected_count"),
        "selected_source_count": manifest_payload.get("selected_source_count"),
        "empty_context": manifest_payload.get("empty_context"),
        "abstained": manifest_payload.get("abstained"),
        "abstention_reason": manifest_payload.get("abstention_reason"),
        "abstention_thresholds": manifest_payload.get("abstention_thresholds"),
        "top_candidate_score": manifest_payload.get("top_candidate_score"),
        "top_candidate_score_margin": manifest_payload.get("top_candidate_score_margin"),
        "ranking_latency_ms": manifest_payload.get("ranking_latency_ms"),
        "artifact_bytes_copied": manifest_payload.get("artifact_bytes_copied"),
    }


def _result_has_evidence_backed_claim(payload: dict[str, Any]) -> bool:
    claims = payload.get("claims", [])
    if not isinstance(claims, list):
        return False
    for item in claims:
        if not isinstance(item, dict):
            continue
        evidence = item.get("evidence", [])
        if isinstance(evidence, list) and any(
            isinstance(entry, str) and entry for entry in evidence
        ):
            return True
    return False


def _build_benchmark_eligibility(
    *,
    result_payload: dict[str, Any],
    result_validation: ResultValidationResult,
    secret_scan: SecretScanResult,
    overall_pass: bool,
) -> dict[str, Any]:
    reasons: list[str] = []
    criteria = {
        "overall_pass": overall_pass,
        "result_schema_valid": result_validation.result_json_valid_schema,
        "secret_clean": len(secret_scan.findings) == 0,
        "evidence_backed_claims": _result_has_evidence_backed_claim(result_payload),
    }
    if not criteria["overall_pass"]:
        reasons.append("score_not_passing")
    if not criteria["result_schema_valid"]:
        reasons.append("result_schema_invalid")
    if not criteria["secret_clean"]:
        reasons.append("secret_scan_findings")
    if not criteria["evidence_backed_claims"]:
        reasons.append("missing_evidence_backed_claim")
    return {
        "eligible": len(reasons) == 0,
        "criteria": criteria,
        "reasons": reasons,
    }


def _build_promotion_summary(
    *,
    failure_classifications: set[str],
    benchmark_eligibility: dict[str, Any],
    execution_metadata: dict[str, Any],
) -> dict[str, Any]:
    return {
        "eligible_release_signal": len(failure_classifications) == 0
        and bool(benchmark_eligibility.get("eligible")),
        "requires_recent_canary": True,
        "requires_benchmark_report": True,
        "requires_signed_provenance": True,
        "blocking_failure_count": len(failure_classifications),
        "blocking_failure_classifications": sorted(failure_classifications),
        "candidates": {
            "retrieval": dict(execution_metadata.get("retrieval_candidate", {})),
            "policy": dict(execution_metadata.get("policy_candidate", {})),
            "model": dict(execution_metadata.get("model_candidate", {})),
        },
    }


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


def _persist_guardrail_artifact(
    context: ScoreContext,
    execution_metadata: dict[str, Any],
    score_payload: dict[str, Any],
) -> None:
    existing_payload = _read_previous_guardrails(execution_metadata["guardrails_artifact_path"])
    merged_decisions = _merge_guardrail_decisions(
        existing_payload,
        list(score_payload.get("guardrails", {}).get("decisions", [])),
    )
    artifact_payload = {
        "selected_profile_id": execution_metadata["execution_profile"],
        "policy_path": execution_metadata["policy_path"],
        "policy_fingerprint": execution_metadata["policy"].get("policy_fingerprint", ""),
        "policy_version_source": execution_metadata["guardrail_snapshot"].get(
            "source_of_truth",
            "",
        ),
        "policy_snapshot": execution_metadata["guardrail_snapshot"],
        "decisions": merged_decisions,
        "effective_policy": execution_metadata["policy"],
        "score_payload_path": str(context.run_dir / "score.json"),
        "score_overall_pass": score_payload.get("overall_pass", False),
        "score_overall_error_code": score_payload.get("overall_error_code"),
    }
    write_json(execution_metadata["guardrails_artifact_path"], artifact_payload)


def _assemble_score_payload(
    context: ScoreContext,
    inputs: ScoreAssemblyInput,
    *,
    execution_metadata: dict[str, Any],
    max_eval_commands: int,
) -> dict[str, Any]:
    failure_classifications = _collect_failure_classifications(inputs)
    overall_pass = len(failure_classifications) == 0
    overall_error_code = "none" if overall_pass else ",".join(sorted(failure_classifications))
    try:
        result_payload = load_json(context.run_dir / DEFAULT_RESULT_FILE)
    except Exception:
        result_payload = {}
    if not isinstance(result_payload, dict):
        result_payload = {}
    benchmark_eligibility = _build_benchmark_eligibility(
        result_payload=result_payload,
        result_validation=inputs.result_validation,
        secret_scan=inputs.secret_scan,
        overall_pass=overall_pass,
    )
    promotion_summary = _build_promotion_summary(
        failure_classifications=failure_classifications,
        benchmark_eligibility=benchmark_eligibility,
        execution_metadata=execution_metadata,
    )

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
        "max_eval_commands": max_eval_commands,
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
        "guardrails": {
            "policy_snapshot": execution_metadata["guardrail_snapshot"],
            "policy_version_source": execution_metadata["guardrail_snapshot"].get(
                "source_of_truth", ""
            ),
            "decisions": _merge_guardrail_decisions(
                list(execution_metadata["previous_guardrail_decisions"]),
                list(inputs.evaluations.tool_guardrail_decisions),
            ),
        },
        "retrieval": _build_retrieval_metadata(context, execution_metadata),
        "candidates": {
            "retrieval": dict(execution_metadata.get("retrieval_candidate", {})),
            "policy": dict(execution_metadata.get("policy_candidate", {})),
            "model": dict(execution_metadata.get("model_candidate", {})),
        },
        "benchmark_eligibility": benchmark_eligibility,
        "promotion_summary": promotion_summary,
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
    eval_timeout_seconds = int(
        os.environ.get("HARNESS_EVAL_TIMEOUT_SECONDS", DEFAULT_EVAL_TIMEOUT_SECONDS)
    )
    max_eval_commands = _to_positive_int(
        os.environ.get("HARNESS_MAX_EVAL_COMMANDS", DEFAULT_MAX_EVAL_COMMANDS),
        default=0,
    )

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
        execution_metadata,
        max_eval_commands=max_eval_commands,
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
        max_eval_commands=max_eval_commands,
    )
    _persist_guardrail_artifact(context, execution_metadata, payload)
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
        try:
            _persist_guardrail_artifact(
                context,
                _load_execution_metadata(context),
                partial_payload,
            )
        except Exception as persist_error:  # noqa: BLE001
            partial_payload["guardrail_persist_error"] = str(persist_error)
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
