#!/usr/bin/env python3
from __future__ import annotations

import copy
import hashlib
import json
import os
import pathlib
import re
import shlex
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

from capabilitylib import DEFAULT_CAPABILITIES_CONFIG, DEFAULT_INTERCEPTION_ACTION_LOG_PATH

if not hasattr(pathlib.Path, "utime"):
    setattr(
        pathlib.Path,
        "utime",
        lambda self, times=None: os.utime(self, times=times),
    )

# Contract constants and defaults
RUNNER_VERSION = "1.0.0"
RUN_CONTRACT_VERSION_V1 = "v1"
RUN_CONTRACT_VERSION_V2 = "v2"
RUN_CONTRACT_VERSION_V3 = "v3"
RUN_CONTRACT_VERSION_V4 = "v4"
RUN_CONTRACT_VERSION = RUN_CONTRACT_VERSION_V2
RESULT_INTERFACE_VERSION = "v1"
EXECUTION_PROFILES = {"strict", "offline", "networked", "heavy_tools", "capability"}
RUN_EVENT_SCHEMA_VERSION = "run-event-v1"
RUNTIME_GOVERNANCE_REGISTRY_VERSION = "v1"

TASK_REQUIRED_SECTIONS = [
    "Goal",
    "Constraints",
    "Done",
    "Eval",
    "Required Artifacts",
    "Result JSON schema (source of truth)",
]
TASK_OPTIONAL_SECTIONS = {"Notes", "Retrieval Quality Rubric"}

DEFAULT_RETRIEVAL_CONFIG = {
    "enabled": False,
    "source": "prior_runs",
    "strategy": "hybrid_v1",
    "max_source_runs": 3,
    "max_candidates": 25,
    "max_artifacts_per_run": 3,
    "max_artifact_bytes": 65536,
    "artifact_selection": "evidence_first",
}

DEFAULT_RETRIEVAL_INDEX_POLICY = {
    "ttl_seconds": 0,
    "max_entries": 0,
    "max_bytes": 0,
}

DEFAULT_RETENTION_POLICY = {
    "run": {
        "ttl_days": 30,
        "max_count": 0,
        "max_bytes": 0,
    },
    "artifact": {
        "ttl_days": 7,
        "max_count": 0,
        "max_bytes": 0,
    },
    "queue": {
        "ttl_days": 7,
        "max_count": 0,
    },
}

DEFAULT_GUARDRAIL_HOOKS = {
    "pre_retrieval": {"enabled": True, "allow": True},
    "pre_context_build": {"enabled": True, "allow": True},
    "pre_run": {"enabled": True, "allow": True},
    "pre_score_dispatch": {"enabled": True, "allow": True},
    "pre_tool_use": {
        "enabled": True,
        "allow": True,
        "allow_network_tools": True,
        "allow_dangerous_commands": True,
    },
}

DEFAULT_GUARDRAIL_POLICY = {
    "hooks": copy.deepcopy(DEFAULT_GUARDRAIL_HOOKS),
}

DEFAULT_POLICY_BASE = {
    "retrieval_index": DEFAULT_RETRIEVAL_INDEX_POLICY,
    "retention": DEFAULT_RETENTION_POLICY,
    "guardrails": DEFAULT_GUARDRAIL_POLICY,
}

_RUN_CONTRACT_BASE_FIELDS: dict[str, Any] = {
    "required_run_files": [
        "task.md",
        "RUN.md",
        "result.schema.json",
        "result.template.json",
        "run.contract.json",
    ],
    "required_directories": ["outputs", "home", "session", "score"],
    "required_task_sections": TASK_REQUIRED_SECTIONS,
    "result_schema_path": "result.schema.json",
    "result_template_path": "result.template.json",
    "manifest_path": "outputs/run_manifest.json",
    "event_log_path": "run-events.jsonl",
}

_RUN_CONTRACT_REQUIRED_KEYS_V1 = [
    "required_run_files",
    "required_directories",
    "required_task_sections",
    "result_schema_path",
    "result_template_path",
    "manifest_path",
    "event_log_path",
    "eval_policy",
]

_RUN_CONTRACT_REQUIRED_KEYS_V2 = [
    "required_run_files",
    "required_directories",
    "required_task_sections",
    "result_schema_path",
    "result_template_path",
    "manifest_path",
    "event_log_path",
    "execution_profile",
    "policy_path",
    "context_dir",
    "context_manifest_path",
    "context_summary_path",
    "retrieval",
]

_RUN_CONTRACT_REQUIRED_KEYS_V3 = [
    "required_run_files",
    "required_directories",
    "required_task_sections",
    "result_schema_path",
    "result_template_path",
    "manifest_path",
    "event_log_path",
    "execution_profile",
    "policy_path",
    "context_dir",
    "context_manifest_path",
    "context_summary_path",
    "retrieval",
    "transport",
    "capabilities",
]

_RUN_CONTRACT_REQUIRED_KEYS_V4 = list(_RUN_CONTRACT_REQUIRED_KEYS_V3)

DEFAULT_INTERCEPTION_CONFIG = {
    "enabled": True,
    "fail_mode": "fail_closed",
    "action_log_path": DEFAULT_INTERCEPTION_ACTION_LOG_PATH,
}

_RUN_CONTRACT_REQUIRED_RETRIEVAL_KEYS = [
    "enabled",
    "source",
    "max_source_runs",
    "max_artifacts_per_run",
    "max_artifact_bytes",
]


def _build_run_contract_template() -> dict[str, Any]:
    return dict(_RUN_CONTRACT_BASE_FIELDS)


def _collect_missing_fields(payload: dict[str, Any], keys: list[str], *, prefix: str) -> list[str]:
    return [f"missing {prefix} field: {key}" for key in keys if key not in payload]


def _to_positive_policy_int(value: str | int | None, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    if parsed < 0:
        return default
    return parsed


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _validate_int_field(payload: dict[str, Any], path: str) -> list[str]:
    value = payload.get(path)
    if value is None:
        return []
    try:
        int(value)
    except (TypeError, ValueError):
        return [f"policy.{path} must be an integer"]
    return []


def _collect_fixed_field_errors(payload: dict[str, Any], expected: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for key, value in expected.items():
        if key in payload and payload[key] != value:
            errors.append(f"{key} must be {value!r}")
    return errors


ENV_ASSIGNMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=.*$")
NETWORK_TOKEN_RE = re.compile(r"\b(?:curl|nc|ping|scp|ssh|telnet|wget)\b")

SECRET_PATTERNS = [
    ("openai_api_key", re.compile(r"\bsk-[A-Za-z0-9]{20,}\b")),
    ("github_token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b")),
    ("slack_token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b")),
    ("aws_access_key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    (
        "generic_secret_assignment",
        re.compile(
            r"(?i)\b(api[_-]?key|secret|token|password)\b\s*[:=]\s*[\"']?[A-Za-z0-9_\-/+=]{12,}"
        ),
    ),
]


# Generic JSON, hash, and environment helpers
def now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_run_event(
    run_id: str,
    phase: str,
    message: str,
    *,
    error_code: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ts": now_utc(),
        "trace_id": str(run_id),
        "run_id": str(run_id),
        "phase": phase,
        "duration_ms": None,
        "error_code": error_code or None,
        "message": message,
    }
    if extra:
        payload.update(extra)
    if payload.get("failure_classification") and "failure_class" not in payload:
        payload["failure_class"] = payload["failure_classification"]
    return payload


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: pathlib.Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except FileNotFoundError:
        return None


def write_json(path: pathlib.Path, payload: Any, *, sort_keys: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=sort_keys, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def script_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def governance_registry_path(repo_root: pathlib.Path | None = None) -> pathlib.Path:
    return (repo_root or script_root()) / "governance" / "runtime-governance-v1.json"


def load_governance_registry(repo_root: pathlib.Path | None = None) -> dict[str, Any]:
    path = governance_registry_path(repo_root=repo_root)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("runtime governance registry must be a JSON object")
    return payload


def compute_dependencies_hash(dependencies: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(dependencies, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value not in {"", "0", "false", "False", "no", "NO"}


def default_policy_path(profile: str = "strict") -> str:
    return f"policies/{profile}.json"


def resolve_policy_path(
    policy_path: str | pathlib.Path | None = None,
    *,
    repo_root: pathlib.Path | None = None,
) -> pathlib.Path:
    if policy_path is None:
        policy_path = default_policy_path()
    candidate = pathlib.Path(policy_path)
    if candidate.is_absolute():
        return candidate
    return (repo_root or script_root()) / candidate


def validate_policy(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for key in [
        "opt_in_env",
        "allow_network_env",
        "allowed_programs",
        "blocked_programs",
        "network_programs",
    ]:
        if key not in payload:
            errors.append(f"missing policy field: {key}")
    for key in ["allowed_programs", "blocked_programs", "network_programs"]:
        if key in payload and not isinstance(payload[key], list):
            errors.append(f"{key} must be an array")

    retrieval_index = payload.get("retrieval_index", {})
    if retrieval_index is not None and not isinstance(retrieval_index, dict):
        errors.append("retrieval_index must be an object")
    elif retrieval_index:
        for key in ["ttl_seconds", "max_entries", "max_bytes"]:
            if key not in retrieval_index:
                continue
            errors.extend(_validate_int_field(retrieval_index, key))

    retention = payload.get("retention", {})
    if retention is not None and not isinstance(retention, dict):
        errors.append("retention must be an object")
    elif retention:
        for scope in ["run", "artifact", "queue"]:
            settings = retention.get(scope, {})
            if settings is None:
                continue
            if not isinstance(settings, dict):
                errors.append(f"retention.{scope} must be an object")
                continue
            for key in ["ttl_days", "max_count", "max_bytes"]:
                if key in settings:
                    errors.extend(_validate_int_field(settings, key))

    guardrails = payload.get("guardrails", {})
    if guardrails is not None and not isinstance(guardrails, dict):
        errors.append("guardrails must be an object")
    elif guardrails:
        hooks = guardrails.get("hooks", {})
        if hooks is not None and not isinstance(hooks, dict):
            errors.append("guardrails.hooks must be an object")
        elif hooks:
            for hook_name, hook_payload in hooks.items():
                if not isinstance(hook_payload, dict):
                    errors.append(f"guardrails.hooks.{hook_name} must be an object")
                    continue
                if "enabled" in hook_payload and not isinstance(hook_payload["enabled"], bool):
                    errors.append(f"guardrails.hooks.{hook_name}.enabled must be a boolean")
                if "allow" in hook_payload and not isinstance(hook_payload["allow"], bool):
                    errors.append(f"guardrails.hooks.{hook_name}.allow must be a boolean")
                if "allow_network_tools" in hook_payload and not isinstance(
                    hook_payload["allow_network_tools"],
                    bool,
                ):
                    errors.append(
                        f"guardrails.hooks.{hook_name}.allow_network_tools must be a boolean"
                    )
                if "allow_dangerous_commands" in hook_payload and not isinstance(
                    hook_payload["allow_dangerous_commands"],
                    bool,
                ):
                    errors.append(
                        f"guardrails.hooks.{hook_name}.allow_dangerous_commands must be a boolean"
                    )
    return errors


def load_policy(
    policy_path: str | pathlib.Path | None = None,
    *,
    repo_root: pathlib.Path | None = None,
) -> dict[str, Any]:
    path = resolve_policy_path(policy_path, repo_root=repo_root)
    payload = json.loads(path.read_text(encoding="utf-8"))
    errors = validate_policy(payload)
    if errors:
        raise ValueError("; ".join(errors))
    payload = _deep_merge(copy.deepcopy(DEFAULT_POLICY_BASE), payload)
    payload["path"] = str(path)
    payload["relative_path"] = str(path.relative_to(repo_root or script_root()))
    payload["policy_fingerprint"] = sha256_text(
        json.dumps(
            {
                key: payload[key]
                for key in sorted(payload)
                if key not in {"path", "relative_path", "policy_fingerprint"}
            },
            sort_keys=True,
            ensure_ascii=False,
        )
    )
    return payload


def policy_source_of_truth(policy: dict[str, Any]) -> str:
    return str(
        policy.get("relative_path")
        or policy.get("path")
        or policy.get("policy_path", "")
        or policy.get("policy_source", "")
        or "policy"
    )


def resolve_retrieval_index_policy(
    payload: dict[str, Any] | None = None,
    *,
    profile_defaults: dict[str, Any] | None = None,
    policy_env_prefix: str = "HARNESS_RETRIEVAL_INDEX",
) -> dict[str, Any]:
    base = copy.deepcopy(DEFAULT_RETRIEVAL_INDEX_POLICY)
    if profile_defaults:
        base.update(profile_defaults)
    if payload is not None:
        for key, value in payload.items():
            if key in base:
                base[key] = value

    env_map = {
        "ttl_seconds": f"{policy_env_prefix}_TTL_SECONDS",
        "max_entries": f"{policy_env_prefix}_MAX_ENTRIES",
        "max_bytes": f"{policy_env_prefix}_MAX_BYTES",
    }
    for key, env_name in env_map.items():
        if env_name in os.environ:
            base[key] = _to_positive_policy_int(os.environ.get(env_name), default=base[key])

    return {
        "ttl_seconds": max(0, int(base["ttl_seconds"])),
        "max_entries": max(0, int(base["max_entries"])),
        "max_bytes": max(0, int(base["max_bytes"])),
    }


def resolve_retention_policy(
    payload: dict[str, Any] | None = None,
    *,
    policy_env_prefix: str = "HARNESS_RETENTION",
) -> dict[str, Any]:
    base = copy.deepcopy(DEFAULT_RETENTION_POLICY)
    if payload is not None:
        base = _deep_merge(base, payload)

    env_map = {
        "run.ttl_days": f"{policy_env_prefix}_RUN_TTL_DAYS",
        "run.max_count": f"{policy_env_prefix}_RUN_MAX_COUNT",
        "run.max_bytes": f"{policy_env_prefix}_RUN_MAX_BYTES",
        "artifact.ttl_days": f"{policy_env_prefix}_ARTIFACT_TTL_DAYS",
        "artifact.max_count": f"{policy_env_prefix}_ARTIFACT_MAX_COUNT",
        "artifact.max_bytes": f"{policy_env_prefix}_ARTIFACT_MAX_BYTES",
        "queue.ttl_days": f"{policy_env_prefix}_QUEUE_TTL_DAYS",
        "queue.max_count": f"{policy_env_prefix}_QUEUE_MAX_COUNT",
        "queue.max_bytes": f"{policy_env_prefix}_QUEUE_MAX_BYTES",
    }
    for dotted_key, env_name in env_map.items():
        section, field = dotted_key.split(".")
        if env_name in os.environ:
            base[section][field] = _to_positive_policy_int(
                os.environ.get(env_name),
                default=base[section][field],
            )

    normalized: dict[str, Any] = {}
    for scope, settings in base.items():
        if not isinstance(settings, dict):
            continue
        normalized[scope] = {
            "ttl_days": max(0, int(settings.get("ttl_days", 0))),
            "max_count": max(0, int(settings.get("max_count", 0))),
            "max_bytes": max(0, int(settings.get("max_bytes", 0))),
        }
        if scope == "queue":
            normalized[scope] = {
                "ttl_days": max(0, int(settings.get("ttl_days", 0))),
                "max_count": max(0, int(settings.get("max_count", 0))),
                "max_bytes": max(0, int(settings.get("max_bytes", 0))),
            }
    return normalized


def evaluate_policy_guardrail(
    policy: dict[str, Any],
    hook: str,
    *,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = context or {}
    source_of_truth = policy_source_of_truth(policy)
    if hook not in {
        "pre_retrieval",
        "pre_context_build",
        "pre_run",
        "pre_score_dispatch",
        "pre_tool_use",
    }:
        return {
            "hook": hook,
            "allowed": False,
            "violations": [f"unknown hook: {hook}"],
            "effective_limits": {},
            "source_of_truth": source_of_truth,
            "policy_fingerprint": policy.get("policy_fingerprint", ""),
        }

    hooks = policy.get("guardrails", {})
    hook_cfg = {}
    if isinstance(hooks, dict):
        hook_cfg = hooks.get("hooks", {}) if isinstance(hooks.get("hooks", {}), dict) else {}
    hook_settings = {}
    if isinstance(hook_cfg, dict):
        hook_settings = hook_cfg.get(hook, {}) if isinstance(hook_cfg.get(hook), dict) else {}

    allowed = bool(hook_settings.get("enabled", True) and hook_settings.get("allow", True))
    violations: list[str] = []
    effective_limits: dict[str, Any] = {}

    if hook_settings:
        for key in ("allow",):
            if key in hook_settings:
                effective_limits[key] = bool(hook_settings[key])

    if hook == "pre_tool_use":
        policy_allow_dangerous_eval = bool(hook_settings.get("allow_dangerous_commands", True))
        policy_allow_network_tasks = bool(hook_settings.get("allow_network_tools", True))
        context_allow_dangerous_eval = bool(context.get("allow_dangerous_eval", False))
        context_allow_network_tasks = bool(context.get("allow_network_tasks", False))

        effective_limits["policy_allows_dangerous_commands"] = policy_allow_dangerous_eval
        effective_limits["policy_allows_network_tools"] = policy_allow_network_tasks
        effective_limits["allow_dangerous_eval"] = context_allow_dangerous_eval
        effective_limits["allow_network_tasks"] = context_allow_network_tasks

        effective_limits["allow_dangerous_eval_effective"] = (
            context_allow_dangerous_eval and policy_allow_dangerous_eval
        )
        effective_limits["allow_network_tasks_effective"] = (
            context_allow_network_tasks and policy_allow_network_tasks
        )

        if (
            context.get("requires_opt_in")
            and not effective_limits["allow_dangerous_eval_effective"]
        ):
            allowed = False
            violations.append("tool_use.requires_opt_in")
        if context.get("network_access") and not effective_limits["allow_network_tasks_effective"]:
            allowed = False
            violations.append("tool_use.network_access")
        blocked_reasons = context.get("blocked_reasons") or []
        if blocked_reasons:
            allowed = False
            violations.extend(str(reason) for reason in blocked_reasons)
    elif hook == "pre_run":
        if context.get("skip_run", False):
            allowed = False
            violations.append("pre_run.skip_requested")
        effective_limits.update(
            {
                "strict_profile": bool(context.get("strict_profile", False)),
                "execution_profile": context.get("execution_profile"),
                "policy_path": context.get("policy_path"),
            }
        )
    elif hook == "pre_score_dispatch":
        if context.get("skip_score", False):
            allowed = False
            violations.append("pre_score_dispatch.skip_requested")
        effective_limits.update(
            {
                "force_score": bool(context.get("force_score", True)),
                "policy_path": context.get("policy_path"),
            }
        )
    elif hook == "pre_retrieval":
        if not context.get("retrieval_enabled", True):
            allowed = False
            violations.append("pre_retrieval.disabled")
        effective_limits.update(
            {
                "retrieval_mode": context.get("retrieval_mode"),
                "retrieval_index_policy": context.get("retrieval_index_policy", {}),
                "policy_path": context.get("policy_path"),
            }
        )
    elif hook == "pre_context_build":
        if context.get("blocked", False):
            allowed = False
            violations.append("pre_context_build.blocked")
        if context.get("max_candidates", 0) <= 0:
            effective_limits["max_candidates"] = 0
            allowed = False
            violations.append("pre_context_build.invalid_limits")
        else:
            effective_limits["max_candidates"] = context.get("max_candidates")
        effective_limits["retrieval_profile_id"] = context.get("retrieval_profile_id")

    decision = {
        "hook": hook,
        "allowed": bool(allowed),
        "violations": violations,
        "effective_limits": effective_limits,
        "source_of_truth": source_of_truth,
        "policy_fingerprint": policy.get("policy_fingerprint", ""),
    }
    if not decision["allowed"] and not decision["violations"]:
        decision["violations"].append(f"hook_disabled:{hook}")
    decision["effective_limits"] = dict(decision["effective_limits"])
    decision["source_of_truth"] = source_of_truth
    decision["policy_version_source"] = source_of_truth
    return decision


def evaluate_policy_guardrail_hook(
    policy: dict[str, Any],
    hook: str,
    *,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return evaluate_policy_guardrail(policy, hook, context=context)


def guardrail_policy_snapshot(policy: dict[str, Any]) -> dict[str, Any]:
    return {
        "policy_path": str(
            policy.get("relative_path")
            or policy.get("path")
            or policy.get("policy_path", "")
            or "policy"
        ),
        "policy_fingerprint": policy.get("policy_fingerprint", ""),
        "hooks": policy.get("guardrails", {}).get("hooks", {})
        if isinstance(policy.get("guardrails", {}).get("hooks", {}), dict)
        else {},
        "source_of_truth": policy_source_of_truth(policy),
    }


def evaluate_required_artifact_path(run_dir: pathlib.Path, raw_path: str) -> dict[str, Any]:
    candidate = pathlib.Path(raw_path)
    result: dict[str, Any] = {
        "path": raw_path,
        "valid": False,
        "status": "invalid_out_of_run_scope",
        "reason": None,
    }

    if candidate.is_absolute():
        result["reason"] = "required artifacts must be relative paths inside the run directory"
        return result

    resolved_run_dir = run_dir.resolve()
    resolved_candidate = (run_dir / candidate).resolve()
    try:
        resolved_candidate.relative_to(resolved_run_dir)
    except ValueError:
        result["reason"] = "required artifact path resolves outside the run directory"
        return result

    result["valid"] = True
    result["status"] = "valid"
    return result


def make_result_template() -> dict[str, Any]:
    return {
        "x-interface-version": RESULT_INTERFACE_VERSION,
        "status": "success",
        "summary": (
            "Updated retrieval scoring for the requested task and wrote "
            "outputs/retrieval-metrics.json with benchmark-ready metrics."
        ),
        "artifacts": [
            {
                "path": "outputs/retrieval-metrics.json",
                "description": (
                    "Metrics snapshot showing the retrieval quality outputs produced by "
                    "this run."
                ),
            }
        ],
        "claims": [
            {
                "claim": (
                    "Retrieval metrics were generated and written to "
                    "outputs/retrieval-metrics.json."
                ),
                "evidence": ["outputs/retrieval-metrics.json"],
            }
        ],
        "remaining_risks": ["optional remaining risks"],
    }


def default_run_contract(
    *,
    version: str = RUN_CONTRACT_VERSION,
    execution_profile: str = "strict",
) -> dict[str, Any]:
    contract = _build_run_contract_template()
    if version == RUN_CONTRACT_VERSION_V1:
        strict_policy = load_policy(default_policy_path("strict"))
        return {
            **contract,
            "run_contract_version": RUN_CONTRACT_VERSION_V1,
            "result_interface_version": RESULT_INTERFACE_VERSION,
            "eval_policy": {
                "opt_in_env": strict_policy["opt_in_env"],
                "allow_network_env": strict_policy["allow_network_env"],
                "allowed_programs": strict_policy["allowed_programs"],
                "blocked_programs": strict_policy["blocked_programs"],
            },
        }

    if execution_profile not in EXECUTION_PROFILES:
        raise ValueError(f"unsupported execution profile: {execution_profile}")

    base_v2 = {
        **contract,
        "run_contract_version": RUN_CONTRACT_VERSION_V2,
        "result_interface_version": RESULT_INTERFACE_VERSION,
        "execution_profile": execution_profile,
        "policy_path": default_policy_path(execution_profile),
        "context_dir": "context",
        "context_manifest_path": "context/retrieval-manifest.json",
        "context_summary_path": "context/retrieval-summary.md",
        "retrieval": {
            **DEFAULT_RETRIEVAL_CONFIG,
            "enabled": execution_profile in {"capability", "heavy_tools"},
        },
    }
    if version == RUN_CONTRACT_VERSION_V2:
        return base_v2
    if version == RUN_CONTRACT_VERSION_V3:
        return {
            **base_v2,
            "run_contract_version": RUN_CONTRACT_VERSION_V3,
            "transport": {"mode": "cli_json"},
            "capabilities": copy.deepcopy(DEFAULT_CAPABILITIES_CONFIG),
        }
    if version == RUN_CONTRACT_VERSION_V4:
        return {
            **base_v2,
            "run_contract_version": RUN_CONTRACT_VERSION_V4,
            "transport": {"mode": "managed_rpc"},
            "capabilities": {
                **copy.deepcopy(DEFAULT_CAPABILITIES_CONFIG),
                "interception": copy.deepcopy(DEFAULT_INTERCEPTION_CONFIG),
            },
        }
    raise ValueError(f"unsupported run contract version: {version}")


def validate_run_contract(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    version = payload.get("run_contract_version")
    if version == RUN_CONTRACT_VERSION_V1:
        expected = default_run_contract(version=RUN_CONTRACT_VERSION_V1)
        if payload.get("result_interface_version") != RESULT_INTERFACE_VERSION:
            errors.append("result_interface_version must be v1")
        errors.extend(
            _collect_missing_fields(payload, _RUN_CONTRACT_REQUIRED_KEYS_V1, prefix="run contract")
        )
        if isinstance(payload.get("eval_policy"), dict):
            for key in ["opt_in_env", "allow_network_env", "allowed_programs", "blocked_programs"]:
                if key not in payload["eval_policy"]:
                    errors.append(f"missing eval_policy field: {key}")
        else:
            errors.append("eval_policy must be an object")
        errors.extend(
            _collect_fixed_field_errors(
                payload,
                {
                    "result_schema_path": expected["result_schema_path"],
                    "result_template_path": expected["result_template_path"],
                    "manifest_path": expected["manifest_path"],
                    "event_log_path": expected["event_log_path"],
                },
            )
        )
        return errors

    if version not in {RUN_CONTRACT_VERSION_V2, RUN_CONTRACT_VERSION_V3, RUN_CONTRACT_VERSION_V4}:
        errors.append("run_contract_version must be v1, v2, v3, or v4")
        return errors

    expected = default_run_contract(version=version)
    if payload.get("result_interface_version") != RESULT_INTERFACE_VERSION:
        errors.append("result_interface_version must be v1")
    errors.extend(
        _collect_missing_fields(
            payload,
            (
                _RUN_CONTRACT_REQUIRED_KEYS_V3
                if version == RUN_CONTRACT_VERSION_V3
                else (
                    _RUN_CONTRACT_REQUIRED_KEYS_V4
                    if version == RUN_CONTRACT_VERSION_V4
                    else _RUN_CONTRACT_REQUIRED_KEYS_V2
                )
            ),
            prefix="run contract",
        )
    )
    errors.extend(
        _collect_fixed_field_errors(
            payload,
            {
                "result_schema_path": expected["result_schema_path"],
                "result_template_path": expected["result_template_path"],
                "manifest_path": expected["manifest_path"],
                "event_log_path": expected["event_log_path"],
                "context_dir": expected["context_dir"],
                "context_manifest_path": expected["context_manifest_path"],
                "context_summary_path": expected["context_summary_path"],
            },
        )
    )
    if payload.get("execution_profile") not in EXECUTION_PROFILES:
        errors.append(
            "execution_profile must be one of: strict, offline, networked, heavy_tools, capability"
        )
    if not isinstance(payload.get("policy_path"), str) or not payload["policy_path"]:
        errors.append("policy_path must be a non-empty string")
    retrieval = payload.get("retrieval")
    if not isinstance(retrieval, dict):
        errors.append("retrieval must be an object")
    else:
        for key in _RUN_CONTRACT_REQUIRED_RETRIEVAL_KEYS:
            if key not in retrieval:
                errors.append(f"missing retrieval field: {key}")
        if retrieval.get("source") != DEFAULT_RETRIEVAL_CONFIG["source"]:
            errors.append("retrieval.source must be 'prior_runs'")
        if (
            "strategy" in retrieval
            and retrieval["strategy"] != DEFAULT_RETRIEVAL_CONFIG["strategy"]
        ):
            errors.append("retrieval.strategy must be 'hybrid_v1'")
        if (
            "artifact_selection" in retrieval
            and retrieval["artifact_selection"] != DEFAULT_RETRIEVAL_CONFIG["artifact_selection"]
        ):
            errors.append("retrieval.artifact_selection must be 'evidence_first'")
        for key in ["max_source_runs", "max_artifacts_per_run", "max_artifact_bytes"]:
            value = retrieval.get(key)
            if not isinstance(value, int) or value < 1:
                errors.append(f"retrieval.{key} must be a positive integer")
        if "max_candidates" in retrieval:
            value = retrieval.get("max_candidates")
            if not isinstance(value, int) or value < 1:
                errors.append("retrieval.max_candidates must be a positive integer")
        if "enabled" in retrieval and not isinstance(retrieval["enabled"], bool):
            errors.append("retrieval.enabled must be a boolean")
    if version in {RUN_CONTRACT_VERSION_V3, RUN_CONTRACT_VERSION_V4}:
        transport = payload.get("transport")
        if not isinstance(transport, dict):
            errors.append("transport must be an object")
        else:
            expected_modes = {"cli_json", "rpc"} if version == RUN_CONTRACT_VERSION_V3 else {"managed_rpc"}
            if transport.get("mode") not in expected_modes:
                errors.append(
                    "transport.mode must be one of: "
                    + ", ".join(sorted(expected_modes))
                )
        capabilities = payload.get("capabilities")
        if not isinstance(capabilities, dict):
            errors.append("capabilities must be an object")
        else:
            if "enabled" in capabilities and not isinstance(capabilities["enabled"], bool):
                errors.append("capabilities.enabled must be a boolean")
            if (
                not isinstance(capabilities.get("library_path"), str)
                or not str(capabilities.get("library_path", "")).strip()
            ):
                errors.append("capabilities.library_path must be a non-empty string")
            if capabilities.get("manifest_path") != DEFAULT_CAPABILITIES_CONFIG["manifest_path"]:
                errors.append(
                    "capabilities.manifest_path must be 'context/capability-manifest.json'"
                )
            subagents = capabilities.get("subagents")
            if not isinstance(subagents, dict):
                errors.append("capabilities.subagents must be an object")
            else:
                if "allowed" in subagents and not isinstance(subagents["allowed"], bool):
                    errors.append("capabilities.subagents.allowed must be a boolean")
                max_agents = subagents.get("max_agents")
                if not isinstance(max_agents, int) or max_agents < 0:
                    errors.append(
                        "capabilities.subagents.max_agents must be a non-negative integer"
                    )
                allowed_profiles = subagents.get("allowed_profiles")
                if not isinstance(allowed_profiles, list):
                    errors.append("capabilities.subagents.allowed_profiles must be an array")
                else:
                    for index, value in enumerate(allowed_profiles):
                        if not isinstance(value, str) or not value.strip():
                            errors.append(
                                "capabilities.subagents.allowed_profiles"
                                f"[{index}] must be a non-empty string"
                            )
                if bool(subagents.get("allowed")) and not allowed_profiles:
                    errors.append(
                        "capabilities.subagents.allowed_profiles must contain at least one profile "
                        "when subagents are allowed"
                    )
            if version == RUN_CONTRACT_VERSION_V4:
                interception = capabilities.get("interception")
                if not isinstance(interception, dict):
                    errors.append("capabilities.interception must be an object")
                else:
                    if interception.get("enabled") is not True:
                        errors.append("capabilities.interception.enabled must be true")
                    if interception.get("fail_mode") != "fail_closed":
                        errors.append("capabilities.interception.fail_mode must be 'fail_closed'")
                    if interception.get("action_log_path") != DEFAULT_INTERCEPTION_ACTION_LOG_PATH:
                        errors.append(
                            "capabilities.interception.action_log_path must be "
                            f"'{DEFAULT_INTERCEPTION_ACTION_LOG_PATH}'"
                        )
            if capabilities.get("enabled") is False and bool(
                (capabilities.get("subagents") or {}).get("allowed")
            ):
                errors.append("capabilities.enabled must be true when subagents are allowed")
    return errors


def load_run_contract(run_contract_path: pathlib.Path) -> dict[str, Any]:
    return json.loads(run_contract_path.read_text(encoding="utf-8"))


def resolve_execution_settings(
    run_contract: dict[str, Any],
    *,
    profile_override: str | None = None,
) -> dict[str, Any]:
    version = run_contract.get("run_contract_version", RUN_CONTRACT_VERSION_V1)
    if profile_override is not None and profile_override not in EXECUTION_PROFILES:
        raise ValueError(f"unsupported execution profile: {profile_override}")

    if version == RUN_CONTRACT_VERSION_V2:
        profile = profile_override or run_contract.get("execution_profile") or "strict"
        retrieval = dict(DEFAULT_RETRIEVAL_CONFIG)
        retrieval.update(run_contract.get("retrieval", {}))
        return {
            "run_contract_version": version,
            "execution_profile": profile,
            "policy_path": (
                default_policy_path(profile) if profile_override else run_contract["policy_path"]
            ),
            "context_dir": run_contract["context_dir"],
            "context_manifest_path": run_contract["context_manifest_path"],
            "context_summary_path": run_contract["context_summary_path"],
            "retrieval": retrieval,
            "retrieval_enabled": profile in {"capability", "heavy_tools"}
            and bool(retrieval.get("enabled") or profile in {"capability", "heavy_tools"}),
        }
    if version in {RUN_CONTRACT_VERSION_V3, RUN_CONTRACT_VERSION_V4}:
        profile = profile_override or run_contract.get("execution_profile") or "strict"
        retrieval = dict(DEFAULT_RETRIEVAL_CONFIG)
        retrieval.update(run_contract.get("retrieval", {}))
        capabilities = copy.deepcopy(DEFAULT_CAPABILITIES_CONFIG)
        capabilities.update(run_contract.get("capabilities", {}))
        capabilities["subagents"] = {
            **copy.deepcopy(DEFAULT_CAPABILITIES_CONFIG["subagents"]),
            **dict((run_contract.get("capabilities") or {}).get("subagents", {})),
        }
        transport = {"mode": "cli_json", **dict(run_contract.get("transport", {}))}
        interception = copy.deepcopy(DEFAULT_INTERCEPTION_CONFIG)
        interception.update(dict((run_contract.get("capabilities") or {}).get("interception", {})))
        return {
            "run_contract_version": version,
            "execution_profile": profile,
            "policy_path": (
                default_policy_path(profile) if profile_override else run_contract["policy_path"]
            ),
            "context_dir": run_contract["context_dir"],
            "context_manifest_path": run_contract["context_manifest_path"],
            "context_summary_path": run_contract["context_summary_path"],
            "retrieval": retrieval,
            "retrieval_enabled": profile in {"capability", "heavy_tools"}
            and bool(retrieval.get("enabled") or profile in {"capability", "heavy_tools"}),
            "transport": transport,
            "transport_mode": transport["mode"],
            "capabilities": capabilities,
            "capabilities_enabled": bool(capabilities.get("enabled", False)),
            "capability_library_path": capabilities.get("library_path"),
            "capability_manifest_path": capabilities.get("manifest_path"),
            "subagents_allowed": bool(capabilities.get("subagents", {}).get("allowed", False)),
            "subagent_max_agents": int(capabilities.get("subagents", {}).get("max_agents", 0) or 0),
            "allowed_subagent_profiles": list(
                capabilities.get("subagents", {}).get("allowed_profiles", [])
            ),
            "interception_enabled": version == RUN_CONTRACT_VERSION_V4,
            "interception_fail_mode": interception.get("fail_mode", "fail_closed"),
            "interception_action_log_path": interception.get(
                "action_log_path", DEFAULT_INTERCEPTION_ACTION_LOG_PATH
            ),
        }

    profile = profile_override or "strict"
    return {
        "run_contract_version": RUN_CONTRACT_VERSION_V1,
        "execution_profile": profile,
        "policy_path": default_policy_path(profile),
        "context_dir": "context",
        "context_manifest_path": "context/retrieval-manifest.json",
        "context_summary_path": "context/retrieval-summary.md",
        "retrieval": dict(DEFAULT_RETRIEVAL_CONFIG),
        "retrieval_enabled": False,
        "transport": {"mode": "cli_json"},
        "transport_mode": "cli_json",
        "capabilities": copy.deepcopy(DEFAULT_CAPABILITIES_CONFIG),
        "capabilities_enabled": False,
        "capability_library_path": DEFAULT_CAPABILITIES_CONFIG["library_path"],
        "capability_manifest_path": DEFAULT_CAPABILITIES_CONFIG["manifest_path"],
        "subagents_allowed": False,
        "subagent_max_agents": 0,
        "allowed_subagent_profiles": [],
    }


# Task parsing helpers
def _commit_section(
    current: str | None,
    lines: list[str],
    *,
    sections: dict[str, str],
    order: list[str],
    duplicates: list[str],
) -> None:
    if current is None:
        return
    content = "\n".join(lines).strip()
    if current in sections:
        duplicates.append(current)
    sections[current] = content
    order.append(current)


def _split_sections(text: str) -> tuple[list[str], dict[str, str], list[str]]:
    order: list[str] = []
    sections: dict[str, str] = {}
    duplicates: list[str] = []
    current: str | None = None
    lines: list[str] = []

    for raw_line in text.splitlines():
        heading = re.match(r"^##\s+(.*?)\s*$", raw_line)
        if heading:
            _commit_section(current, lines, sections=sections, order=order, duplicates=duplicates)
            current = heading.group(1).strip()
            lines = []
            continue
        if current is not None:
            lines.append(raw_line)

    _commit_section(current, lines, sections=sections, order=order, duplicates=duplicates)
    return order, sections, duplicates


def _extract_task_title(text: str) -> str:
    saw_task_heading = False
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not saw_task_heading:
            if re.match(r"^#\s+Task\s*$", stripped):
                saw_task_heading = True
            continue
        if not stripped:
            continue
        if stripped.startswith("## "):
            break
        return stripped
    return ""


def _extract_json_block(section_text: str) -> tuple[str | None, str | None]:
    match = re.search(r"```json\s*(.*?)\s*```", section_text, flags=re.DOTALL | re.IGNORECASE)
    if not match:
        return None, "result schema section must include a fenced ```json block"
    return match.group(1).strip(), None


def _contains_network_indicator(tokens: list[str], text: str) -> bool:
    if any(token.startswith(("http://", "https://")) for token in tokens):
        return True
    if re.search(r"https?://", text):
        return True
    return bool(NETWORK_TOKEN_RE.search(text))


def _unwrap_env_argv(argv: list[str]) -> tuple[list[str], str | None]:
    if not argv or pathlib.Path(argv[0]).name != "env":
        return argv, None

    idx = 1
    while idx < len(argv):
        token = argv[idx]
        if ENV_ASSIGNMENT_RE.match(token):
            idx += 1
            continue
        if token == "-i":
            idx += 1
            continue
        if token == "-u" and idx + 1 < len(argv):
            idx += 2
            continue
        if token.startswith("-u") and len(token) > 2:
            idx += 1
            continue
        break

    if idx >= len(argv):
        return [], "env must delegate to a command in eval mode"
    return argv[idx:], None


def analyze_eval_command(
    raw_command: str,
    *,
    eval_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    policy = eval_policy or load_policy(default_policy_path("strict"))
    detail: dict[str, Any] = {
        "raw": raw_command,
        "argv": [],
        "program": None,
        "safe_for_default_eval": False,
        "requires_opt_in": True,
        "network_access": False,
        "dangerous_reasons": [],
    }
    if re.search(r"(\|\||&&|[|;<>`])|\$\(", raw_command):
        detail["dangerous_reasons"].append("shell metacharacters are not allowed by default")
    try:
        argv = shlex.split(raw_command, posix=True)
    except ValueError as exc:
        detail["dangerous_reasons"].append(f"command parse error: {exc}")
        return detail

    if not argv:
        detail["dangerous_reasons"].append("empty command")
        return detail

    detail["argv"] = argv
    program = argv[0]
    detail["program"] = program
    effective_argv = argv
    env_unwrap_error: str | None = None
    if pathlib.Path(program).name == "env":
        effective_argv, env_unwrap_error = _unwrap_env_argv(argv)
        if env_unwrap_error:
            detail["dangerous_reasons"].append(env_unwrap_error)
        elif effective_argv:
            detail["program"] = effective_argv[0]

    if not effective_argv:
        detail["requires_opt_in"] = bool(detail["dangerous_reasons"])
        detail["safe_for_default_eval"] = not detail["requires_opt_in"]
        return detail

    program_name = pathlib.Path(effective_argv[0]).name
    is_repo_script = effective_argv[0].startswith("./") or effective_argv[0].startswith("starter/")
    is_shell_wrapper = program_name in {"bash", "sh"} and any(
        arg in {"-c", "-lc"} for arg in effective_argv[1:]
    )
    is_python_wrapper = (
        program_name in {"python", "python3"}
        and len(effective_argv) >= 2
        and effective_argv[1] == "-c"
    )
    wrapper_payload = "\n".join(effective_argv[2:] if is_shell_wrapper else effective_argv[2:3])

    if is_shell_wrapper or is_python_wrapper:
        detail["dangerous_reasons"].append(
            f"{program_name} wrapper execution requires {policy['opt_in_env']}=1"
        )
    if program_name in policy["blocked_programs"]:
        detail["dangerous_reasons"].append(f"{program_name} is blocked by the eval policy")
    if program_name in policy["network_programs"] or _contains_network_indicator(
        effective_argv[1:],
        wrapper_payload,
    ):
        detail["network_access"] = True
        detail["dangerous_reasons"].append(
            f"network access requires {policy['allow_network_env']}=1"
        )

    allowed = program_name in policy["allowed_programs"] or is_repo_script
    if not allowed:
        detail["dangerous_reasons"].append(f"{program_name} is not in the default eval allowlist")

    detail["requires_opt_in"] = bool(detail["dangerous_reasons"])
    detail["safe_for_default_eval"] = not detail["requires_opt_in"]
    return detail


def _parse_eval_section(
    section_text: str,
    errors: list[str],
    *,
    eval_policy: dict[str, Any],
) -> tuple[list[str], list[dict[str, Any]]]:
    eval_commands: list[str] = []
    eval_command_details: list[dict[str, Any]] = []
    if not section_text:
        return eval_commands, eval_command_details

    bash_blocks = re.findall(
        r"```bash\s*(.*?)\s*```",
        section_text,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if len(bash_blocks) != 1:
        errors.append("Eval section must contain exactly one fenced ```bash block")
        return eval_commands, eval_command_details

    for raw_line in bash_blocks[0].splitlines():
        command = raw_line.strip()
        if not command or command.startswith("#"):
            continue
        eval_commands.append(command)
        eval_command_details.append(analyze_eval_command(command, eval_policy=eval_policy))
    return eval_commands, eval_command_details


def _parse_required_artifacts(section_text: str) -> list[str]:
    required_artifacts: list[str] = []
    for raw_line in section_text.splitlines():
        match = re.match(r"^\s*[-*]\s+(.*\S)\s*$", raw_line)
        if match:
            required_artifacts.append(match.group(1))
    return required_artifacts


def _parse_schema_section(
    section_text: str | None, errors: list[str]
) -> tuple[str | None, dict[str, Any] | None]:
    if section_text is None:
        return None, None

    schema_text, schema_error = _extract_json_block(section_text)
    if schema_error:
        errors.append(schema_error)
        return None, None
    if not schema_text:
        return None, None

    try:
        schema_payload = json.loads(schema_text)
    except json.JSONDecodeError as exc:
        errors.append(f"result schema JSON is invalid: {exc}")
        return schema_text, None

    if schema_payload.get("x-interface-version") not in {None, RESULT_INTERFACE_VERSION}:
        errors.append("result schema x-interface-version must be v1")
    return schema_text, schema_payload


def parse_task_text(
    text: str,
    *,
    source: str = "<memory>",
    eval_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    errors: list[str] = []
    policy = eval_policy or load_policy(default_policy_path("strict"))
    has_task_heading = bool(re.search(r"(?m)^#\s+Task\s*$", text))
    if not has_task_heading:
        errors.append("missing required heading: # Task")

    section_order, sections, duplicates = _split_sections(text)
    for duplicate in duplicates:
        errors.append(f"duplicate section heading: {duplicate}")

    missing_sections = [name for name in TASK_REQUIRED_SECTIONS if name not in sections]
    for missing in missing_sections:
        errors.append(f"missing required section: {missing}")

    unknown_sections = [
        name
        for name in section_order
        if name not in TASK_REQUIRED_SECTIONS and name not in TASK_OPTIONAL_SECTIONS
    ]

    eval_commands, eval_command_details = _parse_eval_section(
        sections.get("Eval", ""),
        errors,
        eval_policy=policy,
    )
    required_artifacts = _parse_required_artifacts(sections.get("Required Artifacts", ""))
    schema_text, schema_payload = _parse_schema_section(
        sections.get("Result JSON schema (source of truth)"),
        errors,
    )

    return {
        "ok": len(errors) == 0,
        "source": source,
        "task_heading_present": has_task_heading,
        "task_title": _extract_task_title(text),
        "section_order": section_order,
        "sections": sections,
        "missing_sections": missing_sections,
        "unknown_sections": unknown_sections,
        "errors": errors,
        "eval_commands": eval_commands,
        "eval_command_details": eval_command_details,
        "dangerous_eval_commands": [
            detail for detail in eval_command_details if detail["requires_opt_in"]
        ],
        "required_artifacts": required_artifacts,
        "result_schema_block_present": schema_text is not None,
        "result_schema": schema_payload,
        "result_schema_sha256": sha256_text(schema_text) if schema_text else None,
    }


def parse_task_file(
    task_path: pathlib.Path,
    *,
    eval_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return parse_task_text(
        task_path.read_text(encoding="utf-8"),
        source=str(task_path),
        eval_policy=eval_policy,
    )


# Result validation helpers
def _fallback_validate_result(payload: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []

    def add(field: str, expected: str, observed: Any) -> None:
        issues.append(
            {
                "field": field,
                "expected": expected,
                "observed": observed,
            }
        )

    if payload.get("x-interface-version") != RESULT_INTERFACE_VERSION:
        add("x-interface-version", RESULT_INTERFACE_VERSION, payload.get("x-interface-version"))
    if payload.get("status") not in {"success", "partial", "failed"}:
        add("status", "success|partial|failed", payload.get("status"))
    if not isinstance(payload.get("summary"), str) or payload["summary"] == "":
        add("summary", "non-empty string", payload.get("summary"))
    if not isinstance(payload.get("artifacts"), list):
        add("artifacts", "array", payload.get("artifacts"))
    if not isinstance(payload.get("claims"), list):
        add("claims", "array", payload.get("claims"))
    if not isinstance(payload.get("remaining_risks"), list) or not all(
        isinstance(item, str) for item in payload["remaining_risks"]
    ):
        add("remaining_risks", "array of strings", payload.get("remaining_risks"))
    return issues


def validate_result_payload(
    payload: dict[str, Any], schema_payload: dict[str, Any] | None
) -> list[dict[str, Any]]:
    if schema_payload is None:
        return _fallback_validate_result(payload)
    try:
        import jsonschema
    except ImportError:
        return _fallback_validate_result(payload)

    validator = jsonschema.Draft202012Validator(schema_payload)
    issues = []
    for error in sorted(validator.iter_errors(payload), key=lambda item: list(item.absolute_path)):
        path = ".".join(str(item) for item in error.absolute_path) or "<root>"
        issues.append(
            {
                "field": path,
                "expected": error.validator,
                "observed": error.message,
            }
        )
    return issues


def canonicalize_json_file(path: pathlib.Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    write_json(path, payload)
    return payload


# Secret scanning helpers
def scan_text_for_secrets(text: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for name, pattern in SECRET_PATTERNS:
        for match in pattern.finditer(text):
            findings.append(
                {
                    "pattern": name,
                    "match": match.group(0)[:8] + "...",
                }
            )
    return findings


def scan_paths_for_secrets(paths: list[pathlib.Path]) -> list[dict[str, Any]]:
    def scan_path(path: pathlib.Path) -> list[dict[str, Any]]:
        if not path.exists() or not path.is_file():
            return []
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return []
        path_findings: list[dict[str, Any]] = []
        for finding in scan_text_for_secrets(text):
            path_findings.append({"path": str(path), **finding})
        return path_findings

    findings: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(paths)))) as executor:
        for path_findings in executor.map(scan_path, paths):
            findings.extend(path_findings)
    return sorted(findings, key=lambda item: (str(item["path"]), str(item["pattern"])))
