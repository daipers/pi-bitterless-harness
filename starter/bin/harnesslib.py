#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import shlex
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

# Contract constants and defaults
RUNNER_VERSION = "1.0.0"
RUN_CONTRACT_VERSION_V1 = "v1"
RUN_CONTRACT_VERSION_V2 = "v2"
RUN_CONTRACT_VERSION = RUN_CONTRACT_VERSION_V2
RESULT_INTERFACE_VERSION = "v1"
EXECUTION_PROFILES = {"strict", "capability"}

TASK_REQUIRED_SECTIONS = [
    "Goal",
    "Constraints",
    "Done",
    "Eval",
    "Required Artifacts",
    "Result JSON schema (source of truth)",
]
TASK_OPTIONAL_SECTIONS = {"Notes"}

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
    payload = dict(payload)
    payload["path"] = str(path)
    payload["relative_path"] = str(path.relative_to(repo_root or script_root()))
    return payload


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
        "summary": "short summary of what was done",
        "artifacts": [
            {
                "path": "outputs/example-output.txt",
                "description": "proof artifact",
            }
        ],
        "claims": [
            {
                "claim": "brief claim",
                "evidence": ["path or command"],
            }
        ],
        "remaining_risks": ["optional remaining risks"],
    }


def default_run_contract(
    *,
    version: str = RUN_CONTRACT_VERSION,
    execution_profile: str = "strict",
) -> dict[str, Any]:
    if version == RUN_CONTRACT_VERSION_V1:
        strict_policy = load_policy(default_policy_path("strict"))
        return {
            "run_contract_version": RUN_CONTRACT_VERSION_V1,
            "result_interface_version": RESULT_INTERFACE_VERSION,
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
            "eval_policy": {
                "opt_in_env": strict_policy["opt_in_env"],
                "allow_network_env": strict_policy["allow_network_env"],
                "allowed_programs": strict_policy["allowed_programs"],
                "blocked_programs": strict_policy["blocked_programs"],
            },
        }

    if execution_profile not in EXECUTION_PROFILES:
        raise ValueError(f"unsupported execution profile: {execution_profile}")

    return {
        "run_contract_version": RUN_CONTRACT_VERSION_V2,
        "result_interface_version": RESULT_INTERFACE_VERSION,
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
        "execution_profile": execution_profile,
        "policy_path": default_policy_path(execution_profile),
        "context_dir": "context",
        "context_manifest_path": "context/retrieval-manifest.json",
        "context_summary_path": "context/retrieval-summary.md",
        "retrieval": {
            **DEFAULT_RETRIEVAL_CONFIG,
            "enabled": execution_profile == "capability",
        },
    }


def validate_run_contract(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    version = payload.get("run_contract_version")
    if version == RUN_CONTRACT_VERSION_V1:
        expected = default_run_contract(version=RUN_CONTRACT_VERSION_V1)
        if payload.get("result_interface_version") != RESULT_INTERFACE_VERSION:
            errors.append("result_interface_version must be v1")
        for key in [
            "required_run_files",
            "required_directories",
            "required_task_sections",
            "result_schema_path",
            "result_template_path",
            "manifest_path",
            "event_log_path",
            "eval_policy",
        ]:
            if key not in payload:
                errors.append(f"missing run contract field: {key}")
        if isinstance(payload.get("eval_policy"), dict):
            for key in ["opt_in_env", "allow_network_env", "allowed_programs", "blocked_programs"]:
                if key not in payload["eval_policy"]:
                    errors.append(f"missing eval_policy field: {key}")
        else:
            errors.append("eval_policy must be an object")
        for key in [
            "result_schema_path",
            "result_template_path",
            "manifest_path",
            "event_log_path",
        ]:
            if key in payload and payload[key] != expected[key]:
                errors.append(f"{key} must be {expected[key]!r}")
        return errors

    if version != RUN_CONTRACT_VERSION_V2:
        errors.append("run_contract_version must be v1 or v2")
        return errors

    expected = default_run_contract(version=RUN_CONTRACT_VERSION_V2)
    if payload.get("result_interface_version") != RESULT_INTERFACE_VERSION:
        errors.append("result_interface_version must be v1")
    for key in [
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
    ]:
        if key not in payload:
            errors.append(f"missing run contract field: {key}")
    for key in ["result_schema_path", "result_template_path", "manifest_path", "event_log_path"]:
        if key in payload and payload[key] != expected[key]:
            errors.append(f"{key} must be {expected[key]!r}")
    for key in ["context_dir", "context_manifest_path", "context_summary_path"]:
        if key in payload and payload[key] != expected[key]:
            errors.append(f"{key} must be {expected[key]!r}")
    if payload.get("execution_profile") not in EXECUTION_PROFILES:
        errors.append("execution_profile must be strict or capability")
    if not isinstance(payload.get("policy_path"), str) or not payload["policy_path"]:
        errors.append("policy_path must be a non-empty string")
    retrieval = payload.get("retrieval")
    if not isinstance(retrieval, dict):
        errors.append("retrieval must be an object")
    else:
        for key in ["enabled", "source", "max_source_runs", "max_artifacts_per_run", "max_artifact_bytes"]:
            if key not in retrieval:
                errors.append(f"missing retrieval field: {key}")
        if retrieval.get("source") != DEFAULT_RETRIEVAL_CONFIG["source"]:
            errors.append("retrieval.source must be 'prior_runs'")
        if "strategy" in retrieval and retrieval["strategy"] != DEFAULT_RETRIEVAL_CONFIG["strategy"]:
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
                default_policy_path(profile)
                if profile_override
                else run_contract["policy_path"]
            ),
            "context_dir": run_contract["context_dir"],
            "context_manifest_path": run_contract["context_manifest_path"],
            "context_summary_path": run_contract["context_summary_path"],
            "retrieval": retrieval,
            "retrieval_enabled": profile == "capability"
            and bool(retrieval.get("enabled") or profile_override == "capability"),
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
