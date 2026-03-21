#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import shlex
from datetime import UTC, datetime
from typing import Any

RUNNER_VERSION = "1.0.0"
RUN_CONTRACT_VERSION = "v1"
RESULT_INTERFACE_VERSION = "v1"

TASK_REQUIRED_SECTIONS = [
    "Goal",
    "Constraints",
    "Done",
    "Eval",
    "Required Artifacts",
    "Result JSON schema (source of truth)",
]
TASK_OPTIONAL_SECTIONS = {"Notes"}

ALLOWED_EVAL_PROGRAMS = [
    "awk",
    "bash",
    "cat",
    "cmp",
    "cut",
    "diff",
    "echo",
    "env",
    "find",
    "git",
    "grep",
    "head",
    "jq",
    "ls",
    "printf",
    "pwd",
    "py.test",
    "pytest",
    "python",
    "python3",
    "rg",
    "ruff",
    "sed",
    "sh",
    "sort",
    "tail",
    "test",
    "uniq",
    "wc",
    "xargs",
]
BLOCKED_EVAL_PROGRAMS = [
    "curl",
    "dd",
    "docker",
    "kill",
    "killall",
    "kubectl",
    "mkfs",
    "osascript",
    "reboot",
    "rm",
    "rsync",
    "scp",
    "shutdown",
    "ssh",
    "sudo",
    "telnet",
    "truncate",
    "wget",
]
NETWORK_EVAL_PROGRAMS = ["curl", "nc", "ping", "scp", "ssh", "telnet", "wget"]

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


def default_run_contract() -> dict[str, Any]:
    return {
        "run_contract_version": RUN_CONTRACT_VERSION,
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
            "opt_in_env": "HARNESS_ALLOW_DANGEROUS_EVAL",
            "allow_network_env": "HARNESS_ALLOW_NETWORK_TASKS",
            "allowed_programs": ALLOWED_EVAL_PROGRAMS,
            "blocked_programs": BLOCKED_EVAL_PROGRAMS,
        },
    }


def validate_run_contract(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    expected = default_run_contract()
    if payload.get("run_contract_version") != RUN_CONTRACT_VERSION:
        errors.append("run_contract_version must be v1")
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
    for key in ["result_schema_path", "result_template_path", "manifest_path", "event_log_path"]:
        if key in payload and payload[key] != expected[key]:
            errors.append(f"{key} must be {expected[key]!r}")
    return errors


def _split_sections(text: str) -> tuple[list[str], dict[str, str], list[str]]:
    order: list[str] = []
    sections: dict[str, str] = {}
    duplicates: list[str] = []
    current: str | None = None
    lines: list[str] = []

    for raw_line in text.splitlines():
        heading = re.match(r"^##\s+(.*?)\s*$", raw_line)
        if heading:
            if current is not None:
                content = "\n".join(lines).strip()
                if current in sections:
                    duplicates.append(current)
                sections[current] = content
                order.append(current)
            current = heading.group(1).strip()
            lines = []
            continue
        if current is not None:
            lines.append(raw_line)

    if current is not None:
        content = "\n".join(lines).strip()
        if current in sections:
            duplicates.append(current)
        sections[current] = content
        order.append(current)

    return order, sections, duplicates


def _extract_json_block(section_text: str) -> tuple[str | None, str | None]:
    match = re.search(r"```json\s*(.*?)\s*```", section_text, flags=re.DOTALL | re.IGNORECASE)
    if not match:
        return None, "result schema section must include a fenced ```json block"
    return match.group(1).strip(), None


def analyze_eval_command(raw_command: str) -> dict[str, Any]:
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
    program_name = pathlib.Path(program).name
    is_repo_script = program.startswith("./") or program.startswith("starter/")
    if program_name in BLOCKED_EVAL_PROGRAMS:
        detail["dangerous_reasons"].append(f"{program_name} is blocked by the eval policy")
    if program_name in NETWORK_EVAL_PROGRAMS or any(
        token.startswith(("http://", "https://")) for token in argv[1:]
    ):
        detail["network_access"] = True
        detail["dangerous_reasons"].append("network access requires HARNESS_ALLOW_NETWORK_TASKS=1")

    allowed = program_name in ALLOWED_EVAL_PROGRAMS or is_repo_script
    if not allowed:
        detail["dangerous_reasons"].append(f"{program_name} is not in the default eval allowlist")

    detail["requires_opt_in"] = bool(detail["dangerous_reasons"])
    detail["safe_for_default_eval"] = not detail["requires_opt_in"]
    return detail


def parse_task_text(text: str, *, source: str = "<memory>") -> dict[str, Any]:
    errors: list[str] = []
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

    eval_commands: list[str] = []
    eval_command_details: list[dict[str, Any]] = []
    eval_section = sections.get("Eval", "")
    if eval_section:
        bash_blocks = re.findall(
            r"```bash\s*(.*?)\s*```",
            eval_section,
            flags=re.DOTALL | re.IGNORECASE,
        )
        if len(bash_blocks) != 1:
            errors.append("Eval section must contain exactly one fenced ```bash block")
        else:
            for raw_line in bash_blocks[0].splitlines():
                command = raw_line.strip()
                if not command or command.startswith("#"):
                    continue
                eval_commands.append(command)
                eval_command_details.append(analyze_eval_command(command))

    required_artifacts: list[str] = []
    for raw_line in sections.get("Required Artifacts", "").splitlines():
        match = re.match(r"^\s*[-*]\s+(.*\S)\s*$", raw_line)
        if match:
            required_artifacts.append(match.group(1))

    schema_text: str | None = None
    schema_payload: dict[str, Any] | None = None
    if "Result JSON schema (source of truth)" in sections:
        schema_text, schema_error = _extract_json_block(
            sections["Result JSON schema (source of truth)"]
        )
        if schema_error:
            errors.append(schema_error)
        elif schema_text:
            try:
                schema_payload = json.loads(schema_text)
            except json.JSONDecodeError as exc:
                errors.append(f"result schema JSON is invalid: {exc}")
            else:
                if schema_payload.get("x-interface-version") not in {
                    None,
                    RESULT_INTERFACE_VERSION,
                }:
                    errors.append("result schema x-interface-version must be v1")

    return {
        "ok": len(errors) == 0,
        "source": source,
        "task_heading_present": has_task_heading,
        "section_order": section_order,
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


def parse_task_file(task_path: pathlib.Path) -> dict[str, Any]:
    return parse_task_text(task_path.read_text(encoding="utf-8"), source=str(task_path))


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
    findings: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists() or not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for finding in scan_text_for_secrets(text):
            findings.append({"path": str(path), **finding})
    return findings


def compute_dependencies_hash(dependencies: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(dependencies, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value not in {"", "0", "false", "False", "no", "NO"}
