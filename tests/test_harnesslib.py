from __future__ import annotations

import builtins
import json
import pathlib

import harnesslib
import pytest
from harnesslib import (
    RESULT_INTERFACE_VERSION,
    analyze_eval_command,
    canonicalize_json_file,
    compute_dependencies_hash,
    default_run_contract,
    env_flag,
    evaluate_required_artifact_path,
    evaluate_policy_guardrail,
    load_policy,
    make_result_template,
    scan_paths_for_secrets,
    scan_text_for_secrets,
    validate_result_payload,
    validate_run_contract,
)


def test_run_contract_and_template_helpers(tmp_path: pathlib.Path) -> None:
    contract = default_run_contract()
    assert validate_run_contract(contract) == []
    assert contract["run_contract_version"] == "v2"
    assert contract["retrieval"]["strategy"] == "hybrid_v1"
    assert contract["retrieval"]["max_candidates"] == 25
    assert contract["retrieval"]["artifact_selection"] == "evidence_first"
    assert len(make_result_template()["artifacts"]) == 1
    assert compute_dependencies_hash({"python": "3.12"})

    broken = dict(contract)
    broken["result_schema_path"] = "wrong.json"
    assert validate_run_contract(broken)
    assert validate_run_contract({"run_contract_version": "broken", "eval_policy": []})

    v1_contract = default_run_contract(version="v1")
    missing_policy_field = dict(v1_contract)
    missing_policy_field["eval_policy"] = dict(v1_contract["eval_policy"])
    missing_policy_field["eval_policy"].pop("blocked_programs")
    assert "missing eval_policy field: blocked_programs" in validate_run_contract(
        missing_policy_field
    )

    template = make_result_template()
    path = tmp_path / "result.json"
    path.write_text(json.dumps(template, indent=4), encoding="utf-8")
    canonical = canonicalize_json_file(path)
    assert canonical["x-interface-version"] == RESULT_INTERFACE_VERSION
    assert path.read_text(encoding="utf-8").endswith("\n")
    assert scan_paths_for_secrets([tmp_path / "missing.txt"]) == []


def test_policy_helpers_cover_resolution_and_invalid_payloads(
    tmp_path: pathlib.Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    assert harnesslib.resolve_policy_path(None, repo_root=repo_root) == (
        repo_root / "policies" / "strict.json"
    )

    absolute_policy = tmp_path / "absolute-policy.json"
    absolute_policy.write_text(
        json.dumps(
            {
                "opt_in_env": "ALLOW_DANGEROUS",
                "allow_network_env": "ALLOW_NETWORK",
                "allowed_programs": ["python3"],
                "blocked_programs": ["rm"],
                "network_programs": ["curl"],
            }
        ),
        encoding="utf-8",
    )
    assert harnesslib.resolve_policy_path(absolute_policy, repo_root=repo_root) == absolute_policy

    missing_field_errors = harnesslib.validate_policy(
        {
            "opt_in_env": "ALLOW_DANGEROUS",
            "allowed_programs": [],
            "blocked_programs": [],
            "network_programs": [],
        }
    )
    assert "missing policy field: allow_network_env" in missing_field_errors

    type_errors = harnesslib.validate_policy(
        {
            "opt_in_env": "ALLOW_DANGEROUS",
            "allow_network_env": "ALLOW_NETWORK",
            "allowed_programs": "python3",
            "blocked_programs": "rm",
            "network_programs": "curl",
        }
    )
    assert "allowed_programs must be an array" in type_errors
    assert "blocked_programs must be an array" in type_errors
    assert "network_programs must be an array" in type_errors

    invalid_policy = repo_root / "broken-policy.json"
    invalid_policy.write_text(
        json.dumps(
            {
                "opt_in_env": "ALLOW_DANGEROUS",
                "allow_network_env": "ALLOW_NETWORK",
                "allowed_programs": "python3",
                "blocked_programs": [],
                "network_programs": [],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="allowed_programs must be an array"):
        load_policy(invalid_policy, repo_root=repo_root)


def test_validate_result_payload_and_secret_helpers(tmp_path: pathlib.Path, monkeypatch) -> None:
    schema = {
        "type": "object",
        "required": [
            "x-interface-version",
            "status",
            "summary",
            "artifacts",
            "claims",
            "remaining_risks",
        ],
        "properties": {
            "x-interface-version": {"const": "v1"},
            "status": {"enum": ["success", "partial", "failed"]},
            "summary": {"type": "string", "minLength": 1},
            "artifacts": {"type": "array"},
            "claims": {"type": "array"},
            "remaining_risks": {"type": "array"},
        },
        "additionalProperties": False,
    }
    invalid_payload = {"status": "done"}
    issues = validate_result_payload(invalid_payload, schema)
    assert issues
    fallback_issues = validate_result_payload({"status": "done"}, None)
    assert fallback_issues

    secret_file = tmp_path / "secret.txt"
    fake_key = "sk-" + "abcdefghijklmnopqrstuvwxyz1234"
    secret_file.write_text(f"OPENAI_API_KEY={fake_key}", encoding="utf-8")
    assert scan_text_for_secrets(secret_file.read_text(encoding="utf-8"))
    assert scan_paths_for_secrets([secret_file])

    monkeypatch.setenv("HARNESS_TEST_FLAG", "1")
    assert env_flag("HARNESS_TEST_FLAG") is True
    monkeypatch.setenv("HARNESS_TEST_FLAG", "0")
    assert env_flag("HARNESS_TEST_FLAG") is False
    monkeypatch.delenv("HARNESS_TEST_FLAG", raising=False)
    assert env_flag("HARNESS_TEST_FLAG", default=True) is True
    assert compute_dependencies_hash({"python": "3.12"}) == compute_dependencies_hash(
        {"python": "3.12"}
    )


def test_task_parse_helpers_cover_eval_artifacts_and_schema_paths(
    tmp_path: pathlib.Path,
    monkeypatch,
) -> None:
    eval_policy = load_policy("policies/strict.json")
    eval_errors: list[str] = []
    assert harnesslib._parse_eval_section("", eval_errors, eval_policy=eval_policy) == ([], [])
    assert eval_errors == []

    commands, details = harnesslib._parse_eval_section(
        "```bash\n\n# comment\npython3 script.py\n```",
        [],
        eval_policy=eval_policy,
    )
    assert commands == ["python3 script.py"]
    assert details[0]["argv"] == ["python3", "script.py"]

    malformed_errors: list[str] = []
    assert harnesslib._parse_eval_section(
        "no block",
        malformed_errors,
        eval_policy=eval_policy,
    ) == ([], [])
    assert malformed_errors == ["Eval section must contain exactly one fenced ```bash block"]

    assert harnesslib._parse_required_artifacts("- one\nnot a bullet\n* two\n") == ["one", "two"]

    schema_errors: list[str] = []
    assert harnesslib._parse_schema_section(None, schema_errors) == (None, None)
    assert schema_errors == []

    schema_errors = []
    assert harnesslib._parse_schema_section("not json", schema_errors) == (None, None)
    assert "result schema section must include a fenced ```json block" in schema_errors

    schema_errors = []
    schema_text, schema_payload = harnesslib._parse_schema_section(
        "```json\n{\"type\": }\n```",
        schema_errors,
    )
    assert schema_text is not None
    assert schema_payload is None
    assert any("result schema JSON is invalid:" in error for error in schema_errors)

    schema_errors = []
    schema_text, schema_payload = harnesslib._parse_schema_section(
        "```json\n{\"x-interface-version\":\"v2\"}\n```",
        schema_errors,
    )
    assert schema_text is not None
    assert schema_payload == {"x-interface-version": "v2"}
    assert "result schema x-interface-version must be v1" in schema_errors

    task_path = tmp_path / "task.md"
    task_path.write_text(
        "# Task\n\n## Goal\nGoal\n\n## Constraints\n- None\n\n## Done\n- Done\n\n"
        "## Eval\n```bash\npython3 script.py\n```\n\n## Required Artifacts\n- outputs/x\n\n"
        "## Result JSON schema (source of truth)\n```json\n{\"type\":\"object\"}\n```\n",
        encoding="utf-8",
    )
    payload = harnesslib.parse_task_file(task_path)
    assert payload["source"] == str(task_path)
    assert payload["required_artifacts"] == ["outputs/x"]

    monkeypatch.setattr(harnesslib, "_extract_json_block", lambda _text: ("", None))
    assert harnesslib._parse_schema_section("```json\n{}\n```", []) == (None, None)


def test_validate_result_payload_fallback_covers_all_fields() -> None:
    issues = validate_result_payload(
        {
            "x-interface-version": "broken",
            "status": "done",
            "summary": "",
            "artifacts": "bad",
            "claims": "bad",
            "remaining_risks": [1],
        },
        None,
    )

    fields = {issue["field"] for issue in issues}
    assert {"x-interface-version", "status", "summary", "artifacts", "claims"} <= fields
    assert "remaining_risks" in fields


def test_analyze_eval_command_flags_network_and_metacharacters() -> None:
    detail = analyze_eval_command("curl https://example.com")
    assert detail["requires_opt_in"] is True
    assert detail["network_access"] is True

    detail = analyze_eval_command("bash -c 'curl https://example.com'")
    assert detail["requires_opt_in"] is True
    assert detail["network_access"] is True
    assert any("wrapper execution" in reason for reason in detail["dangerous_reasons"])

    detail = analyze_eval_command("python3 -c 'print(1)'")
    assert detail["requires_opt_in"] is True
    assert detail["network_access"] is False

    detail = analyze_eval_command("env FOO=1 python3 -c 'print(1)'")
    assert detail["program"] == "python3"
    assert detail["requires_opt_in"] is True

    detail = analyze_eval_command("python3 -m pytest tests/test_runner_e2e.py -q")
    assert detail["requires_opt_in"] is False

    detail = analyze_eval_command("python3 script.py && echo done")
    assert detail["requires_opt_in"] is True
    empty = analyze_eval_command("")
    assert empty["requires_opt_in"] is True
    broken = analyze_eval_command("'")
    assert broken["requires_opt_in"] is True


def test_run_contract_and_eval_helpers_cover_remaining_error_paths() -> None:
    with pytest.raises(ValueError, match="unsupported execution profile"):
        default_run_contract(execution_profile="broken")

    with pytest.raises(ValueError, match="unsupported execution profile"):
        harnesslib.resolve_execution_settings(default_run_contract(), profile_override="broken")

    broken_v1 = default_run_contract(version="v1")
    broken_v1["result_interface_version"] = "v2"
    broken_v1["event_log_path"] = "wrong.jsonl"
    v1_errors = validate_run_contract(broken_v1)
    assert "result_interface_version must be v1" in v1_errors
    assert "event_log_path must be 'run-events.jsonl'" in v1_errors

    missing_eval_policy_v1 = default_run_contract(version="v1")
    missing_eval_policy_v1.pop("eval_policy")
    missing_eval_policy_errors = validate_run_contract(missing_eval_policy_v1)
    assert "missing run contract field: eval_policy" in missing_eval_policy_errors
    assert "eval_policy must be an object" in missing_eval_policy_errors

    missing_fields_v2 = default_run_contract()
    missing_fields_v2.pop("policy_path")
    missing_fields_v2.pop("retrieval")
    missing_errors = validate_run_contract(missing_fields_v2)
    assert "missing run contract field: policy_path" in missing_errors
    assert "policy_path must be a non-empty string" in missing_errors
    assert "missing run contract field: retrieval" in missing_errors
    assert "retrieval must be an object" in missing_errors

    broken_v2 = default_run_contract()
    broken_v2["result_interface_version"] = "v2"
    broken_v2["context_summary_path"] = "wrong.md"
    broken_v2["execution_profile"] = "broken"
    broken_v2["policy_path"] = ""
    broken_v2["retrieval"] = {
        "enabled": "yes",
        "source": "memory",
        "strategy": "hybrid_v2",
        "max_source_runs": 0,
        "max_candidates": 0,
        "max_artifact_bytes": 0,
        "artifact_selection": "random",
    }
    v2_errors = validate_run_contract(broken_v2)
    assert "result_interface_version must be v1" in v2_errors
    assert "context_summary_path must be 'context/retrieval-summary.md'" in v2_errors
    assert "execution_profile must be one of: strict, offline, networked, heavy_tools, capability" in v2_errors
    assert "policy_path must be a non-empty string" in v2_errors
    assert "missing retrieval field: max_artifacts_per_run" in v2_errors
    assert "retrieval.source must be 'prior_runs'" in v2_errors
    assert "retrieval.strategy must be 'hybrid_v1'" in v2_errors
    assert "retrieval.max_source_runs must be a positive integer" in v2_errors
    assert "retrieval.max_candidates must be a positive integer" in v2_errors
    assert "retrieval.max_artifact_bytes must be a positive integer" in v2_errors
    assert "retrieval.artifact_selection must be 'evidence_first'" in v2_errors
    assert "retrieval.enabled must be a boolean" in v2_errors


def test_analyze_eval_command_and_env_unwrap_cover_remaining_edge_cases() -> None:
    detail = analyze_eval_command("python3 https://example.com")
    assert detail["network_access"] is True

    passthrough_argv, passthrough_error = harnesslib._unwrap_env_argv(["python3", "script.py"])
    assert passthrough_argv == ["python3", "script.py"]
    assert passthrough_error is None

    unwrapped_argv, unwrap_error = harnesslib._unwrap_env_argv(
        ["env", "-i", "-u", "FOO", "-uBAR", "python3", "script.py"]
    )
    assert unwrapped_argv == ["python3", "script.py"]
    assert unwrap_error is None

    env_only = analyze_eval_command("env -i")
    assert "env must delegate to a command in eval mode" in env_only["dangerous_reasons"]
    assert env_only["requires_opt_in"] is True
    assert env_only["safe_for_default_eval"] is False


def test_evaluate_required_artifact_path_enforces_run_bounds(tmp_path: pathlib.Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "outputs").mkdir(parents=True)
    inside = evaluate_required_artifact_path(run_dir, "outputs/claim.txt")
    assert inside["valid"] is True

    absolute = evaluate_required_artifact_path(run_dir, str(tmp_path / "outside.txt"))
    assert absolute["valid"] is False
    assert absolute["status"] == "invalid_out_of_run_scope"

    escaped = evaluate_required_artifact_path(run_dir, "../outside.txt")
    assert escaped["valid"] is False

    outside = tmp_path / "outside.txt"
    outside.write_text("hi\n", encoding="utf-8")
    linked = run_dir / "linked.txt"
    linked.symlink_to(outside)
    symlinked = evaluate_required_artifact_path(run_dir, "linked.txt")
    assert symlinked["valid"] is False


def test_scan_paths_for_secrets_tolerates_read_errors(
    tmp_path: pathlib.Path,
    monkeypatch,
) -> None:
    readable = tmp_path / "readable.txt"
    readable.write_text(
        "OPENAI_API_KEY=" + ("sk-" + "abcdefghijklmnopqrstuvwxyz1234"),
        encoding="utf-8",
    )
    unreadable = tmp_path / "unreadable.txt"
    unreadable.write_text("secret", encoding="utf-8")
    original_read_text = pathlib.Path.read_text

    def fake_read_text(self: pathlib.Path, *args, **kwargs) -> str:
        if self == unreadable:
            raise OSError("boom")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "read_text", fake_read_text)

    findings = scan_paths_for_secrets([unreadable, readable])

    assert len(findings) == 1
    assert findings[0]["path"] == str(readable)


def test_validate_result_payload_falls_back_without_jsonschema(monkeypatch) -> None:
    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "jsonschema":
            raise ImportError("missing for test")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    issues = validate_result_payload({"status": "done"}, {"type": "object"})

    assert issues


def test_policy_guardrail_pre_tool_use_enforces_policy_and_environment() -> None:
    strict = load_policy("policies/strict.json")
    decision = evaluate_policy_guardrail(
        strict,
        "pre_tool_use",
        context={
            "requires_opt_in": True,
            "network_access": True,
            "allow_dangerous_eval": True,
            "allow_network_tasks": False,
        },
    )
    assert decision["allowed"] is False
    assert decision["violations"] == ["tool_use.network_access"]
    assert decision["effective_limits"]["allow_network_tasks_effective"] is False

    offline = load_policy("policies/offline.json")
    decision = evaluate_policy_guardrail(
        offline,
        "pre_tool_use",
        context={
            "requires_opt_in": False,
            "network_access": True,
            "allow_dangerous_eval": True,
            "allow_network_tasks": True,
        },
    )
    assert decision["allowed"] is False
    assert decision["violations"] == ["tool_use.network_access"]
