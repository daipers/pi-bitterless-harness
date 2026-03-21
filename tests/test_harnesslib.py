from __future__ import annotations

import json
import pathlib

from harnesslib import (
    RESULT_INTERFACE_VERSION,
    analyze_eval_command,
    canonicalize_json_file,
    compute_dependencies_hash,
    default_run_contract,
    env_flag,
    make_result_template,
    scan_paths_for_secrets,
    scan_text_for_secrets,
    validate_result_payload,
    validate_run_contract,
)


def test_run_contract_and_template_helpers(tmp_path: pathlib.Path) -> None:
    contract = default_run_contract()
    assert validate_run_contract(contract) == []
    assert len(make_result_template()["artifacts"]) == 1
    assert compute_dependencies_hash({"python": "3.12"})

    broken = dict(contract)
    broken["result_schema_path"] = "wrong.json"
    assert validate_run_contract(broken)
    assert validate_run_contract({"run_contract_version": "broken", "eval_policy": []})

    template = make_result_template()
    path = tmp_path / "result.json"
    path.write_text(json.dumps(template, indent=4), encoding="utf-8")
    canonical = canonicalize_json_file(path)
    assert canonical["x-interface-version"] == RESULT_INTERFACE_VERSION
    assert path.read_text(encoding="utf-8").endswith("\n")
    assert scan_paths_for_secrets([tmp_path / "missing.txt"]) == []


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
    secret_file.write_text("OPENAI_API_KEY=sk-abcdefghijklmnopqrstuvwxyz1234", encoding="utf-8")
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


def test_analyze_eval_command_flags_network_and_metacharacters() -> None:
    detail = analyze_eval_command("curl https://example.com")
    assert detail["requires_opt_in"] is True
    assert detail["network_access"] is True

    detail = analyze_eval_command("python3 script.py && echo done")
    assert detail["requires_opt_in"] is True
    empty = analyze_eval_command("")
    assert empty["requires_opt_in"] is True
    broken = analyze_eval_command("'")
    assert broken["requires_opt_in"] is True
