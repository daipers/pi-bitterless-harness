from __future__ import annotations

import json
import pathlib

import jsonschema
import orchestrator
import run_task
import score_run
from harnesslib import DEFAULT_GUARDRAIL_HOOKS, EXECUTION_PROFILES, load_governance_registry


def load_json(path: pathlib.Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_runtime_governance_schema_is_valid_json_schema() -> None:
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    schema = load_json(repo_root / "starter" / "contracts" / "runtime-governance-v1.schema.json")
    jsonschema.Draft202012Validator.check_schema(schema)


def test_runtime_governance_registry_validates_and_matches_runtime() -> None:
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    schema = load_json(repo_root / "starter" / "contracts" / "runtime-governance-v1.schema.json")
    registry = load_governance_registry(repo_root / "starter")

    jsonschema.validate(registry, schema)

    assert set(registry["execution_profiles"]) == set(EXECUTION_PROFILES)
    assert set(registry["guardrail_hooks"]) == set(DEFAULT_GUARDRAIL_HOOKS)
    assert registry["primary_error_code_precedence"] == list(run_task.PRIMARY_ERROR_CODE_PRECEDENCE)

    runtime_failure_codes = (
        set(run_task.PRIMARY_ERROR_CODE_PRECEDENCE)
        | set(score_run.SCORE_FAILURE_CLASSIFICATIONS)
        | set(orchestrator.ORCHESTRATOR_FAILURE_CLASSIFICATIONS)
    )
    assert set(registry["failure_taxonomy"]) == runtime_failure_codes
