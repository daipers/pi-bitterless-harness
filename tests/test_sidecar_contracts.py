from __future__ import annotations

import json
import pathlib

import jsonschema


def load_json(path: pathlib.Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_sidecar_contract_schemas_are_valid_json_schema() -> None:
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    for rel_path in [
        "starter/contracts/context-manifest-v1.schema.json",
        "starter/contracts/benchmark-report-v1.schema.json",
        "starter/contracts/release-gate-v1.schema.json",
        "starter/contracts/trajectory-record-v1.schema.json",
        "starter/contracts/retrieval-example-v1.schema.json",
        "starter/contracts/retrieval-document-v1.schema.json",
        "starter/contracts/policy-example-v1.schema.json",
        "starter/contracts/model-example-v1.schema.json",
        "starter/contracts/candidate-manifest-v1.schema.json",
        "starter/contracts/candidate-report-v1.schema.json",
        "starter/contracts/run-event-v1.schema.json",
        "starter/contracts/runtime-governance-v1.schema.json",
    ]:
        schema = load_json(repo_root / rel_path)
        jsonschema.Draft202012Validator.check_schema(schema)
