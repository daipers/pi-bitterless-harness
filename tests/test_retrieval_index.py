from __future__ import annotations

import json
import pathlib

from harnesslib import default_run_contract
from harnesslib import parse_task_file
from retrieval_index import build_index_entry, build_query, score_index_entry


def write_capability_run(run_dir: pathlib.Path, *, task_body: str) -> None:
    (run_dir / "outputs").mkdir(parents=True, exist_ok=True)
    (run_dir / "score").mkdir(exist_ok=True)
    (run_dir / "home").mkdir(exist_ok=True)
    (run_dir / "session").mkdir(exist_ok=True)
    (run_dir / "task.md").write_text(task_body, encoding="utf-8")
    (run_dir / "RUN.md").write_text("# Run\n", encoding="utf-8")
    (run_dir / "result.template.json").write_text("{}", encoding="utf-8")
    (run_dir / "run.contract.json").write_text(
        json.dumps(default_run_contract(version="v2", execution_profile="capability"), indent=2)
        + "\n",
        encoding="utf-8",
    )


def write_result_payload(
    run_dir: pathlib.Path,
    *,
    summary: str,
    claims: list[dict[str, object]],
    artifacts: list[dict[str, str]],
    artifact_contents: dict[str, str],
) -> None:
    for rel_path, contents in artifact_contents.items():
        artifact_path = run_dir / rel_path
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(contents, encoding="utf-8")
    (run_dir / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (run_dir / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": summary,
                "artifacts": artifacts,
                "claims": claims,
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )


def test_score_index_entry_quality_prior_prefers_concrete_summary_and_evidence(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Improve harness retrieval

## Goal
Recover claim-rich retrieval context for comet-lattice scoring.

## Constraints
- claimfocus

## Done
- claimready

## Eval
```bash
python3 ../tests/fixtures/pass_eval.py
```

## Required Artifacts
- result.json
- outputs/run_manifest.json

## Result JSON schema (source of truth)
```json
{schema_text.strip()}
```
"""

    current_run = starter / "runs" / "20260322-090000-current"
    write_capability_run(current_run, task_body=task_body)
    (current_run / "result.schema.json").write_text(schema_text, encoding="utf-8")

    generic_run = starter / "runs" / "20260322-090001-generic"
    write_capability_run(generic_run, task_body=task_body)
    (generic_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    write_result_payload(
        generic_run,
        summary="retrieval success",
        claims=[{"claim": "comet-lattice scoring succeeded", "evidence": []}],
        artifacts=[{"path": "outputs/generic.txt", "description": "generic note"}],
        artifact_contents={"outputs/generic.txt": "generic note\n"},
    )

    concrete_run = starter / "runs" / "20260322-090002-concrete"
    write_capability_run(concrete_run, task_body=task_body)
    (concrete_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    write_result_payload(
        concrete_run,
        summary=(
            "Recovered comet-lattice scoring context and preserved evidence in "
            "outputs/comet-proof.txt for later retrieval."
        ),
        claims=[
            {
                "claim": "Comet-lattice scoring evidence was preserved for later retrieval.",
                "evidence": ["outputs/comet-proof.txt"],
            }
        ],
        artifacts=[
            {
                "path": "outputs/comet-proof.txt",
                "description": "Comet-lattice proof artifact containing the preserved scoring evidence.",
            }
        ],
        artifact_contents={"outputs/comet-proof.txt": "comet-lattice proof\n"},
    )

    parsed_task = parse_task_file(current_run / "task.md")
    query = build_query(parsed_task)
    generic_entry = build_index_entry(generic_run)
    concrete_entry = build_index_entry(concrete_run)
    generic_score = score_index_entry(query, generic_entry)
    concrete_score = score_index_entry(query, concrete_entry)

    assert generic_score["score_breakdown"]["quality_prior"] == 0
    assert concrete_score["score_breakdown"]["quality_prior"] == 4
    assert concrete_score["total_score"] > generic_score["total_score"]
    assert concrete_entry["retrieval_profile_id"] == "retrieval-v4-default"
    assert concrete_entry["retrieval_view"]["artifact_records"][0]["excerpt"] == "comet-lattice proof\n"
    assert "outputs/comet-proof.txt" in concrete_entry["retrieval_view"]["text"]
