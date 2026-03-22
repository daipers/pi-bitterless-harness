from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys

from harnesslib import default_run_contract


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


def test_prepare_context_selects_successful_runs_and_copies_only_safe_text_artifacts(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Improve harness retrieval

## Goal
Produce a passing score for harness retrieval.

## Constraints
- Stay local.

## Done
- Score is written.

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

    current_run = starter / "runs" / "20260321-000000-current"
    write_capability_run(current_run, task_body=task_body)
    (current_run / "result.schema.json").write_text(schema_text, encoding="utf-8")

    good_run = starter / "runs" / "20260320-000000-good"
    write_capability_run(good_run, task_body=task_body)
    (good_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (good_run / "outputs" / "text.txt").write_text("retrieval proof\n", encoding="utf-8")
    (good_run / "outputs" / "big.txt").write_text("x" * 70000, encoding="utf-8")
    (good_run / "outputs" / "binary.bin").write_bytes(b"\xff\x00\xff")
    outside_path = good_run.parent / "outside.txt"
    outside_path.write_text("outside\n", encoding="utf-8")
    (good_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "retrieval scoring success",
                "artifacts": [
                    {"path": "outputs/text.txt", "description": "safe text artifact"},
                    {"path": "../outside.txt", "description": "out of scope"},
                    {"path": "outputs/big.txt", "description": "too large"},
                    {"path": "outputs/binary.bin", "description": "binary"},
                    {"path": "outputs/missing.txt", "description": "missing"},
                ],
                "claims": [{"claim": "retrieval helped", "evidence": ["outputs/text.txt"]}],
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (good_run / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )
    (good_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )

    bad_run = starter / "runs" / "20260319-000000-bad"
    write_capability_run(bad_run, task_body=task_body)
    (bad_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (bad_run / "result.json").write_text("{broken\n", encoding="utf-8")
    (bad_run / "score.json").write_text(
        json.dumps({"overall_pass": False, "failure_classifications": ["eval_failed"]}, indent=2)
        + "\n",
        encoding="utf-8",
    )

    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    completed = subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "prepare-context.py"),
            str(current_run),
            "policies/capability.json",
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    summary = (current_run / "context" / "retrieval-summary.md").read_text(encoding="utf-8")

    assert str(current_run / "context" / "retrieval-manifest.json") in completed.stdout
    assert "20260320-000000-good" in manifest["selected_source_run_ids"]
    assert "20260319-000000-bad" not in manifest["selected_source_run_ids"]
    assert manifest["skipped_sources_count"] >= 1
    assert "20260320-000000-good" in summary
    selected_run_dir = current_run / "context" / "source-runs" / "20260320-000000-good"
    assert (selected_run_dir / "task.md").exists()
    assert (selected_run_dir / "result.json").exists()
    assert (selected_run_dir / "score.json").exists()
    assert (
        selected_run_dir / "outputs" / "text.txt"
    ).exists()
    assert not (selected_run_dir / "outputs" / "big.txt").exists()
    assert not (selected_run_dir / "outputs" / "binary.bin").exists()
