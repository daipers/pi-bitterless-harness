from __future__ import annotations

import json
import os
import pathlib
import shutil
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
    assert manifest["index_version"] == "retrieval-v2"
    assert manifest["index_mode"] == "cold_build"
    assert manifest["selection_strategy"] == "hybrid_v1"
    assert manifest["candidate_run_count"] == 2
    assert manifest["eligible_run_count"] == 1
    assert manifest["selected_count"] == 1
    assert manifest["query_token_count"] > 0
    assert manifest["ranking_latency_ms"] >= 0
    assert manifest["refreshed_run_count"] == 2
    assert manifest["evicted_run_count"] == 0
    assert manifest["artifact_bytes_copied"] > 0
    assert "20260320-000000-good" in manifest["selected_source_run_ids"]
    assert "20260319-000000-bad" not in manifest["selected_source_run_ids"]
    assert manifest["skipped_sources_count"] >= 1
    assert manifest["top_candidates"][0]["run_id"] == "20260320-000000-good"
    assert manifest["top_candidates"][0]["selected"] is True
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
    copied_files = manifest["selected_sources"][0]["copied_files"]
    assert copied_files[0]["copy_reason"] == "core_run_file"
    assert copied_files[-1]["copy_reason"] == "claim_evidence"
    assert (
        current_run.parent / ".index" / "retrieval-v2" / "20260320-000000-good.json"
    ).exists()


def test_prepare_context_reuses_refreshes_and_evicts_index_entries(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Improve harness retrieval

## Goal
Produce a passing score for harness retrieval with nebula-vector retrieval anchor.

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
    (good_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (good_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "retrieval scoring success",
                "artifacts": [{"path": "outputs/text.txt", "description": "safe text artifact"}],
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

    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    command = [
        sys.executable,
        str(starter / "bin" / "prepare-context.py"),
        str(current_run),
        "policies/capability.json",
    ]

    subprocess.run(
        command,
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    warm_completed = subprocess.run(
        command,
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    warm_manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    assert warm_completed.returncode == 0
    assert warm_manifest["index_mode"] == "warm_reuse"
    assert warm_manifest["refreshed_run_count"] == 0
    assert warm_manifest["evicted_run_count"] == 0

    refreshed_payload = json.loads((good_run / "result.json").read_text(encoding="utf-8"))
    refreshed_payload["summary"] = "retrieval scoring success updated"
    (good_run / "result.json").write_text(
        json.dumps(refreshed_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    subprocess.run(
        command,
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    refreshed_manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    assert refreshed_manifest["index_mode"] == "incremental_refresh"
    assert refreshed_manifest["refreshed_run_count"] == 1
    assert refreshed_manifest["evicted_run_count"] == 0
    assert (
        refreshed_manifest["selected_sources"][0]["summary"]
        == "retrieval scoring success updated"
    )

    shutil.rmtree(good_run)
    subprocess.run(
        command,
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    evicted_manifest = json.loads(
        (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    assert evicted_manifest["index_mode"] == "incremental_refresh"
    assert evicted_manifest["evicted_run_count"] == 1
    assert evicted_manifest["selected_source_run_ids"] == []


def test_rebuild_retrieval_index_rebuilds_cache_without_touching_runs(
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

    good_run = starter / "runs" / "20260320-000000-good"
    write_capability_run(good_run, task_body=task_body)
    (good_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (good_run / "outputs" / "text.txt").write_text("retrieval proof\n", encoding="utf-8")
    (good_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (good_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "retrieval scoring success",
                "artifacts": [{"path": "outputs/text.txt", "description": "safe text artifact"}],
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

    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    completed = subprocess.run(
        [sys.executable, str(starter / "bin" / "rebuild_retrieval_index.py")],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    payload = json.loads(completed.stdout)
    assert payload["index_mode"] == "cold_build"
    assert payload["candidate_run_count"] == 1
    assert payload["eligible_run_count"] == 1
    assert (starter / "runs" / ".index" / "retrieval-v2" / "20260320-000000-good.json").exists()


def test_prepare_context_prefers_exact_phrase_claim_evidence_and_evidence_first_copy(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Improve harness retrieval

## Goal
Produce a passing score for harness retrieval with nebula-vector retrieval anchor.

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

    confuser_run = starter / "runs" / "20260320-000000-confuser"
    write_capability_run(confuser_run, task_body=task_body)
    (confuser_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (confuser_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (confuser_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "Produce a passing score for harness retrieval quickly",
                "artifacts": [],
                "claims": [],
                "remaining_risks": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (confuser_run / "score.json").write_text(
        json.dumps({"overall_pass": True, "failure_classifications": []}, indent=2) + "\n",
        encoding="utf-8",
    )

    good_run = starter / "runs" / "20260320-000001-good"
    write_capability_run(good_run, task_body=task_body)
    (good_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (good_run / "outputs" / "anchor.txt").write_text(
        "nebula-vector retrieval anchor proof\n",
        encoding="utf-8",
    )
    (good_run / "outputs" / "generic.txt").write_text("generic helper text\n", encoding="utf-8")
    (good_run / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (good_run / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "retrieval scoring success",
                "artifacts": [
                    {
                        "path": "outputs/generic.txt",
                        "description": "generic retrieval notes",
                    },
                    {
                        "path": "outputs/anchor.txt",
                        "description": "nebula-vector retrieval anchor evidence",
                    },
                ],
                "claims": [
                    {
                        "claim": "nebula-vector retrieval anchor proof was preserved",
                        "evidence": ["outputs/anchor.txt"],
                    }
                ],
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

    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    subprocess.run(
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
    assert manifest["selected_source_run_ids"][0] == "20260320-000001-good"
    assert manifest["top_candidates"][0]["run_id"] == "20260320-000001-good"
    assert manifest["top_candidates"][0]["score_breakdown"]["phrase_bonus"] >= 6
    copied_files = manifest["selected_sources"][0]["copied_files"]
    artifact_copy_reasons = [entry["copy_reason"] for entry in copied_files if entry["source_path"].startswith("outputs/")]
    assert artifact_copy_reasons[0] == "claim_evidence"
