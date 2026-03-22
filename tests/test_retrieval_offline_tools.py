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


def test_mine_harder_retrieval_benchmarks_writes_review_queue_proposals(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    corpus_path = starter / "benchmarks" / "retrieval_corpus.json"
    corpus_before = corpus_path.read_text(encoding="utf-8")
    schema_text = (starter / "result.schema.json").read_text(encoding="utf-8")
    task_body = f"""# Task
Recover delta forge claim evidence

## Goal
Recover delta-forge claim evidence for shard ledger scoring.

## Constraints
- deltafocus

## Done
- deltaready

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

    confuser_run = starter / "runs" / "20260325-000000-confuser"
    write_capability_run(confuser_run, task_body=task_body)
    (confuser_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    write_result_payload(
        confuser_run,
        summary="Recovered delta-forge notes for shard ledger scoring without durable proof.",
        claims=[{"claim": "Delta-forge scoring completed.", "evidence": []}],
        artifacts=[{"path": "outputs/delta.txt", "description": "generic shard ledger note"}],
        artifact_contents={"outputs/delta.txt": "generic shard ledger note\n"},
    )

    gold_run = starter / "runs" / "20260325-000001-gold"
    write_capability_run(gold_run, task_body=task_body)
    (gold_run / "result.schema.json").write_text(schema_text, encoding="utf-8")
    write_result_payload(
        gold_run,
        summary="Recovered delta-forge claim evidence and linked it to outputs/delta-proof.txt.",
        claims=[
            {
                "claim": "Delta-forge claim evidence was preserved for shard ledger scoring.",
                "evidence": ["outputs/delta-proof.txt"],
            }
        ],
        artifacts=[
            {
                "path": "outputs/delta-proof.txt",
                "description": "Delta-forge proof artifact demonstrating preserved shard-ledger evidence.",
            }
        ],
        artifact_contents={"outputs/delta-proof.txt": "delta-forge preserved claim evidence\n"},
    )

    completed = subprocess.run(
        [sys.executable, str(starter / "bin" / "mine_harder_retrieval_benchmarks.py")],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ | {"PYTHONPATH": str(starter / "bin")},
    )

    payload = json.loads(completed.stdout)
    assert payload["proposal_count"] >= 1
    assert corpus_path.read_text(encoding="utf-8") == corpus_before
    review_queue = starter / "benchmarks" / "review_queue"
    proposal_paths = sorted(review_queue.glob("*.json"))
    assert proposal_paths
    proposal = json.loads(proposal_paths[0].read_text(encoding="utf-8"))
    assert proposal["case_kind"] in {"same_words_wrong_artifact", "same_claim_weaker_evidence"}
    assert proposal["expected_top_1_run_id"] == "20260325-000001-gold"


def test_sweep_retrieval_profiles_evaluates_candidates_and_writes_best_profile(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    best_profile_path = starter / "runs" / "best-profile.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "sweep_retrieval_profiles.py"),
            "--harness-root",
            str(starter),
            "--limit",
            "4",
            "--write-best",
            str(best_profile_path),
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ | {"PYTHONPATH": str(starter / "bin")},
    )

    payload = json.loads(completed.stdout)
    assert payload["candidate_count"] == 4
    assert payload["best_profile"]["profile_id"]
    assert payload["best_metrics"]["hard_negative_win_rate"] >= 0
    assert best_profile_path.exists()
    best_profile = json.loads(best_profile_path.read_text(encoding="utf-8"))
    assert best_profile["profile_id"] == payload["best_profile"]["profile_id"]
