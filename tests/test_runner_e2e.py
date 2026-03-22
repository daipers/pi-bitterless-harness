from __future__ import annotations

import json
import os
import pathlib
import subprocess

from harnesslib import default_run_contract


def create_run(
    isolated_repo: pathlib.Path,
    title: str,
    *,
    profile: str | None = None,
) -> pathlib.Path:
    env = os.environ | {"PYTHONPATH": str(isolated_repo / "starter" / "bin")}
    command = [str(isolated_repo / "starter" / "bin" / "new-task.sh")]
    if profile is not None:
        command.extend(["--profile", profile])
    command.append(title)
    completed = subprocess.run(
        command,
        cwd=isolated_repo / "starter",
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return isolated_repo / "starter" / completed.stdout.strip()


def replace_eval_command(task_path: pathlib.Path, command: str) -> None:
    text = task_path.read_text(encoding="utf-8")
    text = text.replace("# python3 -m pytest tests/test_runner_e2e.py -q", command)
    task_path.write_text(text, encoding="utf-8")


def run_harness(
    isolated_repo: pathlib.Path,
    run_dir: pathlib.Path,
    scenario: str,
    extra_env: dict[str, str] | None = None,
    extra_args: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ | {
        "PYTHONPATH": str(isolated_repo / "starter" / "bin"),
        "HARNESS_PI_BIN": str(isolated_repo / "tests" / "fixtures" / "fake_pi.py"),
        "FAKE_PI_SCENARIO": scenario,
    }
    if extra_env:
        env |= extra_env
    command = [str(isolated_repo / "starter" / "bin" / "run-task.sh")]
    if extra_args:
        command.extend(extra_args)
    command.append(str(run_dir))
    return subprocess.run(
        command,
        cwd=isolated_repo / "starter",
        capture_output=True,
        text=True,
        env=env,
    )


def seed_successful_prior_run(isolated_repo: pathlib.Path, run_id: str) -> pathlib.Path:
    run_dir = isolated_repo / "starter" / "runs" / run_id
    unique_phrase = "nebula-vector retrieval anchor"
    (run_dir / "outputs").mkdir(parents=True)
    (run_dir / "score").mkdir()
    (run_dir / "home").mkdir()
    (run_dir / "session").mkdir()
    (run_dir / "task.md").write_text(
        """# Task
Fix harness scoring

## Goal
Produce a passing score for harness scoring with nebula-vector retrieval anchor.

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
{}
```
""",
        encoding="utf-8",
    )
    (run_dir / "RUN.md").write_text("# Run\n", encoding="utf-8")
    schema_text = (isolated_repo / "starter" / "result.schema.json").read_text(encoding="utf-8")
    task_text = (run_dir / "task.md").read_text(encoding="utf-8").replace("{}", schema_text.strip())
    (run_dir / "task.md").write_text(task_text, encoding="utf-8")
    (run_dir / "result.schema.json").write_text(schema_text, encoding="utf-8")
    (run_dir / "result.template.json").write_text("{}", encoding="utf-8")
    (run_dir / "run.contract.json").write_text(
        json.dumps(default_run_contract(version="v2", execution_profile="capability"), indent=2)
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "outputs" / "claim.txt").write_text("prior evidence\n", encoding="utf-8")
    (run_dir / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (run_dir / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": (
                    "prior successful harness scoring fix with nebula-vector retrieval anchor"
                ),
                "artifacts": [{"path": "outputs/claim.txt", "description": "prior proof artifact"}],
                "claims": [
                    {
                        "claim": f"fixed scoring flow for {unique_phrase}",
                        "evidence": ["outputs/claim.txt"],
                    }
                ],
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
    return run_dir


def test_runner_happy_path_records_manifest(isolated_repo: pathlib.Path) -> None:
    run_dir = create_run(isolated_repo, "happy path")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")

    completed = run_harness(isolated_repo, run_dir, "happy_path")

    assert completed.returncode == 0
    manifest = json.loads((run_dir / "outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    assert manifest["state"] == "complete"
    assert manifest["execution"]["profile"] == "strict"
    assert score["overall_pass"] is True
    assert score["execution_profile"] == "strict"


def test_new_task_defaults_to_v2_strict_contract(isolated_repo: pathlib.Path) -> None:
    run_dir = create_run(isolated_repo, "default contract")

    contract = json.loads((run_dir / "run.contract.json").read_text(encoding="utf-8"))

    assert contract["run_contract_version"] == "v2"
    assert contract["execution_profile"] == "strict"


def test_runner_v1_contract_still_executes(isolated_repo: pathlib.Path) -> None:
    run_dir = create_run(isolated_repo, "v1 compatibility")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")
    (run_dir / "run.contract.json").write_text(
        json.dumps(default_run_contract(version="v1"), indent=2) + "\n",
        encoding="utf-8",
    )

    completed = run_harness(isolated_repo, run_dir, "happy_path")

    assert completed.returncode == 0
    manifest = json.loads((run_dir / "outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    assert manifest["execution"]["contract_version"] == "v1"
    assert score["overall_pass"] is True


def test_runner_profile_override_capability_materializes_context(
    isolated_repo: pathlib.Path,
) -> None:
    seed_successful_prior_run(isolated_repo, "20260320-000000-prior-success")
    run_dir = create_run(isolated_repo, "capability override target")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")
    task_text = (run_dir / "task.md").read_text(encoding="utf-8")
    task_text = task_text.replace(
        "Describe the desired outcome in plain language.",
        "Produce a passing score for harness scoring with nebula-vector retrieval anchor.",
    )
    task_text = task_text.replace(
        "- Concrete completion criteria",
        (
            "- Concrete completion criteria\n"
            "- Use the nebula-vector retrieval anchor pattern if helpful"
        ),
    )
    (run_dir / "task.md").write_text(task_text, encoding="utf-8")

    completed = run_harness(
        isolated_repo,
        run_dir,
        "happy_path",
        extra_args=["--profile", "capability"],
    )

    assert completed.returncode == 0
    prompt_text = (run_dir / "prompt.txt").read_text(encoding="utf-8")
    manifest = json.loads((run_dir / "outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    retrieval_manifest = json.loads(
        (run_dir / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
    )
    assert (run_dir / "context" / "retrieval-summary.md").exists()
    assert "Retrieved context:" in prompt_text
    assert "context/retrieval-summary.md" in prompt_text
    assert manifest["execution"]["profile"] == "capability"
    assert manifest["context"]["enabled"] is True
    assert manifest["context"]["bootstrap_mode"] == "cold_build"
    assert manifest["context"]["candidate_run_count"] >= 1
    assert manifest["context"]["eligible_run_count"] >= 1
    assert manifest["context"]["selected_count"] >= 1
    assert manifest["context"]["ranking_latency_ms"] >= 0
    assert manifest["context"]["artifact_bytes_copied"] >= 0
    assert score["execution_profile"] == "capability"
    assert score["retrieval"]["enabled"] is True
    assert score["retrieval"]["index_mode"] == "cold_build"
    assert score["retrieval"]["candidate_run_count"] >= 1
    assert score["retrieval"]["eligible_run_count"] >= 1
    assert score["retrieval"]["selected_count"] >= 1
    assert score["retrieval"]["ranking_latency_ms"] >= 0
    assert score["retrieval"]["artifact_bytes_copied"] >= 0
    assert "20260320-000000-prior-success" in retrieval_manifest["selected_source_run_ids"]


def test_runner_contract_failure_blocks_launch(isolated_repo: pathlib.Path) -> None:
    run_dir = create_run(isolated_repo, "bad contract")
    task_path = run_dir / "task.md"
    task_path.write_text(
        task_path.read_text(encoding="utf-8").replace("## Eval", "## Nope"),
        encoding="utf-8",
    )

    completed = run_harness(isolated_repo, run_dir, "happy_path")

    assert completed.returncode != 0
    assert "contract check failed" in completed.stderr


def test_runner_invalid_result_is_scored_as_failure(isolated_repo: pathlib.Path) -> None:
    run_dir = create_run(isolated_repo, "invalid result")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")

    completed = run_harness(isolated_repo, run_dir, "invalid_result")

    assert completed.returncode == 0
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    assert "result_invalid" in score["failure_classifications"]


def test_runner_missing_artifact_is_scored_as_failure(isolated_repo: pathlib.Path) -> None:
    run_dir = create_run(isolated_repo, "missing artifact")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")
    task_text = (run_dir / "task.md").read_text(encoding="utf-8")
    task_text = task_text.replace(
        "- outputs/run_manifest.json",
        "- outputs/run_manifest.json\n- outputs/claim.txt",
    )
    (run_dir / "task.md").write_text(task_text, encoding="utf-8")

    completed = run_harness(isolated_repo, run_dir, "missing_artifact")

    assert completed.returncode == 0
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    assert "eval_failed" in score["failure_classifications"]


def test_runner_eval_failure_is_scored_as_failure(isolated_repo: pathlib.Path) -> None:
    run_dir = create_run(isolated_repo, "eval failure")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/fail_eval.py")

    completed = run_harness(isolated_repo, run_dir, "happy_path")

    assert completed.returncode == 0
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    assert "eval_failed" in score["failure_classifications"]


def test_runner_wrapper_eval_is_blocked_by_contract(isolated_repo: pathlib.Path) -> None:
    run_dir = create_run(isolated_repo, "wrapper eval blocked")
    replace_eval_command(run_dir / "task.md", "python3 -c 'print(1)'")

    completed = run_harness(isolated_repo, run_dir, "happy_path")

    assert completed.returncode != 0
    assert "HARNESS_ALLOW_DANGEROUS_EVAL=1" in completed.stderr


def test_runner_out_of_scope_artifact_is_blocked_by_contract(isolated_repo: pathlib.Path) -> None:
    run_dir = create_run(isolated_repo, "artifact escape blocked")
    task_text = (run_dir / "task.md").read_text(encoding="utf-8")
    task_text = task_text.replace(
        "- outputs/run_manifest.json",
        "- outputs/run_manifest.json\n- ../outside.txt",
    )
    (run_dir / "task.md").write_text(task_text, encoding="utf-8")

    completed = run_harness(isolated_repo, run_dir, "happy_path")

    assert completed.returncode != 0
    assert "required artifact path resolves outside the run directory" in completed.stderr
