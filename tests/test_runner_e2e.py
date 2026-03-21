from __future__ import annotations

import json
import os
import pathlib
import subprocess


def create_run(isolated_repo: pathlib.Path, title: str) -> pathlib.Path:
    env = os.environ | {"PYTHONPATH": str(isolated_repo / "starter" / "bin")}
    completed = subprocess.run(
        [str(isolated_repo / "starter" / "bin" / "new-task.sh"), title],
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
) -> subprocess.CompletedProcess[str]:
    env = os.environ | {
        "PYTHONPATH": str(isolated_repo / "starter" / "bin"),
        "HARNESS_PI_BIN": str(isolated_repo / "tests" / "fixtures" / "fake_pi.py"),
        "FAKE_PI_SCENARIO": scenario,
    }
    if extra_env:
        env |= extra_env
    return subprocess.run(
        [str(isolated_repo / "starter" / "bin" / "run-task.sh"), str(run_dir)],
        cwd=isolated_repo / "starter",
        capture_output=True,
        text=True,
        env=env,
    )


def test_runner_happy_path_records_manifest(isolated_repo: pathlib.Path) -> None:
    run_dir = create_run(isolated_repo, "happy path")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")

    completed = run_harness(isolated_repo, run_dir, "happy_path")

    assert completed.returncode == 0
    manifest = json.loads((run_dir / "outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    assert manifest["state"] == "complete"
    assert score["overall_pass"] is True


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
