from __future__ import annotations

import json
import os
import pathlib
import subprocess

import pytest

RUN_REAL_PI = os.environ.get("HARNESS_RUN_REAL_PI_TESTS") == "1"
pytestmark = pytest.mark.skipif(
    not RUN_REAL_PI,
    reason=(
        "set HARNESS_RUN_REAL_PI_TESTS=1 and HARNESS_PI_AUTH_JSON "
        "to run against the real pi CLI"
    ),
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
STARTER_ROOT = REPO_ROOT / "starter"
BIN_DIR = STARTER_ROOT / "bin"
SCHEMA_TEXT = (STARTER_ROOT / "result.schema.json").read_text(encoding="utf-8").rstrip()


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


def write_task(run_dir: pathlib.Path, label: str) -> None:
    starter_root = run_dir.parents[1]
    relative_claim = f"{run_dir.relative_to(starter_root).as_posix()}/outputs/claim.txt"
    task_text = f"""# Task
Real pi integration: {label}

## Goal
Create the canary artifact and report success through result.json.

## Constraints
- Stay inside `{run_dir}`.
- Create `outputs/claim.txt` with the exact text `canary ok`.
- Write `result.json` as raw JSON only.

## Done
- `outputs/claim.txt` contains `canary ok`.
- `result.json` is valid.
- `outputs/run_manifest.json` exists.

## Eval
```bash
python3 ../tests/fixtures/check_claim.py {relative_claim} "canary ok"
```

## Required Artifacts
- result.json
- outputs/claim.txt
- outputs/run_manifest.json

## Result JSON schema (source of truth)

```json
{SCHEMA_TEXT}
```
"""
    (run_dir / "task.md").write_text(task_text, encoding="utf-8")


def run_harness(
    isolated_repo: pathlib.Path,
    run_dir: pathlib.Path,
    *,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ | {
        "PYTHONPATH": str(isolated_repo / "starter" / "bin"),
        "HARNESS_PI_AUTH_JSON": os.environ["HARNESS_PI_AUTH_JSON"],
    }
    if extra_env:
        env |= extra_env
    command = [str(isolated_repo / "starter" / "bin" / "run-task.sh"), str(run_dir)]
    model = os.environ.get("HARNESS_REAL_PI_MODEL", "")
    if model:
        command.append(model)
    return subprocess.run(
        command,
        cwd=isolated_repo / "starter",
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def test_real_pi_happy_path_records_manifest(isolated_repo: pathlib.Path) -> None:
    run_dir = create_run(isolated_repo, "real pi success")
    write_task(run_dir, "success")

    completed = run_harness(isolated_repo, run_dir)

    assert completed.returncode == 0
    manifest = json.loads((run_dir / "outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    assert manifest["state"] == "complete"
    assert score["overall_pass"] is True


def test_real_pi_proxy_corrupt_result_is_scored_as_failure(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = create_run(isolated_repo, "real pi corrupt result")
    write_task(run_dir, "forced invalid result")

    completed = run_harness(
        isolated_repo,
        run_dir,
        extra_env={
            "HARNESS_PI_BIN": str(isolated_repo / "starter" / "bin" / "real_pi_proxy.py"),
            "HARNESS_REAL_PI_BIN": "pi",
            "HARNESS_REAL_PI_PROXY_MODE": "corrupt-result",
        },
    )

    assert completed.returncode == 0
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    assert "result_invalid" in score["failure_classifications"]
