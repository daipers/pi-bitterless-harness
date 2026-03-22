#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import pathlib
import signal
import subprocess
import time
from typing import Any

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
STARTER_ROOT = REPO_ROOT / "starter"
BIN_DIR = STARTER_ROOT / "bin"
SCHEMA_TEXT = (STARTER_ROOT / "result.schema.json").read_text(encoding="utf-8").rstrip()
CHECK_CLAIM = "../tests/fixtures/check_claim.py"
REAL_PI_PROXY = BIN_DIR / "real_pi_proxy.py"


def base_env() -> dict[str, str]:
    env = os.environ.copy()
    auth_path = env.get("HARNESS_PI_AUTH_JSON")
    if not auth_path:
        raise SystemExit("HARNESS_PI_AUTH_JSON is required for the real pi canary")
    env["PYTHONPATH"] = str(BIN_DIR)
    return env


def create_run(title: str, env: dict[str, str]) -> pathlib.Path:
    completed = subprocess.run(
        [str(BIN_DIR / "new-task.sh"), title],
        cwd=STARTER_ROOT,
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )
    return STARTER_ROOT / completed.stdout.strip()


def write_task(run_dir: pathlib.Path, label: str) -> None:
    relative_claim = f"{run_dir.relative_to(STARTER_ROOT).as_posix()}/outputs/claim.txt"
    payload = f"""# Task
Real pi canary: {label}

## Goal
Create the canary artifact and report success through the standard result contract.

## Constraints
- Stay inside `{run_dir}`.
- Create `outputs/claim.txt` with the exact text `canary ok`.
- Write `result.json` as raw JSON only and make it match the schema below.
- Keep durable artifacts under `outputs/`.

## Done
- `outputs/claim.txt` exists with the exact text `canary ok`.
- `result.json` reports `status: "success"`.
- `outputs/run_manifest.json` exists.

## Eval
```bash
python3 {CHECK_CLAIM} {relative_claim} "canary ok"
```

## Required Artifacts
- result.json
- outputs/claim.txt
- outputs/run_manifest.json

## Result JSON schema (source of truth)

Write `result.json` as raw JSON only. Do not include prose, markdown, or wrapper text.

```json
{SCHEMA_TEXT}
```
"""
    (run_dir / "task.md").write_text(payload, encoding="utf-8")


def run_harness(
    run_dir: pathlib.Path,
    env: dict[str, str],
    *,
    model: str,
) -> subprocess.CompletedProcess[str]:
    command = [str(BIN_DIR / "run-task.sh"), str(run_dir)]
    if model:
        command.append(model)
    return subprocess.run(
        command,
        cwd=STARTER_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def load_json(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def scenario_success(env: dict[str, str], model: str) -> dict[str, Any]:
    run_dir = create_run("real canary success", env)
    write_task(run_dir, "success")
    completed = run_harness(run_dir, env, model=model)
    score = load_json(run_dir / "score.json")
    assert_true(completed.returncode == 0, f"expected success exit, got {completed.returncode}")
    assert_true(score["overall_pass"] is True, "expected overall_pass=true")
    return {"run_dir": str(run_dir), "score": score["overall_error_code"]}


def scenario_corrupt_result(env: dict[str, str], model: str) -> dict[str, Any]:
    run_dir = create_run("real canary corrupt result", env)
    write_task(run_dir, "forced invalid result")
    scenario_env = env | {
        "HARNESS_PI_BIN": str(REAL_PI_PROXY),
        "HARNESS_REAL_PI_BIN": env.get("HARNESS_REAL_PI_BIN", "pi"),
        "HARNESS_REAL_PI_PROXY_MODE": "corrupt-result",
    }
    completed = run_harness(run_dir, scenario_env, model=model)
    score = load_json(run_dir / "score.json")
    assert_true(completed.returncode == 0, f"expected harness exit 0, got {completed.returncode}")
    assert_true("result_invalid" in score["failure_classifications"], "expected result_invalid")
    return {"run_dir": str(run_dir), "score": score["overall_error_code"]}


def scenario_timeout(env: dict[str, str], model: str) -> dict[str, Any]:
    run_dir = create_run("real canary timeout", env)
    write_task(run_dir, "timeout")
    scenario_env = env | {"HARNESS_MODEL_TIMEOUT_SECONDS": "1"}
    completed = run_harness(run_dir, scenario_env, model=model)
    score = load_json(run_dir / "score.json")
    pi_exit = (run_dir / "pi.exit_code.txt").read_text(encoding="utf-8").strip()
    assert_true(completed.returncode == 0, f"expected harness exit 0, got {completed.returncode}")
    assert_true(pi_exit == "124", f"expected pi timeout exit code 124, got {pi_exit}")
    assert_true(
        "model_invocation_failed" in score["failure_classifications"],
        "expected model_invocation_failed",
    )
    return {"run_dir": str(run_dir), "score": score["overall_error_code"], "pi_exit": pi_exit}


def scenario_interrupted(env: dict[str, str], model: str) -> dict[str, Any]:
    run_dir = create_run("real canary interrupted", env)
    write_task(run_dir, "interrupted")
    command = [str(BIN_DIR / "run-task.sh"), str(run_dir)]
    if model:
        command.append(model)
    process = subprocess.Popen(
        command,
        cwd=STARTER_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    deadline = time.time() + 30
    while time.time() < deadline:
        state_path = run_dir / "run.state"
        if state_path.exists() and state_path.read_text(encoding="utf-8").strip() == "running":
            break
        if process.poll() is not None:
            raise RuntimeError(f"interrupt scenario exited before signal with {process.returncode}")
        time.sleep(0.25)
    else:
        process.terminate()
        process.wait(timeout=10)
        raise RuntimeError("interrupt scenario never reached running state")

    process.send_signal(signal.SIGTERM)
    stdout, stderr = process.communicate(timeout=30)
    manifest = load_json(run_dir / "outputs" / "run_manifest.json")
    state = (run_dir / "run.state").read_text(encoding="utf-8").strip()
    assert_true(process.returncode == 130, f"expected signal exit 130, got {process.returncode}")
    assert_true(state == "cancelled", f"expected cancelled state, got {state}")
    assert_true(
        manifest["state"] == "cancelled",
        f"expected cancelled manifest, got {manifest['state']}",
    )
    return {
        "run_dir": str(run_dir),
        "state": state,
        "stdout_tail": stdout.strip().splitlines()[-3:],
        "stderr_tail": stderr.strip().splitlines()[-3:],
    }


def scenario_retry(env: dict[str, str], model: str) -> dict[str, Any]:
    run_dir = create_run("real canary retry", env)
    write_task(run_dir, "retry")
    sentinel = run_dir / ".retry-once"
    scenario_env = env | {
        "HARNESS_PI_BIN": str(REAL_PI_PROXY),
        "HARNESS_REAL_PI_BIN": env.get("HARNESS_REAL_PI_BIN", "pi"),
        "HARNESS_REAL_PI_PROXY_MODE": "startup-fail-once",
        "HARNESS_REAL_PI_PROXY_SENTINEL": str(sentinel),
    }
    completed = run_harness(run_dir, scenario_env, model=model)
    score = load_json(run_dir / "score.json")
    events = (run_dir / "run-events.jsonl").read_text(encoding="utf-8")
    assert_true(completed.returncode == 0, f"expected harness exit 0, got {completed.returncode}")
    assert_true(score["overall_pass"] is True, "expected retry scenario to recover and pass")
    assert_true("retrying pi startup failure" in events, "expected retry event to be recorded")
    return {"run_dir": str(run_dir), "score": score["overall_error_code"]}


def scenario_partial_recovery(env: dict[str, str], model: str) -> dict[str, Any]:
    run_dir = create_run("real canary partial recovery", env)
    write_task(run_dir, "partial recovery")
    lock_dir = run_dir / ".run-lock"
    lock_dir.mkdir(parents=True, exist_ok=True)
    (lock_dir / "pid").write_text("999999\n", encoding="utf-8")
    (run_dir / "transcript.jsonl").write_text('{"event":"partial"}\n', encoding="utf-8")
    (run_dir / "pi.stderr.log").write_text("partial\n", encoding="utf-8")
    (run_dir / "score.json").write_text('{"overall_pass": false}\n', encoding="utf-8")
    (run_dir / "outputs" / "run_manifest.json").write_text(
        '{"state": "partial"}\n',
        encoding="utf-8",
    )

    completed = run_harness(run_dir, env, model=model)
    score = load_json(run_dir / "score.json")
    recovery_root = run_dir / "recovery"
    recovered = sorted(recovery_root.rglob("*"))
    assert_true(completed.returncode == 0, f"expected harness exit 0, got {completed.returncode}")
    assert_true(score["overall_pass"] is True, "expected recovered run to pass")
    assert_true(
        any(path.name == "transcript.jsonl" for path in recovered),
        "expected transcript in recovery archive",
    )
    recovery_entries = [str(path) for path in recovered if path.is_file()]
    return {"run_dir": str(run_dir), "recovery_entries": recovery_entries}


def main() -> int:
    env = base_env()
    model = env.get("HARNESS_REAL_PI_MODEL", "")
    summary_name = f"real-canary-{time.strftime('%Y%m%d-%H%M%S')}.summary.json"
    summary_path = STARTER_ROOT / "runs" / summary_name
    scenarios = [
        ("success", scenario_success),
        ("corrupt_result", scenario_corrupt_result),
        ("timeout", scenario_timeout),
        ("interrupted", scenario_interrupted),
        ("retry", scenario_retry),
        ("partial_recovery", scenario_partial_recovery),
    ]

    results: list[dict[str, Any]] = []
    exit_code = 0
    for name, func in scenarios:
        try:
            details = func(env, model)
        except Exception as exc:
            exit_code = 1
            results.append({"scenario": name, "ok": False, "error": str(exc)})
        else:
            results.append({"scenario": name, "ok": True, **details})

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "supported_pi_version": (REPO_ROOT / "PI_VERSION").read_text(encoding="utf-8").strip(),
        "results": results,
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(summary_path)
    print(json.dumps(summary, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
