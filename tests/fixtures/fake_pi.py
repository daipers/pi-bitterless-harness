#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import pathlib
import sys
import time


def find_task_path(argv: list[str]) -> pathlib.Path:
    for arg in argv:
        if arg.startswith("@"):
            return pathlib.Path(arg[1:]).resolve()
    raise SystemExit("task path not provided")


def load_run_scenario(run_dir: pathlib.Path) -> dict[str, object]:
    payload = {"scenario": os.environ.get("FAKE_PI_SCENARIO", "happy_path")}
    scenario_path = run_dir / ".fake-pi-scenario.json"
    if not scenario_path.exists():
        return payload
    try:
        loaded = json.loads(scenario_path.read_text(encoding="utf-8"))
    except Exception:
        return payload
    if isinstance(loaded, dict):
        payload.update(loaded)
    return payload


def write_result(run_dir: pathlib.Path, payload: dict[str, object]) -> None:
    (run_dir / "result.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_optional_subagent_usage(run_dir: pathlib.Path, scenario_payload: dict[str, object]) -> None:
    usage = scenario_payload.get("subagent_usage")
    if not isinstance(usage, dict):
        return
    (run_dir / "outputs").mkdir(parents=True, exist_ok=True)
    (run_dir / "outputs" / "subagent-usage.json").write_text(
        json.dumps(usage, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    if "--version" in sys.argv:
        print("fake-pi 1.0.0")
        return 0

    run_dir = find_task_path(sys.argv[1:]).parent
    scenario_payload = load_run_scenario(run_dir)
    scenario = str(scenario_payload.get("scenario", "happy_path"))
    sentinel = pathlib.Path(
        str(
            scenario_payload.get(
                "sentinel_path",
                os.environ.get("FAKE_PI_STARTUP_ONCE_SENTINEL", run_dir / ".fake-pi-startup-once"),
            )
        )
    )

    print(json.dumps({"event": "session.started", "scenario": scenario}))
    sys.stdout.flush()

    if scenario == "happy_path":
        (run_dir / "outputs").mkdir(parents=True, exist_ok=True)
        (run_dir / "outputs" / "claim.txt").write_text("claim\n", encoding="utf-8")
        write_optional_subagent_usage(run_dir, scenario_payload)
        write_result(
            run_dir,
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "fake pi completed the task",
                "artifacts": [
                    {
                        "path": "outputs/claim.txt",
                        "description": "happy path proof",
                    }
                ],
                "claims": [
                    {
                        "claim": "the task completed",
                        "evidence": ["outputs/claim.txt"],
                    }
                ],
                "remaining_risks": [],
            },
        )
        print(json.dumps({"event": "session.completed"}))
        return 0

    if scenario == "startup_fail_once":
        if not sentinel.exists():
            sentinel.parent.mkdir(parents=True, exist_ok=True)
            sentinel.write_text("failed-once\n", encoding="utf-8")
            print("startup failed once", file=sys.stderr)
            return 75
        (run_dir / "outputs").mkdir(parents=True, exist_ok=True)
        (run_dir / "outputs" / "claim.txt").write_text("claim\n", encoding="utf-8")
        write_optional_subagent_usage(run_dir, scenario_payload)
        write_result(
            run_dir,
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "fake pi recovered after one startup failure",
                "artifacts": [
                    {
                        "path": "outputs/claim.txt",
                        "description": "retry recovery proof",
                    }
                ],
                "claims": [
                    {
                        "claim": "the task recovered after a retry",
                        "evidence": ["outputs/claim.txt"],
                    }
                ],
                "remaining_risks": [],
            },
        )
        print(json.dumps({"event": "session.completed", "retry_recovered": True}))
        return 0

    if scenario == "invalid_result":
        (run_dir / "result.json").write_text("{not valid json\n", encoding="utf-8")
        print(json.dumps({"event": "session.completed"}))
        return 0

    if scenario == "missing_artifact":
        write_result(
            run_dir,
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "artifact missing",
                "artifacts": [],
                "claims": [],
                "remaining_risks": [],
            },
        )
        print(json.dumps({"event": "session.completed"}))
        return 0

    if scenario == "startup_failure":
        print("startup failed", file=sys.stderr)
        return 75

    if scenario == "auth_failure":
        print("authentication failed: token expired", file=sys.stderr)
        return 77

    if scenario == "partial_transcript_hang":
        print(json.dumps({"event": "session.partial", "status": "waiting"}))
        sys.stdout.flush()
        time.sleep(float(scenario_payload.get("sleep_seconds", 5)))
        return 124

    if scenario == "permission_denied":
        outputs_dir = run_dir / "outputs"
        outputs_dir.mkdir(parents=True, exist_ok=True)
        outputs_dir.chmod(0o500)
        try:
            (outputs_dir / "claim.txt").write_text("claim\n", encoding="utf-8")
        except PermissionError:
            print("permission denied while writing outputs/claim.txt", file=sys.stderr)
            return 13
        finally:
            outputs_dir.chmod(0o700)
        return 13

    if scenario == "transcript_flood":
        event_count = int(scenario_payload.get("event_count", 2048))
        for index in range(event_count):
            print(json.dumps({"event": "session.chunk", "chunk": index, "text": "flood"}))
        sys.stdout.flush()
        (run_dir / "outputs").mkdir(parents=True, exist_ok=True)
        (run_dir / "outputs" / "claim.txt").write_text("claim\n", encoding="utf-8")
        write_optional_subagent_usage(run_dir, scenario_payload)
        write_result(
            run_dir,
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": "fake pi completed after emitting a large transcript",
                "artifacts": [
                    {
                        "path": "outputs/claim.txt",
                        "description": "flood scenario proof",
                    }
                ],
                "claims": [
                    {
                        "claim": "the task completed despite large transcript volume",
                        "evidence": ["outputs/claim.txt"],
                    }
                ],
                "remaining_risks": [],
            },
        )
        print(json.dumps({"event": "session.completed"}))
        return 0

    print(f"unknown scenario: {scenario}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
