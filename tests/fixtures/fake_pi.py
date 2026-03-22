#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import pathlib
import sys


def find_task_path(argv: list[str]) -> pathlib.Path:
    for arg in argv:
        if arg.startswith("@"):
            return pathlib.Path(arg[1:]).resolve()
    raise SystemExit("task path not provided")


def write_result(run_dir: pathlib.Path, payload: dict[str, object]) -> None:
    (run_dir / "result.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    if "--version" in sys.argv:
        print("fake-pi 1.0.0")
        return 0

    run_dir = find_task_path(sys.argv[1:]).parent
    scenario = os.environ.get("FAKE_PI_SCENARIO", "happy_path")

    print(json.dumps({"event": "session.started", "scenario": scenario}))

    if scenario == "happy_path":
        (run_dir / "outputs").mkdir(parents=True, exist_ok=True)
        (run_dir / "outputs" / "claim.txt").write_text("claim\n", encoding="utf-8")
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

    print(f"unknown scenario: {scenario}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
