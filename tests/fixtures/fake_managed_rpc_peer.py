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


def load_scenario(run_dir: pathlib.Path) -> dict[str, object]:
    payload = {"scenario": os.environ.get("FAKE_MANAGED_RPC_SCENARIO", "happy_path")}
    scenario_path = run_dir / ".fake-managed-rpc-scenario.json"
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


def read_decision() -> dict[str, object]:
    raw_line = sys.stdin.readline()
    if not raw_line:
        return {}
    try:
        payload = json.loads(raw_line)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def emit_request(request_id: str, payload: dict[str, object]) -> dict[str, object]:
    print(json.dumps({"type": "intercept_request", "request_id": request_id, "payload": payload}))
    sys.stdout.flush()
    return read_decision()


def default_requests() -> list[dict[str, object]]:
    return [
        {
            "request_id": "spawn-1",
            "payload": {
                "action": "spawn",
                "agent_id": "reader-1",
                "profile_id": "focused_reader",
                "prompt_tokens": 120,
            },
        },
        {
            "request_id": "tool-1",
            "payload": {
                "action": "tool",
                "agent_id": "reader-1",
                "profile_id": "focused_reader",
                "tool": "read",
                "read_paths": ["starter/README.md"],
                "write_paths": [],
                "network_access": False,
                "runtime_seconds": 2,
            },
        },
    ]


def main() -> int:
    if "--version" in sys.argv:
        print("fake-managed-rpc-peer 1.0.0")
        return 0
    if "--managed-rpc-probe" in sys.argv:
        print(json.dumps({"managed_rpc": True, "protocol": "jsonl_v1"}))
        return 0

    run_dir = find_task_path(sys.argv[1:]).parent
    scenario = load_scenario(run_dir)
    requests = scenario.get("requests")
    request_payloads = requests if isinstance(requests, list) else default_requests()

    print(json.dumps({"event": "session.started", "transport": "managed_rpc"}))
    sys.stdout.flush()

    for index, item in enumerate(request_payloads, start=1):
        if not isinstance(item, dict):
            continue
        request_id = str(item.get("request_id") or f"req-{index}")
        payload = dict(item.get("payload", {})) if isinstance(item.get("payload"), dict) else {}
        decision = emit_request(request_id, payload)
        if decision.get("allow") is not True:
            print(json.dumps({"event": "session.denied", "request_id": request_id}))
            sys.stdout.flush()
            return 42

    (run_dir / "outputs").mkdir(parents=True, exist_ok=True)
    (run_dir / "outputs" / "claim.txt").write_text("claim\n", encoding="utf-8")
    write_result(
        run_dir,
        {
            "x-interface-version": "v1",
            "status": "success",
            "summary": "managed rpc peer completed the task",
            "artifacts": [{"path": "outputs/claim.txt", "description": "managed rpc proof"}],
            "claims": [{"claim": "managed rpc task completed", "evidence": ["outputs/claim.txt"]}],
            "remaining_risks": [],
        },
    )
    print(json.dumps({"event": "session.completed", "transport": "managed_rpc"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
