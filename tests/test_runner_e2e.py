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


def write_fake_pi_scenario(run_dir: pathlib.Path, payload: dict[str, object]) -> None:
    (run_dir / ".fake-pi-scenario.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )


def write_fake_managed_rpc_scenario(run_dir: pathlib.Path, payload: dict[str, object]) -> None:
    (run_dir / ".fake-managed-rpc-scenario.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )


def test_runner_happy_path_records_manifest(isolated_repo: pathlib.Path) -> None:
    run_dir = create_run(isolated_repo, "happy path")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")

    completed = run_harness(isolated_repo, run_dir, "happy_path")

    assert completed.returncode == 0
    manifest = json.loads((run_dir / "outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    assert manifest["state"] == "complete"
    assert manifest["trace_id"] == run_dir.name
    assert manifest["execution"]["profile"] == "strict"
    assert manifest["primary_error_code"] is None
    assert manifest["failure_classifications"] == []
    assert score["overall_pass"] is True
    assert score["execution_profile"] == "strict"


def test_new_task_defaults_to_v2_strict_contract(isolated_repo: pathlib.Path) -> None:
    run_dir = create_run(isolated_repo, "default contract")

    contract = json.loads((run_dir / "run.contract.json").read_text(encoding="utf-8"))

    assert contract["run_contract_version"] == "v2"
    assert contract["execution_profile"] == "strict"


def test_runner_and_score_events_share_trace_id(isolated_repo: pathlib.Path) -> None:
    run_dir = create_run(isolated_repo, "telemetry trace")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")

    completed = run_harness(isolated_repo, run_dir, "happy_path")

    assert completed.returncode == 0
    manifest = json.loads((run_dir / "outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    events = [
        json.loads(line)
        for line in (run_dir / "run-events.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert manifest["trace_id"] == run_dir.name
    assert {event["trace_id"] for event in events} == {run_dir.name}
    assert any(event["message"] == "starting model attempt" for event in events)
    assert any(event["message"] == "starting score generation" for event in events)


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
    assert "20260320-000000-prior-success" in retrieval_manifest["selected_source_run_ids"]


def test_runner_v3_cli_subagent_contract_fails_fast(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = create_run(isolated_repo, "v3 cli subagent fail")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")
    contract = default_run_contract(version="v3")
    contract["capabilities"]["enabled"] = True
    contract["capabilities"]["subagents"] = {
        "allowed": True,
        "max_agents": 1,
        "allowed_profiles": ["focused_reader"],
    }
    (run_dir / "run.contract.json").write_text(
        json.dumps(contract, indent=2) + "\n",
        encoding="utf-8",
    )

    completed = run_harness(isolated_repo, run_dir, "happy_path")

    assert completed.returncode == 2
    assert (
        "transport.mode" in completed.stderr
        or "subagent-capable runs require transport.mode" in completed.stderr
    )
    assert not (run_dir / "transcript.jsonl").exists() or (run_dir / "transcript.jsonl").read_text(
        encoding="utf-8"
    ) == ""


def test_runner_v3_rpc_materializes_capability_manifest_and_scores_usage(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = create_run(isolated_repo, "v3 rpc subagent pass")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")
    contract = default_run_contract(version="v3")
    contract["transport"] = {"mode": "rpc"}
    contract["capabilities"]["enabled"] = True
    contract["capabilities"]["subagents"] = {
        "allowed": True,
        "max_agents": 1,
        "allowed_profiles": ["focused_reader"],
    }
    (run_dir / "run.contract.json").write_text(
        json.dumps(contract, indent=2) + "\n",
        encoding="utf-8",
    )
    write_fake_pi_scenario(
        run_dir,
        {
            "scenario": "happy_path",
            "subagent_usage": {
                "usage_version": "v1",
                "spawned_agents": [
                    {
                        "agent_id": "reader-1",
                        "profile_id": "focused_reader",
                        "tool_calls": ["read"],
                        "read_paths": ["starter/README.md"],
                        "write_paths": [],
                        "network_access": False,
                        "prompt_tokens": 120,
                        "runtime_seconds": 12,
                    }
                ],
            },
        },
    )

    completed = run_harness(isolated_repo, run_dir, "happy_path")

    assert completed.returncode == 0
    capability_manifest = json.loads(
        (run_dir / "context" / "capability-manifest.json").read_text(encoding="utf-8")
    )
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    manifest = json.loads((run_dir / "outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    assert capability_manifest["transport"]["mode"] == "rpc"
    assert capability_manifest["subagents"]["allowed_profiles"] == ["focused_reader"]
    assert score["capabilities"]["enabled"] is True
    assert score["capabilities"]["usage_valid"] is True
    assert score["capabilities"]["spawned_profile_ids"] == ["focused_reader"]
    assert manifest["capabilities"]["transport_mode"] == "rpc"
    assert manifest["capabilities"]["usage_valid"] is True


def test_runner_v3_rpc_rejects_invalid_subagent_usage(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = create_run(isolated_repo, "v3 rpc invalid usage")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")
    contract = default_run_contract(version="v3")
    contract["transport"] = {"mode": "rpc"}
    contract["capabilities"]["enabled"] = True
    contract["capabilities"]["subagents"] = {
        "allowed": True,
        "max_agents": 1,
        "allowed_profiles": ["focused_reader"],
    }
    (run_dir / "run.contract.json").write_text(
        json.dumps(contract, indent=2) + "\n",
        encoding="utf-8",
    )
    write_fake_pi_scenario(
        run_dir,
        {
            "scenario": "happy_path",
            "subagent_usage": {
                "usage_version": "v1",
                "spawned_agents": [
                    {
                        "agent_id": "reader-1",
                        "profile_id": "focused_reader",
                        "tool_calls": ["write"],
                        "read_paths": ["../outside.txt"],
                        "write_paths": ["starter/README.md"],
                        "network_access": True,
                        "prompt_tokens": 5001,
                        "runtime_seconds": 999,
                    }
                ],
            },
        },
    )

    completed = run_harness(isolated_repo, run_dir, "happy_path")

    assert completed.returncode == 0
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    assert score["overall_pass"] is False
    assert "eval_failed" in score["failure_classifications"]
    assert score["capabilities"]["usage_valid"] is False
    assert "subagents.write_not_allowed:focused_reader" in score["capabilities"]["violations"]


def test_runner_async_scoring_surfaces_pre_score_capability_audit(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = create_run(isolated_repo, "v3 rpc async invalid usage")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")
    contract = default_run_contract(version="v3")
    contract["transport"] = {"mode": "rpc"}
    contract["capabilities"]["enabled"] = True
    contract["capabilities"]["subagents"] = {
        "allowed": True,
        "max_agents": 1,
        "allowed_profiles": ["focused_reader"],
    }
    (run_dir / "run.contract.json").write_text(
        json.dumps(contract, indent=2) + "\n",
        encoding="utf-8",
    )
    write_fake_pi_scenario(
        run_dir,
        {
            "scenario": "happy_path",
            "subagent_usage": {
                "usage_version": "v1",
                "spawned_agents": [
                    {
                        "agent_id": "reader-1",
                        "profile_id": "focused_reader",
                        "tool_calls": ["write"],
                        "read_paths": ["../outside.txt"],
                        "write_paths": ["starter/README.md"],
                        "network_access": True,
                        "prompt_tokens": 5001,
                        "runtime_seconds": 999,
                    }
                ],
            },
        },
    )

    completed = run_harness(
        isolated_repo,
        run_dir,
        "happy_path",
        extra_env={"HARNESS_ASYNC_SCORING": "1"},
    )

    assert completed.returncode == 0
    manifest = json.loads((run_dir / "outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    preview = json.loads(
        (run_dir / "outputs" / "subagent-usage-validation.json").read_text(encoding="utf-8")
    )
    events = [
        json.loads(line)
        for line in (run_dir / "run-events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert manifest["state"] == "score_pending"
    assert manifest["capabilities"]["usage_validation_path"] == "outputs/subagent-usage-validation.json"
    assert manifest["capabilities"]["usage_valid"] is False
    assert "subagents.write_not_allowed:focused_reader" in manifest["capabilities"]["usage_violations"]
    assert preview["usage_valid"] is False
    assert "subagents.write_not_allowed:focused_reader" in preview["violations"]
    assert any(
        event["message"] == "subagent usage violations detected prior to scoring"
        for event in events
    )


def test_runner_v4_managed_rpc_happy_path_records_live_interception(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = create_run(isolated_repo, "v4 managed rpc happy path")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")
    contract = default_run_contract(version="v4")
    contract["capabilities"]["enabled"] = True
    contract["capabilities"]["subagents"] = {
        "allowed": True,
        "max_agents": 1,
        "allowed_profiles": ["focused_reader"],
    }
    (run_dir / "run.contract.json").write_text(
        json.dumps(contract, indent=2) + "\n",
        encoding="utf-8",
    )
    write_fake_managed_rpc_scenario(run_dir, {})

    completed = run_harness(
        isolated_repo,
        run_dir,
        "happy_path",
        extra_env={
            "HARNESS_PI_BIN": str(isolated_repo / "tests" / "fixtures" / "fake_managed_rpc_peer.py"),
        },
    )

    assert completed.returncode == 0
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    manifest = json.loads((run_dir / "outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    action_log = [
        json.loads(line)
        for line in (run_dir / "outputs" / "subagent-action-log.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert score["overall_pass"] is True
    assert score["capabilities"]["interception_enabled"] is True
    assert score["capabilities"]["allowed_action_count"] == 2
    assert score["capabilities"]["denied_action_count"] == 0
    assert manifest["capabilities"]["action_log_path"] == "outputs/subagent-action-log.jsonl"
    assert manifest["capabilities"]["allowed_action_count"] == 2
    assert len(action_log) == 2
    assert all(entry["decision"] == "allow" for entry in action_log)


def test_runner_v4_managed_rpc_denial_fails_closed(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = create_run(isolated_repo, "v4 managed rpc denied action")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")
    contract = default_run_contract(version="v4")
    contract["capabilities"]["enabled"] = True
    contract["capabilities"]["subagents"] = {
        "allowed": True,
        "max_agents": 1,
        "allowed_profiles": ["focused_reader"],
    }
    (run_dir / "run.contract.json").write_text(
        json.dumps(contract, indent=2) + "\n",
        encoding="utf-8",
    )
    write_fake_managed_rpc_scenario(
        run_dir,
        {
            "requests": [
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
                        "tool": "write",
                        "read_paths": [],
                        "write_paths": ["starter/README.md"],
                        "network_access": True,
                        "runtime_seconds": 2,
                    },
                },
            ]
        },
    )

    completed = run_harness(
        isolated_repo,
        run_dir,
        "happy_path",
        extra_env={
            "HARNESS_PI_BIN": str(isolated_repo / "tests" / "fixtures" / "fake_managed_rpc_peer.py"),
        },
    )

    assert completed.returncode == 0
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    manifest = json.loads((run_dir / "outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    events = [
        json.loads(line)
        for line in (run_dir / "run-events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert score["overall_pass"] is False
    assert "guardrail_policy_violation" in score["failure_classifications"]
    assert score["capabilities"]["denied_action_count"] == 1
    assert "subagents.write_not_allowed:focused_reader" in score["capabilities"]["violations"]
    assert manifest["capabilities"]["first_denial"]["request_id"] == "tool-1"
    assert any(event["message"] == "subagent_tool_denied" for event in events)
    assert not (run_dir / "outputs" / "claim.txt").exists()


def test_runner_v4_contract_failure_blocks_launch_without_interception(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = create_run(isolated_repo, "v4 invalid contract")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")
    contract = default_run_contract(version="v4")
    contract["capabilities"]["interception"]["enabled"] = False
    (run_dir / "run.contract.json").write_text(
        json.dumps(contract, indent=2) + "\n",
        encoding="utf-8",
    )

    completed = run_harness(
        isolated_repo,
        run_dir,
        "happy_path",
        extra_env={
            "HARNESS_PI_BIN": str(isolated_repo / "tests" / "fixtures" / "fake_managed_rpc_peer.py"),
        },
    )

    assert completed.returncode == 2
    assert "capabilities.interception.enabled must be true" in completed.stderr


def test_runner_v4_requires_managed_rpc_capable_peer(
    isolated_repo: pathlib.Path,
) -> None:
    run_dir = create_run(isolated_repo, "v4 invalid peer")
    replace_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")
    contract = default_run_contract(version="v4")
    contract["capabilities"]["enabled"] = True
    contract["capabilities"]["subagents"] = {
        "allowed": True,
        "max_agents": 1,
        "allowed_profiles": ["focused_reader"],
    }
    (run_dir / "run.contract.json").write_text(
        json.dumps(contract, indent=2) + "\n",
        encoding="utf-8",
    )

    completed = run_harness(
        isolated_repo,
        run_dir,
        "happy_path",
        extra_env={
            "HARNESS_PI_BIN": str(isolated_repo / "tests" / "fixtures" / "fake_pi.py"),
        },
    )

    assert completed.returncode == 2
    assert "managed_rpc requires a peer that supports pre-execution interception" in completed.stderr


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
    manifest = json.loads((run_dir / "outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    score = json.loads((run_dir / "score.json").read_text(encoding="utf-8"))
    assert manifest["primary_error_code"] == "result_invalid"
    assert "result_invalid" in manifest["failure_classifications"]
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
