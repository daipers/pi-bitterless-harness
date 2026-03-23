from __future__ import annotations

import json
import pathlib

import jsonschema
import orchestrator
import run_task
import score_run


def load_schema() -> dict:
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    return json.loads(
        (repo_root / "starter" / "contracts" / "run-event-v1.schema.json").read_text(
            encoding="utf-8"
        )
    )


def read_events(path: pathlib.Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_runner_score_and_orchestrator_events_match_run_event_contract(tmp_path: pathlib.Path) -> None:
    schema = load_schema()
    event_log_path = tmp_path / "runs" / "event-contract" / "run-events.jsonl"
    run_dir = event_log_path.parent
    run_dir.mkdir(parents=True)

    runner = run_task.RunTaskRunner([str(run_dir)])
    runner._log_event("prepare", "runner event")

    context = score_run.ScoreContext(
        task_path=run_dir / "task.md",
        run_dir=run_dir,
        exit_code_path=run_dir / "pi.exit_code.txt",
        out_path=run_dir / "score.json",
        schema_path=run_dir / "result.schema.json",
        event_log_path=event_log_path,
        repo_root=tmp_path,
        worker_id="score-worker-1",
        attempt=2,
    )
    score_run.append_event(context, "score", "score event")

    orchestrator._append_run_event(
        run_dir,
        "state_repair",
        "orchestrator event",
        worker_id="orchestrator",
        attempt=1,
        failure_classification="orchestrator_worker_unavailable",
    )

    events = read_events(event_log_path)

    assert len(events) == 3
    assert {event["trace_id"] for event in events} == {run_dir.name}
    for event in events:
        jsonschema.validate(event, schema)
        assert event["trace_id"] == event["run_id"] == run_dir.name
    assert events[1]["worker_id"] == "score-worker-1"
    assert events[1]["attempt"] == 2
    assert events[2]["failure_classification"] == "orchestrator_worker_unavailable"
    assert events[2]["failure_class"] == "orchestrator_worker_unavailable"


def test_orchestrator_pre_manifest_event_uses_run_id_trace_id(tmp_path: pathlib.Path) -> None:
    schema = load_schema()
    run_dir = tmp_path / "runs" / "pre-manifest-event"
    run_dir.mkdir(parents=True)

    orchestrator._append_run_event(
        run_dir,
        "state_repair",
        "detected stale state before manifest creation",
        worker_id="orchestrator",
    )

    events = read_events(run_dir / "run-events.jsonl")

    assert len(events) == 1
    jsonschema.validate(events[0], schema)
    assert events[0]["trace_id"] == run_dir.name
    assert events[0]["run_id"] == run_dir.name
    assert not (run_dir / "outputs" / "run_manifest.json").exists()
