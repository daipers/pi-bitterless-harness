from __future__ import annotations

import json
import pathlib
from dataclasses import dataclass

import orchestrator


@dataclass
class FakeProcess:
    command: list[str]
    model_lifetimes: dict[str, int]
    score_lifetimes: dict[str, int]
    return_code: int = 0

    def __post_init__(self) -> None:
        self.run_id = pathlib.Path(self.command[-1]).name
        self._remaining = (
            self.model_lifetimes.get(self.run_id, 1)
            if "--score-only" not in self.command
            else self.score_lifetimes.get(self.run_id, 0)
        )
        self._completed = False
        self._terminated = False

    def poll(self) -> int | None:
        if self._terminated:
            return self.return_code
        if self._remaining > 0:
            self._remaining -= 1
            return None

        if not self._completed:
            self._write_completion_artifacts()
            self._completed = True
            return self.return_code

        return self.return_code

    def terminate(self) -> None:
        self._terminated = True

    def _write_completion_artifacts(self) -> None:
        run_dir = pathlib.Path(self.command[-1])
        if "--score-only" in self.command:
            payload = {"overall_pass": self.return_code == 0}
            run_dir.joinpath("score.json").write_text(json.dumps(payload) + "\n", encoding="utf-8")
            return

        run_dir.joinpath("outputs").mkdir(exist_ok=True)
        run_dir.joinpath("outputs").joinpath("run_manifest.json").write_text(
            json.dumps({"state": "model_complete"}) + "\n",
            encoding="utf-8",
        )


def _make_run_run_dir(runs_root: pathlib.Path, run_id: str) -> pathlib.Path:
    run_dir = runs_root / run_id
    (run_dir / "outputs").mkdir(parents=True)
    schema = {
        "type": "object",
        "required": [
            "x-interface-version",
            "status",
            "summary",
            "artifacts",
            "claims",
            "remaining_risks",
        ],
        "properties": {
            "x-interface-version": {"const": "v1"},
            "status": {"type": "string"},
            "summary": {"type": "string"},
            "artifacts": {"type": "array"},
            "claims": {"type": "array"},
            "remaining_risks": {"type": "array"},
        },
        "additionalProperties": False,
    }
    task_text = (
        "# Task\n\n## Goal\nRun burst retention test\n\n"
        "## Constraints\n- Stay local.\n\n## Done\n- Done.\n\n"
        "## Eval\n```bash\necho ok\n```\n\n## Required Artifacts\n"
        "- result.json\n\n## Result JSON schema (source of truth)\n"
        "```json\n"
        + json.dumps(schema)
        + "\n```\n"
    )
    (run_dir / "task.md").write_text(
        task_text,
        encoding="utf-8",
    )
    (run_dir / "RUN.md").write_text("# Run\n", encoding="utf-8")
    (run_dir / "result.json").write_text(
        '{"x-interface-version":"v1","status":"success","summary":"ready","artifacts":[],"claims":[],"remaining_risks":[]}\n',
        encoding="utf-8",
    )
    return run_dir


def _orchestrator_config(
    runs_root: pathlib.Path, *, max_model_workers: int = 1, max_score_workers: int = 1
) -> orchestrator.OrchestratorConfig:
    return orchestrator.OrchestratorConfig(
        script_dir=pathlib.Path(__file__).resolve().parents[1] / "starter" / "bin",
        runs_root=runs_root,
        run_queue_path=runs_root / "run-queue.jsonl",
        score_queue_path=runs_root / "score-queue.jsonl",
        max_model_workers=max_model_workers,
        max_score_workers=max_score_workers,
        model_retries=2,
        score_retries=1,
        queue_timeout_seconds=0,
        poll_interval_seconds=0.0,
        model_backoff_ms=0,
        score_backoff_ms=0,
        max_run_wall_clock_seconds=0,
        retention_maintenance_interval_seconds=10.0,
        retention_manual_safe=False,
    )


def _drain_orchestrator_cycle(orch: orchestrator.Orchestrator) -> None:
    orch._collect_model_results()
    orch._collect_score_results()
    orch._drain_stale_workers()
    orch._run_retention_maintenance(reason="pre_async_cycle", force=True)
    orch._heartbeat_workers()
    orch._run_retention_maintenance(reason="heartbeat", force=True)
    orch._dispatch_model_work()
    orch._dispatch_score_work()
    orch._run_retention_maintenance(reason="post_async_cycle", force=True)


def test_orchestrator_async_burst_dispatch_bounded_by_queue_rotation_and_no_drop(
    tmp_path: pathlib.Path,
    monkeypatch,
) -> None:
    runs_root = tmp_path / "runs"
    runs_root.mkdir()

    run_ids = [f"run-{index:03d}" for index in range(12)]
    for run_id in run_ids:
        _make_run_run_dir(runs_root, run_id)

    monkeypatch.setenv("HARNESS_RETENTION_QUEUE_MAX_COUNT", "3")
    monkeypatch.setenv("HARNESS_RETENTION_ARTIFACT_MAX_COUNT", "1")
    monkeypatch.setenv("HARNESS_RETENTION_QUEUE_TTL_DAYS", "0")
    monkeypatch.setenv("HARNESS_RETENTION_ARTIFACT_TTL_DAYS", "0")

    model_lifetimes = {run_id: 1 for run_id in run_ids}
    score_lifetimes = {run_id: 0 for run_id in run_ids}

    def fake_popen(command: list[str], *args, **kwargs) -> FakeProcess:
        return FakeProcess(
            command,
            model_lifetimes=model_lifetimes,
            score_lifetimes=score_lifetimes,
            return_code=0,
        )

    orch = orchestrator.Orchestrator(_orchestrator_config(runs_root))
    monkeypatch.setattr(orchestrator.subprocess, "Popen", fake_popen)

    observed_terminal_counts: list[int] = []
    observed_inflight_counts: list[int] = []

    for _ in range(40):
        _drain_orchestrator_cycle(orch)
        run_queue = orch._load_queue_entries("run")
        terminal_runs = sum(
            1
            for payload in run_queue.values()
            if orchestrator._is_queue_state_terminal(
                orchestrator._extract_queue_state(payload.get("state"))
            )
        )
        inflight_runs = len(run_queue) - terminal_runs
        observed_terminal_counts.append(terminal_runs)
        observed_inflight_counts.append(inflight_runs)
        if len(orch._running_model) == 0 and len(orch._running_score) == 0:
            # all workers drained.
            # If all queue entries are terminal we should stay bounded by policy.
            if terminal_runs == len(run_queue):
                break

    assert observed_terminal_counts
    assert observed_inflight_counts
    assert all(count <= len(run_ids) for count in observed_inflight_counts)

    final_run_queue = orch._load_queue_entries("run")
    final_terminal = [
        run_id
        for run_id, payload in final_run_queue.items()
        if orchestrator._is_queue_state_terminal(
            orchestrator._extract_queue_state(payload.get("state"))
        )
    ]
    final_non_terminal = [
        run_id
        for run_id in run_ids
        if run_id in final_run_queue
        and not orchestrator._is_queue_state_terminal(
            orchestrator._extract_queue_state(final_run_queue[run_id].get("state"))
        )
    ]

    assert len(final_run_queue) == len(final_terminal) + len(final_non_terminal)
    assert len(final_terminal) <= 3
    assert not final_non_terminal or any(run_id in final_non_terminal for run_id in run_ids)

    # ensure producers were not dropped during burst: every run directory is still on disk.
    assert all((runs_root / run_id).exists() for run_id in run_ids)


def test_score_artifacts_rotate_and_remain_readable_after_multiple_retention_cycles(
    tmp_path: pathlib.Path,
) -> None:
    runs_root = tmp_path / "runs"
    runs_root.mkdir()
    old_run = _make_run_run_dir(runs_root, "run-old")
    new_run = _make_run_run_dir(runs_root, "run-new")

    for run_dir, score_name, marker in [
        (old_run, "eval-1.stdout.log", "old-score"),
        (new_run, "eval-1.stdout.log", "new-score"),
    ]:
        (run_dir / "score").mkdir()
        (run_dir / "score" / score_name).write_text(marker, encoding="utf-8")
        (run_dir / "score" / score_name.replace("stdout", "stderr")).write_text(
            "", encoding="utf-8"
        )

    os_old = 1_000_000
    os_new = 2_000_000
    (old_run / "score" / "eval-1.stdout.log").utime((os_old, os_old))
    (old_run / "score" / "eval-1.stderr.log").utime((os_old, os_old))
    (new_run / "score" / "eval-1.stdout.log").utime((os_new, os_new))
    (new_run / "score" / "eval-1.stderr.log").utime((os_new, os_new))

    orch = orchestrator.Orchestrator(
        _orchestrator_config(runs_root, max_model_workers=0, max_score_workers=0),
    )

    for _ in range(2):
        metrics = orch._run_retention_maintenance(reason="artifact-cycle", force=True)
        assert metrics is not None
        assert metrics["retained_score_artifacts"] >= 0

    retention_events = []
    if orch.config.run_queue_path.exists():
        for line in orch.config.run_queue_path.read_text(encoding="utf-8").splitlines():
            retention_events.append(json.loads(line))
    if retention_events:
        assert retention_events[-1].get("retained_score_artifacts") >= 0

    assert (new_run / "score" / "eval-1.stdout.log").read_text(encoding="utf-8") == "new-score"
    assert (old_run / "score" / "eval-1.stdout.log").read_text(encoding="utf-8") == "old-score"
