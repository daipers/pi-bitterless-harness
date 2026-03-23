from __future__ import annotations

import json
import pathlib

import build_replay_corpus


def make_run(
    runs_root: pathlib.Path,
    run_id: str,
    *,
    transcript_line: str,
    stderr_line: str,
    primary_error_code: str | None,
    failure_classifications: list[str],
) -> None:
    run_dir = runs_root / run_id
    (run_dir / "outputs").mkdir(parents=True)
    (run_dir / "task.md").write_text(
        "# Task\nHARNESS_PI_AUTH_JSON=/tmp/auth.json\n",
        encoding="utf-8",
    )
    (run_dir / "run-events.jsonl").write_text(
        json.dumps({"message": "event", "path": "context/source-runs/old-run"}) + "\n"
        + json.dumps({"message": "safe event"}) + "\n",
        encoding="utf-8",
    )
    (run_dir / "transcript.jsonl").write_text(transcript_line + "\n", encoding="utf-8")
    (run_dir / "pi.stderr.log").write_text(stderr_line + "\n", encoding="utf-8")
    (run_dir / "outputs" / "run_manifest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-22T00:00:00Z",
                "state": "complete",
                "primary_error_code": primary_error_code,
                "failure_classifications": failure_classifications,
                "git": {"sha": "abc123"},
                "dependencies": {"pi": "pi 0.61.1"},
                "timings": {"run_finished_epoch_ms": 1234},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "score.json").write_text(
        json.dumps(
            {
                "overall_pass": primary_error_code is None,
                "overall_error_code": "none" if primary_error_code is None else primary_error_code,
                "failure_classifications": failure_classifications,
            }
        )
        + "\n",
        encoding="utf-8",
    )


def test_build_replay_corpus_redacts_and_sorts(tmp_path: pathlib.Path) -> None:
    runs_root = tmp_path / "runs"
    runs_root.mkdir()
    make_run(
        runs_root,
        "run-b",
        transcript_line="Authorization: Bearer secret-token",
        stderr_line="Authorization: Bearer super-secret",
        primary_error_code="model_invocation_failed",
        failure_classifications=["model_invocation_failed"],
    )
    make_run(
        runs_root,
        "run-a",
        transcript_line="HARNESS_PI_AUTH_JSON=/tmp/private.json",
        stderr_line="auth.json=/tmp/private.json",
        primary_error_code=None,
        failure_classifications=[],
    )

    records = build_replay_corpus.build_corpus(
        runs_root,
        limit=10,
        transcript_lines=5,
        event_lines=5,
    )

    assert [record["run_id"] for record in records] == ["run-a", "run-b"]
    assert records[0]["benchmark_labels"] == ["success"]
    assert "[redacted]" in "\n".join(records[0]["evidence"]["task_excerpt"])
    assert "[redacted]" in "\n".join(records[0]["evidence"]["transcript_excerpt"])
    assert "[redacted]" in "\n".join(records[1]["evidence"]["stderr_excerpt"])
    combined_event_excerpt = (
        records[0]["evidence"]["event_excerpt"] + records[1]["evidence"]["event_excerpt"]
    )
    assert all(
        "context/source-runs/" not in line
        for line in combined_event_excerpt
    )
