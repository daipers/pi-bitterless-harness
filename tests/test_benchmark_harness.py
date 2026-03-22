from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys


def test_benchmark_harness_emits_retrieval_relevance_metrics(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    completed = subprocess.run(
        [sys.executable, str(starter / "bin" / "benchmark_harness.py"), "--mode", "retrieval"],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ | {"PYTHONPATH": str(starter / "bin")},
    )

    payload = json.loads(completed.stdout)
    retrieval = payload["retrieval"]
    assert retrieval["scenario_count"] == 8
    assert retrieval["top_1_hit_rate"] >= 0
    assert retrieval["top_3_hit_rate"] >= retrieval["top_1_hit_rate"]
    assert retrieval["abstention_hit_rate"] >= 0
    assert retrieval["empty_context_rate"] >= 0
    assert retrieval["mean_selected_source_count"] >= 0
    assert retrieval["mean_selected_score"] >= 0
    assert retrieval["hard_negative_win_rate"] >= 0
    assert retrieval["retrieval_profile_id"] == "retrieval-v4-default"
    assert retrieval["cold_build_ms"] >= 0
    assert retrieval["warm_reuse_ms"] >= 0
    assert retrieval["cold_index_mode"] == "cold_build"
    assert retrieval["warm_index_mode"] == "warm_reuse"
    assert len(retrieval["scenario_results"]) == 8


def test_analyze_retrieval_benchmarks_records_history_and_emits_trend(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    first_out = starter / "runs" / "retrieval-first.json"
    second_out = starter / "runs" / "retrieval-second.json"
    benchmark_command = [
        sys.executable,
        str(starter / "bin" / "benchmark_harness.py"),
        "--mode",
        "retrieval",
    ]

    first = subprocess.run(
        benchmark_command + ["--out", str(first_out)],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    second = subprocess.run(
        benchmark_command + ["--out", str(second_out)],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    first_payload = json.loads(first.stdout)
    second_payload = json.loads(second.stdout)
    analysis = subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "analyze_retrieval_benchmarks.py"),
            str(first_out),
            str(second_out),
            "--history-dir",
            str(starter / "runs"),
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    analysis_payload = json.loads(analysis.stdout)
    history_path = starter / "runs" / "retrieval-benchmark.history.jsonl"
    summary_paths = sorted(starter.glob("runs/retrieval-benchmark-*.summary.json"))

    assert history_path.exists()
    assert len(history_path.read_text(encoding="utf-8").splitlines()) == 2
    assert len(summary_paths) == 2
    assert first_payload["retrieval"]["scenario_count"] == 8
    assert second_payload["retrieval"]["scenario_count"] == 8
    assert analysis_payload["history_length"] == 2
    assert analysis_payload["trend"]["history_length"] == 2
    assert "delta_vs_previous" in analysis_payload["trend"]
    assert "prune_candidates" in analysis_payload["latest_analysis"]


def test_benchmark_harness_emits_replay_workload_metrics(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    replay_corpus = starter / "benchmarks" / "replay-corpus.json"
    history_dir = starter / "runs" / "history"
    replay_corpus.write_text(
        json.dumps(
            [
                {
                    "run_id": "replay-success",
                    "benchmark_labels": ["success"],
                    "evidence": {"task_excerpt": ["# Task", "Replay success workload"]},
                },
                {
                    "run_id": "replay-invalid",
                    "benchmark_labels": ["result_invalid"],
                    "evidence": {"task_excerpt": ["# Task", "Replay invalid result workload"]},
                },
                {
                    "run_id": "replay-retry",
                    "benchmark_labels": ["retry"],
                    "evidence": {"task_excerpt": ["# Task", "Replay retry workload"]},
                },
            ],
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "benchmark_harness.py"),
            "--mode",
            "replay",
            "--replay-corpus",
            str(replay_corpus),
            "--replay-runs",
            "3",
            "--history-dir",
            str(history_dir),
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    payload = json.loads(completed.stdout)["replay"]
    assert payload["corpus_size"] == 3
    assert payload["sampled_run_count"] == 3
    assert len(payload["workload_metrics"]) == 2
    assert payload["workload_metrics"][0]["concurrency"] == 1
    assert payload["workload_metrics"][1]["concurrency"] == 2
    assert (history_dir / "replay-benchmark.history.jsonl").exists()
    assert sorted(history_dir.glob("replay-benchmark-*.summary.json"))


def test_benchmark_harness_emits_generated_fault_injection_metrics(
    isolated_repo: pathlib.Path,
) -> None:
    starter = isolated_repo / "starter"
    env = os.environ | {"PYTHONPATH": str(starter / "bin")}
    novel_out = starter / "runs" / "fault-novel.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(starter / "bin" / "benchmark_harness.py"),
            "--mode",
            "fault-injection",
            "--fault-samples",
            "4",
            "--fault-seed",
            "11",
            "--fault-corpus-out",
            str(novel_out),
        ],
        cwd=starter,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    payload = json.loads(completed.stdout)["fault_injection"]
    assert payload["sample_count"] == 4
    assert payload["seed"] == 11
    assert len(payload["cases"]) == 4
    assert novel_out.exists()
