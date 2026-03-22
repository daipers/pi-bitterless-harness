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
