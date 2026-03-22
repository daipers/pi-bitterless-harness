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
    assert retrieval["scenario_count"] == 3
    assert retrieval["top_1_hit_rate"] >= 0
    assert retrieval["top_3_hit_rate"] >= retrieval["top_1_hit_rate"]
    assert retrieval["mean_selected_score"] >= 0
    assert retrieval["cold_build_ms"] >= 0
    assert retrieval["warm_reuse_ms"] >= 0
    assert retrieval["cold_index_mode"] == "cold_build"
    assert retrieval["warm_index_mode"] == "warm_reuse"
