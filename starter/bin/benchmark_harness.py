#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import pathlib
import shutil
import statistics
import subprocess  # nosec B404 - harness benchmark uses fixed local subprocess invocations
import tempfile
import time


def main() -> None:
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    fake_pi = repo_root / "tests" / "fixtures" / "fake_pi.py"
    latencies = []
    disk_bytes = []
    fd_counts = []
    with tempfile.TemporaryDirectory(prefix="bitterless-bench-") as tmp_dir_name:
        tmp_dir = pathlib.Path(tmp_dir_name)
        harness_root = tmp_dir / "starter"
        shutil.copytree(
            repo_root / "starter",
            harness_root,
            ignore=shutil.ignore_patterns("runs"),
        )
        subprocess.run(  # nosec B603 - fixed local harness script path and static argv
            [str(harness_root / "bin" / "new-task.sh"), "benchmark task"],
            cwd=str(harness_root),
            text=True,
            check=True,
            capture_output=True,
            env=os.environ | {"PYTHONPATH": str(harness_root / "bin")},
        )
        run_dir = max((harness_root / "runs").iterdir(), key=lambda path: path.stat().st_mtime)
        task_text = (run_dir / "task.md").read_text(encoding="utf-8")
        task_text = task_text.replace(
            "# python3 -m pytest tests/test_runner_e2e.py -q",
            "python3 -c \"print('benchmark ok')\"",
        )
        (run_dir / "task.md").write_text(task_text, encoding="utf-8")
        for _ in range(3):
            started = time.perf_counter()
            subprocess.run(  # nosec B603 - fixed local harness script path and controlled env
                [str(harness_root / "bin" / "run-task.sh"), str(run_dir)],
                cwd=str(harness_root),
                check=True,
                env=os.environ
                | {
                    "PYTHONPATH": str(harness_root / "bin"),
                    "HARNESS_FORCE_RERUN": "1",
                    "HARNESS_PI_BIN": str(fake_pi),
                    "FAKE_PI_SCENARIO": "happy_path",
                },
                capture_output=True,
                text=True,
            )
            latencies.append((time.perf_counter() - started) * 1000.0)
            disk_bytes.append(
                sum(path.stat().st_size for path in run_dir.rglob("*") if path.is_file())
            )
            fd_counts.append(
                len(list(pathlib.Path("/dev/fd").iterdir()))
                if pathlib.Path("/dev/fd").exists()
                else 0
            )

    if len(latencies) < 20:
        p95_latency_ms = max(latencies)
    else:
        p95_latency_ms = statistics.quantiles(latencies, n=20)[18]
    payload = {
        "runs": len(latencies),
        "p95_latency_ms": round(p95_latency_ms, 2),
        "max_disk_bytes": max(disk_bytes) if disk_bytes else 0,
        "max_fd_count": max(fd_counts) if fd_counts else 0,
        "backpressure_policy": {
            "disk_used_threshold_percent": int(
                os.environ.get("HARNESS_DISK_USED_THRESHOLD_PERCENT", "90")
            ),
            "free_mb_threshold": int(os.environ.get("HARNESS_FREE_MB_THRESHOLD", "512")),
        },
    }
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
