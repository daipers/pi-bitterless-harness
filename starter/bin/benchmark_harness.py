#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import statistics
import subprocess  # nosec B404 - harness benchmark uses fixed local subprocess invocations
import sys
import tempfile
import time

from harnesslib import default_run_contract


def create_benchmark_run(
    harness_root: pathlib.Path,
    title: str,
    *,
    profile: str = "strict",
) -> pathlib.Path:
    subprocess.run(  # nosec B603 - fixed local harness script path and static argv
        [str(harness_root / "bin" / "new-task.sh"), "--profile", profile, title],
        cwd=str(harness_root),
        text=True,
        check=True,
        capture_output=True,
        env=os.environ | {"PYTHONPATH": str(harness_root / "bin")},
    )
    return max((harness_root / "runs").iterdir(), key=lambda path: path.stat().st_mtime)


def set_eval_command(task_path: pathlib.Path, command: str) -> None:
    task_text = task_path.read_text(encoding="utf-8")
    task_text = task_text.replace(
        "# python3 -m pytest tests/test_runner_e2e.py -q",
        command,
    )
    task_path.write_text(task_text, encoding="utf-8")


def benchmark_run_task(harness_root: pathlib.Path, fake_pi: pathlib.Path) -> dict[str, object]:
    latencies = []
    disk_bytes = []
    fd_counts = []

    run_dir = create_benchmark_run(harness_root, "benchmark task")
    set_eval_command(run_dir / "task.md", "python3 ../tests/fixtures/pass_eval.py")

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

    p95_latency_ms = (
        max(latencies)
        if len(latencies) < 20
        else statistics.quantiles(latencies, n=20)[18]
    )
    return {
        "runs": len(latencies),
        "p95_latency_ms": round(p95_latency_ms, 2),
        "max_disk_bytes": max(disk_bytes) if disk_bytes else 0,
        "max_fd_count": max(fd_counts) if fd_counts else 0,
    }


def benchmark_retrieval(harness_root: pathlib.Path) -> dict[str, object]:
    schema_text = (harness_root / "result.schema.json").read_text(encoding="utf-8").rstrip()
    runs_root = harness_root / "runs"
    prepare_context = harness_root / "bin" / "prepare-context.py"
    env = os.environ | {"PYTHONPATH": str(harness_root / "bin")}

    def write_run(
        run_dir: pathlib.Path,
        *,
        goal: str,
        summary: str,
        claims: list[dict[str, object]],
        artifacts: list[dict[str, str]],
        artifact_contents: dict[str, str],
    ) -> None:
        (run_dir / "outputs").mkdir(parents=True)
        (run_dir / "score").mkdir()
        (run_dir / "home").mkdir()
        (run_dir / "session").mkdir()
        (run_dir / "task.md").write_text(
            f"""# Task
Retrieval benchmark seed

## Goal
{goal}

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
{schema_text}
```
""",
            encoding="utf-8",
        )
        (run_dir / "RUN.md").write_text("# Run\n", encoding="utf-8")
        (run_dir / "result.schema.json").write_text(schema_text + "\n", encoding="utf-8")
        (run_dir / "result.template.json").write_text("{}", encoding="utf-8")
        (run_dir / "run.contract.json").write_text(
            json.dumps(default_run_contract(execution_profile="capability"), indent=2)
            + "\n",
            encoding="utf-8",
        )
        for rel_path, contents in artifact_contents.items():
            artifact_path = run_dir / rel_path
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_text(contents, encoding="utf-8")
        (run_dir / "outputs" / "run_manifest.json").write_text(
            json.dumps({"overall_pass": True}, indent=2) + "\n",
            encoding="utf-8",
        )
        (run_dir / "result.json").write_text(
            json.dumps(
                {
                    "x-interface-version": "v1",
                    "status": "success",
                    "summary": summary,
                    "artifacts": artifacts,
                    "claims": claims,
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

    scenarios = [
        {
            "target_goal": "Produce a passing score for harness retrieval with nebula-vector retrieval anchor.",
            "expected_run_id": "20260322-010001-anchor-good",
        },
        {
            "target_goal": "Recover claim-rich retrieval context for comet-lattice scoring.",
            "expected_run_id": "20260322-010003-claim-rich",
        },
        {
            "target_goal": "Select artifact-backed retrieval notes for aurora proof bundle.",
            "expected_run_id": "20260322-010005-artifact-rich",
        },
    ]

    write_run(
        runs_root / "20260322-010000-anchor-confuser",
        goal="Produce a passing score for harness retrieval quickly.",
        summary="Produce a passing score for harness retrieval quickly.",
        claims=[],
        artifacts=[],
        artifact_contents={},
    )
    write_run(
        runs_root / "20260322-010001-anchor-good",
        goal="Produce a passing score for harness retrieval with nebula-vector retrieval anchor.",
        summary="retrieval scoring success",
        claims=[
            {
                "claim": "nebula-vector retrieval anchor proof was preserved",
                "evidence": ["outputs/anchor.txt"],
            }
        ],
        artifacts=[
            {"path": "outputs/anchor.txt", "description": "nebula-vector retrieval anchor evidence"}
        ],
        artifact_contents={"outputs/anchor.txt": "nebula-vector retrieval anchor proof\n"},
    )
    write_run(
        runs_root / "20260322-010002-claim-confuser",
        goal="Recover retrieval context for scoring.",
        summary="claim-light retrieval scoring notes",
        claims=[],
        artifacts=[],
        artifact_contents={},
    )
    write_run(
        runs_root / "20260322-010003-claim-rich",
        goal="Recover claim-rich retrieval context for comet-lattice scoring.",
        summary="compact summary",
        claims=[
            {
                "claim": "comet-lattice scoring succeeded with preserved claim evidence",
                "evidence": ["outputs/claim.txt"],
            }
        ],
        artifacts=[{"path": "outputs/claim.txt", "description": "claim evidence"}],
        artifact_contents={"outputs/claim.txt": "comet-lattice scoring evidence\n"},
    )
    write_run(
        runs_root / "20260322-010004-artifact-confuser",
        goal="Select retrieval notes for proof bundle.",
        summary="artifact-light summary",
        claims=[],
        artifacts=[],
        artifact_contents={},
    )
    write_run(
        runs_root / "20260322-010005-artifact-rich",
        goal="Select artifact-backed retrieval notes for aurora proof bundle.",
        summary="artifact backed retrieval success",
        claims=[
            {
                "claim": "aurora proof bundle was captured",
                "evidence": ["outputs/aurora.txt"],
            }
        ],
        artifacts=[{"path": "outputs/aurora.txt", "description": "aurora proof bundle artifact"}],
        artifact_contents={"outputs/aurora.txt": "aurora proof bundle evidence\n"},
    )

    index_root = harness_root / "runs" / ".index"
    if index_root.exists():
        shutil.rmtree(index_root)

    scenario_manifests: list[dict[str, object]] = []

    cold_ms = 0.0
    warm_ms = 0.0
    cold_manifest: dict[str, object] = {}
    warm_manifest: dict[str, object] = {}

    for index, scenario in enumerate(scenarios):
        current_run = create_benchmark_run(
            harness_root,
            f"retrieval benchmark target {index}",
            profile="capability",
        )
        set_eval_command(current_run / "task.md", "python3 ../tests/fixtures/pass_eval.py")
        task_text = (current_run / "task.md").read_text(encoding="utf-8")
        task_text = task_text.replace(
            "Describe the desired outcome in plain language.",
            scenario["target_goal"],
        )
        (current_run / "task.md").write_text(task_text, encoding="utf-8")

        started = time.perf_counter()
        subprocess.run(
            [sys.executable, str(prepare_context), str(current_run), "policies/capability.json"],
            cwd=str(harness_root),
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        duration_ms = (time.perf_counter() - started) * 1000.0
        manifest = json.loads(
            (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
        )
        scenario_manifests.append(
            {
                "expected_run_id": scenario["expected_run_id"],
                "manifest": manifest,
            }
        )
        if index == 0:
            cold_ms = duration_ms
            cold_manifest = manifest
            warm_started = time.perf_counter()
            subprocess.run(
                [sys.executable, str(prepare_context), str(current_run), "policies/capability.json"],
                cwd=str(harness_root),
                check=True,
                capture_output=True,
                text=True,
                env=env,
            )
            warm_ms = (time.perf_counter() - warm_started) * 1000.0
            warm_manifest = json.loads(
                (current_run / "context" / "retrieval-manifest.json").read_text(encoding="utf-8")
            )

    top_1_hits = 0
    top_3_hits = 0
    selected_scores: list[float] = []
    for payload in scenario_manifests:
        expected_run_id = str(payload["expected_run_id"])
        manifest = payload["manifest"]
        top_candidates = manifest.get("top_candidates", [])
        top_ids = [item.get("run_id") for item in top_candidates[:3]]
        if top_candidates and top_candidates[0].get("run_id") == expected_run_id:
            top_1_hits += 1
        if expected_run_id in top_ids:
            top_3_hits += 1
        selected_sources = manifest.get("selected_sources", [])
        if selected_sources:
            selected_scores.append(float(selected_sources[0].get("total_score", 0)))

    return {
        "scenario_count": len(scenarios),
        "top_1_hit_rate": round(top_1_hits / len(scenarios), 2),
        "top_3_hit_rate": round(top_3_hits / len(scenarios), 2),
        "mean_selected_score": round(sum(selected_scores) / len(selected_scores), 2),
        "cold_build_ms": round(cold_ms, 2),
        "warm_reuse_ms": round(warm_ms, 2),
        "cold_index_mode": cold_manifest.get("index_mode"),
        "warm_index_mode": warm_manifest.get("index_mode"),
        "candidate_run_count": warm_manifest.get("candidate_run_count"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Bitterless Harness")
    parser.add_argument(
        "--mode",
        choices=["all", "run-task", "retrieval"],
        default="all",
        help="which benchmark set to run",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    fake_pi = repo_root / "tests" / "fixtures" / "fake_pi.py"
    with tempfile.TemporaryDirectory(prefix="bitterless-bench-") as tmp_dir_name:
        tmp_dir = pathlib.Path(tmp_dir_name)
        harness_root = tmp_dir / "starter"
        shutil.copytree(
            repo_root / "starter",
            harness_root,
            ignore=shutil.ignore_patterns("runs"),
        )
        payload: dict[str, object] = {
            "backpressure_policy": {
                "disk_used_threshold_percent": int(
                    os.environ.get("HARNESS_DISK_USED_THRESHOLD_PERCENT", "90")
                ),
                "free_mb_threshold": int(os.environ.get("HARNESS_FREE_MB_THRESHOLD", "512")),
            }
        }
        if args.mode in {"all", "run-task"}:
            payload["run_task"] = benchmark_run_task(harness_root, fake_pi)
        if args.mode in {"all", "retrieval"}:
            payload["retrieval"] = benchmark_retrieval(harness_root)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
