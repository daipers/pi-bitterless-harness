#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import pathlib
import random
import shutil
import statistics
import subprocess  # nosec B404 - harness benchmark uses fixed local subprocess invocations
import sys
import tempfile
import time
from typing import Any

from build_replay_corpus import excerpt_lines
from harnesslib import default_run_contract
from harvester import harvest
from retrieval_index import load_retrieval_profile

SUMMARY_METRIC_KEYS = (
    "top_1_hit_rate",
    "top_3_hit_rate",
    "abstention_hit_rate",
    "empty_context_rate",
    "mean_selected_source_count",
    "mean_selected_score",
    "hard_negative_win_rate",
)


def create_benchmark_run(
    harness_root: pathlib.Path,
    title: str,
    *,
    profile: str = "strict",
) -> pathlib.Path:
    subprocess.run(
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
        subprocess.run(
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
        disk_bytes.append(sum(path.stat().st_size for path in run_dir.rglob("*") if path.is_file()))
        fd_counts.append(
            len(list(pathlib.Path("/dev/fd").iterdir())) if pathlib.Path("/dev/fd").exists() else 0
        )

    p95_latency_ms = (
        max(latencies) if len(latencies) < 20 else statistics.quantiles(latencies, n=20)[18]
    )
    return {
        "runs": len(latencies),
        "p95_latency_ms": round(p95_latency_ms, 2),
        "max_disk_bytes": max(disk_bytes) if disk_bytes else 0,
        "max_fd_count": max(fd_counts) if fd_counts else 0,
    }


def load_json(path: pathlib.Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_task(
    task_path: pathlib.Path,
    *,
    schema_text: str,
    title: str,
    goal: str,
    constraints: list[str] | None = None,
    done: list[str] | None = None,
) -> None:
    constraint_lines = constraints or ["Stay local."]
    done_lines = done or ["Score is written."]
    task_path.write_text(
        "\n".join(
            [
                "# Task",
                title,
                "",
                "## Goal",
                goal,
                "",
                "## Constraints",
                *[f"- {item}" for item in constraint_lines],
                "",
                "## Done",
                *[f"- {item}" for item in done_lines],
                "",
                "## Eval",
                "```bash",
                "python3 ../tests/fixtures/pass_eval.py",
                "```",
                "",
                "## Required Artifacts",
                "- result.json",
                "- outputs/run_manifest.json",
                "",
                "## Retrieval Quality Rubric",
                "- `summary`: write 1-3 outcome-focused sentences with concrete identifiers, "
                "outputs, or checks.",
                "- `claims`: keep each claim atomic and cite evidence paths or exact "
                "verification commands.",
                "- `artifacts[].description`: explain what the artifact proves or contains, "
                "not just the filename.",
                "",
                "## Result JSON schema (source of truth)",
                "```json",
                schema_text,
                "```",
                "",
            ]
        ),
        encoding="utf-8",
    )


def write_seed_run(
    run_dir: pathlib.Path,
    *,
    schema_text: str,
    seed_run: dict[str, Any],
) -> None:
    (run_dir / "outputs").mkdir(parents=True)
    (run_dir / "score").mkdir()
    (run_dir / "home").mkdir()
    (run_dir / "session").mkdir()
    write_task(
        run_dir / "task.md",
        schema_text=schema_text,
        title=str(seed_run.get("title", "Retrieval benchmark seed")),
        goal=str(seed_run["goal"]),
        constraints=[str(item) for item in seed_run.get("constraints", ["seedscope"])],
        done=[str(item) for item in seed_run.get("done", ["seedready"])],
    )
    (run_dir / "RUN.md").write_text("# Run\n", encoding="utf-8")
    (run_dir / "result.schema.json").write_text(schema_text + "\n", encoding="utf-8")
    (run_dir / "result.template.json").write_text("{}", encoding="utf-8")
    (run_dir / "run.contract.json").write_text(
        json.dumps(default_run_contract(execution_profile="capability"), indent=2) + "\n",
        encoding="utf-8",
    )
    for rel_path, contents in dict(seed_run.get("artifact_contents", {})).items():
        artifact_path = run_dir / rel_path
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(str(contents), encoding="utf-8")
    (run_dir / "outputs" / "run_manifest.json").write_text(
        json.dumps({"overall_pass": True}, indent=2) + "\n",
        encoding="utf-8",
    )
    (run_dir / "result.json").write_text(
        json.dumps(
            {
                "x-interface-version": "v1",
                "status": "success",
                "summary": seed_run["summary"],
                "artifacts": seed_run["artifacts"],
                "claims": seed_run["claims"],
                "remaining_risks": seed_run.get("remaining_risks", []),
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


def clear_runs_dir(runs_root: pathlib.Path) -> None:
    runs_root.mkdir(parents=True, exist_ok=True)
    for child in runs_root.iterdir():
        if child.name.startswith("."):
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
            continue
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def load_retrieval_corpus(harness_root: pathlib.Path) -> list[dict[str, Any]]:
    payload = load_json(harness_root / "benchmarks" / "retrieval_corpus.json")
    if not isinstance(payload, list):
        raise ValueError("retrieval benchmark corpus must be a JSON array")
    return payload


def build_scenario_result(
    scenario: dict[str, Any],
    manifest: dict[str, Any],
    *,
    duration_ms: float,
) -> dict[str, Any]:
    top_candidates = manifest.get("top_candidates", [])
    top_ids = [str(item.get("run_id", "")) for item in top_candidates[:3]]
    candidate_scores = {
        str(item.get("run_id", "")): float(item.get("total_score", 0)) for item in top_candidates
    }
    selected_sources = manifest.get("selected_sources", [])
    expected_empty_context = bool(scenario.get("expected_empty_context", False))
    expected_top_1_run_id = scenario.get("expected_top_1_run_id")
    selected_source_count = int(
        manifest.get("selected_source_count", manifest.get("selected_count", 0))
    )
    empty_context = bool(manifest.get("empty_context", selected_source_count == 0))
    selected_score = 0.0
    if selected_sources:
        selected_score = float(selected_sources[0].get("total_score", 0))

    hard_negative_run_ids = [
        str(item)
        for item in scenario.get("hard_negative_run_ids", [])
        if isinstance(item, str) and item
    ]
    if not hard_negative_run_ids and isinstance(expected_top_1_run_id, str):
        hard_negative_run_ids = [
            str(seed_run["run_id"])
            for seed_run in scenario.get("seed_runs", [])
            if str(seed_run.get("run_id")) != expected_top_1_run_id
        ]

    top_1_hit = False
    top_3_hit = False
    abstention_hit = False
    hard_negative_pass = expected_empty_context
    strongest_confuser_run_id = None
    strongest_confuser_score = None
    strongest_confuser_margin = None

    if expected_empty_context:
        abstention_hit = empty_context
    elif isinstance(expected_top_1_run_id, str):
        top_1_hit = bool(
            top_candidates and top_candidates[0].get("run_id") == expected_top_1_run_id
        )
        top_3_hit = expected_top_1_run_id in top_ids

        gold_score = candidate_scores.get(expected_top_1_run_id)
        strongest_confuser: tuple[str, float] | None = None
        for run_id in hard_negative_run_ids:
            confuser_score = candidate_scores.get(run_id)
            if confuser_score is None:
                continue
            if strongest_confuser is None or confuser_score > strongest_confuser[1]:
                strongest_confuser = (run_id, confuser_score)

        if strongest_confuser is None:
            hard_negative_pass = True
        elif gold_score is not None:
            strongest_confuser_run_id = strongest_confuser[0]
            strongest_confuser_score = round(strongest_confuser[1], 2)
            strongest_confuser_margin = round(gold_score - strongest_confuser[1], 2)
            hard_negative_pass = gold_score > strongest_confuser[1]
        else:
            hard_negative_pass = False

    return {
        "scenario_id": scenario["scenario_id"],
        "case_kind": scenario.get("case_kind"),
        "provenance": scenario.get("provenance"),
        "query_goal": scenario["query_goal"],
        "expected_top_1_run_id": expected_top_1_run_id,
        "expected_empty_context": expected_empty_context,
        "hard_negative_run_ids": hard_negative_run_ids,
        "selected_source_count": selected_source_count,
        "empty_context": empty_context,
        "selected_score": round(selected_score, 2),
        "top_candidate_run_ids": top_ids,
        "top_1_hit": top_1_hit,
        "top_3_hit": top_3_hit,
        "abstention_hit": abstention_hit,
        "hard_negative_pass": hard_negative_pass,
        "strongest_confuser_run_id": strongest_confuser_run_id,
        "strongest_confuser_score": strongest_confuser_score,
        "strongest_confuser_margin": strongest_confuser_margin,
        "duration_ms": round(duration_ms, 2),
    }


def round_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 2)


def round_mean(values: list[float | int]) -> float:
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def benchmark_retrieval(
    harness_root: pathlib.Path,
    *,
    profile_path: pathlib.Path | None = None,
) -> dict[str, object]:
    schema_text = (harness_root / "result.schema.json").read_text(encoding="utf-8").rstrip()
    runs_root = harness_root / "runs"
    prepare_context = harness_root / "bin" / "prepare-context.py"
    env = os.environ | {"PYTHONPATH": str(harness_root / "bin")}
    if profile_path is not None:
        env["HARNESS_RETRIEVAL_PROFILE_PATH"] = str(profile_path)
        profile = load_retrieval_profile(profile_path)
    else:
        profile = load_retrieval_profile(repo_root=harness_root)
    corpus = load_retrieval_corpus(harness_root)

    cold_ms = 0.0
    warm_ms = 0.0
    cold_manifest: dict[str, Any] = {}
    warm_manifest: dict[str, Any] = {}
    scenario_results: list[dict[str, Any]] = []

    for index, scenario in enumerate(corpus):
        clear_runs_dir(runs_root)
        for seed_run in scenario["seed_runs"]:
            write_seed_run(
                runs_root / str(seed_run["run_id"]),
                schema_text=schema_text,
                seed_run=seed_run,
            )

        current_run = create_benchmark_run(
            harness_root,
            f"retrieval benchmark {scenario['scenario_id']}",
            profile="capability",
        )
        write_task(
            current_run / "task.md",
            schema_text=schema_text,
            title=str(
                scenario.get("query_title", f"Retrieval benchmark {scenario['scenario_id']}")
            ),
            goal=str(scenario["query_goal"]),
            constraints=[str(item) for item in scenario.get("query_constraints", ["queryscope"])],
            done=[str(item) for item in scenario.get("query_done", ["queryready"])],
        )
        (current_run / "result.schema.json").write_text(schema_text + "\n", encoding="utf-8")

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
        manifest = load_json(current_run / "context" / "retrieval-manifest.json")
        scenario_results.append(build_scenario_result(scenario, manifest, duration_ms=duration_ms))

        if index == 0:
            cold_ms = duration_ms
            cold_manifest = manifest
            warm_started = time.perf_counter()
            subprocess.run(
                [
                    sys.executable,
                    str(prepare_context),
                    str(current_run),
                    "policies/capability.json",
                ],
                cwd=str(harness_root),
                check=True,
                capture_output=True,
                text=True,
                env=env,
            )
            warm_ms = (time.perf_counter() - warm_started) * 1000.0
            warm_manifest = load_json(current_run / "context" / "retrieval-manifest.json")

    positive_results = [item for item in scenario_results if not item["expected_empty_context"]]
    abstention_results = [item for item in scenario_results if item["expected_empty_context"]]

    retrieval: dict[str, Any] = {
        "scenario_count": len(scenario_results),
        "retrieval_profile_id": profile["profile_id"],
        "top_1_hit_rate": round_rate(
            sum(1 for item in positive_results if item["top_1_hit"]),
            len(positive_results),
        ),
        "top_3_hit_rate": round_rate(
            sum(1 for item in positive_results if item["top_3_hit"]),
            len(positive_results),
        ),
        "abstention_hit_rate": round_rate(
            sum(1 for item in abstention_results if item["abstention_hit"]),
            len(abstention_results),
        ),
        "empty_context_rate": round_rate(
            sum(1 for item in scenario_results if item["empty_context"]),
            len(scenario_results),
        ),
        "mean_selected_source_count": round_mean(
            [int(item["selected_source_count"]) for item in scenario_results]
        ),
        "mean_selected_score": round_mean(
            [float(item["selected_score"]) for item in scenario_results]
        ),
        "hard_negative_win_rate": round_rate(
            sum(1 for item in positive_results if item["hard_negative_pass"]),
            len(positive_results),
        ),
        "cold_build_ms": round(cold_ms, 2),
        "warm_reuse_ms": round(warm_ms, 2),
        "cold_index_mode": cold_manifest.get("index_mode"),
        "warm_index_mode": warm_manifest.get("index_mode"),
        "candidate_run_count": warm_manifest.get("candidate_run_count"),
        "scenario_results": scenario_results,
    }
    return retrieval


def load_replay_corpus(corpus_path: pathlib.Path) -> list[dict[str, Any]]:
    payload = load_json(corpus_path)
    if not isinstance(payload, list):
        raise ValueError("replay benchmark corpus must be a JSON array")
    return [item for item in payload if isinstance(item, dict)]


def replay_scenario_for_record(record: dict[str, Any]) -> tuple[dict[str, Any], str]:
    labels = {str(item) for item in record.get("benchmark_labels", []) if str(item)}
    if "retry_recovered" in labels or "retry" in labels:
        return {"scenario": "startup_fail_once"}, "python3 ../tests/fixtures/pass_eval.py"
    if "result_invalid" in labels:
        return {"scenario": "invalid_result"}, "python3 ../tests/fixtures/pass_eval.py"
    if "auth_failure" in labels:
        return {"scenario": "auth_failure"}, "python3 ../tests/fixtures/pass_eval.py"
    if "timeout" in labels or "deadline_exceeded" in labels:
        return {"scenario": "partial_transcript_hang", "sleep_seconds": 2}, (
            "python3 ../tests/fixtures/pass_eval.py"
        )
    if "model_invocation_failed" in labels:
        return {"scenario": "startup_failure"}, "python3 ../tests/fixtures/pass_eval.py"
    if "contract_invalid" in labels:
        return {"scenario": "happy_path"}, "python3 -c 'print(1)'"
    if "eval_failed" in labels:
        return {"scenario": "happy_path"}, "python3 ../tests/fixtures/fail_eval.py"
    if "transcript_flood" in labels:
        return {"scenario": "transcript_flood", "event_count": 1024}, (
            "python3 ../tests/fixtures/pass_eval.py"
        )
    return {"scenario": "happy_path"}, "python3 ../tests/fixtures/pass_eval.py"


def write_replay_task(
    task_path: pathlib.Path,
    *,
    schema_text: str,
    title: str,
    goal: str,
    eval_command: str,
) -> None:
    write_task(
        task_path,
        schema_text=schema_text,
        title=title,
        goal=goal,
        constraints=["Stay local.", "Use the replay evidence only as a representative workload."],
        done=["Score is written.", "outputs/run_manifest.json is written."],
    )
    set_eval_command(task_path, eval_command)


def prepare_replay_runs(
    harness_root: pathlib.Path,
    corpus: list[dict[str, Any]],
    *,
    max_runs: int,
) -> list[dict[str, Any]]:
    runs_root = harness_root / "runs"
    schema_text = (harness_root / "result.schema.json").read_text(encoding="utf-8").rstrip()
    selected_records = corpus[: max(1, min(max_runs, len(corpus)))]
    prepared: list[dict[str, Any]] = []
    for index, record in enumerate(selected_records):
        run_dir = create_benchmark_run(harness_root, f"replay workload {index + 1}")
        scenario_payload, eval_command = replay_scenario_for_record(record)
        evidence = record.get("evidence", {}) if isinstance(record.get("evidence"), dict) else {}
        task_excerpt = evidence.get("task_excerpt", [])
        goal = "Replay representative production-like evidence."
        if isinstance(task_excerpt, list):
            goal = "\n".join(str(line) for line in task_excerpt[:3] if str(line).strip()) or goal
        write_replay_task(
            run_dir / "task.md",
            schema_text=schema_text,
            title=f"Replay benchmark {record.get('run_id', index + 1)}",
            goal=goal,
            eval_command=eval_command,
        )
        (run_dir / ".fake-pi-scenario.json").write_text(
            json.dumps(scenario_payload, indent=2) + "\n",
            encoding="utf-8",
        )
        prepared.append(
            {
                "run_dir": run_dir,
                "record": record,
                "scenario": scenario_payload["scenario"],
            }
        )
    assert runs_root.exists()
    return prepared


def run_orchestrator_benchmark(
    harness_root: pathlib.Path,
    fake_pi: pathlib.Path,
    *,
    max_model_workers: int,
    max_score_workers: int,
) -> dict[str, Any]:
    runs_root = harness_root / "runs"
    benchmark_duration_seconds = max(
        2,
        int(os.environ.get("HARNESS_BENCHMARK_ORCHESTRATOR_DURATION_SECONDS", "5")),
    )
    env = os.environ | {
        "PYTHONPATH": str(harness_root / "bin"),
        "HARNESS_PI_BIN": str(fake_pi),
        "HARNESS_ORCHESTRATOR_MODEL_TIMEOUT_SECONDS": "1",
        "HARNESS_ORCHESTRATOR_SCORE_TIMEOUT_SECONDS": "30",
    }
    started = time.perf_counter()
    subprocess.run(
        [
            sys.executable,
            str(harness_root / "bin" / "orchestrator.py"),
            "--runs-root",
            str(runs_root),
            "--max-model-workers",
            str(max_model_workers),
            "--max-score-workers",
            str(max_score_workers),
            "--duration-seconds",
            str(benchmark_duration_seconds),
        ],
        cwd=str(harness_root),
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    metrics = harvest(runs_root, window_days=1)
    metrics["duration_ms"]["wall_clock"] = round((time.perf_counter() - started) * 1000.0, 2)
    return metrics


def append_history_snapshot(
    history_dir: pathlib.Path,
    *,
    stem: str,
    payload: dict[str, Any],
    row: dict[str, Any],
) -> None:
    history_dir.mkdir(parents=True, exist_ok=True)
    history_path = history_dir / f"{stem}.history.jsonl"
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")
    snapshot_index = len(history_path.read_text(encoding="utf-8").splitlines())
    snapshot_path = history_dir / f"{stem}-{snapshot_index}.summary.json"
    snapshot_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def benchmark_replay(
    harness_root: pathlib.Path,
    fake_pi: pathlib.Path,
    *,
    corpus_path: pathlib.Path,
    max_runs: int,
    history_dir: pathlib.Path | None = None,
) -> dict[str, Any]:
    corpus = load_replay_corpus(corpus_path)
    sampled = corpus[: max(1, min(max_runs, len(corpus)))]
    workload_metrics: list[dict[str, Any]] = []

    for concurrency in (1, 2):
        clear_runs_dir(harness_root / "runs")
        prepared = prepare_replay_runs(harness_root, sampled, max_runs=max_runs)
        metrics = run_orchestrator_benchmark(
            harness_root,
            fake_pi,
            max_model_workers=concurrency,
            max_score_workers=concurrency,
        )
        retry_records = [
            item
            for item in prepared
            if "retry" in {str(label) for label in item["record"].get("benchmark_labels", [])}
            or item["scenario"] == "startup_fail_once"
        ]
        retry_successes = 0
        for item in retry_records:
            score_payload = load_json(item["run_dir"] / "score.json")
            if score_payload.get("overall_pass") is True:
                retry_successes += 1
        terminal_total = (
            metrics["totals"]["complete"] + metrics["totals"]["cancelled"] + metrics["totals"]["failed"]
        )
        workload_metrics.append(
            {
                "concurrency": concurrency,
                "sampled_runs": len(prepared),
                "completion_rate": round(
                    terminal_total / max(1, metrics["totals"]["total_runs"]),
                    2,
                ),
                "pass_rate_percent": metrics["pass_rate_percent"],
                "timeout_rate": round(
                    metrics["failure_classification_counts"].get("deadline_exceeded", 0)
                    / max(1, metrics["totals"]["total_runs"]),
                    2,
                ),
                "retry_recovery_rate": round(
                    retry_successes / max(1, len(retry_records)),
                    2,
                ),
                "queue_saturation_events": metrics["queue_saturation"]["events_total"],
                "p95_duration_ms": metrics["duration_ms"]["p95"],
                "p99_duration_ms": metrics["duration_ms"]["p99"],
                "wall_clock_ms": metrics["duration_ms"]["wall_clock"],
            }
        )

    payload = {
        "corpus_size": len(corpus),
        "sampled_run_count": len(sampled),
        "source_corpus_path": str(corpus_path),
        "workload_metrics": workload_metrics,
    }
    if history_dir is not None and workload_metrics:
        latest = workload_metrics[-1]
        append_history_snapshot(
            history_dir,
            stem="replay-benchmark",
            payload=payload,
            row={
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "sampled_run_count": payload["sampled_run_count"],
                "pass_rate_percent": latest["pass_rate_percent"],
                "queue_saturation_events": latest["queue_saturation_events"],
                "p95_duration_ms": latest["p95_duration_ms"],
                "p99_duration_ms": latest["p99_duration_ms"],
            },
        )
    return payload


def benchmark_fault_injection(
    harness_root: pathlib.Path,
    fake_pi: pathlib.Path,
    *,
    sample_count: int,
    seed: int,
    corpus_out: pathlib.Path | None = None,
) -> dict[str, Any]:
    schema_text = (harness_root / "result.schema.json").read_text(encoding="utf-8").rstrip()
    rng = random.Random(seed)
    cases = [
        ("invalid_result", {"scenario": "invalid_result"}, "python3 ../tests/fixtures/pass_eval.py"),
        ("startup_failure", {"scenario": "startup_failure"}, "python3 ../tests/fixtures/pass_eval.py"),
        ("auth_failure", {"scenario": "auth_failure"}, "python3 ../tests/fixtures/pass_eval.py"),
        (
            "partial_transcript_hang",
            {"scenario": "partial_transcript_hang", "sleep_seconds": 2},
            "python3 ../tests/fixtures/pass_eval.py",
        ),
        ("permission_denied", {"scenario": "permission_denied"}, "python3 ../tests/fixtures/pass_eval.py"),
        ("transcript_flood", {"scenario": "transcript_flood", "event_count": 2048}, "python3 ../tests/fixtures/pass_eval.py"),
        ("contract_invalid", {"scenario": "happy_path"}, "python3 -c 'print(1)'"),
    ]
    selected = [cases[rng.randrange(len(cases))] for _ in range(max(1, sample_count))]
    clear_runs_dir(harness_root / "runs")

    results: list[dict[str, Any]] = []
    novel_failure_records: list[dict[str, Any]] = []
    observed_failures: set[str] = set()
    env = os.environ | {
        "PYTHONPATH": str(harness_root / "bin"),
        "HARNESS_PI_BIN": str(fake_pi),
        "HARNESS_MODEL_TIMEOUT_SECONDS": "1",
        "HARNESS_MAX_TRANSCRIPT_BYTES": "4096",
    }

    for index, case in enumerate(selected):
        case_name, scenario_payload, eval_command = case
        run_dir = create_benchmark_run(harness_root, f"fault injection {index + 1}")
        write_replay_task(
            run_dir / "task.md",
            schema_text=schema_text,
            title=f"Generated fault {case_name}",
            goal=f"Exercise generated fault case {case_name}.",
            eval_command=eval_command,
        )
        (run_dir / ".fake-pi-scenario.json").write_text(
            json.dumps(scenario_payload, indent=2) + "\n",
            encoding="utf-8",
        )
        completed = subprocess.run(
            [str(harness_root / "bin" / "run-task.sh"), str(run_dir)],
            cwd=str(harness_root),
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )
        score_payload = load_json(run_dir / "score.json")
        manifest = load_json(run_dir / "outputs" / "run_manifest.json")
        failures = sorted(
            {
                str(item)
                for item in (
                    (score_payload.get("failure_classifications") or [])
                    + (manifest.get("failure_classifications") or [])
                )
                if str(item)
            }
        )
        novel = [item for item in failures if item not in observed_failures]
        observed_failures.update(failures)
        result = {
            "case": case_name,
            "scenario": scenario_payload["scenario"],
            "exit_code": completed.returncode,
            "failures": failures,
            "novel_failures": novel,
        }
        results.append(result)
        if novel:
            novel_failure_records.append(
                {
                    "record_version": "v1",
                    "run_id": run_dir.name,
                    "source_label": "generated_fault_injection",
                    "benchmark_labels": failures or [case_name],
                    "metadata": {
                        "generated_case": case_name,
                        "overall_pass": score_payload.get("overall_pass"),
                    },
                    "evidence": {
                        "manifest": manifest,
                        "score": score_payload,
                        "event_excerpt": excerpt_lines(run_dir / "run-events.jsonl", line_limit=10),
                        "transcript_excerpt": excerpt_lines(run_dir / "transcript.jsonl", line_limit=10),
                        "stderr_excerpt": excerpt_lines(run_dir / "pi.stderr.log", line_limit=10),
                    },
                }
            )

    if corpus_out is not None:
        corpus_out.parent.mkdir(parents=True, exist_ok=True)
        corpus_out.write_text(json.dumps(novel_failure_records, indent=2) + "\n", encoding="utf-8")

    return {
        "sample_count": sample_count,
        "seed": seed,
        "cases": results,
        "novel_failure_records": len(novel_failure_records),
        "fault_corpus_path": str(corpus_out) if corpus_out is not None else None,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Bitterless Harness")
    parser.add_argument(
        "--mode",
        choices=["all", "run-task", "retrieval", "replay", "fault-injection"],
        default="all",
        help="which benchmark set to run",
    )
    parser.add_argument(
        "--profile-path",
        help="override the retrieval profile used for retrieval benchmarks",
    )
    parser.add_argument(
        "--replay-corpus",
        help="path to a replay corpus JSON file",
    )
    parser.add_argument(
        "--replay-runs",
        type=int,
        default=6,
        help="maximum number of replay records to exercise",
    )
    parser.add_argument(
        "--fault-samples",
        type=int,
        default=6,
        help="number of generated fault-injection cases to execute",
    )
    parser.add_argument(
        "--fault-seed",
        type=int,
        default=7,
        help="seed for generated fault-injection sampling",
    )
    parser.add_argument(
        "--fault-corpus-out",
        help="optional output path for novel generated failure records",
    )
    parser.add_argument(
        "--history-dir",
        help="optional directory for replay benchmark history snapshots",
    )
    parser.add_argument(
        "--out",
        help="optional path to write the benchmark payload JSON",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    payload: dict[str, object] = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "backpressure_policy": {
            "disk_used_threshold_percent": int(
                os.environ.get("HARNESS_DISK_USED_THRESHOLD_PERCENT", "90")
            ),
            "free_mb_threshold": int(os.environ.get("HARNESS_FREE_MB_THRESHOLD", "512")),
        },
    }
    profile_path = pathlib.Path(args.profile_path).resolve() if args.profile_path else None
    replay_corpus_path = pathlib.Path(args.replay_corpus).resolve() if args.replay_corpus else None
    history_dir = pathlib.Path(args.history_dir).resolve() if args.history_dir else None
    fault_corpus_out = pathlib.Path(args.fault_corpus_out).resolve() if args.fault_corpus_out else None
    with tempfile.TemporaryDirectory(prefix="bitterless-bench-") as tmp_dir_name:
        tmp_dir = pathlib.Path(tmp_dir_name)
        harness_root = tmp_dir / "starter"
        temp_tests_root = tmp_dir / "tests"
        shutil.copytree(
            repo_root / "starter",
            harness_root,
            ignore=shutil.ignore_patterns("runs"),
        )
        shutil.copytree(repo_root / "tests", temp_tests_root)
        fake_pi = temp_tests_root / "fixtures" / "fake_pi.py"
        if args.mode in {"all", "run-task"}:
            payload["run_task"] = benchmark_run_task(harness_root, fake_pi)
        if args.mode in {"all", "retrieval"}:
            payload["retrieval"] = benchmark_retrieval(
                harness_root,
                profile_path=profile_path,
            )
        if args.mode in {"all", "replay"}:
            if replay_corpus_path is None:
                raise ValueError("--replay-corpus is required for replay mode")
            payload["replay"] = benchmark_replay(
                harness_root,
                fake_pi,
                corpus_path=replay_corpus_path,
                max_runs=max(1, args.replay_runs),
                history_dir=history_dir,
            )
        if args.mode in {"all", "fault-injection"}:
            payload["fault_injection"] = benchmark_fault_injection(
                harness_root,
                fake_pi,
                sample_count=max(1, args.fault_samples),
                seed=args.fault_seed,
                corpus_out=fault_corpus_out,
            )

    serialized = json.dumps(payload, indent=2)
    if args.out:
        output_path = pathlib.Path(args.out).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(serialized + "\n", encoding="utf-8")
    print(serialized)


if __name__ == "__main__":
    main()
