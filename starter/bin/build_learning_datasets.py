#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

from build_replay_corpus import excerpt_lines, redact_text
from harnesslib import now_utc, parse_task_file, sha256_text
from learninglib import dataset_manifest_entry, write_jsonl
from retrieval_index import build_index_entry


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Bitterless learning datasets from runs")
    parser.add_argument("--runs-root", default=None)
    parser.add_argument("--out-root", required=True)
    parser.add_argument("--transcript-lines", type=int, default=40)
    parser.add_argument("--event-lines", type=int, default=60)
    return parser.parse_args(argv)


def script_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def default_runs_root() -> pathlib.Path:
    return script_root() / "runs"


def read_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def read_jsonl(path: pathlib.Path, *, limit: int | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payloads: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if limit is not None and len(payloads) >= limit:
            break
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            payload = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads


def run_dirs(runs_root: pathlib.Path) -> list[pathlib.Path]:
    return sorted(
        [
            path
            for path in runs_root.iterdir()
            if path.is_dir() and not path.name.startswith(".") and path.name != "recovery"
        ],
        key=lambda path: path.name,
    )


def input_fingerprint(run_dir: pathlib.Path, manifest: dict[str, Any]) -> str:
    snapshots = manifest.get("snapshots", {}) if isinstance(manifest.get("snapshots"), dict) else {}
    payload = {
        "run_id": run_dir.name,
        "task_sha256": snapshots.get("task_sha256"),
        "run_contract_sha256": snapshots.get("run_contract_sha256"),
        "result_schema_sha256": snapshots.get("result_schema_sha256"),
    }
    return sha256_text(json.dumps(payload, sort_keys=True, ensure_ascii=False))


def task_payload(run_dir: pathlib.Path) -> dict[str, Any]:
    task_path = run_dir / "task.md"
    parsed = parse_task_file(task_path) if task_path.exists() else {"ok": False, "errors": []}
    task_text = task_path.read_text(encoding="utf-8") if task_path.exists() else ""
    return {
        "path": str(task_path),
        "task_title": parsed.get("task_title"),
        "sections": dict(parsed.get("sections", {})),
        "required_artifacts": list(parsed.get("required_artifacts", [])),
        "eval_commands": list(parsed.get("eval_commands", [])),
        "task_excerpt": [redact_text(line) for line in task_text.splitlines()[:24]],
        "parse_ok": parsed.get("ok", False),
        "parse_errors": list(parsed.get("errors", [])),
    }


def patch_stats(run_dir: pathlib.Path) -> dict[str, Any]:
    patch_path = run_dir / "patch.diff"
    if not patch_path.exists():
        return {"files_changed": 0, "added_lines": 0, "removed_lines": 0}
    files_changed = 0
    added_lines = 0
    removed_lines = 0
    for line in patch_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("diff --git "):
            files_changed += 1
        elif line.startswith("+") and not line.startswith("+++"):
            added_lines += 1
        elif line.startswith("-") and not line.startswith("---"):
            removed_lines += 1
    return {
        "files_changed": files_changed,
        "added_lines": added_lines,
        "removed_lines": removed_lines,
    }


def clean_run(run_dir: pathlib.Path, score: dict[str, Any]) -> bool:
    if not score:
        return False
    if score.get("result_json_valid_schema") is False:
        return False
    secret_scan = score.get("secret_scan", {})
    return not isinstance(secret_scan, dict) or not secret_scan.get("findings")


def build_trajectory_record(
    run_dir: pathlib.Path,
    manifest: dict[str, Any],
    score: dict[str, Any],
    *,
    transcript_lines: int,
    event_lines: int,
) -> dict[str, Any]:
    return {
        "trajectory_record_version": "v1",
        "generated_at": now_utc(),
        "run_id": run_dir.name,
        "input_fingerprint": input_fingerprint(run_dir, manifest),
        "overall_pass": bool(score.get("overall_pass", False)),
        "failure_classifications": list(score.get("failure_classifications", [])),
        "task": task_payload(run_dir),
        "context": {
            "manifest": read_json(run_dir / "context" / "retrieval-manifest.json"),
            "summary_excerpt": excerpt_lines(
                run_dir / "context" / "retrieval-summary.md",
                line_limit=20,
            ),
            "source_run_ids": list(
                (read_json(run_dir / "context" / "retrieval-manifest.json") or {}).get(
                    "selected_source_run_ids", []
                )
            ),
        },
        "events": read_jsonl(run_dir / "run-events.jsonl", limit=event_lines),
        "transcript_excerpt": excerpt_lines(
            run_dir / "transcript.jsonl",
            line_limit=transcript_lines,
        ),
        "score": score,
        "patch_stats": patch_stats(run_dir),
    }


def build_retrieval_example(
    run_dir: pathlib.Path,
    manifest: dict[str, Any],
    score: dict[str, Any],
) -> dict[str, Any] | None:
    context_manifest = read_json(run_dir / "context" / "retrieval-manifest.json")
    if not context_manifest:
        return None
    query = task_payload(run_dir)
    top_candidates = list(context_manifest.get("top_candidates", []))
    gold_source_run_ids = list(context_manifest.get("selected_source_run_ids", []))
    hard_negative_run_ids = [
        str(item.get("run_id"))
        for item in top_candidates
        if item.get("selected") is False and item.get("run_id")
    ]
    return {
        "retrieval_example_version": "v1",
        "generated_at": now_utc(),
        "example_id": f"run:{run_dir.name}",
        "query": {
            "task_title": context_manifest.get("query", {}).get("task_title", query.get("task_title")),
            "goal": context_manifest.get("query", {}).get(
                "goal", query.get("sections", {}).get("Goal", "")
            ),
            "constraints": context_manifest.get("query", {}).get(
                "constraints", query.get("sections", {}).get("Constraints", "")
            ),
            "done": context_manifest.get("query", {}).get(
                "done", query.get("sections", {}).get("Done", "")
            ),
            "text": context_manifest.get("query", {}).get("text", ""),
        },
        "candidate_set": [
            {
                **candidate,
                "document": {
                    "text": str(candidate.get("document_text", "")),
                    "summary": str(candidate.get("summary", "")),
                    "claims": list(candidate.get("claims", [])),
                    "evidence_paths": list(candidate.get("evidence_paths", [])),
                    "artifact_records": list(candidate.get("artifact_records", [])),
                },
            }
            for candidate in top_candidates
            if isinstance(candidate, dict)
        ],
        "gold_source_run_ids": gold_source_run_ids,
        "hard_negative_run_ids": hard_negative_run_ids,
        "abstention_label": bool(context_manifest.get("empty_context", False)),
        "usefulness_label": bool(score.get("overall_pass", False))
        and len(gold_source_run_ids) > 0
        and not bool(context_manifest.get("abstained", False)),
        "source": {
            "run_id": run_dir.name,
            "context_manifest_path": str(run_dir / "context" / "retrieval-manifest.json"),
        },
    }


def build_retrieval_document(run_dir: pathlib.Path) -> dict[str, Any] | None:
    entry = build_index_entry(run_dir)
    if not entry.get("eligible"):
        return None
    retrieval_view = dict(entry.get("retrieval_view", {}))
    query_sections = {
        "task_title": str(retrieval_view.get("task_title", "")),
        "goal": str(retrieval_view.get("goal", "")),
        "constraints": str(retrieval_view.get("constraints", "")),
        "done": str(retrieval_view.get("done", "")),
    }
    return {
        "retrieval_document_version": "v1",
        "generated_at": now_utc(),
        "run_id": run_dir.name,
        "text": str(retrieval_view.get("text", "")),
        "query_sections": query_sections,
        "summary": str(entry.get("summary", "")),
        "claims": list(entry.get("claims", [])),
        "evidence_paths": list(retrieval_view.get("evidence_paths", [])),
        "artifact_records": list(entry.get("artifact_records", [])),
        "quality": dict(entry.get("quality", {})),
        "source_snapshot_fingerprint": str(entry.get("source_snapshot_fingerprint", "")),
    }


def build_policy_example(
    run_dir: pathlib.Path,
    manifest: dict[str, Any],
    score: dict[str, Any],
) -> dict[str, Any]:
    retrieval = score.get("retrieval", {}) if isinstance(score.get("retrieval"), dict) else {}
    context_manifest = read_json(run_dir / "context" / "retrieval-manifest.json")
    task = task_payload(run_dir)
    timings = manifest.get("timings", {}) if isinstance(manifest.get("timings"), dict) else {}
    candidates = (
        manifest.get("candidates", {})
        if isinstance(manifest.get("candidates"), dict)
        else {}
    )
    return {
        "policy_example_version": "v1",
        "generated_at": now_utc(),
        "example_id": f"run:{run_dir.name}",
        "features": {
            "execution_profile": score.get("execution_profile"),
            "policy_path": score.get("policy_path"),
            "eval_command_count": len(task.get("eval_commands", [])),
            "required_artifact_count": len(task.get("required_artifacts", [])),
            "selected_source_count": retrieval.get("selected_source_count"),
            "abstained": retrieval.get("abstained"),
            "candidate_run_count": retrieval.get("candidate_run_count"),
            "duration_ms": timings.get("run_duration_ms"),
            "context_empty": context_manifest.get("empty_context"),
            "failure_classification_count": len(score.get("failure_classifications", [])),
            "task_text": "\n".join(
                [
                    str(task.get("task_title", "")),
                    str(task.get("sections", {}).get("Goal", "")),
                    str(task.get("sections", {}).get("Constraints", "")),
                    str(task.get("sections", {}).get("Done", "")),
                ]
            ).strip(),
            "top_candidate_score": retrieval.get("top_candidate_score"),
            "ranking_latency_ms": retrieval.get("ranking_latency_ms"),
        },
        "labels": {
            "overall_pass": bool(score.get("overall_pass", False)),
            "execution_profile": score.get("execution_profile"),
            "retry_recommended": bool(
                "model_invocation_failed" in set(score.get("failure_classifications", []))
            ),
            "attempt_recovery": bool((run_dir / "recovery").exists()),
            "benchmark_eligible": bool((score.get("benchmark_eligibility") or {}).get("eligible")),
            "retrieval_budget": {
                "selected_source_count": retrieval.get("selected_source_count", 0),
                "candidate_run_count": retrieval.get("candidate_run_count", 0),
            },
            "capability_profile": (
                list((score.get("capabilities") or {}).get("spawned_profile_ids", []))[0]
                if list((score.get("capabilities") or {}).get("spawned_profile_ids", []))
                else None
            ),
        },
        "candidate_context": candidates,
        "observed": {
            "task": {
                "title": task.get("task_title"),
                "goal": task.get("sections", {}).get("Goal", ""),
                "constraints": task.get("sections", {}).get("Constraints", ""),
                "done": task.get("sections", {}).get("Done", ""),
            },
            "retrieval": retrieval,
            "timings": timings,
            "failure_classifications": list(score.get("failure_classifications", [])),
            "capabilities": score.get("capabilities", {}),
        },
    }


def build_model_example(
    run_dir: pathlib.Path,
    manifest: dict[str, Any],
    score: dict[str, Any],
    *,
    transcript_lines: int,
) -> dict[str, Any] | None:
    if score.get("overall_pass") is not True:
        return None
    result_payload = read_json(run_dir / "result.json")
    if not result_payload:
        return None
    context_manifest = read_json(run_dir / "context" / "retrieval-manifest.json")
    return {
        "model_example_version": "v1",
        "generated_at": now_utc(),
        "example_id": f"run:{run_dir.name}",
        "task": task_payload(run_dir),
        "context": {
            "summary_excerpt": excerpt_lines(
                run_dir / "context" / "retrieval-summary.md",
                line_limit=20,
            ),
            "selected_source_run_ids": list(context_manifest.get("selected_source_run_ids", [])),
        },
        "trajectory": {
            "events": read_jsonl(run_dir / "run-events.jsonl", limit=20),
            "transcript_excerpt": excerpt_lines(
                run_dir / "transcript.jsonl",
                line_limit=transcript_lines,
            ),
        },
        "target": result_payload,
    }


def dedupe_rows(rows: list[dict[str, Any]], *, key_fields: list[str]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        key = "|".join(str(row.get(field, "")) for field in key_fields)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    deduped.sort(key=lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False))
    return deduped


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    runs_root = pathlib.Path(args.runs_root).resolve() if args.runs_root else default_runs_root()
    out_root = pathlib.Path(args.out_root).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    trajectory_rows: list[dict[str, Any]] = []
    retrieval_rows: list[dict[str, Any]] = []
    retrieval_document_rows: list[dict[str, Any]] = []
    policy_rows: list[dict[str, Any]] = []
    model_rows: list[dict[str, Any]] = []

    for run_dir in run_dirs(runs_root):
        manifest = read_json(run_dir / "outputs" / "run_manifest.json")
        score = read_json(run_dir / "score.json")
        if not manifest or not clean_run(run_dir, score):
            continue
        trajectory_rows.append(
            build_trajectory_record(
                run_dir,
                manifest,
                score,
                transcript_lines=max(1, args.transcript_lines),
                event_lines=max(1, args.event_lines),
            )
        )
        retrieval_row = build_retrieval_example(run_dir, manifest, score)
        if retrieval_row is not None:
            retrieval_rows.append(retrieval_row)
        retrieval_document_row = build_retrieval_document(run_dir)
        if retrieval_document_row is not None:
            retrieval_document_rows.append(retrieval_document_row)
        policy_rows.append(build_policy_example(run_dir, manifest, score))
        model_row = build_model_example(
            run_dir,
            manifest,
            score,
            transcript_lines=max(1, args.transcript_lines),
        )
        if model_row is not None:
            model_rows.append(model_row)

    trajectory_rows = dedupe_rows(trajectory_rows, key_fields=["run_id", "input_fingerprint"])
    retrieval_rows = dedupe_rows(retrieval_rows, key_fields=["example_id"])
    retrieval_document_rows = dedupe_rows(
        retrieval_document_rows,
        key_fields=["run_id", "source_snapshot_fingerprint"],
    )
    policy_rows = dedupe_rows(policy_rows, key_fields=["example_id"])
    model_rows = dedupe_rows(model_rows, key_fields=["example_id"])

    outputs = {
        "trajectory_records": out_root / "trajectory-records.jsonl",
        "retrieval_examples": out_root / "retrieval-examples.jsonl",
        "retrieval_documents": out_root / "retrieval-documents.jsonl",
        "policy_examples": out_root / "policy-examples.jsonl",
        "model_examples": out_root / "model-examples.jsonl",
    }
    write_jsonl(outputs["trajectory_records"], trajectory_rows)
    write_jsonl(outputs["retrieval_examples"], retrieval_rows)
    write_jsonl(outputs["retrieval_documents"], retrieval_document_rows)
    write_jsonl(outputs["policy_examples"], policy_rows)
    write_jsonl(outputs["model_examples"], model_rows)

    manifest = {
        "learning_dataset_manifest_version": "v1",
        "generated_at": now_utc(),
        "runs_root": str(runs_root),
        "datasets": {
            name: {
                **dataset_manifest_entry(path),
                "row_count": len(
                    {
                        "trajectory_records": trajectory_rows,
                        "retrieval_examples": retrieval_rows,
                        "retrieval_documents": retrieval_document_rows,
                        "policy_examples": policy_rows,
                        "model_examples": model_rows,
                    }[name]
                ),
            }
            for name, path in outputs.items()
        },
    }
    (out_root / "learning-datasets.manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
