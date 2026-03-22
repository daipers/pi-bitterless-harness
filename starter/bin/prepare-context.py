#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import shutil
import sys
import time
from typing import Any

from harnesslib import (
    evaluate_required_artifact_path,
    load_policy,
    load_run_contract,
    parse_task_file,
    resolve_execution_settings,
    sha256_file,
    sha256_text,
    write_json,
)
from retrieval_index import (
    build_query,
    build_query_text,
    lexical_score,
    runs_root,
    score_index_entry,
    sync_retrieval_index,
)

SKIPPED_TOP_LEVEL = {"home", "session", "recovery"}
SKIPPED_FILES = {
    "transcript.jsonl",
    "pi.stderr.log",
    "patch.diff",
    "git.status.txt",
    "pi.exit_code.txt",
    "run-events.jsonl",
}


def usage() -> int:
    print("usage: prepare-context.py /path/to/run-dir [policy-path]", file=sys.stderr)
    return 2


def is_utf8_text(path: pathlib.Path) -> bool:
    try:
        path.read_text(encoding="utf-8")
    except Exception:
        return False
    return True


def eligible_artifact_source_path(
    source_run_dir: pathlib.Path,
    rel_path: str,
    *,
    max_bytes: int,
) -> pathlib.Path | None:
    validated = evaluate_required_artifact_path(source_run_dir, rel_path)
    if not validated["valid"]:
        return None
    source_path = (source_run_dir / rel_path).resolve()
    if not source_path.exists() or not source_path.is_file():
        return None
    if source_path.name in SKIPPED_FILES:
        return None
    if any(part in SKIPPED_TOP_LEVEL for part in pathlib.Path(rel_path).parts):
        return None
    try:
        if source_path.stat().st_size > max_bytes:
            return None
    except OSError:
        return None
    if not is_utf8_text(source_path):
        return None
    return source_path


def _copy_file(
    *,
    source_path: pathlib.Path,
    source_rel_path: str,
    destination_root: pathlib.Path,
    copy_reason: str,
) -> dict[str, Any]:
    destination_path = destination_root / source_rel_path
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_path, destination_path)
    return {
        "source_path": source_rel_path,
        "destination_path": str(destination_path),
        "sha256": sha256_file(destination_path),
        "copy_reason": copy_reason,
        "bytes_copied": source_path.stat().st_size,
    }


def artifact_relevance_score(query: dict[str, Any], artifact: dict[str, Any]) -> int:
    description_score = lexical_score(
        query["candidate_tokens"],
        dict(artifact.get("description_tokens", {})),
    )
    excerpt_score = lexical_score(
        query["candidate_tokens"],
        dict(artifact.get("excerpt_tokens", {})),
    )
    return description_score + excerpt_score


def copy_context_artifacts(
    *,
    query: dict[str, Any],
    source_run_dir: pathlib.Path,
    destination_root: pathlib.Path,
    artifact_records: list[dict[str, Any]],
    max_artifacts: int,
    max_bytes: int,
) -> list[dict[str, Any]]:
    copied: list[dict[str, Any]] = []
    seen_paths: set[str] = set()

    evidence_candidates: list[tuple[int, str, dict[str, Any]]] = []
    fallback_candidates: list[tuple[int, str, dict[str, Any]]] = []
    for artifact in artifact_records:
        rel_path = str(artifact.get("path", ""))
        if not rel_path or rel_path in seen_paths:
            continue
        source_path = eligible_artifact_source_path(source_run_dir, rel_path, max_bytes=max_bytes)
        if source_path is None:
            continue
        relevance = artifact_relevance_score(query, artifact)
        target = evidence_candidates if artifact.get("evidence_linked") else fallback_candidates
        target.append((relevance, rel_path, artifact | {"source_path_obj": source_path}))

    evidence_candidates.sort(key=lambda item: (-item[0], item[1]))
    fallback_candidates.sort(key=lambda item: (-item[0], item[1]))

    for _, rel_path, artifact in evidence_candidates + fallback_candidates:
        if len(copied) >= max_artifacts:
            break
        if rel_path in seen_paths:
            continue
        seen_paths.add(rel_path)
        copied.append(
            _copy_file(
                source_path=artifact["source_path_obj"],
                source_rel_path=rel_path,
                destination_root=destination_root,
                copy_reason="claim_evidence" if artifact.get("evidence_linked") else "artifact_relevance",
            )
        )
    return copied


def build_summary(selected_sources: list[dict[str, Any]]) -> str:
    lines = [
        "# Retrieved Context",
        "",
        f"Selected {len(selected_sources)} prior successful run(s).",
    ]
    if not selected_sources:
        lines.extend(["", "No relevant prior runs were selected."])
        return "\n".join(lines) + "\n"

    for item in selected_sources:
        lines.extend(
            [
                "",
                f"## {item['run_id']}",
                f"- Retrieval score: {item['total_score']}",
                f"- Summary: {item['summary'] or 'No summary recorded.'}",
            ]
        )
        claims = item.get("claims", [])
        if claims:
            lines.append("- Claims: " + "; ".join(claims[:3]))
        copied_files = item.get("copied_files", [])
        if copied_files:
            lines.append(
                "- Copied files: "
                + ", ".join(pathlib.Path(entry["destination_path"]).name for entry in copied_files)
            )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) not in {1, 2}:
        return usage()

    run_dir = pathlib.Path(args[0]).resolve()
    policy = load_policy(args[1]) if len(args) == 2 else None
    run_contract = load_run_contract(run_dir / "run.contract.json")
    settings = resolve_execution_settings(run_contract)
    parsed_task = parse_task_file(run_dir / "task.md", eval_policy=policy)
    if not parsed_task["ok"]:
        raise SystemExit("; ".join(parsed_task["errors"]))

    retrieval = settings["retrieval"]
    context_dir = run_dir / settings["context_dir"]
    if context_dir.exists():
        shutil.rmtree(context_dir)
    context_dir.mkdir(parents=True, exist_ok=True)

    query_text = build_query_text(parsed_task)
    query = build_query(parsed_task)
    selected_sources: list[dict[str, Any]] = []
    index_state = sync_retrieval_index(
        runs_root(run_dir),
        exclude_run_id=run_dir.name,
        eval_policy=policy,
    )
    indexed_entries = index_state["entries"]
    skipped_sources = sum(1 for entry in indexed_entries if not entry.get("eligible"))
    eligible_run_count = sum(1 for entry in indexed_entries if entry.get("eligible"))

    ranking_started = time.perf_counter()
    stage1_candidates: list[dict[str, Any]] = []
    for entry in indexed_entries:
        if not entry.get("eligible"):
            continue
        scored = score_index_entry(query, entry)
        if scored["stage1_score"] <= 0:
            continue
        stage1_candidates.append(
            {
                **scored,
                "summary": str(entry.get("summary", "")),
                "claims": list(entry.get("claims", [])),
                "artifact_records": list(entry.get("artifact_records", [])),
                "source_run_dir": pathlib.Path(str(entry["source_run_path"])),
            }
        )

    stage1_candidates.sort(key=lambda item: (-item["stage1_score"], item["run_id"]))
    rerank_pool = stage1_candidates[: int(retrieval["max_candidates"])]
    rerank_pool.sort(
        key=lambda item: (
            -item["total_score"],
            -item["stage1_score"],
            item["run_id"],
        )
    )
    ranking_latency_ms = round((time.perf_counter() - ranking_started) * 1000.0, 2)
    selected_sources = rerank_pool[: int(retrieval["max_source_runs"])]
    selected_run_ids = [item["run_id"] for item in selected_sources]

    selected_manifest_entries: list[dict[str, Any]] = []
    copied_artifact_bytes = 0
    top_candidates = [
        {
            "run_id": item["run_id"],
            "total_score": item["total_score"],
            "score_breakdown": dict(item["score_breakdown"]),
            "selected": item["run_id"] in selected_run_ids,
        }
        for item in rerank_pool
    ]

    for item in selected_sources:
        source_run_dir = item["source_run_dir"]
        destination_root = context_dir / "source-runs" / item["run_id"]
        destination_root.mkdir(parents=True, exist_ok=True)
        copied_core_files: list[dict[str, Any]] = []
        for rel_path in ["task.md", "result.json", "score.json"]:
            source_path = source_run_dir / rel_path
            copied_core_files.append(
                _copy_file(
                    source_path=source_path,
                    source_rel_path=rel_path,
                    destination_root=destination_root,
                    copy_reason="core_run_file",
                )
            )

        copied_artifacts = copy_context_artifacts(
            query=query,
            source_run_dir=source_run_dir,
            destination_root=destination_root,
            artifact_records=item.get("artifact_records", []),
            max_artifacts=int(retrieval["max_artifacts_per_run"]),
            max_bytes=int(retrieval["max_artifact_bytes"]),
        )
        copied_artifact_bytes += sum(entry["bytes_copied"] for entry in copied_artifacts)
        selected_manifest_entries.append(
            {
                "run_id": item["run_id"],
                "score": item["total_score"],
                "total_score": item["total_score"],
                "score_breakdown": dict(item["score_breakdown"]),
                "summary": item["summary"],
                "copied_files": copied_core_files + copied_artifacts,
            }
        )
        item["copied_files"] = copied_core_files + copied_artifacts

    manifest_path = run_dir / settings["context_manifest_path"]
    summary_path = run_dir / settings["context_summary_path"]
    selection_strategy = str(retrieval.get("strategy", "hybrid_v1"))

    manifest_payload = {
        "index_version": index_state["index_version"],
        "index_mode": index_state["index_mode"],
        "selection_strategy": selection_strategy,
        "candidate_run_count": index_state["candidate_run_count"],
        "eligible_run_count": eligible_run_count,
        "selected_count": len(selected_sources),
        "query_text_hash": sha256_text(query_text),
        "query_token_count": query["token_count"],
        "ranking_latency_ms": ranking_latency_ms,
        "refreshed_run_count": index_state["refreshed_run_count"],
        "evicted_run_count": index_state["evicted_run_count"],
        "artifact_bytes_copied": copied_artifact_bytes,
        "selected_source_run_ids": selected_run_ids,
        "selected_sources": selected_manifest_entries,
        "top_candidates": top_candidates,
        "skipped_sources_count": skipped_sources,
    }
    write_json(manifest_path, manifest_payload)
    summary_path.write_text(build_summary(selected_sources), encoding="utf-8")
    print(str(manifest_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
