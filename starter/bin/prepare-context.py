#!/usr/bin/env python3
from __future__ import annotations

import os
import pathlib
import shutil
import sys
import time
from typing import Any

from harnesslib import (
    evaluate_policy_guardrail,
    guardrail_policy_snapshot,
    load_policy,
    load_run_contract,
    parse_task_file,
    resolve_execution_settings,
    resolve_retrieval_index_policy,
    sha256_file,
    sha256_text,
    write_json,
)
from learninglib import effective_candidate_mode
from retrieval_index import (
    build_query,
    build_query_text,
    dense_stage1_ranking,
    load_retrieval_profile,
    load_runtime_retrieval_candidate,
    rank_index_entries,
    retrieval_candidate_is_dense,
    runs_root,
    safe_text_artifact_excerpt,
    sync_retrieval_index,
)


def usage() -> int:
    print("usage: prepare-context.py /path/to/run-dir [policy-path]", file=sys.stderr)
    return 2


def _copy_existing_file(
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


def _write_derived_file(
    *,
    destination_path: pathlib.Path,
    source_path_label: str,
    contents: str,
    copy_reason: str,
) -> dict[str, Any]:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    destination_path.write_text(contents, encoding="utf-8")
    return {
        "source_path": source_path_label,
        "destination_path": str(destination_path),
        "sha256": sha256_file(destination_path),
        "copy_reason": copy_reason,
        "bytes_copied": destination_path.stat().st_size,
    }


def copy_context_artifacts(
    *,
    source_run_dir: pathlib.Path,
    destination_root: pathlib.Path,
    artifact_records: list[dict[str, Any]],
    max_artifacts: int,
    max_bytes: int,
    excerpt_char_limit: int,
) -> list[dict[str, Any]]:
    copied: list[dict[str, Any]] = []
    seen_paths: set[str] = set()

    for artifact in artifact_records:
        rel_path = str(artifact.get("path", ""))
        if not rel_path or rel_path in seen_paths:
            continue
        if not artifact.get("evidence_linked") or not artifact.get("eligible_for_copy"):
            continue
        excerpt_payload = safe_text_artifact_excerpt(
            source_run_dir,
            rel_path,
            max_bytes=max_bytes,
            char_limit=excerpt_char_limit,
        )
        if excerpt_payload is None:
            continue
        source_path = (source_run_dir / rel_path).resolve()
        copied.append(
            _copy_existing_file(
                source_path=source_path,
                source_rel_path=rel_path,
                destination_root=destination_root,
                copy_reason="claim_evidence",
            )
        )
        seen_paths.add(rel_path)
        if len(copied) >= max_artifacts:
            break
    return copied


def evaluate_abstention(
    ranked_sources: list[dict[str, Any]],
    *,
    retrieval_profile: dict[str, Any],
) -> dict[str, Any]:
    abstention_policy = dict(retrieval_profile.get("abstention", {}))
    enabled = bool(abstention_policy.get("enabled", True))
    min_top_score = int(abstention_policy.get("min_top_score", 0))
    min_score_margin = int(abstention_policy.get("min_score_margin", 0))
    top_score = float(ranked_sources[0]["total_score"]) if ranked_sources else 0.0
    second_score = float(ranked_sources[1]["total_score"]) if len(ranked_sources) > 1 else None
    score_margin = None if second_score is None else round(top_score - second_score, 2)
    reason = None

    if not ranked_sources:
        reason = "no_eligible_candidates"
    elif enabled and top_score < min_top_score:
        reason = "low_top_score"
    elif (
        enabled
        and second_score is not None
        and float(top_score - second_score) < float(min_score_margin)
    ):
        reason = "low_score_margin"

    return {
        "enabled": enabled,
        "abstained": reason is not None,
        "reason": reason,
        "top_score": round(top_score, 2) if ranked_sources else None,
        "second_score": round(second_score, 2) if second_score is not None else None,
        "score_margin": score_margin,
        "thresholds": {
            "min_top_score": min_top_score,
            "min_score_margin": min_score_margin,
        },
    }


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
                f"- Usefulness probability: {item.get('usefulness_probability', 'n/a')}",
                f"- Summary: {item['summary'] or 'No summary recorded.'}",
                f"- View: {item['view_path']}",
            ]
        )
        claim_records = item.get("claim_records", [])
        if claim_records:
            lines.append("- Claims:")
            for claim in claim_records[:3]:
                evidence = ", ".join(claim.get("evidence", [])) or "none recorded"
                lines.append(f"  - {claim.get('claim', '')} (evidence: {evidence})")
        artifact_records = [
            artifact
            for artifact in item.get("artifact_records", [])
            if artifact.get("evidence_linked") or artifact.get("excerpt")
        ]
        if artifact_records:
            lines.append("- Evidence artifacts:")
            for artifact in artifact_records[:3]:
                description = (
                    str(artifact.get("description", "")).strip() or "No description recorded."
                )
                lines.append(f"  - {artifact.get('path', '')}: {description}")
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
    run_contract = load_run_contract(run_dir / "run.contract.json")
    profile_override = os.environ.get("HARNESS_EXECUTION_PROFILE") or None
    settings = resolve_execution_settings(run_contract, profile_override=profile_override)
    if len(args) == 2:
        policy = load_policy(args[1], repo_root=run_dir.parent.parent)
    else:
        policy = load_policy(settings["policy_path"], repo_root=run_dir.parent.parent)
    parsed_task = parse_task_file(run_dir / "task.md", eval_policy=policy)
    if not parsed_task["ok"]:
        raise SystemExit("; ".join(parsed_task["errors"]))

    retrieval = settings["retrieval"]
    if os.environ.get("HARNESS_CONTEXT_MAX_SOURCE_RUNS"):
        retrieval["max_source_runs"] = max(1, int(os.environ["HARNESS_CONTEXT_MAX_SOURCE_RUNS"]))
    if os.environ.get("HARNESS_CONTEXT_MAX_CANDIDATES"):
        retrieval["max_candidates"] = max(1, int(os.environ["HARNESS_CONTEXT_MAX_CANDIDATES"]))
    retrieval_profile = load_retrieval_profile(repo_root=run_dir.parent.parent)
    retrieval_candidate = load_runtime_retrieval_candidate(repo_root=run_dir.parent.parent)
    retrieval_candidate_mode = effective_candidate_mode(retrieval_candidate)
    selection_strategy = str(retrieval.get("strategy", "hybrid_v1"))
    retrieval_enabled = bool(settings.get("retrieval_enabled", False))
    retrieval_index_policy = resolve_retrieval_index_policy(
        policy.get("retrieval_index"),
        profile_defaults=None,
    )

    pre_retrieval_decision = evaluate_policy_guardrail(
        policy,
        "pre_retrieval",
        context={
            "retrieval_enabled": retrieval_enabled,
            "retrieval_mode": selection_strategy,
            "retrieval_index_policy": retrieval_index_policy,
            "policy_path": policy.get("policy_path", settings["policy_path"]),
        },
    )
    if not pre_retrieval_decision["allowed"]:
        raise SystemExit("; ".join(pre_retrieval_decision["violations"]) or "pre_retrieval denied")

    pre_context_decision = evaluate_policy_guardrail(
        policy,
        "pre_context_build",
        context={
            "max_candidates": int(retrieval["max_candidates"]),
            "retrieval_profile_id": retrieval_profile["profile_id"],
            "retrieval_mode": selection_strategy,
            "retrieval_profile": retrieval_profile.get("profile_id"),
        },
    )
    if not pre_context_decision["allowed"]:
        raise SystemExit(
            "; ".join(pre_context_decision["violations"]) or "pre_context_build denied"
        )

    guardrail_decisions = [pre_retrieval_decision, pre_context_decision]

    context_dir = run_dir / settings["context_dir"]
    if context_dir.exists():
        shutil.rmtree(context_dir)
    context_dir.mkdir(parents=True, exist_ok=True)

    query_text = build_query_text(parsed_task)
    query = build_query(parsed_task)
    index_state = sync_retrieval_index(
        runs_root(run_dir),
        exclude_run_id=run_dir.name,
        eval_policy=policy,
        retrieval_profile=retrieval_profile,
        retrieval_mode=selection_strategy,
        **retrieval_index_policy,
    )
    indexed_entries = index_state["entries"]
    skipped_sources = sum(1 for entry in indexed_entries if not entry.get("eligible"))
    eligible_run_count = sum(1 for entry in indexed_entries if entry.get("eligible"))

    ranking_started = time.perf_counter()
    dense_stage1_enabled = retrieval_candidate_is_dense(retrieval_candidate)
    selection = (
        dict(retrieval_candidate.get("runtime", {}).get("selection", {}))
        if retrieval_candidate
        else {}
    )
    dense_stage1_k = int(selection.get("stage1_k", retrieval["max_candidates"]))
    dense_stage1 = None
    dense_fallback_reason = None
    stage1_source = "lexical"
    if dense_stage1_enabled:
        dense_stage1 = dense_stage1_ranking(
            query_text,
            indexed_entries,
            index_root=pathlib.Path(index_state["index_root"]),
            retrieval_candidate=retrieval_candidate,
            stage1_k=max(1, dense_stage1_k),
        )
        if dense_stage1["ok"] and retrieval_candidate_mode == "active":
            stage1_source = "dense_candidate"
        elif not dense_stage1["ok"]:
            dense_fallback_reason = dense_stage1["fallback_reason"]

    rerank_pool = rank_index_entries(
        query,
        indexed_entries,
        retrieval_profile=retrieval_profile,
        max_candidates=int(retrieval["max_candidates"]),
        retrieval_candidate=retrieval_candidate,
        prefer_candidate_scores=retrieval_candidate_mode == "active",
        stage1_score_overrides=dense_stage1.get("scores") if dense_stage1 else None,
        use_stage1_overrides=bool(
            dense_stage1
            and dense_stage1.get("ok")
            and retrieval_candidate_mode == "active"
            and dense_stage1_enabled
        ),
    )
    ranking_latency_ms = round((time.perf_counter() - ranking_started) * 1000.0, 2)
    abstention = evaluate_abstention(rerank_pool, retrieval_profile=retrieval_profile)
    candidate_shadow: dict[str, Any] | None = None
    selection_source = "legacy"
    candidate_rerank_pool = rerank_pool
    if (
        retrieval_candidate
        and retrieval_candidate_mode in {"shadow", "active"}
        and dense_stage1_enabled
        and dense_stage1
        and dense_stage1.get("ok")
    ):
        candidate_rerank_pool = rank_index_entries(
            query,
            indexed_entries,
            retrieval_profile=retrieval_profile,
            max_candidates=int(retrieval["max_candidates"]),
            retrieval_candidate=retrieval_candidate,
            prefer_candidate_scores=True,
            stage1_score_overrides=dense_stage1.get("scores"),
            use_stage1_overrides=True,
        )
    if retrieval_candidate and retrieval_candidate_mode in {"shadow", "active"}:
        runtime = dict(retrieval_candidate.get("runtime", {}))
        selection = dict(runtime.get("selection", {}))
        abstention_runtime = dict(runtime.get("abstention", {}))
        max_sources = int(selection.get("max_selected_sources", retrieval["max_source_runs"]))
        probability_threshold = float(abstention_runtime.get("probability_threshold", 0.5))
        candidate_selected = [
            item
            for item in candidate_rerank_pool
            if float(item.get("usefulness_probability") or 0.0) >= probability_threshold
        ][:max_sources]
        candidate_shadow = {
            "candidate_id": retrieval_candidate.get("candidate_id"),
            "mode": retrieval_candidate_mode,
            "selection_source": "candidate",
            "probability_threshold": probability_threshold,
            "selected_source_run_ids": [item["run_id"] for item in candidate_selected],
            "abstained": len(candidate_selected) == 0,
            "top_usefulness_probability": (
                candidate_rerank_pool[0].get("usefulness_probability")
                if candidate_rerank_pool
                else None
            ),
            "dense_stage1_enabled": dense_stage1_enabled,
            "dense_stage1_run_ids": (
                list(dense_stage1.get("ordered_run_ids", [])) if dense_stage1 else []
            ),
            "dense_stage1_top_scores": (
                list(dense_stage1.get("top_scores", [])) if dense_stage1 else []
            ),
        }
        if retrieval_candidate_mode == "active":
            selection_source = "candidate"
            selected_sources = candidate_selected
            abstention = {
                "enabled": True,
                "abstained": len(candidate_selected) == 0,
                "reason": "low_predicted_usefulness" if len(candidate_selected) == 0 else None,
                "top_score": (
                    rerank_pool[0].get("usefulness_probability") if rerank_pool else None
                ),
                "second_score": (
                    rerank_pool[1].get("usefulness_probability") if len(rerank_pool) > 1 else None
                ),
                "score_margin": None,
                "thresholds": {"probability_threshold": probability_threshold},
            }
        else:
            selected_sources = (
                [] if abstention["abstained"] else rerank_pool[: int(retrieval["max_source_runs"])]
            )
    else:
        selected_sources = (
            [] if abstention["abstained"] else rerank_pool[: int(retrieval["max_source_runs"])]
        )
    selected_run_ids = [item["run_id"] for item in selected_sources]

    selected_manifest_entries: list[dict[str, Any]] = []
    copied_artifact_bytes = 0
    top_candidates = [
        {
            "run_id": item["run_id"],
            "total_score": item["total_score"],
            "score_breakdown": dict(item["score_breakdown"]),
            "usefulness_probability": item.get("usefulness_probability"),
            "candidate_score": item.get("candidate_score"),
            "dense_stage1_score": item.get("dense_stage1_score"),
            "selected": item["run_id"] in selected_run_ids,
            "summary": item.get("summary", ""),
            "claims": list(item.get("claims", [])),
            "artifact_records": list(item.get("artifact_records", [])),
            "evidence_paths": list(item.get("retrieval_view", {}).get("evidence_paths", [])),
            "document_text": str(item.get("retrieval_view", {}).get("text", "")),
            "source_snapshot_fingerprint": item.get("retrieval_view", {}).get(
                "source_snapshot_fingerprint", ""
            )
            or item.get("source_snapshot_fingerprint", ""),
        }
        for item in rerank_pool
    ]

    for item in selected_sources:
        source_run_dir = item["source_run_dir"]
        destination_root = context_dir / "source-runs" / item["run_id"]
        destination_root.mkdir(parents=True, exist_ok=True)
        view_path = destination_root / "retrieval-view.md"
        copied_files: list[dict[str, Any]] = [
            _write_derived_file(
                destination_path=view_path,
                source_path_label="retrieval_view",
                contents=str(item.get("retrieval_view", {}).get("text", "")),
                copy_reason="retrieval_view",
            )
        ]

        copied_artifacts = copy_context_artifacts(
            source_run_dir=source_run_dir,
            destination_root=destination_root,
            artifact_records=item.get("artifact_records", []),
            max_artifacts=int(retrieval["max_artifacts_per_run"]),
            max_bytes=int(retrieval_profile["view_excerpt_max_bytes"]),
            excerpt_char_limit=int(retrieval_profile["view_excerpt_char_limit"]),
        )
        copied_artifact_bytes += sum(entry["bytes_copied"] for entry in copied_artifacts)
        copied_files.extend(copied_artifacts)
        copy_summary = {
            "copied_file_count": len(copied_files),
            "retrieval_view_count": 1,
            "claim_evidence_copy_count": sum(
                1 for entry in copied_files if entry["copy_reason"] == "claim_evidence"
            ),
            "evidence_backed_claim_count": int(
                item.get("quality", {}).get("evidence_backed_claim_count", 0)
            ),
            "evidence_linked_artifact_count": int(
                item.get("quality", {}).get("evidence_linked_artifact_count", 0)
            ),
        }

        relative_view_path = str(view_path.relative_to(run_dir))
        selected_manifest_entries.append(
            {
                "run_id": item["run_id"],
                "score": item["total_score"],
                "total_score": item["total_score"],
                "score_breakdown": dict(item["score_breakdown"]),
                "summary": item["summary"],
                "view_path": relative_view_path,
                "copied_files": copied_files,
                "copy_summary": copy_summary,
                "usefulness_probability": item.get("usefulness_probability"),
            }
        )
        item["view_path"] = relative_view_path
        item["copied_files"] = copied_files
        item["copy_summary"] = copy_summary

    manifest_path = run_dir / settings["context_manifest_path"]
    summary_path = run_dir / settings["context_summary_path"]
    selection_strategy = str(retrieval.get("strategy", "hybrid_v1"))

    manifest_payload = {
        "context_manifest_version": "v1",
        "index_version": index_state["index_version"],
        "index_mode": index_state["index_mode"],
        "selection_strategy": selection_strategy,
        "selection_source": selection_source,
        "stage1_source": stage1_source,
        "policy_path": policy.get("policy_path", settings["policy_path"]),
        "retrieval_index_policy": retrieval_index_policy,
        "retrieval_profile_id": retrieval_profile["profile_id"],
        "retrieval_profile_fingerprint": retrieval_profile.get("profile_fingerprint"),
        "retrieval_candidate_id": retrieval_candidate.get("candidate_id")
        if retrieval_candidate
        else None,
        "retrieval_candidate_mode": retrieval_candidate_mode,
        "dense_candidate_id": retrieval_candidate.get("candidate_id")
        if retrieval_candidate and dense_stage1_enabled
        else None,
        "dense_candidate_mode": retrieval_candidate_mode if dense_stage1_enabled else None,
        "dense_fallback_reason": dense_fallback_reason,
        "dense_stage1_k": max(1, dense_stage1_k) if dense_stage1_enabled else None,
        "retriever_version": (
            retrieval_candidate.get("runtime", {}).get("retriever_version")
            if retrieval_candidate
            else None
        ),
        "reranker_version": (
            retrieval_candidate.get("runtime", {}).get("reranker_version")
            if retrieval_candidate
            else None
        ),
        "abstention_model_version": (
            retrieval_candidate.get("runtime", {}).get("abstention_model_version")
            if retrieval_candidate
            else None
        ),
        "retrieval_mode": selection_strategy,
        "index_provenance_token": index_state["index_provenance_token"],
        "source_snapshot_fingerprints": index_state["source_snapshot_fingerprints"],
        "index_refreshes": index_state["index_refreshes"],
        "stale_removed": index_state["stale_removed"],
        "compacted_removed": index_state["compacted_removed"],
        "compacted_kept": index_state["compacted_kept"],
        "index_rebuild_count": index_state["index_rebuild_count"],
        "candidate_run_count": index_state["candidate_run_count"],
        "eligible_run_count": eligible_run_count,
        "selected_count": len(selected_sources),
        "selected_source_count": len(selected_sources),
        "max_index_entries": index_state["max_index_entries"],
        "max_index_bytes": index_state["max_index_bytes"],
        "empty_context": len(selected_sources) == 0,
        "abstained": abstention["abstained"],
        "abstention_reason": abstention["reason"],
        "abstention_thresholds": abstention["thresholds"],
        "top_candidate_score": abstention["top_score"],
        "top_candidate_score_margin": abstention["score_margin"],
        "query_text_hash": sha256_text(query_text),
        "query_token_count": query["token_count"],
        "ranking_latency_ms": ranking_latency_ms,
        "refreshed_run_count": index_state["refreshed_run_count"],
        "evicted_run_count": index_state["evicted_run_count"],
        "artifact_bytes_copied": copied_artifact_bytes,
        "selected_source_run_ids": selected_run_ids,
        "selected_sources": selected_manifest_entries,
        "top_candidates": top_candidates,
        "query": {
            "task_title": query.get("task_title", ""),
            "goal": query.get("sections", {}).get("Goal", ""),
            "constraints": query.get("sections", {}).get("Constraints", ""),
            "done": query.get("sections", {}).get("Done", ""),
            "text": query_text,
        },
        "skipped_sources_count": skipped_sources,
        "candidate_shadow": candidate_shadow,
        "guardrail_decisions": guardrail_decisions,
        "guardrail_policy": guardrail_policy_snapshot(policy),
    }
    write_json(manifest_path, manifest_payload)
    summary_path.write_text(build_summary(selected_sources), encoding="utf-8")
    print(str(manifest_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
