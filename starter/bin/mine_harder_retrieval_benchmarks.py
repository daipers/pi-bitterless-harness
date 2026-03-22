#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import sys
from typing import Any

from harnesslib import load_policy, write_json
from retrieval_index import (
    build_query,
    lexical_score,
    load_retrieval_profile,
    merge_counters,
    parse_task_file,
    runs_root,
    sync_retrieval_index,
)


def usage() -> int:
    print(
        "usage: mine_harder_retrieval_benchmarks.py [runs-root] [policy-path] [output-dir]",
        file=sys.stderr,
    )
    return 2


def default_runs_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1] / "runs"


def default_output_dir() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1] / "benchmarks" / "review_queue"


def entry_similarity(entry: dict[str, Any], confuser: dict[str, Any]) -> int:
    left = merge_counters(
        dict_to_counter(entry.get("document_tokens", {}).get("summary", {})),
        dict_to_counter(entry.get("document_tokens", {}).get("claims", {})),
        dict_to_counter(entry.get("document_tokens", {}).get("artifact_descriptions", {})),
    )
    right = merge_counters(
        dict_to_counter(confuser.get("document_tokens", {}).get("summary", {})),
        dict_to_counter(confuser.get("document_tokens", {}).get("claims", {})),
        dict_to_counter(confuser.get("document_tokens", {}).get("artifact_descriptions", {})),
    )
    return lexical_score(left, dict(right))


def dict_to_counter(payload: dict[str, int]) -> Any:
    from collections import Counter

    return Counter(payload)


def proposal_key(payload: dict[str, Any]) -> str:
    return "|".join(
        [
            str(payload["case_kind"]),
            str(payload["provenance"]["query_text_hash"]),
            str(payload["provenance"]["gold_run_id"]),
            str(payload["provenance"]["confuser_run_id"]),
        ]
    )


def load_existing_keys(output_dir: pathlib.Path) -> set[str]:
    keys: set[str] = set()
    if not output_dir.exists():
        return keys
    for path in output_dir.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict) and "case_kind" in payload and "provenance" in payload:
            keys.add(proposal_key(payload))
    return keys


def entry_to_seed_run(entry: dict[str, Any]) -> dict[str, Any]:
    view = dict(entry.get("retrieval_view", {}))
    artifact_contents = {
        artifact["path"]: str(artifact.get("excerpt", ""))
        for artifact in entry.get("artifact_records", [])
        if artifact.get("excerpt")
    }
    return {
        "run_id": entry["run_id"],
        "title": view.get("task_title") or entry["run_id"],
        "goal": view.get("goal") or "",
        "constraints": [item for item in [view.get("constraints")] if item],
        "done": [item for item in [view.get("done")] if item],
        "summary": entry.get("summary", ""),
        "claims": list(entry.get("claim_records", [])),
        "artifacts": [
            {
                "path": artifact["path"],
                "description": artifact.get("description", ""),
            }
            for artifact in entry.get("artifact_records", [])
        ],
        "artifact_contents": artifact_contents,
    }


def build_query_from_entry(run_dir: pathlib.Path, policy: dict[str, Any] | None) -> dict[str, Any]:
    parsed_task = parse_task_file(run_dir / "task.md", eval_policy=policy)
    if not parsed_task["ok"]:
        raise ValueError("; ".join(parsed_task["errors"]))
    return build_query(parsed_task)


def propose_same_words_wrong_artifact(
    gold: dict[str, Any],
    confuser: dict[str, Any],
) -> dict[str, Any] | None:
    gold_artifacts = [artifact for artifact in gold.get("artifact_records", []) if artifact.get("excerpt")]
    confuser_artifacts = [
        artifact for artifact in confuser.get("artifact_records", []) if artifact.get("excerpt")
    ]
    if not gold_artifacts or not confuser_artifacts:
        return None
    if set(gold["artifact_paths"]) == set(confuser["artifact_paths"]):
        return None
    if entry_similarity(gold, confuser) < 8:
        return None
    return {
        "case_kind": "same_words_wrong_artifact",
        "query_title": gold["retrieval_view"].get("task_title", gold["run_id"]),
        "query_goal": gold["retrieval_view"].get("goal", ""),
        "query_constraints": [gold["retrieval_view"].get("constraints", "")] if gold["retrieval_view"].get("constraints") else [],
        "query_done": [gold["retrieval_view"].get("done", "")] if gold["retrieval_view"].get("done") else [],
        "expected_top_1_run_id": gold["run_id"],
        "hard_negative_run_ids": [confuser["run_id"]],
        "seed_runs": [entry_to_seed_run(confuser), entry_to_seed_run(gold)],
    }


def propose_same_claim_weaker_evidence(
    gold: dict[str, Any],
    confuser: dict[str, Any],
) -> dict[str, Any] | None:
    gold_quality = dict(gold.get("quality", {}))
    confuser_quality = dict(confuser.get("quality", {}))
    if int(gold_quality.get("evidence_backed_claim_count", 0)) < 1:
        return None
    if int(confuser_quality.get("evidence_backed_claim_count", 0)) >= int(
        gold_quality.get("evidence_backed_claim_count", 0)
    ):
        return None
    if entry_similarity(gold, confuser) < 6:
        return None
    return {
        "case_kind": "same_claim_weaker_evidence",
        "query_title": gold["retrieval_view"].get("task_title", gold["run_id"]),
        "query_goal": gold["retrieval_view"].get("goal", ""),
        "query_constraints": [gold["retrieval_view"].get("constraints", "")] if gold["retrieval_view"].get("constraints") else [],
        "query_done": [gold["retrieval_view"].get("done", "")] if gold["retrieval_view"].get("done") else [],
        "expected_top_1_run_id": gold["run_id"],
        "hard_negative_run_ids": [confuser["run_id"]],
        "seed_runs": [entry_to_seed_run(confuser), entry_to_seed_run(gold)],
    }


def attach_provenance(base: dict[str, Any], gold: dict[str, Any], confuser: dict[str, Any]) -> dict[str, Any]:
    scenario_id = (
        f"mined-{base['case_kind']}-{str(gold.get('query_text_hash', 'unknown'))[:8]}-"
        f"{gold['run_id']}-{confuser['run_id']}"
    )
    return {
        **base,
        "scenario_id": scenario_id,
        "provenance": {
            "source": "starter/bin/mine_harder_retrieval_benchmarks.py",
            "query_text_hash": gold.get("query_text_hash"),
            "gold_run_id": gold["run_id"],
            "confuser_run_id": confuser["run_id"],
        },
    }


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) > 3:
        return usage()

    runs_dir = pathlib.Path(args[0]).resolve() if args else default_runs_root()
    policy = load_policy(args[1], repo_root=runs_dir.parent) if len(args) >= 2 else None
    output_dir = pathlib.Path(args[2]).resolve() if len(args) == 3 else default_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    retrieval_profile = load_retrieval_profile(repo_root=runs_dir.parent)
    state = sync_retrieval_index(
        runs_root(runs_dir),
        exclude_run_id=None,
        eval_policy=policy,
        retrieval_profile=retrieval_profile,
    )
    entries = [entry for entry in state["entries"] if entry.get("eligible")]
    existing_keys = load_existing_keys(output_dir)
    written_paths: list[str] = []

    for gold in entries:
        gold_quality = dict(gold.get("quality", {}))
        if not (
            int(gold_quality.get("evidence_backed_claim_count", 0)) >= 1
            or int(gold_quality.get("descriptive_artifact_count", 0)) >= 1
        ):
            continue
        for confuser in entries:
            if confuser["run_id"] == gold["run_id"]:
                continue
            for builder in [propose_same_words_wrong_artifact, propose_same_claim_weaker_evidence]:
                proposal = builder(gold, confuser)
                if proposal is None:
                    continue
                proposal = attach_provenance(proposal, gold, confuser)
                key = proposal_key(proposal)
                if key in existing_keys:
                    continue
                destination = output_dir / f"{proposal['scenario_id']}.json"
                write_json(destination, proposal, sort_keys=False)
                written_paths.append(str(destination))
                existing_keys.add(key)

    payload = {
        "index_version": state["index_version"],
        "retrieval_profile_id": retrieval_profile["profile_id"],
        "eligible_run_count": len(entries),
        "proposal_count": len(written_paths),
        "proposal_paths": written_paths,
        "output_dir": str(output_dir),
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
