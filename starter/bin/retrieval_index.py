#!/usr/bin/env python3
from __future__ import annotations

import collections
import concurrent.futures
import json
import pathlib
import re
import shutil
from typing import Any

from harnesslib import DEFAULT_RETRIEVAL_CONFIG, evaluate_required_artifact_path, parse_task_file, write_json

INDEX_VERSION = "retrieval-v2"
INDEX_ROOT_PARTS = (".index", INDEX_VERSION)
TOKEN_RE = re.compile(r"[a-z0-9]+")
FIELD_WEIGHTS = {
    "goal_overlap": 4,
    "constraints_overlap": 2,
    "done_overlap": 1,
    "summary_overlap": 3,
    "claim_overlap": 4,
    "artifact_overlap": 2,
}
PHRASE_BONUS_PER_FIELD = 6
PHRASE_BONUS_CAP = 12
ARTIFACT_EXCERPT_BYTES = 2048
SKIPPED_TOP_LEVEL = {"home", "session", "recovery"}
SKIPPED_FILES = {
    "transcript.jsonl",
    "pi.stderr.log",
    "patch.diff",
    "git.status.txt",
    "pi.exit_code.txt",
    "run-events.jsonl",
}


def tokenize(text: str) -> collections.Counter[str]:
    return collections.Counter(TOKEN_RE.findall(text.lower()))


def lexical_score(query: collections.Counter[str], document: dict[str, int]) -> int:
    return sum(min(count, int(document.get(token, 0))) for token, count in query.items())


def parse_json_file(path: pathlib.Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def runs_root(run_dir: pathlib.Path) -> pathlib.Path:
    candidate = run_dir.resolve()
    if candidate.name == "runs":
        return candidate
    parent = candidate.parent
    if parent.name != "runs":
        raise ValueError(f"run directory must live under runs/: {run_dir}")
    return parent


def retrieval_index_root_for_runs(runs_dir: pathlib.Path) -> pathlib.Path:
    return runs_dir / INDEX_ROOT_PARTS[0] / INDEX_ROOT_PARTS[1]


def retrieval_index_root_for_run(run_dir: pathlib.Path) -> pathlib.Path:
    return retrieval_index_root_for_runs(runs_root(run_dir))


def candidate_run_dirs(
    runs_dir: pathlib.Path,
    *,
    exclude_run_id: str | None = None,
) -> list[pathlib.Path]:
    candidates = [
        path
        for path in runs_dir.iterdir()
        if path.is_dir() and not path.name.startswith(".") and path.name != exclude_run_id
    ]
    return sorted(candidates, key=lambda path: path.name)


def file_fingerprint(path: pathlib.Path) -> dict[str, Any]:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return {"exists": False, "mtime_ns": None, "size": None}
    return {"exists": True, "mtime_ns": stat.st_mtime_ns, "size": stat.st_size}


def run_input_fingerprints(run_dir: pathlib.Path) -> dict[str, dict[str, Any]]:
    return {
        "task_md": file_fingerprint(run_dir / "task.md"),
        "result_json": file_fingerprint(run_dir / "result.json"),
        "score_json": file_fingerprint(run_dir / "score.json"),
    }


def index_path(index_root: pathlib.Path, run_id: str) -> pathlib.Path:
    return index_root / f"{run_id}.json"


def load_index_entry(index_root: pathlib.Path, run_id: str) -> dict[str, Any] | None:
    return parse_json_file(index_path(index_root, run_id))


def normalize_match_text(text: str) -> str:
    return " ".join(text.lower().split())


def serialize_counter(counter: collections.Counter[str]) -> dict[str, int]:
    return {token: int(count) for token, count in counter.items() if count > 0}


def merge_counters(*counters: collections.Counter[str]) -> collections.Counter[str]:
    merged: collections.Counter[str] = collections.Counter()
    for counter in counters:
        merged.update(counter)
    return merged


def build_query_sections(parsed_task: dict[str, Any]) -> dict[str, str]:
    sections = parsed_task.get("sections", {})
    return {
        "task_title": str(parsed_task.get("task_title", "")),
        "goal": sections.get("Goal", ""),
        "constraints": sections.get("Constraints", ""),
        "done": sections.get("Done", ""),
    }


def build_query_text(parsed_task: dict[str, Any]) -> str:
    query_sections = build_query_sections(parsed_task)
    return "\n".join(
        [
            query_sections["task_title"],
            query_sections["goal"],
            query_sections["constraints"],
            query_sections["done"],
        ]
    ).strip()


def build_query(parsed_task: dict[str, Any]) -> dict[str, Any]:
    sections = build_query_sections(parsed_task)
    task_lines = [sections["task_title"]]
    for key in ["goal", "constraints", "done"]:
        task_lines.extend(
            line.strip("- ").strip()
            for line in sections[key].splitlines()
            if line.strip()
        )
    phrase_source_lines = [sections["task_title"]]
    phrase_source_lines.extend(
        line.strip("- ").strip() for line in sections["goal"].splitlines() if line.strip()
    )
    phrases: list[str] = []
    for line in phrase_source_lines:
        tokens = TOKEN_RE.findall(line.lower())
        if len(tokens) >= 2:
            phrases.append(" ".join(tokens))
        for window_size in range(3, min(5, len(tokens)) + 1):
            for start in range(0, len(tokens) - window_size + 1):
                phrases.append(" ".join(tokens[start : start + window_size]))
    phrase_candidates = sorted({phrase for phrase in phrases if phrase}, key=len, reverse=True)
    field_tokens = {
        "task_title": tokenize(sections["task_title"]),
        "goal": tokenize(sections["goal"]),
        "constraints": tokenize(sections["constraints"]),
        "done": tokenize(sections["done"]),
    }
    candidate_tokens = merge_counters(
        field_tokens["task_title"],
        field_tokens["goal"],
        field_tokens["constraints"],
        field_tokens["done"],
    )
    return {
        "text": build_query_text(parsed_task),
        "phrases": phrase_candidates,
        "field_tokens": field_tokens,
        "candidate_tokens": candidate_tokens,
        "token_count": sum(candidate_tokens.values()),
    }


def _valid_artifact_source_path(source_run_dir: pathlib.Path, rel_path: str) -> pathlib.Path | None:
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
    return source_path


def _safe_text_artifact_excerpt(
    source_run_dir: pathlib.Path,
    rel_path: str,
    *,
    max_bytes: int,
) -> tuple[str, int] | None:
    source_path = _valid_artifact_source_path(source_run_dir, rel_path)
    if source_path is None:
        return None
    try:
        size = source_path.stat().st_size
    except OSError:
        return None
    if size > max_bytes:
        return None
    try:
        excerpt = source_path.read_text(encoding="utf-8")
    except Exception:
        return None
    return excerpt[:ARTIFACT_EXCERPT_BYTES], size


def eligible_claims(result_payload: dict[str, Any]) -> list[str]:
    return [
        item.get("claim", "")
        for item in result_payload.get("claims", [])
        if isinstance(item, dict) and item.get("claim")
    ]


def claim_evidence_paths(result_payload: dict[str, Any]) -> set[str]:
    paths: set[str] = set()
    for item in result_payload.get("claims", []):
        if not isinstance(item, dict):
            continue
        evidence = item.get("evidence", [])
        if not isinstance(evidence, list):
            continue
        for candidate in evidence:
            if isinstance(candidate, str) and candidate:
                paths.add(candidate)
    return paths


def declared_artifacts(result_payload: dict[str, Any]) -> list[dict[str, str]]:
    artifacts: list[dict[str, str]] = []
    for item in result_payload.get("artifacts", []):
        if not isinstance(item, dict):
            continue
        rel_path = item.get("path")
        if not isinstance(rel_path, str) or not rel_path:
            continue
        artifacts.append(
            {
                "path": rel_path,
                "description": str(item.get("description", "")),
            }
        )
    return artifacts


def build_index_entry(
    source_run_dir: pathlib.Path,
    *,
    eval_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fingerprints = run_input_fingerprints(source_run_dir)
    entry: dict[str, Any] = {
        "index_version": INDEX_VERSION,
        "run_id": source_run_dir.name,
        "source_run_path": str(source_run_dir.resolve()),
        "input_fingerprints": fingerprints,
        "eligible": False,
        "skip_reason": None,
        "overall_pass": False,
        "summary": "",
        "claims": [],
        "artifact_paths": [],
        "artifact_records": [],
        "quality": {
            "summary_present": False,
            "claim_count": 0,
            "eligible_artifact_count": 0,
        },
        "document_tokens": {},
    }

    task_path = source_run_dir / "task.md"
    result_path = source_run_dir / "result.json"
    score_path = source_run_dir / "score.json"

    if not task_path.is_file() or not result_path.is_file() or not score_path.is_file():
        entry["skip_reason"] = "missing_required_file"
        return entry

    score_payload = parse_json_file(score_path)
    if not isinstance(score_payload, dict):
        entry["skip_reason"] = "score_unreadable"
        return entry
    if score_payload.get("overall_pass") is not True:
        entry["skip_reason"] = "score_not_passing"
        return entry

    result_payload = parse_json_file(result_path)
    if not isinstance(result_payload, dict):
        entry["skip_reason"] = "result_unreadable"
        return entry

    parsed_task = parse_task_file(task_path, eval_policy=eval_policy)
    if not parsed_task["ok"]:
        entry["skip_reason"] = "task_parse_failed"
        return entry

    query_sections = build_query_sections(parsed_task)
    summary = str(result_payload.get("summary", ""))
    claims = eligible_claims(result_payload)
    evidence_paths = claim_evidence_paths(result_payload)
    artifacts = declared_artifacts(result_payload)

    artifact_description_tokens: collections.Counter[str] = collections.Counter()
    artifact_excerpt_tokens: collections.Counter[str] = collections.Counter()
    artifact_records: list[dict[str, Any]] = []
    eligible_artifact_count = 0

    for artifact in artifacts:
        rel_path = artifact["path"]
        description = artifact["description"]
        description_tokens = tokenize(description)
        artifact_description_tokens.update(description_tokens)
        evidence_linked = rel_path in evidence_paths
        excerpt = ""
        excerpt_size = None
        if evidence_linked:
            excerpt_payload = _safe_text_artifact_excerpt(
                source_run_dir,
                rel_path,
                max_bytes=int(DEFAULT_RETRIEVAL_CONFIG["max_artifact_bytes"]),
            )
            if excerpt_payload is not None:
                excerpt, excerpt_size = excerpt_payload
                artifact_excerpt_tokens.update(tokenize(excerpt))
        safe_text = _safe_text_artifact_excerpt(
            source_run_dir,
            rel_path,
            max_bytes=int(DEFAULT_RETRIEVAL_CONFIG["max_artifact_bytes"]),
        )
        eligible_for_copy = safe_text is not None
        if eligible_for_copy:
            eligible_artifact_count += 1
        artifact_records.append(
            {
                "path": rel_path,
                "description": description,
                "description_tokens": serialize_counter(description_tokens),
                "evidence_linked": evidence_linked,
                "eligible_for_copy": eligible_for_copy,
                "excerpt": excerpt,
                "excerpt_tokens": serialize_counter(tokenize(excerpt)),
                "excerpt_size_bytes": excerpt_size,
            }
        )

    entry["eligible"] = True
    entry["overall_pass"] = True
    entry["summary"] = summary
    entry["claims"] = claims
    entry["artifact_paths"] = [artifact["path"] for artifact in artifacts]
    entry["artifact_records"] = artifact_records
    entry["quality"] = {
        "summary_present": bool(summary.strip()),
        "claim_count": len(claims),
        "eligible_artifact_count": eligible_artifact_count,
    }
    entry["document_tokens"] = {
        "task_title": serialize_counter(tokenize(query_sections["task_title"])),
        "goal": serialize_counter(tokenize(query_sections["goal"])),
        "constraints": serialize_counter(tokenize(query_sections["constraints"])),
        "done": serialize_counter(tokenize(query_sections["done"])),
        "summary": serialize_counter(tokenize(summary)),
        "claims": serialize_counter(tokenize("\n".join(claims))),
        "artifact_descriptions": serialize_counter(artifact_description_tokens),
        "artifact_excerpts": serialize_counter(artifact_excerpt_tokens),
    }
    return entry


def entry_is_fresh(entry: dict[str, Any], source_run_dir: pathlib.Path) -> bool:
    return (
        entry.get("index_version") == INDEX_VERSION
        and entry.get("input_fingerprints") == run_input_fingerprints(source_run_dir)
    )


def write_index_entry(index_root: pathlib.Path, entry: dict[str, Any]) -> pathlib.Path:
    path = index_path(index_root, str(entry["run_id"]))
    write_json(path, entry)
    return path


def stage1_candidate_score(query: dict[str, Any], entry: dict[str, Any]) -> int:
    document_tokens = entry.get("document_tokens", {})
    candidate_tokens = merge_counters(
        collections.Counter(document_tokens.get("task_title", {})),
        collections.Counter(document_tokens.get("goal", {})),
        collections.Counter(document_tokens.get("constraints", {})),
        collections.Counter(document_tokens.get("done", {})),
    )
    return lexical_score(query["candidate_tokens"], dict(candidate_tokens))


def _field_overlap_score(
    query_tokens: collections.Counter[str],
    entry: dict[str, Any],
    entry_field: str,
    weight_key: str,
) -> int:
    field_tokens = collections.Counter(entry.get("document_tokens", {}).get(entry_field, {}))
    return FIELD_WEIGHTS[weight_key] * lexical_score(query_tokens, dict(field_tokens))


def _field_phrase_hit(query: dict[str, Any], texts: list[str]) -> bool:
    haystacks = [normalize_match_text(text) for text in texts if text]
    if not haystacks:
        return False
    return any(phrase in haystack for phrase in query["phrases"] for haystack in haystacks)


def score_index_entry(query: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any]:
    artifact_texts = []
    for artifact in entry.get("artifact_records", []):
        artifact_texts.append(str(artifact.get("description", "")))
        artifact_texts.append(str(artifact.get("excerpt", "")))

    score_breakdown = {
        "goal_overlap": _field_overlap_score(
            query["field_tokens"]["goal"], entry, "goal", "goal_overlap"
        ),
        "constraints_overlap": _field_overlap_score(
            query["field_tokens"]["constraints"], entry, "constraints", "constraints_overlap"
        ),
        "done_overlap": _field_overlap_score(
            query["field_tokens"]["done"], entry, "done", "done_overlap"
        ),
        "summary_overlap": _field_overlap_score(
            query["candidate_tokens"], entry, "summary", "summary_overlap"
        ),
        "claim_overlap": _field_overlap_score(
            query["candidate_tokens"], entry, "claims", "claim_overlap"
        ),
        "artifact_overlap": FIELD_WEIGHTS["artifact_overlap"]
        * lexical_score(
            query["candidate_tokens"],
            dict(
                merge_counters(
                    collections.Counter(entry.get("document_tokens", {}).get("artifact_descriptions", {})),
                    collections.Counter(entry.get("document_tokens", {}).get("artifact_excerpts", {})),
                )
            ),
        ),
        "phrase_bonus": 0,
        "quality_prior": 0,
    }
    phrase_hits = {
        "summary": _field_phrase_hit(query, [str(entry.get("summary", ""))]),
        "claims": _field_phrase_hit(query, list(entry.get("claims", []))),
        "artifacts": _field_phrase_hit(query, artifact_texts),
    }
    score_breakdown["phrase_bonus"] = min(
        PHRASE_BONUS_CAP,
        PHRASE_BONUS_PER_FIELD * sum(1 for matched in phrase_hits.values() if matched),
    )

    quality = entry.get("quality", {})
    if int(quality.get("claim_count", 0)) > 0:
        score_breakdown["quality_prior"] += 2
    if int(quality.get("eligible_artifact_count", 0)) > 0:
        score_breakdown["quality_prior"] += 1

    total_score = sum(score_breakdown.values())
    return {
        "run_id": entry["run_id"],
        "total_score": total_score,
        "score_breakdown": score_breakdown,
        "phrase_hits": phrase_hits,
        "stage1_score": stage1_candidate_score(query, entry),
    }


def sync_retrieval_index(
    runs_dir: pathlib.Path,
    *,
    exclude_run_id: str | None = None,
    eval_policy: dict[str, Any] | None = None,
    force_rebuild: bool = False,
) -> dict[str, Any]:
    runs_dir = runs_root(runs_dir)
    index_root = retrieval_index_root_for_runs(runs_dir)
    had_index = index_root.exists()

    if force_rebuild and index_root.exists():
        shutil.rmtree(index_root)
        had_index = False

    index_root.mkdir(parents=True, exist_ok=True)

    run_dirs = candidate_run_dirs(runs_dir, exclude_run_id=exclude_run_id)
    run_ids = {path.name for path in run_dirs}
    existing_index_paths = {
        path.stem: path for path in index_root.glob("*.json") if path.is_file()
    }

    evicted_count = 0
    for run_id, path in existing_index_paths.items():
        if run_id in run_ids:
            continue
        path.unlink()
        evicted_count += 1

    refreshed_count = 0
    entries: list[dict[str, Any]] = []
    refreshed_entries: dict[str, dict[str, Any]] = {}
    stale_run_dirs: list[pathlib.Path] = []
    for run_dir in run_dirs:
        existing_entry = load_index_entry(index_root, run_dir.name)
        if force_rebuild or existing_entry is None or not entry_is_fresh(existing_entry, run_dir):
            stale_run_dirs.append(run_dir)
        else:
            entries.append(existing_entry)

    if stale_run_dirs:
        max_workers = min(8, len(stale_run_dirs))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_run_id = {
                executor.submit(build_index_entry, run_dir, eval_policy=eval_policy): run_dir.name
                for run_dir in stale_run_dirs
            }
            for future in concurrent.futures.as_completed(future_to_run_id):
                entry = future.result()
                refreshed_entries[str(entry["run_id"])] = entry
                write_index_entry(index_root, entry)
                refreshed_count += 1

    entries = []
    for run_dir in run_dirs:
        if run_dir.name in refreshed_entries:
            entries.append(refreshed_entries[run_dir.name])
            continue
        existing_entry = load_index_entry(index_root, run_dir.name)
        if existing_entry is not None:
            entries.append(existing_entry)

    if force_rebuild or (not had_index and run_dirs):
        index_mode = "cold_build"
    elif refreshed_count > 0 or evicted_count > 0:
        index_mode = "incremental_refresh"
    else:
        index_mode = "warm_reuse"

    return {
        "index_root": index_root,
        "index_version": INDEX_VERSION,
        "index_mode": index_mode,
        "candidate_run_count": len(run_dirs),
        "refreshed_run_count": refreshed_count,
        "evicted_run_count": evicted_count,
        "entries": entries,
    }
