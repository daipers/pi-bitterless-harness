#!/usr/bin/env python3
from __future__ import annotations

import collections
import concurrent.futures
import json
import pathlib
import re
import shutil
from datetime import UTC, datetime
from typing import Any

import numpy
from dense_retrieval import (
    DEFAULT_EMBEDDING_DIM,
    DEFAULT_HASH_DIM,
    DEFAULT_PROJECTION_SEED,
    encode_text,
    load_dense_retriever_runtime,
    load_text_encoder_runtime,
    score_pair,
)
from harnesslib import (
    evaluate_required_artifact_path,
    now_utc,
    parse_task_file,
    sha256_text,
    write_json,
)
from learninglib import (
    candidate_runtime,
    effective_candidate_mode,
    load_candidate_manifest,
    sigmoid,
)

INDEX_VERSION = "retrieval-v4"
INDEX_ROOT_PARTS = (".index", INDEX_VERSION)
INDEX_DEFAULT_TTL_SECONDS = 0
INDEX_DEFAULT_MAX_ENTRIES = 0
DENSE_STAGE1_CACHE_DIR = "dense-stage1-cache"


def _to_positive_int(value: str | int | None, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    if parsed < 0:
        return default
    return parsed


def _now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


TOKEN_RE = re.compile(r"[a-z0-9]+")
SKIPPED_TOP_LEVEL = {"home", "session", "recovery"}
SKIPPED_FILES = {
    "transcript.jsonl",
    "pi.stderr.log",
    "patch.diff",
    "git.status.txt",
    "pi.exit_code.txt",
    "run-events.jsonl",
}
DEFAULT_RETRIEVAL_PROFILE = {
    "profile_id": "retrieval-v4-default",
    "field_weights": {
        "task_title_overlap": 2,
        "goal_overlap": 4,
        "constraints_overlap": 2,
        "done_overlap": 1,
        "summary_overlap": 3,
        "claim_overlap": 4,
        "artifact_overlap": 2,
        "evidence_path_overlap": 2,
    },
    "phrase_bonus_per_field": 6,
    "phrase_bonus_cap": 12,
    "stage1_candidate_cutoff": 25,
    "view_artifact_selection": "evidence_first",
    "view_excerpt_artifact_limit": 2,
    "view_excerpt_max_bytes": 65536,
    "view_excerpt_char_limit": 512,
    "quality_prior": {
        "summary_token_threshold": 8,
        "summary_bonus": 1,
        "evidence_backed_claim_bonus": 2,
        "descriptive_artifact_bonus": 1,
    },
    "abstention": {
        "enabled": True,
        "min_top_score": 6,
        "min_score_margin": 2,
    },
}


def harness_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def default_retrieval_profile_path(repo_root: pathlib.Path | None = None) -> pathlib.Path:
    return (repo_root or harness_root()) / "retrieval" / "active_profile.json"


def resolve_retrieval_profile_path(
    profile_path: str | pathlib.Path | None = None,
    *,
    repo_root: pathlib.Path | None = None,
) -> pathlib.Path:
    if profile_path is None:
        env_path = os_environ("HARNESS_RETRIEVAL_PROFILE_PATH")
        if env_path:
            profile_path = env_path
        else:
            return default_retrieval_profile_path(repo_root)
    candidate = pathlib.Path(profile_path)
    if candidate.is_absolute():
        return candidate
    return (repo_root or harness_root()) / candidate


def os_environ(name: str) -> str | None:
    try:
        import os
    except Exception:
        return None
    return os.environ.get(name)


def validate_retrieval_profile(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if not isinstance(payload.get("profile_id"), str) or not payload["profile_id"]:
        errors.append("retrieval profile must include non-empty profile_id")
    field_weights = payload.get("field_weights")
    if not isinstance(field_weights, dict):
        errors.append("retrieval profile field_weights must be an object")
    else:
        for key in DEFAULT_RETRIEVAL_PROFILE["field_weights"]:
            value = field_weights.get(key)
            if not isinstance(value, int) or value < 0:
                errors.append(
                    f"retrieval profile field_weights.{key} must be a non-negative integer"
                )
    for key in [
        "phrase_bonus_per_field",
        "phrase_bonus_cap",
        "stage1_candidate_cutoff",
        "view_excerpt_artifact_limit",
        "view_excerpt_max_bytes",
        "view_excerpt_char_limit",
    ]:
        value = payload.get(key)
        if not isinstance(value, int) or value < 1:
            errors.append(f"retrieval profile {key} must be a positive integer")
    if payload.get("view_artifact_selection") not in {"evidence_first", "descriptive_first"}:
        errors.append(
            "retrieval profile view_artifact_selection must be evidence_first or descriptive_first"
        )
    quality_prior = payload.get("quality_prior")
    if not isinstance(quality_prior, dict):
        errors.append("retrieval profile quality_prior must be an object")
    else:
        for key in [
            "summary_token_threshold",
            "summary_bonus",
            "evidence_backed_claim_bonus",
            "descriptive_artifact_bonus",
        ]:
            value = quality_prior.get(key)
            if not isinstance(value, int) or value < 0:
                errors.append(
                    f"retrieval profile quality_prior.{key} must be a non-negative integer"
                )
    abstention = payload.get("abstention")
    if not isinstance(abstention, dict):
        errors.append("retrieval profile abstention must be an object")
    else:
        if not isinstance(abstention.get("enabled"), bool):
            errors.append("retrieval profile abstention.enabled must be a boolean")
        for key in ["min_top_score", "min_score_margin"]:
            value = abstention.get(key)
            if not isinstance(value, int) or value < 0:
                errors.append(f"retrieval profile abstention.{key} must be a non-negative integer")
    return errors


def load_retrieval_profile(
    profile_path: str | pathlib.Path | None = None,
    *,
    repo_root: pathlib.Path | None = None,
) -> dict[str, Any]:
    path = resolve_retrieval_profile_path(profile_path, repo_root=repo_root)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("retrieval profile must be a JSON object")
    merged = json.loads(json.dumps(DEFAULT_RETRIEVAL_PROFILE))
    merged.update({key: value for key, value in payload.items() if key != "field_weights"})
    merged["field_weights"] = dict(DEFAULT_RETRIEVAL_PROFILE["field_weights"])
    merged["field_weights"].update(payload.get("field_weights", {}))
    merged["quality_prior"] = dict(DEFAULT_RETRIEVAL_PROFILE["quality_prior"])
    merged["quality_prior"].update(payload.get("quality_prior", {}))
    merged["abstention"] = dict(DEFAULT_RETRIEVAL_PROFILE["abstention"])
    merged["abstention"].update(payload.get("abstention", {}))
    errors = validate_retrieval_profile(merged)
    if errors:
        raise ValueError("; ".join(errors))
    merged["path"] = str(path)
    merged["profile_fingerprint"] = sha256_text(
        json.dumps(
            {
                key: value
                for key, value in merged.items()
                if key not in {"path", "profile_fingerprint"}
            },
            sort_keys=True,
            ensure_ascii=False,
        )
    )
    return merged


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
    phrase_source_lines = [sections["task_title"]]
    for key in ["goal", "constraints", "done"]:
        phrase_source_lines.extend(
            line.strip("- ").strip() for line in sections[key].splitlines() if line.strip()
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


def safe_text_artifact_excerpt(
    source_run_dir: pathlib.Path,
    rel_path: str,
    *,
    max_bytes: int,
    char_limit: int,
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
    return excerpt[:char_limit], size


def claim_records(result_payload: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in result_payload.get("claims", []):
        if not isinstance(item, dict):
            continue
        claim = item.get("claim")
        if not isinstance(claim, str) or not claim:
            continue
        evidence = item.get("evidence", [])
        if not isinstance(evidence, list):
            evidence = []
        records.append(
            {
                "claim": claim,
                "evidence": [
                    candidate for candidate in evidence if isinstance(candidate, str) and candidate
                ],
            }
        )
    return records


def claim_evidence_paths(result_payload: dict[str, Any]) -> set[str]:
    paths: set[str] = set()
    for item in claim_records(result_payload):
        for candidate in item["evidence"]:
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


def _artifact_excerpt_sort_key(artifact: dict[str, Any], selection: str) -> tuple[Any, ...]:
    description_token_count = sum(collections.Counter(artifact["description_tokens"]).values())
    if selection == "descriptive_first":
        return (-description_token_count, artifact["path"])
    return (0 if artifact["evidence_linked"] else 1, artifact["path"])


def _render_claim_lines(claim_payloads: list[dict[str, Any]]) -> list[str]:
    if not claim_payloads:
        return ["- none recorded"]
    lines: list[str] = []
    for item in claim_payloads:
        evidence = ", ".join(item["evidence"]) if item["evidence"] else "none recorded"
        lines.append(f"- {item['claim']} (evidence: {evidence})")
    return lines


def _render_artifact_lines(artifact_records: list[dict[str, Any]]) -> list[str]:
    if not artifact_records:
        return ["- none recorded"]
    lines: list[str] = []
    for artifact in artifact_records:
        description = artifact["description"].strip() or "No description recorded."
        line = f"- {artifact['path']}: {description}"
        if artifact.get("excerpt"):
            snippet = normalize_match_text(str(artifact["excerpt"]))[:180]
            line += f" | excerpt: {snippet}"
        lines.append(line)
    return lines


def render_retrieval_view(view: dict[str, Any]) -> str:
    lines = [
        f"# Retrieval View: {view['run_id']}",
        "",
        f"Task title: {view['task_title'] or 'Untitled task'}",
        f"Goal: {view['goal'] or 'No goal recorded.'}",
    ]
    if view.get("constraints"):
        lines.append(f"Constraints: {view['constraints']}")
    if view.get("done"):
        lines.append(f"Done: {view['done']}")
    lines.extend(
        [
            "",
            "## Summary",
            view["summary"] or "No summary recorded.",
            "",
            "## Claims",
            *_render_claim_lines(view["claim_records"]),
            "",
            "## Evidence Paths",
        ]
    )
    evidence_paths = view.get("evidence_paths", [])
    lines.extend([f"- {path}" for path in evidence_paths] or ["- none recorded"])
    lines.extend(["", "## Artifacts", *_render_artifact_lines(view["artifact_records"]), ""])
    return "\n".join(lines)


def build_retrieval_view(
    source_run_dir: pathlib.Path,
    *,
    query_sections: dict[str, str],
    result_payload: dict[str, Any],
    retrieval_profile: dict[str, Any],
) -> dict[str, Any]:
    claim_payloads = claim_records(result_payload)
    claims = [item["claim"] for item in claim_payloads]
    evidence_paths = sorted(claim_evidence_paths(result_payload))
    summary = str(result_payload.get("summary", ""))
    artifacts = declared_artifacts(result_payload)

    artifact_records: list[dict[str, Any]] = []
    descriptive_artifact_count = 0
    eligible_artifact_count = 0
    evidence_linked_artifact_count = 0

    for artifact in artifacts:
        rel_path = artifact["path"]
        description = artifact["description"]
        description_tokens = tokenize(description)
        if sum(description_tokens.values()) >= 3:
            descriptive_artifact_count += 1
        evidence_linked = rel_path in evidence_paths
        if evidence_linked:
            evidence_linked_artifact_count += 1
        safe_text = safe_text_artifact_excerpt(
            source_run_dir,
            rel_path,
            max_bytes=int(retrieval_profile["view_excerpt_max_bytes"]),
            char_limit=int(retrieval_profile["view_excerpt_char_limit"]),
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
                "excerpt": "",
                "excerpt_tokens": {},
                "excerpt_size_bytes": safe_text[1] if safe_text is not None else None,
                "safe_excerpt": safe_text[0] if safe_text is not None else "",
            }
        )

    excerpt_candidates = [
        artifact
        for artifact in artifact_records
        if artifact["evidence_linked"]
        and artifact["eligible_for_copy"]
        and artifact["safe_excerpt"]
    ]
    excerpt_candidates.sort(
        key=lambda artifact: _artifact_excerpt_sort_key(
            artifact,
            str(retrieval_profile["view_artifact_selection"]),
        )
    )
    for artifact in excerpt_candidates[: int(retrieval_profile["view_excerpt_artifact_limit"])]:
        artifact["excerpt"] = str(artifact["safe_excerpt"])
        artifact["excerpt_tokens"] = serialize_counter(tokenize(artifact["excerpt"]))
    for artifact in artifact_records:
        artifact.pop("safe_excerpt", None)

    view = {
        "run_id": source_run_dir.name,
        "task_title": query_sections["task_title"],
        "goal": query_sections["goal"],
        "constraints": query_sections["constraints"],
        "done": query_sections["done"],
        "summary": summary,
        "claims": claims,
        "claim_records": claim_payloads,
        "evidence_paths": evidence_paths,
        "artifact_records": artifact_records,
        "quality": {
            "summary_present": bool(summary.strip()),
            "claim_count": len(claims),
            "summary_token_count": sum(tokenize(summary).values()),
            "artifact_count": len(artifact_records),
            "descriptive_artifact_count": descriptive_artifact_count,
            "evidence_backed_claim_count": sum(1 for item in claim_payloads if item["evidence"]),
            "evidence_linked_artifact_count": evidence_linked_artifact_count,
            "eligible_artifact_count": eligible_artifact_count,
        },
    }
    view["text"] = render_retrieval_view(view)
    return view


def build_index_entry(
    source_run_dir: pathlib.Path,
    *,
    eval_policy: dict[str, Any] | None = None,
    retrieval_profile: dict[str, Any] | None = None,
    retrieval_mode: str | None = None,
) -> dict[str, Any]:
    profile = retrieval_profile or load_retrieval_profile()
    fingerprints = run_input_fingerprints(source_run_dir)
    entry: dict[str, Any] = {
        "indexed_at": now_utc(),
        "indexed_at_ms": _now_ms(),
        "index_version": INDEX_VERSION,
        "retrieval_profile_id": profile["profile_id"],
        "retrieval_profile_fingerprint": profile["profile_fingerprint"],
        "retrieval_mode": retrieval_mode or "hybrid_v1",
        "run_id": source_run_dir.name,
        "source_run_path": str(source_run_dir.resolve()),
        "input_fingerprints": fingerprints,
        "eligible": False,
        "skip_reason": None,
        "overall_pass": False,
        "query_text_hash": None,
        "summary": "",
        "claims": [],
        "claim_records": [],
        "artifact_paths": [],
        "artifact_records": [],
        "retrieval_view": {},
        "quality": {
            "summary_present": False,
            "claim_count": 0,
            "summary_token_count": 0,
            "artifact_count": 0,
            "descriptive_artifact_count": 0,
            "evidence_backed_claim_count": 0,
            "evidence_linked_artifact_count": 0,
            "eligible_artifact_count": 0,
        },
        "document_tokens": {},
        "encoder_text": "",
        "encoder_text_fingerprint": "",
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
    entry["overall_pass"] = True
    benchmark_eligibility = score_payload.get("benchmark_eligibility")
    if isinstance(benchmark_eligibility, dict) and benchmark_eligibility.get("eligible") is False:
        entry["skip_reason"] = "benchmark_ineligible"
        return entry
    if score_payload.get("result_json_valid_schema") is False:
        entry["skip_reason"] = "result_schema_invalid"
        return entry
    secret_scan = score_payload.get("secret_scan")
    if isinstance(secret_scan, dict) and secret_scan.get("findings"):
        entry["skip_reason"] = "secret_scan_findings"
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
    view = build_retrieval_view(
        source_run_dir,
        query_sections=query_sections,
        result_payload=result_payload,
        retrieval_profile=profile,
    )
    entry["query_text_hash"] = sha256_text(build_query_text(parsed_task))
    entry["summary"] = view["summary"]
    entry["claims"] = list(view["claims"])
    entry["claim_records"] = list(view["claim_records"])
    entry["artifact_paths"] = [artifact["path"] for artifact in view["artifact_records"]]
    entry["artifact_records"] = list(view["artifact_records"])
    entry["retrieval_view"] = view
    entry["quality"] = dict(view["quality"])
    entry["encoder_text"] = str(view["text"])
    entry["encoder_text_fingerprint"] = sha256_text(entry["encoder_text"])
    artifact_description_tokens = collections.Counter()
    artifact_excerpt_tokens = collections.Counter()
    for artifact in view["artifact_records"]:
        artifact_description_tokens.update(collections.Counter(artifact["description_tokens"]))
        artifact_excerpt_tokens.update(collections.Counter(artifact["excerpt_tokens"]))

    entry["eligible"] = True
    entry["overall_pass"] = True
    entry["document_tokens"] = {
        "task_title": serialize_counter(tokenize(view["task_title"])),
        "goal": serialize_counter(tokenize(view["goal"])),
        "constraints": serialize_counter(tokenize(view["constraints"])),
        "done": serialize_counter(tokenize(view["done"])),
        "summary": serialize_counter(tokenize(view["summary"])),
        "claims": serialize_counter(tokenize("\n".join(view["claims"]))),
        "artifact_descriptions": serialize_counter(artifact_description_tokens),
        "artifact_excerpts": serialize_counter(artifact_excerpt_tokens),
        "evidence_paths": serialize_counter(tokenize("\n".join(view["evidence_paths"]))),
        "view_text": serialize_counter(tokenize(view["text"])),
    }
    if int(view["quality"].get("evidence_backed_claim_count", 0)) < 1:
        entry["skip_reason"] = "missing_evidence_backed_claim"
        return entry
    entry["source_snapshot_fingerprint"] = entry_source_snapshot_fingerprint(entry)
    return entry


def entry_is_fresh(
    entry: dict[str, Any],
    source_run_dir: pathlib.Path,
    *,
    retrieval_profile: dict[str, Any],
    ttl_seconds: int,
) -> bool:
    if ttl_seconds > 0 and entry.get("indexed_at_ms"):
        try:
            age_ms = _now_ms() - int(entry["indexed_at_ms"])
        except (TypeError, ValueError):
            return False
        if age_ms > ttl_seconds * 1000:
            return False
    return (
        entry.get("index_version") == INDEX_VERSION
        and entry.get("retrieval_profile_fingerprint") == retrieval_profile["profile_fingerprint"]
        and entry.get("input_fingerprints") == run_input_fingerprints(source_run_dir)
    )


def write_index_entry(index_root: pathlib.Path, entry: dict[str, Any]) -> pathlib.Path:
    path = index_path(index_root, str(entry["run_id"]))
    write_json(path, entry)
    return path


def entry_source_snapshot_fingerprint(entry: dict[str, Any]) -> str:
    return sha256_text(
        json.dumps(
            {
                key: entry.get(key)
                for key in ["run_id", "indexed_at_ms", "input_fingerprints", "query_text_hash"]
            },
            sort_keys=True,
            ensure_ascii=False,
        )
    )


def _entry_sort_key(entry: dict[str, Any]) -> tuple[int, str]:
    return (-int(entry.get("indexed_at_ms") or 0), str(entry.get("run_id") or ""))


def _entry_payload_bytes(entry: dict[str, Any]) -> int:
    try:
        return len(json.dumps(entry, sort_keys=True, ensure_ascii=False).encode("utf-8"))
    except TypeError:
        return len(json.dumps({}, sort_keys=True).encode("utf-8"))


def _normalize_run_dirs(run_dirs: list[pathlib.Path]) -> list[pathlib.Path]:
    seen: set[str] = set()
    normalized: list[pathlib.Path] = []
    for run_dir in sorted(run_dirs, key=lambda path: path.name):
        run_id = run_dir.name
        if run_id in seen:
            continue
        seen.add(run_id)
        normalized.append(run_dir)
    return normalized


def _build_index_provenance(
    entries: list[dict[str, Any]],
    *,
    retrieval_profile: dict[str, Any],
    retrieval_mode: str,
    index_ttl_seconds: int,
    max_index_entries: int,
    max_index_bytes: int,
    index_mode: str,
) -> str:
    source_fingerprints = [
        {
            "run_id": entry.get("run_id"),
            "source_snapshot_fingerprint": entry.get("source_snapshot_fingerprint"),
        }
        for entry in sorted(entries, key=lambda item: str(item.get("run_id", "")))
    ]
    return sha256_text(
        json.dumps(
            {
                "index_version": INDEX_VERSION,
                "index_mode": index_mode,
                "retrieval_profile_id": retrieval_profile["profile_id"],
                "retrieval_profile_fingerprint": retrieval_profile["profile_fingerprint"],
                "retrieval_mode": retrieval_mode,
                "index_ttl_seconds": index_ttl_seconds,
                "max_index_entries": max_index_entries,
                "max_index_bytes": max_index_bytes,
                "entries": source_fingerprints,
            },
            sort_keys=True,
            ensure_ascii=False,
        )
    )


def retrieval_candidate_is_dense(retrieval_candidate: dict[str, Any] | None) -> bool:
    if not retrieval_candidate:
        return False
    runtime = candidate_runtime(retrieval_candidate)
    stage1 = runtime.get("stage1", {})
    retriever = runtime.get("retriever", {})
    return (
        str(runtime.get("retriever_version", "")) == "dense-hashed-shared-encoder-v1"
        or (isinstance(stage1, dict) and str(stage1.get("type", "")) == "dense-v1")
        or (isinstance(retriever, dict) and str(retriever.get("retriever_type", "")) == "dense-v1")
    )


def dense_stage1_cache_root(
    index_root: pathlib.Path,
    retrieval_candidate: dict[str, Any],
) -> pathlib.Path:
    candidate_id = str(retrieval_candidate.get("candidate_id", "retrieval-candidate")).strip()
    return index_root / DENSE_STAGE1_CACHE_DIR / candidate_id


def _dense_stage1_cache_path(cache_root: pathlib.Path, run_id: str) -> pathlib.Path:
    return cache_root / f"{run_id}.json"


def _dense_stage1_embedding_for_entry(
    cache_root: pathlib.Path,
    entry: dict[str, Any],
    *,
    dense_runtime: dict[str, Any],
) -> numpy.ndarray:
    cache_root.mkdir(parents=True, exist_ok=True)
    cache_path = _dense_stage1_cache_path(cache_root, str(entry["run_id"]))
    expected = {
        "candidate_artifact_fingerprint": dense_runtime["artifact_fingerprint"],
        "source_snapshot_fingerprint": entry.get("source_snapshot_fingerprint"),
        "encoder_text_fingerprint": entry.get("encoder_text_fingerprint"),
        "embedding_dim": dense_runtime["embedding_dim"],
    }
    cached = parse_json_file(cache_path)
    if isinstance(cached, dict):
        embedding = cached.get("embedding")
        metadata_matches = all(cached.get(key) == value for key, value in expected.items())
        if metadata_matches and isinstance(embedding, list):
            array = numpy.asarray(embedding, dtype=numpy.float32)
            if array.shape == (dense_runtime["embedding_dim"],):
                return array

    encoded = encode_text(
        str(entry.get("encoder_text", "")),
        hash_dim=dense_runtime["hash_dim"],
        feature_weights=dense_runtime["feature_weights"],
        projection=dense_runtime["projection"],
    )
    write_json(
        cache_path,
        {
            **expected,
            "run_id": entry["run_id"],
            "embedding": [round(float(value), 8) for value in encoded.tolist()],
        },
    )
    return encoded


def dense_stage1_ranking(
    query_text: str,
    entries: list[dict[str, Any]],
    *,
    index_root: pathlib.Path,
    retrieval_candidate: dict[str, Any],
    stage1_k: int,
) -> dict[str, Any]:
    try:
        dense_runtime = load_dense_retriever_runtime(
            dict(candidate_runtime(retrieval_candidate).get("retriever", {}))
        )
    except Exception as exc:
        return {
            "ok": False,
            "fallback_reason": str(exc),
            "scores": {},
            "ordered_run_ids": [],
            "top_scores": [],
            "stage1_k": stage1_k,
        }

    cache_root = dense_stage1_cache_root(index_root, retrieval_candidate)
    query_embedding = encode_text(
        query_text,
        hash_dim=dense_runtime["hash_dim"],
        feature_weights=dense_runtime["feature_weights"],
        projection=dense_runtime["projection"],
    )
    ranked: list[tuple[str, float]] = []
    for entry in entries:
        if not entry.get("eligible"):
            continue
        embedding = _dense_stage1_embedding_for_entry(
            cache_root,
            entry,
            dense_runtime=dense_runtime,
        )
        score = float(numpy.dot(query_embedding, embedding))
        ranked.append((str(entry["run_id"]), round(score, 6)))
    ranked.sort(key=lambda item: (-item[1], item[0]))
    limited = ranked[: max(1, stage1_k)]
    return {
        "ok": True,
        "fallback_reason": None,
        "scores": {run_id: score for run_id, score in limited},
        "ordered_run_ids": [run_id for run_id, _ in limited],
        "top_scores": [
            {"run_id": run_id, "dense_stage1_score": score} for run_id, score in limited[:5]
        ],
        "stage1_k": stage1_k,
        "cache_root": str(cache_root),
    }


def stage1_candidate_score(query: dict[str, Any], entry: dict[str, Any]) -> int:
    field_tokens = collections.Counter(entry.get("document_tokens", {}).get("view_text", {}))
    return lexical_score(query["candidate_tokens"], dict(field_tokens))


def candidate_query_text(query: dict[str, Any]) -> str:
    sections = query.get("sections", {}) if isinstance(query.get("sections"), dict) else {}
    explicit = str(query.get("text", "")).strip()
    if explicit:
        return explicit
    return "\n".join(
        [
            str(query.get("task_title", "")),
            str(sections.get("Goal", "")),
            str(sections.get("Constraints", "")),
            str(sections.get("Done", "")),
        ]
    ).strip()


def candidate_document_text(entry: dict[str, Any]) -> str:
    explicit = str(entry.get("retrieval_view", {}).get("text", "")).strip()
    if explicit:
        return explicit
    parts = [str(entry.get("summary", "")).strip()]
    parts.extend(str(item).strip() for item in entry.get("claims", []) if str(item).strip())
    parts.extend(
        str(item).strip()
        for item in entry.get("retrieval_view", {}).get("evidence_paths", [])
        if str(item).strip()
    )
    for artifact in entry.get("artifact_records", []):
        if not isinstance(artifact, dict):
            continue
        parts.append(str(artifact.get("description", "")).strip())
        parts.append(str(artifact.get("excerpt", "")).strip())
    return "\n".join(part for part in parts if part).strip()


def _candidate_reranker_runtime(retrieval_candidate: dict[str, Any]) -> dict[str, Any] | None:
    runtime = candidate_runtime(retrieval_candidate)
    reranker = runtime.get("reranker", {})
    if not isinstance(reranker, dict):
        return None
    encoder_payload = reranker.get("encoder")
    if isinstance(encoder_payload, dict) and encoder_payload.get("feature_weights_path"):
        return load_text_encoder_runtime(encoder_payload)
    artifact_paths = reranker.get("artifact_paths", {})
    if isinstance(artifact_paths, dict) and artifact_paths.get("feature_weights_path"):
        payload = {
            "feature_weights_path": artifact_paths.get("feature_weights_path"),
            "config_path": artifact_paths.get("config_path"),
            "hash_dim": reranker.get("hash_dim", DEFAULT_HASH_DIM),
            "embedding_dim": reranker.get("embedding_dim", DEFAULT_EMBEDDING_DIM),
            "projection_seed": reranker.get("projection_seed", DEFAULT_PROJECTION_SEED),
            "artifact_fingerprint": reranker.get("artifact_fingerprint", ""),
            "score_mode": reranker.get("score_mode", "cosine"),
        }
        return load_text_encoder_runtime(payload)
    return None


def _field_overlap_score(
    query_tokens: collections.Counter[str],
    entry: dict[str, Any],
    entry_field: str,
    weight: int,
) -> int:
    field_tokens = collections.Counter(entry.get("document_tokens", {}).get(entry_field, {}))
    return weight * lexical_score(query_tokens, dict(field_tokens))


def _field_phrase_hit(query: dict[str, Any], texts: list[str]) -> bool:
    haystacks = [normalize_match_text(text) for text in texts if text]
    if not haystacks:
        return False
    return any(phrase in haystack for phrase in query["phrases"] for haystack in haystacks)


def score_index_entry(
    query: dict[str, Any],
    entry: dict[str, Any],
    retrieval_profile: dict[str, Any] | None = None,
    retrieval_candidate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = retrieval_profile or load_retrieval_profile()
    weights = profile["field_weights"]
    artifact_texts = []
    for artifact in entry.get("artifact_records", []):
        artifact_texts.append(str(artifact.get("description", "")))
        artifact_texts.append(str(artifact.get("excerpt", "")))

    score_breakdown = {
        "task_title_overlap": _field_overlap_score(
            query["field_tokens"]["task_title"],
            entry,
            "task_title",
            int(weights["task_title_overlap"]),
        ),
        "goal_overlap": _field_overlap_score(
            query["field_tokens"]["goal"], entry, "goal", int(weights["goal_overlap"])
        ),
        "constraints_overlap": _field_overlap_score(
            query["field_tokens"]["constraints"],
            entry,
            "constraints",
            int(weights["constraints_overlap"]),
        ),
        "done_overlap": _field_overlap_score(
            query["field_tokens"]["done"], entry, "done", int(weights["done_overlap"])
        ),
        "summary_overlap": _field_overlap_score(
            query["candidate_tokens"], entry, "summary", int(weights["summary_overlap"])
        ),
        "claim_overlap": _field_overlap_score(
            query["candidate_tokens"], entry, "claims", int(weights["claim_overlap"])
        ),
        "artifact_overlap": int(weights["artifact_overlap"])
        * lexical_score(
            query["candidate_tokens"],
            dict(
                merge_counters(
                    collections.Counter(
                        entry.get("document_tokens", {}).get("artifact_descriptions", {})
                    ),
                    collections.Counter(
                        entry.get("document_tokens", {}).get("artifact_excerpts", {})
                    ),
                )
            ),
        ),
        "evidence_path_overlap": _field_overlap_score(
            query["candidate_tokens"],
            entry,
            "evidence_paths",
            int(weights["evidence_path_overlap"]),
        ),
        "phrase_bonus": 0,
        "quality_prior": 0,
    }
    phrase_hits = {
        "summary": _field_phrase_hit(query, [str(entry.get("summary", ""))]),
        "claims": _field_phrase_hit(query, list(entry.get("claims", []))),
        "artifacts": _field_phrase_hit(query, artifact_texts),
        "evidence_paths": _field_phrase_hit(
            query,
            list(entry.get("retrieval_view", {}).get("evidence_paths", [])),
        ),
    }
    score_breakdown["phrase_bonus"] = min(
        int(profile["phrase_bonus_cap"]),
        int(profile["phrase_bonus_per_field"])
        * sum(1 for matched in phrase_hits.values() if matched),
    )

    quality = entry.get("quality", {})
    quality_prior = profile["quality_prior"]
    if int(quality.get("summary_token_count", 0)) >= int(quality_prior["summary_token_threshold"]):
        score_breakdown["quality_prior"] += int(quality_prior["summary_bonus"])
    if int(quality.get("evidence_backed_claim_count", 0)) >= 1:
        score_breakdown["quality_prior"] += int(quality_prior["evidence_backed_claim_bonus"])
    if int(quality.get("descriptive_artifact_count", 0)) >= 1:
        score_breakdown["quality_prior"] += int(quality_prior["descriptive_artifact_bonus"])

    total_score = sum(score_breakdown.values())
    candidate_features = {
        **{key: float(value) for key, value in score_breakdown.items()},
        "stage1_score": float(stage1_candidate_score(query, entry)),
        "summary_token_count": float(quality.get("summary_token_count", 0)),
        "claim_count": float(quality.get("claim_count", 0)),
        "artifact_count": float(quality.get("artifact_count", 0)),
        "evidence_backed_claim_count": float(quality.get("evidence_backed_claim_count", 0)),
        "descriptive_artifact_count": float(quality.get("descriptive_artifact_count", 0)),
    }
    usefulness_probability = None
    candidate_score = None
    candidate_summary: dict[str, Any] = {}
    effective_mode = effective_candidate_mode(retrieval_candidate)
    if retrieval_candidate and effective_mode in {"shadow", "active"}:
        runtime = candidate_runtime(retrieval_candidate)
        reranker = dict(runtime.get("reranker", {}))
        bias = float(reranker.get("bias", 0.0))
        encoder_runtime = None
        try:
            encoder_runtime = _candidate_reranker_runtime(retrieval_candidate)
        except Exception:
            encoder_runtime = None
        if encoder_runtime is not None:
            linear_score = (
                score_pair(
                    candidate_query_text(query),
                    candidate_document_text(entry),
                    runtime=encoder_runtime,
                )
                + bias
            )
            usefulness_probability = round(sigmoid(linear_score), 6)
            candidate_score = round(float(linear_score), 6)
        else:
            feature_weights = dict(reranker.get("feature_weights", {}))
            linear_score = bias
            for feature_name, feature_value in candidate_features.items():
                linear_score += float(feature_weights.get(feature_name, 0.0)) * float(feature_value)
            usefulness_probability = round(sigmoid(linear_score), 6)
            candidate_score = usefulness_probability
        candidate_summary = {
            "candidate_id": retrieval_candidate.get("candidate_id"),
            "mode": effective_mode,
            "retriever_version": runtime.get("retriever_version"),
            "reranker_version": runtime.get("reranker_version"),
            "abstention_model_version": runtime.get("abstention_model_version"),
        }
    return {
        "run_id": entry["run_id"],
        "total_score": total_score,
        "score_breakdown": score_breakdown,
        "phrase_hits": phrase_hits,
        "stage1_score": candidate_features["stage1_score"],
        "candidate_score": candidate_score,
        "usefulness_probability": usefulness_probability,
        "candidate_features": candidate_features,
        "candidate_summary": candidate_summary,
    }


def rank_index_entries(
    query: dict[str, Any],
    entries: list[dict[str, Any]],
    *,
    retrieval_profile: dict[str, Any],
    max_candidates: int | None = None,
    retrieval_candidate: dict[str, Any] | None = None,
    prefer_candidate_scores: bool = False,
    stage1_score_overrides: dict[str, float] | None = None,
    use_stage1_overrides: bool = False,
) -> list[dict[str, Any]]:
    cutoff = int(retrieval_profile["stage1_candidate_cutoff"])
    if max_candidates is not None:
        cutoff = min(cutoff, int(max_candidates))
    if retrieval_candidate:
        selection = dict(candidate_runtime(retrieval_candidate).get("selection", {}))
        stage1 = dict(candidate_runtime(retrieval_candidate).get("stage1", {}))
        cutoff = min(
            cutoff,
            int(selection.get("stage1_k", stage1.get("max_candidates", cutoff))),
        )

    stage1_candidates: list[dict[str, Any]] = []
    for entry in entries:
        if not entry.get("eligible"):
            continue
        scored = score_index_entry(
            query,
            entry,
            retrieval_profile=retrieval_profile,
            retrieval_candidate=retrieval_candidate,
        )
        dense_stage1_score = None
        if stage1_score_overrides is not None:
            dense_stage1_score = stage1_score_overrides.get(str(entry["run_id"]))
        if use_stage1_overrides:
            if dense_stage1_score is None:
                continue
            stage1_selection_score = float(dense_stage1_score)
        else:
            if scored["stage1_score"] <= 0:
                continue
            stage1_selection_score = float(scored["stage1_score"])

        scored_with_stage1 = {
            **scored,
            "stage1_selection_score": stage1_selection_score,
            "dense_stage1_score": dense_stage1_score,
        }
        if scored_with_stage1["stage1_selection_score"] <= 0:
            continue
        stage1_candidates.append(
            {
                **scored_with_stage1,
                "summary": str(entry.get("summary", "")),
                "claims": list(entry.get("claims", [])),
                "claim_records": list(entry.get("claim_records", [])),
                "artifact_records": list(entry.get("artifact_records", [])),
                "retrieval_view": dict(entry.get("retrieval_view", {})),
                "source_run_dir": pathlib.Path(str(entry["source_run_path"])),
            }
        )

    stage1_candidates.sort(
        key=lambda item: (-float(item["stage1_selection_score"]), item["run_id"])
    )
    rerank_pool = stage1_candidates[:cutoff]
    if (
        prefer_candidate_scores
        and retrieval_candidate
        and effective_candidate_mode(retrieval_candidate) == "active"
    ):
        rerank_pool.sort(
            key=lambda item: (
                -float(item.get("candidate_score") or 0.0),
                -item["stage1_score"],
                item["run_id"],
            )
        )
    else:
        rerank_pool.sort(
            key=lambda item: (
                -item["total_score"],
                -item["stage1_score"],
                item["run_id"],
            )
        )
    return rerank_pool


def load_runtime_retrieval_candidate(
    *,
    repo_root: pathlib.Path | None = None,
) -> dict[str, Any] | None:
    return load_candidate_manifest("retrieval", repo_root=repo_root)


def sync_retrieval_index(
    runs_dir: pathlib.Path,
    *,
    exclude_run_id: str | None = None,
    eval_policy: dict[str, Any] | None = None,
    force_rebuild: bool = False,
    retrieval_profile: dict[str, Any] | None = None,
    index_ttl_seconds: int | None = None,
    max_index_entries: int | None = None,
    max_index_bytes: int | None = None,
    ttl_seconds: int | None = None,
    max_entries: int | None = None,
    max_bytes: int | None = None,
    retrieval_mode: str | None = None,
) -> dict[str, Any]:
    runs_dir = runs_root(runs_dir)
    profile = retrieval_profile or load_retrieval_profile(repo_root=runs_dir.parent)
    index_root = retrieval_index_root_for_runs(runs_dir)
    retrieval_mode = retrieval_mode or "hybrid_v1"
    ttl_seconds = _to_positive_int(
        index_ttl_seconds if index_ttl_seconds is not None else ttl_seconds,
        default=INDEX_DEFAULT_TTL_SECONDS,
    )
    max_entries = _to_positive_int(
        max_index_entries if max_index_entries is not None else max_entries,
        default=INDEX_DEFAULT_MAX_ENTRIES,
    )
    max_bytes = _to_positive_int(
        max_index_bytes if max_index_bytes is not None else max_bytes,
        default=0,
    )
    had_index = index_root.exists()
    index_rebuild_count = 0

    if force_rebuild and index_root.exists():
        shutil.rmtree(index_root)
        index_rebuild_count += 1
        had_index = False

    index_root.mkdir(parents=True, exist_ok=True)

    run_dirs = _normalize_run_dirs(candidate_run_dirs(runs_dir, exclude_run_id=exclude_run_id))
    run_ids = {path.name for path in run_dirs}
    existing_index_paths = {path.stem: path for path in index_root.glob("*.json") if path.is_file()}
    parse_failures = 0
    corrupted_paths: list[pathlib.Path] = []
    existing_entries: dict[str, dict[str, Any]] = {}

    for run_id, path in existing_index_paths.items():
        raw = parse_json_file(path)
        if not isinstance(raw, dict):
            parse_failures += 1
            corrupted_paths.append(path)
            continue
        existing_entries[run_id] = raw

    if parse_failures > 0:
        index_rebuild_count += 1
        for path in corrupted_paths:
            try:
                path.unlink()
            except OSError:
                pass
        existing_entries.clear()
        existing_index_paths = {}
        if index_root.exists():
            shutil.rmtree(index_root)
            index_root.mkdir(parents=True, exist_ok=True)

    evicted_count = 0
    for run_id, path in existing_index_paths.items():
        if run_id in run_ids:
            continue
        try:
            path.unlink()
        except OSError:
            pass
        else:
            evicted_count += 1

    stale_removed = 0
    refreshed_count = 0
    refreshed_entries: dict[str, dict[str, Any]] = {}
    stale_run_dirs: list[pathlib.Path] = []
    for run_dir in run_dirs:
        existing_entry = load_index_entry(index_root, run_dir.name)
        if (
            force_rebuild
            or existing_entry is None
            or not entry_is_fresh(
                existing_entry,
                run_dir,
                retrieval_profile=profile,
                ttl_seconds=ttl_seconds,
            )
        ):
            stale_run_dirs.append(run_dir)
            stale_removed += 1

    if stale_run_dirs:
        max_workers = min(8, len(stale_run_dirs))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_run_id = {
                executor.submit(
                    build_index_entry,
                    run_dir,
                    eval_policy=eval_policy,
                    retrieval_profile=profile,
                    retrieval_mode=retrieval_mode,
                ): run_dir.name
                for run_dir in stale_run_dirs
            }
            for future in concurrent.futures.as_completed(future_to_run_id):
                entry = future.result()
                refreshed_entries[str(entry["run_id"])] = entry
                write_index_entry(index_root, entry)
                refreshed_count += 1

    candidate_entries: dict[str, dict[str, Any]] = {}
    for run_dir in run_dirs:
        run_id = run_dir.name
        if run_id in refreshed_entries:
            candidate_entries[run_id] = refreshed_entries[run_id]
            continue
        existing_entry = existing_entries.get(run_id)
        if existing_entry is not None:
            candidate_entries[run_id] = existing_entry

    compacted_removed = 0
    compacted_kept = len(candidate_entries)
    entries = [entry for _, entry in candidate_entries.items()]
    entries.sort(key=_entry_sort_key)

    if max_entries > 0 and len(entries) > max_entries:
        dropped_entries = entries[max_entries:]
        compacted_removed += len(dropped_entries)
        entries = entries[:max_entries]
        for entry in dropped_entries:
            path = index_path(index_root, str(entry["run_id"]))
            try:
                path.unlink()
            except OSError:
                pass

    if max_bytes > 0:
        sorted_oldest_first = sorted(
            entries,
            key=lambda item: (int(item.get("indexed_at_ms") or 0), str(item.get("run_id", ""))),
        )
        total_bytes = sum(_entry_payload_bytes(entry) for entry in sorted_oldest_first)
        while sorted_oldest_first and total_bytes > max_bytes:
            candidate = sorted_oldest_first.pop(0)
            sorted_oldest_first_bytes = _entry_payload_bytes(candidate)
            path = index_path(index_root, str(candidate["run_id"]))
            try:
                path.unlink()
            except OSError:
                pass
            compacted_removed += 1
            compacted_kept -= 1
            total_bytes -= sorted_oldest_first_bytes
        entries = sorted_oldest_first

    compacted_kept = len(entries)
    kept_ids = {str(entry.get("run_id", "")) for entry in entries}
    for run_dir in run_dirs:
        path = index_path(index_root, run_dir.name)
        if run_dir.name in kept_ids:
            if run_dir.name in refreshed_entries:
                write_index_entry(index_root, refreshed_entries[run_dir.name])
            elif run_dir.name in existing_entries:
                write_index_entry(index_root, existing_entries[run_dir.name])
            continue
        try:
            if path.exists():
                path.unlink()
        except OSError:
            pass

    if force_rebuild or index_rebuild_count > 0 or not had_index:
        index_mode = "cold_build"
    elif refreshed_count > 0 or evicted_count > 0 or compacted_removed > 0 or stale_removed > 0:
        index_mode = "incremental_refresh"
    else:
        index_mode = "warm_reuse"

    source_snapshot_fingerprints = {
        str(entry.get("run_id")): str(entry.get("source_snapshot_fingerprint")) for entry in entries
    }
    index_state_token = _build_index_provenance(
        entries,
        retrieval_profile=profile,
        retrieval_mode=retrieval_mode,
        index_ttl_seconds=ttl_seconds,
        max_index_entries=max_entries,
        max_index_bytes=max_bytes,
        index_mode=index_mode,
    )

    return {
        "index_root": index_root,
        "index_version": INDEX_VERSION,
        "index_mode": index_mode,
        "candidate_run_count": len(run_dirs),
        "index_ttl_seconds": ttl_seconds,
        "max_index_entries": max_entries,
        "max_index_bytes": max_bytes,
        "index_rebuild_count": index_rebuild_count,
        "stale_removed": stale_removed,
        "compacted_removed": compacted_removed,
        "compacted_kept": compacted_kept,
        "index_refreshes": refreshed_count,
        "refreshed_run_count": refreshed_count,
        "evicted_run_count": evicted_count,
        "source_snapshot_fingerprints": source_snapshot_fingerprints,
        "index_provenance_token": index_state_token,
        "entries": entries,
        "retrieval_profile_id": profile["profile_id"],
        "retrieval_profile_fingerprint": profile["profile_fingerprint"],
        "retrieval_mode": retrieval_mode,
    }
