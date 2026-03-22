#!/usr/bin/env python3
from __future__ import annotations

import collections
import json
import pathlib
import re
import shutil
import sys
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

SKIPPED_TOP_LEVEL = {"home", "session", "recovery"}
SKIPPED_FILES = {
    "transcript.jsonl",
    "pi.stderr.log",
    "patch.diff",
    "git.status.txt",
    "pi.exit_code.txt",
    "run-events.jsonl",
}
TOKEN_RE = re.compile(r"[a-z0-9]+")


def usage() -> int:
    print("usage: prepare-context.py /path/to/run-dir [policy-path]", file=sys.stderr)
    return 2


def tokenize(text: str) -> collections.Counter[str]:
    return collections.Counter(TOKEN_RE.findall(text.lower()))


def lexical_score(query: collections.Counter[str], document: collections.Counter[str]) -> int:
    return sum(min(count, document[token]) for token, count in query.items())


def build_query_text(parsed_task: dict[str, Any]) -> str:
    sections = parsed_task.get("sections", {})
    return "\n".join(
        [
            sections.get("Goal", ""),
            sections.get("Constraints", ""),
            sections.get("Done", ""),
        ]
    ).strip()


def parse_json_file(path: pathlib.Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def candidate_runs(run_dir: pathlib.Path) -> list[pathlib.Path]:
    runs_root = run_dir.parent
    if runs_root.name != "runs":
        return []
    return sorted(
        [path for path in runs_root.iterdir() if path.is_dir() and path.name != run_dir.name],
        key=lambda path: path.name,
    )


def build_source_text(parsed_task: dict[str, Any], result_payload: dict[str, Any]) -> str:
    sections = parsed_task.get("sections", {})
    artifact_descriptions = "\n".join(
        item.get("description", "")
        for item in result_payload.get("artifacts", [])
        if isinstance(item, dict)
    )
    claims = "\n".join(
        item.get("claim", "") for item in result_payload.get("claims", []) if isinstance(item, dict)
    )
    return "\n".join(
        [
            sections.get("Goal", ""),
            sections.get("Done", ""),
            result_payload.get("summary", ""),
            claims,
            artifact_descriptions,
        ]
    ).strip()


def is_utf8_text(path: pathlib.Path) -> bool:
    try:
        path.read_text(encoding="utf-8")
    except Exception:
        return False
    return True


def copy_context_artifacts(
    *,
    source_run_dir: pathlib.Path,
    destination_root: pathlib.Path,
    result_payload: dict[str, Any],
    max_artifacts: int,
    max_bytes: int,
) -> list[dict[str, Any]]:
    copied: list[dict[str, Any]] = []
    for artifact in result_payload.get("artifacts", []):
        if len(copied) >= max_artifacts:
            break
        if not isinstance(artifact, dict):
            continue
        rel_path = artifact.get("path")
        if not isinstance(rel_path, str) or not rel_path:
            continue
        validated = evaluate_required_artifact_path(source_run_dir, rel_path)
        if not validated["valid"]:
            continue
        source_path = (source_run_dir / rel_path).resolve()
        if not source_path.exists() or not source_path.is_file():
            continue
        if source_path.name in SKIPPED_FILES:
            continue
        if any(part in SKIPPED_TOP_LEVEL for part in pathlib.Path(rel_path).parts):
            continue
        if source_path.stat().st_size > max_bytes:
            continue
        if not is_utf8_text(source_path):
            continue

        destination_path = destination_root / rel_path
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, destination_path)
        copied.append(
            {
                "source_path": rel_path,
                "destination_path": str(destination_path),
                "sha256": sha256_file(destination_path),
            }
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
                f"- Retrieval score: {item['score']}",
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
    query_tokens = tokenize(query_text)
    selected_sources: list[dict[str, Any]] = []
    skipped_sources = 0

    for source_run_dir in candidate_runs(run_dir):
        task_path = source_run_dir / "task.md"
        result_path = source_run_dir / "result.json"
        score_path = source_run_dir / "score.json"
        if not task_path.is_file() or not result_path.is_file() or not score_path.is_file():
            skipped_sources += 1
            continue

        score_payload = parse_json_file(score_path)
        result_payload = parse_json_file(result_path)
        if not isinstance(score_payload, dict) or score_payload.get("overall_pass") is not True:
            skipped_sources += 1
            continue
        if not isinstance(result_payload, dict):
            skipped_sources += 1
            continue

        parsed_source_task = parse_task_file(task_path, eval_policy=policy)
        if not parsed_source_task["ok"]:
            skipped_sources += 1
            continue

        score = lexical_score(
            query_tokens,
            tokenize(build_source_text(parsed_source_task, result_payload)),
        )
        if score <= 0:
            continue

        selected_sources.append(
            {
                "run_id": source_run_dir.name,
                "score": score,
                "summary": str(result_payload.get("summary", "")),
                "claims": [
                    item.get("claim", "")
                    for item in result_payload.get("claims", [])
                    if isinstance(item, dict) and item.get("claim")
                ],
                "source_run_dir": source_run_dir,
                "result_payload": result_payload,
            }
        )

    selected_sources.sort(key=lambda item: (-item["score"], item["run_id"]))
    selected_sources = selected_sources[: retrieval["max_source_runs"]]

    selected_manifest_entries: list[dict[str, Any]] = []
    selected_run_ids: list[str] = []

    for item in selected_sources:
        source_run_dir = item["source_run_dir"]
        destination_root = context_dir / "source-runs" / item["run_id"]
        destination_root.mkdir(parents=True, exist_ok=True)
        copied_core_files: list[dict[str, Any]] = []
        for rel_path in ["task.md", "result.json", "score.json"]:
            source_path = source_run_dir / rel_path
            destination_path = destination_root / rel_path
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_path, destination_path)
            copied_core_files.append(
                {
                    "source_path": rel_path,
                    "destination_path": str(destination_path),
                    "sha256": sha256_file(destination_path),
                }
            )

        copied_artifacts = copy_context_artifacts(
            source_run_dir=source_run_dir,
            destination_root=destination_root,
            result_payload=item["result_payload"],
            max_artifacts=retrieval["max_artifacts_per_run"],
            max_bytes=retrieval["max_artifact_bytes"],
        )
        selected_manifest_entries.append(
            {
                "run_id": item["run_id"],
                "score": item["score"],
                "summary": item["summary"],
                "copied_files": copied_core_files + copied_artifacts,
            }
        )
        item["copied_files"] = copied_core_files + copied_artifacts
        selected_run_ids.append(item["run_id"])

    manifest_path = run_dir / settings["context_manifest_path"]
    summary_path = run_dir / settings["context_summary_path"]

    manifest_payload = {
        "query_text_hash": sha256_text(query_text),
        "selected_source_run_ids": selected_run_ids,
        "selected_sources": selected_manifest_entries,
        "skipped_sources_count": skipped_sources,
    }
    write_json(manifest_path, manifest_payload)
    summary_path.write_text(build_summary(selected_sources), encoding="utf-8")
    print(str(manifest_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
