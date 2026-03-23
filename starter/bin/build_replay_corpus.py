#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import re
from typing import Any

SECRET_PATTERNS = [
    re.compile(r"(?i)(authorization:\s*bearer\s+)[A-Za-z0-9._-]+"),
    re.compile(r"(?i)(api[_-]?key['\"]?\s*[:=]\s*['\"]?)[^'\"\s]+"),
    re.compile(r"(?i)(token['\"]?\s*[:=]\s*['\"]?)[^'\"\s]+"),
    re.compile(r"(?i)(HARNESS_PI_AUTH_JSON=)[^\s]+"),
    re.compile(r"(?i)(auth\.json[:=]?\s*)[^\s]+"),
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a sanitized replay corpus from run evidence."
    )
    parser.add_argument("--runs-root", default=None)
    parser.add_argument("--out", required=True)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--transcript-lines", type=int, default=20)
    parser.add_argument("--event-lines", type=int, default=20)
    return parser.parse_args(argv)


def default_runs_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1] / "runs"


def read_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def redact_text(text: str) -> str:
    scrubbed = text
    for pattern in SECRET_PATTERNS:
        scrubbed = pattern.sub(r"\1[redacted]", scrubbed)
    return scrubbed


def excerpt_lines(path: pathlib.Path, *, line_limit: int) -> list[str]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []
    cleaned = [redact_text(line) for line in lines if "context/source-runs/" not in line]
    return cleaned[:line_limit]


def detect_source_label(run_dir: pathlib.Path, task_excerpt: list[str]) -> str:
    if run_dir.name.startswith("real-canary-"):
        return "real_canary"
    joined = "\n".join(task_excerpt)
    if "Real pi canary:" in joined:
        return "real_canary"
    if "Real pi integration:" in joined:
        return "real_pi_integration"
    return "operator_run"


def collect_labels(manifest: dict[str, Any], score: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    seen: set[str] = set()

    def add(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, list):
            values = value
        else:
            values = [item.strip() for item in str(value).split(",") if item.strip()]
        for item in values:
            text = str(item).strip()
            if not text or text == "none" or text in seen:
                continue
            seen.add(text)
            labels.append(text)

    add(manifest.get("primary_error_code"))
    add(manifest.get("failure_classifications"))
    add(score.get("failure_classifications"))
    add(score.get("overall_error_code"))
    if score.get("overall_pass") is True:
        add("success")
    return labels


def build_record(
    run_dir: pathlib.Path,
    *,
    transcript_lines: int,
    event_lines: int,
) -> dict[str, Any] | None:
    manifest = read_json(run_dir / "outputs" / "run_manifest.json")
    score = read_json(run_dir / "score.json")
    if not manifest and not score:
        return None

    task_excerpt = excerpt_lines(run_dir / "task.md", line_limit=24)
    event_excerpt = excerpt_lines(run_dir / "run-events.jsonl", line_limit=event_lines)
    transcript_excerpt = excerpt_lines(run_dir / "transcript.jsonl", line_limit=transcript_lines)
    stderr_excerpt = excerpt_lines(run_dir / "pi.stderr.log", line_limit=transcript_lines)
    labels = collect_labels(manifest, score)

    return {
        "record_version": "v1",
        "run_id": run_dir.name,
        "source_label": detect_source_label(run_dir, task_excerpt),
        "benchmark_labels": labels,
        "metadata": {
            "state": manifest.get("state"),
            "overall_pass": score.get("overall_pass"),
            "primary_error_code": manifest.get("primary_error_code"),
            "git_sha": (manifest.get("git") or {}).get("sha"),
            "generated_at": manifest.get("generated_at"),
            "finished_at": (manifest.get("timings") or {}).get("run_finished_epoch_ms"),
            "pi_dependency": (manifest.get("dependencies") or {}).get("pi"),
        },
        "evidence": {
            "task_excerpt": task_excerpt,
            "manifest": manifest,
            "score": score,
            "event_excerpt": event_excerpt,
            "transcript_excerpt": transcript_excerpt,
            "stderr_excerpt": stderr_excerpt,
        },
    }


def build_corpus(
    runs_root: pathlib.Path,
    *,
    limit: int,
    transcript_lines: int,
    event_lines: int,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    run_dirs = sorted(
        [path for path in runs_root.iterdir() if path.is_dir() and not path.name.startswith(".")],
        key=lambda path: path.name,
        reverse=True,
    )
    for run_dir in run_dirs[: max(1, limit)]:
        record = build_record(
            run_dir,
            transcript_lines=transcript_lines,
            event_lines=event_lines,
        )
        if record is not None:
            records.append(record)
    records.sort(key=lambda item: str(item["run_id"]))
    return records


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    runs_root = pathlib.Path(args.runs_root).resolve() if args.runs_root else default_runs_root()
    records = build_corpus(
        runs_root,
        limit=max(1, args.limit),
        transcript_lines=max(1, args.transcript_lines),
        event_lines=max(1, args.event_lines),
    )
    out_path = pathlib.Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(records, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"records": len(records), "out": str(out_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
