#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
from collections import Counter
from datetime import UTC, datetime
from typing import Any


def _to_state(value: str | None) -> str:
    value = (value or "").strip().lower()
    if value == "done":
        return "complete"
    return value


def now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def _to_positive_int(value: str | int | None, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return parsed if parsed >= 0 else default


def _read_json(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_jsonl(path: pathlib.Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payloads: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            payload = json.loads(raw_line)
        except Exception:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads


def _read_manifest(run_dir: pathlib.Path) -> dict[str, Any]:
    return _read_json(run_dir / "outputs" / "run_manifest.json")


def _read_score(run_dir: pathlib.Path) -> dict[str, Any]:
    return _read_json(run_dir / "score.json")


def _read_state(run_dir: pathlib.Path) -> str:
    path = run_dir / "run.state"
    if path.exists():
        try:
            return _to_state(path.read_text(encoding="utf-8").strip())
        except Exception:
            return ""
    manifest = _read_manifest(run_dir)
    return _to_state(manifest.get("state", ""))


def _parse_ts_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = int(value)
        return numeric * 1000 if numeric < 10_000_000_000 else numeric
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        numeric = int(text)
        return numeric * 1000 if numeric < 10_000_000_000 else numeric
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(text, fmt)
            except ValueError:
                continue
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return int(parsed.timestamp() * 1000)
    return None


def _duration_ms(run: dict[str, Any]) -> int:
    manifest = run["manifest"]
    timings = manifest.get("timings", {})
    duration = timings.get("run_duration_ms") or timings.get("model_duration_ms")
    if isinstance(duration, int) and duration > 0:
        return duration
    try:
        return max(
            0,
            int(timings.get("run_finished_epoch_ms", 0))
            - int(timings.get("run_started_epoch_ms", 0)),
        )
    except Exception:
        return 0


def _percentile(values: list[int], ratio: float) -> int:
    if not values:
        return 0
    values = sorted(values)
    index = min(len(values) - 1, max(0, int((len(values) - 1) * ratio)))
    return values[index]


def _split_codes(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    raw = str(value)
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _collect_runs(root: pathlib.Path, *, window_days: int) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    if not root.exists():
        return entries

    window_ms = max(0, _to_positive_int(window_days, default=30) * 24 * 60 * 60 * 1000)
    cutoff_ms = now_ms() - window_ms
    for run_dir in root.iterdir():
        if not run_dir.is_dir() or run_dir.name.startswith("."):
            continue

        manifest = _read_manifest(run_dir)
        score = _read_score(run_dir)
        state = _read_state(run_dir)
        events = _read_jsonl(run_dir / "run-events.jsonl")
        duration = _duration_ms({"manifest": manifest})
        run_finished = _parse_ts_ms(manifest.get("timings", {}).get("run_finished_epoch_ms"))
        is_terminal = state in {"complete", "failed", "cancelled"}

        if is_terminal:
            if run_finished:
                if run_finished < cutoff_ms:
                    continue
            elif window_ms and now_ms() - int(run_dir.stat().st_mtime * 1000) > window_ms:
                continue
        elif window_ms and now_ms() - int(run_dir.stat().st_mtime * 1000) > window_ms:
            continue

        entries.append(
            {
                "run_dir": run_dir,
                "state": state,
                "manifest": manifest,
                "score": score,
                "events": events,
                "duration_ms": duration,
            }
        )
    return entries


def harvest(root: pathlib.Path, *, window_days: int = 30) -> dict[str, Any]:
    entries = _collect_runs(root, window_days=window_days)
    totals = {
        "total_runs": len(entries),
        "complete": 0,
        "cancelled": 0,
        "failed": 0,
        "running": 0,
        "scoring": 0,
        "score_pending": 0,
        "model_running": 0,
        "queued": 0,
        "model_complete": 0,
        "complete_pass": 0,
        "complete_fail": 0,
    }

    durations: list[int] = []
    failure_by_code: Counter[str] = Counter()
    eval_failure_by_cause: Counter[str] = Counter()
    saturation_events = 0
    saturation_last_24h: Counter[str] = Counter()
    offending: list[dict[str, Any]] = []
    longest_runs: list[dict[str, Any]] = []
    now = now_ms()
    trend_window = 24 * 60 * 60 * 1000

    for entry in entries:
        run_dir = entry["run_dir"]
        state = entry["state"]
        manifest = entry["manifest"]
        score_payload = entry["score"]
        events = entry["events"]
        duration_ms = entry["duration_ms"]

        if state == "complete":
            totals["complete"] += 1
            if score_payload.get("overall_pass") is True:
                totals["complete_pass"] += 1
            else:
                totals["complete_fail"] += 1
        elif state == "cancelled":
            totals["cancelled"] += 1
        elif state == "failed":
            totals["failed"] += 1
        elif state in {"scoring", "score_pending"}:
            totals["scoring"] += 1
        elif state == "model_running":
            totals["model_running"] += 1
        elif state == "model_complete":
            totals["model_complete"] += 1
        elif state == "queued" or not state:
            totals["queued"] += 1
        elif state != "complete":
            totals["running"] += 1

        if duration_ms > 0:
            durations.append(duration_ms)

        manifest_error_codes = _split_codes(manifest.get("error_code"))
        for label in manifest_error_codes:
            failure_by_code[label] += 1

        score_error_codes = _split_codes(score_payload.get("overall_error_code"))
        for label in score_error_codes:
            failure_by_code[label] += 1
            eval_failure_by_cause[label] += 1

        for label in _split_codes(score_payload.get("failure_classifications")):
            failure_by_code[label] += 1
            eval_failure_by_cause[label] += 1

        for event in events:
            if (
                event.get("failure_classification") == "score_backpressure"
                or event.get("failure_class") == "score_backpressure"
                or event.get("heartbeat_reason") == "resource_cap_exceeded"
            ):
                saturation_events += 1
                event_ts = _parse_ts_ms(event.get("ts"))
                if event_ts and now - event_ts <= trend_window:
                    bucket = datetime.fromtimestamp(event_ts / 1000, tz=UTC).strftime(
                        "%Y-%m-%dT%H:00Z"
                    )
                    saturation_last_24h[bucket] += 1

        event_codes = [
            _to_state(event.get("failure_classification"))
            for event in events
            if event.get("failure_classification")
        ]
        for label in event_codes:
            failure_by_code[label] += 1

        error_label = (
            "; ".join(_split_codes(manifest.get("error_code")))
            if manifest.get("error_code")
            else (
                "none"
                if state == "complete"
                else "; ".join(_split_codes(score_payload.get("overall_error_code")))
            )
        )
        if state in {"failed", "cancelled"} or error_label not in {"", "none"}:
            offending.append(
                {
                    "run_id": run_dir.name,
                    "state": state,
                    "duration_ms": duration_ms,
                    "error_code": error_label or "none",
                    "evaluation_failed": bool(
                        score_error_codes or score_payload.get("failure_classifications")
                    ),
                    "failures": sorted(
                        set(
                            _split_codes(manifest.get("error_code"))
                            + _split_codes(score_payload.get("overall_error_code"))
                        )
                    ),
                }
            )

        if duration_ms > 0:
            longest_runs.append(
                {
                    "run_id": run_dir.name,
                    "state": state,
                    "duration_ms": duration_ms,
                    "worker_id": manifest.get("orchestration", {}).get("worker_id"),
                    "queue_wait_ms": manifest.get("orchestration", {}).get("queue_wait_ms", 0),
                    "score_wait_ms": manifest.get("orchestration", {}).get("score_wait_ms", 0),
                }
            )

    durations.sort()
    offending.sort(key=lambda item: (item["duration_ms"], item["run_id"]), reverse=True)
    longest_runs.sort(key=lambda item: item["duration_ms"], reverse=True)
    saturation_hours = [
        {"hour_utc": hour, "events": count} for hour, count in sorted(saturation_last_24h.items())
    ]
    saturation_hours = saturation_hours[-24:] if saturation_hours else []

    pass_rate = 0.0
    if totals["complete"] > 0:
        pass_rate = (totals["complete_pass"] / totals["complete"]) * 100.0

    return {
        "window_days": window_days,
        "totals": totals,
        "pass_rate_percent": round(pass_rate, 2),
        "duration_ms": {
            "p50": _percentile(durations, 0.50),
            "p95": _percentile(durations, 0.95),
            "p99": _percentile(durations, 0.99),
            "max": durations[-1] if durations else 0,
        },
        "failure_classification_counts": dict(failure_by_code),
        "eval_failure_by_cause": dict(eval_failure_by_cause),
        "queue_saturation": {
            "events_total": saturation_events,
            "events_last_24h": sum(saturation_last_24h.values()),
            "trend_last_24h_by_hour": saturation_hours,
        },
        "top_offending_tasks": offending[:10],
        "longest_runs": longest_runs[:10],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="harvester health summary for orchestrator")
    parser.add_argument("--runs-root", default=None)
    parser.add_argument("--window-days", type=int, default=30)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    runs_root = pathlib.Path(
        args.runs_root
        if args.runs_root is not None
        else pathlib.Path(__file__).resolve().parents[1] / "runs"
    ).resolve()
    payload = harvest(runs_root, window_days=args.window_days)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
