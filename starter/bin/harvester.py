#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import pathlib
from collections import Counter
from datetime import UTC, datetime
from typing import Any

STALE_NON_TERMINAL_MS = 15 * 60 * 1000


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


def _is_locked(run_dir: pathlib.Path) -> bool:
    lock_dir = run_dir / ".run-lock"
    if not lock_dir.is_dir():
        return False
    pid_file = lock_dir / "pid"
    if not pid_file.is_file():
        return False
    try:
        pid = int(pid_file.read_text(encoding="utf-8").strip())
    except Exception:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _parse_ts_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int | float):
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


def _top_failure_causes(counter: Counter[str], *, limit: int = 5) -> list[dict[str, Any]]:
    ranked = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    return [{"code": code, "count": count} for code, count in ranked[:limit]]


def _latest_canary_status(root: pathlib.Path) -> dict[str, Any]:
    candidates = sorted(root.glob("real-canary-*.summary.json"))
    if not candidates:
        return {
            "latest_summary_path": None,
            "completed_epoch_ms": None,
            "freshness_hours": None,
            "all_passed": None,
        }
    latest = max(candidates, key=lambda path: path.stat().st_mtime)
    payload = _read_json(latest)
    completed_epoch_ms = _parse_ts_ms(payload.get("finished_at") or payload.get("generated_at"))
    freshness_hours = None
    if completed_epoch_ms is not None:
        freshness_hours = round((now_ms() - completed_epoch_ms) / (60 * 60 * 1000), 2)
    overall_ok = payload.get("overall_ok")
    all_passed = overall_ok if isinstance(overall_ok, bool) else None
    if all_passed is None:
        totals = payload.get("scenario_totals", {})
        failed = totals.get("failed") if isinstance(totals, dict) else None
        if isinstance(failed, int):
            all_passed = failed == 0
    return {
        "latest_summary_path": str(latest.resolve()),
        "completed_epoch_ms": completed_epoch_ms,
        "freshness_hours": freshness_hours,
        "all_passed": all_passed,
    }


def _split_codes(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    raw = str(value)
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def read_queue_entries(path: pathlib.Path) -> dict[str, dict[str, Any]]:
    entries: dict[str, dict[str, Any]] = {}
    for entry in _read_jsonl(path):
        run_id = str(entry.get("run_id", "")).strip()
        if not run_id:
            continue
        entries[run_id] = dict(entry)
    return entries


def _effective_state(run_dir: pathlib.Path, manifest: dict[str, Any] | None = None) -> str:
    state = _read_state(run_dir)
    manifest = manifest or _read_manifest(run_dir)
    manifest_state = _to_state(manifest.get("state", ""))

    if state == "complete" and manifest_state == "complete":
        return "complete"
    if state == "running":
        return "running" if _is_locked(run_dir) else "partial"
    if state in {"claimed", "model_running", "scoring"} and not _is_locked(run_dir):
        return "partial"
    return state or manifest_state


def _artifact_paths(run_dir: pathlib.Path) -> dict[str, str]:
    return {
        "events": str((run_dir / "run-events.jsonl").resolve()),
        "transcript": str((run_dir / "transcript.jsonl").resolve()),
        "manifest": str((run_dir / "outputs" / "run_manifest.json").resolve()),
        "score": str((run_dir / "score.json").resolve()),
        "patch": str((run_dir / "patch.diff").resolve()),
        "result": str((run_dir / "result.json").resolve()),
    }


def _execution_profile(
    run_dir: pathlib.Path,
    manifest: dict[str, Any],
    score: dict[str, Any],
) -> str:
    profile = str(manifest.get("execution", {}).get("profile", "")).strip()
    if profile:
        return profile
    profile = str(score.get("execution_profile", "")).strip()
    if profile:
        return profile
    contract = _read_json(run_dir / "run.contract.json")
    return str(contract.get("execution_profile", "")).strip()


def collect_run_rows(root: pathlib.Path, *, window_days: int = 30) -> list[dict[str, Any]]:
    entries = _collect_runs(root, window_days=window_days)
    queue_root = root / ".orchestrator"
    run_queue = read_queue_entries(queue_root / "run_queue.jsonl")
    score_queue = read_queue_entries(queue_root / "score_queue.jsonl")
    rows: list[dict[str, Any]] = []

    for entry in entries:
        run_dir = entry["run_dir"]
        manifest = entry["manifest"]
        score = entry["score"]
        effective_state = _effective_state(run_dir, manifest)
        run_queue_entry = run_queue.get(run_dir.name, {})
        score_queue_entry = score_queue.get(run_dir.name, {})
        failure_classifications = sorted(
            set(
                _split_codes(manifest.get("failure_classifications"))
                + _split_codes(score.get("failure_classifications"))
            )
        )
        primary_error_code = str(manifest.get("primary_error_code") or "").strip()
        if not primary_error_code:
            primary_error_code = str(score.get("overall_error_code") or "").strip()

        rows.append(
            {
                "run_id": run_dir.name,
                "run_dir": str(run_dir.resolve()),
                "state": effective_state,
                "overall_pass": score.get("overall_pass")
                if isinstance(score.get("overall_pass"), bool)
                else None,
                "primary_error_code": primary_error_code,
                "failure_classifications": failure_classifications,
                "execution_profile": _execution_profile(run_dir, manifest, score),
                "duration_ms": entry["duration_ms"],
                "queue_wait_ms": int(
                    manifest.get("orchestration", {}).get("queue_wait_ms", 0) or 0
                ),
                "score_wait_ms": int(
                    manifest.get("orchestration", {}).get("score_wait_ms", 0) or 0
                ),
                "worker_id": str(
                    manifest.get("orchestration", {}).get("worker_id")
                    or run_queue_entry.get("worker_id")
                    or score_queue_entry.get("worker_id")
                    or ""
                ),
                "run_queue_state": _to_state(run_queue_entry.get("state")),
                "score_queue_state": _to_state(score_queue_entry.get("state")),
                "artifact_paths": _artifact_paths(run_dir),
                "updated_epoch_ms": int(run_dir.stat().st_mtime * 1000),
            }
        )

    rows.sort(key=lambda item: (item["updated_epoch_ms"], item["run_id"]), reverse=True)
    return rows


def harvest_repo(root: pathlib.Path, *, window_days: int = 30) -> dict[str, Any]:
    return {
        "summary": harvest(root, window_days=window_days),
        "runs": collect_run_rows(root, window_days=window_days),
    }


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
    queue_waits: list[int] = []
    score_waits: list[int] = []
    last_updated_epoch_ms: int | None = None
    last_success_epoch_ms: int | None = None
    last_failure_epoch_ms: int | None = None
    stale_non_terminal_count = 0
    oldest_non_terminal_age_ms = 0

    for entry in entries:
        run_dir = entry["run_dir"]
        state = _effective_state(run_dir, entry["manifest"])
        manifest = entry["manifest"]
        score_payload = entry["score"]
        events = entry["events"]
        duration_ms = entry["duration_ms"]
        updated_epoch_ms = int(run_dir.stat().st_mtime * 1000)
        run_finished_epoch_ms = _parse_ts_ms(manifest.get("timings", {}).get("run_finished_epoch_ms"))
        queue_wait_ms = int(manifest.get("orchestration", {}).get("queue_wait_ms", 0) or 0)
        score_wait_ms = int(manifest.get("orchestration", {}).get("score_wait_ms", 0) or 0)

        if queue_wait_ms > 0:
            queue_waits.append(queue_wait_ms)
        if score_wait_ms > 0:
            score_waits.append(score_wait_ms)
        if last_updated_epoch_ms is None or updated_epoch_ms > last_updated_epoch_ms:
            last_updated_epoch_ms = updated_epoch_ms

        if state == "complete":
            totals["complete"] += 1
            if score_payload.get("overall_pass") is True:
                totals["complete_pass"] += 1
                if run_finished_epoch_ms is not None:
                    if last_success_epoch_ms is None or run_finished_epoch_ms > last_success_epoch_ms:
                        last_success_epoch_ms = run_finished_epoch_ms
            else:
                totals["complete_fail"] += 1
                if run_finished_epoch_ms is not None:
                    if last_failure_epoch_ms is None or run_finished_epoch_ms > last_failure_epoch_ms:
                        last_failure_epoch_ms = run_finished_epoch_ms
        elif state == "cancelled":
            totals["cancelled"] += 1
            if updated_epoch_ms > (last_failure_epoch_ms or 0):
                last_failure_epoch_ms = updated_epoch_ms
        elif state == "failed":
            totals["failed"] += 1
            if updated_epoch_ms > (last_failure_epoch_ms or 0):
                last_failure_epoch_ms = updated_epoch_ms
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

        if state not in {"complete", "failed", "cancelled"}:
            age_ms = max(0, now - updated_epoch_ms)
            oldest_non_terminal_age_ms = max(oldest_non_terminal_age_ms, age_ms)
            if age_ms >= STALE_NON_TERMINAL_MS:
                stale_non_terminal_count += 1

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
        "queue_wait_ms": {
            "p50": _percentile(queue_waits, 0.50),
            "p95": _percentile(queue_waits, 0.95),
        },
        "score_wait_ms": {
            "p50": _percentile(score_waits, 0.50),
            "p95": _percentile(score_waits, 0.95),
        },
        "activity": {
            "last_updated_epoch_ms": last_updated_epoch_ms,
            "last_success_epoch_ms": last_success_epoch_ms,
            "last_failure_epoch_ms": last_failure_epoch_ms,
            "stale_non_terminal_count": stale_non_terminal_count,
            "oldest_non_terminal_age_ms": oldest_non_terminal_age_ms,
        },
        "failure_classification_counts": dict(failure_by_code),
        "top_failure_causes": _top_failure_causes(failure_by_code),
        "eval_failure_by_cause": dict(eval_failure_by_cause),
        "queue_saturation": {
            "events_total": saturation_events,
            "events_last_24h": sum(saturation_last_24h.values()),
            "trend_last_24h_by_hour": saturation_hours,
        },
        "canary_status": _latest_canary_status(root),
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
