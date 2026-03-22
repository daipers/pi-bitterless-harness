#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import pathlib
import urllib.parse
import urllib.request
import zipfile
from datetime import UTC, datetime, timedelta
from typing import Any


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify real-pi canary release evidence.")
    parser.add_argument("--summary-glob", default=None, help="glob for local canary summaries")
    parser.add_argument("--github-repo", default=None, help="owner/repo for GitHub API lookup")
    parser.add_argument(
        "--workflow-file",
        default="real-pi-canary.yml",
        help="workflow file to query for canary history",
    )
    parser.add_argument("--branch", default="main", help="branch to verify")
    parser.add_argument(
        "--artifact-name",
        default="real-pi-canary-summary",
        help="artifact name that contains the canary summary JSON",
    )
    parser.add_argument("--github-token", default=None, help="GitHub token for API access")
    parser.add_argument("--min-runs", type=int, default=2, help="minimum successful summaries")
    parser.add_argument(
        "--freshness-hours",
        type=int,
        default=36,
        help="maximum age in hours for the oldest accepted summary",
    )
    parser.add_argument(
        "--min-pass-rate",
        type=float,
        default=1.0,
        help="minimum fraction of summaries that must be overall_ok",
    )
    parser.add_argument(
        "--expected-pi-version",
        default=None,
        help="required PI_VERSION value; defaults to repo PI_VERSION",
    )
    return parser.parse_args(argv)


def now_utc() -> datetime:
    return datetime.now(UTC)


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[2]


def read_json(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def expected_pi_version(args: argparse.Namespace) -> str:
    if args.expected_pi_version:
        return args.expected_pi_version
    return (repo_root() / "PI_VERSION").read_text(encoding="utf-8").strip()


def request_json(url: str, *, token: str | None = None) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            **({"Authorization": f"Bearer {token}"} if token else {}),
        },
    )
    with urllib.request.urlopen(request) as response:  # noqa: S310 - trusted API URL
        return json.loads(response.read().decode("utf-8"))


def download_artifact_json(url: str, *, token: str | None = None) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            **({"Authorization": f"Bearer {token}"} if token else {}),
        },
    )
    with urllib.request.urlopen(request) as response:  # noqa: S310 - trusted API URL
        payload = response.read()
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        for name in archive.namelist():
            if name.endswith(".json"):
                return json.loads(archive.read(name).decode("utf-8"))
    raise ValueError("artifact zip did not contain a JSON summary")


def load_local_summaries(pattern: str) -> list[dict[str, Any]]:
    root = repo_root()
    return [
        read_json(path)
        for path in sorted(root.glob(pattern), reverse=True)
        if path.is_file()
    ]


def load_github_summaries(args: argparse.Namespace) -> list[dict[str, Any]]:
    if not args.github_repo:
        raise ValueError("--github-repo is required for GitHub verification")
    encoded_workflow = urllib.parse.quote(args.workflow_file, safe="")
    encoded_branch = urllib.parse.quote(args.branch, safe="")
    token = args.github_token
    url = (
        f"https://api.github.com/repos/{args.github_repo}/actions/workflows/{encoded_workflow}/runs"
        f"?branch={encoded_branch}&status=success&per_page=20"
    )
    payload = request_json(url, token=token)
    summaries: list[dict[str, Any]] = []
    for run in payload.get("workflow_runs", []):
        artifacts = request_json(str(run["artifacts_url"]), token=token)
        for artifact in artifacts.get("artifacts", []):
            if artifact.get("name") != args.artifact_name or artifact.get("expired"):
                continue
            summary = download_artifact_json(str(artifact["archive_download_url"]), token=token)
            summaries.append(summary)
            break
    return summaries


def select_recent_summaries(
    summaries: list[dict[str, Any]],
    *,
    min_runs: int,
    freshness_hours: int,
) -> list[dict[str, Any]]:
    cutoff = now_utc() - timedelta(hours=freshness_hours)
    recent = [
        summary
        for summary in summaries
        if summary.get("generated_at") and parse_iso(str(summary["generated_at"])) >= cutoff
    ]
    recent.sort(key=lambda item: parse_iso(str(item["generated_at"])), reverse=True)
    return recent[:min_runs]


def validate_summaries(
    summaries: list[dict[str, Any]],
    *,
    min_runs: int,
    freshness_hours: int,
    min_pass_rate: float,
    expected_pi: str,
) -> dict[str, Any]:
    selected = select_recent_summaries(
        summaries,
        min_runs=min_runs,
        freshness_hours=freshness_hours,
    )
    if len(selected) < min_runs:
        raise SystemExit(
            f"insufficient fresh canary evidence: found {len(selected)} summary file(s), need {min_runs}"
        )

    if any(str(summary.get("supported_pi_version", "")) != expected_pi for summary in selected):
        raise SystemExit("canary evidence PI_VERSION drift detected")

    successful = [summary for summary in selected if summary.get("overall_ok") is True]
    pass_rate = len(successful) / len(selected)
    if pass_rate < min_pass_rate:
        raise SystemExit(
            f"canary pass rate {pass_rate:.2f} is below required minimum {min_pass_rate:.2f}"
        )

    if any(int((summary.get("scenario_totals") or {}).get("failed", 0)) > 0 for summary in selected):
        raise SystemExit("canary evidence includes failed scenarios")

    return {
        "selected_runs": len(selected),
        "pass_rate": round(pass_rate, 2),
        "freshest_generated_at": selected[0].get("generated_at"),
        "oldest_generated_at": selected[-1].get("generated_at"),
        "supported_pi_version": expected_pi,
        "git_shas": [summary.get("git_sha") for summary in selected],
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.summary_glob and not args.github_repo:
        raise SystemExit("provide either --summary-glob or --github-repo")

    summaries = (
        load_local_summaries(args.summary_glob)
        if args.summary_glob
        else load_github_summaries(args)
    )
    report = validate_summaries(
        summaries,
        min_runs=max(1, args.min_runs),
        freshness_hours=max(1, args.freshness_hours),
        min_pass_rate=max(0.0, min(1.0, args.min_pass_rate)),
        expected_pi=expected_pi_version(args),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
