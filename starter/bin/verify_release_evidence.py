#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import pathlib
import re
import urllib.parse
import urllib.request
import zipfile
from datetime import UTC, datetime, timedelta
from typing import Any

GITHUB_API_HOST = "api.github.com"
PYTHON_VERSION_RE = re.compile(r"(\d+\.\d+(?:\.\d+)?)")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Bitterless Harness release evidence.")
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
    parser.add_argument(
        "--benchmark-report",
        default=None,
        help="optional benchmark report JSON produced by benchmark_harness.py",
    )
    parser.add_argument(
        "--replay-report",
        default=None,
        help="optional replay benchmark JSON produced by benchmark_harness.py",
    )
    parser.add_argument(
        "--fault-report",
        default=None,
        help="optional fault-injection benchmark JSON produced by benchmark_harness.py",
    )
    parser.add_argument(
        "--provenance-file",
        default=None,
        help="optional release provenance JSON produced by build-release-artifacts.sh",
    )
    parser.add_argument(
        "--expected-python-version",
        default=None,
        help="required Python version prefix; defaults to repo .python-version",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="optional path to write the release gate JSON",
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


def expected_python_version(args: argparse.Namespace) -> str:
    if args.expected_python_version:
        return args.expected_python_version
    return (repo_root() / ".python-version").read_text(encoding="utf-8").strip()


def extract_python_version(value: str) -> str | None:
    match = PYTHON_VERSION_RE.search(value)
    return match.group(1) if match else None


def python_minor_version(value: str | None) -> str | None:
    if not value:
        return None
    extracted = extract_python_version(value)
    if not extracted:
        return None
    parts = extracted.split(".")
    if len(parts) < 2:
        return None
    return ".".join(parts[:2])


def python_version_policy(value: str) -> str:
    minor = python_minor_version(value)
    return f"{minor}.x" if minor else value


def validate_github_api_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != GITHUB_API_HOST:
        raise ValueError(f"untrusted GitHub API URL: {url}")
    if not parsed.path.startswith("/repos/"):
        raise ValueError(f"unexpected GitHub API path: {url}")
    return url


def github_api_request(url: str, *, token: str | None = None) -> urllib.request.Request:
    return urllib.request.Request(
        validate_github_api_url(url),
        headers={
            "Accept": "application/vnd.github+json",
            **({"Authorization": f"Bearer {token}"} if token else {}),
        },
    )


def request_json(url: str, *, token: str | None = None) -> dict[str, Any]:
    request = github_api_request(url, token=token)
    # Bandit: the request URL is validated as https://api.github.com/repos/...
    with urllib.request.urlopen(request) as response:  # nosec B310
        return json.loads(response.read().decode("utf-8"))


def download_artifact_json(url: str, *, token: str | None = None) -> dict[str, Any]:
    request = github_api_request(url, token=token)
    # Bandit: the request URL is validated as https://api.github.com/repos/...
    with urllib.request.urlopen(request) as response:  # nosec B310
        payload = response.read()
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        for name in archive.namelist():
            if name.endswith(".json"):
                return json.loads(archive.read(name).decode("utf-8"))
    raise ValueError("artifact zip did not contain a JSON summary")


def load_local_summaries(pattern: str) -> list[dict[str, Any]]:
    candidate = pathlib.Path(pattern)
    if candidate.is_absolute():
        root = candidate.parent
        pattern = candidate.name
    else:
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
            "insufficient fresh canary evidence: "
            f"found {len(selected)} summary file(s), need {min_runs}"
        )

    if any(str(summary.get("supported_pi_version", "")) != expected_pi for summary in selected):
        raise SystemExit("canary evidence PI_VERSION drift detected")

    successful = [summary for summary in selected if summary.get("overall_ok") is True]
    pass_rate = len(successful) / len(selected)
    if pass_rate < min_pass_rate:
        raise SystemExit(
            f"canary pass rate {pass_rate:.2f} is below required minimum {min_pass_rate:.2f}"
        )

    if any(
        int((summary.get("scenario_totals") or {}).get("failed", 0)) > 0
        for summary in selected
    ):
        raise SystemExit("canary evidence includes failed scenarios")

    return {
        "passed": True,
        "selected_runs": len(selected),
        "pass_rate": round(pass_rate, 2),
        "freshest_generated_at": selected[0].get("generated_at"),
        "oldest_generated_at": selected[-1].get("generated_at"),
        "supported_pi_version": expected_pi,
        "git_shas": [summary.get("git_sha") for summary in selected],
    }


def validate_benchmark_report(path: pathlib.Path) -> dict[str, Any]:
    payload = read_json(path)
    if payload.get("benchmark_report_version") != "v1":
        raise SystemExit("benchmark report is missing benchmark_report_version=v1")
    if payload.get("overall_pass") is not True:
        raise SystemExit("benchmark report did not pass promotion thresholds")
    promotion_summary = payload.get("promotion_summary", {})
    return {
        "passed": True,
        "path": str(path),
        "benchmark_report_version": payload.get("benchmark_report_version"),
        "overall_pass": True,
        "bundle_id": promotion_summary.get("bundle_id"),
        "gated_sections": list(promotion_summary.get("gated_sections", [])),
        "threshold_results": dict(promotion_summary.get("threshold_results", {})),
        "candidate_types": dict(promotion_summary.get("candidate_types", {})),
    }


def validate_provenance(
    path: pathlib.Path,
    *,
    expected_pi: str,
) -> dict[str, Any]:
    payload = read_json(path)
    if payload.get("supported_pi_version") != expected_pi:
        raise SystemExit("release provenance PI_VERSION drift detected")
    required_fields = ["version", "artifact", "checksum_file", "created_at"]
    missing = [field for field in required_fields if not payload.get(field)]
    if missing:
        raise SystemExit(f"release provenance missing required field(s): {', '.join(missing)}")
    return {
        "passed": True,
        "path": str(path),
        "artifact": payload.get("artifact"),
        "checksum_file": payload.get("checksum_file"),
        "created_at": payload.get("created_at"),
        "git_sha": payload.get("git_sha"),
        "supported_pi_version": payload.get("supported_pi_version"),
        "python_version": payload.get("python_version"),
    }


def skipped_check(reason: str) -> dict[str, Any]:
    return {
        "passed": True,
        "skipped": True,
        "reason": reason,
    }


def validate_runtime_evidence(
    *,
    expected_python: str,
    expected_pi: str,
    provenance: dict[str, Any] | None,
) -> dict[str, Any]:
    if provenance is None:
        return skipped_check("release provenance not provided")
    provenance_python = str((provenance or {}).get("python_version") or "")
    provenance_pi = str((provenance or {}).get("supported_pi_version") or "")
    expected_minor = python_minor_version(expected_python)
    provenance_minor = python_minor_version(provenance_python)
    runtime_matches = (
        provenance_pi == expected_pi
        and provenance_minor is not None
        and provenance_minor == expected_minor
    )
    return {
        "passed": runtime_matches,
        "expected_python_policy": python_version_policy(expected_python),
        "expected_python_version": expected_python,
        "expected_pi_version": expected_pi,
        "provenance_python_version": provenance_python or None,
        "provenance_python_minor": provenance_minor,
        "provenance_supported_pi_version": provenance_pi or None,
    }


def build_release_gate_report(args: argparse.Namespace) -> dict[str, Any]:
    if not args.summary_glob and not args.github_repo:
        raise SystemExit("provide either --summary-glob or --github-repo")

    summaries = (
        load_local_summaries(args.summary_glob)
        if args.summary_glob
        else load_github_summaries(args)
    )
    expected_pi = expected_pi_version(args)
    expected_python = expected_python_version(args)
    canary_report = validate_summaries(
        summaries,
        min_runs=max(1, args.min_runs),
        freshness_hours=max(1, args.freshness_hours),
        min_pass_rate=max(0.0, min(1.0, args.min_pass_rate)),
        expected_pi=expected_pi,
    )

    benchmark_report = None
    if args.benchmark_report:
        benchmark_report = validate_benchmark_report(pathlib.Path(args.benchmark_report).resolve())
    else:
        benchmark_report = skipped_check("benchmark report not provided")

    if args.replay_report:
        replay_report = validate_benchmark_report(pathlib.Path(args.replay_report).resolve())
    else:
        replay_report = skipped_check("replay benchmark report not provided")

    if args.fault_report:
        fault_report = validate_benchmark_report(pathlib.Path(args.fault_report).resolve())
    else:
        fault_report = skipped_check("fault-injection benchmark report not provided")

    provenance_report = None
    provenance_payload = None
    if args.provenance_file:
        provenance_path = pathlib.Path(args.provenance_file).resolve()
        provenance_payload = read_json(provenance_path)
        provenance_report = validate_provenance(provenance_path, expected_pi=expected_pi)
    else:
        provenance_report = skipped_check("release provenance not provided")

    runtime_report = validate_runtime_evidence(
        expected_python=expected_python,
        expected_pi=expected_pi,
        provenance=provenance_payload,
    )
    checks = {
        "canary": canary_report,
        "benchmark": benchmark_report,
        "replay_benchmark": replay_report,
        "fault_injection_benchmark": fault_report,
        "provenance": provenance_report,
        "runtime": runtime_report,
    }
    overall_pass = all(bool(check.get("passed")) for check in checks.values())
    return {
        "release_gate_version": "v1",
        "generated_at": now_utc().replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "overall_pass": overall_pass,
        "checks": checks,
        "artifacts": {
            "benchmark_report": benchmark_report.get("path"),
            "replay_report": replay_report.get("path"),
            "fault_report": fault_report.get("path"),
            "provenance_file": provenance_report.get("path"),
        },
        "summary": {
            "selected_canary_runs": canary_report.get("selected_runs"),
            "benchmark_gated_sections": benchmark_report.get("gated_sections", []),
            "expected_pi_version": expected_pi,
            "expected_python_policy": python_version_policy(expected_python),
            "expected_python_version": expected_python,
        },
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = build_release_gate_report(args)
    if args.out:
        out_path = pathlib.Path(args.out).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    if not report["overall_pass"]:
        raise SystemExit("release gate failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
