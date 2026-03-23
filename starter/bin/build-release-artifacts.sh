#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
version="$(cat "$repo_root/VERSION")"
dist_dir="$repo_root/dist"
artifact_base="pi-bitterless-harness-${version}"
tarball="$dist_dir/${artifact_base}.tar.gz"
checksum_file="$tarball.sha256"
provenance_file="$dist_dir/${artifact_base}.provenance.json"
release_gate_file="$dist_dir/${artifact_base}.release-gate.json"
promotion_bundle_file="$dist_dir/${artifact_base}.promotion-bundle.json"
benchmark_report_input="${HARNESS_BENCHMARK_REPORT:-}"
canary_summary_glob="${HARNESS_CANARY_SUMMARY_GLOB:-}"
replay_report_input="${HARNESS_REPLAY_REPORT:-}"
fault_report_input="${HARNESS_FAULT_REPORT:-}"

"$script_dir/check-supported-runtime.sh"

mkdir -p "$dist_dir"
rm -f "$tarball" "$checksum_file" "$provenance_file" "$release_gate_file" "$promotion_bundle_file"

tar \
  --exclude="./dist" \
  --exclude="./starter/runs" \
  -czf "$tarball" \
  -C "$repo_root" .

shasum -a 256 "$tarball" | awk '{print $1}' > "$checksum_file"

python3 - <<'PY' "$repo_root" "$version" "$tarball" "$checksum_file" "$provenance_file"
import json
import pathlib
import subprocess
import sys
from datetime import UTC, datetime

repo_root = pathlib.Path(sys.argv[1])
version = sys.argv[2]
tarball = pathlib.Path(sys.argv[3])
checksum_file = pathlib.Path(sys.argv[4])
provenance_file = pathlib.Path(sys.argv[5])
pi_version_file = repo_root / "PI_VERSION"

git_sha = subprocess.run(
    ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
    text=True,
    capture_output=True,
    check=False,
).stdout.strip() or None

payload = {
    "version": version,
    "artifact": tarball.name,
    "checksum_file": checksum_file.name,
    "created_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "git_sha": git_sha,
    "python_version": subprocess.run(["python3", "--version"], text=True, capture_output=True, check=False).stdout.strip(),
    "supported_pi_version": pi_version_file.read_text(encoding="utf-8").strip()
    if pi_version_file.exists()
    else None,
}
provenance_file.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY

if [[ -n "$benchmark_report_input" && -n "$canary_summary_glob" ]]; then
  release_gate_args=(
    --summary-glob "$canary_summary_glob"
    --benchmark-report "$benchmark_report_input"
    --provenance-file "$provenance_file"
    --out "$release_gate_file"
  )
  if [[ -n "$replay_report_input" ]]; then
    release_gate_args+=(--replay-report "$replay_report_input")
  fi
  if [[ -n "$fault_report_input" ]]; then
    release_gate_args+=(--fault-report "$fault_report_input")
  fi
  python3 "$script_dir/verify_release_evidence.py" "${release_gate_args[@]}"
fi

python3 - <<'PY' \
  "$version" \
  "$tarball" \
  "$checksum_file" \
  "$provenance_file" \
  "$promotion_bundle_file" \
  "$benchmark_report_input" \
  "$replay_report_input" \
  "$fault_report_input" \
  "$release_gate_file"
import json
import pathlib
import sys
from datetime import UTC, datetime

version = sys.argv[1]
tarball = pathlib.Path(sys.argv[2])
checksum_file = pathlib.Path(sys.argv[3])
provenance_file = pathlib.Path(sys.argv[4])
bundle_file = pathlib.Path(sys.argv[5])
benchmark_report = pathlib.Path(sys.argv[6]) if sys.argv[6] else None
replay_report = pathlib.Path(sys.argv[7]) if sys.argv[7] else None
fault_report = pathlib.Path(sys.argv[8]) if sys.argv[8] else None
release_gate = pathlib.Path(sys.argv[9]) if sys.argv[9] else None

payload = {
    "bundle_version": "v1",
    "created_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "version": version,
    "artifacts": {
        "tarball": tarball.name,
        "checksum_file": checksum_file.name,
        "provenance_file": provenance_file.name,
        "benchmark_report": benchmark_report.name if benchmark_report and benchmark_report.exists() else None,
        "replay_report": replay_report.name if replay_report and replay_report.exists() else None,
        "fault_report": fault_report.name if fault_report and fault_report.exists() else None,
        "release_gate": release_gate.name if release_gate and release_gate.exists() else None,
    },
}
bundle_file.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY

echo "$tarball"
