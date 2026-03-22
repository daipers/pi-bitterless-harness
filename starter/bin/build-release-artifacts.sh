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

mkdir -p "$dist_dir"
rm -f "$tarball" "$checksum_file" "$provenance_file"

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

echo "$tarball"
