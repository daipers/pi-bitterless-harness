#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

if [[ -d "$repo_root/.venv/bin" ]]; then
  export PATH="$repo_root/.venv/bin:$PATH"
fi

"$script_dir/check-tools.sh"

for script in "$script_dir"/*.sh; do
  bash -n "$script"
done

shellcheck "$script_dir"/*.sh
ruff check "$repo_root/starter/bin" "$repo_root/tests"
python3 -m pytest
bandit -q -r "$repo_root/starter/bin"
pip-audit -r "$repo_root/requirements-dev.txt"
python3 "$script_dir/scan_secrets.py" "$repo_root"
if [[ -f "$repo_root/Dockerfile" ]]; then
  trivy fs --scanners vuln,secret,misconfig "$repo_root"
else
  echo "trivy placeholder: no container image or Dockerfile present"
fi
