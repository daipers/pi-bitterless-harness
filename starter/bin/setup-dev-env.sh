#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
venv_dir="$repo_root/.venv"

python3 -m venv "$venv_dir"
"$venv_dir/bin/python" -m pip install --upgrade pip
"$venv_dir/bin/python" -m pip install -r "$repo_root/requirements-dev.txt"

echo "dev environment ready: $venv_dir"

