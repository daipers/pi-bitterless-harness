#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
venv_dir="$repo_root/.venv"
python_version_file="$repo_root/.python-version"
expected_python="$(tr -d '[:space:]' < "$python_version_file")"
expected_python_minor="$(printf '%s' "$expected_python" | cut -d. -f1-2)"
python_bin="python${expected_python_minor}"

if ! command -v "$python_bin" >/dev/null 2>&1; then
  current_python="$(
    python3 - <<'PY'
import platform
print(platform.python_version())
PY
  )"
  current_python_minor="$(printf '%s' "$current_python" | cut -d. -f1-2)"
  if [[ "$current_python_minor" != "$expected_python_minor" ]]; then
    echo "supported python runtime ${expected_python_minor}.x is required to create the dev environment" >&2
    echo "install ${python_bin} or run setup with a matching python3 on PATH" >&2
    exit 1
  fi
  python_bin="python3"
fi

"$python_bin" -m venv "$venv_dir"
"$venv_dir/bin/python" -m pip install --upgrade pip
"$venv_dir/bin/python" -m pip install -r "$repo_root/requirements-dev.txt"

echo "dev environment ready: $venv_dir (python: $("$venv_dir/bin/python" --version 2>&1))"
