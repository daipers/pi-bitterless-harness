#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
python_version_file="$repo_root/.python-version"
pi_version_file="$repo_root/PI_VERSION"

expected_python="$(tr -d '[:space:]' < "$python_version_file")"
expected_python_minor="$(printf '%s' "$expected_python" | cut -d. -f1-2)"
current_python="$(
  python3 - <<'PY'
import platform
print(platform.python_version())
PY
)"
current_python_minor="$(printf '%s' "$current_python" | cut -d. -f1-2)"

if [[ "$current_python_minor" != "$expected_python_minor" ]]; then
  echo "unsupported python runtime: expected ${expected_python_minor}.x from ${python_version_file}, got ${current_python}" >&2
  exit 1
fi

expected_pi="$(tr -d '[:space:]' < "$pi_version_file")"
current_pi="$(
  pi --version 2>&1 | awk 'NF { line = $0 } END { print line }'
)"

if [[ "$current_pi" != "$expected_pi" ]]; then
  echo "unsupported pi runtime: expected ${expected_pi} from ${pi_version_file}, got ${current_pi}" >&2
  exit 1
fi

echo "supported runtime check passed: python ${current_python}, pi ${current_pi}"
