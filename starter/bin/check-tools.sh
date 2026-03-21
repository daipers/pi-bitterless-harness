#!/usr/bin/env bash
set -euo pipefail

missing=0
for tool in python3 bash git jq ruff pytest shellcheck bandit pip-audit trivy; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "missing required tool: $tool" >&2
    missing=1
  fi
done

if [[ "$missing" -ne 0 ]]; then
  echo "install the missing tools before running the ship gate" >&2
  exit 1
fi

echo "all required tools are available"

