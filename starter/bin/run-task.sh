#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PYTHONPATH="${script_dir}${PYTHONPATH:+:$PYTHONPATH}"
export PYTHONPATH

exec python3 "$script_dir/run_task.py" "$@"
