#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 /absolute/or/relative/path/to/run-dir" >&2
  exit 2
fi

run_dir="$1"
archive_path="${2:-${run_dir%/}.tgz}"
tar -czf "$archive_path" -C "$(dirname "$run_dir")" "$(basename "$run_dir")"
echo "$archive_path"

