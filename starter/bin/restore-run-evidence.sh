#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 /path/to/archive.tgz /path/to/destination-dir" >&2
  exit 2
fi

archive_path="$1"
destination_dir="$2"
mkdir -p "$destination_dir"
tar -xzf "$archive_path" -C "$destination_dir"
echo "$destination_dir"

