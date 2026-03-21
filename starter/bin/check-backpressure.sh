#!/usr/bin/env bash
set -euo pipefail

target_dir="${1:-.}"
disk_threshold="${HARNESS_DISK_USED_THRESHOLD_PERCENT:-90}"
free_mb_threshold="${HARNESS_FREE_MB_THRESHOLD:-512}"
load_per_cpu_threshold="${HARNESS_LOAD_PER_CPU_THRESHOLD:-2.0}"

disk_used="$(df -Pk "$target_dir" | awk 'NR==2 {gsub(/%/, "", $5); print $5}')"
free_mb="$(df -Pm "$target_dir" | awk 'NR==2 {print $4}')"
load_avg="$(python3 - <<'PY'
import os
try:
    print(os.getloadavg()[0])
except (AttributeError, OSError):
    print(0.0)
PY
)"
cpu_count="$(python3 - <<'PY'
import os
print(os.cpu_count() or 1)
PY
)"
max_load="$(python3 - <<'PY' "$cpu_count" "$load_per_cpu_threshold"
import sys
print(float(sys.argv[1]) * float(sys.argv[2]))
PY
)"

if [[ "$disk_used" -ge "$disk_threshold" ]]; then
  echo "backpressure active: disk usage ${disk_used}% exceeds ${disk_threshold}%" >&2
  exit 1
fi
if [[ "$free_mb" -le "$free_mb_threshold" ]]; then
  echo "backpressure active: free space ${free_mb}MB is below ${free_mb_threshold}MB" >&2
  exit 1
fi
if ! python3 - <<'PY' "$load_avg" "$max_load"
import sys
if float(sys.argv[1]) > float(sys.argv[2]):
    raise SystemExit(1)
PY
then
  echo "backpressure active: load average ${load_avg} exceeds ${max_load}" >&2
  exit 1
fi

echo "backpressure check passed"

