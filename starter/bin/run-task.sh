#!/usr/bin/env bash
set -euo pipefail

profile_override=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      if [[ $# -lt 2 ]]; then
        echo "usage: $0 [--profile strict|capability] runs/<run-id> [model-pattern]" >&2
        exit 2
      fi
      profile_override="$2"
      shift 2
      ;;
    --profile=*)
      profile_override="${1#*=}"
      shift
      ;;
    --help|-h)
      echo "usage: $0 [--profile strict|capability] runs/<run-id> [model-pattern]" >&2
      exit 0
      ;;
    --*)
      echo "unknown option: $1" >&2
      echo "usage: $0 [--profile strict|capability] runs/<run-id> [model-pattern]" >&2
      exit 2
      ;;
    *)
      break
      ;;
  esac
done

if [[ $# -lt 1 ]]; then
  echo "usage: $0 [--profile strict|capability] runs/<run-id> [model-pattern]" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

run_dir="$(python3 - <<'PY' "$1" "$repo_root"
import pathlib, sys

candidate = pathlib.Path(sys.argv[1]).expanduser()
repo_root = pathlib.Path(sys.argv[2]).resolve()
if not candidate.is_absolute():
    candidate = repo_root / candidate
print(candidate.resolve())
PY
)"
model="${2:-}"
pi_bin="${HARNESS_PI_BIN:-pi}"
strict_mode="${HARNESS_STRICT_MODE:-1}"
force_rerun="${HARNESS_FORCE_RERUN:-0}"
model_timeout_seconds="${HARNESS_MODEL_TIMEOUT_SECONDS:-900}"
retry_count="${HARNESS_PI_RETRY_COUNT:-2}"

case "${profile_override:-}" in
  ""|strict|capability)
    ;;
  *)
    echo "unsupported profile override: $profile_override" >&2
    exit 2
    ;;
esac

if [[ ! -d "$run_dir" ]]; then
  echo "run directory not found: $run_dir" >&2
  exit 2
fi

run_id="$(basename "$run_dir")"
task_md="$run_dir/task.md"
run_md="$run_dir/RUN.md"
run_schema_path="$run_dir/result.schema.json"
result_template_path="$run_dir/result.template.json"
run_contract_path="$run_dir/run.contract.json"
manifest_path="$run_dir/outputs/run_manifest.json"
event_log_path="$run_dir/run-events.jsonl"
state_file="$run_dir/run.state"
lock_dir="$run_dir/.run-lock"
trace_id="$(PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - <<'PY'
import uuid
print(uuid.uuid4().hex)
PY
)"
phase="resolve"
error_code=""
run_started_epoch_ms="$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"
pi_started_epoch_ms=""
pi_finished_epoch_ms=""
score_started_epoch_ms=""
score_finished_epoch_ms=""
run_finished_epoch_ms=""
run_contract_version=""
execution_profile="strict"
policy_path="policies/strict.json"
context_enabled="0"
context_manifest_rel=""
context_summary_rel=""
context_source_run_ids=""
context_bootstrap_mode=""

mkdir -p "$run_dir/outputs" "$run_dir/home" "$run_dir/session" "$run_dir/score"
mkdir -p "$run_dir/home/.pi/agent"

log_event() {
  local phase_name="$1"
  local message="$2"
  local error="${3:-}"
  PHASE_NAME="$phase_name" MESSAGE="$message" ERROR_CODE="$error" \
  EVENT_LOG_PATH="$event_log_path" TRACE_ID="$trace_id" RUN_ID="$run_id" \
  python3 - <<'PY'
import json
import os
import pathlib
from datetime import UTC, datetime

path = pathlib.Path(os.environ["EVENT_LOG_PATH"])
path.parent.mkdir(parents=True, exist_ok=True)
payload = {
    "ts": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "trace_id": os.environ["TRACE_ID"],
    "run_id": os.environ["RUN_ID"],
    "phase": os.environ["PHASE_NAME"],
    "duration_ms": None,
    "error_code": os.environ["ERROR_CODE"] or None,
    "message": os.environ["MESSAGE"],
}
with path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(payload, sort_keys=True) + "\n")
PY
}

write_manifest() {
  local state="$1"
  local phase_name="$2"
  local error="${3:-}"
  local run_finished="${4:-}"
  RUN_DIR="$run_dir" REPO_ROOT="$repo_root" RUN_ID="$run_id" TRACE_ID="$trace_id" \
  MANIFEST_PATH="$manifest_path" STATE="$state" PHASE_NAME="$phase_name" ERROR_CODE="$error" \
  RUN_STARTED_EPOCH_MS="$run_started_epoch_ms" PI_STARTED_EPOCH_MS="$pi_started_epoch_ms" \
  PI_FINISHED_EPOCH_MS="$pi_finished_epoch_ms" SCORE_STARTED_EPOCH_MS="$score_started_epoch_ms" \
  SCORE_FINISHED_EPOCH_MS="$score_finished_epoch_ms" RUN_FINISHED_EPOCH_MS="$run_finished" \
  PI_BIN="$pi_bin" STRICT_MODE="$strict_mode" FORCE_RERUN="$force_rerun" \
  MODEL_TIMEOUT_SECONDS="$model_timeout_seconds" RETRY_COUNT="$retry_count" \
  RUN_CONTRACT_VERSION="$run_contract_version" EXECUTION_PROFILE="$execution_profile" \
  POLICY_PATH="$policy_path" CONTEXT_ENABLED="$context_enabled" \
  CONTEXT_MANIFEST_REL="$context_manifest_rel" CONTEXT_SUMMARY_REL="$context_summary_rel" \
  CONTEXT_SOURCE_RUN_IDS="$context_source_run_ids" CONTEXT_BOOTSTRAP_MODE="$context_bootstrap_mode" \
  PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - <<'PY'
from __future__ import annotations

import json
import os
import pathlib
import subprocess
from typing import Any

from harnesslib import RUNNER_VERSION, compute_dependencies_hash, now_utc, sha256_file, write_json

run_dir = pathlib.Path(os.environ["RUN_DIR"])
repo_root = pathlib.Path(os.environ["REPO_ROOT"])
manifest_path = pathlib.Path(os.environ["MANIFEST_PATH"])


def read_json(path: pathlib.Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def command_output(command: list[str]) -> str:
    try:
        completed = subprocess.run(command, text=True, capture_output=True, check=False)
    except Exception as exc:
        return f"unavailable: {exc}"
    return (completed.stdout or completed.stderr or "").strip()


def git_sha() -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def duration(start: str, end: str) -> int | None:
    if not start or not end:
        return None
    return max(0, int(end) - int(start))


score_payload = read_json(run_dir / "score.json") or {}
context_payload = (
    read_json(run_dir / os.environ["CONTEXT_MANIFEST_REL"])
    if os.environ["CONTEXT_ENABLED"] == "1" and os.environ["CONTEXT_MANIFEST_REL"]
    else {}
) or {}
dependencies = {
    "pi": command_output([os.environ["PI_BIN"], "--version"]),
    "python3": command_output(["python3", "--version"]),
    "bash": command_output(["bash", "--version"]),
    "git": command_output(["git", "--version"]),
}
manifest = {
    "manifest_version": "v1",
    "runner_version": RUNNER_VERSION,
    "run_id": os.environ["RUN_ID"],
    "trace_id": os.environ["TRACE_ID"],
    "generated_at": now_utc(),
    "state": os.environ["STATE"],
    "phase": os.environ["PHASE_NAME"],
    "error_code": os.environ["ERROR_CODE"] or None,
    "run_contract_version": "v1",
    "paths": {
        "task_md": "task.md",
        "run_md": "RUN.md",
        "run_contract": "run.contract.json",
        "result_schema": "result.schema.json",
        "result_template": "result.template.json",
        "result_json": "result.json",
        "score_json": "score.json",
        "event_log": "run-events.jsonl",
        "manifest": "outputs/run_manifest.json",
    },
    "dependencies": {
        **dependencies,
        "hash": compute_dependencies_hash(dependencies),
    },
    "git": {
        "sha": git_sha(),
    },
    "timings": {
        "run_started_epoch_ms": int(os.environ["RUN_STARTED_EPOCH_MS"]),
        "pi_started_epoch_ms": int(os.environ["PI_STARTED_EPOCH_MS"]) if os.environ["PI_STARTED_EPOCH_MS"] else None,
        "pi_finished_epoch_ms": int(os.environ["PI_FINISHED_EPOCH_MS"]) if os.environ["PI_FINISHED_EPOCH_MS"] else None,
        "score_started_epoch_ms": int(os.environ["SCORE_STARTED_EPOCH_MS"]) if os.environ["SCORE_STARTED_EPOCH_MS"] else None,
        "score_finished_epoch_ms": int(os.environ["SCORE_FINISHED_EPOCH_MS"]) if os.environ["SCORE_FINISHED_EPOCH_MS"] else None,
        "run_finished_epoch_ms": int(os.environ["RUN_FINISHED_EPOCH_MS"]) if os.environ["RUN_FINISHED_EPOCH_MS"] else None,
        "pi_duration_ms": duration(os.environ["PI_STARTED_EPOCH_MS"], os.environ["PI_FINISHED_EPOCH_MS"]),
        "score_duration_ms": duration(os.environ["SCORE_STARTED_EPOCH_MS"], os.environ["SCORE_FINISHED_EPOCH_MS"]),
        "run_duration_ms": duration(os.environ["RUN_STARTED_EPOCH_MS"], os.environ["RUN_FINISHED_EPOCH_MS"]),
    },
    "snapshots": {
        "task_sha256": sha256_file(run_dir / "task.md"),
        "run_md_sha256": sha256_file(run_dir / "RUN.md"),
        "run_contract_sha256": sha256_file(run_dir / "run.contract.json"),
        "result_schema_sha256": sha256_file(run_dir / "result.schema.json"),
        "prompt_sha256": sha256_file(run_dir / "prompt.txt"),
        "result_sha256": sha256_file(run_dir / "result.json"),
        "score_sha256": sha256_file(run_dir / "score.json"),
    },
    "invariants": {
        "task_exists": (run_dir / "task.md").exists(),
        "run_md_exists": (run_dir / "RUN.md").exists(),
        "run_contract_exists": (run_dir / "run.contract.json").exists(),
        "result_schema_exists": (run_dir / "result.schema.json").exists(),
        "result_template_exists": (run_dir / "result.template.json").exists(),
        "event_log_exists": (run_dir / "run-events.jsonl").exists(),
        "writeable_outputs_dir": os.access(run_dir / "outputs", os.W_OK),
        "score_available": (run_dir / "score.json").exists(),
        "overall_pass": score_payload.get("overall_pass"),
    },
    "audit": {
        "strict_mode": os.environ["STRICT_MODE"] not in {"0", "false", "False"},
        "force_rerun": os.environ["FORCE_RERUN"] not in {"0", "false", "False"},
        "allow_dangerous_eval": os.environ.get("HARNESS_ALLOW_DANGEROUS_EVAL") not in {None, "", "0", "false", "False"},
        "allow_network_tasks": os.environ.get("HARNESS_ALLOW_NETWORK_TASKS") not in {None, "", "0", "false", "False"},
        "model_timeout_seconds": int(os.environ["MODEL_TIMEOUT_SECONDS"]),
        "retry_count": int(os.environ["RETRY_COUNT"]),
        "score_failure_classifications": score_payload.get("failure_classifications", []),
        "secret_scan": {
            "scanned_path_count": (
                (score_payload.get("secret_scan") or {}).get("scanned_path_count")
            ),
            "skipped_path_count": (
                (score_payload.get("secret_scan") or {}).get("skipped_path_count")
            ),
            "skipped_reason_counts": (
                (score_payload.get("secret_scan") or {}).get("skipped_reason_counts")
            ),
        },
    },
    "execution": {
        "contract_version": os.environ["RUN_CONTRACT_VERSION"] or None,
        "profile": os.environ["EXECUTION_PROFILE"],
        "policy_path": os.environ["POLICY_PATH"],
    },
    "context": {
        "enabled": os.environ["CONTEXT_ENABLED"] == "1",
        "manifest_path": os.environ["CONTEXT_MANIFEST_REL"] or None,
        "summary_path": os.environ["CONTEXT_SUMMARY_REL"] or None,
        "source_run_ids": [
            item for item in os.environ["CONTEXT_SOURCE_RUN_IDS"].split(",") if item
        ],
        "bootstrap_mode": os.environ["CONTEXT_BOOTSTRAP_MODE"] or None,
        "candidate_run_count": context_payload.get("candidate_run_count"),
        "eligible_run_count": context_payload.get("eligible_run_count"),
        "selected_count": context_payload.get("selected_count"),
        "ranking_latency_ms": context_payload.get("ranking_latency_ms"),
        "artifact_bytes_copied": context_payload.get("artifact_bytes_copied"),
    },
}
write_json(manifest_path, manifest)
PY
}

archive_partial_run() {
  local stamp
  stamp="$(date +%Y%m%d-%H%M%S)"
  local recovery_dir="$run_dir/recovery/$stamp"
  mkdir -p "$recovery_dir"
  for rel in transcript.jsonl pi.stderr.log prompt.txt score.json git.status.txt patch.diff pi.exit_code.txt run-events.jsonl score outputs/run_manifest.json result.json; do
    if [[ -e "$run_dir/$rel" ]]; then
      mv "$run_dir/$rel" "$recovery_dir/" 2>/dev/null || true
    fi
  done
}

# shellcheck disable=SC2329  # Invoked indirectly by trap on process exit.
cleanup() {
  rm -rf "$lock_dir"
}

# shellcheck disable=SC2329  # Invoked indirectly by trap on INT/TERM.
handle_signal() {
  local signal_name="$1"
  error_code="cancelled"
  phase="cancelled"
  printf 'cancelled\n' > "$state_file"
  run_finished_epoch_ms="$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"
  log_event "$phase" "received $signal_name" "$error_code"
  write_manifest "cancelled" "$phase" "$error_code" "$run_finished_epoch_ms"
  cleanup
  exit 130
}

trap 'handle_signal SIGINT' INT
trap 'handle_signal SIGTERM' TERM
trap 'cleanup' EXIT

current_state="new"
if [[ -d "$lock_dir" ]]; then
  if [[ -f "$lock_dir/pid" ]] && kill -0 "$(cat "$lock_dir/pid")" 2>/dev/null; then
    echo "run state: running"
    exit 3
  fi
  current_state="partial"
fi
if [[ -f "$manifest_path" ]]; then
  manifest_state="$(python3 - <<'PY' "$manifest_path"
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    print("partial")
else:
    print(payload.get("state", "partial"))
PY
)"
  if [[ "$manifest_state" == "complete" && "$force_rerun" != "1" ]]; then
    echo "run state: complete"
    exit 0
  fi
  if [[ "$manifest_state" == "complete" && "$force_rerun" == "1" ]]; then
    current_state="partial"
  fi
  if [[ "$manifest_state" != "complete" ]]; then
    current_state="partial"
  fi
fi

if [[ "$current_state" == "partial" ]]; then
  archive_partial_run
fi

mkdir -p "$lock_dir"
printf '%s\n' "$$" > "$lock_dir/pid"
printf 'running\n' > "$state_file"

phase="validate"
log_event "$phase" "starting run validation"

for path in "$task_md" "$run_md"; do
  if [[ ! -f "$path" ]]; then
    echo "missing required file: $path" >&2
    error_code="contract_invalid"
    write_manifest "partial" "$phase" "$error_code" ""
    exit 2
  fi
done

if [[ -f "$repo_root/result.schema.json" ]]; then
  cp "$repo_root/result.schema.json" "$run_schema_path"
fi

if [[ ! -f "$result_template_path" ]]; then
  PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - "$result_template_path" <<'PY'
from harnesslib import make_result_template, write_json
import pathlib
import sys

write_json(pathlib.Path(sys.argv[1]), make_result_template())
PY
fi

if [[ ! -f "$run_contract_path" ]]; then
  PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - "$run_contract_path" <<'PY'
from harnesslib import default_run_contract, write_json
import pathlib
import sys

write_json(pathlib.Path(sys.argv[1]), default_run_contract(version="v2", execution_profile="strict"))
PY
fi

execution_settings=()
while IFS= read -r line; do
  execution_settings+=("$line")
done < <(PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - "$run_contract_path" "$profile_override" <<'PY'
import pathlib
import sys

from harnesslib import load_run_contract, resolve_execution_settings

run_contract = load_run_contract(pathlib.Path(sys.argv[1]))
profile_override = sys.argv[2] or None
settings = resolve_execution_settings(run_contract, profile_override=profile_override)
print(settings["run_contract_version"])
print(settings["execution_profile"])
print(settings["policy_path"])
print("1" if settings["retrieval_enabled"] else "0")
print(settings["context_manifest_path"])
print(settings["context_summary_path"])
PY
)
run_contract_version="${execution_settings[0]:-}"
execution_profile="${execution_settings[1]:-strict}"
policy_path="${execution_settings[2]:-policies/strict.json}"
context_enabled="${execution_settings[3]:-0}"
context_manifest_rel="${execution_settings[4]:-context/retrieval-manifest.json}"
context_summary_rel="${execution_settings[5]:-context/retrieval-summary.md}"

for exe in bash python3 cat git; do
  if ! command -v "$exe" >/dev/null 2>&1; then
    echo "missing required executable: $exe" >&2
    error_code="contract_invalid"
    write_manifest "partial" "$phase" "$error_code" ""
    exit 127
  fi
done

if [[ "$pi_bin" == */* ]]; then
  if [[ ! -x "$pi_bin" ]]; then
    echo "pi executable is not runnable: $pi_bin" >&2
    error_code="contract_invalid"
    write_manifest "partial" "$phase" "$error_code" ""
    exit 127
  fi
else
  if ! command -v "$pi_bin" >/dev/null 2>&1; then
    echo "pi is not on PATH" >&2
    error_code="contract_invalid"
    write_manifest "partial" "$phase" "$error_code" ""
    exit 127
  fi
fi

"$pi_bin" --version > "$run_dir/pi.version.txt" 2>&1 || {
  echo "pi --version probe failed" >&2
  error_code="contract_invalid"
  write_manifest "partial" "$phase" "$error_code" ""
  exit 127
}

touch "$run_dir/.write-probe"
rm -f "$run_dir/.write-probe"

if [[ "$strict_mode" != "0" ]]; then
  "$script_dir/check-backpressure.sh" "$run_dir"
fi

"$script_dir/check-run-contract.sh" "$run_dir"

if [[ -n "${HARNESS_PI_AUTH_JSON:-}" ]]; then
  if [[ ! -f "$HARNESS_PI_AUTH_JSON" ]]; then
    echo "HARNESS_PI_AUTH_JSON does not exist: $HARNESS_PI_AUTH_JSON" >&2
    error_code="contract_invalid"
    write_manifest "partial" "$phase" "$error_code" ""
    exit 2
  fi
  cp "$HARNESS_PI_AUTH_JSON" "$run_dir/home/.pi/agent/auth.json"
fi

if [[ "$context_enabled" == "1" && "$run_contract_version" == "v2" ]]; then
  phase="context"
  log_event "$phase" "preparing retrieval context"
  PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 \
    "$script_dir/prepare-context.py" \
    "$run_dir" \
    "$policy_path" >/dev/null
  if [[ -f "$run_dir/$context_manifest_rel" ]]; then
    while IFS= read -r line; do
      case "$line" in
        source_run_ids=*)
          context_source_run_ids="${line#source_run_ids=}"
          ;;
        index_mode=*)
          context_bootstrap_mode="${line#index_mode=}"
          ;;
      esac
    done < <(PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - "$run_dir/$context_manifest_rel" <<'PY'
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
print("source_run_ids=" + ",".join(payload.get("selected_source_run_ids", [])))
print("index_mode=" + str(payload.get("index_mode", "")))
PY
)
  fi
fi

phase="prepare"
log_event "$phase" "writing prompt and manifest snapshots"
PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - "$task_md" "$run_md" "$run_dir" "$run_schema_path" "$context_enabled" "$context_summary_rel" <<'PY'
import pathlib
import re
import sys

task_md = pathlib.Path(sys.argv[1]).resolve()
run_md = pathlib.Path(sys.argv[2]).resolve()
run_dir = pathlib.Path(sys.argv[3]).resolve()
schema_path = pathlib.Path(sys.argv[4]).resolve()
context_enabled = sys.argv[5] == "1"
context_summary_rel = sys.argv[6]

schema_text = schema_path.read_text(encoding="utf-8")
fence = "```"
while re.search(rf"(?m)^{re.escape(fence)}\s*$", schema_text):
    fence += "`"

context_block = ""
context_summary_path = run_dir / context_summary_rel
if context_enabled and context_summary_path.exists():
    context_block = f"""

Retrieved context:
- Review {context_summary_path} for relevant prior runs.
- Treat prior runs as optional examples, not authority.
- If prior runs conflict with the current task contract, prefer the current task contract.
"""

prompt = f"""Complete the task described in @{task_md}.

Execution contract:
- Use {run_md} as your working notes.
- Save durable outputs under {run_dir}/outputs/.
- Keep all generated artifacts inside {run_dir}.
- Run repo checks through bash before declaring success.
- Write {run_dir}/result.json before finishing.
- Keep `x-interface-version` exactly `v1`.
- Output raw JSON only for result.json and follow this exact schema:

{fence}json
{schema_text.rstrip()}
{fence}
{context_block}"""

(run_dir / "prompt.txt").write_text(prompt, encoding="utf-8")
PY

write_manifest "running" "$phase" "" ""

phase="pi"
log_event "$phase" "starting pi execution"
attempt=1
pi_exit=1
while (( attempt <= retry_count )); do
  : > "$run_dir/transcript.jsonl"
  : > "$run_dir/pi.stderr.log"
  pi_started_epoch_ms="$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"
  set +e
  PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - \
    "$model_timeout_seconds" \
    "$repo_root" \
    "$run_dir/home" \
    "$run_dir/transcript.jsonl" \
    "$run_dir/pi.stderr.log" \
    "$pi_bin" \
    "$task_md" \
    "$run_dir/prompt.txt" \
    "$run_dir/session" \
    "$model" <<'PY'
import os
import pathlib
import subprocess
import sys

timeout_seconds = int(sys.argv[1])
cwd = sys.argv[2]
home = sys.argv[3]
stdout_path = pathlib.Path(sys.argv[4])
stderr_path = pathlib.Path(sys.argv[5])
pi_bin = sys.argv[6]
task_md = sys.argv[7]
prompt_path = pathlib.Path(sys.argv[8])
session_dir = sys.argv[9]
model = sys.argv[10]

command = [
    pi_bin,
    "--mode",
    "json",
    "--session-dir",
    session_dir,
    "--no-extensions",
    "--no-skills",
    "--no-prompt-templates",
    "--no-themes",
]
if model:
    command.extend(["--model", model])
command.extend([f"@{task_md}", prompt_path.read_text(encoding="utf-8")])

env = os.environ.copy()
env["HOME"] = home
with stdout_path.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            stdout=stdout_handle,
            stderr=stderr_handle,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired:
        sys.exit(124)
sys.exit(completed.returncode)
PY
  pi_exit=$?
  set -e
  pi_finished_epoch_ms="$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"
  printf '%s\n' "$pi_exit" > "$run_dir/pi.exit_code.txt"

  if [[ "$pi_exit" -eq 0 ]]; then
    break
  fi
  if [[ ! -s "$run_dir/transcript.jsonl" && "$attempt" -lt "$retry_count" ]]; then
    log_event "$phase" "retrying pi startup failure" "model_invocation_failed"
    sleep "$attempt"
    attempt=$((attempt + 1))
    continue
  fi
  break
done

if git -C "$repo_root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git -C "$repo_root" status --short > "$run_dir/git.status.txt" || true
  git -C "$repo_root" diff --binary > "$run_dir/patch.diff" || true
else
  printf 'not a git repository\n' > "$run_dir/git.status.txt"
  : > "$run_dir/patch.diff"
fi

phase="score"
score_started_epoch_ms="$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"
log_event "$phase" "starting scoring"
score_attempt=1
while (( score_attempt <= 2 )); do
  set +e
  HARNESS_EXECUTION_PROFILE="$execution_profile" \
  HARNESS_POLICY_PATH="$policy_path" \
  HARNESS_CONTEXT_ENABLED="$context_enabled" \
  HARNESS_CONTEXT_MANIFEST_PATH="$context_manifest_rel" \
  HARNESS_CONTEXT_SOURCE_RUN_IDS="$context_source_run_ids" \
  PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 \
    "$script_dir/score_run.py" \
    "$task_md" \
    "$run_dir" \
    "$run_dir/pi.exit_code.txt" \
    "$run_dir/score.json" \
    "$run_schema_path" \
    "$event_log_path" >/dev/null
  score_exit=$?
  set -e
  if [[ "$score_exit" -eq 0 ]]; then
    break
  fi
  if (( score_attempt == 2 )); then
    break
  fi
  log_event "$phase" "retrying score generation" "eval_failed"
  sleep "$score_attempt"
  score_attempt=$((score_attempt + 1))
done
score_finished_epoch_ms="$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"

phase="finalize"
run_finished_epoch_ms="$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"
error_code="$(python3 - <<'PY' "$run_dir/score.json" "$pi_exit"
import json
import pathlib
import sys

score_path = pathlib.Path(sys.argv[1])
pi_exit = int(sys.argv[2])
if not score_path.exists():
    print("eval_failed")
    raise SystemExit(0)
payload = json.loads(score_path.read_text(encoding="utf-8"))
if payload.get("overall_pass") is True:
    print("")
elif payload.get("overall_error_code"):
    print(payload["overall_error_code"])
elif pi_exit != 0:
    print("model_invocation_failed")
else:
    print("eval_failed")
PY
)"
printf 'complete\n' > "$state_file"
write_manifest "complete" "$phase" "$error_code" "$run_finished_epoch_ms"
log_event "$phase" "run complete" "$error_code"

echo "run state: complete"
echo "run complete: $run_dir"
echo "pi exit code: $pi_exit"
echo "score: $run_dir/score.json"

exit 0
