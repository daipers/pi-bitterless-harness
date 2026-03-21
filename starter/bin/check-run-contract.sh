#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 runs/<run-id>" >&2
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

if [[ ! -d "$run_dir" ]]; then
  echo "run directory not found: $run_dir" >&2
  exit 2
fi

task_md="$run_dir/task.md"
run_md="$run_dir/RUN.md"
run_schema_path="$run_dir/result.schema.json"
run_contract_path="$run_dir/run.contract.json"

errors=0

if [[ ! -f "$task_md" ]]; then
  echo "missing required task file: $task_md" >&2
  errors=$((errors + 1))
fi
if [[ ! -f "$run_md" ]]; then
  echo "missing required run file: $run_md" >&2
  errors=$((errors + 1))
fi
if [[ ! -f "$run_schema_path" ]]; then
  echo "missing run schema: $run_schema_path" >&2
  errors=$((errors + 1))
fi
if [[ ! -f "$run_contract_path" ]]; then
  echo "missing run contract: $run_contract_path" >&2
  errors=$((errors + 1))
fi
if [[ -f "$run_schema_path" ]]; then
  if ! PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - "$run_schema_path" <<'PY'
import json
import sys

json.load(open(sys.argv[1], encoding="utf-8"))
PY
  then
    echo "run schema is not valid JSON: $run_schema_path" >&2
    errors=$((errors + 1))
  fi
fi
if [[ -f "$run_contract_path" ]]; then
  if ! PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - "$run_contract_path" <<'PY'
import json
import pathlib
import sys

from harnesslib import validate_run_contract

payload = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
errors = validate_run_contract(payload)
if errors:
    raise SystemExit("; ".join(errors))
PY
  then
    echo "run contract is invalid: $run_contract_path" >&2
    errors=$((errors + 1))
  fi
fi
if [[ -f "$task_md" ]] && ! grep -q '^## Result JSON schema (source of truth)' "$task_md"; then
  echo "task contract missing: ## Result JSON schema (source of truth)" >&2
  errors=$((errors + 1))
fi
if [[ -f "$task_md" ]] && ! grep -q '^## Eval' "$task_md"; then
  echo "task contract missing: ## Eval" >&2
  errors=$((errors + 1))
fi
if [[ -f "$task_md" ]] && ! grep -q '^## Required Artifacts' "$task_md"; then
  echo "task contract missing: ## Required Artifacts" >&2
  errors=$((errors + 1))
fi

if [[ ! -f "$run_dir/result.template.json" ]]; then
  echo "missing required run artifact: $run_dir/result.template.json" >&2
  errors=$((errors + 1))
fi

if [[ -f "$task_md" ]]; then
  parse_output="$(PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 "$script_dir/parse_task.py" "$task_md" 2>/dev/null || true)"
  if [[ -z "$parse_output" ]]; then
    echo "failed to parse task sections from: $task_md" >&2
    errors=$((errors + 1))
  else
    if ! PARSE_OUTPUT="$parse_output" python3 - "$run_dir/task.md" <<'PY'
import json
import os

data_text = os.environ.get("PARSE_OUTPUT", "").strip()
if not data_text:
    raise SystemExit("no parse output")

data = json.loads(data_text)
if data.get("ok") is not True:
    raise SystemExit("; ".join(data.get("errors", [])))
if not isinstance(data.get("eval_commands"), list):
    raise SystemExit("eval_commands must be a JSON array")
if not isinstance(data.get("required_artifacts"), list):
    raise SystemExit("required_artifacts must be a JSON array")
if not isinstance(data.get("eval_command_details"), list):
    raise SystemExit("eval_command_details must be a JSON array")
dangerous = data.get("dangerous_eval_commands", [])
allow_dangerous = os.environ.get("HARNESS_ALLOW_DANGEROUS_EVAL") not in {"", "0", None, "false", "False"}
allow_network = os.environ.get("HARNESS_ALLOW_NETWORK_TASKS") not in {"", "0", None, "false", "False"}
if dangerous and not allow_dangerous:
    raise SystemExit("dangerous eval commands require HARNESS_ALLOW_DANGEROUS_EVAL=1")
for item in dangerous:
    if item.get("network_access") and not allow_network:
        raise SystemExit("networked eval commands require HARNESS_ALLOW_NETWORK_TASKS=1")
PY
    then
      echo "task sections not parse-clean: eval_commands or required_artifacts malformed" >&2
      errors=$((errors + 1))
    fi
  fi
fi

if [[ -f "$task_md" ]] && [[ -f "$run_schema_path" ]]; then
  if ! PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - "$task_md" "$run_schema_path" <<'PY'
import json
import pathlib
import sys

from harnesslib import parse_task_file

parsed = parse_task_file(pathlib.Path(sys.argv[1]))
schema_payload = json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
if parsed.get("result_schema") != schema_payload:
    raise SystemExit("task schema block must match run result.schema.json exactly")
PY
  then
    echo "task schema block does not match run schema" >&2
    errors=$((errors + 1))
  fi
fi

if (( errors > 0 )); then
  echo "contract check failed: ${errors} issue(s)" >&2
  exit 2
fi

echo "run contract check passed: $run_dir"
exit 0
