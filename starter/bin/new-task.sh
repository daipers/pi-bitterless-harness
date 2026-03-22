#!/usr/bin/env bash
set -euo pipefail

profile="strict"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      if [[ $# -lt 2 ]]; then
        echo "usage: $0 [--profile strict|capability] \"short task title\"" >&2
        exit 2
      fi
      profile="$2"
      shift 2
      ;;
    --profile=*)
      profile="${1#*=}"
      shift
      ;;
    --help|-h)
      echo "usage: $0 [--profile strict|capability] \"short task title\"" >&2
      exit 0
      ;;
    --*)
      echo "unknown option: $1" >&2
      echo "usage: $0 [--profile strict|capability] \"short task title\"" >&2
      exit 2
      ;;
    *)
      break
      ;;
  esac
done

if [[ $# -lt 1 ]]; then
  echo "usage: $0 [--profile strict|capability] \"short task title\"" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
schema_source="$repo_root/result.schema.json"
contract_source="$repo_root/contracts/run-contract-v2.schema.json"
title="$*"
timestamp="$(date +%Y%m%d-%H%M%S)"
slug="$(printf '%s' "$title" \
  | tr '[:upper:]' '[:lower:]' \
  | tr -cd 'a-z0-9[:space:]-' \
  | sed -E 's/[[:space:]]+/-/g; s/^-+//; s/-+$//; s/--+/-/g')"
if [[ -z "$slug" ]]; then
  slug="task"
fi
base_run_id="${timestamp}-${slug}"

mkdir -p "${repo_root}/runs"
run_id="$base_run_id"
run_dir="${repo_root}/runs/${run_id}"
if [[ -e "$run_dir" ]]; then
  suffix=2
  while [[ -e "$run_dir" ]]; do
    run_id="${base_run_id}-${suffix}"
    run_dir="${repo_root}/runs/${run_id}"
    suffix=$((suffix + 1))
  done
fi
run_schema_path="$run_dir/result.schema.json"
result_template_path="$run_dir/result.template.json"
run_contract_path="$run_dir/run.contract.json"

mkdir -p "$run_dir/outputs" "$run_dir/home" "$run_dir/session" "$run_dir/score"

task_template="$repo_root/task.template.md"
run_template="$repo_root/RUN.template.md"
if [[ ! -f "$run_dir/task.md" ]]; then
  if [[ -f "$task_template" ]]; then
    cp "$task_template" "$run_dir/task.md"
  else
    cat > "$run_dir/task.md" <<'EOF'
# Task
Describe the task in one line.

## Goal
Describe the desired outcome in plain language.

## Constraints
- Add explicit limits here
- Mention any files or APIs that must not change
- Mention time or scope constraints if they matter

## Done
- Concrete completion criteria
- External checks should pass
- `result.json` should be written

## Eval
```bash
# One command per non-comment line
# Example:
# npm test -- login.integration.test.ts
```

## Required Artifacts
- result.json

## Notes
Optional context, pointers, or hypotheses.
EOF
  fi
fi

if [[ ! -f "${run_dir}/RUN.md" ]]; then
  if [[ -f "$run_template" ]]; then
    cp "$run_template" "$run_dir/RUN.md"
  else
    cat > "$run_dir/RUN.md" <<'EOF'
# Run Log

## Status
pending

## Working Notes
- Start here.

## Files Touched
- none yet

## Commands Run
- none yet

## Open Questions
- none yet

## Final Check
- [ ] tests/checks run
- [ ] artifacts saved under outputs/
- [ ] result.json written
EOF
  fi
fi

if [[ -f "$schema_source" ]]; then
  cp "$schema_source" "$run_schema_path"
else
  PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - "$run_schema_path" <<'PY'
from harnesslib import RESULT_INTERFACE_VERSION, write_json
import pathlib
import sys

write_json(
    pathlib.Path(sys.argv[1]),
    {
        "x-interface-version": RESULT_INTERFACE_VERSION,
        "status": "success",
        "summary": (
            "Implemented the requested change and wrote outputs/example-report.json "
            "with the final retrieval-quality results."
        ),
        "artifacts": [
            {
                "path": "outputs/example-report.json",
                "description": "Final report artifact capturing the concrete result produced by this run.",
            }
        ],
        "claims": [
            {
                "claim": "The requested retrieval-quality output was generated successfully.",
                "evidence": ["outputs/example-report.json"],
            }
        ],
        "remaining_risks": ["optional remaining risks"],
    },
)
PY
fi

if [[ ! -f "$result_template_path" ]]; then
  PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - "$result_template_path" <<'PY'
from harnesslib import make_result_template, write_json
import pathlib
import sys

write_json(pathlib.Path(sys.argv[1]), make_result_template())
PY
fi

PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - "$run_contract_path" "$profile" <<'PY'
from harnesslib import default_run_contract, write_json
import pathlib
import sys

write_json(
    pathlib.Path(sys.argv[1]),
    default_run_contract(version="v2", execution_profile=sys.argv[2]),
)
PY

if [[ -f "$contract_source" ]]; then
  cp "$contract_source" "$run_dir/run_contract_version.schema.json"
fi

PYTHONPATH="$script_dir${PYTHONPATH:+:$PYTHONPATH}" python3 - "$run_dir/task.md" "$run_schema_path" <<'PY'
import pathlib
import re
import sys

task_path = pathlib.Path(sys.argv[1])
schema_path = pathlib.Path(sys.argv[2])
task_text = task_path.read_text(encoding="utf-8")
schema_block = (
    "## Result JSON schema (source of truth)\n\n"
    "Write `result.json` as raw JSON only. Do not include prose, markdown, or wrapper text.\n\n"
    "```json\n"
    f"{schema_path.read_text(encoding='utf-8').rstrip()}\n"
    "```\n"
)

pattern = re.compile(
    r"^## Result JSON schema \(source of truth\)\n(?:.*\n)*?(?=^## |\Z)",
    flags=re.MULTILINE,
)
if pattern.search(task_text):
    task_text = pattern.sub(schema_block + "\n", task_text, count=1)
else:
    task_text = task_text.rstrip() + "\n\n" + schema_block
task_path.write_text(task_text, encoding="utf-8")
PY

echo "runs/${run_id}"
case "$profile" in
  strict|capability)
    ;;
  *)
    echo "unsupported profile: $profile" >&2
    exit 2
    ;;
esac
