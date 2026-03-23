#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

if [[ -d "$repo_root/.venv/bin" ]]; then
  export PATH="$repo_root/.venv/bin:$PATH"
fi

"$script_dir/check-tools.sh"
"$script_dir/check-supported-runtime.sh"

for script in "$script_dir"/*.sh; do
  bash -n "$script"
done

shellcheck "$script_dir"/*.sh
ruff check "$repo_root/starter/bin" "$repo_root/tests"
python3 -m pytest
python3 - <<'PY' "$repo_root"
import json
import pathlib
import sys

import jsonschema

repo_root = pathlib.Path(sys.argv[1])
for rel_path in [
    "starter/contracts/context-manifest-v1.schema.json",
    "starter/contracts/benchmark-report-v1.schema.json",
    "starter/contracts/release-gate-v1.schema.json",
    "starter/contracts/trajectory-record-v1.schema.json",
    "starter/contracts/retrieval-example-v1.schema.json",
    "starter/contracts/retrieval-document-v1.schema.json",
    "starter/contracts/policy-example-v1.schema.json",
    "starter/contracts/model-example-v1.schema.json",
    "starter/contracts/candidate-manifest-v1.schema.json",
    "starter/contracts/candidate-report-v1.schema.json",
    "starter/contracts/run-event-v1.schema.json",
    "starter/contracts/runtime-governance-v1.schema.json",
]:
    schema = json.loads((repo_root / rel_path).read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
registry = json.loads(
    (repo_root / "starter" / "governance" / "runtime-governance-v1.json").read_text(
        encoding="utf-8"
    )
)
registry_schema = json.loads(
    (repo_root / "starter" / "contracts" / "runtime-governance-v1.schema.json").read_text(
        encoding="utf-8"
    )
)
jsonschema.validate(registry, registry_schema)
PY
bandit -q --ini "$repo_root/.bandit" -r "$repo_root/starter/bin"
pip-audit -r "$repo_root/requirements-dev.txt"
python3 "$script_dir/scan_secrets.py" "$repo_root"
if [[ -f "$repo_root/Dockerfile" ]]; then
  trivy fs --scanners vuln,secret,misconfig "$repo_root"
else
  echo "trivy placeholder: no container image or Dockerfile present"
fi
