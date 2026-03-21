# Threat Model

## Scope
- The harness executes model-guided repo work through `pi` using file-based task contracts.
- The main trust boundaries are task markdown, eval commands, run artifacts, the local filesystem, and optional auth material copied into the isolated run `HOME`.

## Top Threats
1. Dangerous eval commands mutate or exfiltrate data outside the intended repo scope.
2. Secrets are copied into run artifacts, transcripts, patches, or outputs and later shipped as evidence.
3. Partial or cancelled runs leave corrupted state that looks successful to later automation.
4. Unpinned tooling versions make CI and local runs disagree about schema validation, linting, or security findings.
5. Recovery and migration steps lose run evidence, making rollback or audits incomplete.

## Guardrails
- `run-task.sh` always uses an isolated run `HOME`.
- Eval commands are parsed into normalized commands, blocked on dangerous patterns by default, and require explicit opt-in for unsafe/network behavior.
- `score_run.py` scans run artifacts for likely secrets before reporting a clean pass.
- `outputs/run_manifest.json` records contract version, dependency versions, hashes, timing data, and failure classifications.
- Release artifacts are emitted with detached SHA-256 checksums and provenance JSON.

## Residual Risk
- A user can still opt into dangerous eval or network tasks explicitly; this is intentional but auditable through manifest fields and event logs.
- The harness does not sandbox the model or the shell beyond normal OS permissions, so CI and human review remain part of the safety boundary.

