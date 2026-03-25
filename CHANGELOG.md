# Changelog

## 1.0.0
- Hardened the harness around a versioned run contract, structured manifests, stricter task parsing, and explicit failure classification.
- Added deterministic ship gates: preflight tooling checks, security scans, integration/property/e2e tests, and CI enforcement.
- Added release/readiness helpers for evidence archiving, reproducible artifacts, provenance output, and rollback drills.
- Clarified that `managed_rpc` is gated by `--managed-rpc-probe` contract validation today, while real CLI canaries and future managed canary tracks are reported separately until a real managed transport adapter exists.
