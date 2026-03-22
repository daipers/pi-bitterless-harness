#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import sys

from harnesslib import load_policy
from retrieval_index import sync_retrieval_index


def usage() -> int:
    print(
        "usage: rebuild_retrieval_index.py [runs-root] [policy-path]",
        file=sys.stderr,
    )
    return 2


def default_runs_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1] / "runs"


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) > 2:
        return usage()

    runs_root = pathlib.Path(args[0]).resolve() if args else default_runs_root()
    policy = load_policy(args[1], repo_root=runs_root.parent) if len(args) == 2 else None
    state = sync_retrieval_index(
        runs_root,
        exclude_run_id=None,
        eval_policy=policy,
        force_rebuild=True,
    )
    payload = {
        "index_root": str(state["index_root"]),
        "index_version": state["index_version"],
        "index_mode": state["index_mode"],
        "retrieval_profile_id": state.get("retrieval_profile_id"),
        "candidate_run_count": state["candidate_run_count"],
        "refreshed_run_count": state["refreshed_run_count"],
        "evicted_run_count": state["evicted_run_count"],
        "eligible_run_count": sum(1 for entry in state["entries"] if entry.get("eligible")),
        "ineligible_run_count": sum(1 for entry in state["entries"] if not entry.get("eligible")),
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
