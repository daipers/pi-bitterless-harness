#!/usr/bin/env python3
from __future__ import annotations

import os
import pathlib
import subprocess
import sys


def find_task_path(argv: list[str]) -> pathlib.Path | None:
    for arg in argv:
        if arg.startswith("@"):
            return pathlib.Path(arg[1:]).resolve()
    return None


def main() -> int:
    target_bin = os.environ.get("HARNESS_REAL_PI_BIN", "pi")
    mode = os.environ.get("HARNESS_REAL_PI_PROXY_MODE", "passthrough")

    if mode == "startup-fail-once":
        sentinel_raw = os.environ.get("HARNESS_REAL_PI_PROXY_SENTINEL")
        if not sentinel_raw:
            print(
                "HARNESS_REAL_PI_PROXY_SENTINEL is required for startup-fail-once mode",
                file=sys.stderr,
            )
            return 2
        sentinel = pathlib.Path(sentinel_raw)
        if not sentinel.exists():
            sentinel.parent.mkdir(parents=True, exist_ok=True)
            sentinel.write_text("failed-once\n", encoding="utf-8")
            return 75

    completed = subprocess.run([target_bin, *sys.argv[1:]], check=False)

    if completed.returncode == 0 and mode == "corrupt-result":
        task_path = find_task_path(sys.argv[1:])
        if task_path is None:
            print("task path not provided", file=sys.stderr)
            return 2
        (task_path.parent / "result.json").write_text("{not valid json\n", encoding="utf-8")

    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
