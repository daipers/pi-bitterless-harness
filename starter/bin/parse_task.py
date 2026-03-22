#!/usr/bin/env python3
import json
import pathlib
import sys

from harnesslib import load_policy, parse_task_file


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) not in {1, 2}:
        print("usage: parse_task.py /path/to/task.md [policy-path]", file=sys.stderr)
        return 2

    task_path = pathlib.Path(args[0]).resolve()
    eval_policy = load_policy(args[1]) if len(args) == 2 else None
    payload = parse_task_file(task_path, eval_policy=eval_policy)
    print(json.dumps(payload, indent=2))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
