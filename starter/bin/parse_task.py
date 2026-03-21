#!/usr/bin/env python3
import json
import pathlib
import sys

from harnesslib import parse_task_file


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1:
        print("usage: parse_task.py /path/to/task.md", file=sys.stderr)
        return 2

    task_path = pathlib.Path(args[0]).resolve()
    payload = parse_task_file(task_path)
    print(json.dumps(payload, indent=2))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
