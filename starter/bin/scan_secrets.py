#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import sys

from harnesslib import scan_paths_for_secrets


def collect_files(root: pathlib.Path) -> list[pathlib.Path]:
    paths = []
    for child in root.rglob("*"):
        if child.is_dir():
            continue
        if any(
            part
            in {
                "home",
                "session",
                ".git",
                "node_modules",
                ".pytest_cache",
                ".ruff_cache",
                ".hypothesis",
                ".venv",
                "__pycache__",
                "dist",
            }
            for part in child.parts
        ):
            continue
        paths.append(child)
    return paths


if len(sys.argv) < 2:
    print("usage: scan_secrets.py <path> [<path> ...]", file=sys.stderr)
    sys.exit(2)

all_files: list[pathlib.Path] = []
for raw_path in sys.argv[1:]:
    path = pathlib.Path(raw_path).resolve()
    if path.is_file():
        all_files.append(path)
    elif path.is_dir():
        all_files.extend(collect_files(path))

findings = scan_paths_for_secrets(all_files)
print(json.dumps({"paths_scanned": len(all_files), "findings": findings}, indent=2))
sys.exit(1 if findings else 0)
