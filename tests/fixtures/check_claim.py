#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: check_claim.py /path/to/file expected-text", file=sys.stderr)
        return 2

    path = pathlib.Path(sys.argv[1])
    expected = sys.argv[2]
    actual = path.read_text(encoding="utf-8").strip()
    if actual != expected:
        print(f"expected {expected!r}, got {actual!r}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
