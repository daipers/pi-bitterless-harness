from __future__ import annotations

import pathlib
import shutil
import subprocess
import sys

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
STARTER_BIN = REPO_ROOT / "starter" / "bin"

if str(STARTER_BIN) not in sys.path:
    sys.path.insert(0, str(STARTER_BIN))


@pytest.fixture(autouse=True)
def stable_backpressure_thresholds(monkeypatch) -> None:
    # Keep runner subprocess tests deterministic instead of depending on host load or free space.
    monkeypatch.setenv("HARNESS_DISK_USED_THRESHOLD_PERCENT", "100")
    monkeypatch.setenv("HARNESS_FREE_MB_THRESHOLD", "0")
    monkeypatch.setenv("HARNESS_LOAD_PER_CPU_THRESHOLD", "999")


@pytest.fixture
def isolated_repo(tmp_path: pathlib.Path) -> pathlib.Path:
    destination = tmp_path / "repo"
    shutil.copytree(
        REPO_ROOT,
        destination,
        ignore=shutil.ignore_patterns(
            ".git",
            "runs",
            "starter/runs",
            ".pytest_cache",
            ".ruff_cache",
            "dist",
        ),
    )
    subprocess.run(["git", "init"], cwd=destination, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "config", "user.email", "tests@example.com"],
        cwd=destination,
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Harness Tests"],
        cwd=destination,
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(["git", "add", "."], cwd=destination, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "commit", "-m", "initial"],
        cwd=destination,
        check=True,
        capture_output=True,
        text=True,
    )
    return destination
