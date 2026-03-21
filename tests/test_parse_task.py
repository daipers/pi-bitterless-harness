from __future__ import annotations

import pathlib

import parse_task
from harnesslib import parse_task_file, parse_task_text
from hypothesis import given
from hypothesis import strategies as st

VALID_TASK = """# Task
Ship the harness.

## Goal
Launch v1.

## Constraints
- Keep the contract stable.

## Done
- Tests pass.

## Eval
```bash
python3 ../tests/fixtures/pass_eval.py
```

## Required Artifacts
- result.json
- outputs/run_manifest.json

## Notes
Optional notes.

## Result JSON schema (source of truth)
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "x-interface-version": "v1",
  "type": "object"
}
```
"""


def test_parse_task_returns_normalized_eval_details(tmp_path: pathlib.Path) -> None:
    task_path = tmp_path / "task.md"
    task_path.write_text(VALID_TASK, encoding="utf-8")

    payload = parse_task_file(task_path)

    assert payload["ok"] is True
    assert payload["eval_commands"] == ["python3 ../tests/fixtures/pass_eval.py"]
    assert payload["eval_command_details"][0]["argv"] == [
        "python3",
        "../tests/fixtures/pass_eval.py",
    ]
    assert payload["dangerous_eval_commands"] == []


def test_parse_task_reports_structured_errors() -> None:
    payload = parse_task_text("# Task\n\n## Goal\nx\n", source="inline")

    assert payload["ok"] is False
    assert "missing required section: Eval" in payload["errors"]
    assert "missing required section: Result JSON schema (source of truth)" in payload["errors"]


def test_parse_task_flags_dangerous_eval() -> None:
    text = VALID_TASK.replace("python3 ../tests/fixtures/pass_eval.py", "rm -rf /tmp/boom")
    payload = parse_task_text(text, source="inline")

    assert payload["ok"] is True
    assert payload["dangerous_eval_commands"][0]["program"] == "rm"
    assert payload["dangerous_eval_commands"][0]["requires_opt_in"] is True


def test_parse_task_main_returns_zero_for_valid_task(tmp_path: pathlib.Path, capsys) -> None:
    task_path = tmp_path / "task.md"
    task_path.write_text(VALID_TASK, encoding="utf-8")

    exit_code = parse_task.main([str(task_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "\"ok\": true" in captured.out


def test_parse_task_main_usage_error(capsys) -> None:
    exit_code = parse_task.main([])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "usage: parse_task.py" in captured.err


def test_parse_task_reports_duplicate_and_unknown_sections() -> None:
    text = VALID_TASK + "\n## Goal\nDuplicate\n\n## Bonus\nUnexpected\n"

    payload = parse_task_text(text, source="inline")

    assert payload["ok"] is False
    assert "duplicate section heading: Goal" in payload["errors"]
    assert "Bonus" in payload["unknown_sections"]


def test_parse_task_reports_schema_block_errors() -> None:
    missing_fence = VALID_TASK.replace(
        (
            "```json\n{\n  \"$schema\": "
            "\"https://json-schema.org/draft/2020-12/schema\",\n"
            "  \"x-interface-version\": \"v1\",\n"
            "  \"type\": \"object\"\n}\n```"
        ),
        "not json",
    )
    payload = parse_task_text(missing_fence, source="inline")
    assert any("fenced ```json block" in error for error in payload["errors"])

    bad_schema = VALID_TASK.replace('"x-interface-version": "v1"', '"x-interface-version": "v2"')
    payload = parse_task_text(bad_schema, source="inline")
    assert "result schema x-interface-version must be v1" in payload["errors"]


@given(st.text(max_size=400))
def test_parse_task_never_crashes_on_fuzzed_markdown(text: str) -> None:
    payload = parse_task_text(text, source="fuzz")
    assert isinstance(payload["ok"], bool)
    assert isinstance(payload["errors"], list)
