from __future__ import annotations

import pathlib

import pytest
from capabilitylib import (
    build_capability_manifest,
    evaluate_intercepted_subagent_action,
    load_capability_library,
    summarize_interception_log,
    validate_subagent_usage,
)


def test_load_capability_library_merges_fragments_and_rejects_duplicates(
    tmp_path: pathlib.Path,
) -> None:
    repo_root = tmp_path / "starter"
    repo_root.mkdir()
    (repo_root / "library.yaml").write_text(
        """
capability_library_version: v1
entries:
  - kind: tool_bundle
    id: default_tools
    description: base tools
    tools: [read, bash]
""".strip()
        + "\n",
        encoding="utf-8",
    )
    fragment_dir = repo_root / "library.d"
    fragment_dir.mkdir()
    (fragment_dir / "10-reader.yaml").write_text(
        """
entries:
  - kind: subagent_profile
    id: reader
    description: reader
    tool_bundles: [default_tools]
    transports: [rpc]
    allow_network: false
    allow_write: false
    read_scopes: [.]
    write_scopes: []
    budgets:
      max_spawn_count: 1
      max_tokens: 1000
      max_runtime_seconds: 60
    expected_artifacts: []
""".strip()
        + "\n",
        encoding="utf-8",
    )

    library = load_capability_library(repo_root=repo_root)

    assert library["relative_path"] == "library.yaml"
    assert library["fragment_paths"] == ["library.yaml", "library.d/10-reader.yaml"]
    assert set(library["tool_bundles"]) == {"default_tools"}
    assert set(library["subagent_profiles"]) == {"reader"}

    (fragment_dir / "20-duplicate.yaml").write_text(
        """
entries:
  - kind: tool_bundle
    id: default_tools
    description: dupe
    tools: [read]
""".strip()
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate capability id: default_tools"):
        load_capability_library(repo_root=repo_root)


def test_load_capability_library_rejects_choreography_fields(tmp_path: pathlib.Path) -> None:
    repo_root = tmp_path / "starter"
    repo_root.mkdir()
    (repo_root / "library.yaml").write_text(
        """
capability_library_version: v1
entries:
  - kind: tool_bundle
    id: default_tools
    description: base tools
    tools: [read]
  - kind: subagent_profile
    id: reader
    description: reader
    tool_bundles: [default_tools]
    transports: [rpc]
    allow_network: false
    allow_write: false
    read_scopes: [.]
    write_scopes: []
    budgets:
      max_spawn_count: 1
      max_tokens: 1000
      max_runtime_seconds: 60
    expected_artifacts: []
    workflow: review-then-patch
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="forbidden choreography field: workflow"):
        load_capability_library(repo_root=repo_root)


def test_validate_subagent_usage_enforces_profiles_tools_budgets_and_scopes(
    tmp_path: pathlib.Path,
) -> None:
    repo_root = tmp_path / "starter"
    repo_root.mkdir()
    manifest = build_capability_manifest(
        library={
            "relative_path": "library.yaml",
            "fingerprint": "abc123",
            "tool_bundles": {
                "default_tools": {
                    "kind": "tool_bundle",
                    "id": "default_tools",
                    "description": "base",
                    "tools": ["read", "bash"],
                }
            },
            "subagent_profiles": {
                "reader": {
                    "kind": "subagent_profile",
                    "id": "reader",
                    "description": "reader",
                    "tool_bundles": ["default_tools"],
                    "transports": ["rpc"],
                    "allow_network": False,
                    "allow_write": False,
                    "read_scopes": ["starter/**"],
                    "write_scopes": [],
                    "budgets": {
                        "max_spawn_count": 1,
                        "max_tokens": 1000,
                        "max_runtime_seconds": 60,
                    },
                    "expected_artifacts": [],
                }
            },
        },
        transport_mode="rpc",
        capabilities={
            "enabled": True,
            "subagents": {
                "allowed": True,
                "max_agents": 1,
                "allowed_profiles": ["reader"],
            },
        },
    )

    valid_usage = {
        "usage_version": "v1",
        "spawned_agents": [
            {
                "agent_id": "reader-1",
                "profile_id": "reader",
                "tool_calls": ["read"],
                "read_paths": ["starter/README.md"],
                "write_paths": [],
                "network_access": False,
                "prompt_tokens": 120,
                "runtime_seconds": 10,
            }
        ],
    }
    valid = validate_subagent_usage(valid_usage, manifest, repo_root=repo_root.parent)
    assert valid["valid"] is True
    assert valid["spawned_profile_ids"] == ["reader"]

    invalid_usage = {
        "usage_version": "v1",
        "spawned_agents": [
            {
                "agent_id": "reader-1",
                "profile_id": "reader",
                "tool_calls": ["write"],
                "read_paths": ["../outside.txt"],
                "write_paths": ["starter/result.json"],
                "network_access": True,
                "prompt_tokens": 5000,
                "runtime_seconds": 90,
            },
            {
                "agent_id": "reader-2",
                "profile_id": "reader",
                "tool_calls": ["read"],
                "read_paths": ["starter/README.md"],
                "write_paths": [],
                "network_access": False,
                "prompt_tokens": 10,
                "runtime_seconds": 1,
            },
            {
                "agent_id": "reader-3",
                "profile_id": "unknown",
                "tool_calls": ["read"],
                "read_paths": ["starter/README.md"],
                "write_paths": [],
                "network_access": False,
                "prompt_tokens": 10,
                "runtime_seconds": 1,
            },
        ],
    }
    invalid = validate_subagent_usage(invalid_usage, manifest, repo_root=repo_root.parent)
    assert invalid["valid"] is False
    assert "subagents.agent_count_exceeded" in invalid["violations"]
    assert "subagents.tool_not_allowed:reader:write" in invalid["violations"]
    assert "subagents.network_not_allowed:reader" in invalid["violations"]
    assert any(
        item.startswith("subagents.read_scope_violation:reader:")
        for item in invalid["violations"]
    )
    assert "subagents.write_not_allowed:reader" in invalid["violations"]
    assert "subagents.token_budget_exceeded:reader" in invalid["violations"]
    assert "subagents.runtime_budget_exceeded:reader" in invalid["violations"]
    assert "subagents.profile_not_allowed:unknown" in invalid["violations"]
    assert "subagents.spawn_budget_exceeded:reader" in invalid["violations"]


def test_evaluate_intercepted_subagent_action_enforces_live_rules(
    tmp_path: pathlib.Path,
) -> None:
    manifest = build_capability_manifest(
        library={
            "relative_path": "library.yaml",
            "fingerprint": "abc123",
            "tool_bundles": {
                "default_tools": {
                    "kind": "tool_bundle",
                    "id": "default_tools",
                    "description": "base",
                    "tools": ["read", "write"],
                }
            },
            "subagent_profiles": {
                "reader": {
                    "kind": "subagent_profile",
                    "id": "reader",
                    "description": "reader",
                    "tool_bundles": ["default_tools"],
                    "transports": ["managed_rpc"],
                    "allow_network": False,
                    "allow_write": False,
                    "read_scopes": ["starter/**"],
                    "write_scopes": [],
                    "budgets": {
                        "max_spawn_count": 1,
                        "max_tokens": 1000,
                        "max_runtime_seconds": 60,
                    },
                    "expected_artifacts": [],
                }
            },
        },
        transport_mode="managed_rpc",
        capabilities={
            "enabled": True,
            "subagents": {
                "allowed": True,
                "max_agents": 1,
                "allowed_profiles": ["reader"],
            },
        },
    )

    state: dict[str, object] = {}
    spawn = evaluate_intercepted_subagent_action(
        {
            "action": "spawn",
            "request_id": "spawn-1",
            "agent_id": "reader-1",
            "profile_id": "reader",
            "prompt_tokens": 120,
        },
        manifest,
        repo_root=tmp_path,
        state=state,
    )
    assert spawn["allowed"] is True
    state = dict(spawn["state"])

    allowed_read = evaluate_intercepted_subagent_action(
        {
            "action": "tool",
            "request_id": "tool-1",
            "agent_id": "reader-1",
            "profile_id": "reader",
            "tool": "read",
            "read_paths": ["starter/README.md"],
            "write_paths": [],
            "network_access": False,
            "runtime_seconds": 2,
        },
        manifest,
        repo_root=tmp_path,
        state=state,
    )
    assert allowed_read["allowed"] is True
    state = dict(allowed_read["state"])

    denied_write = evaluate_intercepted_subagent_action(
        {
            "action": "tool",
            "request_id": "tool-2",
            "agent_id": "reader-1",
            "profile_id": "reader",
            "tool": "write",
            "read_paths": [],
            "write_paths": ["starter/README.md"],
            "network_access": True,
            "runtime_seconds": 2,
        },
        manifest,
        repo_root=tmp_path,
        state=state,
    )
    assert denied_write["allowed"] is False
    assert "subagents.write_not_allowed:reader" in denied_write["violations"]
    assert "subagents.network_not_allowed:reader" in denied_write["violations"]


def test_summarize_interception_log_reports_denials() -> None:
    summary = summarize_interception_log(
        [
            {
                "action": "spawn",
                "agent_id": "reader-1",
                "profile_id": "reader",
                "decision": "allow",
                "prompt_tokens": 100,
            },
            {
                "action": "tool",
                "agent_id": "reader-1",
                "profile_id": "reader",
                "decision": "allow",
                "runtime_seconds": 3,
            },
            {
                "action": "tool",
                "agent_id": "reader-1",
                "profile_id": "reader",
                "decision": "deny",
                "violations": ["subagents.write_not_allowed:reader"],
            },
        ]
    )

    assert summary["usage_present"] is True
    assert summary["usage_valid"] is False
    assert summary["allowed_action_count"] == 2
    assert summary["denied_action_count"] == 1
    assert summary["agent_count"] == 1
    assert summary["total_prompt_tokens"] == 100
    assert summary["total_runtime_seconds"] == 3.0
    assert summary["first_denial"]["violations"] == ["subagents.write_not_allowed:reader"]
