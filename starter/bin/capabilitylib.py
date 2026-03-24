#!/usr/bin/env python3
from __future__ import annotations

import fnmatch
import hashlib
import json
import pathlib
from datetime import UTC, datetime
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover - exercised via runtime environments
    yaml = None


CAPABILITY_LIBRARY_VERSION = "v1"
CAPABILITY_MANIFEST_VERSION = "v1"
SUBAGENT_USAGE_VERSION = "v1"
INTERCEPTION_LOG_VERSION = "v1"
DEFAULT_LIBRARY_PATH = "library.yaml"
DEFAULT_LIBRARY_DIR = "library.d"
DEFAULT_MANIFEST_PATH = "context/capability-manifest.json"
DEFAULT_USAGE_PATH = "outputs/subagent-usage.json"
DEFAULT_INTERCEPTION_ACTION_LOG_PATH = "outputs/subagent-action-log.jsonl"
SUPPORTED_TRANSPORTS = {"cli_json", "rpc", "managed_rpc"}
SUPPORTED_TOOLS = {"read", "write", "edit", "bash"}
ENTRY_KINDS = {"tool_bundle", "subagent_profile"}
CHOREOGRAPHY_KEYS = {
    "auto_spawn",
    "fallback_profile",
    "handoff",
    "handoffs",
    "next_profile",
    "pipeline",
    "pipelines",
    "route",
    "routes",
    "routing",
    "sequence",
    "task_routing",
    "task_types",
    "workflow",
    "workflows",
}

DEFAULT_CAPABILITIES_CONFIG: dict[str, Any] = {
    "enabled": False,
    "library_path": DEFAULT_LIBRARY_PATH,
    "manifest_path": DEFAULT_MANIFEST_PATH,
    "subagents": {
        "allowed": False,
        "max_agents": 0,
        "allowed_profiles": [],
    },
}


def script_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def write_json(path: pathlib.Path, payload: Any, *, sort_keys: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=sort_keys, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _coerce_path(path: str | pathlib.Path, *, repo_root: pathlib.Path | None) -> pathlib.Path:
    candidate = pathlib.Path(path).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return ((repo_root or script_root()) / candidate).resolve()


def resolve_library_path(
    library_path: str | pathlib.Path | None = None,
    *,
    repo_root: pathlib.Path | None = None,
) -> pathlib.Path:
    candidate = library_path or DEFAULT_LIBRARY_PATH
    return _coerce_path(candidate, repo_root=repo_root)


def _require_yaml() -> None:
    if yaml is None:
        raise ValueError("PyYAML is required to load capability libraries")


def _load_yaml(path: pathlib.Path) -> Any:
    _require_yaml()
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {} if payload is None else payload


def _entry_allowed_keys(kind: str) -> set[str]:
    if kind == "tool_bundle":
        return {"kind", "id", "description", "tools"}
    if kind == "subagent_profile":
        return {
            "kind",
            "id",
            "description",
            "tool_bundles",
            "transports",
            "allow_network",
            "allow_write",
            "read_scopes",
            "write_scopes",
            "budgets",
            "expected_artifacts",
        }
    return {"kind", "id"}


def _list_of_strings(value: Any, *, field: str, errors: list[str]) -> list[str]:
    if not isinstance(value, list):
        errors.append(f"{field} must be an array")
        return []
    items: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            errors.append(f"{field}[{index}] must be a non-empty string")
            continue
        items.append(item.strip())
    return items


def _validate_budget_payload(
    payload: Any,
    *,
    field_prefix: str,
    errors: list[str],
) -> dict[str, int]:
    normalized = {
        "max_spawn_count": 0,
        "max_tokens": 0,
        "max_runtime_seconds": 0,
    }
    if not isinstance(payload, dict):
        errors.append(f"{field_prefix} must be an object")
        return normalized
    for key in normalized:
        value = payload.get(key)
        if not isinstance(value, int) or value < 0:
            errors.append(f"{field_prefix}.{key} must be a non-negative integer")
            continue
        normalized[key] = value
    return normalized


def _validate_entry(entry: Any, *, index: int, errors: list[str]) -> dict[str, Any] | None:
    if not isinstance(entry, dict):
        errors.append(f"entries[{index}] must be an object")
        return None
    kind = str(entry.get("kind", "")).strip()
    entry_id = str(entry.get("id", "")).strip()
    if kind not in ENTRY_KINDS:
        errors.append(f"entries[{index}].kind must be one of: tool_bundle, subagent_profile")
        return None
    if not entry_id:
        errors.append(f"entries[{index}].id must be a non-empty string")
        return None
    for key in CHOREOGRAPHY_KEYS:
        if key in entry:
            errors.append(f"entries[{index}] includes forbidden choreography field: {key}")
    unknown = sorted(set(entry) - _entry_allowed_keys(kind))
    for key in unknown:
        errors.append(f"entries[{index}] contains unsupported field for {kind}: {key}")
    description = entry.get("description")
    if not isinstance(description, str) or not description.strip():
        errors.append(f"entries[{index}].description must be a non-empty string")
    if kind == "tool_bundle":
        tools = _list_of_strings(entry.get("tools"), field=f"entries[{index}].tools", errors=errors)
        for tool in tools:
            if tool not in SUPPORTED_TOOLS:
                errors.append(
                    f"entries[{index}].tools contains unsupported tool: {tool}"
                )
        return {
            "kind": kind,
            "id": entry_id,
            "description": str(description or "").strip(),
            "tools": sorted(dict.fromkeys(tools)),
        }

    tool_bundles = _list_of_strings(
        entry.get("tool_bundles"),
        field=f"entries[{index}].tool_bundles",
        errors=errors,
    )
    transports = _list_of_strings(
        entry.get("transports"),
        field=f"entries[{index}].transports",
        errors=errors,
    )
    for transport in transports:
        if transport not in SUPPORTED_TRANSPORTS:
            errors.append(
                f"entries[{index}].transports contains unsupported transport: {transport}"
            )
    read_scopes = _list_of_strings(
        entry.get("read_scopes"),
        field=f"entries[{index}].read_scopes",
        errors=errors,
    )
    write_scopes = _list_of_strings(
        entry.get("write_scopes"),
        field=f"entries[{index}].write_scopes",
        errors=errors,
    )
    expected_artifacts = _list_of_strings(
        entry.get("expected_artifacts"),
        field=f"entries[{index}].expected_artifacts",
        errors=errors,
    )
    allow_network = entry.get("allow_network")
    allow_write = entry.get("allow_write")
    if not isinstance(allow_network, bool):
        errors.append(f"entries[{index}].allow_network must be a boolean")
    if not isinstance(allow_write, bool):
        errors.append(f"entries[{index}].allow_write must be a boolean")
    budgets = _validate_budget_payload(
        entry.get("budgets"),
        field_prefix=f"entries[{index}].budgets",
        errors=errors,
    )
    if budgets["max_spawn_count"] < 1:
        errors.append(f"entries[{index}].budgets.max_spawn_count must be at least 1")
    if not tool_bundles:
        errors.append(f"entries[{index}].tool_bundles must contain at least one id")
    if not transports:
        errors.append(f"entries[{index}].transports must contain at least one transport")
    if not read_scopes:
        errors.append(f"entries[{index}].read_scopes must contain at least one scope")
    if allow_write is False and write_scopes:
        errors.append(f"entries[{index}].write_scopes must be empty when allow_write is false")
    if allow_write is True and not write_scopes:
        errors.append(f"entries[{index}].write_scopes must contain at least one scope")
    return {
        "kind": kind,
        "id": entry_id,
        "description": str(description or "").strip(),
        "tool_bundles": sorted(dict.fromkeys(tool_bundles)),
        "transports": sorted(dict.fromkeys(transports)),
        "allow_network": bool(allow_network),
        "allow_write": bool(allow_write),
        "read_scopes": sorted(dict.fromkeys(read_scopes)),
        "write_scopes": sorted(dict.fromkeys(write_scopes)),
        "budgets": budgets,
        "expected_artifacts": sorted(dict.fromkeys(expected_artifacts)),
    }


def validate_capability_library(payload: Any) -> list[str]:
    errors: list[str] = []
    if not isinstance(payload, dict):
        return ["capability library must be a JSON/YAML object"]
    if payload.get("capability_library_version") != CAPABILITY_LIBRARY_VERSION:
        errors.append("capability_library_version must be v1")
    entries = payload.get("entries")
    if not isinstance(entries, list):
        errors.append("entries must be an array")
        return errors
    normalized_entries: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, entry in enumerate(entries):
        normalized = _validate_entry(entry, index=index, errors=errors)
        if normalized is None:
            continue
        if normalized["id"] in seen_ids:
            errors.append(f"duplicate capability id: {normalized['id']}")
            continue
        seen_ids.add(normalized["id"])
        normalized_entries.append(normalized)
    tool_bundle_ids = {
        entry["id"] for entry in normalized_entries if entry["kind"] == "tool_bundle"
    }
    for entry in normalized_entries:
        if entry["kind"] != "subagent_profile":
            continue
        for bundle_id in entry["tool_bundles"]:
            if bundle_id not in tool_bundle_ids:
                errors.append(
                    f"subagent_profile {entry['id']} references unknown tool_bundle: {bundle_id}"
                )
    return errors


def _normalize_fragment(
    payload: Any,
    *,
    path: pathlib.Path,
    require_version: bool,
) -> dict[str, Any]:
    if isinstance(payload, list):
        payload = {"entries": payload}
    if not isinstance(payload, dict):
        raise ValueError(f"capability library fragment must be an object: {path}")
    normalized = dict(payload)
    if (
        require_version
        and normalized.get("capability_library_version") != CAPABILITY_LIBRARY_VERSION
    ):
        raise ValueError(f"{path} must declare capability_library_version: v1")
    if "entries" not in normalized:
        raise ValueError(f"{path} must contain entries")
    return normalized


def load_capability_library(
    library_path: str | pathlib.Path | None = None,
    *,
    repo_root: pathlib.Path | None = None,
) -> dict[str, Any]:
    root_path = resolve_library_path(library_path, repo_root=repo_root)
    if not root_path.exists():
        raise ValueError(f"capability library not found: {root_path}")
    merged_entries: list[dict[str, Any]] = []
    seen_ids: dict[str, pathlib.Path] = {}
    fragment_paths: list[pathlib.Path] = [root_path]
    root_payload = _normalize_fragment(_load_yaml(root_path), path=root_path, require_version=True)
    fragment_dir = root_path.parent / DEFAULT_LIBRARY_DIR
    for fragment in sorted(fragment_dir.glob("*.yml")) + sorted(fragment_dir.glob("*.yaml")):
        fragment_paths.append(fragment)
    for index, path in enumerate(fragment_paths):
        payload = root_payload if index == 0 else _normalize_fragment(
            _load_yaml(path),
            path=path,
            require_version=False,
        )
        entries = payload.get("entries", [])
        if not isinstance(entries, list):
            raise ValueError(f"{path} entries must be an array")
        for item in entries:
            if not isinstance(item, dict):
                raise ValueError(f"{path} contains a non-object entry")
            entry_id = str(item.get("id", "")).strip()
            if entry_id and entry_id in seen_ids:
                raise ValueError(f"duplicate capability id: {entry_id}")
            if entry_id:
                seen_ids[entry_id] = path
            merged_entries.append(dict(item))
    combined = {
        "capability_library_version": CAPABILITY_LIBRARY_VERSION,
        "entries": merged_entries,
    }
    errors = validate_capability_library(combined)
    if errors:
        raise ValueError("; ".join(errors))
    normalized_entries = [
        _validate_entry(entry, index=index, errors=[])
        for index, entry in enumerate(merged_entries)
    ]
    sanitized_entries = [entry for entry in normalized_entries if entry is not None]
    serialized = json.dumps(
        {
            "capability_library_version": CAPABILITY_LIBRARY_VERSION,
            "entries": sanitized_entries,
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    resolved_root = repo_root or script_root()
    if root_path.is_relative_to(resolved_root):
        relative_root = str(root_path.relative_to(resolved_root))
    else:
        relative_root = str(root_path)
    return {
        "capability_library_version": CAPABILITY_LIBRARY_VERSION,
        "entries": sanitized_entries,
        "path": str(root_path),
        "relative_path": relative_root,
        "fragment_paths": [
            (
                str(path.relative_to(resolved_root))
                if path.is_relative_to(resolved_root)
                else str(path)
            )
            for path in fragment_paths
        ],
        "fingerprint": sha256_text(serialized),
        "tool_bundles": {
            entry["id"]: dict(entry)
            for entry in sanitized_entries
            if entry["kind"] == "tool_bundle"
        },
        "subagent_profiles": {
            entry["id"]: dict(entry)
            for entry in sanitized_entries
            if entry["kind"] == "subagent_profile"
        },
    }


def build_capability_manifest(
    *,
    library: dict[str, Any],
    transport_mode: str,
    capabilities: dict[str, Any],
) -> dict[str, Any]:
    subagents = dict(DEFAULT_CAPABILITIES_CONFIG["subagents"])
    subagents.update(capabilities.get("subagents", {}))
    allowed_profiles = [
        profile_id
        for profile_id in list(subagents.get("allowed_profiles", []))
        if profile_id in library.get("subagent_profiles", {})
    ]
    tool_bundle_ids: set[str] = set()
    profiles: list[dict[str, Any]] = []
    for profile_id in allowed_profiles:
        profile = dict(library["subagent_profiles"][profile_id])
        profiles.append(profile)
        tool_bundle_ids.update(profile.get("tool_bundles", []))
    tool_bundles = [
        dict(library["tool_bundles"][bundle_id])
        for bundle_id in sorted(tool_bundle_ids)
        if bundle_id in library.get("tool_bundles", {})
    ]
    return {
        "capability_manifest_version": CAPABILITY_MANIFEST_VERSION,
        "generated_at": now_utc(),
        "library_path": library.get("relative_path") or library.get("path"),
        "library_fingerprint": library.get("fingerprint"),
        "library_fragment_paths": list(library.get("fragment_paths", [])),
        "transport": {"mode": transport_mode},
        "capabilities_enabled": bool(capabilities.get("enabled", False)),
        "subagents": {
            "allowed": bool(subagents.get("allowed", False)),
            "max_agents": int(subagents.get("max_agents", 0)),
            "allowed_profiles": allowed_profiles,
            "usage_path": DEFAULT_USAGE_PATH,
        },
        "tool_bundles": tool_bundles,
        "subagent_profiles": profiles,
    }


def _normalize_repo_relative(path_value: Any, *, repo_root: pathlib.Path) -> str | None:
    if not isinstance(path_value, str) or not path_value.strip():
        return None
    candidate = pathlib.Path(path_value.strip())
    if candidate.is_absolute():
        try:
            return candidate.resolve().relative_to(repo_root.resolve()).as_posix()
        except Exception:
            return None
    try:
        return (repo_root / candidate).resolve().relative_to(repo_root.resolve()).as_posix()
    except Exception:
        return None


def _scope_matches(path_value: str, scopes: list[str]) -> bool:
    if not scopes:
        return False
    for raw_scope in scopes:
        scope = raw_scope.strip()
        if scope in {"", ".", "./", "*", "**"}:
            return True
        normalized = scope.lstrip("./")
        if fnmatch.fnmatch(path_value, normalized):
            return True
        if fnmatch.fnmatch(path_value, f"{normalized}/**"):
            return True
        if path_value == normalized or path_value.startswith(f"{normalized}/"):
            return True
    return False


def _manifest_profiles(manifest_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(item.get("id")): item
        for item in manifest_payload.get("subagent_profiles", [])
        if isinstance(item, dict) and item.get("id")
    }


def _manifest_allowed_tools(manifest_payload: dict[str, Any], profile: dict[str, Any]) -> set[str]:
    allowed_tools: set[str] = set()
    for bundle_id in profile.get("tool_bundles", []):
        for bundle in manifest_payload.get("tool_bundles", []):
            if isinstance(bundle, dict) and bundle.get("id") == bundle_id:
                allowed_tools.update(bundle.get("tools", []))
    return allowed_tools


def initialize_interception_state() -> dict[str, Any]:
    return {
        "active_agents": {},
        "profile_spawn_counts": {},
        "allowed_action_count": 0,
        "denied_action_count": 0,
        "first_denial": None,
    }


def evaluate_intercepted_subagent_action(
    request_payload: dict[str, Any],
    manifest_payload: dict[str, Any] | None,
    *,
    repo_root: pathlib.Path,
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest_payload = manifest_payload or {}
    state = state if isinstance(state, dict) else initialize_interception_state()
    profiles = _manifest_profiles(manifest_payload)
    allowed_profiles = set((manifest_payload.get("subagents") or {}).get("allowed_profiles", []))
    max_agents = int((manifest_payload.get("subagents") or {}).get("max_agents", 0) or 0)
    action = str(request_payload.get("action", "")).strip()
    request_id = str(request_payload.get("request_id", "")).strip()
    agent_id = str(request_payload.get("agent_id", "")).strip()
    profile_id = str(request_payload.get("profile_id", "")).strip()
    violations: list[str] = []
    normalized_read_paths: list[str] = []
    normalized_write_paths: list[str] = []

    if action not in {"spawn", "tool"}:
        violations.append("subagents.action_invalid")
    if not agent_id:
        violations.append("subagents.agent_record_invalid")
    if not profile_id:
        violations.append("subagents.profile_id_missing")
    if profile_id and (profile_id not in allowed_profiles or profile_id not in profiles):
        violations.append(f"subagents.profile_not_allowed:{profile_id}")

    profile = profiles.get(profile_id, {})
    budgets = dict(profile.get("budgets", {})) if isinstance(profile, dict) else {}
    allowed_tools = (
        _manifest_allowed_tools(manifest_payload, profile) if isinstance(profile, dict) else set()
    )
    active_agents = state.setdefault("active_agents", {})
    profile_spawn_counts = state.setdefault("profile_spawn_counts", {})

    if action == "spawn" and profile_id and profile_id in profiles and agent_id:
        prompt_tokens = request_payload.get("prompt_tokens", 0)
        if not isinstance(prompt_tokens, int) or prompt_tokens < 0:
            violations.append(f"subagents.prompt_tokens_invalid:{profile_id}")
        elif budgets.get("max_tokens", 0) and prompt_tokens > budgets["max_tokens"]:
            violations.append(f"subagents.token_budget_exceeded:{profile_id}")
        if max_agents >= 0 and len(active_agents) >= max_agents and agent_id not in active_agents:
            violations.append("subagents.agent_count_exceeded")
        next_count = int(profile_spawn_counts.get(profile_id, 0)) + (
            0 if agent_id in active_agents else 1
        )
        max_spawn_count = int(budgets.get("max_spawn_count", 0) or 0)
        if max_spawn_count and next_count > max_spawn_count:
            violations.append(f"subagents.spawn_budget_exceeded:{profile_id}")
    elif action == "tool" and profile_id and profile_id in profiles:
        if agent_id not in active_agents:
            violations.append("subagents.agent_record_invalid")
        tool = str(request_payload.get("tool", "")).strip()
        if tool not in allowed_tools:
            violations.append(f"subagents.tool_not_allowed:{profile_id}:{tool}")
        if bool(request_payload.get("network_access")) and not bool(
            profile.get("allow_network", False)
        ):
            violations.append(f"subagents.network_not_allowed:{profile_id}")
        read_paths = _list_of_strings(request_payload.get("read_paths", []), field="read_paths", errors=[])
        write_paths = _list_of_strings(
            request_payload.get("write_paths", []), field="write_paths", errors=[]
        )
        for path_value in read_paths:
            normalized = _normalize_repo_relative(path_value, repo_root=repo_root)
            if normalized is None or not _scope_matches(
                normalized,
                list(profile.get("read_scopes", [])),
            ):
                violations.append(f"subagents.read_scope_violation:{profile_id}:{path_value}")
            elif normalized:
                normalized_read_paths.append(normalized)
        if write_paths and not bool(profile.get("allow_write", False)):
            violations.append(f"subagents.write_not_allowed:{profile_id}")
        for path_value in write_paths:
            normalized = _normalize_repo_relative(path_value, repo_root=repo_root)
            if normalized is None or not _scope_matches(
                normalized, list(profile.get("write_scopes", []))
            ):
                violations.append(f"subagents.write_scope_violation:{profile_id}:{path_value}")
            elif normalized:
                normalized_write_paths.append(normalized)
        runtime_seconds = request_payload.get("runtime_seconds")
        if runtime_seconds is not None:
            if not isinstance(runtime_seconds, int | float) or float(runtime_seconds) < 0:
                violations.append(f"subagents.runtime_invalid:{profile_id}")
            elif budgets.get("max_runtime_seconds", 0) and float(runtime_seconds) > float(
                budgets["max_runtime_seconds"]
            ):
                violations.append(f"subagents.runtime_budget_exceeded:{profile_id}")

    allowed = len(violations) == 0
    if allowed:
        if action == "spawn" and agent_id and profile_id:
            active_agents[agent_id] = {
                "profile_id": profile_id,
                "prompt_tokens": int(request_payload.get("prompt_tokens", 0) or 0),
            }
            profile_spawn_counts[profile_id] = int(profile_spawn_counts.get(profile_id, 0)) + 1
        state["allowed_action_count"] = int(state.get("allowed_action_count", 0)) + 1
    else:
        state["denied_action_count"] = int(state.get("denied_action_count", 0)) + 1
        if state.get("first_denial") is None:
            state["first_denial"] = {
                "request_id": request_id or None,
                "agent_id": agent_id or None,
                "profile_id": profile_id or None,
                "action": action or None,
                "violations": list(violations),
            }

    return {
        "log_version": INTERCEPTION_LOG_VERSION,
        "request_id": request_id or None,
        "action": action or None,
        "agent_id": agent_id or None,
        "profile_id": profile_id or None,
        "tool": str(request_payload.get("tool", "")).strip() or None,
        "decision": "allow" if allowed else "deny",
        "allowed": allowed,
        "violations": list(violations),
        "normalized_read_paths": normalized_read_paths,
        "normalized_write_paths": normalized_write_paths,
        "network_access": bool(request_payload.get("network_access", False)),
        "prompt_tokens": request_payload.get("prompt_tokens"),
        "runtime_seconds": request_payload.get("runtime_seconds"),
        "state": state,
    }


def summarize_interception_log(
    log_payload: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    entries = [item for item in (log_payload or []) if isinstance(item, dict)]
    spawned_profile_ids: list[str] = []
    denied_violations: list[str] = []
    prompt_tokens = 0
    runtime_by_agent: dict[str, float] = {}
    first_denial = None
    allowed_action_count = 0
    denied_action_count = 0
    spawned_agents: set[str] = set()
    for entry in entries:
        decision = str(entry.get("decision", "")).strip()
        if decision == "allow":
            allowed_action_count += 1
        elif decision == "deny":
            denied_action_count += 1
            for violation in entry.get("violations", []):
                if isinstance(violation, str):
                    denied_violations.append(violation)
            if first_denial is None:
                first_denial = {
                    "request_id": entry.get("request_id"),
                    "agent_id": entry.get("agent_id"),
                    "profile_id": entry.get("profile_id"),
                    "action": entry.get("action"),
                    "violations": list(entry.get("violations", [])),
                }
        action = str(entry.get("action", "")).strip()
        if action == "spawn" and decision == "allow":
            profile_id = str(entry.get("profile_id", "")).strip()
            agent_id = str(entry.get("agent_id", "")).strip()
            if profile_id:
                spawned_profile_ids.append(profile_id)
            if agent_id:
                spawned_agents.add(agent_id)
            prompt_value = entry.get("prompt_tokens", 0)
            if isinstance(prompt_value, int) and prompt_value >= 0:
                prompt_tokens += prompt_value
        runtime_value = entry.get("runtime_seconds")
        agent_id = str(entry.get("agent_id", "")).strip()
        if agent_id and isinstance(runtime_value, int | float) and float(runtime_value) >= 0:
            runtime_by_agent[agent_id] = max(runtime_by_agent.get(agent_id, 0.0), float(runtime_value))
    return {
        "usage_present": bool(entries),
        "usage_version": INTERCEPTION_LOG_VERSION,
        "usage_valid": denied_action_count == 0,
        "violations": sorted(dict.fromkeys(denied_violations)),
        "agent_count": len(spawned_agents),
        "spawned_profile_ids": sorted(dict.fromkeys(spawned_profile_ids)),
        "total_prompt_tokens": prompt_tokens,
        "total_runtime_seconds": round(sum(runtime_by_agent.values()), 3),
        "allowed_action_count": allowed_action_count,
        "denied_action_count": denied_action_count,
        "first_denial": first_denial,
    }


def validate_subagent_usage(
    usage_payload: dict[str, Any] | None,
    manifest_payload: dict[str, Any] | None,
    *,
    repo_root: pathlib.Path,
) -> dict[str, Any]:
    manifest_payload = manifest_payload or {}
    profiles = {
        item.get("id"): item
        for item in manifest_payload.get("subagent_profiles", [])
        if isinstance(item, dict) and item.get("id")
    }
    allowed_profiles = set((manifest_payload.get("subagents") or {}).get("allowed_profiles", []))
    max_agents = int((manifest_payload.get("subagents") or {}).get("max_agents", 0) or 0)
    result = {
        "usage_present": usage_payload is not None,
        "usage_version": None,
        "valid": True,
        "violations": [],
        "agent_count": 0,
        "spawned_profile_ids": [],
        "total_prompt_tokens": 0,
        "total_runtime_seconds": 0.0,
    }
    if usage_payload is None:
        return result
    if not isinstance(usage_payload, dict):
        result["valid"] = False
        result["violations"].append("subagents.usage_payload_invalid")
        return result
    result["usage_version"] = usage_payload.get("usage_version")
    if usage_payload.get("usage_version") != SUBAGENT_USAGE_VERSION:
        result["violations"].append("subagents.usage_version_invalid")
    spawned_agents = usage_payload.get("spawned_agents", [])
    if not isinstance(spawned_agents, list):
        result["violations"].append("subagents.spawned_agents_invalid")
        result["valid"] = False
        return result
    result["agent_count"] = len(spawned_agents)
    if max_agents >= 0 and len(spawned_agents) > max_agents:
        result["violations"].append("subagents.agent_count_exceeded")
    profile_counts: dict[str, int] = {}
    spawned_profile_ids: list[str] = []
    total_tokens = 0
    total_runtime = 0.0
    for agent in spawned_agents:
        if not isinstance(agent, dict):
            result["violations"].append("subagents.agent_record_invalid")
            continue
        profile_id = str(agent.get("profile_id", "")).strip()
        if not profile_id:
            result["violations"].append("subagents.profile_id_missing")
            continue
        spawned_profile_ids.append(profile_id)
        profile_counts[profile_id] = profile_counts.get(profile_id, 0) + 1
        if profile_id not in allowed_profiles or profile_id not in profiles:
            result["violations"].append(f"subagents.profile_not_allowed:{profile_id}")
            continue
        profile = profiles[profile_id]
        budgets = profile.get("budgets", {})
        tools = _list_of_strings(agent.get("tool_calls", []), field="tool_calls", errors=[])
        allowed_tools: set[str] = set()
        for bundle_id in profile.get("tool_bundles", []):
            for bundle in manifest_payload.get("tool_bundles", []):
                if isinstance(bundle, dict) and bundle.get("id") == bundle_id:
                    allowed_tools.update(bundle.get("tools", []))
        for tool in tools:
            if tool not in allowed_tools:
                result["violations"].append(f"subagents.tool_not_allowed:{profile_id}:{tool}")
        if bool(agent.get("network_access")) and not bool(profile.get("allow_network", False)):
            result["violations"].append(f"subagents.network_not_allowed:{profile_id}")
        read_paths = _list_of_strings(agent.get("read_paths", []), field="read_paths", errors=[])
        write_paths = _list_of_strings(agent.get("write_paths", []), field="write_paths", errors=[])
        for path_value in read_paths:
            normalized = _normalize_repo_relative(path_value, repo_root=repo_root)
            if normalized is None or not _scope_matches(
                normalized,
                list(profile.get("read_scopes", [])),
            ):
                result["violations"].append(f"subagents.read_scope_violation:{profile_id}:{path_value}")
        if write_paths and not bool(profile.get("allow_write", False)):
            result["violations"].append(f"subagents.write_not_allowed:{profile_id}")
        for path_value in write_paths:
            normalized = _normalize_repo_relative(path_value, repo_root=repo_root)
            if normalized is None or not _scope_matches(
                normalized, list(profile.get("write_scopes", []))
            ):
                result["violations"].append(
                    f"subagents.write_scope_violation:{profile_id}:{path_value}"
                )
        prompt_tokens = agent.get("prompt_tokens", 0)
        if not isinstance(prompt_tokens, int) or prompt_tokens < 0:
            result["violations"].append(f"subagents.prompt_tokens_invalid:{profile_id}")
        else:
            total_tokens += prompt_tokens
            if budgets.get("max_tokens", 0) and prompt_tokens > budgets["max_tokens"]:
                result["violations"].append(f"subagents.token_budget_exceeded:{profile_id}")
        runtime_seconds = agent.get("runtime_seconds", 0)
        if not isinstance(runtime_seconds, int | float) or float(runtime_seconds) < 0:
            result["violations"].append(f"subagents.runtime_invalid:{profile_id}")
        else:
            total_runtime += float(runtime_seconds)
            if budgets.get("max_runtime_seconds", 0) and float(runtime_seconds) > float(
                budgets["max_runtime_seconds"]
            ):
                result["violations"].append(f"subagents.runtime_budget_exceeded:{profile_id}")
    for profile_id, count in profile_counts.items():
        profile = profiles.get(profile_id)
        if not isinstance(profile, dict):
            continue
        max_spawn_count = int((profile.get("budgets") or {}).get("max_spawn_count", 0) or 0)
        if max_spawn_count and count > max_spawn_count:
            result["violations"].append(f"subagents.spawn_budget_exceeded:{profile_id}")
    result["spawned_profile_ids"] = sorted(dict.fromkeys(spawned_profile_ids))
    result["total_prompt_tokens"] = total_tokens
    result["total_runtime_seconds"] = round(total_runtime, 3)
    result["valid"] = len(result["violations"]) == 0
    return result
