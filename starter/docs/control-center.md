# Bitterless Command Center

The Bitterless command center is a local-only Textual TUI for operating multiple
file-native Bitterless harness repos from one terminal.

## Startup

Install the operator dependency set:

```bash
starter/bin/setup-dev-env.sh
```

Create a config file from the example:

```bash
cp starter/control-center.example.toml ~/.config/bitterless/control-center.toml
```

Launch the command center:

```bash
python3 starter/bin/control_center.py --check
python3 starter/bin/control_center.py
```

Print an example config without launching the UI:

```bash
python3 starter/bin/control_center.py --print-example-config
```

Run only the startup preflight and exit:

```bash
python3 starter/bin/control_center.py --check
```

## Config format

The default config path is `~/.config/bitterless/control-center.toml`.

Supported keys:

- `[ui]`
  - `refresh_interval_seconds`
  - `window_days`
- `[[repo]]`
  - required: `id`, `name`, `root`
  - optional: `runs_root`, `auto_start`, `default_profile`, `max_model_workers`,
    `max_score_workers`, `orchestrator_poll_seconds`

If `runs_root` is omitted the command center uses `<root>/starter/runs`.

## Keybindings

- `j` / `k`: move in the focused table
- arrows: native table navigation
- `tab`: rotate focus between repo list, run table, and details
- `/`: set a run filter
- `:`: open the command palette
- `enter`: focus the detail tabs
- `f`: toggle follow mode for `Events`, `Transcript`, or `Patch`
- `s`: cycle the active repo/run sort key
- `r`: reverse the active repo/run sort
- `a`: archive the selected run evidence
- `R`: open a restore command for the selected run
- `y`: run the selected repo runtime check
- `q`: quit

## Filters

Plain terms match `run_id`, state, error code, profile, and failure classifications.

Structured filters:

- `state:failed`
- `failure:eval_failed`
- `profile:capability`
- `age:7`

Example:

```text
state:failed profile:capability
```

## Command palette

Repo commands:

- `repo start [repo-id]`
- `repo stop [repo-id]`
- `repo restart [repo-id]`
- `repo canary [repo-id]`
- `runtime-check [repo-id]`

Run commands:

- `run cancel [run-id]`
- `run enqueue [run-id]`
- `run rerun [run-id]`
- `archive-run [run-id]`
- `restore-evidence [run-id] [/path/archive.tgz] [--force]`

Artifact commands:

- `open manifest`
- `open patch`
- `open events`
- `open transcript`
- `open score`
- `open-run-path [run-id]`
- `open-archive-path [run-id]`

Sorting and follow commands:

- `sort-runs [updated|state|pass|duration|queue_wait|score_wait|profile] [asc|desc]`
- `sort-repos [name|orchestrator|queue|in_flight|pass|p95] [asc|desc]`
- `toggle-follow [events|transcript|patch]`

If an id is omitted the currently selected repo or run is used.

## Action semantics

- Orchestrator processes are owned by the command center for configured repos.
- Orchestrator stdout/stderr are captured under `<runs_root>/.orchestrator/`.
- Supervisor health is persisted under `<runs_root>/.orchestrator/supervisor-status.json`.
- `run cancel` creates `.orchestrator-cancel`.
- `run enqueue` appends a queued model entry to `.orchestrator/run_queue.jsonl` and
  marks `run.state` as `queued` when the run is eligible.
- `run rerun` launches `starter/bin/run-task.sh` with `HARNESS_FORCE_RERUN=1`.
- `repo canary` launches `python3 starter/bin/run_real_canary.py`.
- `runtime-check` launches `starter/bin/check-supported-runtime.sh`.
- `archive-run` launches `starter/bin/archive-run-evidence.sh` and records the archive path
  in the repo health pane when the command completes.
- `restore-evidence` launches `starter/bin/restore-run-evidence.sh` into the selected repo
  `runs_root`. If the extracted run directory already exists, pass `--force` to confirm.
- `Overview` now shows absolute artifact paths for the selected run.
- `Patch` is a dedicated detail tab and is hidden when no patch artifact is present.
- The summary bar tracks fleet stale runs, failing runtime checks, canary health, and the
  active repo/run sort plus current filter.

## Limitations

- Local machine only
- No SSH or multi-host management
- No chat panel
- No policy promotion or release-gate controls
- Startup preflight blocks launch when the Python runtime, Textual dependency, repo layout,
  or a live prior command-center-managed orchestrator PID is invalid
- Persistent truth remains the existing run files and queue JSONL logs
