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
python3 starter/bin/control_center.py
```

Print an example config without launching the UI:

```bash
python3 starter/bin/control_center.py --print-example-config
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

Run commands:

- `run cancel [run-id]`
- `run enqueue [run-id]`
- `run rerun [run-id]`

Artifact commands:

- `open manifest`
- `open patch`
- `open events`
- `open transcript`
- `open score`

If an id is omitted the currently selected repo or run is used.

## Action semantics

- Orchestrator processes are owned by the command center for configured repos.
- Orchestrator stdout/stderr are captured under `<runs_root>/.orchestrator/`.
- `run cancel` creates `.orchestrator-cancel`.
- `run enqueue` appends a queued model entry to `.orchestrator/run_queue.jsonl` and
  marks `run.state` as `queued` when the run is eligible.
- `run rerun` launches `starter/bin/run-task.sh` with `HARNESS_FORCE_RERUN=1`.
- `repo canary` launches `python3 starter/bin/run_real_canary.py`.

## Limitations

- Local machine only
- No SSH or multi-host management
- No chat panel
- No policy promotion or release-gate controls
- Persistent truth remains the existing run files and queue JSONL logs
