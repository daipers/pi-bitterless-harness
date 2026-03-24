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
  - optional: `runs_root`, `auto_start`, `default_profile`, `default_model`,
    `max_model_workers`, `max_score_workers`, `orchestrator_poll_seconds`

If `runs_root` is omitted the command center uses `<root>/starter/runs`.

## Keybindings

- `j` / `k`: move in the focused table
- arrows: native table navigation
- `tab`: rotate focus between repo list, filter text, run list, and detail tabs
- `/`: focus the visible filter bar text field
- `:`: open the searchable command picker
- `o`: open the most useful artifact for the selected run
- `?`: open the in-app help tab with shortcuts, filter examples, and common commands
- `enter`: focus the detail tabs
- `Chat` tab: operator chat for repo/run control and review-before-launch new runs
- `f`: toggle follow mode for `Events`, `Transcript`, or `Patch`
- `s`: cycle the active repo/run sort key
- `r`: reverse the active repo/run sort
- `a`: archive the selected run evidence
- `R`: open a restore command for the selected run
- `y`: run the selected repo runtime check
- `q`: quit

## Filters

Above the filter chips there is a built-in `Saved Views` row for:

- `All`
- `Failures`
- `Queued`
- `Capability`
- `Recent 24h`
- `Long-running`

Applying a saved view resets the filter/sort seed for that workflow, then you can refine it further with chips or text.

The run pane has a visible guided filter bar with chips for:

- `Failed`
- `Queued`
- `Capability`
- `Last 24h`
- trailing text refinement

Matching is AND-based across every enabled chip plus the text box.

The text refinement matches plain terms against `run_id`, state, error code,
profile, and failure classifications.

`/` focuses the text field, and `filter clear` still works from the raw command path.

## Command palette

Press `:` to open a searchable picker that surfaces context-aware run actions first,
then recommended actions, repo actions, and navigation/filter actions.

With a blank query the picker is grouped into:

- `Recommended Now`
- `Recent Commands`
- `Saved Views`
- `All Actions`

The picker supports keyboard selection, and the highlighted result can be moved with the picker cursor actions before pressing `Enter`.

The picker includes:

- selected-run actions like `Open Best Artifact`, `Open Transcript`, `Open Score`, `Cancel`, `Rerun`
- repo actions like `Runtime Check`, `Start Repo`, `Stop Repo`, `Restart Repo`, `Run Canary`
- navigation, saved-view, and filter helpers like `Open Help`, `Toggle Failed Filter`, `Focus Newest Failed Run`, and `Failures`
- `Open Raw Command Prompt...` as a fallback into the original command entry flow

The raw command executor remains available for operators who prefer direct commands.

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
- `open chat`
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

## Detail Pane

The right pane now keeps the same high-signal structure for every selection:

- dismissible `Getting Started` guidance on first load, with empty-state variants when no repo or run is available
- current target card with repo, run, state, pass/fail, profile, relative age, and safe next actions
- alert banner with plain-language health warnings and failures
- alert action buttons for the highest-priority next steps
- run timeline strip for `Queued -> Claimed -> Model Running -> Scoring -> Complete`
- inline action rail for common run and repo actions, including visible shortcut suffixes where a real binding exists
- action hint strip and best-artifact reason note
- recent activity panel with session-scoped actions, supervisor messages, and managed command state
- existing tabbed artifacts below that shell

Repo and run tables now prepend compact ASCII badges such as `[OK]`, `[!]`, `[FAIL]`, `[Q]`, and `[HOT]` to make scanning easier without adding more columns.

`Open Best Artifact` chooses the most useful tab for the selected run:

- failed or cancelled runs prefer `Score`, then `Transcript`, then `Events`
- passing complete runs prefer `Patch`, then `Overview`
- in-flight runs prefer `Events`

When `Open Best Artifact` runs, the detail shell also explains why that tab was chosen, for example:

- `Opened Score because this run failed.`
- `Opened Events because this run is still in progress.`

Destructive UI actions such as `Stop Repo`, `Restart Repo`, `Cancel Run`,
`Rerun`, and force restore now use modal `Confirm` / `Cancel` buttons instead
of typed confirmation.

Repo detail memory is preserved when switching repos, including the active tab,
overview preview mode, and enabled follow streams.

## Chat Follow-Ups

The latest assistant reply in `Chat` can expose clickable follow-up actions for
common next steps. Examples:

- failed-run summaries can offer `Focus Newest Failed Run`, `Filter Failed`, and `Open Score for Newest Failed`
- queue summaries can offer `Filter Queued`, `Open Health`, and `Runtime Check`
- current-run summaries can offer `Open Best Artifact`, `Open Transcript`, and `Rerun` for terminal failures
- canary summaries can offer `Open Health` and `Run Canary`

## Action semantics

- Orchestrator processes are owned by the command center for configured repos.
- Orchestrator stdout/stderr are captured under `<runs_root>/.orchestrator/`.
- Supervisor health is persisted under `<runs_root>/.orchestrator/supervisor-status.json`.
- Chat audit is persisted under `<runs_root>/.orchestrator/chat-log.jsonl`.
- Pending chat confirmation state is persisted under `<runs_root>/.orchestrator/chat-state.json`.
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
- Repo and run panes now show the current selection, visible counts, active sort, and filter state.
- When a filter hides every run, the detail panes explain that state instead of showing a hidden run.
- `Help` is a dedicated detail tab with quick-start guidance, shortcut reminders, and command examples.
- `Chat` is an operator shell over the existing harness actions. It can answer bounded
  status questions, stage confirmed control actions, and draft new runs before launch.
- New run drafts use repo `default_profile` and `default_model` when present, and require
  explicit `confirm` before create+launch or create+enqueue.
- The summary bar tracks fleet stale runs, failing runtime checks, canary health, and the
  active repo/run sort plus current filter.

## Limitations

- Local machine only
- No SSH or multi-host management
- No policy promotion or release-gate controls
- Chat does not rewrite tasks behind the operator's back or act as a planner/manager layer
- Startup preflight blocks launch when the Python runtime, Textual dependency, repo layout,
  or a live prior command-center-managed orchestrator PID is invalid
- Persistent truth remains the existing run files and queue JSONL logs
