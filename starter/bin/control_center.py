#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shlex
import sys
import time
from pathlib import Path

from control_centerlib import (
    REPO_SORT_KEYS,
    RUN_SORT_KEYS,
    ControlCenterService,
    FleetSnapshot,
    RepoSnapshot,
    RunRow,
    SortState,
    build_example_config_text,
    render_duration_ms,
    render_startup_preflight,
    run_startup_preflight,
)

try:
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.containers import Horizontal, Vertical
    from textual.widgets import DataTable, Footer, Header, Input, Static, TabbedContent, TabPane
except ModuleNotFoundError as exc:  # pragma: no cover - handled in main
    TEXTUAL_IMPORT_ERROR = exc
else:
    TEXTUAL_IMPORT_ERROR = None


if TEXTUAL_IMPORT_ERROR is None:

    class ControlCenterApp(App[None]):
        CSS = """
        Screen {
          layout: vertical;
        }

        #summary-bar {
          height: 2;
          padding: 0 1;
          background: $surface;
          color: $text;
        }

        #body {
          height: 1fr;
          layout: horizontal;
        }

        #repo-table {
          width: 42;
        }

        #run-table {
          width: 1fr;
        }

        #detail-pane {
          width: 44%;
          min-width: 40;
        }

        #health-text, #overview-text, #events-text, #transcript-text, #score-text, #patch-text {
          overflow-y: auto;
        }

        #status-line {
          height: 2;
          padding: 0 1;
          background: $boost;
          color: $text;
        }

        #command-input {
          dock: bottom;
          display: none;
        }
        """

        BINDINGS = [
            Binding("q", "quit", "Quit"),
            Binding("j", "cursor_down", show=False),
            Binding("k", "cursor_up", show=False),
            Binding("tab", "cycle_focus", "Next Pane"),
            Binding("/", "open_filter", "Filter"),
            Binding(":", "open_command", "Command"),
            Binding("enter", "focus_details", "Details"),
            Binding("f", "toggle_follow", "Follow"),
            Binding("s", "cycle_sort", "Sort"),
            Binding("r", "reverse_sort", "Reverse"),
            Binding("a", "archive_run", "Archive"),
            Binding("shift+r", "restore_evidence", "Restore"),
            Binding("y", "runtime_check", "Runtime"),
            Binding("escape", "close_input", show=False),
        ]

        def __init__(self, service: ControlCenterService):
            super().__init__()
            self.service = service
            self.snapshot = FleetSnapshot(repos=(), totals={}, pass_rate_percent=0.0)
            self.selected_repo_id = ""
            self.selected_run_id = ""
            self.filter_text = ""
            self.input_mode = ""
            self.overview_preview = "manifest"
            self._repo_keys: list[str] = []
            self._run_keys: list[str] = []
            self.repo_sort = SortState(key="name", reverse=False)
            self.run_sort = SortState(key="updated", reverse=True)
            self.follow_mode = {"events": False, "transcript": False, "patch": False}
            self._detail_selection_key = ""
            self._status_hold_until = 0.0
            self._last_repo_message = ""

        def compose(self) -> ComposeResult:
            yield Header(show_clock=True)
            yield Static(id="summary-bar")
            with Horizontal(id="body"):
                yield DataTable(id="repo-table")
                yield DataTable(id="run-table")
                with Vertical(id="detail-pane"):
                    with TabbedContent(id="detail-tabs"):
                        with TabPane("Health", id="tab-health"):
                            yield Static(id="health-text")
                        with TabPane("Overview", id="tab-overview"):
                            yield Static(id="overview-text")
                        with TabPane("Events", id="tab-events"):
                            yield Static(id="events-text")
                        with TabPane("Transcript", id="tab-transcript"):
                            yield Static(id="transcript-text")
                        with TabPane("Score", id="tab-score"):
                            yield Static(id="score-text")
                        with TabPane("Patch", id="tab-patch"):
                            yield Static(id="patch-text")
            yield Static(id="status-line")
            yield Input(id="command-input")
            yield Footer()

        def on_mount(self) -> None:
            repo_table = self.query_one("#repo-table", DataTable)
            repo_table.cursor_type = "row"
            repo_table.zebra_stripes = True
            repo_table.add_columns("Repo", "Orch", "Queue", "InFlight", "Pass", "P95")

            run_table = self.query_one("#run-table", DataTable)
            run_table.cursor_type = "row"
            run_table.zebra_stripes = True
            run_table.add_columns("Run", "State", "Pass", "Profile", "Error", "Dur", "Queue")

            self.set_interval(self.service.config.ui.refresh_interval_seconds, self.refresh_data)
            self.refresh_data()
            self.query_one("#repo-table", DataTable).focus()

        def on_unmount(self) -> None:
            self.service.close()

        def _set_status(self, message: str, *, hold_seconds: float = 0.0) -> None:
            self._status_hold_until = time.time() + max(0.0, hold_seconds)
            self.query_one("#status-line", Static).update(message)

        def _selected_repo(self) -> RepoSnapshot | None:
            for repo in self.snapshot.repos:
                if repo.repo.id == self.selected_repo_id:
                    return repo
            return self.snapshot.repos[0] if self.snapshot.repos else None

        def _selected_run(self) -> RunRow | None:
            repo = self._selected_repo()
            if repo is None:
                return None
            for run in repo.runs:
                if run.run_id == self.selected_run_id:
                    return run
            return repo.runs[0] if repo.runs else None

        def _current_stream_kind(self) -> str:
            return {
                "tab-events": "events",
                "tab-transcript": "transcript",
                "tab-patch": "patch",
            }.get(self._detail_tab().active, "")

        def _sort_label(self, state: SortState) -> str:
            return f"{state.key}{' desc' if state.reverse else ' asc'}"

        def _populate_repo_table(self) -> None:
            table = self.query_one("#repo-table", DataTable)
            table.clear(columns=False)
            self._repo_keys = []
            for repo in self.service.sort_repos(self.snapshot.repos, self.repo_sort):
                self._repo_keys.append(repo.repo.id)
                table.add_row(
                    repo.repo.name,
                    repo.orchestrator.state,
                    str(repo.queue_depth),
                    str(repo.in_flight_count),
                    f"{repo.summary.get('pass_rate_percent', 0.0):.1f}%",
                    render_duration_ms(int(repo.summary.get("duration_ms", {}).get("p95", 0))),
                )
            if not self.selected_repo_id and self._repo_keys:
                self.selected_repo_id = self._repo_keys[0]
            if self.selected_repo_id in self._repo_keys:
                table.move_cursor(row=self._repo_keys.index(self.selected_repo_id))

        def _populate_run_table(self) -> None:
            table = self.query_one("#run-table", DataTable)
            table.clear(columns=False)
            self._run_keys = []
            repo = self._selected_repo()
            if repo is None:
                return
            filtered_runs = self.service.filter_runs(repo.runs, self.filter_text)
            for run in self.service.sort_runs(filtered_runs, self.run_sort):
                self._run_keys.append(run.run_id)
                table.add_row(
                    run.run_id,
                    run.state,
                    "-" if run.overall_pass is None else ("yes" if run.overall_pass else "no"),
                    run.execution_profile or "-",
                    run.primary_error_code or "-",
                    render_duration_ms(run.duration_ms),
                    render_duration_ms(run.queue_wait_ms),
                )
            if self.selected_run_id not in self._run_keys and self._run_keys:
                self.selected_run_id = self._run_keys[0]
            if self.selected_run_id in self._run_keys:
                table.move_cursor(row=self._run_keys.index(self.selected_run_id))

        def _detail_tab(self) -> TabbedContent:
            return self.query_one("#detail-tabs", TabbedContent)

        def _set_active_tab(self, tab_id: str) -> None:
            self._detail_tab().active = tab_id

        def _sync_patch_tab(self, has_patch: bool) -> None:
            patch_pane = self.query_one("#tab-patch", TabPane)
            patch_pane.styles.display = "block" if has_patch else "none"
            if not has_patch and self._detail_tab().active == "tab-patch":
                self._set_active_tab("tab-overview")

        def _update_stream_widget(self, widget_id: str, content: str, *, follow: bool) -> None:
            widget = self.query_one(widget_id, Static)
            widget.update(content)
            if follow:
                widget.scroll_end(animate=False)

        def _update_detail(self, *, force_streams: bool = False) -> None:
            repo = self._selected_repo()
            run = self._selected_run()
            health = self.query_one("#health-text", Static)
            overview = self.query_one("#overview-text", Static)
            events = self.query_one("#events-text", Static)
            transcript = self.query_one("#transcript-text", Static)
            score = self.query_one("#score-text", Static)
            patch = self.query_one("#patch-text", Static)
            if repo is None:
                self._sync_patch_tab(False)
                for widget in (health, overview, events, transcript, score, patch):
                    widget.update("No repo selected.")
                return
            health.update(self.service.repo_health_text(repo.repo.id))
            if run is None:
                self._sync_patch_tab(False)
                for widget in (overview, events, transcript, score, patch):
                    widget.update("No run selected.")
                return
            selection_key = f"{repo.repo.id}:{run.run_id}"
            selection_changed = selection_key != self._detail_selection_key
            active_tab = self._detail_tab().active
            has_patch = self.service.run_has_patch(repo.repo.id, run.run_id)
            self._sync_patch_tab(has_patch)
            overview.update(
                self.service.overview_text(
                    repo.repo.id,
                    run.run_id,
                    preview=self.overview_preview,
                )
            )
            score.update(self.service.read_artifact(repo.repo.id, run.run_id, "score"))
            if selection_changed or force_streams or (
                active_tab == "tab-events" and self.follow_mode["events"]
            ):
                self._update_stream_widget(
                    "#events-text",
                    self.service.read_artifact(repo.repo.id, run.run_id, "events"),
                    follow=active_tab == "tab-events" and self.follow_mode["events"],
                )
            if selection_changed or force_streams or (
                active_tab == "tab-transcript" and self.follow_mode["transcript"]
            ):
                self._update_stream_widget(
                    "#transcript-text",
                    self.service.read_artifact(repo.repo.id, run.run_id, "transcript"),
                    follow=active_tab == "tab-transcript" and self.follow_mode["transcript"],
                )
            if has_patch and (
                selection_changed
                or force_streams
                or (active_tab == "tab-patch" and self.follow_mode["patch"])
            ):
                self._update_stream_widget(
                    "#patch-text",
                    self.service.read_artifact(repo.repo.id, run.run_id, "patch"),
                    follow=active_tab == "tab-patch" and self.follow_mode["patch"],
                )
            elif not has_patch:
                patch.update("Patch is not available.")
            self._detail_selection_key = selection_key

        def _update_summary_bar(self) -> None:
            self.query_one("#summary-bar", Static).update(
                " | ".join(
                    [
                        f"Repos {len(self.snapshot.repos)}",
                        f"Runs {self.snapshot.totals.get('total_runs', 0)}",
                        f"Queued {self.snapshot.totals.get('queued', 0)}",
                        f"InFlight {self.snapshot.totals.get('in_flight', 0)}",
                        f"Stale {self.snapshot.totals.get('stale_runs', 0)}",
                        f"RuntimeFail {self.snapshot.totals.get('repos_runtime_failing', 0)}",
                        f"CanaryBad {self.snapshot.totals.get('repos_canary_bad', 0)}",
                        f"Pass {self.snapshot.pass_rate_percent:.1f}%",
                        f"RepoSort {self._sort_label(self.repo_sort)}",
                        f"RunSort {self._sort_label(self.run_sort)}",
                        f"Filter {self.filter_text or '(none)'}",
                        "/ filter",
                        ": command",
                        "q quit",
                    ]
                )
            )

        def _sync_status_from_repo(self) -> None:
            if time.time() < self._status_hold_until:
                return
            repo = self._selected_repo()
            if repo is None or not repo.recent_messages:
                return
            latest_message = repo.recent_messages[-1]
            if latest_message != self._last_repo_message:
                self._last_repo_message = latest_message
                self._set_status(latest_message)

        def refresh_data(self) -> None:
            self.snapshot = self.service.refresh()
            if not self.selected_repo_id and self.snapshot.repos:
                self.selected_repo_id = self.snapshot.repos[0].repo.id
            self._populate_repo_table()
            self._populate_run_table()
            self._update_detail()
            self._update_summary_bar()
            self._sync_status_from_repo()

        def _open_input(self, mode: str, value: str, placeholder: str) -> None:
            widget = self.query_one("#command-input", Input)
            self.input_mode = mode
            widget.value = value
            widget.placeholder = placeholder
            widget.styles.display = "block"
            widget.focus()

        def action_open_filter(self) -> None:
            self._open_input("filter", self.filter_text, "state:failed profile:capability")

        def action_open_command(self) -> None:
            self._open_input(
                "command",
                "",
                "archive-run | restore-evidence | runtime-check | sort-runs duration desc",
            )

        def action_close_input(self) -> None:
            widget = self.query_one("#command-input", Input)
            widget.styles.display = "none"
            self.input_mode = ""
            self.query_one("#run-table", DataTable).focus()

        def action_cycle_focus(self) -> None:
            focus_order = [
                self.query_one("#repo-table", DataTable),
                self.query_one("#run-table", DataTable),
                self._detail_tab(),
            ]
            current = self.focused
            if current not in focus_order:
                focus_order[0].focus()
                return
            focus_order[(focus_order.index(current) + 1) % len(focus_order)].focus()

        def action_focus_details(self) -> None:
            self._detail_tab().focus()

        def action_toggle_follow(self) -> None:
            kind = self._current_stream_kind()
            if not kind:
                self._set_status(
                    "follow mode only applies to events, transcript, or patch",
                    hold_seconds=2.0,
                )
                return
            self.follow_mode[kind] = not self.follow_mode[kind]
            self._update_detail(force_streams=True)
            self._set_status(
                f"{kind} follow {'enabled' if self.follow_mode[kind] else 'disabled'}",
                hold_seconds=2.0,
            )

        def action_cycle_sort(self) -> None:
            focused = self.focused
            if isinstance(focused, DataTable) and focused.id == "repo-table":
                keys = list(REPO_SORT_KEYS)
                index = keys.index(self.repo_sort.key)
                self.repo_sort = SortState(
                    key=keys[(index + 1) % len(keys)],
                    reverse=self.repo_sort.reverse,
                )
                self._populate_repo_table()
                self._set_status(f"repo sort: {self._sort_label(self.repo_sort)}", hold_seconds=2.0)
                return
            if isinstance(focused, DataTable) and focused.id == "run-table":
                keys = list(RUN_SORT_KEYS)
                index = keys.index(self.run_sort.key)
                self.run_sort = SortState(
                    key=keys[(index + 1) % len(keys)],
                    reverse=self.run_sort.reverse,
                )
                self._populate_run_table()
                self._set_status(f"run sort: {self._sort_label(self.run_sort)}", hold_seconds=2.0)
                return
            self._set_status("focus the repo or run table before sorting", hold_seconds=2.0)

        def action_reverse_sort(self) -> None:
            focused = self.focused
            if isinstance(focused, DataTable) and focused.id == "repo-table":
                self.repo_sort = SortState(
                    key=self.repo_sort.key,
                    reverse=not self.repo_sort.reverse,
                )
                self._populate_repo_table()
                self._set_status(f"repo sort: {self._sort_label(self.repo_sort)}", hold_seconds=2.0)
                return
            if isinstance(focused, DataTable) and focused.id == "run-table":
                self.run_sort = SortState(
                    key=self.run_sort.key,
                    reverse=not self.run_sort.reverse,
                )
                self._populate_run_table()
                self._set_status(f"run sort: {self._sort_label(self.run_sort)}", hold_seconds=2.0)
                return
            self._set_status("focus the repo or run table before reversing sort", hold_seconds=2.0)

        def action_archive_run(self) -> None:
            repo = self._selected_repo()
            run = self._selected_run()
            if repo is None or run is None:
                self._set_status("select a run before archiving", hold_seconds=2.0)
                return
            self._set_status(self.service.archive_run(repo.repo.id, run.run_id), hold_seconds=3.0)
            self.refresh_data()

        def action_restore_evidence(self) -> None:
            run = self._selected_run()
            seed = f"restore-evidence {run.run_id} " if run is not None else "restore-evidence "
            self._open_input(
                "command",
                seed,
                "restore-evidence [run-id] [/path/archive.tgz] [--force]",
            )

        def action_runtime_check(self) -> None:
            repo = self._selected_repo()
            if repo is None:
                self._set_status("select a repo before running runtime check", hold_seconds=2.0)
                return
            self._set_status(self.service.runtime_check(repo.repo.id), hold_seconds=3.0)
            self.refresh_data()

        def action_cursor_down(self) -> None:
            self._move_table_cursor(1)

        def action_cursor_up(self) -> None:
            self._move_table_cursor(-1)

        def _move_table_cursor(self, delta: int) -> None:
            focused = self.focused
            if not isinstance(focused, DataTable):
                return
            row_count = focused.row_count
            if row_count <= 0:
                return
            target = max(0, min(row_count - 1, focused.cursor_row + delta))
            focused.move_cursor(row=target)

        def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
            if event.data_table.id == "repo-table" and 0 <= event.cursor_row < len(self._repo_keys):
                self.selected_repo_id = self._repo_keys[event.cursor_row]
                self._populate_run_table()
                self._update_detail()
            elif event.data_table.id == "run-table" and 0 <= event.cursor_row < len(self._run_keys):
                self.selected_run_id = self._run_keys[event.cursor_row]
                self._update_detail()

        def on_input_submitted(self, event: Input.Submitted) -> None:
            widget = self.query_one("#command-input", Input)
            value = event.value.strip()
            widget.styles.display = "none"
            mode = self.input_mode
            self.input_mode = ""
            if mode == "filter":
                self.filter_text = value
                self._populate_run_table()
                self._update_detail(force_streams=True)
                self._set_status(f"filter set to: {self.filter_text or '(none)'}", hold_seconds=2.0)
                return
            if mode == "command":
                self._execute_command(value)

        def _parse_sort_command(self, tokens: list[str], *, repo_scope: bool) -> None:
            allowed = REPO_SORT_KEYS if repo_scope else RUN_SORT_KEYS
            current = self.repo_sort if repo_scope else self.run_sort
            key = tokens[1] if len(tokens) > 1 else current.key
            if key not in allowed:
                self._set_status(f"unknown sort key: {key}", hold_seconds=2.0)
                return
            reverse = current.reverse
            if len(tokens) > 2:
                direction = tokens[2].lower()
                if direction in {"asc", "ascending"}:
                    reverse = False
                elif direction in {"desc", "descending"}:
                    reverse = True
            state = SortState(key=key, reverse=reverse)
            if repo_scope:
                self.repo_sort = state
                self._populate_repo_table()
                self._set_status(f"repo sort: {self._sort_label(self.repo_sort)}", hold_seconds=2.0)
            else:
                self.run_sort = state
                self._populate_run_table()
                self._set_status(f"run sort: {self._sort_label(self.run_sort)}", hold_seconds=2.0)

        def _parse_restore_command(
            self,
            tokens: list[str],
            repo: RepoSnapshot | None,
            run: RunRow | None,
        ) -> None:
            repo_id = repo.repo.id if repo else ""
            force = "--force" in tokens[1:]
            args = [token for token in tokens[1:] if token != "--force"]
            run_id = run.run_id if run else ""
            archive_path = ""
            if args:
                first = args[0]
                if "/" in first or first.endswith(".tgz"):
                    archive_path = first
                else:
                    run_id = first
            if len(args) > 1:
                archive_path = args[1]
            self._set_status(
                self.service.restore_evidence(
                    repo_id,
                    run_id,
                    archive_path=archive_path,
                    force=force,
                ),
                hold_seconds=3.0,
            )

        def _execute_command(self, command: str) -> None:
            try:
                tokens = shlex.split(command)
            except ValueError as exc:
                self._set_status(f"command parse error: {exc}", hold_seconds=2.0)
                return
            if not tokens:
                self._set_status("no command entered", hold_seconds=2.0)
                return

            repo = self._selected_repo()
            run = self._selected_run()
            try:
                if tokens[0] == "archive-run":
                    repo_id = repo.repo.id if repo else ""
                    run_id = tokens[1] if len(tokens) > 1 else (run.run_id if run else "")
                    self._set_status(self.service.archive_run(repo_id, run_id), hold_seconds=3.0)
                elif tokens[0] == "restore-evidence":
                    self._parse_restore_command(tokens, repo, run)
                elif tokens[0] == "runtime-check":
                    repo_id = tokens[1] if len(tokens) > 1 else (repo.repo.id if repo else "")
                    self._set_status(self.service.runtime_check(repo_id), hold_seconds=3.0)
                elif tokens[0] == "open-run-path":
                    repo_id = repo.repo.id if repo else ""
                    run_id = tokens[1] if len(tokens) > 1 else (run.run_id if run else "")
                    self._set_status(self.service.open_run_path(repo_id, run_id), hold_seconds=5.0)
                elif tokens[0] == "open-archive-path":
                    repo_id = repo.repo.id if repo else ""
                    run_id = tokens[1] if len(tokens) > 1 else (run.run_id if run else "")
                    self._set_status(
                        self.service.open_archive_path(repo_id, run_id),
                        hold_seconds=5.0,
                    )
                elif tokens[0] == "sort-runs":
                    self._parse_sort_command(tokens, repo_scope=False)
                elif tokens[0] == "sort-repos":
                    self._parse_sort_command(tokens, repo_scope=True)
                elif tokens[0] == "toggle-follow":
                    target = tokens[1] if len(tokens) > 1 else self._current_stream_kind()
                    if target not in self.follow_mode:
                        self._set_status(
                            f"unknown follow target: {target or '-'}",
                            hold_seconds=2.0,
                        )
                    else:
                        self.follow_mode[target] = not self.follow_mode[target]
                        self._update_detail(force_streams=True)
                        self._set_status(
                            (
                                f"{target} follow "
                                f"{'enabled' if self.follow_mode[target] else 'disabled'}"
                            ),
                            hold_seconds=2.0,
                        )
                elif tokens[0] == "repo":
                    action = tokens[1] if len(tokens) > 1 else ""
                    repo_id = tokens[2] if len(tokens) > 2 else (repo.repo.id if repo else "")
                    if action == "start":
                        self._set_status(self.service.start_repo(repo_id), hold_seconds=3.0)
                    elif action == "stop":
                        self._set_status(self.service.stop_repo(repo_id), hold_seconds=3.0)
                    elif action == "restart":
                        self._set_status(self.service.restart_repo(repo_id), hold_seconds=3.0)
                    elif action == "canary":
                        self._set_status(self.service.run_canary(repo_id), hold_seconds=3.0)
                    else:
                        self._set_status(f"unknown repo action: {action}", hold_seconds=2.0)
                elif tokens[0] == "run":
                    action = tokens[1] if len(tokens) > 1 else ""
                    run_id = tokens[2] if len(tokens) > 2 else (run.run_id if run else "")
                    repo_id = repo.repo.id if repo else ""
                    if action == "cancel":
                        self._set_status(self.service.cancel_run(repo_id, run_id), hold_seconds=3.0)
                    elif action == "enqueue":
                        self._set_status(
                            self.service.enqueue_run(repo_id, run_id),
                            hold_seconds=3.0,
                        )
                    elif action == "rerun":
                        self._set_status(self.service.rerun_run(repo_id, run_id), hold_seconds=3.0)
                    else:
                        self._set_status(f"unknown run action: {action}", hold_seconds=2.0)
                elif tokens[0] == "open":
                    target = tokens[1] if len(tokens) > 1 else "manifest"
                    if target == "health":
                        self._set_active_tab("tab-health")
                    elif target == "events":
                        self._set_active_tab("tab-events")
                    elif target == "transcript":
                        self._set_active_tab("tab-transcript")
                    elif target == "score":
                        self._set_active_tab("tab-score")
                    elif target == "patch":
                        self._set_active_tab("tab-patch")
                    else:
                        self.overview_preview = "manifest"
                        self._set_active_tab("tab-overview")
                    self._update_detail(force_streams=True)
                    self._set_status(f"opened {target}", hold_seconds=2.0)
                else:
                    self._set_status(f"unknown command: {tokens[0]}", hold_seconds=2.0)
            except Exception as exc:
                self._set_status(str(exc), hold_seconds=3.0)
            finally:
                self.refresh_data()


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bitterless Harness terminal command center.")
    parser.add_argument("--config", default=None, help="optional path to control-center.toml")
    parser.add_argument(
        "--check",
        action="store_true",
        help="run startup preflight checks and exit",
    )
    parser.add_argument(
        "--print-example-config",
        action="store_true",
        help="print a starter config to stdout and exit",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    if args.print_example_config:
        print(build_example_config_text(repo_root))
        return 0
    config_path = Path(args.config).expanduser().resolve() if args.config else None
    report = run_startup_preflight(config_path, textual_import_error=TEXTUAL_IMPORT_ERROR)
    if args.check or not report.ok:
        print(
            render_startup_preflight(report),
            file=(sys.stdout if args.check else sys.stderr),
            end="",
        )
    if args.check:
        return 0 if report.ok else 2
    if not report.ok or report.config is None:
        return 2
    service = ControlCenterService(report.config)
    app = ControlCenterApp(service)
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
