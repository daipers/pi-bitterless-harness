#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shlex
import sys
import time
from pathlib import Path

from control_centerlib import (
    ControlCenterService,
    FleetSnapshot,
    RepoSnapshot,
    RunRow,
    build_example_config_text,
    load_control_center_config,
    render_duration_ms,
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
          width: 30;
        }

        #run-table {
          width: 1fr;
        }

        #detail-pane {
          width: 44%;
          min-width: 40;
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

        def compose(self) -> ComposeResult:
            yield Header(show_clock=True)
            yield Static(id="summary-bar")
            with Horizontal(id="body"):
                yield DataTable(id="repo-table")
                yield DataTable(id="run-table")
                with Vertical(id="detail-pane"):
                    with TabbedContent(id="detail-tabs"):
                        with TabPane("Overview", id="tab-overview"):
                            yield Static(id="overview-text")
                        with TabPane("Events", id="tab-events"):
                            yield Static(id="events-text")
                        with TabPane("Transcript", id="tab-transcript"):
                            yield Static(id="transcript-text")
                        with TabPane("Score", id="tab-score"):
                            yield Static(id="score-text")
            yield Static(id="status-line")
            yield Input(id="command-input")
            yield Footer()

        def on_mount(self) -> None:
            repo_table = self.query_one("#repo-table", DataTable)
            repo_table.cursor_type = "row"
            repo_table.zebra_stripes = True
            repo_table.add_columns("Repo", "Orch", "Queue", "Pass")

            run_table = self.query_one("#run-table", DataTable)
            run_table.cursor_type = "row"
            run_table.zebra_stripes = True
            run_table.add_columns("Run", "State", "Pass", "Profile", "Error", "Dur", "Queue")

            self.set_interval(self.service.config.ui.refresh_interval_seconds, self.refresh_data)
            self.refresh_data()
            self.query_one("#repo-table", DataTable).focus()

        def on_unmount(self) -> None:
            self.service.close()

        def _set_status(self, message: str) -> None:
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

        def _filtered_runs(self, runs: tuple[RunRow, ...]) -> list[RunRow]:
            filtered = list(runs)
            tokens = [token for token in shlex.split(self.filter_text) if token.strip()]
            cutoff_ms = None
            for token in tokens:
                if ":" not in token:
                    continue
                key, value = token.split(":", 1)
                if key in {"age", "window"}:
                    try:
                        days = int(value)
                    except ValueError:
                        continue
                    cutoff_ms = int(time.time() * 1000) - (days * 24 * 60 * 60 * 1000)
            if cutoff_ms is not None:
                filtered = [run for run in filtered if run.updated_epoch_ms >= cutoff_ms]

            for token in tokens:
                if ":" not in token:
                    continue
                key, value = token.split(":", 1)
                if key == "state":
                    filtered = [run for run in filtered if run.state == value]
                elif key == "failure":
                    filtered = [
                        run
                        for run in filtered
                        if value in run.failure_classifications or run.primary_error_code == value
                    ]
                elif key == "profile":
                    filtered = [run for run in filtered if run.execution_profile == value]

            plain_terms = [token.lower() for token in tokens if ":" not in token]
            if plain_terms:
                filtered = [
                    run
                    for run in filtered
                    if all(
                        term
                        in " ".join(
                            [
                                run.run_id.lower(),
                                run.state.lower(),
                                run.primary_error_code.lower(),
                                run.execution_profile.lower(),
                                " ".join(run.failure_classifications).lower(),
                            ]
                        )
                        for term in plain_terms
                    )
                ]
            return filtered

        def _populate_repo_table(self) -> None:
            table = self.query_one("#repo-table", DataTable)
            table.clear(columns=False)
            self._repo_keys = []
            for repo in self.snapshot.repos:
                self._repo_keys.append(repo.repo.id)
                table.add_row(
                    repo.repo.name,
                    repo.orchestrator.state,
                    str(repo.queue_depth),
                    f"{repo.summary.get('pass_rate_percent', 0.0):.1f}%",
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
            for run in self._filtered_runs(repo.runs):
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

        def _update_detail(self) -> None:
            repo = self._selected_repo()
            run = self._selected_run()
            overview = self.query_one("#overview-text", Static)
            events = self.query_one("#events-text", Static)
            transcript = self.query_one("#transcript-text", Static)
            score = self.query_one("#score-text", Static)
            if repo is None or run is None:
                for widget in (overview, events, transcript, score):
                    widget.update("No run selected.")
                return
            overview.update(
                self.service.overview_text(
                    repo.repo.id,
                    run.run_id,
                    preview=self.overview_preview,
                )
            )
            events.update(self.service.read_artifact(repo.repo.id, run.run_id, "events"))
            transcript.update(self.service.read_artifact(repo.repo.id, run.run_id, "transcript"))
            score.update(self.service.read_artifact(repo.repo.id, run.run_id, "score"))

        def _update_summary_bar(self) -> None:
            self.query_one("#summary-bar", Static).update(
                " | ".join(
                    [
                        f"Repos {len(self.snapshot.repos)}",
                        f"Runs {self.snapshot.totals.get('total_runs', 0)}",
                        f"Pass {self.snapshot.pass_rate_percent:.1f}%",
                        "/ filter",
                        ": command",
                        "q quit",
                    ]
                )
            )

        def refresh_data(self) -> None:
            self.snapshot = self.service.refresh()
            if not self.selected_repo_id and self.snapshot.repos:
                self.selected_repo_id = self.snapshot.repos[0].repo.id
            self._populate_repo_table()
            self._populate_run_table()
            self._update_detail()
            self._update_summary_bar()

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
            self._open_input("command", "", "repo start | run cancel | open transcript")

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
                self._update_detail()
                self._set_status(f"filter set to: {self.filter_text or '(none)'}")
                return
            if mode == "command":
                self._execute_command(value)

        def _execute_command(self, command: str) -> None:
            try:
                tokens = shlex.split(command)
            except ValueError as exc:
                self._set_status(f"command parse error: {exc}")
                return
            if not tokens:
                self._set_status("no command entered")
                return

            repo = self._selected_repo()
            run = self._selected_run()
            try:
                if tokens[0] == "repo":
                    action = tokens[1] if len(tokens) > 1 else ""
                    repo_id = tokens[2] if len(tokens) > 2 else (repo.repo.id if repo else "")
                    if action == "start":
                        self._set_status(self.service.start_repo(repo_id))
                    elif action == "stop":
                        self._set_status(self.service.stop_repo(repo_id))
                    elif action == "restart":
                        self._set_status(self.service.restart_repo(repo_id))
                    elif action == "canary":
                        self._set_status(self.service.run_canary(repo_id))
                    else:
                        self._set_status(f"unknown repo action: {action}")
                elif tokens[0] == "run":
                    action = tokens[1] if len(tokens) > 1 else ""
                    run_id = tokens[2] if len(tokens) > 2 else (run.run_id if run else "")
                    repo_id = repo.repo.id if repo else ""
                    if action == "cancel":
                        self._set_status(self.service.cancel_run(repo_id, run_id))
                    elif action == "enqueue":
                        self._set_status(self.service.enqueue_run(repo_id, run_id))
                    elif action == "rerun":
                        self._set_status(self.service.rerun_run(repo_id, run_id))
                    else:
                        self._set_status(f"unknown run action: {action}")
                elif tokens[0] == "open":
                    target = tokens[1] if len(tokens) > 1 else "manifest"
                    if target == "events":
                        self._set_active_tab("tab-events")
                    elif target == "transcript":
                        self._set_active_tab("tab-transcript")
                    elif target == "score":
                        self._set_active_tab("tab-score")
                    elif target == "patch":
                        self.overview_preview = "patch"
                        self._set_active_tab("tab-overview")
                    else:
                        self.overview_preview = "manifest"
                        self._set_active_tab("tab-overview")
                    self._update_detail()
                    self._set_status(f"opened {target}")
                else:
                    self._set_status(f"unknown command: {tokens[0]}")
            except Exception as exc:
                self._set_status(str(exc))
            finally:
                self.refresh_data()


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bitterless Harness terminal command center.")
    parser.add_argument("--config", default=None, help="optional path to control-center.toml")
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
    if TEXTUAL_IMPORT_ERROR is not None:
        print(
            "textual is required for control_center.py. Install requirements-dev.txt first.",
            file=sys.stderr,
        )
        return 2
    config_path = Path(args.config).expanduser().resolve() if args.config else None
    service = ControlCenterService(load_control_center_config(config_path))
    app = ControlCenterApp(service)
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
