#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shlex
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from control_centerlib import (
    REPO_SORT_KEYS,
    RUN_SORT_KEYS,
    AlertBadge,
    ArtifactRecommendation,
    ControlCenterService,
    FleetSnapshot,
    format_timestamp_ms,
    RepoSnapshot,
    RepoViewState,
    RunFilterState,
    RunRow,
    SavedViewPreset,
    SortState,
    TargetSummary,
    TimelineStep,
    UIAction,
    build_example_config_text,
    render_duration_ms,
    render_startup_preflight,
    run_startup_preflight,
)

try:
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.containers import Horizontal, Vertical
    from textual.css.query import NoMatches
    from textual.screen import ModalScreen
    from textual.widgets import (
        Button,
        DataTable,
        Footer,
        Header,
        Input,
        Static,
        TabbedContent,
        TabPane,
    )
except ModuleNotFoundError as exc:  # pragma: no cover - handled in main
    TEXTUAL_IMPORT_ERROR = exc
else:
    TEXTUAL_IMPORT_ERROR = None


def _chunked(items: tuple[UIAction, ...], size: int) -> list[tuple[UIAction, ...]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _tab_title(tab_id: str) -> str:
    return {
        "tab-chat": "chat",
        "tab-health": "health",
        "tab-overview": "overview",
        "tab-events": "events",
        "tab-transcript": "transcript",
        "tab-score": "score",
        "tab-patch": "patch",
        "tab-help": "help",
    }.get(tab_id, tab_id.removeprefix("tab-"))


@dataclass(frozen=True)
class ActivityEntry:
    ts_ms: int
    state: str
    target: str
    message: str
    repo_id: str


@dataclass(frozen=True)
class PickerResult:
    action: UIAction
    section: str
    priority: int
    row_index: int


if TEXTUAL_IMPORT_ERROR is None:

    class ConfirmationScreen(ModalScreen[bool]):
        CSS = """
        ConfirmationScreen {
          align: center middle;
        }

        #confirm-dialog {
          width: 60;
          border: round $accent;
          background: $surface;
          padding: 1 2;
          layout: vertical;
        }

        #confirm-actions {
          height: 3;
          layout: horizontal;
          margin-top: 1;
        }

        #confirm-message {
          padding: 0 0 1 0;
        }
        """

        BINDINGS = [
            Binding("escape", "dismiss_false", show=False),
        ]

        def __init__(self, action: UIAction):
            super().__init__()
            self.action = action

        def compose(self) -> ComposeResult:
            with Vertical(id="confirm-dialog"):
                yield Static(self.action.label, id="confirm-title")
                yield Static(
                    (
                        "This action changes repo state.\n\n"
                        f"Command: {self.action.command_text}"
                    ),
                    id="confirm-message",
                )
                with Horizontal(id="confirm-actions"):
                    yield Button("Confirm", id="confirm-yes", variant="error")
                    yield Button("Cancel", id="confirm-no")

        def action_dismiss_false(self) -> None:
            self.dismiss(False)

        def on_button_pressed(self, event: Button.Pressed) -> None:
            self.dismiss(event.button.id == "confirm-yes")


    class CommandPickerScreen(ModalScreen[str | None]):
        CSS = """
        CommandPickerScreen {
          align: center middle;
        }

        #picker-dialog {
          width: 90;
          max-height: 32;
          border: round $accent;
          background: $surface;
          padding: 1 2;
          layout: vertical;
        }

        .picker-section {
          height: 1;
          color: $text-muted;
          margin-top: 1;
        }

        #picker-results {
          height: auto;
          layout: vertical;
          margin-top: 1;
        }

        .picker-result {
          width: 1fr;
          margin-bottom: 1;
        }
        """

        BINDINGS = [
            Binding("escape", "dismiss_none", show=False),
            Binding("down", "cursor_down", show=False),
            Binding("up", "cursor_up", show=False),
            Binding("enter", "submit_selection", show=False),
        ]

        def __init__(self, results: tuple[PickerResult, ...]):
            super().__init__()
            self.results = results
            self.visible_results: tuple[PickerResult, ...] = ()
            self.highlighted_index = 0

        def compose(self) -> ComposeResult:
            with Vertical(id="picker-dialog"):
                yield Static("Command Picker", id="picker-title")
                yield Input(id="picker-query", placeholder="Search actions or commands")
                with Vertical(id="picker-results"):
                    for index in range(4):
                        yield Static("", id=f"picker-section-{index}", classes="picker-section")
                    for index in range(12):
                        yield Button("", id=f"picker-result-{index}", classes="picker-result")

        def on_mount(self) -> None:
            self.query_one("#picker-query", Input).focus()
            self._refresh_results("")

        def action_dismiss_none(self) -> None:
            self.dismiss(None)

        def _display_label(self, action: UIAction) -> str:
            label = action.label
            if action.shortcut_label:
                label += f" ({action.shortcut_label})"
            if not action.enabled and action.disabled_reason:
                label += f" ({action.disabled_reason})"
            return label

        def _match_rank(self, result: PickerResult, query: str) -> tuple[int, int, int, str]:
            action = result.action
            label = action.label.lower()
            command = action.command_text.lower()
            aliases = [alias.lower() for alias in action.aliases]
            haystack = " ".join([label, command, " ".join(aliases)])
            if label == query:
                return (0, result.priority, result.row_index, label)
            if label.startswith(query):
                return (1, result.priority, result.row_index, label)
            if command.startswith(query):
                return (2, result.priority, result.row_index, label)
            if query in haystack:
                return (3, result.priority, result.row_index, label)
            if any(query in alias for alias in aliases):
                return (4, result.priority, result.row_index, label)
            letters = "".join(char for char in haystack if char.isalnum() or char == " ")
            query_letters = "".join(char for char in query if char.isalnum() or char == " ")
            if query_letters and all(char in letters for char in query_letters):
                return (5, result.priority, len(haystack), label)
            return (9, result.priority, len(haystack), label)

        def _blank_query_results(self) -> tuple[PickerResult, ...]:
            ordered = sorted(self.results, key=lambda item: (item.priority, item.row_index, item.action.label.lower()))
            return tuple(ordered[:12])

        def _search_results(self, query: str) -> tuple[PickerResult, ...]:
            ordered = sorted(self.results, key=lambda item: self._match_rank(item, query))
            return tuple(
                item for item in ordered if self._match_rank(item, query)[0] < 9
            )[:12]

        def _apply_highlight(self) -> None:
            for index in range(12):
                button = self.query_one(f"#picker-result-{index}", Button)
                if index >= len(self.visible_results):
                    continue
                action = self.visible_results[index].action
                if index == self.highlighted_index:
                    button.variant = "primary"
                elif action.requires_confirmation:
                    button.variant = "warning"
                else:
                    button.variant = "default"

        def _refresh_results(self, query: str) -> None:
            normalized = query.lower().strip()
            self.visible_results = (
                self._blank_query_results() if not normalized else self._search_results(normalized)
            )
            self.highlighted_index = 0 if self.visible_results else -1

            if not normalized:
                grouped: list[tuple[str, list[PickerResult]]] = []
                for result in self.visible_results:
                    if not grouped or grouped[-1][0] != result.section:
                        grouped.append((result.section, [result]))
                    else:
                        grouped[-1][1].append(result)
                for section_index in range(4):
                    title = self.query_one(f"#picker-section-{section_index}", Static)
                    if section_index >= len(grouped):
                        title.update("")
                        title.styles.display = "none"
                    else:
                        title.update(grouped[section_index][0])
                        title.styles.display = "block"
            else:
                self.query_one("#picker-section-0", Static).update("Search Results")
                self.query_one("#picker-section-0", Static).styles.display = "block"
                for section_index in range(1, 4):
                    title = self.query_one(f"#picker-section-{section_index}", Static)
                    title.update("")
                    title.styles.display = "none"

            for index in range(12):
                button = self.query_one(f"#picker-result-{index}", Button)
                if index >= len(self.visible_results):
                    button.label = ""
                    button.disabled = True
                    button.styles.display = "none"
                    continue
                result = self.visible_results[index]
                button.label = self._display_label(result.action)
                button.disabled = False
                button.styles.display = "block"
            self._apply_highlight()

        def action_cursor_down(self) -> None:
            if not self.visible_results:
                return
            self.highlighted_index = min(len(self.visible_results) - 1, self.highlighted_index + 1)
            self._apply_highlight()

        def action_cursor_up(self) -> None:
            if not self.visible_results:
                return
            self.highlighted_index = max(0, self.highlighted_index - 1)
            self._apply_highlight()

        def action_submit_selection(self) -> None:
            if not self.visible_results or self.highlighted_index < 0:
                return
            self.dismiss(self.visible_results[self.highlighted_index].action.id)

        def on_input_changed(self, event: Input.Changed) -> None:
            if event.input.id == "picker-query":
                self._refresh_results(event.value)

        def on_input_submitted(self, event: Input.Submitted) -> None:
            if event.input.id == "picker-query":
                self.action_submit_selection()

        def on_key(self, event) -> None:
            if event.key == "down":
                event.stop()
                self.action_cursor_down()
            elif event.key == "up":
                event.stop()
                self.action_cursor_up()

        def on_button_pressed(self, event: Button.Pressed) -> None:
            if not event.button.id.startswith("picker-result-"):
                return
            index = int(event.button.id.rsplit("-", 1)[1])
            if 0 <= index < len(self.visible_results):
                self.dismiss(self.visible_results[index].action.id)


    class ControlCenterApp(App[None]):
        CSS = """
        Screen {
          layout: vertical;
        }

        #summary-bar {
          height: 3;
          padding: 0 1;
          background: $surface;
          color: $text;
        }

        #body {
          height: 1fr;
          layout: horizontal;
        }

        #repo-pane {
          width: 42;
          layout: vertical;
          border: round $surface-lighten-1;
          padding: 0 0 1 0;
        }

        #run-pane {
          width: 1fr;
          layout: vertical;
          border: round $surface-lighten-1;
          padding: 0 0 1 0;
        }

        #filter-pane {
          layout: vertical;
          border: round $surface-lighten-1;
          margin-bottom: 1;
        }

        #detail-pane {
          width: 48%;
          min-width: 52;
          layout: vertical;
          border: round $surface-lighten-1;
          padding: 0 0 1 0;
        }

        #repo-label, #run-label {
          height: 2;
          padding: 0 1;
          background: $surface-lighten-1;
          color: $text;
        }

        #saved-views {
          height: 3;
          layout: horizontal;
          padding: 0 1;
          background: $surface;
        }

        .saved-view {
          margin-right: 1;
        }

        #filter-bar {
          height: 3;
          layout: horizontal;
          padding: 0 1;
          background: $panel-lighten-1;
        }

        .filter-chip {
          margin-right: 1;
        }

        #filter-text {
          width: 1fr;
        }

        #repo-table, #run-table {
          height: 1fr;
        }

        #intro-card, #target-card, #alert-banner, #timeline-strip, #recent-activity {
          padding: 0 1;
          background: $surface;
          color: $text;
          margin-bottom: 1;
        }

        #intro-card {
          height: auto;
        }

        #intro-actions {
          height: 3;
          layout: horizontal;
          margin-bottom: 1;
        }

        #target-card {
          height: 6;
        }

        #alert-banner {
          height: auto;
        }

        #timeline-strip {
          height: 3;
        }

        #alert-actions {
          height: 3;
          layout: horizontal;
          margin-bottom: 1;
        }

        .alert-action {
          width: 1fr;
          margin-right: 1;
        }

        #action-rail {
          height: 8;
          layout: vertical;
        }

        .action-row {
          height: 2;
          layout: horizontal;
        }

        .action-slot {
          width: 1fr;
          margin-right: 1;
        }

        #action-hint, #artifact-note {
          height: 2;
          padding: 0 1;
          background: $surface-lighten-1;
          color: $text-muted;
          margin-bottom: 1;
        }

        #recent-activity {
          height: 6;
        }

        #detail-tabs {
          height: 1fr;
        }

        #chat-pane {
          height: 1fr;
          layout: vertical;
        }

        #chat-banner {
          height: 5;
          padding: 0 1;
          background: $surface-lighten-1;
          color: $text;
        }

        #chat-history, #health-text, #overview-text, #events-text, #transcript-text, #score-text, #patch-text, #help-text {
          overflow-y: auto;
        }

        #chat-followups {
          height: 4;
          layout: horizontal;
          margin: 1 0;
        }

        .chat-followup {
          width: 1fr;
          margin-right: 1;
        }

        #chat-input {
          dock: bottom;
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

        .is-focused {
          border: round $accent;
        }

        .is-active #repo-label, .is-active #run-label {
          background: $accent-darken-2;
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
            Binding("o", "open_best_artifact", "Best Artifact"),
            Binding("?", "open_help", "Help"),
            Binding("escape", "close_input", show=False),
        ]

        def __init__(self, service: ControlCenterService):
            super().__init__()
            self.service = service
            self.snapshot = FleetSnapshot(repos=(), totals={}, pass_rate_percent=0.0)
            self.selected_repo_id = ""
            self.selected_run_id = ""
            self.selected_run_ids: dict[str, str] = {}
            self.input_mode = ""
            self.overview_preview = "manifest"
            self.filter_state = RunFilterState()
            self._repo_keys: list[str] = []
            self._run_keys: list[str] = []
            self.repo_sort = SortState(key="name", reverse=False)
            self.run_sort = SortState(key="updated", reverse=True)
            self.follow_mode = {"events": False, "transcript": False, "patch": False}
            self._detail_selection_key = ""
            self._status_hold_until = 0.0
            self._last_repo_message = ""
            self._repo_views: dict[str, RepoViewState] = {}
            self._current_action_slots: dict[str, UIAction] = {}
            self._current_chat_followups: dict[str, UIAction] = {}
            self._current_alert_actions: dict[str, UIAction] = {}
            self._picker_actions: dict[str, UIAction] = {}
            self._saved_view_presets: tuple[SavedViewPreset, ...] = self.service.saved_view_presets()
            self._dismissed_intro = False
            self._artifact_note = ""
            self._action_hint = ""
            self._suppress_best_artifact_record = False
            self._session_activity: dict[str, list[ActivityEntry]] = {}
            self._seen_repo_messages: dict[str, set[str]] = {}
            self._seen_command_ids: dict[str, set[str]] = {}
            self._recent_picker_actions: list[UIAction] = []

        def compose(self) -> ComposeResult:
            yield Header(show_clock=True)
            yield Static(id="summary-bar")
            with Horizontal(id="body"):
                with Vertical(id="repo-pane"):
                    yield Static(id="repo-label")
                    yield DataTable(id="repo-table")
                with Vertical(id="run-pane"):
                    yield Static(id="run-label")
                    with Vertical(id="filter-pane"):
                        with Horizontal(id="saved-views"):
                            for preset_id, label in (
                                ("saved-view-all", "All"),
                                ("saved-view-failures", "Failures"),
                                ("saved-view-queued", "Queued"),
                                ("saved-view-capability", "Capability"),
                                ("saved-view-recent24h", "Recent 24h"),
                                ("saved-view-long-running", "Long-running"),
                            ):
                                yield Button(label, id=preset_id, classes="saved-view")
                        with Horizontal(id="filter-bar"):
                            yield Button("Failed", id="filter-failed", classes="filter-chip")
                            yield Button("Queued", id="filter-queued", classes="filter-chip")
                            yield Button("Capability", id="filter-capability", classes="filter-chip")
                            yield Button("Last 24h", id="filter-last24h", classes="filter-chip")
                            yield Input(id="filter-text", placeholder="Refine visible runs")
                    yield DataTable(id="run-table")
                with Vertical(id="detail-pane"):
                    yield Static(id="intro-card")
                    with Horizontal(id="intro-actions"):
                        yield Button("Open Help (?)", id="intro-help")
                        yield Button("Focus Chat", id="intro-chat")
                        yield Button("Show Failures", id="intro-failures")
                        yield Button("Dismiss", id="intro-dismiss")
                    yield Static(id="target-card")
                    yield Static(id="alert-banner")
                    with Horizontal(id="alert-actions"):
                        for index in range(3):
                            yield Button("", id=f"alert-action-{index}", classes="alert-action")
                    yield Static(id="timeline-strip")
                    with Vertical(id="action-rail"):
                        for row_index in range(3):
                            with Horizontal(id=f"action-row-{row_index}", classes="action-row"):
                                for col_index in range(4):
                                    slot = row_index * 4 + col_index
                                    yield Button("", id=f"action-slot-{slot}", classes="action-slot")
                    yield Static(id="action-hint")
                    yield Static(id="artifact-note")
                    yield Static(id="recent-activity")
                    with TabbedContent(id="detail-tabs"):
                        with TabPane("Chat", id="tab-chat"):
                            with Vertical(id="chat-pane"):
                                yield Static(id="chat-banner")
                                yield Static(id="chat-history")
                                with Horizontal(id="chat-followups"):
                                    for index in range(4):
                                        yield Button(
                                            "",
                                            id=f"chat-followup-{index}",
                                            classes="chat-followup",
                                        )
                                yield Input(id="chat-input", placeholder="Ask about runs, or type /new ...")
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
                        with TabPane("Help", id="tab-help"):
                            yield Static(id="help-text")
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
            self._update_focus_state()

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
            if repo is None or not self._run_keys:
                return None
            run_id = self.selected_run_id if self.selected_run_id in self._run_keys else self._run_keys[0]
            for row in repo.runs:
                if row.run_id == run_id:
                    return row
            return None

        def _focused_area_label(self) -> str:
            focused = self.focused
            if isinstance(focused, DataTable):
                if focused.id == "repo-table":
                    return "repo list"
                if focused.id == "run-table":
                    return "run list"
            if focused is not None and getattr(focused, "id", "") == "filter-text":
                return "filter text"
            if focused is not None and getattr(focused, "id", "") == "chat-input":
                return "chat input"
            if focused is not None and getattr(focused, "id", "") == "command-input":
                return "raw command input"
            if focused is not None and getattr(focused, "id", "") == "detail-tabs":
                return "detail tabs"
            return "workspace"

        def _display_action_label(self, action: UIAction) -> str:
            return (
                f"{action.label} ({action.shortcut_label})"
                if action.shortcut_label
                else action.label
            )

        def _onboarding_text(self, repo: RepoSnapshot | None, run: RunRow | None) -> str:
            if repo is None:
                return (
                    "Getting Started\n"
                    "No repos are available yet.\n"
                    "Add a repo to the control-center config, then relaunch the app."
                )
            if not repo.runs:
                return (
                    "Getting Started\n"
                    f"{repo.repo.name} has no runs yet.\n"
                    "Open Chat to draft a run or wait for the orchestrator to create one."
                )
            if run is None or not self._run_keys:
                return (
                    "Getting Started\n"
                    "No visible run is selected.\n"
                    "Use a saved view or clear filters to bring a run back into view."
                )
            return (
                "Getting Started\n"
                "1. Pick a repo or saved view.\n"
                "2. Scan alerts and the timeline.\n"
                "3. Use Open Best Artifact or Chat for the next step."
            )

        def _show_intro_card(self, repo: RepoSnapshot | None, run: RunRow | None) -> bool:
            if repo is None or not repo.runs or run is None or not self._run_keys:
                return True
            return not self._dismissed_intro

        def _record_activity(
            self,
            repo_id: str,
            *,
            state: str,
            target: str,
            message: str,
            ts_ms: int | None = None,
        ) -> None:
            entries = self._session_activity.setdefault(repo_id, [])
            entries.append(
                ActivityEntry(
                    ts_ms=ts_ms or int(time.time() * 1000),
                    state=state,
                    target=target,
                    message=message,
                    repo_id=repo_id,
                )
            )
            self._session_activity[repo_id] = entries[-40:]

        def _sync_recent_activity(self) -> None:
            for repo in self.snapshot.repos:
                seen_messages = self._seen_repo_messages.setdefault(repo.repo.id, set())
                for message in repo.recent_messages:
                    if message in seen_messages:
                        continue
                    seen_messages.add(message)
                    self._record_activity(
                        repo.repo.id,
                        state="INFO",
                        target=repo.repo.name,
                        message=message,
                    )

                seen_commands = self._seen_command_ids.setdefault(repo.repo.id, set())
                for command in repo.active_commands:
                    command_key = (
                        f"{command.command_id}:{command.state}:{command.completed_epoch_ms or 0}"
                    )
                    if command_key in seen_commands:
                        continue
                    seen_commands.add(command_key)
                    self._record_activity(
                        repo.repo.id,
                        state="RUNNING" if command.state == "running" else command.state.upper(),
                        target=repo.repo.name,
                        message=command.label,
                        ts_ms=command.completed_epoch_ms or command.started_epoch_ms,
                    )

        def _recent_activity_text(self, repo_id: str) -> str:
            entries = sorted(
                self._session_activity.get(repo_id, ()),
                key=lambda item: item.ts_ms,
                reverse=True,
            )
            deduped: list[ActivityEntry] = []
            for entry in entries:
                if deduped and (
                    deduped[-1].message == entry.message
                    and deduped[-1].state == entry.state
                    and deduped[-1].target == entry.target
                ):
                    continue
                deduped.append(entry)
                if len(deduped) >= 5:
                    break
            if not deduped:
                return "Recent Activity\nNo recent actions in this session."
            lines = ["Recent Activity"]
            for entry in deduped:
                lines.append(
                    f"{format_timestamp_ms(entry.ts_ms)} | {entry.state} | {entry.target} | {entry.message}"
                )
            return "\n".join(lines)

        def _saved_view_action(self, preset: SavedViewPreset) -> UIAction:
            return UIAction(
                id=f"saved-view-{preset.id}",
                label=preset.label,
                kind="preset",
                scope="view",
                command_text=f"saved-view {preset.id}",
                hint=preset.description,
                group="saved-views",
            )

        def _update_saved_views(self) -> None:
            for preset in self._saved_view_presets:
                button = self.query_one(f"#saved-view-{preset.id}", Button)
                button.variant = (
                    "primary"
                    if self.service.saved_view_matches(preset.id, self.filter_state, self.run_sort)
                    else "default"
                )

        def _update_focus_state(self) -> None:
            focus_groups = {
                "repo-pane": False,
                "filter-pane": False,
                "run-pane": False,
                "detail-pane": False,
            }
            focused = self.focused
            focused_id = getattr(focused, "id", "")
            if focused_id == "repo-table":
                focus_groups["repo-pane"] = True
            elif focused_id in {
                "filter-text",
                "filter-failed",
                "filter-queued",
                "filter-capability",
                "filter-last24h",
                "saved-view-all",
                "saved-view-failures",
                "saved-view-queued",
                "saved-view-capability",
                "saved-view-recent24h",
                "saved-view-long-running",
            }:
                focus_groups["filter-pane"] = True
            elif focused_id == "run-table":
                focus_groups["run-pane"] = True
            elif focused is not None:
                focus_groups["detail-pane"] = True
            for container_id, is_focused in focus_groups.items():
                try:
                    container = self.query_one(f"#{container_id}")
                except NoMatches:
                    continue
                container.set_class(is_focused, "is-focused")
                container.set_class(is_focused, "is-active")

        def _alert_actions(self, alerts: tuple[AlertBadge, ...]) -> tuple[UIAction, ...]:
            severity_order = {"critical": 0, "warning": 1, "info": 2, "success": 3}
            seen: set[str] = set()
            actions: list[UIAction] = []
            ordered_alerts = sorted(
                alerts,
                key=lambda alert: (severity_order.get(alert.severity, 4), alert.label.lower()),
            )
            for alert in ordered_alerts:
                if alert.action is None or alert.action.id in seen:
                    continue
                seen.add(alert.action.id)
                actions.append(alert.action)
                if len(actions) >= 3:
                    break
            return tuple(actions)

        def _update_alert_actions(self, alerts: tuple[AlertBadge, ...]) -> None:
            self._current_alert_actions.clear()
            actions = self._alert_actions(alerts)
            for index in range(3):
                button = self.query_one(f"#alert-action-{index}", Button)
                if index >= len(actions):
                    button.label = ""
                    button.disabled = True
                    button.styles.display = "none"
                    continue
                action = actions[index]
                button.label = self._display_action_label(action)
                button.disabled = not action.enabled
                button.styles.display = "block"
                self._current_alert_actions[button.id] = action

        def _update_action_hint(self, actions: tuple[UIAction, ...]) -> None:
            focused_id = getattr(self.focused, "id", "")
            action = self._current_action_slots.get(focused_id) or self._current_alert_actions.get(focused_id)
            if action is None:
                action = next((candidate for candidate in actions if candidate.enabled), None)
            hint = action.hint if action is not None else "Focus an action to see a quick description."
            self._action_hint = hint
            try:
                self.query_one("#action-hint", Static).update(f"Action Hint: {hint}")
            except NoMatches:
                return

        def _sort_label(self, state: SortState) -> str:
            return f"{state.key}{' desc' if state.reverse else ' asc'}"

        def _filter_label(self) -> str:
            labels: list[str] = []
            if self.filter_state.failed_only:
                labels.append("failed")
            if self.filter_state.queued_only:
                labels.append("queued")
            if self.filter_state.capability_only:
                labels.append("capability")
            if self.filter_state.last_24h_only:
                labels.append("24h")
            if self.filter_state.text:
                labels.append(self.filter_state.text)
            return ", ".join(labels) if labels else "(none)"

        def _current_view_state(self) -> RepoViewState:
            enabled = tuple(
                kind for kind, is_enabled in self.follow_mode.items() if is_enabled
            )
            return RepoViewState(
                active_tab=self._detail_tab().active,
                overview_preview=self.overview_preview,
                follow_streams=enabled,
            )

        def _remember_repo_view(self, repo_id: str) -> None:
            if not repo_id:
                return
            self._repo_views[repo_id] = self._current_view_state()
            if self.selected_run_id:
                self.selected_run_ids[repo_id] = self.selected_run_id

        def _restore_repo_view(self, repo_id: str) -> None:
            state = self._repo_views.get(repo_id)
            restored_tab = state.active_tab if state is not None else "tab-chat"
            self.overview_preview = state.overview_preview if state is not None else "manifest"
            enabled_follows = set(state.follow_streams if state is not None else ())
            self.follow_mode = {kind: kind in enabled_follows for kind in self.follow_mode}
            run = self._selected_run()
            if run is not None and restored_tab == "tab-patch" and not self.service.run_has_patch(repo_id, run.run_id):
                restored_tab = self.service.recommended_artifact_tab(repo_id, run.run_id)
            self._set_active_tab(restored_tab)

        def _populate_repo_table(self) -> None:
            table = self.query_one("#repo-table", DataTable)
            table.clear(columns=False)
            self._repo_keys = []
            for repo in self.service.sort_repos(self.snapshot.repos, self.repo_sort):
                self._repo_keys.append(repo.repo.id)
                table.add_row(
                    f"{self.service.repo_health_badge(repo.repo.id)} {repo.repo.name}",
                    repo.orchestrator.state,
                    f"{self.service.repo_queue_badge(repo.repo.id)} {repo.queue_depth}",
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
            desired_run_id = self.selected_run_ids.get(repo.repo.id, self.selected_run_id)
            filtered_runs = self.service.filter_runs(repo.runs, self.filter_state)
            for run in self.service.sort_runs(filtered_runs, self.run_sort):
                self._run_keys.append(run.run_id)
                table.add_row(
                    f"{self.service.run_state_badge(repo.repo.id, run.run_id)} {run.run_id}",
                    f"{self.service.run_queue_badge(repo.repo.id, run.run_id)} {run.state}",
                    "-" if run.overall_pass is None else ("yes" if run.overall_pass else "no"),
                    run.execution_profile or "-",
                    run.primary_error_code or "-",
                    render_duration_ms(run.duration_ms),
                    render_duration_ms(run.queue_wait_ms),
                )
            if desired_run_id in self._run_keys:
                self.selected_run_id = desired_run_id
            elif self._run_keys:
                self.selected_run_id = self._run_keys[0]
            else:
                self.selected_run_id = ""
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
                repo = self._selected_repo()
                run = self._selected_run()
                if repo is not None and run is not None:
                    self._set_active_tab(self.service.recommended_artifact_tab(repo.repo.id, run.run_id))
                else:
                    self._set_active_tab("tab-overview")

        def _update_filter_bar(self) -> None:
            button_states = {
                "filter-failed": self.filter_state.failed_only,
                "filter-queued": self.filter_state.queued_only,
                "filter-capability": self.filter_state.capability_only,
                "filter-last24h": self.filter_state.last_24h_only,
            }
            for button_id, enabled in button_states.items():
                button = self.query_one(f"#{button_id}", Button)
                button.variant = "primary" if enabled else "default"
            filter_input = self.query_one("#filter-text", Input)
            if filter_input.value != self.filter_state.text:
                filter_input.value = self.filter_state.text

        def _update_context_labels(self) -> None:
            repo = self._selected_repo()
            selected_run = self._selected_run()
            repo_label = (
                f"Repos | selected: {repo.repo.name if repo else '-'} | "
                f"visible: {len(self._repo_keys)}/{len(self.snapshot.repos)} | "
                f"sort: {self._sort_label(self.repo_sort)}"
            )
            run_label = (
                f"Runs | repo: {repo.repo.name if repo else '-'} | "
                f"selected: {selected_run.run_id if selected_run else 'none'} | "
                f"visible: {len(self._run_keys)}/{len(repo.runs) if repo else 0} | "
                f"sort: {self._sort_label(self.run_sort)} | "
                f"filter: {self._filter_label()}"
            )
            self.query_one("#repo-label", Static).update(repo_label)
            self.query_one("#run-label", Static).update(run_label)

        def _chat_banner_text(self, repo: RepoSnapshot, run: RunRow | None) -> str:
            lines = [self.service.chat_banner_text(repo.repo.id)]
            if run is None:
                lines.append("No visible run is selected. Pick a run or adjust the visible filters.")
            else:
                lines.append(
                    f"Selected run: {run.run_id} | state={run.state} | "
                    f"pass={'pass' if run.overall_pass is True else 'fail' if run.overall_pass is False else 'pending'} | "
                    f"profile={run.execution_profile or '-'}"
                )
            lines.append(
                "Try: show failed runs | queue depth | current run status | /new fix flaky login"
            )
            return "\n".join(lines)

        def _empty_run_message(self, repo: RepoSnapshot) -> str:
            if not repo.runs:
                return (
                    "No runs are available for this repo yet.\n\n"
                    "Use the Chat tab to draft a run, or wait for the orchestrator to create one."
                )
            return (
                f"No runs match the current filter: {self._filter_label()}\n\n"
                "Toggle a filter chip off or clear the text refinement to widen the list."
            )

        def _help_text(self) -> str:
            repo = self._selected_repo()
            run = self._selected_run()
            enabled_follow = ", ".join(
                kind for kind, enabled in self.follow_mode.items() if enabled
            ) or "none"
            lines = [
                "Quick guide",
                "=" * 11,
                f"Focus: {self._focused_area_label()}",
                f"Selected repo: {repo.repo.name if repo else '-'}",
                f"Selected run: {run.run_id if run else '-'}",
                f"Run filter: {self._filter_label()}",
                f"Repo sort: {self._sort_label(self.repo_sort)}",
                f"Run sort: {self._sort_label(self.run_sort)}",
                f"Follow mode: {enabled_follow}",
                "",
                "Keyboard",
                "=" * 8,
                "tab: cycle repo list, filter text, run list, and detail tabs",
                "/: focus the guided filter text input",
                ": open the searchable command picker",
                "o: open the most useful artifact for the selected run",
                "f: toggle follow on Events, Transcript, or Patch",
                "a / R / y: archive, restore, or run the repo runtime check",
                "?: open this Help tab",
                "",
                "Filters",
                "=" * 7,
                "Use the visible chips for Failed, Queued, Capability, and Last 24h.",
                "The trailing text field refines the visible list with plain text matching.",
                "",
                "Chat",
                "=" * 4,
                "Latest assistant replies can expose follow-up buttons for the next useful action.",
            ]
            return "\n".join(lines)

        def _render_target_card(self, summary: TargetSummary) -> str:
            safe_actions = ", ".join(action.label for action in summary.recommended_actions) or "-"
            return "\n".join(
                [
                    f"Target: {summary.repo_name} ({summary.repo_id})",
                    f"Run: {summary.run_id}",
                    (
                        f"State: {summary.run_state} | Pass: {summary.pass_label} | "
                        f"Profile: {summary.profile} | Updated: {summary.age_text}"
                    ),
                    f"Safe next actions: {safe_actions}",
                ]
            )

        def _render_alerts(self, alerts: tuple[AlertBadge, ...]) -> str:
            if not alerts:
                return "Alerts: none"
            lines = ["Alerts"]
            for alert in alerts[:6]:
                detail = f" - {alert.detail}" if alert.detail else ""
                lines.append(f"[{alert.severity.upper()}] {alert.label}{detail}")
            return "\n".join(lines)

        def _render_timeline(self, steps: tuple[TimelineStep, ...]) -> str:
            markers = {
                "done": "[x]",
                "current": "[>]",
                "upcoming": "[ ]",
                "problem": "[!]",
            }
            return " -> ".join(
                f"{markers.get(step.status, '[ ]')} {step.label}" for step in steps
            )

        def _update_action_rail(self, actions: tuple[UIAction, ...]) -> None:
            self._current_action_slots.clear()
            flat_actions = list(actions[:12])
            for slot in range(12):
                button = self.query_one(f"#action-slot-{slot}", Button)
                if slot >= len(flat_actions):
                    button.label = ""
                    button.disabled = True
                    button.styles.display = "none"
                    continue
                action = flat_actions[slot]
                button.label = self._display_action_label(action)
                button.disabled = not action.enabled
                button.variant = "warning" if action.requires_confirmation else "default"
                button.styles.display = "block"
                self._current_action_slots[button.id] = action
            self._update_action_hint(actions)

        def _update_chat_followups(self, repo_id: str) -> None:
            self._current_chat_followups.clear()
            actions = self.service.chat_follow_up_actions(repo_id)[:4]
            for index in range(4):
                button = self.query_one(f"#chat-followup-{index}", Button)
                if index >= len(actions):
                    button.label = ""
                    button.disabled = True
                    button.styles.display = "none"
                    continue
                action = actions[index]
                button.label = self._display_action_label(action)
                button.disabled = not action.enabled
                button.styles.display = "block"
                self._current_chat_followups[button.id] = action

        def _update_stream_widget(self, widget_id: str, content: str, *, follow: bool) -> None:
            widget = self.query_one(widget_id, Static)
            widget.update(content)
            if follow:
                widget.scroll_end(animate=False)

        def _update_detail(self, *, force_streams: bool = False) -> None:
            repo = self._selected_repo()
            run = self._selected_run()
            intro_card = self.query_one("#intro-card", Static)
            chat_banner = self.query_one("#chat-banner", Static)
            chat_history = self.query_one("#chat-history", Static)
            health = self.query_one("#health-text", Static)
            overview = self.query_one("#overview-text", Static)
            events = self.query_one("#events-text", Static)
            transcript = self.query_one("#transcript-text", Static)
            score = self.query_one("#score-text", Static)
            patch = self.query_one("#patch-text", Static)
            help_text = self.query_one("#help-text", Static)
            target_card = self.query_one("#target-card", Static)
            alert_banner = self.query_one("#alert-banner", Static)
            timeline_strip = self.query_one("#timeline-strip", Static)
            recent_activity = self.query_one("#recent-activity", Static)
            artifact_note = self.query_one("#artifact-note", Static)

            if repo is None:
                for widget in (
                    intro_card,
                    chat_banner,
                    chat_history,
                    health,
                    overview,
                    events,
                    transcript,
                    score,
                    patch,
                    help_text,
                    target_card,
                    alert_banner,
                    timeline_strip,
                    recent_activity,
                    artifact_note,
                ):
                    widget.update("No repo selected.")
                self._update_action_rail(())
                self._update_alert_actions(())
                return

            intro_card.update(self._onboarding_text(repo, run))
            intro_visible = self._show_intro_card(repo, run)
            intro_card.styles.display = "block" if intro_visible else "none"
            self.query_one("#intro-actions").styles.display = "block" if intro_visible else "none"
            chat_banner.update(self._chat_banner_text(repo, run))
            chat_history.update(self.service.chat_history_text(repo.repo.id))
            self._update_chat_followups(repo.repo.id)
            health.update(self.service.repo_health_text(repo.repo.id))
            help_text.update(self._help_text())
            recent_activity.update(self._recent_activity_text(repo.repo.id))
            artifact_note.update(self._artifact_note or "Best artifact note: waiting for a recommendation.")

            if run is None:
                empty_message = self._empty_run_message(repo)
                target_card.update(
                    "\n".join(
                        [
                            f"Target: {repo.repo.name} ({repo.repo.id})",
                            "Run: none visible",
                            f"Filter: {self._filter_label()}",
                        ]
                    )
                )
                repo_alerts = self.service.build_repo_alerts(repo.repo.id)
                alert_banner.update(self._render_alerts(repo_alerts))
                self._update_alert_actions(repo_alerts)
                timeline_strip.update("No run selected.")
                self._update_action_rail(())
                for widget in (overview, events, transcript, score, patch):
                    widget.update(empty_message)
                return

            selection_key = f"{repo.repo.id}:{run.run_id}"
            selection_changed = selection_key != self._detail_selection_key
            if selection_changed:
                self._artifact_note = ""
            summary = self.service.build_target_summary(repo.repo.id, run.run_id)
            actions = self.service.build_context_actions(repo.repo.id, run.run_id)
            alerts = self.service.build_repo_alerts(repo.repo.id) + self.service.build_run_alerts(
                repo.repo.id, run.run_id
            )
            timeline = self.service.build_run_timeline(repo.repo.id, run.run_id)
            active_tab = self._detail_tab().active
            has_patch = self.service.run_has_patch(repo.repo.id, run.run_id)

            target_card.update(self._render_target_card(summary))
            alert_banner.update(self._render_alerts(alerts))
            self._update_alert_actions(alerts)
            timeline_strip.update(self._render_timeline(timeline))
            self._update_action_rail(actions)
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
                "\n".join(
                    [
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
                            ]
                        ),
                        " | ".join(
                            [
                                f"Focus {self._focused_area_label()}",
                                f"Repo {self._selected_repo().repo.id if self._selected_repo() else '-'}",
                                f"Run {self._selected_run().run_id if self._selected_run() else '-'}",
                                f"Filter {self._filter_label()}",
                                "/ filter",
                                ": picker",
                                "o best",
                                "? help",
                                "q quit",
                            ]
                        ),
                    ]
                )
            )

        def _default_status_text(self) -> str:
            repo = self._selected_repo()
            run = self._selected_run()
            return (
                f"Ready | Focus {self._focused_area_label()} | "
                f"Repo {repo.repo.name if repo else '-'} | "
                f"Run {run.run_id if run else '-'} | "
                "Use : for commands or the action rail for common tasks"
            )

        def _sync_status_from_repo(self) -> None:
            if time.time() < self._status_hold_until:
                return
            repo = self._selected_repo()
            if repo is None or not repo.recent_messages:
                self._set_status(self._default_status_text())
                return
            latest_message = repo.recent_messages[-1]
            if latest_message != self._last_repo_message:
                self._last_repo_message = latest_message
                self._set_status(latest_message)
                return
            self._set_status(self._default_status_text())

        def refresh_data(self) -> None:
            self.snapshot = self.service.refresh()
            self._sync_recent_activity()
            if not self.selected_repo_id and self.snapshot.repos:
                self.selected_repo_id = self.snapshot.repos[0].repo.id
            self._populate_repo_table()
            self._populate_run_table()
            self._update_filter_bar()
            self._update_saved_views()
            self._update_context_labels()
            self._update_detail()
            self._update_summary_bar()
            self._sync_status_from_repo()
            self._update_focus_state()

        def _apply_filter_state(self, *, status_message: str | None = None) -> None:
            self._populate_run_table()
            self._update_filter_bar()
            self._update_saved_views()
            self._update_context_labels()
            self._update_detail(force_streams=True)
            self._update_summary_bar()
            if status_message:
                self._set_status(status_message, hold_seconds=2.0)

        def action_open_filter(self) -> None:
            self.query_one("#filter-text", Input).focus()

        def _navigation_actions(self) -> tuple[UIAction, ...]:
            repo = self._selected_repo()
            repo_id = repo.repo.id if repo else ""
            return (
                UIAction(
                    "open-chat",
                    "Open Chat",
                    "open",
                    "navigation",
                    "open chat",
                    open_tab="tab-chat",
                    hint="Jump to the Chat tab for guided control actions.",
                    group="all-actions",
                ),
                UIAction(
                    "open-health",
                    "Open Health",
                    "open",
                    "navigation",
                    "open health",
                    open_tab="tab-health",
                    hint="Open repo health, runtime, and canary details.",
                    group="all-actions",
                ),
                UIAction(
                    "open-help",
                    "Open Help",
                    "open",
                    "navigation",
                    "open help",
                    open_tab="tab-help",
                    hint="Open the quick help tab.",
                    shortcut_label="?",
                    group="all-actions",
                ),
                UIAction(
                    "filter-failed",
                    "Toggle Failed Filter",
                    "filter",
                    "navigation",
                    "filter failed",
                    hint="Toggle the Failed chip in the guided filters.",
                    group="all-actions",
                ),
                UIAction(
                    "filter-queued",
                    "Toggle Queued Filter",
                    "filter",
                    "navigation",
                    "filter queued",
                    hint="Toggle the Queued chip in the guided filters.",
                    group="all-actions",
                ),
                UIAction(
                    "filter-capability",
                    "Toggle Capability Filter",
                    "filter",
                    "navigation",
                    "filter capability",
                    hint="Toggle the Capability chip in the guided filters.",
                    group="all-actions",
                ),
                UIAction(
                    "filter-last24h",
                    "Toggle Last 24h Filter",
                    "filter",
                    "navigation",
                    "filter last24h",
                    hint="Toggle the Last 24h chip in the guided filters.",
                    group="all-actions",
                ),
                UIAction(
                    "clear-filters",
                    "Clear Filters",
                    "filter",
                    "navigation",
                    "filter clear",
                    hint="Clear all guided filters and text refinement.",
                    group="all-actions",
                ),
                UIAction(
                    "focus-newest-failed",
                    "Focus Newest Failed Run",
                    "navigation",
                    "repo",
                    "focus newest-failed",
                    enabled=bool(repo_id and self.service.newest_failed_run_id(repo_id)),
                    disabled_reason="no failed run available",
                    hint="Jump to the newest failed run in the current repo.",
                    group="all-actions",
                ),
                UIAction(
                    "raw-command",
                    "Open Raw Command Prompt...",
                    "raw",
                    "navigation",
                    "__open_raw_command_prompt__",
                    aliases=("raw", "manual"),
                    hint="Use the original raw command input.",
                    group="all-actions",
                ),
            )

        def _picker_results(self) -> tuple[PickerResult, ...]:
            repo = self._selected_repo()
            run = self._selected_run()
            results: list[PickerResult] = []
            row_index = 0

            recommended: list[UIAction] = []
            if repo is not None and run is not None:
                alerts = self.service.build_repo_alerts(repo.repo.id) + self.service.build_run_alerts(
                    repo.repo.id, run.run_id
                )
                recommended.extend(self._alert_actions(alerts))
                recommended.extend(
                    action for action in self.service.build_context_actions(repo.repo.id, run.run_id) if action.enabled
                )
            seen_recommended: set[str] = set()
            for action in recommended:
                if action.id in seen_recommended:
                    continue
                seen_recommended.add(action.id)
                results.append(
                    PickerResult(
                        action=action,
                        section="Recommended Now",
                        priority=0,
                        row_index=row_index,
                    )
                )
                row_index += 1

            for action in self._recent_picker_actions[:5]:
                results.append(
                    PickerResult(
                        action=action,
                        section="Recent Commands",
                        priority=1,
                        row_index=row_index,
                    )
                )
                row_index += 1

            for preset in self._saved_view_presets:
                results.append(
                    PickerResult(
                        action=self._saved_view_action(preset),
                        section="Saved Views",
                        priority=2,
                        row_index=row_index,
                    )
                )
                row_index += 1

            all_actions = list(self._navigation_actions())
            if repo is not None and run is not None:
                all_actions = list(self.service.build_context_actions(repo.repo.id, run.run_id)) + all_actions
            seen_all: set[str] = set()
            for action in all_actions:
                if action.id in seen_all:
                    continue
                seen_all.add(action.id)
                results.append(
                    PickerResult(
                        action=action,
                        section="All Actions",
                        priority=3,
                        row_index=row_index,
                    )
                )
                row_index += 1
            return tuple(results)

        def _remember_picker_action(self, action: UIAction) -> None:
            key = (action.id, action.command_text)
            self._recent_picker_actions = [
                action,
                *[
                    existing
                    for existing in self._recent_picker_actions
                    if (existing.id, existing.command_text) != key
                ],
            ][:5]

        def action_open_command(self) -> None:
            picker_results = self._picker_results()
            self._picker_actions = {result.action.id: result.action for result in picker_results}
            self.push_screen(
                CommandPickerScreen(picker_results),
                callback=self._handle_picker_result,
            )

        def _handle_picker_result(self, action_id: str | None) -> None:
            if not action_id:
                return
            action = self._picker_actions.get(action_id)
            if action is None:
                return
            self._remember_picker_action(action)
            if action.command_text == "__open_raw_command_prompt__":
                self._open_input(
                    "command",
                    "",
                    "archive-run | restore-evidence | runtime-check | sort-runs duration desc",
                )
                return
            self._execute_ui_action(action)

        def action_open_help(self) -> None:
            self._remember_picker_action(
                UIAction(
                    id="open-help",
                    label="Open Help",
                    kind="open",
                    scope="navigation",
                    command_text="open help",
                    open_tab="tab-help",
                    shortcut_label="?",
                )
            )
            self._set_active_tab("tab-help")
            self._update_detail(force_streams=True)
            self._set_status("opened help", hold_seconds=2.0)

        def action_open_best_artifact(self) -> None:
            repo = self._selected_repo()
            run = self._selected_run()
            if repo is None or run is None:
                self._set_status("select a run first", hold_seconds=2.0)
                return
            if self._suppress_best_artifact_record:
                self._suppress_best_artifact_record = False
            else:
                self._record_action_execution(
                    next(
                        action
                        for action in self.service.build_context_actions(repo.repo.id, run.run_id)
                        if action.id == "open-best-artifact"
                    )
                )
            recommendation = self.service.artifact_recommendation(repo.repo.id, run.run_id)
            self._set_active_tab(recommendation.tab_id)
            self._artifact_note = f"Best artifact: {recommendation.reason}"
            self._update_detail(force_streams=True)
            self._set_status(recommendation.reason, hold_seconds=2.0)

        def _open_input(self, mode: str, value: str, placeholder: str) -> None:
            widget = self.query_one("#command-input", Input)
            self.input_mode = mode
            widget.value = value
            widget.placeholder = placeholder
            widget.styles.display = "block"
            widget.focus()

        def action_close_input(self) -> None:
            widget = self.query_one("#command-input", Input)
            widget.styles.display = "none"
            self.input_mode = ""
            self.query_one("#run-table", DataTable).focus()

        def action_cycle_focus(self) -> None:
            focus_order = [
                self.query_one("#repo-table", DataTable),
                self.query_one("#filter-text", Input),
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
            kind = {
                "tab-events": "events",
                "tab-transcript": "transcript",
                "tab-patch": "patch",
            }.get(self._detail_tab().active, "")
            if not kind:
                self._set_status("follow only applies to events, transcript, or patch", hold_seconds=2.0)
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
                self.repo_sort = SortState(keys[(index + 1) % len(keys)], self.repo_sort.reverse)
                self._populate_repo_table()
                self._update_context_labels()
                self._set_status(f"repo sort: {self._sort_label(self.repo_sort)}", hold_seconds=2.0)
                return
            if isinstance(focused, DataTable) and focused.id == "run-table":
                keys = list(RUN_SORT_KEYS)
                index = keys.index(self.run_sort.key)
                self.run_sort = SortState(keys[(index + 1) % len(keys)], self.run_sort.reverse)
                self._populate_run_table()
                self._update_context_labels()
                self._set_status(f"run sort: {self._sort_label(self.run_sort)}", hold_seconds=2.0)
                return
            self._set_status("focus the repo or run table before sorting", hold_seconds=2.0)

        def action_reverse_sort(self) -> None:
            focused = self.focused
            if isinstance(focused, DataTable) and focused.id == "repo-table":
                self.repo_sort = SortState(self.repo_sort.key, not self.repo_sort.reverse)
                self._populate_repo_table()
                self._update_context_labels()
                self._set_status(f"repo sort: {self._sort_label(self.repo_sort)}", hold_seconds=2.0)
                return
            if isinstance(focused, DataTable) and focused.id == "run-table":
                self.run_sort = SortState(self.run_sort.key, not self.run_sort.reverse)
                self._populate_run_table()
                self._update_context_labels()
                self._set_status(f"run sort: {self._sort_label(self.run_sort)}", hold_seconds=2.0)
                return
            self._set_status("focus the repo or run table before reversing sort", hold_seconds=2.0)

        def action_archive_run(self) -> None:
            repo = self._selected_repo()
            run = self._selected_run()
            if repo is None or run is None:
                self._set_status("select a run before archiving", hold_seconds=2.0)
                return
            self._execute_command(f"archive-run {run.run_id}")

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
            self._execute_command(f"runtime-check {repo.repo.id}")

        def action_cursor_down(self) -> None:
            self._move_table_cursor(1)

        def action_cursor_up(self) -> None:
            self._move_table_cursor(-1)

        def _move_table_cursor(self, delta: int) -> None:
            focused = self.focused
            if not isinstance(focused, DataTable):
                return
            if focused.row_count <= 0:
                return
            target = max(0, min(focused.row_count - 1, focused.cursor_row + delta))
            focused.move_cursor(row=target)

        def _toggle_filter(self, name: str) -> None:
            if name == "failed":
                self.filter_state = self.service.build_filter_state(
                    failed_only=not self.filter_state.failed_only,
                    queued_only=self.filter_state.queued_only,
                    capability_only=self.filter_state.capability_only,
                    last_24h_only=self.filter_state.last_24h_only,
                    text=self.filter_state.text,
                )
            elif name == "queued":
                self.filter_state = self.service.build_filter_state(
                    failed_only=self.filter_state.failed_only,
                    queued_only=not self.filter_state.queued_only,
                    capability_only=self.filter_state.capability_only,
                    last_24h_only=self.filter_state.last_24h_only,
                    text=self.filter_state.text,
                )
            elif name == "capability":
                self.filter_state = self.service.build_filter_state(
                    failed_only=self.filter_state.failed_only,
                    queued_only=self.filter_state.queued_only,
                    capability_only=not self.filter_state.capability_only,
                    last_24h_only=self.filter_state.last_24h_only,
                    text=self.filter_state.text,
                )
            elif name == "last24h":
                self.filter_state = self.service.build_filter_state(
                    failed_only=self.filter_state.failed_only,
                    queued_only=self.filter_state.queued_only,
                    capability_only=self.filter_state.capability_only,
                    last_24h_only=not self.filter_state.last_24h_only,
                    text=self.filter_state.text,
                )
            elif name == "clear":
                self.filter_state = self.service.build_filter_state()
            self._apply_filter_state(status_message=f"filter: {self._filter_label()}")

        def _apply_saved_view(self, preset_id: str) -> None:
            preset = self.service.saved_view_preset(preset_id)
            if preset is None:
                self._set_status(f"unknown saved view: {preset_id}", hold_seconds=2.0)
                return
            self.filter_state = preset.filter_state
            self.run_sort = preset.run_sort
            if preset.preferred_tab:
                self._set_active_tab(preset.preferred_tab)
            self._apply_filter_state(status_message=f"view: {preset.label.lower()}")

        def _record_action_execution(self, action: UIAction) -> None:
            repo = self._selected_repo()
            repo_id = repo.repo.id if repo is not None else ""
            if repo_id:
                self._record_activity(
                    repo_id,
                    state="ACTION",
                    target=repo.repo.name,
                    message=action.label,
                )
            self._remember_picker_action(action)

        def _execute_ui_action(self, action: UIAction) -> None:
            if not action.enabled:
                self._set_status(action.disabled_reason or f"{action.label} is unavailable", hold_seconds=2.0)
                return
            if action.requires_confirmation:
                self.push_screen(
                    ConfirmationScreen(action),
                    callback=lambda confirmed: self._confirm_ui_action(action, confirmed),
                )
                return
            self._record_action_execution(action)
            if action.id == "open-best-artifact":
                self._suppress_best_artifact_record = True
            self._execute_command(action.command_text)

        def _confirm_ui_action(self, action: UIAction, confirmed: bool) -> None:
            if not confirmed:
                self._set_status(f"cancelled {action.label.lower()}", hold_seconds=2.0)
                return
            self._record_action_execution(action)
            if action.id == "open-best-artifact":
                self._suppress_best_artifact_record = True
            self._execute_command(action.command_text)

        def _select_run(self, run_id: str, *, open_tab: str | None = None) -> None:
            repo = self._selected_repo()
            if repo is None:
                return
            if run_id not in {row.run_id for row in repo.runs}:
                self._set_status(f"run not found: {run_id}", hold_seconds=2.0)
                return
            self.selected_run_id = run_id
            self.selected_run_ids[repo.repo.id] = run_id
            self._populate_run_table()
            if open_tab is not None:
                self._set_active_tab(open_tab)
            self._update_context_labels()
            self._update_detail(force_streams=True)
            self._update_summary_bar()

        def on_button_pressed(self, event: Button.Pressed) -> None:
            button_id = event.button.id
            if button_id in self._current_action_slots:
                self._execute_ui_action(self._current_action_slots[button_id])
                return
            if button_id in self._current_alert_actions:
                self._execute_ui_action(self._current_alert_actions[button_id])
                return
            if button_id in self._current_chat_followups:
                self._execute_ui_action(self._current_chat_followups[button_id])
                return
            if button_id == "intro-help":
                self.action_open_help()
            elif button_id == "intro-chat":
                self._set_active_tab("tab-chat")
                self.query_one("#chat-input", Input).focus()
                self._update_detail(force_streams=True)
            elif button_id == "intro-failures":
                self._apply_saved_view("failures")
            elif button_id == "intro-dismiss":
                self._dismissed_intro = True
                self._update_detail(force_streams=True)
            elif button_id == "filter-failed":
                self._toggle_filter("failed")
            elif button_id == "filter-queued":
                self._toggle_filter("queued")
            elif button_id == "filter-capability":
                self._toggle_filter("capability")
            elif button_id == "filter-last24h":
                self._toggle_filter("last24h")
            elif button_id.startswith("saved-view-"):
                self._apply_saved_view(button_id.removeprefix("saved-view-"))
            self._update_focus_state()

        def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
            if event.data_table.id == "repo-table" and 0 <= event.cursor_row < len(self._repo_keys):
                next_repo_id = self._repo_keys[event.cursor_row]
                if next_repo_id != self.selected_repo_id:
                    self._remember_repo_view(self.selected_repo_id)
                    self.selected_repo_id = next_repo_id
                    self.selected_run_id = self.selected_run_ids.get(next_repo_id, "")
                    self._populate_run_table()
                    self._restore_repo_view(next_repo_id)
                    self._update_filter_bar()
                    self._update_context_labels()
                    self._update_detail(force_streams=True)
                    self._update_summary_bar()
            elif event.data_table.id == "run-table" and 0 <= event.cursor_row < len(self._run_keys):
                self.selected_run_id = self._run_keys[event.cursor_row]
                repo = self._selected_repo()
                if repo is not None:
                    self.selected_run_ids[repo.repo.id] = self.selected_run_id
                self._update_context_labels()
                self._update_detail()
                self._update_summary_bar()

        def on_input_changed(self, event: Input.Changed) -> None:
            if event.input.id != "filter-text":
                return
            self.filter_state = self.service.build_filter_state(
                failed_only=self.filter_state.failed_only,
                queued_only=self.filter_state.queued_only,
                capability_only=self.filter_state.capability_only,
                last_24h_only=self.filter_state.last_24h_only,
                text=event.value,
            )
            self._apply_filter_state()

        def on_key(self, event) -> None:
            focused = self.focused
            if (
                event.character == "?"
                and isinstance(focused, Input)
                and not focused.value
                and focused.id in {"chat-input", "filter-text", "command-input", "picker-query"}
            ):
                event.prevent_default()
                event.stop()
                self.action_open_help()

        def on_focus(self, event) -> None:
            self._update_focus_state()
            self._update_action_hint(tuple(self._current_action_slots.values()))

        def on_blur(self, event) -> None:
            self._update_focus_state()

        def on_descendant_focus(self, event) -> None:
            self._update_focus_state()
            self._update_action_hint(tuple(self._current_action_slots.values()))

        def on_descendant_blur(self, event) -> None:
            self._update_focus_state()

        def _handle_chat_result(self, result) -> None:
            if result.focus_run_id:
                self.selected_run_id = result.focus_run_id
                repo = self._selected_repo()
                if repo is not None:
                    self.selected_run_ids[repo.repo.id] = result.focus_run_id
            if result.open_tab:
                self._set_active_tab(result.open_tab)
            self._set_status(result.reply, hold_seconds=3.0)
            self.refresh_data()

        def on_input_submitted(self, event: Input.Submitted) -> None:
            value = event.value.strip()
            if event.input.id == "chat-input":
                repo = self._selected_repo()
                if repo is None:
                    self._set_status("select a repo before using chat", hold_seconds=2.0)
                    return
                result = self.service.submit_chat_message(
                    repo.repo.id,
                    value,
                    selected_run_id=self.selected_run_id,
                )
                event.input.value = ""
                self._handle_chat_result(result)
                return
            if event.input.id == "filter-text":
                self.query_one("#run-table", DataTable).focus()
                return

            widget = self.query_one("#command-input", Input)
            widget.styles.display = "none"
            mode = self.input_mode
            self.input_mode = ""
            if mode == "command":
                action = UIAction(
                    id=f"raw:{value}",
                    label=value,
                    kind="raw",
                    scope="navigation",
                    command_text=value,
                    hint="Repeat the same raw command later from the picker.",
                    group="recent",
                )
                self._record_action_execution(action)
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
            if force:
                action = UIAction(
                    id="restore-force",
                    label=f"Restore evidence for `{run_id or 'selected run'}`",
                    kind="service",
                    scope="run",
                    command_text=" ".join(tokens),
                    requires_confirmation=True,
                )
                self._execute_ui_action(action)
                return
            self._set_status(
                self.service.restore_evidence(repo_id, run_id, archive_path=archive_path, force=force),
                hold_seconds=3.0,
            )

        def _execute_special_command(self, tokens: list[str]) -> bool:
            repo = self._selected_repo()
            run = self._selected_run()
            if tokens[0] == "open-best-artifact":
                self.action_open_best_artifact()
                return True
            if tokens[0] == "filter":
                target = tokens[1] if len(tokens) > 1 else ""
                if target in {"failed", "queued", "capability", "last24h"}:
                    self._toggle_filter(target)
                elif target == "clear":
                    self._toggle_filter("clear")
                else:
                    self._set_status(f"unknown filter target: {target or '-'}", hold_seconds=2.0)
                return True
            if tokens[0] == "saved-view":
                preset_id = tokens[1] if len(tokens) > 1 else ""
                self._apply_saved_view(preset_id)
                return True
            if tokens[:2] == ["focus", "newest-failed"]:
                if repo is None:
                    self._set_status("select a repo first", hold_seconds=2.0)
                    return True
                run_id = self.service.newest_failed_run_id(repo.repo.id)
                if not run_id:
                    self._set_status("no failed run available", hold_seconds=2.0)
                    return True
                recommendation = self.service.artifact_recommendation(repo.repo.id, run_id)
                self._select_run(
                    run_id,
                    open_tab=recommendation.tab_id,
                )
                self._set_status(f"focused {run_id}", hold_seconds=2.0)
                return True
            if tokens[:3] == ["open", "score", "newest-failed"]:
                if repo is None:
                    self._set_status("select a repo first", hold_seconds=2.0)
                    return True
                run_id = self.service.newest_failed_run_id(repo.repo.id)
                if not run_id:
                    self._set_status("no failed run available", hold_seconds=2.0)
                    return True
                self._select_run(run_id, open_tab="tab-score")
                self._set_status("opened score", hold_seconds=2.0)
                return True
            if tokens[0] == "repo-view-memory":
                if repo is not None:
                    self._remember_repo_view(repo.repo.id)
                    self._set_status("saved repo view", hold_seconds=2.0)
                return True
            return False

        def _execute_command(self, command: str) -> None:
            try:
                tokens = shlex.split(command)
            except ValueError as exc:
                self._set_status(f"command parse error: {exc}", hold_seconds=2.0)
                return
            if not tokens:
                self._set_status("no command entered", hold_seconds=2.0)
                return

            if self._execute_special_command(tokens):
                self.refresh_data()
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
                    self._set_status(self.service.open_archive_path(repo_id, run_id), hold_seconds=5.0)
                elif tokens[0] == "sort-runs":
                    self._parse_sort_command(tokens, repo_scope=False)
                elif tokens[0] == "sort-repos":
                    self._parse_sort_command(tokens, repo_scope=True)
                elif tokens[0] == "toggle-follow":
                    target = tokens[1] if len(tokens) > 1 else _tab_title(self._detail_tab().active)
                    if target not in self.follow_mode:
                        self._set_status(f"unknown follow target: {target or '-'}", hold_seconds=2.0)
                    else:
                        self.follow_mode[target] = not self.follow_mode[target]
                        self._update_detail(force_streams=True)
                        self._set_status(
                            f"{target} follow {'enabled' if self.follow_mode[target] else 'disabled'}",
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
                        self._set_status(self.service.enqueue_run(repo_id, run_id), hold_seconds=3.0)
                    elif action == "rerun":
                        self._set_status(self.service.rerun_run(repo_id, run_id), hold_seconds=3.0)
                    else:
                        self._set_status(f"unknown run action: {action}", hold_seconds=2.0)
                elif tokens[0] == "open":
                    target = tokens[1] if len(tokens) > 1 else "manifest"
                    if target == "chat":
                        self._set_active_tab("tab-chat")
                    elif target == "health":
                        self._set_active_tab("tab-health")
                    elif target == "events":
                        self._set_active_tab("tab-events")
                    elif target == "transcript":
                        self._set_active_tab("tab-transcript")
                    elif target == "score":
                        self._set_active_tab("tab-score")
                    elif target == "patch":
                        self._set_active_tab("tab-patch")
                    elif target == "help":
                        self._set_active_tab("tab-help")
                    else:
                        self.overview_preview = "manifest"
                        self._set_active_tab("tab-overview")
                    self._update_detail(force_streams=True)
                    self._set_status(f"opened {target}", hold_seconds=2.0)
                else:
                    self._set_status(f"unknown command: {tokens[0]}", hold_seconds=2.0)
            except Exception as exc:  # pragma: no cover - surfaced to operator
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
