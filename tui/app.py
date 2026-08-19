"""Textual application: live pipeline, logs, knowledge, and RCA."""

from __future__ import annotations

import argparse
from uuid import uuid4

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    RichLog,
    Static,
)
from textual.worker import Worker, WorkerState

from nexgen_shared.schemas import RCAReport

from tui.client import NexGenClient
from tui.runtime import ServiceSupervisor

CSS = """
Screen {
    background: #0b1220;
    color: #e8eef9;
}

Header {
    background: #111b2e;
    color: #7dd3fc;
}

Footer {
    background: #111b2e;
}

#banner {
    height: 5;
    padding: 1 2;
    background: #111b2e;
    color: #7dd3fc;
    text-style: bold;
    border: heavy #1e3a5f;
}

#health-row {
    height: 3;
    padding: 0 1;
}

.health {
    width: 1fr;
    height: 3;
    content-align: center middle;
    border: tall #1e3a5f;
    color: #94a3b8;
}

.health.up {
    color: #86efac;
    border: tall #166534;
}

.health.down {
    color: #fca5a5;
    border: tall #7f1d1d;
}

#body {
    height: 1fr;
}

#pipeline {
    width: 32;
    border: tall #1e3a5f;
    background: #0f172a;
    padding: 1;
}

.stage {
    height: 3;
    padding: 0 1;
    color: #64748b;
}

.stage.active {
    color: #fde68a;
    text-style: bold;
}

.stage.done {
    color: #86efac;
}

#mid {
    width: 1fr;
}

#logs, #knowledge {
    height: 1fr;
    border: tall #1e3a5f;
    background: #0f172a;
}

#rca {
    height: 14;
    border: tall #7c3aed;
    background: #140b24;
    padding: 1;
}

#prompt-row {
    height: 3;
    padding: 0 1;
}

#query-input {
    width: 1fr;
    background: #111b2e;
    border: tall #38bdf8;
}

#go {
    width: 18;
    background: #0e7490;
    color: #ecfeff;
}
"""

BANNER = (
    "NEXGEN  ·  natural-language RCA\n"
    "Master orchestrates  →  Query (KQL / logs)  +  RAG (runbooks)\n"
    "Type a question. The three services talk only over HTTP."
)

STAGES = (
    ("session", "1  session"),
    ("intent", "2  intent"),
    ("planner", "3  DAG plan"),
    ("executor", "4  fetch logs + docs"),
    ("reasoner", "5  reason / validate"),
    ("final", "6  synthesise RCA"),
)


class HealthLamp(Static):
    """Traffic-light for one downstream HTTP service."""

    def set_state(self, name: str, up: bool) -> None:
        """Render ``NAME ● live`` or ``NAME ○ down``.

        Args:
            name: Service label.
            up: Whether ``/health`` succeeded.
        """
        self.set_class(up, "up")
        self.set_class(not up, "down")
        mark = "●" if up else "○"
        mode = "live" if up else "down"
        self.update(f"{name.upper()}  {mark}  {mode}")


class NexGenApp(App[None]):
    """Operator console that drives Master, Query, and RAG together."""

    CSS = CSS
    TITLE = "NexGen"
    BINDINGS = [
        Binding("ctrl+c", "quit", "Quit"),
        Binding("ctrl+l", "focus_input", "Focus prompt"),
    ]

    def __init__(
        self,
        client: NexGenClient | None = None,
        supervisor: ServiceSupervisor | None = None,
    ) -> None:
        super().__init__()
        self.client = client or NexGenClient()
        self.supervisor = supervisor
        self.session_id = str(uuid4())
        self._busy = False

    def compose(self) -> ComposeResult:
        """Build the dashboard layout."""
        yield Header(show_clock=True)
        yield Static(BANNER, id="banner")
        with Horizontal(id="health-row"):
            yield HealthLamp("MASTER  ○  …", id="h-master", classes="health")
            yield HealthLamp("QUERY  ○  …", id="h-query", classes="health")
            yield HealthLamp("RAG  ○  …", id="h-rag", classes="health")
        with Horizontal(id="body"):
            with Vertical(id="pipeline"):
                yield Label("PIPELINE")
                for key, label in STAGES:
                    yield Static(f"○  {label}", id=f"stage-{key}", classes="stage")
            with Vertical(id="mid"):
                yield DataTable(id="logs")
                yield RichLog(id="knowledge", highlight=True, markup=True)
        yield RichLog(id="rca", highlight=True, markup=True)
        with Horizontal(id="prompt-row"):
            yield Input(
                placeholder="Why did payments fail at 09:57?",
                id="query-input",
            )
            yield Button("Investigate", id="go", variant="primary")
        yield Footer()

    def on_mount(self) -> None:
        """Initialise tables and start the health poller."""
        table = self.query_one("#logs", DataTable)
        table.add_columns("time", "service", "level", "message")
        table.cursor_type = "row"
        self.query_one("#knowledge", RichLog).write(
            "[dim]Knowledge chunks will appear here after Query + RAG return.[/]"
        )
        self.query_one("#rca", RichLog).write(
            "[dim]Submit a question to run the full Master → Query → RAG loop.[/]"
        )
        self.set_interval(2.0, self._tick_health)
        self.query_one("#query-input", Input).focus()

    def _tick_health(self) -> None:
        """Schedule an async health probe without blocking the UI thread."""
        self.run_worker(self._poll_health(), exclusive=True, group="health")

    async def _poll_health(self) -> None:
        status = await self.client.health()
        mapping = {
            "master": "#h-master",
            "query": "#h-query",
            "rag": "#h-rag",
        }
        for name, widget_id in mapping.items():
            self.query_one(widget_id, HealthLamp).set_state(name, status.get(name, False))

    def action_focus_input(self) -> None:
        """Move the cursor to the prompt."""
        self.query_one("#query-input", Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Enter in the prompt starts an investigation."""
        self._submit(event.value)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Investigate button starts an investigation."""
        if event.button.id == "go":
            self._submit(self.query_one("#query-input", Input).value)

    def _submit(self, text: str) -> None:
        question = text.strip()
        if not question or self._busy:
            return
        self._busy = True
        self._reset_stages()
        self.query_one("#rca", RichLog).clear()
        self.query_one("#rca", RichLog).write("[yellow]Investigating across Master, Query, and RAG…[/]")
        self.run_worker(self._investigate(question), exclusive=True, thread=False)

    def _reset_stages(self) -> None:
        for key, label in STAGES:
            widget = self.query_one(f"#stage-{key}", Static)
            widget.update(f"○  {label}")
            widget.set_class(False, "active")
            widget.set_class(False, "done")

    def _mark_stage(self, key: str, active: bool = False, done: bool = False) -> None:
        label = dict(STAGES)[key]
        widget = self.query_one(f"#stage-{key}", Static)
        mark = "●" if done or active else "○"
        widget.update(f"{mark}  {label}")
        widget.set_class(active, "active")
        widget.set_class(done, "done")

    async def _investigate(self, question: str) -> RCAReport:
        self._mark_stage("session", active=True)
        report_task = self.client.investigate(question, self.session_id)
        # Drive the pipeline lamps from Master's live trace while HTTP is in flight.
        async def _follow() -> RCAReport:
            import asyncio

            investigate = asyncio.create_task(report_task)
            while not investigate.done():
                qid = self.client.last_query_id
                if qid:
                    try:
                        events = await self.client.trace(qid)
                    except Exception:
                        events = []
                    for event in events:
                        stage = str(event.get("stage", ""))
                        if stage in dict(STAGES):
                            self._mark_stage(stage, done=True)
                await asyncio.sleep(0.2)
            return await investigate

        report = await _follow()
        events = await self.client.trace(report.query_id)
        seen: set[str] = set()
        for event in events:
            stage = str(event.get("stage", ""))
            if stage in dict(STAGES):
                self._mark_stage(stage, done=True)
                seen.add(stage)
        for key, _ in STAGES:
            if key not in seen:
                self._mark_stage(key, done=True)
        self._render_report(report)
        return report

    def _render_report(self, report: RCAReport) -> None:
        table = self.query_one("#logs", DataTable)
        table.clear()
        knowledge = self.query_one("#knowledge", RichLog)
        knowledge.clear()
        rca = self.query_one("#rca", RichLog)
        rca.clear()

        log_items = [e for e in report.evidence if e.type == "log"]
        other = [e for e in report.evidence if e.type != "log"]
        if not log_items:
            table.add_row("—", "—", "—", "No log citations on this RCA")
        for item in log_items:
            table.add_row("cited", item.ref, "LOG", item.snippet or "")

        if other:
            for item in other:
                knowledge.write(
                    f"[bold cyan]{item.type}[/]  [dim]{item.ref}[/]\n{item.snippet or ''}\n"
                )
        else:
            knowledge.write("[dim]No knowledge citations.[/]")

        pct = int(report.confidence * 100)
        bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
        rca.write(f"[bold magenta]RCA[/]   confidence {bar} {report.confidence:.2f}")
        rca.write(f"[white]{report.root_cause_summary}[/]")
        rca.write(f"[dim]{report.reasoning_trace_summary}[/]")
        if report.recommended_actions:
            rca.write("[bold]Actions[/]")
            for action in report.recommended_actions:
                rca.write(f"  → {action}")
        rca.write(f"[dim]query {report.query_id}   MTTR ~{report.mttr_estimate_minutes} min[/]")

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.worker.state in {WorkerState.SUCCESS, WorkerState.ERROR, WorkerState.CANCELLED}:
            self._busy = False
        if event.worker.state == WorkerState.ERROR and event.worker.error:
            rca = self.query_one("#rca", RichLog)
            rca.clear()
            rca.write(f"[red]Investigation failed:[/] {event.worker.error}")

    def on_unmount(self) -> None:
        if self.supervisor is not None:
            self.supervisor.stop()


def run() -> None:
    """Parse CLI flags, optionally spawn the three services, then start Textual."""
    parser = argparse.ArgumentParser(description="NexGen operator TUI")
    parser.add_argument(
        "--attach",
        action="store_true",
        help="Do not spawn services; attach to already-running localhost ports.",
    )
    args = parser.parse_args()
    supervisor: ServiceSupervisor | None = None
    if not args.attach:
        supervisor = ServiceSupervisor()
        supervisor.start()
    app = NexGenApp(supervisor=supervisor)
    app.run()
