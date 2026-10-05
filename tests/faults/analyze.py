"""
Score the fault-injection runs and write tests/faults/RESULTS.md.

Two parts:
  - Live end-to-end: the answer NexGen gave during each run (Master -> Query -> Elasticsearch).
  - Ablation: each run's saved logs replayed offline through the same Master pipeline, in
    rules-only and rules+LLM mode, reporting the top-ranked suspect (before the validator) and
    the final answer. Only the fetch step is replaced (by the snapshot); everything else is real.

    cd master && uv run python ../tests/faults/analyze.py [--llm]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "master"))

from nexgen_shared.schemas import LogHit, LogRetrievalResult, UserQuery  # noqa: E402

from src.orchestrator import MasterOrchestrator  # noqa: E402
from src.reasoner import names  # noqa: E402
from src.settings import Settings  # noqa: E402

HERE = Path(__file__).resolve().parent
RUNS = HERE / "runs"
MAX_LINES = 200  # what Query returns to Master (oldest first)
# Failure kinds, fixed before the runs: "slowness" failures make things slow or leak resources
# rather than throw errors, so they leave few error logs for a log-based tool to find.
SLOWNESS = {"adHighCpu", "adManualGc", "imageSlowLoad", "intlShippingSlowdown",
            "productCatalogLockContention", "emailMemoryLeak"}


class SnapshotExecutor:
    """Stands in for the fetch step: returns the logs saved during the live run."""

    def __init__(self) -> None:
        self.rows: list[list[Any]] = []

    async def execute(self, graph: Any, question: str) -> tuple[LogRetrievalResult, None]:
        hits = [LogHit(timestamp=t, service=s, level=lvl, message=m, trace_id=f"snap-{i}")
                for i, (t, s, lvl, m) in enumerate(self.rows[:MAX_LINES])]
        return LogRetrievalResult(query_id="replay", status="success", kql_generated="(snapshot)",
                                  syntax_valid=True, refinement_attempts=0, hits=hits,
                                  hit_count=len(hits), error=None), None


async def replay(records: list[dict], llm: bool, delay: float) -> dict[str, dict]:
    """Run every saved snapshot through Master; returns run_id -> {top, final}."""
    overrides: dict[str, Any] = {"TOPOLOGY_CONFIG_PATH": str(HERE / "topology.json"),
                                 "REDIS_URL": "redis://localhost:1/0"}
    if not llm:
        overrides["OPENAI_API_KEY"] = ""
    orchestrator = MasterOrchestrator(Settings(**overrides))
    executor = SnapshotExecutor()
    orchestrator.executor = executor  # type: ignore[assignment]
    out = {}
    for i, rec in enumerate(records):
        if llm and i:
            await asyncio.sleep(delay)
        executor.rows = rec["logs"]["problem_lines"]
        run = await orchestrator.run(UserQuery(query_id=rec["run_id"], raw_text=rec["question"],
                                               session_id="replay", timestamp_utc=datetime.now(timezone.utc)))
        out[rec["run_id"]] = {
            "top": run.candidates[0].culprit_service if run.candidates else None,
            "final": run.accepted.culprit_service if run.accepted else None,
        }
    return out


def score(records: list[dict], picks: dict[str, dict], key: str) -> int:
    return sum(picks[r["run_id"]][key] in r["accepted"] for r in records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--llm", action="store_true", help="also replay with the LLM (uses OPENAI_* from master/.env)")
    parser.add_argument("--delay", type=float, default=8.0, help="seconds between LLM replays")
    args = parser.parse_args()

    records = sorted((json.loads(p.read_text()) for p in RUNS.glob("*.json")), key=lambda r: r["run_id"])
    n = len(records)
    rules = asyncio.run(replay(records, llm=False, delay=0))
    llm = asyncio.run(replay(records, llm=True, delay=args.delay)) if args.llm else None

    live_ok = sum(r["correct"] for r in records)
    seconds = [r["seconds"] for r in records]
    p95 = sorted(seconds)[min(n - 1, round(0.95 * (n - 1)))]

    def frac(k: int) -> str:
        return f"{k}/{n} ({100 * k / n:.0f}%)"

    def visible(r: dict) -> bool:
        """Pre-declared: the culprit wrote at least one WARN/ERROR line during the failure."""
        return any(row[1] in r["accepted"] for row in r["logs"]["problem_lines"])

    seen_in_logs = [r for r in records if visible(r)]

    def named_in_logs(r: dict) -> bool:
        """Post-hoc: the culprit's name appears in some WARN/ERROR line (as the writer or mentioned)."""
        return any(row[1] in r["accepted"] or any(names(row[3] or "", c) for c in r["accepted"])
                   for row in r["logs"]["problem_lines"])

    named = [r for r in records if named_in_logs(r)]
    declined = [r for r in records if r["answer"] is None]
    answered = [r for r in records if r["answer"] is not None]
    errors = [r for r in records if r["flag"] not in SLOWNESS]
    slow = [r for r in records if r["flag"] in SLOWNESS]

    def part(group: list[dict]) -> str:
        k = sum(r["correct"] for r in group)
        return f"{k}/{len(group)}" if group else "-"

    lines = [
        "# Fault-injection results (real app, real failures)",
        "",
        f"Generated by `tests/faults/analyze.py` on {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC. Do not edit by hand.",
        "",
        "**Setup:** OpenTelemetry Demo v3.1.0 (Astronomy Shop) on one EC2 m6i.xlarge, with its load generator running.",
        "Failures were switched on through the demo's own flags; logs reached NexGen's Elasticsearch through an",
        "extra collector exporter; the dependency map was built from Jaeger traces (`topology_from_jaeger.py`).",
        f"{n} runs = {len({r['flag'] for r in records})} failure types x repeats. Each failure ran 3 minutes before asking;",
        "logs were cleared before each run. The collector's own self-monitoring logs were excluded (not part of the app).",
        "Live runs used the full pipeline: Master (rules + `gpt-oss-120b` re-ranking) -> Query (`gpt-oss-20b` KQL) -> Elasticsearch.",
        "The replay rows feed each run's saved logs through the same Master code with the LLM switched off.",
        "A pilot of 4 runs found three bugs (Query's prompt hid `log.level` among 155 fields; a misleading",
        "\"missing evidence\" message; no recovery check between runs). They were fixed and the pilot discarded.",
        "Rows marked **Post-hoc** were added after seeing the results (to separate \"evidence missing\" from",
        "\"reasoning wrong\"); every other row was decided before the runs.",
        "",
        "| | Result |",
        "|---|---|",
        f"| Live end-to-end (rules + LLM), root cause correct | {frac(live_ok)} |",
        f"| - error failures (something throws) | {part(errors)} |",
        f"| - slowness failures (CPU, GC, delays, leaks) | {part(slow)} |",
        f"| - failures visible in the logs (culprit logged a WARN/ERROR) | {part(seen_in_logs)} |",
        f"| - failures invisible in the logs | {part([r for r in records if not visible(r)])} |",
        f"| LLM calls that failed and fell back to rules | {sum(r.get('llm_fallbacks', 0) for r in records)} |",
        f"| Live end-to-end latency, median / p95 | {statistics.median(seconds):.1f} s / {p95:.1f} s |",
        f"| **Post-hoc:** culprit named anywhere in WARN/ERROR lines (its own or mentioned by another service) | {part(named)} |",
        f"| **Post-hoc:** culprit never named in any WARN/ERROR line | {part([r for r in records if not named_in_logs(r)])} |",
        f"| **Post-hoc:** declined to answer (no WARN/ERROR lines at all) | {len(declined)}/{n}, none wrongly blamed |",
        f"| **Post-hoc:** when it named a service, it was right | {part(answered)} |",
        f"| Replay, rules only, top suspect (no validator) | {frac(score(records, rules, 'top'))} |",
        f"| Replay, rules only, final answer | {frac(score(records, rules, 'final'))} |",
    ]
    if llm:
        lines += [f"| Replay, rules + LLM, final answer | {frac(score(records, llm, 'final'))} |"]
    lines += ["", "| Run | Accepted | In logs? | Live answer | Rules top | Rules final |" + (" LLM final |" if llm else ""),
              "|---|---|---|---|---|---|" + ("---|" if llm else "")]
    for r in records:
        row = (f"| {r['run_id']} | {', '.join(r['accepted'])} | {'yes' if visible(r) else 'no'} | "
               f"{r['answer']} {'✅' if r['correct'] else '❌'} | "
               f"{rules[r['run_id']]['top']} | {rules[r['run_id']]['final']} |")
        if llm:
            row += f" {llm[r['run_id']]['final']} |"
        lines.append(row)
    (HERE / "RESULTS.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines[:26]))


if __name__ == "__main__":
    main()
