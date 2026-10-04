"""
Benchmark for the Master orchestrator.

    uv run python eval/run_eval.py --mode rules   # no LLM, instant
    uv run python eval/run_eval.py --mode llm     # uses OPENAI_* settings from master/.env

Each mode saves eval/results_<mode>.json; eval/RESULTS.md is rebuilt from whichever exist.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # make `src` importable

from nexgen_shared.schemas import UserQuery  # noqa: E402

from src.fixtures import FixtureBackend  # noqa: E402
from src.llm import ask_json  # noqa: E402
from src.orchestrator import MasterOrchestrator  # noqa: E402
from src.settings import MASTER_DIR, Settings  # noqa: E402

EVAL_DIR = MASTER_DIR / "eval"
INTENT_PATH = MASTER_DIR / "data" / "intent_queries.json"


def route_of(logs: bool, docs: bool) -> str:
    """Name of the route an IntentResult took."""
    return "both" if logs and docs else "logs" if logs else "docs"


class FallbackCounter(logging.Handler):
    """Counts LLM calls that failed and silently fell back to the rules."""

    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.count = 0

    def emit(self, record: logging.LogRecord) -> None:
        if "LLM" in record.getMessage():
            self.count += 1


def percentile(values: list[float], pct: float) -> float:
    """Simple nearest-rank percentile."""
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(pct / 100 * (len(ordered) - 1)))]


async def evaluate(mode: str, delay: float) -> dict[str, Any]:
    """Run the routing and root-cause benchmarks for one mode and return the numbers."""
    overrides: dict[str, Any] = {"MOCK_SERVICES": True}
    if mode == "rules":
        overrides["OPENAI_API_KEY"] = ""
    settings = Settings(**overrides)
    if mode == "llm" and not settings.openai_api_key:
        raise SystemExit("--mode llm needs OPENAI_API_KEY in master/.env (see .env.example)")
    orchestrator = MasterOrchestrator(settings)
    if mode == "llm":  # fail fast instead of quietly measuring the rules again
        try:
            await ask_json(orchestrator.reasoner.llm, settings.openai_model_name,  # type: ignore[arg-type]
                           'Reply with JSON only: {"ok": true}', "ping")
        except Exception as exc:
            raise SystemExit(f"LLM check failed for model {settings.openai_model_name}: {exc}")
    fallbacks = FallbackCounter()
    logging.getLogger("src").addHandler(fallbacks)

    # 1. Routing accuracy
    routing_misses, intent_ms = [], []
    queries = json.loads(INTENT_PATH.read_text())
    for item in queries:
        start = time.perf_counter()
        intent = await orchestrator.intent.classify(item["q"])
        intent_ms.append((time.perf_counter() - start) * 1000)
        got = route_of(intent.logs_needed, intent.docs_needed)
        if got != item["route"]:
            routing_misses.append({"question": item["q"], "expected": item["route"], "got": got})

    # 2. Root cause on every scenario
    rows = []
    for i, scenario in enumerate(FixtureBackend().scenarios):
        if mode == "llm" and i > 0:
            await asyncio.sleep(delay)  # stay inside free-tier rate limits
        query = UserQuery(
            query_id=f"eval-{scenario['id']}", raw_text=scenario["question"],
            session_id=f"eval-{mode}-{scenario['id']}", timestamp_utc=datetime.now(timezone.utc),
        )
        run = await orchestrator.run(query)
        top = run.candidates[0].culprit_service if run.candidates else None
        final = run.accepted.culprit_service if run.accepted else None
        rows.append({
            "id": scenario["id"], "kind": scenario["kind"], "expected": scenario["expected_culprit"],
            "top_candidate": top, "final": final,
            "top_correct": top == scenario["expected_culprit"],
            "final_correct": final == scenario["expected_culprit"],
            "e008_rejections": run.report.reasoning_trace_summary.count("[E008]"),
            "confidence": run.report.confidence,
            "tokens_before": run.tokens_before, "tokens_after": run.tokens_after,
            "latency_ms": run.timings_ms["total"],
            "trace": run.report.reasoning_trace_summary,
        })
        print(f"  {scenario['id']:<10} expected={scenario['expected_culprit']:<13} final={final!s:<13} "
              f"{'OK' if final == scenario['expected_culprit'] else 'MISS'}")

    by_kind = {
        kind: {"correct": sum(r["final_correct"] for r in rows if r["kind"] == kind),
               "total": sum(r["kind"] == kind for r in rows)}
        for kind in ("normal", "trap", "red_herring")
    }
    traps = [r for r in rows if r["kind"] == "trap"]
    before, after = sum(r["tokens_before"] for r in rows), sum(r["tokens_after"] for r in rows)
    latencies = [r["latency_ms"] for r in rows]
    return {
        "mode": mode,
        "model": settings.openai_model_name if mode == "llm" else None,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "routing": {"correct": len(queries) - len(routing_misses), "total": len(queries),
                    "p50_ms": round(statistics.median(intent_ms), 2), "misses": routing_misses},
        "scenarios": len(rows),
        "top_candidate_correct": sum(r["top_correct"] for r in rows),
        "final_correct": sum(r["final_correct"] for r in rows),
        "by_kind": by_kind,
        "traps_caught": sum(r["e008_rejections"] > 0 for r in traps),
        "traps_total": len(traps),
        "tokens_before": before, "tokens_after": after,
        "token_reduction_pct": round(100 * (before - after) / before, 1) if before else 0.0,
        "latency_p50_ms": round(percentile(latencies, 50), 1),
        "latency_p95_ms": round(percentile(latencies, 95), 1),
        "llm_fallbacks": fallbacks.count,
        "rows": rows,
    }


def frac(a: int, b: int) -> str:
    return f"{a}/{b} ({100 * a / b:.0f}%)" if b else "-"


def write_markdown(results: dict[str, dict[str, Any]]) -> str:
    """Build RESULTS.md from the saved results of each mode."""
    modes = [m for m in ("rules", "llm") if m in results]
    heads = {"rules": "Rules only", "llm": f"Rules + LLM ({results.get('llm', {}).get('model')})"}

    def row(label: str, fn: Any) -> str:
        return f"| {label} | " + " | ".join(fn(results[m]) for m in modes) + " |"

    lines = [
        "# Master benchmark results",
        "",
        "Generated by `uv run python eval/run_eval.py --mode rules|llm`. Do not edit by hand.",
        "",
        "**Data:** 24 hand-written synthetic incidents in `data/scenarios.json` (6 failure types x 4:",
        "12 normal, 6 *trap* = earlier noise from a service the symptom cannot depend on,",
        "6 *red herring* = earlier noise from a service it does depend on), and 40 labelled",
        "routing questions in `data/intent_queries.json`. Query/RAG are replaced by these fixtures.",
        "",
        "| Metric | " + " | ".join(heads[m] for m in modes) + " |",
        "|---|" + "---|" * len(modes),
        row("Routing accuracy (40 questions)", lambda r: frac(r["routing"]["correct"], r["routing"]["total"])),
        row("Root cause: top-ranked candidate, no validator", lambda r: frac(r["top_candidate_correct"], r["scenarios"])),
        row("Root cause: final answer (after validator)", lambda r: frac(r["final_correct"], r["scenarios"])),
        row("  - normal incidents", lambda r: frac(r["by_kind"]["normal"]["correct"], r["by_kind"]["normal"]["total"])),
        row("  - trap incidents", lambda r: frac(r["by_kind"]["trap"]["correct"], r["by_kind"]["trap"]["total"])),
        row("  - red-herring incidents", lambda r: frac(r["by_kind"]["red_herring"]["correct"], r["by_kind"]["red_herring"]["total"])),
        row("Traps where validator fired E008", lambda r: frac(r["traps_caught"], r["traps_total"])),
        row("Log tokens removed by de-duplication", lambda r: f"{r['token_reduction_pct']}% ({r['tokens_before']} -> {r['tokens_after']})"),
        row("End-to-end latency p50 / p95", lambda r: f"{r['latency_p50_ms']} ms / {r['latency_p95_ms']} ms"),
        row("LLM calls that failed and fell back to rules", lambda r: str(r.get("llm_fallbacks", 0)) if r["mode"] == "llm" else "-"),
        "",
        "## Misses",
        "",
    ]
    for m in modes:
        misses = [f"{r['id']} (expected {r['expected']}, got {r['final']})" for r in results[m]["rows"] if not r["final_correct"]]
        routing = [f"\"{x['question']}\" ({x['expected']} -> {x['got']})" for x in results[m]["routing"]["misses"]]
        lines.append(f"- **{heads[m]}** root cause: {', '.join(misses) or 'none'}")
        lines.append(f"- **{heads[m]}** routing: {', '.join(routing) or 'none'}")
    lines += [
        "",
        "## Caveats (say these if asked)",
        "",
        "- The incidents are synthetic and small (24). They were written to exercise each failure",
        "  mode, so the *rules-only* numbers per category are largely by construction; the LLM",
        "  column is the part that is genuinely measured.",
        "- Latency here excludes Elasticsearch/Qdrant because the downstream services are fixtures.",
        "- The routing questions were written by the same author as the keyword rules.",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=["rules", "llm"], default="rules")
    parser.add_argument("--delay", type=float, default=15.0, help="seconds between scenarios in llm mode")
    args = parser.parse_args()

    print(f"Running benchmark in {args.mode} mode...")
    result = asyncio.run(evaluate(args.mode, args.delay))
    (EVAL_DIR / f"results_{args.mode}.json").write_text(json.dumps(result, indent=2))

    saved = {m: json.loads((EVAL_DIR / f"results_{m}.json").read_text())
             for m in ("rules", "llm") if (EVAL_DIR / f"results_{m}.json").exists()}
    markdown = write_markdown(saved)
    (EVAL_DIR / "RESULTS.md").write_text(markdown)
    print("\n" + markdown)


if __name__ == "__main__":
    main()
