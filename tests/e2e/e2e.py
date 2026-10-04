"""
End-to-end test of the whole NexGen stack: real Elasticsearch, Qdrant, Query, RAG and Master.
tests/e2e/run.sh starts everything and calls this script in two phases:

    setup  - create one Elasticsearch log index per service
    run    - ingest RAG's documents (rag/data/docs), then replay each incident from master/data/scenarios.json:
             load its logs into Elasticsearch (shifted to "now"), ask Master the incident's question
             over HTTP, and compare the root cause Master reports with the expected one.

Results are written to tests/e2e/RESULTS.md.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
DATA = json.loads((ROOT / "master" / "data" / "scenarios.json").read_text())
SERVICES = sorted(json.loads((ROOT / "master" / "config" / "topology.json").read_text()))

ES, RAG, MASTER = "http://localhost:9200", "http://localhost:8002", "http://localhost:8000"
MAPPING = {
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date"},
            "service": {"properties": {"name": {"type": "keyword"}}},
            "log": {"properties": {"level": {"type": "keyword"}}},
            "message": {"type": "text"},
            "trace": {"properties": {"id": {"type": "keyword"}}},
        }
    }
}


def setup() -> None:
    """Create <service>-logs indices (before Query starts, so its schema cache sees them)."""
    with httpx.Client(timeout=30) as client:
        for service in SERVICES:
            response = client.put(f"{ES}/{service}-logs", json=MAPPING)
            if response.status_code not in (200, 400):  # 400 = index already exists
                response.raise_for_status()
    print(f"setup: {len(SERVICES)} log indices")


def load_incident(client: httpx.Client, scenario: dict[str, Any]) -> None:
    """Replace all log documents with this incident's logs, shifted so the last line is 1 minute ago."""
    client.post(f"{ES}/*-logs/_delete_by_query?refresh=true&conflicts=proceed",
                json={"query": {"match_all": {}}}).raise_for_status()
    times = [datetime.fromisoformat(f"{DATA['date']}T{row[0]}+00:00") for row in scenario["logs"]]
    shift = datetime.now(timezone.utc) - timedelta(minutes=1) - max(times)
    lines = []
    for i, ((_, service, level, message), at) in enumerate(zip(scenario["logs"], times)):
        lines.append(json.dumps({"index": {"_index": f"{service}-logs"}}))
        lines.append(json.dumps({
            "@timestamp": (at + shift).isoformat(), "service.name": service, "log.level": level,
            "message": message, "trace.id": f"{scenario['id']}-{i}",
        }))
    response = client.post(f"{ES}/_bulk?refresh=true", content="\n".join(lines) + "\n",
                           headers={"Content-Type": "application/x-ndjson"})
    response.raise_for_status()
    if response.json().get("errors"):
        raise RuntimeError(f"bulk load failed for {scenario['id']}: {response.text[:300]}")


def run(delay: float, limit: int | None) -> None:
    """Ingest runbooks, replay incidents through Master, and write RESULTS.md."""
    rows = []
    with httpx.Client(timeout=300) as client:
        ingest = client.post(f"{RAG}/ingest", json={"source_type": "local_file", "full_reindex": True})
        ingest.raise_for_status()
        print("RAG ingest:", ingest.json())

        scenarios = DATA["scenarios"][:limit]
        for i, scenario in enumerate(scenarios):
            if i:
                time.sleep(delay)  # stay inside the LLM free-tier tokens-per-minute limit
            load_incident(client, scenario)
            started = time.perf_counter()
            report = client.post(f"{MASTER}/query", json={
                "query_id": f"e2e-{scenario['id']}", "raw_text": scenario["question"],
                "session_id": f"e2e-{scenario['id']}", "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            }).json()
            seconds = time.perf_counter() - started
            culprit = next((e["ref"].removeprefix("service:") for e in report.get("evidence", [])
                            if e["type"] == "root_cause"), None)
            rows.append({
                "id": scenario["id"], "kind": scenario["kind"], "expected": scenario["expected_culprit"],
                "got": culprit, "correct": culprit == scenario["expected_culprit"],
                "confidence": report.get("confidence"), "seconds": round(seconds, 1),
                "docs_cited": sum(e["type"] not in ("log", "root_cause") for e in report.get("evidence", [])),
                "summary": report.get("root_cause_summary", str(report))[:160],
            })
            print(f"  {scenario['id']:<10} expected={scenario['expected_culprit']:<13} got={culprit!s:<13} "
                  f"{'OK  ' if rows[-1]['correct'] else 'MISS'} {seconds:5.1f}s  conf={report.get('confidence')}")

    write_results(rows)


def write_results(rows: list[dict[str, Any]]) -> None:
    """Write tests/e2e/RESULTS.md."""
    correct = sum(r["correct"] for r in rows)
    seconds = sorted(r["seconds"] for r in rows)
    p95 = seconds[min(len(seconds) - 1, round(0.95 * (len(seconds) - 1)))]
    kinds = {k: [r for r in rows if r["kind"] == k] for k in ("normal", "trap", "red_herring")}
    lines = [
        "# End-to-end results (real stack)",
        "",
        f"Generated by `tests/e2e/run.sh` on {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC. Do not edit by hand.",
        "",
        "Master -> Query (Groq KQL generation -> Elasticsearch -> PII masking) and RAG (Ollama embeddings ->",
        "Qdrant hybrid search -> re-ranking -> compression), for the same incidents as `master/eval`.",
        "",
        f"- **Root cause correct:** {correct}/{len(rows)}"
        + "".join(f" | {k}: {sum(r['correct'] for r in v)}/{len(v)}" for k, v in kinds.items() if v),
        f"- **Latency per question:** p50 {statistics.median(seconds):.1f} s, p95 {p95:.1f} s",
        f"- **Answers citing at least one runbook:** {sum(r['docs_cited'] > 0 for r in rows)}/{len(rows)}",
        "",
        "| Incident | Kind | Expected | Got | Confidence | Seconds | Summary |",
        "|---|---|---|---|---|---|---|",
    ]
    lines += [
        f"| {r['id']} | {r['kind']} | {r['expected']} | {r['got']} {'✅' if r['correct'] else '❌'} | "
        f"{r['confidence']} | {r['seconds']} | {r['summary'].replace('|', '/')} |"
        for r in rows
    ]
    (HERE / "RESULTS.md").write_text("\n".join(lines) + "\n")
    print("\n" + "\n".join(lines[:9]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("phase", choices=["setup", "run"])
    parser.add_argument("--delay", type=float, default=45.0, help="seconds between incidents")
    parser.add_argument("--limit", type=int, default=None, help="only the first N incidents")
    args = parser.parse_args()
    setup() if args.phase == "setup" else run(args.delay, args.limit)


if __name__ == "__main__":
    main()
