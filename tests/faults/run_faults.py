"""
Fault-injection evaluation: break a real microservice app on purpose and check whether
NexGen names the service that was broken.

Target app: the OpenTelemetry Demo ("Astronomy Shop", v3.1.0). Its failure switches live in
src/flagd/demo.flagd.json and flagd reloads that file automatically. Its logs reach NexGen's
Elasticsearch (data stream logs-nexgen-default) through an extra collector exporter.

For each experiment:
  0. check the shop is up (front page answers 200)
  1. clear the logs, so only logs from this incident are searched
  2. switch the failure on and wait --warm seconds
  3. save a snapshot of the WARN/ERROR logs (used later by analyze.py)
  4. ask NexGen's Master the on-call question over HTTP and record its answer
  5. switch the failure off, wait --cool seconds, then until the broken service is quiet

The experiment list (failures, accepted answers, questions) is fixed below and was decided
before any run.

    python tests/faults/run_faults.py --demo ~/otel-demo --reps 3
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

ES = "http://localhost:9200/logs-nexgen-default"  # data stream the collector writes to
MASTER = "http://localhost:8000"
SHOP = "http://localhost:8080"
MASTER_LOG = Path.home() / "logs" / "master.log"
OUT = Path(__file__).resolve().parent / "runs"

FAILING = "Why are frontend requests failing?"
CHECKOUT = "Why are checkouts failing?"
SLOW = "Why is the frontend slow?"

# flag, variant to switch on, accepted root-cause services, question an on-call engineer would ask
EXPERIMENTS: list[tuple[str, str, set[str], str]] = [
    ("adFailure", "on", {"ad"}, FAILING),
    ("cartFailure", "100%", {"cart"}, FAILING),
    ("productCatalogFailure", "on", {"product-catalog"}, FAILING),
    ("recommendationCacheFailure", "on", {"recommendation"}, FAILING),
    ("failedReadinessProbe", "on", {"cart"}, FAILING),
    ("paymentFailure", "100%", {"payment"}, CHECKOUT),
    ("paymentUnreachable", "on", {"payment"}, CHECKOUT),
    ("emailMemoryLeak", "10000x", {"email"}, "Why are checkouts failing or slow?"),
    ("adHighCpu", "on", {"ad"}, SLOW),
    ("adManualGc", "on", {"ad"}, SLOW),
    ("imageSlowLoad", "10sec", {"image-provider"}, SLOW),
    ("intlShippingSlowdown", "10sec", {"shipping"}, SLOW),
    ("productCatalogLockContention", "on", {"product-catalog", "astronomy-db"}, SLOW),
]


def set_flag(flags_file: Path, flag: str, variant: str) -> None:
    """Switch one failure flag to ``variant`` ("off" to disable); flagd picks the change up itself."""
    data = json.loads(flags_file.read_text())
    data["flags"][flag]["defaultVariant"] = variant
    flags_file.write_text(json.dumps(data, indent=2) + "\n")


def all_off(flags_file: Path) -> None:
    """Make sure no failure is active."""
    data = json.loads(flags_file.read_text())
    for flag in data["flags"].values():
        flag["defaultVariant"] = "off"
    flags_file.write_text(json.dumps(data, indent=2) + "\n")


def clear_logs(client: httpx.Client) -> None:
    client.post(f"{ES}/_delete_by_query?refresh=true&conflicts=proceed",
                json={"query": {"match_all": {}}}).raise_for_status()


def wait_until_healthy(client: httpx.Client, limit: int = 300) -> bool:
    """The shop's front page must answer 200 before a run, so a crashed demo is not scored as a miss."""
    deadline = time.time() + limit
    while time.time() < deadline:
        try:
            if client.get(SHOP, timeout=10).status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        time.sleep(10)
    return False


def wait_until_quiet(client: httpx.Client, services: set[str], limit: int = 180) -> None:
    """After a failure is switched off, wait until those services logged no WARN/ERROR for 60 s."""
    query = {"query": {"bool": {"filter": [
        {"terms": {"service.name": sorted(services)}},
        {"terms": {"log.level": ["ERROR", "WARN"]}},
        {"range": {"@timestamp": {"gte": "now-60s"}}},
    ]}}}
    deadline = time.time() + limit
    while time.time() < deadline and client.post(f"{ES}/_count", json=query).json().get("count", 0) > 0:
        time.sleep(15)


def llm_fallbacks(since: int) -> int:
    """LLM calls in Master that failed and fell back to rules, counted from Master's log since ``since``."""
    if not MASTER_LOG.exists():
        return 0
    with MASTER_LOG.open() as f:
        f.seek(since)
        return sum("failed" in line and ("LLM" in line or "re-rank" in line) for line in f)


def snapshot(client: httpx.Client) -> dict[str, Any]:
    """WARN/ERROR lines currently stored, oldest first (messages cut to 300 characters)."""
    client.post(f"{ES}/_refresh")
    total = client.get(f"{ES}/_count").json().get("count", 0)
    body = {
        "size": 2000,
        "sort": [{"@timestamp": "asc"}],
        "query": {"terms": {"log.level": ["ERROR", "WARN"]}},
        "_source": ["@timestamp", "service.name", "log.level", "message"],
    }
    hits = client.post(f"{ES}/_search", json=body).json()["hits"]["hits"]
    rows = []
    for hit in hits:
        src = hit["_source"]
        service = src.get("service", {}).get("name") if isinstance(src.get("service"), dict) else src.get("service.name")
        level = src.get("log", {}).get("level") if isinstance(src.get("log"), dict) else src.get("log.level")
        rows.append([src.get("@timestamp"), service, level, str(src.get("message", ""))[:300]])
    return {"total_log_lines": total, "problem_lines": rows}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--demo", default=str(Path.home() / "otel-demo"))
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--warm", type=int, default=180, help="seconds the failure runs before asking")
    parser.add_argument("--cool", type=int, default=120, help="seconds to recover after switching off")
    parser.add_argument("--only", nargs="*", help="run only these flags")
    args = parser.parse_args()

    flags_file = Path(args.demo) / "src" / "flagd" / "demo.flagd.json"
    OUT.mkdir(exist_ok=True)
    all_off(flags_file)
    experiments = [e for e in EXPERIMENTS if not args.only or e[0] in args.only]

    with httpx.Client(timeout=300) as client:
        for rep in range(1, args.reps + 1):
            for flag, variant, accepted, question in experiments:
                run_id = f"{flag}-{rep}"
                if (OUT / f"{run_id}.json").exists():
                    print(f"skip {run_id} (already done)")
                    continue
                healthy = wait_until_healthy(client)
                clear_logs(client)
                set_flag(flags_file, flag, variant)
                started = datetime.now(timezone.utc)
                time.sleep(args.warm)
                logs = snapshot(client)

                log_offset = MASTER_LOG.stat().st_size if MASTER_LOG.exists() else 0
                t0 = time.perf_counter()
                report = client.post(f"{MASTER}/query", json={
                    "query_id": run_id, "raw_text": question, "session_id": run_id,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                }).json()
                seconds = round(time.perf_counter() - t0, 2)
                set_flag(flags_file, flag, "off")

                answer = next((e["ref"].removeprefix("service:") for e in report.get("evidence", [])
                               if e.get("type") == "root_cause"), None)
                trace = report.get("reasoning_trace_summary", "")
                record = {
                    "run_id": run_id, "flag": flag, "variant": variant, "accepted": sorted(accepted),
                    "question": question, "started_utc": started.isoformat(), "answer": answer,
                    "correct": answer in accepted, "seconds": seconds, "shop_healthy_before": healthy,
                    "log_search": trace.split(" | ")[0] if trace.startswith("log search:") else None,
                    "llm_fallbacks": llm_fallbacks(log_offset), "report": report, "logs": logs,
                }
                (OUT / f"{run_id}.json").write_text(json.dumps(record, indent=2))
                print(f"{run_id:<34} answer={answer!s:<16} {'OK  ' if record['correct'] else 'MISS'} "
                      f"{seconds:6.1f}s  problem_lines={len(logs['problem_lines'])}  "
                      f"llm_fallbacks={record['llm_fallbacks']}  {record['log_search']}", flush=True)
                time.sleep(args.cool)
                wait_until_quiet(client, accepted)
    all_off(flags_file)


if __name__ == "__main__":
    main()
