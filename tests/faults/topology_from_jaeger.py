"""
Build NexGen's service dependency map (topology.json) from real traces.

Jaeger records which service called which. Its dependencies API returns
[{"parent": "frontend", "child": "cart", "callCount": 120}, ...] for a time window,
which becomes {"frontend": {"dependencies": ["cart", ...]}, ...}.

    python tests/faults/topology_from_jaeger.py --out tests/faults/topology.json
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import httpx


def build_topology(edges: list[dict]) -> dict[str, dict[str, list[str]]]:
    """Turn Jaeger parent -> child call edges into NexGen's topology format (self-calls dropped)."""
    graph: dict[str, set[str]] = {}
    for edge in edges:
        parent, child = edge["parent"], edge["child"]
        graph.setdefault(parent, set())
        graph.setdefault(child, set())
        if parent != child:
            graph[parent].add(child)
    return {service: {"dependencies": sorted(deps)} for service, deps in sorted(graph.items())}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--jaeger", default="http://localhost:8080/jaeger/ui")
    parser.add_argument("--lookback-minutes", type=int, default=60)
    parser.add_argument("--out", default="tests/faults/topology.json")
    args = parser.parse_args()

    now_ms = int(time.time() * 1000)
    response = httpx.get(f"{args.jaeger}/api/dependencies",
                         params={"endTs": now_ms, "lookback": args.lookback_minutes * 60_000}, timeout=30)
    response.raise_for_status()
    topology = build_topology(response.json()["data"])
    Path(args.out).write_text(json.dumps(topology, indent=2) + "\n")
    print(f"{len(topology)} services, {sum(len(v['dependencies']) for v in topology.values())} call edges -> {args.out}")


if __name__ == "__main__":
    main()
