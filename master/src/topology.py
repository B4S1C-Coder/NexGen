"""Service call graph loaded from config/topology.json."""

from __future__ import annotations

import json
from pathlib import Path


class Topology:
    """
    Which service calls which. ``graph["gateway"]["dependencies"] == ["payments", ...]``
    means gateway calls payments, so a payments failure can show up as gateway errors.
    """

    def __init__(self, graph: dict[str, dict[str, list[str]]]) -> None:
        self.graph = graph

    @classmethod
    def load(cls, path: Path) -> Topology:
        """Read the topology JSON file; a missing file gives an empty topology."""
        return cls(json.loads(path.read_text()) if path.exists() else {})

    @property
    def services(self) -> list[str]:
        """Every service name that appears in the graph."""
        names = set(self.graph)
        for node in self.graph.values():
            names.update(node.get("dependencies", []))
        return sorted(names)

    def dependencies(self, service: str) -> list[str]:
        """Services that ``service`` calls directly."""
        return self.graph.get(service, {}).get("dependencies", [])

    def reachable(self, start: str) -> list[str]:
        """Every service ``start`` depends on, directly or through other services."""
        seen: list[str] = []
        stack = list(self.dependencies(start))
        while stack:
            current = stack.pop(0)
            if current not in seen and current != start:
                seen.append(current)
                stack.extend(self.dependencies(current))
        return seen

    def can_reach(self, start: str, target: str) -> bool:
        """
        True if ``start`` depends on ``target`` directly or through other services
        (or they are the same service). A failure in ``target`` can only cause
        symptoms in ``start`` when this is true.
        """
        return start == target or target in self.reachable(start)
