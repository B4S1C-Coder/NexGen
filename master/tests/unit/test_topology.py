from pathlib import Path

from src.topology import Topology


def test_can_reach_follows_calls_transitively(topology):
    assert topology.can_reach("gateway", "db-primary")
    assert topology.can_reach("payments", "payments")
    assert not topology.can_reach("payments", "gateway")
    assert not topology.can_reach("gateway", "notifications")


def test_reachable_lists_all_downstream_services(topology):
    assert topology.reachable("gateway") == ["payments", "db-primary"]
    assert topology.reachable("db-primary") == []


def test_services_include_dependencies(topology):
    assert topology.services == ["db-primary", "gateway", "notifications", "payments"]


def test_missing_file_gives_empty_topology(tmp_path: Path):
    assert Topology.load(tmp_path / "nope.json").services == []
