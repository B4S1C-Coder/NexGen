"""Unit tests for FewShotSelector (few_shot.py)."""

from __future__ import annotations

from pathlib import Path

from src.few_shot import EXAMPLES_PATH, FewShotExample, FewShotSelector, _load_examples


def test_loads_real_examples_file() -> None:
    examples = _load_examples(EXAMPLES_PATH)
    assert len(examples) >= 10
    assert all(ex.nl and ex.kql and ex.score is None for ex in examples)


def test_missing_file_returns_empty_list(tmp_path: Path) -> None:
    assert _load_examples(tmp_path / "missing.jsonl") == []


def test_bad_lines_are_skipped(tmp_path: Path) -> None:
    path = tmp_path / "ex.jsonl"
    path.write_text('{"nl": "a", "kql": "b"}\nnot json\n{"nl": "missing kql"}\n')
    assert _load_examples(path) == [FewShotExample(nl="a", kql="b")]


async def test_select_ranks_by_shared_words(tmp_path: Path) -> None:
    path = tmp_path / "ex.jsonl"
    path.write_text(
        '{"nl": "count inventory requests", "kql": "k1"}\n'
        '{"nl": "show payments errors last hour", "kql": "k2"}\n'
        '{"nl": "show auth errors", "kql": "k3"}\n'
    )
    selector = FewShotSelector(path=path, top_k=2)
    await selector.startup()

    results = await selector.select("show me payments errors")

    assert [r.kql for r in results] == ["k2", "k3"]
    assert results[0].score == 3.0  # show, payments, errors


async def test_select_before_startup_returns_empty() -> None:
    assert await FewShotSelector().select("anything") == []
