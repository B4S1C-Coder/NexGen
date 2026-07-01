"""Evaluation harness for the NL-to-KQL pipeline (TASKS.md P5-1).

Runs each natural-language question in data/eval_set.jsonl through the
full generate->validate->repair pipeline, executes the generated KQL and
the ground-truth KQL against a seeded Elasticsearch index, and compares
the returned document sets. Reports three metrics:

    ESR (Execution Success Rate)
        Fraction of questions where the generated KQL returns the SAME
        set of documents as the ground-truth KQL. Primary metric.

    EM (Exact Match)
        Fraction where generated KQL == ground-truth KQL after
        whitespace/case normalisation. Purely syntactic; informational.

    schema_hallucination_rate
        Fraction whose generated KQL references a field absent from the
        evaluation schema (per KQLValidator field checks).

The suite FAILS if ESR < 0.90 or schema_hallucination_rate > 0.02.

Two layers:
  * TestEvalSetDataset / TestSeedDocuments run with NO external services.
  * TestNLToKQLEvaluationHarness needs live Elasticsearch (docker compose
    up elasticsearch) AND a GROQ_API_KEY in query/.env, and self-skips
    otherwise.

Run with:
    pytest tests/eval/test_esr.py -v -s
"""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from elasticsearch import AsyncElasticsearch

from src.executor import ElasticsearchExecutor
from src.few_shot import FewShotExample
from src.generator import GeneratorSettings, KQLGenerator
from src.repair import RepairAgent
from src.schema_linker import FieldMeta, SchemaContext
from src.validator import KQLValidator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ES_URL = "http://localhost:9200"

DATA_DIR = Path(__file__).parent.parent.parent / "data"
EVAL_SET_PATH = DATA_DIR / "eval_set.jsonl"
FALLBACK_EXAMPLES_PATH = DATA_DIR / "fallback_examples.jsonl"

# Unique index per run ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â avoids stale data from previous runs
TEST_INDEX = f"nexgen-eval-{uuid.uuid4().hex[:8]}"

# Required categories per TASKS.md P5-1
REQUIRED_CATEGORIES = {
    "time_range",
    "service_filter",
    "level_filter",
    "nested_field",
    "aggregation_count",
    "multi_condition",
}

MIN_EVAL_SET_SIZE = 40

# P5-1 pass/fail thresholds
ESR_THRESHOLD = 0.90
HALLUCINATION_THRESHOLD = 0.02

# ---------------------------------------------------------------------------
# Dataset helpers
# ---------------------------------------------------------------------------

def load_jsonl(path: Path) -> list[dict[str, Any]]:
    """Load a newline-delimited JSON file into a list of dicts.

    Args:
        path: Path to a .jsonl file.

    Returns:
        One dict per non-empty line, in file order.
    """
    rows: list[dict[str, Any]] = []
    with open(path, encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def normalize_kql(kql: str) -> str:
    """Normalise a KQL string for exact-match comparison.

    Collapses whitespace runs to a single space, strips ends, and
    lower-cases. Makes EM tolerant of cosmetic differences while still
    requiring the same tokens in the same order.

    Args:
        kql: A raw KQL string.

    Returns:
        Normalised KQL string suitable for == comparison.
    """
    return re.sub(r"\s+", " ", kql.strip()).lower()


# ---------------------------------------------------------------------------
# Dataset-only tests (no external services required)
# ---------------------------------------------------------------------------

class TestEvalSetDataset:
    """Structural validation of data/eval_set.jsonl.

    Requires neither Elasticsearch nor Groq ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â always runs.
    """

    def test_eval_set_has_minimum_size(self) -> None:
        """Dataset must contain at least 40 examples (TASKS.md P5-1)."""
        eval_set = load_jsonl(EVAL_SET_PATH)
        assert len(eval_set) >= MIN_EVAL_SET_SIZE

    def test_eval_set_covers_required_categories(self) -> None:
        """Dataset must cover every required category at least once."""
        eval_set = load_jsonl(EVAL_SET_PATH)
        categories = {row["category"] for row in eval_set}
        missing = REQUIRED_CATEGORIES - categories
        assert not missing, f"eval_set.jsonl missing categories: {missing}"

    def test_eval_set_entries_have_required_fields(self) -> None:
        """Every entry must have non-empty id, category, nl, and kql."""
        eval_set = load_jsonl(EVAL_SET_PATH)
        for row in eval_set:
            for key in ("id", "category", "nl", "kql"):
                assert key in row, f"entry missing '{key}': {row}"
                assert str(row[key]).strip(), f"entry empty '{key}': {row}"

    def test_eval_set_ids_are_unique(self) -> None:
        """Every entry id must be unique."""
        eval_set = load_jsonl(EVAL_SET_PATH)
        ids = [row["id"] for row in eval_set]
        assert len(ids) == len(set(ids))

    def test_eval_set_ground_truth_passes_validator(self) -> None:
        """Every ground-truth KQL must itself pass KQLValidator.

        Catches typos in the dataset before they can muddy ESR/EM.
        """
        eval_set = load_jsonl(EVAL_SET_PATH)
        schema_ctx = build_eval_schema_ctx(TEST_INDEX)
        validator = KQLValidator()
        for row in eval_set:
            result = validator.validate(row["kql"], schema_ctx)
            assert result.valid, (
                f"{row['id']}: ground-truth KQL failed validation: "
                f"{result.errors}"
            )

# ---------------------------------------------------------------------------
# Evaluation schema
# ---------------------------------------------------------------------------

def build_eval_schema_ctx(index_name: str) -> SchemaContext:
    """Build the fixed SchemaContext for the evaluation index.

    Covers every field referenced by data/eval_set.jsonl. The nested
    items field is annotated is_nested=True so the pipeline uses the
    curly-brace wrapper documented in prompts/generator.txt.

    Args:
        index_name: Name of the seeded evaluation index.

    Returns:
        SchemaContext targeting index_name with all evaluation fields.
    """
    return SchemaContext(
        selected_indices=[index_name],
        relevant_fields=[
            FieldMeta("service.name", "keyword"),
            FieldMeta("log.level", "keyword"),
            FieldMeta("@timestamp", "date"),
            FieldMeta("message", "text"),
            FieldMeta("trace.id", "keyword"),
            FieldMeta("http.status_code", "integer"),
            FieldMeta("http.response_time_ms", "integer"),
            FieldMeta("environment", "keyword"),
            FieldMeta("error.code", "keyword"),
            FieldMeta("items", "nested", is_nested=True, nested_path="items"),
            FieldMeta("product_name", "keyword", is_nested=True, nested_path="items"),
            FieldMeta("quantity", "integer", is_nested=True, nested_path="items"),
            FieldMeta("warehouse", "keyword", is_nested=True, nested_path="items"),
        ],
        time_field="@timestamp",
        max_result_size=200,
    )


# ---------------------------------------------------------------------------
# Seed fixture data
# ---------------------------------------------------------------------------

def _index_mapping() -> dict[str, Any]:
    """Return the Elasticsearch mapping for the evaluation index.

    Returns:
        Mapping dict for client.indices.create(body={"mappings": ...}).
    """
    return {
        "properties": {
            "doc_id": {"type": "keyword"},
            "service.name": {"type": "keyword"},
            "log.level": {"type": "keyword"},
            "@timestamp": {"type": "date"},
            "message": {"type": "text"},
            "trace.id": {"type": "keyword"},
            "http.status_code": {"type": "integer"},
            "http.response_time_ms": {"type": "integer"},
            "environment": {"type": "keyword"},
            "error.code": {"type": "keyword"},
            "items": {
                "type": "nested",
                "properties": {
                    "product_name": {"type": "keyword"},
                    "quantity": {"type": "integer"},
                    "warehouse": {"type": "keyword"},
                },
            },
        }
    }


def _build_seed_documents(now: datetime) -> list[dict[str, Any]]:
    """Build the fixture log corpus for the evaluation index.

    Timestamps are relative to now so time-range questions have
    deterministic, non-trivial matching sets. Each document carries a
    unique doc_id so result sets can be compared by identity.

    Args:
        now: Reference timestamp (UTC).

    Returns:
        List of document dicts covering every field in eval_set.jsonl.
    """

    def ago(**kwargs: float) -> str:
        return (now - timedelta(**kwargs)).isoformat()

    return [
        {"doc_id": "seed-001", "service.name": "auth", "log.level": "ERROR",
         "@timestamp": ago(minutes=5), "environment": "production",
         "http.status_code": 401, "http.response_time_ms": 120,
         "message": "invalid token presented"},
        {"doc_id": "seed-002", "service.name": "payments", "log.level": "ERROR",
         "@timestamp": ago(minutes=20), "environment": "production",
         "http.status_code": 500, "http.response_time_ms": 2200,
         "message": "payment gateway timeout", "trace.id": "abc-123-xyz",
         "error.code": "E003"},
        {"doc_id": "seed-003", "service.name": "gateway", "log.level": "ERROR",
         "@timestamp": ago(minutes=40), "environment": "staging",
         "http.status_code": 500, "http.response_time_ms": 900,
         "message": "bad gateway upstream"},
        {"doc_id": "seed-004", "service.name": "orders", "log.level": "WARN",
         "@timestamp": ago(minutes=90), "environment": "production",
         "http.status_code": 200, "http.response_time_ms": 300,
         "message": "order queue backlog growing",
         "items": [{"product_name": "laptop", "quantity": 1, "warehouse": "east"}]},
        {"doc_id": "seed-005", "service.name": "orders", "log.level": "INFO",
         "@timestamp": ago(hours=3), "environment": "production",
         "http.status_code": 200, "http.response_time_ms": 150,
         "message": "order processed successfully",
         "items": [{"product_name": "keyboard", "quantity": 5, "warehouse": "east"}]},
        {"doc_id": "seed-006", "service.name": "inventory", "log.level": "DEBUG",
         "@timestamp": ago(minutes=45), "environment": "development",
         "http.status_code": 200, "http.response_time_ms": 1500,
         "message": "stock level check",
         "items": [{"product_name": "monitor", "quantity": 2, "warehouse": "west"}]},
        {"doc_id": "seed-007", "service.name": "orders", "log.level": "INFO",
         "@timestamp": ago(hours=2), "environment": "production",
         "http.status_code": 200, "http.response_time_ms": 180,
         "message": "bulk order shipped",
         "items": [{"product_name": "mouse", "quantity": 10, "warehouse": "east"}]},
        {"doc_id": "seed-008", "service.name": "notifications", "log.level": "INFO",
         "@timestamp": ago(minutes=10), "environment": "production",
         "http.status_code": 200, "http.response_time_ms": 80,
         "message": "email sent to customer"},
        {"doc_id": "seed-009", "service.name": "auth", "log.level": "WARN",
         "@timestamp": ago(hours=26), "environment": "production",
         "http.status_code": 200, "http.response_time_ms": 400,
         "message": "token near expiry"},
        {"doc_id": "seed-010", "service.name": "payments", "log.level": "ERROR",
         "@timestamp": ago(days=8), "environment": "production",
         "http.status_code": 500, "http.response_time_ms": 3000,
         "error.code": "E001", "message": "database connection lost"},
        {"doc_id": "seed-011", "service.name": "payments", "log.level": "ERROR",
         "@timestamp": ago(days=35), "environment": "production",
         "http.status_code": 500, "http.response_time_ms": 2800,
         "error.code": "E003", "message": "legacy payment processor failure"},
        {"doc_id": "seed-012", "service.name": "gateway", "log.level": "INFO",
         "@timestamp": ago(hours=2), "environment": "production",
         "http.status_code": 200, "http.response_time_ms": 180,
         "message": "upstream health check ok"},
        {"doc_id": "seed-013", "service.name": "auth", "log.level": "ERROR",
         "@timestamp": ago(hours=5), "environment": "staging",
         "http.status_code": 401, "http.response_time_ms": 130,
         "message": "invalid credentials supplied"},
        {"doc_id": "seed-014", "service.name": "orders", "log.level": "ERROR",
         "@timestamp": ago(hours=2), "environment": "production",
         "http.status_code": 503, "http.response_time_ms": 2100,
         "message": "order service unavailable"},
        {"doc_id": "seed-015", "service.name": "auth", "log.level": "ERROR",
         "@timestamp": ago(minutes=50), "environment": "production",
         "http.status_code": 500, "http.response_time_ms": 2500,
         "message": "auth backend internal error"},
        {"doc_id": "seed-016", "service.name": "gateway", "log.level": "WARN",
         "@timestamp": ago(hours=3), "environment": "production",
         "http.status_code": 200, "http.response_time_ms": 350,
         "message": "upstream latency elevated"},
        {"doc_id": "seed-017", "service.name": "orders", "log.level": "WARN",
         "@timestamp": ago(minutes=10), "environment": "production",
         "http.status_code": 200, "http.response_time_ms": 220,
         "message": "order item low stock warning",
         "items": [{"product_name": "laptop", "quantity": 2, "warehouse": "central"}]},
        {"doc_id": "seed-018", "service.name": "payments", "log.level": "WARN",
         "@timestamp": ago(minutes=20), "environment": "production",
         "http.status_code": 200, "http.response_time_ms": 300,
         "message": "payment retried successfully"},
        {"doc_id": "seed-019", "service.name": "inventory", "log.level": "INFO",
         "@timestamp": ago(hours=12), "environment": "production",
         "http.status_code": 200, "http.response_time_ms": 200,
         "message": "nightly stock sync completed"},
    ]


# ---------------------------------------------------------------------------
# Elasticsearch fixture lifecycle
# ---------------------------------------------------------------------------

async def seed_index(client: AsyncElasticsearch) -> None:
    """Create TEST_INDEX with the eval mapping and seed all documents.

    Args:
        client: An open AsyncElasticsearch client.

    Returns:
        None
    """
    try:
        await client.indices.delete(index=TEST_INDEX)
    except Exception:
        pass

    await client.indices.create(
        index=TEST_INDEX, body={"mappings": _index_mapping()}
    )

    now = datetime.now(timezone.utc)
    for doc in _build_seed_documents(now):
        await client.index(index=TEST_INDEX, id=doc["doc_id"], body=doc)

    await client.indices.refresh(index=TEST_INDEX)


async def delete_index(client: AsyncElasticsearch) -> None:
    """Delete TEST_INDEX, ignoring errors if it does not exist.

    Args:
        client: An open AsyncElasticsearch client.

    Returns:
        None
    """
    try:
        await client.indices.delete(index=TEST_INDEX)
    except Exception:
        pass


async def matching_doc_ids(
    executor: ElasticsearchExecutor,
    kql: str,
    schema_ctx: SchemaContext,
) -> set[str]:
    """Execute kql against the eval index; return matching doc_ids.

    Args:
        executor: A started ElasticsearchExecutor.
        kql: KQL to execute. Blank strings short-circuit to empty set.
        schema_ctx: SchemaContext targeting the eval index.

    Returns:
        Set of doc_id values from matching documents' _source.
    """
    if not kql.strip():
        return set()
    result = await executor.execute(
        kql=kql, schema_ctx=schema_ctx, max_results=schema_ctx.max_result_size
    )
    return {hit["doc_id"] for hit in result.hits if "doc_id" in hit}

# ---------------------------------------------------------------------------
# Seed-document fixture tests (no external services required)
# ---------------------------------------------------------------------------

class TestSeedDocuments:
    """Validation of the seed-document fixture generator.

    Requires neither Elasticsearch nor Groq.
    """

    def test_seed_documents_have_unique_ids(self) -> None:
        """Every seed document must have a unique doc_id (>= 15 docs)."""
        docs = _build_seed_documents(datetime.now(timezone.utc))
        doc_ids = [d["doc_id"] for d in docs]
        assert len(doc_ids) == len(set(doc_ids))
        assert len(docs) >= 15

    def test_seed_documents_cover_all_mapped_fields(self) -> None:
        """At least one seed document populates every top-level field."""
        docs = _build_seed_documents(datetime.now(timezone.utc))
        seen: set[str] = set()
        for doc in docs:
            seen.update(doc.keys())
        mapped = set(_index_mapping()["properties"].keys())
        assert mapped.issubset(seen), f"never populated: {mapped - seen}"


# ---------------------------------------------------------------------------
# Live evaluation harness (requires Elasticsearch + GROQ_API_KEY)
# ---------------------------------------------------------------------------

def _groq_key_present() -> bool:
    """True if a Groq API key is configured (env or .env)."""
    return bool(GeneratorSettings().groq_api_key.strip())


requires_live_stack = pytest.mark.skipif(
    not _groq_key_present(),
    reason="Live ESR eval needs GROQ_API_KEY and a running Elasticsearch. "
    "Set GROQ_API_KEY in query/.env and start ES (docker compose up "
    "elasticsearch) to run this test.",
)


@requires_live_stack
class TestNLToKQLEvaluationHarness:
    """End-to-end ESR / EM / schema-hallucination evaluation (P5-1)."""

    @pytest.mark.asyncio
    async def test_pipeline_meets_esr_and_hallucination_thresholds(self) -> None:
        """Run all 40 NLQs; assert ESR >= 0.90 and hallucination <= 0.02."""
        eval_set = load_jsonl(EVAL_SET_PATH)
        schema_ctx = build_eval_schema_ctx(TEST_INDEX)
        known_fields = {f.name for f in schema_ctx.relevant_fields}

        # Static few-shot examples (bypass Qdrant): reuse fallback corpus
        fallback = load_jsonl(FALLBACK_EXAMPLES_PATH)
        examples = [
            FewShotExample(nl=r["nl"], kql=r["kql"]) for r in fallback[:5]
        ]

        client = AsyncElasticsearch(ES_URL)
        generator = KQLGenerator()
        validator = KQLValidator()
        executor = ElasticsearchExecutor()

        em_hits = 0
        esr_hits = 0
        halluc_hits = 0
        per_category: dict[str, dict[str, int]] = {}
        failures: list[str] = []

        try:
            await seed_index(client)
            generator.startup()
            await executor.startup()
            agent = RepairAgent(generator, validator)

            for row in eval_set:
                cat = row["category"]
                bucket = per_category.setdefault(
                    cat, {"n": 0, "em": 0, "esr": 0, "halluc": 0}
                )
                bucket["n"] += 1

                # 1. Generate KQL through the full repair pipeline
                try:
                    generated = await agent.repair(
                        natural_language=row["nl"],
                        schema_ctx=schema_ctx,
                        examples=examples,
                    )
                except Exception as exc:  # generation failed outright
                    failures.append(f"{row['id']} GENERATION ERROR: {exc}")
                    continue

                # 2. Exact match (normalised)
                if normalize_kql(generated) == normalize_kql(row["kql"]):
                    em_hits += 1
                    bucket["em"] += 1

                # 3. Schema hallucination â€” any generated field not in schema
                gen_fields = set(re.findall(r"([\w.@]+)\s*[:<>!=]", generated))
                gen_fields -= {"AND", "OR", "NOT", "NOW", "and", "or", "not"}
                if gen_fields - known_fields:
                    halluc_hits += 1
                    bucket["halluc"] += 1

                # 4. ESR â€” same document set as ground truth
                try:
                    gen_ids = await matching_doc_ids(executor, generated, schema_ctx)
                    truth_ids = await matching_doc_ids(executor, row["kql"], schema_ctx)
                    if gen_ids == truth_ids:
                        esr_hits += 1
                        bucket["esr"] += 1
                    else:
                        failures.append(
                            f"{row['id']} [{cat}] ESR MISS\n"
                            f"    nl:    {row['nl']}\n"
                            f"    truth: {row['kql']}\n"
                            f"    gen:   {generated}\n"
                            f"    truth_ids={sorted(truth_ids)} gen_ids={sorted(gen_ids)}"
                        )
                except Exception as exc:
                    failures.append(f"{row['id']} EXECUTION ERROR: {exc}")

        finally:
            await executor.shutdown()
            generator.shutdown()
            await delete_index(client)
            await client.close()

        total = len(eval_set)
        esr = esr_hits / total
        em = em_hits / total
        halluc_rate = halluc_hits / total

        # ---- Report ----
        print("\n" + "=" * 60)
        print("NL-to-KQL EVALUATION REPORT (P5-1)")
        print("=" * 60)
        print(f"Total examples:            {total}")
        print(f"ESR  (execution success):  {esr:.3f}  (threshold >= {ESR_THRESHOLD})")
        print(f"EM   (exact match):        {em:.3f}")
        print(f"Schema hallucination rate: {halluc_rate:.3f}  (threshold <= {HALLUCINATION_THRESHOLD})")
        print("-" * 60)
        print(f"{'category':<20}{'n':>4}{'ESR':>8}{'EM':>8}{'halluc':>8}")
        for cat in sorted(per_category):
            b = per_category[cat]
            print(
                f"{cat:<20}{b['n']:>4}"
                f"{b['esr'] / b['n']:>8.2f}"
                f"{b['em'] / b['n']:>8.2f}"
                f"{b['halluc'] / b['n']:>8.2f}"
            )
        if failures:
            print("-" * 60)
            print(f"{len(failures)} example(s) missed:")
            for f in failures:
                print("  " + f)
        print("=" * 60)

        # ---- Gates ----
        assert esr >= ESR_THRESHOLD, (
            f"ESR {esr:.3f} below threshold {ESR_THRESHOLD}. "
            f"See report above for the {len(failures)} miss(es)."
        )
        assert halluc_rate <= HALLUCINATION_THRESHOLD, (
            f"Schema hallucination rate {halluc_rate:.3f} exceeds "
            f"threshold {HALLUCINATION_THRESHOLD}."
        )
