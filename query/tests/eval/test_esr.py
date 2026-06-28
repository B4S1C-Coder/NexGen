"""P5-1: NL-to-KQL evaluation harness.

Two layers:

1. Offline dataset checks (always run): the eval set is well-formed,
   covers all six categories, and every ground-truth KQL passes the real
   KQLValidator and transpiles via kql_to_dsl. No infra needed.

2. Live ESR grading (runs only when Groq + Elasticsearch are reachable;
   skips cleanly otherwise): seeds a temp ES index, runs each NLQ through
   the real pipeline (generator -> validator -> executor), and reports
   ESR (result-set match), EM (exact match) and schema-hallucination rate.
   Fails if ESR < 0.90 or schema_hallucination_rate > 0.02.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.executor import ElasticsearchExecutor
from src.few_shot import FewShotExample, _load_fallback_examples, FALLBACK_PATH
from src.generator import GeneratorSettings, KQLGenerator
from src.kql_dsl import kql_to_dsl
from src.repair import RepairAgent
from src.schema_linker import FieldMeta, SchemaContext
from src.validator import KQLValidator, _check_field_names

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

EVAL_SET_PATH = Path(__file__).parent.parent.parent / "data" / "eval_set.jsonl"
TEST_INDEX = f"nexgen-eval-{uuid.uuid4().hex[:8]}"

REQUIRED_CATEGORIES = {
    "time_range", "service_filter", "level_filter",
    "nested_field", "aggregation_count", "multi_condition",
}
MIN_EVAL_SET_SIZE = 40
ESR_THRESHOLD = 0.90
HALLUCINATION_THRESHOLD = 0.02


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_eval_set() -> list[dict]:
    """Load the eval dataset as a list of dicts."""
    rows = []
    with open(EVAL_SET_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def normalize_kql(kql: str) -> str:
    """Normalise whitespace and case for exact-match comparison."""
    return re.sub(r"\s+", " ", kql.strip()).lower()


def eval_schema_ctx(index_name: str) -> SchemaContext:
    """Fixed SchemaContext describing the evaluation index fields."""
    return SchemaContext(
        selected_indices=[index_name],
        relevant_fields=[
            FieldMeta(name="service.name", es_type="keyword"),
            FieldMeta(name="log.level", es_type="keyword"),
            FieldMeta(name="@timestamp", es_type="date"),
            FieldMeta(name="message", es_type="text"),
            FieldMeta(name="trace.id", es_type="keyword"),
            FieldMeta(name="http.status_code", es_type="long"),
            FieldMeta(name="http.response_time_ms", es_type="long"),
            FieldMeta(name="environment", es_type="keyword"),
            FieldMeta(name="error.code", es_type="keyword"),
            FieldMeta(name="items", es_type="nested", is_nested=True, nested_path="items"),
            FieldMeta(name="items.product_name", es_type="keyword", is_nested=True, nested_path="items"),
            FieldMeta(name="items.quantity", es_type="long", is_nested=True, nested_path="items"),
            FieldMeta(name="items.warehouse", es_type="keyword", is_nested=True, nested_path="items"),
            FieldMeta(name="product_name", es_type="keyword", is_nested=True, nested_path="items"),
            FieldMeta(name="quantity", es_type="long", is_nested=True, nested_path="items"),
            FieldMeta(name="warehouse", es_type="keyword", is_nested=True, nested_path="items"),
        ],
        time_field="@timestamp",
        max_result_size=200,
    )


def index_mapping() -> dict:
    """Elasticsearch mapping for the evaluation index."""
    return {
        "properties": {
            "doc_id": {"type": "keyword"},
            "service.name": {"type": "keyword"},
            "log.level": {"type": "keyword"},
            "@timestamp": {"type": "date"},
            "message": {"type": "text"},
            "trace.id": {"type": "keyword"},
            "http.status_code": {"type": "long"},
            "http.response_time_ms": {"type": "long"},
            "environment": {"type": "keyword"},
            "error.code": {"type": "keyword"},
            "items": {
                "type": "nested",
                "properties": {
                    "product_name": {"type": "keyword"},
                    "quantity": {"type": "long"},
                    "warehouse": {"type": "keyword"},
                },
            },
        }
    }


def build_seed_documents(now: datetime) -> list[dict]:
    """Fixture log corpus; timestamps relative to now for time-range queries."""
    def ago(**kw):
        return (now - timedelta(**kw)).isoformat()
    return [
        {"doc_id": "s01", "service.name": "auth", "log.level": "ERROR", "@timestamp": ago(minutes=5), "environment": "production", "http.status_code": 401, "http.response_time_ms": 120, "message": "invalid token"},
        {"doc_id": "s02", "service.name": "payments", "log.level": "ERROR", "@timestamp": ago(minutes=20), "environment": "production", "http.status_code": 500, "http.response_time_ms": 2200, "message": "gateway timeout", "trace.id": "abc-123-xyz", "error.code": "E003"},
        {"doc_id": "s03", "service.name": "gateway", "log.level": "ERROR", "@timestamp": ago(minutes=40), "environment": "staging", "http.status_code": 500, "http.response_time_ms": 900, "message": "bad gateway"},
        {"doc_id": "s04", "service.name": "orders", "log.level": "WARN", "@timestamp": ago(minutes=90), "environment": "production", "http.status_code": 200, "http.response_time_ms": 300, "message": "queue backlog", "items": [{"product_name": "laptop", "quantity": 1, "warehouse": "east"}]},
        {"doc_id": "s05", "service.name": "orders", "log.level": "INFO", "@timestamp": ago(hours=3), "environment": "production", "http.status_code": 200, "http.response_time_ms": 150, "message": "order ok", "items": [{"product_name": "keyboard", "quantity": 5, "warehouse": "east"}]},
        {"doc_id": "s06", "service.name": "inventory", "log.level": "DEBUG", "@timestamp": ago(minutes=45), "environment": "development", "http.status_code": 200, "http.response_time_ms": 1500, "message": "stock check", "items": [{"product_name": "monitor", "quantity": 2, "warehouse": "west"}]},
        {"doc_id": "s07", "service.name": "notifications", "log.level": "INFO", "@timestamp": ago(minutes=10), "environment": "production", "http.status_code": 200, "http.response_time_ms": 80, "message": "email sent"},
        {"doc_id": "s08", "service.name": "auth", "log.level": "WARN", "@timestamp": ago(hours=26), "environment": "production", "http.status_code": 200, "http.response_time_ms": 400, "message": "token near expiry"},
        {"doc_id": "s09", "service.name": "payments", "log.level": "ERROR", "@timestamp": ago(days=8), "environment": "production", "http.status_code": 500, "http.response_time_ms": 3000, "error.code": "E001", "message": "db connection lost"},
        {"doc_id": "s10", "service.name": "gateway", "log.level": "INFO", "@timestamp": ago(hours=2), "environment": "production", "http.status_code": 200, "http.response_time_ms": 180, "message": "health ok"},
        {"doc_id": "s11", "service.name": "auth", "log.level": "ERROR", "@timestamp": ago(minutes=50), "environment": "production", "http.status_code": 500, "http.response_time_ms": 2500, "message": "auth backend error"},
        {"doc_id": "s12", "service.name": "orders", "log.level": "ERROR", "@timestamp": ago(hours=2), "environment": "production", "http.status_code": 503, "http.response_time_ms": 2100, "message": "order unavailable"},
        {"doc_id": "s13", "service.name": "inventory", "log.level": "INFO", "@timestamp": ago(hours=12), "environment": "production", "http.status_code": 200, "http.response_time_ms": 200, "message": "nightly sync done"},
        {"doc_id": "s14", "service.name": "payments", "log.level": "WARN", "@timestamp": ago(minutes=20), "environment": "production", "http.status_code": 200, "http.response_time_ms": 300, "message": "payment retried"},
        {"doc_id": "s15", "service.name": "gateway", "log.level": "WARN", "@timestamp": ago(hours=3), "environment": "production", "http.status_code": 200, "http.response_time_ms": 350, "message": "latency elevated"},
    ]


# ---------------------------------------------------------------------------
# Layer 1: offline dataset checks (always run)
# ---------------------------------------------------------------------------

class TestEvalSetDataset:
    """Structural validation of eval_set.jsonl. No infra needed."""

    def test_minimum_size(self):
        assert len(load_eval_set()) >= MIN_EVAL_SET_SIZE

    def test_covers_required_categories(self):
        cats = {r["category"] for r in load_eval_set()}
        assert REQUIRED_CATEGORIES - cats == set()

    def test_entries_have_required_fields(self):
        for r in load_eval_set():
            for key in ("id", "category", "nl", "kql"):
                assert key in r and str(r[key]).strip()

    def test_ids_unique(self):
        ids = [r["id"] for r in load_eval_set()]
        assert len(ids) == len(set(ids))

    def test_ground_truth_validates_and_transpiles(self):
        schema_ctx = eval_schema_ctx(TEST_INDEX)
        v = KQLValidator()
        for r in load_eval_set():
            res = v.validate(r["kql"], schema_ctx)
            assert res.valid, f"{r['id']}: {res.errors}"
            kql_to_dsl(r["kql"])


class TestSeedDocuments:
    """Validation of the seed fixture generator. No infra needed."""

    def test_unique_doc_ids(self):
        docs = build_seed_documents(datetime.now(timezone.utc))
        ids = [d["doc_id"] for d in docs]
        assert len(ids) == len(set(ids)) and len(docs) >= 15


# ---------------------------------------------------------------------------
# Layer 2: live ESR grading (skips when Groq or ES unavailable)
# ---------------------------------------------------------------------------

def groq_available() -> bool:
    return bool(GeneratorSettings().groq_api_key.strip())


async def es_available() -> bool:
    """True if Elasticsearch responds to a ping. Index-independent."""
    from elasticsearch import AsyncElasticsearch
    from src.executor import ExecutorSettings

    client = AsyncElasticsearch(hosts=[ExecutorSettings().elasticsearch_url])
    try:
        ok = await client.ping()
        return bool(ok)
    except Exception:
        return False
    finally:
        await client.close()


@pytest.mark.asyncio
class TestNLToKQLEvaluationHarness:
    """End-to-end ESR / EM / hallucination grading over the eval set."""

    async def test_esr_and_hallucination_thresholds(self):
        if not groq_available():
            pytest.skip("GROQ_API_KEY not set — skipping live ESR grading")
        if not await es_available():
            pytest.skip("Elasticsearch not reachable — skipping live ESR grading")

        from elasticsearch import AsyncElasticsearch
        from src.executor import ExecutorSettings

        es = AsyncElasticsearch(hosts=[ExecutorSettings().elasticsearch_url])
        schema_ctx = eval_schema_ctx(TEST_INDEX)

        # Seed the index
        try:
            await es.indices.delete(index=TEST_INDEX)
        except Exception:
            pass
        await es.indices.create(index=TEST_INDEX, mappings=index_mapping())
        for doc in build_seed_documents(datetime.now(timezone.utc)):
            await es.index(index=TEST_INDEX, id=doc["doc_id"], document=doc)
        await es.indices.refresh(index=TEST_INDEX)

        # Build the pipeline
        generator = KQLGenerator()
        generator.startup()
        validator = KQLValidator()
        repair = RepairAgent(generator=generator, validator=validator)
        executor = ElasticsearchExecutor()
        await executor.startup()
        examples = _load_fallback_examples(FALLBACK_PATH)

        async def matching_ids(kql: str) -> set:
            if not kql.strip():
                return set()
            res = await executor.execute(kql=kql, schema_ctx=schema_ctx, max_results=200)
            return {h["doc_id"] for h in res.hits if "doc_id" in h}

        rows = load_eval_set()
        n = len(rows)
        esr_hits = 0
        em_hits = 0
        hallucinated = 0

        for r in rows:
            try:
                gen_kql = await repair.repair(r["nl"], schema_ctx, examples)
            except Exception:
                continue  # generation failure counts as a miss

            # schema hallucination: generated KQL references unknown fields
            field_errors = _check_field_names(gen_kql, schema_ctx)
            if field_errors:
                hallucinated += 1

            # exact match (normalised)
            if normalize_kql(gen_kql) == normalize_kql(r["kql"]):
                em_hits += 1

            # ESR: do generated and ground-truth return the same docs?
            try:
                gen_ids = await matching_ids(gen_kql)
                truth_ids = await matching_ids(r["kql"])
                if gen_ids == truth_ids:
                    esr_hits += 1
            except Exception:
                pass  # execution failure counts as a miss

        esr = esr_hits / n
        em = em_hits / n
        hallucination_rate = hallucinated / n

        print(f"\nESR={esr:.3f}  EM={em:.3f}  hallucination={hallucination_rate:.3f}  (n={n})")

        # Cleanup
        try:
            await es.indices.delete(index=TEST_INDEX)
        except Exception:
            pass
        await es.close()
        await executor.shutdown()

        assert esr >= ESR_THRESHOLD, f"ESR {esr:.3f} below {ESR_THRESHOLD}"
        assert hallucination_rate <= HALLUCINATION_THRESHOLD, (
            f"hallucination {hallucination_rate:.3f} above {HALLUCINATION_THRESHOLD}"
        )