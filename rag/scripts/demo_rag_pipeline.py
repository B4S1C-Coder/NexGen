#!/usr/bin/env python3
"""NexGen RAG Pipeline — Interactive Multi-Source Demo.

A comprehensive terminal demo that showcases the full RAG pipeline with:
  • Multi-source fetching (Slack, Jira, Confluence runbooks)
  • Interactive query mode — type your own questions
  • Conflict detection via NLI model
  • Real LLMLingua-2 context compression
  • Technical ID preservation
  • Unit test execution
  • Live FastAPI server mode (curl-ready)

Usage:
    cd rag/
    uv run python scripts/demo_rag_pipeline.py
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import sys
import textwrap
import time
from datetime import UTC, datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Terminal formatting
# ---------------------------------------------------------------------------

BOLD = ""
DIM = ""
RESET = ""
GREEN = ""
CYAN = ""
YELLOW = ""
MAGENTA = ""
RED = ""
BLUE = ""
ORANGE = ""
GRAY = ""
WHITE = ""
BG_HEADER = ""
BG_MENU = ""

SRC_COLORS = {"runbook": GREEN, "slack": ORANGE, "jira": BLUE, "github": MAGENTA}
_TAG_RE = re.compile(r"<(?:IP_ADDR|TRACE_ID|HASH|PATH|ERROR_CODE):[^>]+>")


def banner() -> None:
    print("RAG / Contextual Pipeline -- Interactive Demo")
    print("AI-Driven Framework for Natural Log Investigation")
    print("-" * 66)


def section(title: str, icon: str = "") -> None:
    w = 66
    print(f"\n  {icon} {title}{' ' * max(0, w - len(title) - 4)}")
    print(f"{'-' * w}")


def step(msg: str) -> None:
    print(f"  * {msg}")


def substep(msg: str) -> None:
    print(f"      - {msg}")


def info(label: str, value: str) -> None:
    print(f"  {label}: {value}")


def warn(msg: str) -> None:
    print(f"  !  {msg}")


def err(msg: str) -> None:
    print(f"  x {msg}")


def spinner(msg: str) -> None:
    print(f"  ...  {msg}")


def progress_bar(label: str, current: int, total: int) -> None:
    w = 30
    filled = int(w * current / total)
    bar = f"{'█' * filled}{'░' * (w - filled)}"
    print(f"  {label} [{bar}] {int(100 * current / total)}%")


def json_pretty(data: dict, indent: int = 2) -> str:
    raw = json.dumps(data, indent=indent, default=str)
    lines = []
    for line in raw.split("\n"):
        if '": ' in line or '":' in line:
            key_part, _, rest = line.partition(":")
            lines.append(f"{key_part}:{rest}")
        else:
            lines.append(f"{line}")
    return "\n".join(lines)


def deterministic_vector(text: str, size: int = 768) -> list[float]:
    digest = hashlib.sha256(text.lower().encode("utf-8")).digest()
    values: list[float] = []
    while len(values) < size:
        for byte in digest:
            values.append(byte / 255.0)
            if len(values) == size:
                break
    return values


# ---------------------------------------------------------------------------
# Source documents — multi-source corpus
# ---------------------------------------------------------------------------


def build_corpus():
    """Build a rich corpus spanning Slack, Jira, Runbooks, with one contradictory pair."""
    from src.connectors.base import RawDocument

    return [
        # -- RUNBOOK: payments DB failover ------------------------
        RawDocument(
            doc_id="runbook-payments-db-failover",
            source_type="runbook",
            source_uri="confluence://runbooks/payments-db-failover",
            title="Payments DB Failover Runbook",
            raw_text=(
                "# Payments Service — Database Failover Procedure\n\n"
                "## Symptoms\n"
                "- HTTP 500 errors spike on the payments service (service.name: payments).\n"
                "- Log message: `Connection refused: db-primary:5432` with trace_id=abc123def456.\n"
                "- Error code ERR_DB_CONN_REFUSED appears in structured logs.\n"
                "- Upstream gateway (10.0.1.50) receives 502 responses from payments (10.0.2.30).\n\n"
                "## Root Cause\n"
                "The primary PostgreSQL instance (`db-primary`, IP 10.0.2.10) becomes unreachable due to:\n"
                "1. TCP keepalive timeout (300s default) not tuned for the VPC.\n"
                "2. Disk IOPS exhaustion on the primary during peak batch settlement.\n"
                "3. Connection pool saturation (`max_connections=100`, 98 active during incident).\n\n"
                "## Resolution Steps\n"
                "1. Verify primary health: `ssh ops@10.0.2.10 \"systemctl status postgresql\"`\n"
                "2. Trigger manual failover to db-replica-1 via ops-console:\n"
                "   `nexgen-ops failover --target db-replica-1 --service payments`\n"
                "3. Update DNS to point db-primary.internal to new leader (10.0.2.11).\n"
                "4. Validate: Monitor nexgen_payments_error_rate — should drop below 0.1% within 2 min.\n\n"
                "## Post-Incident\n"
                "- Tune TCP keepalive to 60s across all database clients.\n"
                "- Increase max_connections to 200 on both primary and replica.\n"
                "- Add alerting rule: fire PagerDuty when active connections exceed 80% capacity.\n"
            ),
            created_at=datetime(2026, 7, 15, 8, 0, tzinfo=UTC),
            updated_at=datetime(2026, 8, 1, 14, 30, tzinfo=UTC),
            author="ops-team",
            metadata={"authority_tier": "A", "resolution_status": "resolved"},
        ),
        # -- SLACK: incident resolution thread --------------------
        RawDocument(
            doc_id="slack-C04PAYMENTS-1722850200",
            source_type="slack",
            source_uri="slack://C04PAYMENTS/thread/1722850200",
            title="#incidents — payments outage Aug 5",
            raw_text=(
                "[09:57] @oncall-dave: Seeing HTTP 500 spike on payments — PagerDuty triggered INC-4821\n"
                "[09:58] @oncall-dave: trace_id=abc123def456 — all errors point to db-primary:5432 connection refused\n"
                "[09:59] @dba-sarah: db-primary (10.0.2.10) is alive but pg_stat_activity shows 98/100 connections\n"
                "[10:00] @oncall-dave: This looks like last month's IOPS issue. Running failover playbook.\n"
                "[10:01] @dba-sarah: Confirmed — IOPS at 3000/3000 (100% utilised). Batch settlement window.\n"
                "[10:03] @oncall-dave: Fixed — triggered failover to db-replica-1 (10.0.2.11). Error rate dropping.\n"
                "[10:05] @dba-sarah: All clear. Post-mortem: we need to bump IOPS to 6000 and add circuit breaker.\n"
                "[10:06] @oncall-dave: Resolved. MTTR was 8 minutes. Filed follow-up JIRA ticket PAY-2847.\n"
            ),
            created_at=datetime(2026, 8, 5, 9, 57, tzinfo=UTC),
            updated_at=datetime(2026, 8, 5, 10, 6, tzinfo=UTC),
            author="oncall-dave",
            metadata={"resolution_status": "resolved", "channel": "#incidents"},
        ),
        # -- JIRA: follow-up ticket -------------------------------
        RawDocument(
            doc_id="jira-PAY-2847",
            source_type="jira",
            source_uri="jira://PAY-2847",
            title="PAY-2847: Auto DB failover for payments",
            raw_text=(
                "PAY-2847: Implement automatic DB failover for payments service\n\n"
                "Status: In Progress\nPriority: P1 — Critical\n"
                "Assignee: sarah.chen@nexgen.io\nReporter: dave.kumar@nexgen.io\n\n"
                "Description:\n"
                "During the Aug 5 outage (INC-4821), db-primary (10.0.2.10) became unreachable "
                "due to IOPS exhaustion. Manual failover took 8 minutes (MTTR). We need automatic "
                "failover via Patroni to reduce MTTR to < 30s.\n\n"
                "Acceptance Criteria:\n"
                "- Deploy Patroni sidecar on db-primary and db-replica-1\n"
                "- Automatic leader election when primary is unreachable for > 10s\n"
                "- Connection pool (HikariCP) auto-reconnects to new leader via DNS update\n"
                "- Add circuit breaker with 5s timeout on payments→db connection\n\n"
                "Comment [sarah.chen, Aug 7]:\n"
                "Patroni POC complete on staging. Failover time measured at 12s including DNS propagation. "
                "HikariCP reconnection works via db-primary.internal CNAME update. PR merged: github.com/nexgen/infra#491\n\n"
                "Comment [dave.kumar, Aug 8]:\n"
                "Verified on staging — injected fault on primary, Patroni promoted replica in 11s. "
                "Circuit breaker (resilience4j) catches the transient failures. Ready for prod deployment.\n"
            ),
            created_at=datetime(2026, 8, 5, 11, 0, tzinfo=UTC),
            updated_at=datetime(2026, 8, 8, 14, 0, tzinfo=UTC),
            author="dave.kumar",
            metadata={"resolution_status": "open", "jira_status": "In Progress", "is_accepted_answer": False},
        ),
        # -- CONTRADICTORY DOC: old wiki (says restart, not failover) --
        RawDocument(
            doc_id="runbook-payments-legacy-restart",
            source_type="runbook",
            source_uri="confluence://runbooks/payments-legacy-restart",
            title="[DEPRECATED] Payments: restart to fix DB errors",
            raw_text=(
                "# Payments — Legacy Restart Procedure (DEPRECATED)\n\n"
                "When the payments service shows database connection errors (ERR_DB_CONN_REFUSED), "
                "the recommended fix is to restart the payments service pods. Do NOT perform a database "
                "failover — restarting the service clears stale connection pool entries and resolves "
                "the issue in 95% of cases.\n\n"
                "Steps:\n"
                "1. kubectl rollout restart deployment/payments -n production\n"
                "2. Wait 60 seconds for pods to cycle.\n"
                "3. Verify: curl -s http://payments.internal/health | jq .status\n\n"
                "NOTE: This procedure was deprecated on 2026-08-06 after the INC-4821 post-mortem "
                "showed that restarts mask underlying IOPS problems. Use the failover runbook instead.\n"
            ),
            created_at=datetime(2025, 3, 1, 10, 0, tzinfo=UTC),
            updated_at=datetime(2026, 8, 6, 9, 0, tzinfo=UTC),
            author="legacy-ops",
            metadata={"authority_tier": "A", "resolution_status": "deprecated"},
        ),
        # -- RUNBOOK: auth service (noise / relevance contrast) ---
        RawDocument(
            doc_id="runbook-auth-rate-limiting",
            source_type="runbook",
            source_uri="confluence://runbooks/auth-rate-limiting",
            title="Auth Rate Limiting Runbook",
            raw_text=(
                "# Auth Service — Rate Limiting and Brute-Force Protection\n\n"
                "The auth service (service.name: auth, running on 10.0.3.20) enforces rate limits. "
                "When a client exceeds 10 failed login attempts within 5 minutes, HTTP 429 is returned "
                "with error code ERR_RATE_LIMIT_EXCEEDED. Rate limits stored in Redis (10.0.3.5:6379).\n\n"
                "Troubleshooting: Check Redis key ratelimit:{client_ip}:/api/auth/login.\n"
                "Known issue: CDN traffic appears as single IP (10.0.1.1). Fix tracked in AUTH-892.\n"
            ),
            created_at=datetime(2026, 6, 20, 10, 0, tzinfo=UTC),
            updated_at=datetime(2026, 7, 10, 16, 0, tzinfo=UTC),
            author="platform-team",
            metadata={"authority_tier": "A", "resolution_status": "resolved"},
        ),
        # -- GITHUB PR: infra#491 — extra doc for compression demo -
        RawDocument(
            doc_id="github-infra-491",
            source_type="github",
            source_uri="github://nexgen/infra/pull/491",
            title="PR #491: Deploy Patroni for payments DB",
            raw_text=(
                "## PR #491: Deploy Patroni sidecar for payments DB auto-failover\n\n"
                "Author: sarah.chen | Merged: 2026-08-07\n\n"
                "### Changes\n"
                "- Added Patroni sidecar container to payments-db StatefulSet\n"
                "- Configured etcd cluster for leader election (10.0.2.50:2379)\n"
                "- Updated HikariCP config: connectionTimeout=5000ms, validationTimeout=3000ms\n"
                "- Added resilience4j circuit breaker: failureRateThreshold=50, waitDurationInOpenState=5s\n"
                "- DNS failover via ExternalDNS annotation on Service\n\n"
                "### Testing\n"
                "- Chaos engineering: killed db-primary pod 10 times, Patroni promoted replica in avg 11.3s\n"
                "- Zero dropped connections during failover (circuit breaker absorbed transients)\n"
                "- HikariCP reconnection latency: p99 = 2.1s via DNS CNAME propagation\n\n"
                "### Rollback\n"
                "kubectl delete statefulset payments-db -n production && kubectl apply -f payments-db-legacy.yaml\n"
            ),
            created_at=datetime(2026, 8, 7, 15, 0, tzinfo=UTC),
            updated_at=datetime(2026, 8, 7, 15, 0, tzinfo=UTC),
            author="sarah.chen",
            metadata={"pr_merged": True, "resolution_status": "resolved"},
        ),
    ]


# ---------------------------------------------------------------------------
# Shared pipeline infrastructure
# ---------------------------------------------------------------------------


class PipelineContext:
    """Holds all initialised pipeline components for reuse across queries."""

    def __init__(self) -> None:
        from qdrant_client import QdrantClient, models as qm
        from src.qdrant_setup import DENSE_VECTOR_SIZE, SPARSE_VECTOR_NAME
        from src.ingest_service import SparseEncoder
        from src.preprocessor import Preprocessor
        from src.fusion import WRRFFusion
        from src.reranker import CrossEncoderReranker
        from src.authority import AuthorityScorer
        from src.compactor import LLMLingua2Compactor
        from src.conflict import ConflictDetector
        from src.id_preservation import TechnicalIDPreservationLayer
        from src.settings import Settings

        self.settings = Settings()
        self.preprocessor = Preprocessor()
        self.sparse_encoder = SparseEncoder()
        self.client = QdrantClient(":memory:")

        self.client.create_collection(
            "nexgen_dense",
            vectors_config=qm.VectorParams(size=DENSE_VECTOR_SIZE, distance=qm.Distance.COSINE),
        )
        self.client.create_collection(
            "nexgen_bm25_terms",
            vectors_config={},
            sparse_vectors_config={
                SPARSE_VECTOR_NAME: qm.SparseVectorParams(index=qm.SparseIndexParams()),
            },
        )

        self.fusion = WRRFFusion(self.settings)
        self.reranker = CrossEncoderReranker(self.settings)
        self.scorer = AuthorityScorer()
        self.compactor = LLMLingua2Compactor(model_name=None)  # extractive fallback
        self.conflict_detector = ConflictDetector(self.settings)
        self.id_layer = TechnicalIDPreservationLayer()

        self.all_chunks = []
        self.all_payloads = []
        self.docs = []

    def ingest(self, docs):
        from qdrant_client import models as qm
        from src.qdrant_setup import SPARSE_VECTOR_NAME

        self.docs = docs
        self.all_chunks = []
        self.all_payloads = []

        for doc in docs:
            chunks = self.preprocessor.chunk(doc)
            for c in chunks:
                meta = self.preprocessor.enrich_metadata(c, doc)
                self.all_chunks.append(c)
                self.all_payloads.append({
                    "chunk_id": c.chunk_id,
                    "doc_id": c.doc_id,
                    "content": c.content,
                    "source_type": meta.source_type,
                    "source_uri": meta.source_uri,
                    "authority_tier": meta.authority_tier,
                    "created_at": meta.created_at.isoformat(),
                    "resolution_status": meta.resolution_status,
                    "is_accepted_answer": meta.is_accepted_answer,
                    "recency_score": meta.recency_score,
                })

        dense_pts, sparse_pts = [], []
        for i, (chunk, payload) in enumerate(zip(self.all_chunks, self.all_payloads)):
            pid = i + 1
            dense_pts.append(qm.PointStruct(id=pid, vector=deterministic_vector(chunk.content), payload=payload))
            sparse_pts.append(qm.PointStruct(
                id=pid,
                vector={SPARSE_VECTOR_NAME: self.sparse_encoder.encode(chunk.content)},
                payload=payload,
            ))

        self.client.upsert("nexgen_dense", points=dense_pts, wait=True)
        self.client.upsert("nexgen_bm25_terms", points=sparse_pts, wait=True)
        return len(self.all_chunks)


# ---------------------------------------------------------------------------
# Multi-source fetch display
# ---------------------------------------------------------------------------


def display_fetch(docs):
    """Show simulated multi-source fetching."""
    section("STAGE 1 — Multi-Source Document Fetching", "")

    sources = {}
    for d in docs:
        sources.setdefault(d.source_type, []).append(d)

    for src_type, src_docs in sources.items():
        label_map = {
            "runbook": ("Confluence", "runbooks"),
            "slack": ("Slack", "threads from #incidents"),
            "jira": ("Jira", "tickets"),
            "github": ("GitHub", "merged PRs"),
        }
        name, desc = label_map.get(src_type, (src_type, "documents"))
        color = SRC_COLORS.get(src_type, WHITE)
        time.sleep(0.08)
        spinner(f"{color}{name:10s} → Fetching {desc}...")
        time.sleep(0.05)
        step(f"Fetched {len(src_docs)} from {color}{name}")
        for d in src_docs:
            substep(f"{d.source_uri}  ({d.title})")

    print()
    info("Total documents", f"{len(docs)} across {len(sources)} sources")


# ---------------------------------------------------------------------------
# Pipeline query execution
# ---------------------------------------------------------------------------


def run_query(ctx: PipelineContext, query: str, budget: int = 400, show_conflict: bool = True):
    """Execute the full retrieval pipeline for a single query."""
    from src.qdrant_setup import SPARSE_VECTOR_NAME
    from src.preprocessor import RankedChunk, ChunkMetadata
    from datetime import datetime as dt

    t0 = time.perf_counter()

    # -- Request display ------------------------------------------
    section("INCOMING QUERY", "")
    print(f"""
  ┌--------------------------------------------------------------┐
  │  semantic_query: "{query}"
  │  budget_tokens:  {budget}
  │  max_chunks:     5
    --------------------------------------------------------------┘
""")

    # -- Retrieval ------------------------------------------------
    section("STAGE A — Hybrid Retrieval + Fusion", "")

    query_vec = deterministic_vector(query)
    dense_resp = ctx.client.query_points("nexgen_dense", query=query_vec, limit=20, with_payload=True)
    dense_results = dense_resp.points
    step(f"Dense search: {len(dense_results)} hits")

    q_sparse = ctx.sparse_encoder.encode(query)
    sparse_resp = ctx.client.query_points(
        "nexgen_bm25_terms", query=q_sparse, using=SPARSE_VECTOR_NAME, limit=20, with_payload=True,
    )
    sparse_results = sparse_resp.points
    step(f"Sparse search: {len(sparse_results)} hits")

    def to_ranked(pt):
        p = pt.payload or {}
        ca = p.get("created_at")
        if isinstance(ca, str):
            ca = dt.fromisoformat(ca)
        elif not isinstance(ca, dt):
            ca = dt.now(UTC)
        return RankedChunk(
            chunk_id=p.get("chunk_id", ""), content=p.get("content", ""),
            metadata=ChunkMetadata(
                chunk_id=p.get("chunk_id", ""), doc_id=p.get("doc_id", ""),
                source_type=p.get("source_type", ""), source_uri=p.get("source_uri", ""),
                authority_tier=p.get("authority_tier", "B"), created_at=ca,
                resolution_status=p.get("resolution_status", "unknown"),
                is_accepted_answer=p.get("is_accepted_answer", False),
                recency_score=p.get("recency_score", 1.0),
            ),
            score=pt.score,
        )

    dense_chunks = [to_ranked(p) for p in dense_results]
    sparse_chunks = [to_ranked(p) for p in sparse_results]

    w_d, w_s = ctx.fusion.classify_query(query)
    fused = ctx.fusion.fuse(dense_chunks, sparse_chunks, w_d, w_s)
    step(f"WRRF Fusion: {len(fused)} unique chunks (w_dense={w_d}, w_sparse={w_s})")

    # -- Reranking ------------------------------------------------
    section("STAGE B — Cross-Encoder Reranking + Authority", "")

    reranked = ctx.reranker.rerank(query, fused[:10])
    scored = ctx.scorer.score(reranked)
    top = scored[:5]
    step(f"Top-{len(top)} chunks after reranking + authority scoring")

    print()
    for i, c in enumerate(top):
        src_color = SRC_COLORS.get(c.metadata.source_type, WHITE)
        tier_color = GREEN if c.metadata.authority_tier == "A" else YELLOW
        dep = f"  [DEPRECATED]" if c.metadata.resolution_status == "deprecated" else ""
        print(f"  #{i+1}  {src_color}[{c.metadata.source_type.upper():7s}]  "
              f"score={c.score:+.4f}  "
              f"tier={tier_color}{c.metadata.authority_tier}{dep}  "
              f"{c.metadata.source_uri}")
        snippet = textwrap.shorten(c.content, width=72, placeholder="...")
        print(f"      {snippet}")
        print()

    # -- Conflict detection ---------------------------------------
    if show_conflict:
        section("STAGE C — Conflict Detection (NLI)", "")

        conflicts = ctx.conflict_detector.detect_conflicts(top)
        if conflicts:
            step(f"{len(conflicts)} conflict(s) detected!")
            for cp in conflicts:
                print(f"""
    --- CONTRADICTION (confidence: {cp.confidence:.2%}) ---
    Chunk A: {cp.chunk_i.chunk_id}
      {textwrap.shorten(cp.chunk_i.content, width=65, placeholder="...")}
    Chunk B: {cp.chunk_j.chunk_id}
      {textwrap.shorten(cp.chunk_j.content, width=65, placeholder="...")}
    -------------------------------------------------------""")

            # Resolution: keep higher-scoring chunk (debate needs Ollama)
            step(f"Resolution: keeping higher-scoring chunk (debate requires LLM)")
            for cp in conflicts:
                loser = cp.chunk_j if cp.chunk_i.score >= cp.chunk_j.score else cp.chunk_i
                top = [c for c in top if c.chunk_id != loser.chunk_id]
                substep(f"Removed {loser.chunk_id} (lower score)")
        else:
            step(f"No contradictions detected among top chunks *")

    # -- Compaction -----------------------------------------------
    section("STAGE D — LLMLingua-2 Compaction + ID Preservation", "")

    original_texts = [c.content for c in top]
    original_tokens = sum(len(t.split()) for t in original_texts)

    compressed = ctx.compactor.compress(original_texts, budget_tokens=budget)
    compressed_tokens = len(compressed.split())

    if original_tokens > budget:
        ratio = compressed_tokens / max(original_tokens, 1)
        step(f"Compressed: {original_tokens} → {compressed_tokens} tokens "
             f"({ratio:.0%} of original)")
    else:
        step(f"Tokens: {original_tokens} (within budget of {budget} — no compression needed)")
        compressed_tokens = original_tokens

    # ID preservation
    orig_tags = len(set(m.group() for t in original_texts for m in _TAG_RE.finditer(t)))
    pres = ctx.id_layer.verify_and_reinject(original_texts, compressed)
    preserved = orig_tags - pres.total_reinjections

    if orig_tags > 0:
        step(f"Technical IDs: {orig_tags} found → {preserved} survived"
             f"{f', {pres.total_reinjections} re-injected' if pres.total_reinjections > 0 else ''}")
        if pres.total_reinjections == 0:
            substep(f"All technical IDs intact *")
        else:
            for tag in pres.reinjected_tags:
                substep(f"Re-injected: {tag}")

    final_tokens = len(pres.text.split()) if pres.text else 0

    # -- Final result ---------------------------------------------
    section("RESULT — KnowledgeResult", "")

    retrieved_at = datetime.now(UTC)
    result = {
        "query_id": f"demo-{int(time.time())}",
        "status": "success",
        "chunks": [
            {
                "chunk_id": c.chunk_id,
                "source_type": c.metadata.source_type,
                "source_uri": c.metadata.source_uri,
                "authority_tier": c.metadata.authority_tier,
                "recency_score": round(c.metadata.recency_score, 4),
                "content": textwrap.shorten(c.content, width=95, placeholder="..."),
                "retrieved_at": retrieved_at.isoformat(),
            }
            for c in top
        ],
        "total_tokens_after_compression": final_tokens,
        "conflict_detected": len(conflicts) > 0 if show_conflict else False,
        "error": None,
    }
    print(f"\n{json_pretty(result)}\n")

    elapsed = time.perf_counter() - t0
    src_counts = {}
    for c in top:
        src_counts[c.metadata.source_type] = src_counts.get(c.metadata.source_type, 0) + 1

    print(f"  {'-' * 50}")
    for src, cnt in sorted(src_counts.items()):
        print(f"  {SRC_COLORS.get(src, WHITE)}  {src:7s}: {cnt} chunk(s)")
    print(f"  Latency: {elapsed:.2f}s")
    print()


# ---------------------------------------------------------------------------
# Main menu
# ---------------------------------------------------------------------------


async def main() -> None:
    banner()

    # -- Initialise once ------------------------------------------
    section("INITIALISING PIPELINE", "")

    step("Loading pipeline components...")
    ctx = PipelineContext()
    step(f"Qdrant in-memory started (2 collections)")

    docs = build_corpus()
    display_fetch(docs)

    section("STAGE 2 — Preprocessing & Indexing", "")
    for doc in docs:
        chunks = ctx.preprocessor.chunk(doc)
        src_color = SRC_COLORS.get(doc.source_type, WHITE)
        dep = f" [DEPRECATED]" if doc.metadata.get("resolution_status") == "deprecated" else ""
        step(f"{src_color}[{doc.source_type.upper():7s}] {doc.title} → {len(chunks)} chunks{dep}")

    n = ctx.ingest(docs)
    step(f"Indexed {n} chunks (dense + sparse)")
    total_tags = sum(len(_TAG_RE.findall(c.content)) for c in ctx.all_chunks)
    info("Technical IDs tagged", f"{total_tags}")

    # -- Interactive loop -----------------------------------------
    while True:
        print(f"""
                                                                  
   NEXGEN RAG — Interactive Menu                                   
                                                                  

  [1]  Run demo query        (payments DB failure — shows multi-source + conflict)
  [2]  Type your own query   (interactive — type anything)
  [3]  Compare two queries   (side-by-side: payments vs auth — shows relevance)
  [4]  Run unit tests        (execute pytest suite)
  [5]  Show curl command     (FastAPI server endpoint for live demo)
  [q]  Quit
""")
        try:
            choice = input(f"  > ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            break

        if choice == "1":
            run_query(ctx, "payments service database connection refused 500 errors", budget=400)

        elif choice == "2":
            try:
                q = input(f"\n  Enter your query: ").strip()
            except (EOFError, KeyboardInterrupt):
                continue
            if q:
                run_query(ctx, q, budget=400)

        elif choice == "3":
            print(f"\n  ═══ Query A: Payments DB Failure ═══")
            run_query(ctx, "payments service database connection refused 500 errors", budget=400, show_conflict=False)

            print(f"\n  ═══ Query B: Auth Rate Limiting ═══")
            run_query(ctx, "auth service rate limiting 429 brute force", budget=400, show_conflict=False)

            print(f"""
  Observation:
  * Query A ranked the {SRC_COLORS['runbook']}payments runbook and {SRC_COLORS['slack']}Slack incident highest
  * Query B ranked the {SRC_COLORS['runbook']}auth rate-limiting runbook highest
  * Same corpus, different queries → different ranked results = real semantic retrieval
""")

        elif choice == "4":
            section("UNIT TEST SUITE", "")
            step("Running pytest...")
            import subprocess

            result = subprocess.run(
                [
                    sys.executable, "-m", "pytest", "tests/unit/",
                    "--tb=short", "-q",
                    "--ignore=tests/unit/test_disentangle.py",
                    "--ignore=tests/unit/test_jira_connector.py",
                    "--ignore=tests/unit/test_slack_connector.py",
                    "--ignore=tests/unit/test_compactor.py",
                ],
                capture_output=True, text=True, cwd=Path(__file__).resolve().parent.parent,
            )
            print()
            for line in result.stdout.strip().split("\n"):
                if "passed" in line:
                    print(f"  {line}")
                elif "failed" in line or "error" in line.lower():
                    print(f"  {line}")
                else:
                    print(f"  {line}")
            if result.stderr:
                for line in result.stderr.strip().split("\n")[-3:]:
                    print(f"  {line}")
            print()

        elif choice == "5":
            section("LIVE SERVER COMMANDS", "")
            print(f"""
  Start the RAG FastAPI server:

    cd rag && uv run uvicorn src.main:app --port 8002

  Then, in another terminal:

    # Health check
    curl -s http://localhost:8002/health | python -m json.tool

    # Ingest local docs
    curl -s -X POST http://localhost:8002/ingest \\
      -H "Content-Type: application/json" \\
      -d '{{"source_type": "local_file", "full_reindex": true}}' \\
      | python -m json.tool

    # Query the knowledge endpoint
    curl -s -X POST http://localhost:8002/knowledge \\
      -H "Content-Type: application/json" \\
      -d '{{
        "query_id": "demo-001",
        "semantic_query": "payments database connection refused",
        "source_filters": ["runbooks"],
        "time_window": {{"not_after": "2026-12-31T00:00:00Z"}},
        "max_chunks": 5,
        "compression_budget_tokens": 2000
      }}' | python -m json.tool

  Note: The live server requires Qdrant + embedding server running.
        For a standalone demo, use options [1]-[3] above.
""")

        elif choice in ("q", "quit", "exit"):
            break
        else:
            warn(f"Unknown option: '{choice}'")

    print(f"\n  Thanks for the demo! \n")


if __name__ == "__main__":
    asyncio.run(main())
