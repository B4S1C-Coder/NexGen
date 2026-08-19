from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
import asyncio
from datetime import datetime, timezone
import httpx

from qdrant_client import AsyncQdrantClient, QdrantClient

from nexgen_shared.logging import configure_structlog, get_logger
from nexgen_shared.runtime import mock_services_enabled
from nexgen_shared.schemas import KnowledgeChunk, KnowledgeRequest, KnowledgeResult

from .dev_fixtures import mock_knowledge

from .authority import AuthorityScorer
from .compactor import LLMLingua2Compactor
from .conflict import ConflictDetector
from .connectors.local_file import LocalFileConnector
from .debate import MultiAgentDebate
from .dense import DenseRetriever
from .fusion import WRRFFusion
from .id_preservation import TechnicalIDPreservationLayer
from .ingest_service import (
    IngestRequest,
    IngestResponse,
    IngestService,
    OllamaEmbedder,
    UnsupportedSourceError,
)
from .preprocessor import Preprocessor, RankedChunk
from .reranker import CrossEncoderReranker
from .settings import Settings
from .sparse import SparseRetriever


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Configure the RAG service application state for startup and shutdown."""

    settings = Settings()
    app.state.settings = settings
    app.state.mock_services = mock_services_enabled()

    configure_structlog(log_level=settings.log_level, json_format=False)
    app.state.log = get_logger(service="rag", query_id=None)
    app.state.log.info("startup", rag_port=settings.rag_port, mock=app.state.mock_services)

    if app.state.mock_services:
        try:
            yield
        finally:
            app.state.log.info("shutdown")
        return

    # Synchronous client for ingest
    app.state.ingest_service = IngestService(
        qdrant_client=QdrantClient(url=settings.qdrant_url),
        connectors={"local_file": LocalFileConnector(settings.docs_path)},
        preprocessor=Preprocessor(),
        embedder=OllamaEmbedder(
            base_url=settings.llamacpp_embed_server_url,
            model=settings.embedding_model,
        ),
        dense_collection=settings.dense_collection,
        sparse_collection=settings.sparse_collection,
    )

    # Asynchronous client and components for retrieval
    async_qdrant_client = AsyncQdrantClient(url=settings.qdrant_url)
    httpx_client = httpx.AsyncClient()
    app.state.async_qdrant_client = async_qdrant_client
    app.state.httpx_client = httpx_client
    app.state.dense_retriever = DenseRetriever(async_qdrant_client, httpx_client, settings)
    app.state.sparse_retriever = SparseRetriever(async_qdrant_client, settings)
    app.state.fusion = WRRFFusion(settings)
    app.state.reranker = CrossEncoderReranker(settings)
    app.state.authority_scorer = AuthorityScorer()

    # Phase 3 components: conflict detection + compaction
    app.state.conflict_detector = ConflictDetector(settings)
    app.state.debate = MultiAgentDebate(settings)
    app.state.compactor = LLMLingua2Compactor(
        model_name=settings.llmlingua2_model,
    )
    app.state.id_preservation = TechnicalIDPreservationLayer()

    try:
        yield
    finally:
        await httpx_client.aclose()
        await async_qdrant_client.close()
        app.state.log.info("shutdown")


app = FastAPI(title="nexgen-rag", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    """Return a basic liveness response for the RAG service."""

    return {"status": "ok", "service": "rag"}


@app.post("/ingest", response_model=IngestResponse)
async def ingest(request: IngestRequest) -> IngestResponse:
    """Fetch source documents, preprocess them, and upsert them into Qdrant."""

    try:
        return await app.state.ingest_service.ingest(request)
    except UnsupportedSourceError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/knowledge", response_model=KnowledgeResult)
async def knowledge(request: KnowledgeRequest) -> KnowledgeResult:
    """Return knowledge payload from the full RAG retrieval pipeline.

    The pipeline now includes conflict detection, multi-agent debate
    resolution, LLMLingua-2 context compaction, and technical-ID
    preservation — all wired after the authority scoring step.
    """

    dense_retriever: DenseRetriever = app.state.dense_retriever
    sparse_retriever: SparseRetriever = app.state.sparse_retriever
    fusion: WRRFFusion = app.state.fusion
    reranker: CrossEncoderReranker = app.state.reranker
    scorer: AuthorityScorer = app.state.authority_scorer
    conflict_detector: ConflictDetector = app.state.conflict_detector
    debate: MultiAgentDebate = app.state.debate
    compactor: LLMLingua2Compactor = app.state.compactor
    id_preservation: TechnicalIDPreservationLayer = app.state.id_preservation
    log = app.state.log

    if getattr(app.state, "mock_services", False) or mock_services_enabled():
        return mock_knowledge(request)

    # 1. Parallel retrieval
    dense_chunks, sparse_chunks = await asyncio.gather(
        dense_retriever.retrieve(request),
        sparse_retriever.retrieve(request)
    )

    # 2. WRRF Fusion
    w_dense, w_sparse = fusion.classify_query(request.semantic_query)
    fused_chunks = fusion.fuse(dense_chunks, sparse_chunks, w_dense, w_sparse)

    # 3. Cross-Encoder Reranking (limit candidates for performance)
    candidate_chunks = fused_chunks[: request.max_chunks * 2]
    reranked_chunks = reranker.rerank(request.semantic_query, candidate_chunks)

    # 4. Authority Scoring
    scored_chunks = scorer.score(reranked_chunks)

    # 5. Final Top-K Slice
    top_chunks = scored_chunks[: request.max_chunks]

    # 6. Conflict Detection
    conflict_detected = False
    conflict_pairs = conflict_detector.detect_conflicts(top_chunks)

    if conflict_pairs:
        conflict_detected = True
        log.info(
            "conflicts_detected",
            count=len(conflict_pairs),
            query_id=request.query_id,
        )

        # 7. Multi-Agent Debate — resolve each conflict
        for pair in conflict_pairs:
            try:
                winner_chunk = await debate.resolve(pair)
                # Replace the losing chunk with the winner/merged chunk
                top_chunks = _replace_conflict_loser(
                    top_chunks, pair, winner_chunk
                )
            except Exception as exc:
                log.warning(
                    "debate_failed",
                    error=str(exc),
                    chunk_i=pair.chunk_i.chunk_id,
                    chunk_j=pair.chunk_j.chunk_id,
                )
                # On E007 or other failure, keep the higher-scoring chunk
                if pair.chunk_i.score >= pair.chunk_j.score:
                    top_chunks = [
                        c for c in top_chunks
                        if c.chunk_id != pair.chunk_j.chunk_id
                    ]
                else:
                    top_chunks = [
                        c for c in top_chunks
                        if c.chunk_id != pair.chunk_i.chunk_id
                    ]

    # 8. LLMLingua-2 Context Compaction
    original_chunk_texts = [c.content for c in top_chunks]
    compressed_text = compactor.compress(
        original_chunk_texts,
        budget_tokens=request.compression_budget_tokens,
    )

    # 9. Technical ID Preservation
    preservation_result = id_preservation.verify_and_reinject(
        original_chunk_texts,
        compressed_text,
    )
    final_text = preservation_result.text
    total_tokens = len(final_text.split()) if final_text else 0

    if preservation_result.total_reinjections > 0:
        log.info(
            "ids_reinjected",
            count=preservation_result.total_reinjections,
            tags=preservation_result.reinjected_tags,
            query_id=request.query_id,
        )

    # 10. Construct result chunks
    # After compaction, we return individual chunk metadata but with the
    # compressed content distributed back. For simplicity, when compaction
    # produces a single merged string, we keep individual chunk metadata
    # but mark the content as the compressed form.
    retrieved_at = datetime.now(timezone.utc)
    knowledge_chunks = [
        KnowledgeChunk(
            chunk_id=c.chunk_id,
            source_type=c.metadata.source_type,
            source_uri=c.metadata.source_uri,
            authority_tier=c.metadata.authority_tier,
            recency_score=c.metadata.recency_score,
            content=c.content,
            retrieved_at=retrieved_at,
        )
        for c in top_chunks
    ]

    return KnowledgeResult(
        query_id=request.query_id,
        status="success",
        chunks=knowledge_chunks,
        total_tokens_after_compression=total_tokens,
        conflict_detected=conflict_detected,
        error=None,
    )


def _replace_conflict_loser(
    chunks: list[RankedChunk],
    pair: "ConflictPair",  # noqa: F821 — forward ref to avoid circular import
    winner: RankedChunk,
) -> list[RankedChunk]:
    """Replace the losing chunk(s) in a conflict with the winner/merged chunk.

    If the winner is one of the original chunks, the other is simply removed.
    If the winner is a merged chunk (new chunk_id ending with '-merged'),
    both originals are replaced with the merged version.

    Parameters:
        chunks: Current list of top-k chunks.
        pair: The ConflictPair that was resolved.
        winner: The winning or merged RankedChunk.

    Returns:
        Updated chunk list with the conflict resolved.
    """
    loser_ids = {pair.chunk_i.chunk_id, pair.chunk_j.chunk_id}

    if winner.chunk_id.endswith("-merged"):
        # Replace both with the merged chunk
        result = [c for c in chunks if c.chunk_id not in loser_ids]
        result.append(winner)
    elif winner.chunk_id == pair.chunk_i.chunk_id:
        # Remove chunk_j (loser)
        result = [c for c in chunks if c.chunk_id != pair.chunk_j.chunk_id]
    else:
        # Remove chunk_i (loser)
        result = [c for c in chunks if c.chunk_id != pair.chunk_i.chunk_id]

    return result
