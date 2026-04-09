from datetime import datetime, timezone
import uuid
from nexgen_shared.schemas import KnowledgeRequest, KnowledgeResult, KnowledgeChunk
from .temporal import MockTemporalFilter
from .dense import MockDenseRetriever
from .sparse import MockSparseRetriever
from .fusion import MockWRRFFusion
from .reranker import MockCrossEncoderReranker
from .authority import MockAuthorityScorer
from .conflict import MockConflictDetector
from .debate import MockMultiAgentDebate
from .compactor import MockLLMLingua2Compactor
from .id_preservation import MockIDPreservation

class MockRAGPipeline:
    def __init__(self):
        self.temporal = MockTemporalFilter()
        self.dense = MockDenseRetriever()
        self.sparse = MockSparseRetriever()
        self.fusion = MockWRRFFusion()
        self.reranker = MockCrossEncoderReranker()
        self.authority = MockAuthorityScorer()
        self.conflict = MockConflictDetector()
        self.debate = MockMultiAgentDebate()
        self.compactor = MockLLMLingua2Compactor()
        self.id_pres = MockIDPreservation()

    async def retrieve_knowledge(self, request: KnowledgeRequest) -> KnowledgeResult:
        filt = self.temporal.build_filter(request.time_window)
        d_res = self.dense.retrieve(request.semantic_query)
        s_res = self.sparse.retrieve(request.semantic_query)
        f_res = self.fusion.fuse(d_res, s_res)
        f_res = self.reranker.rerank(request.semantic_query, f_res)
        f_res = self.authority.score(f_res)
        conflicts = self.conflict.detect(f_res)
        if conflicts:
            self.debate.debate(conflicts)
        final_tokens = self.compactor.compress(f_res, request.compression_budget_tokens)
        f_res = self.id_pres.verify(f_res)
        chunks = [
            KnowledgeChunk(
                chunk_id=str(uuid.uuid4()), source_type="runbook", source_uri="mock://doc",
                authority_tier="A", recency_score=0.9, content=res["text"], retrieved_at=datetime.now(timezone.utc)
            ) for res in f_res
        ]
        return KnowledgeResult(
            query_id=request.query_id, status="success", chunks=chunks,
            total_tokens_after_compression=final_tokens, conflict_detected=bool(conflicts), error=None
        )
