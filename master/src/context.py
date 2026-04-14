import tiktoken
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from src.intent import IntentResult
from nexgen_shared.schemas import LogRetrievalResult, KnowledgeResult, LogHit, KnowledgeChunk

class RCASynthesisInput(BaseModel):
    query_id: str
    original_query: str
    log_evidence: List[LogHit] = []
    knowledge_context: List[KnowledgeChunk] = []
    reasoning_trace: List[str] = []

class ContextAssembler:
    """
    Validates execution results and parses them efficiently via token pruning & 
    LongContextReorder preventing context poisoning before hitting the reasoning loop.
    """
    def __init__(self, max_tokens: int = 4000):
        self.max_tokens = max_tokens
        try:
            self.tokenizer = tiktoken.get_encoding("cl100k_base")
        except Exception:
            self.tokenizer = None

    def _count_tokens(self, text: str) -> int:
        if self.tokenizer:
            return len(self.tokenizer.encode(text))
        # Fallback heuristic
        return len(text) // 4

    def is_context_sufficient(self, intent: IntentResult, logs: Optional[LogRetrievalResult]) -> bool:
        """
        Determines if there is enough data fetched to proceed with RCASynthesis.
        If logs were required but returned 0 hits, context is flagged insufficient.
        """
        if intent.logs_needed:
            if not logs or not logs.hits or len(logs.hits) == 0:
                return False
        return True

    def assemble(self, original_query: str, query_id: str,
                 log_result: Optional[LogRetrievalResult], 
                 knowledge_result: Optional[KnowledgeResult], 
                 intent: IntentResult) -> RCASynthesisInput:
        
        log_evidence = []
        if log_result and log_result.hits:
            log_evidence = self._prune_and_reorder_logs(log_result.hits)

        knowledge_context = []
        if knowledge_result and knowledge_result.chunks:
            # We assume the external RAG module (via the Mock compactor) handled knowledge pruning already
            knowledge_context = knowledge_result.chunks

        return RCASynthesisInput(
            query_id=query_id,
            original_query=original_query,
            log_evidence=log_evidence,
            knowledge_context=knowledge_context
        )

    def _prune_and_reorder_logs(self, hits: List[LogHit]) -> List[LogHit]:
        """
        Prunes raw logs sliding-window style ensuring we don't breach MAX_TOKENS.
        Applies LongContextReorder to the trimmed remainder.
        """
        current_tokens = 0
        trimmed_hits = []
        
        for hit in hits:
            hit_str = f"{hit.timestamp} {hit.service} {hit.level} {hit.message}"
            tokens = self._count_tokens(hit_str)
            if current_tokens + tokens > self.max_tokens:
                break
            trimmed_hits.append(hit)
            current_tokens += tokens
            
        if not trimmed_hits:
            return []

        # Assuming sequence is naturally sorted by age, LongContextReorder anchors context edges
        ranked = list(trimmed_hits)
        
        reordered = [None] * len(ranked)
        left = 0
        right = len(ranked) - 1
        
        for i, hit in enumerate(ranked):
            if i % 2 == 0:
                reordered[left] = hit
                left += 1
            else:
                reordered[right] = hit
                right -= 1
                
        return reordered
