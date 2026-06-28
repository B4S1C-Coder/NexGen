import httpx

from nexgen_shared.errors import E007KnowledgeConflictUnresolved
from src.conflict import ConflictPair
from src.preprocessor import RankedChunk
from src.settings import Settings


class MultiAgentDebate:
    """Resolves knowledge conflicts using a multi-agent LLM debate."""

    def __init__(self, settings: Settings, http_client: httpx.AsyncClient) -> None:
        self.settings = settings
        self.http_client = http_client

        with open("prompts/debate_agent.txt", "r", encoding="utf-8") as f:
            self.agent_prompt = f.read()

        with open("prompts/debate_aggregator.txt", "r", encoding="utf-8") as f:
            self.aggregator_prompt = f.read()

    async def _call_llm(self, prompt: str) -> str:
        """Wrapper to call the generation LLM endpoint."""
        url = f"{self.settings.llamacpp_generate_server_url.rstrip('/')}/completion"
        payload = {
            "prompt": prompt,
            "n_predict": 300,
            "temperature": 0.1,
            "stop": ["</s>"]
        }
        try:
            response = await self.http_client.post(url, json=payload, timeout=30.0)
            response.raise_for_status()
            data = response.json()
            return data.get("content", "").strip()
        except Exception as e:
            raise E007KnowledgeConflictUnresolved(f"LLM call failed: {e}")

    async def resolve(self, conflict: ConflictPair) -> RankedChunk:
        """
        Resolves a conflict between two chunks via multi-agent debate.
        Returns the winning chunk or a merged chunk.
        """
        chunk1 = conflict.chunk_a
        chunk2 = conflict.chunk_b

        for _ in range(self.settings.max_debate_rounds):
            # Agent 1 defends chunk 1
            prompt1 = self.agent_prompt.replace("{chunk_content}", chunk1.content).replace("{opposing_chunk_content}", chunk2.content)
            arg1 = await self._call_llm(prompt1)

            # Agent 2 defends chunk 2
            prompt2 = self.agent_prompt.replace("{chunk_content}", chunk2.content).replace("{opposing_chunk_content}", chunk1.content)
            arg2 = await self._call_llm(prompt2)

            # Aggregator evaluates
            agg_prompt = (
                f"{self.aggregator_prompt}\n\n"
                f"Argument 1:\n{arg1}\n\n"
                f"Argument 2:\n{arg2}\n\n"
                f"Chunk 1 Recency: {chunk1.metadata.created_at}, Tier: {chunk1.metadata.authority_tier}\n"
                f"Chunk 2 Recency: {chunk2.metadata.created_at}, Tier: {chunk2.metadata.authority_tier}"
            )
            decision = await self._call_llm(agg_prompt)

            decision_upper = decision.upper()
            if "WINNER: 1" in decision_upper:
                return chunk1
            elif "WINNER: 2" in decision_upper:
                return chunk2
            elif "WINNER: MERGE" in decision_upper:
                # Extract justification as the merged content
                parts = decision.split("\n", 1)
                merged_content = parts[1].strip() if len(parts) > 1 else decision
                # Limit to roughly 200 tokens via word split fallback
                merged_content = " ".join(merged_content.split()[:200])

                merged_chunk = RankedChunk(
                    chunk_id=f"merged-{chunk1.chunk_id}-{chunk2.chunk_id}",
                    content=merged_content,
                    metadata=chunk1.metadata,
                    score=max(chunk1.score, chunk2.score)
                )
                return merged_chunk

        raise E007KnowledgeConflictUnresolved("Max debate rounds exhausted without consensus.")
