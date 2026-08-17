"""Multi-agent debate module for resolving knowledge conflicts (rag.md §5.2).

When the ConflictDetector identifies contradictions between retrieved chunks,
this module runs a structured debate between two LLM agents — each defending
one of the conflicting chunks — and an aggregator agent that decides the
winner or merges them into a reconciled summary.
"""

from __future__ import annotations

import re
from pathlib import Path

import httpx

from nexgen_shared.errors import E007KnowledgeConflictUnresolved

from .conflict import ConflictPair
from .preprocessor import ChunkMetadata, RankedChunk
from .settings import Settings

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"


class MultiAgentDebate:
    """Resolves knowledge conflicts via structured LLM debate.

    Two LLM agents defend their respective chunks using evidence-constrained
    prompts. An aggregator evaluates the arguments and outputs ``WINNER: 1``,
    ``WINNER: 2``, or ``WINNER: MERGE``. On merge, a reconciled summary
    (≤200 tokens) is produced. If no consensus is reached after
    ``max_debate_rounds``, raises ``E007KnowledgeConflictUnresolved``.

    Parameters:
        settings: RAG service settings providing Ollama URL and max rounds.
    """

    def __init__(self, settings: Settings) -> None:
        self._ollama_base_url = settings.ollama_base_url.rstrip("/")
        self._max_rounds = settings.max_debate_rounds
        self._agent_prompt_template = self._load_prompt("debate_agent.txt")
        self._aggregator_prompt_template = self._load_prompt("debate_aggregator.txt")

    @staticmethod
    def _load_prompt(filename: str) -> str:
        """Load a prompt template from the prompts directory.

        Parameters:
            filename: Name of the prompt file to load.

        Returns:
            The raw prompt template string.
        """
        path = _PROMPTS_DIR / filename
        return path.read_text(encoding="utf-8")

    async def _call_llm(self, prompt: str, client: httpx.AsyncClient) -> str:
        """Call the Ollama OpenAI-compatible chat endpoint.

        Parameters:
            prompt: The full prompt to send as a user message.
            client: Shared async HTTP client.

        Returns:
            The assistant's response text.
        """
        response = await client.post(
            f"{self._ollama_base_url}/v1/chat/completions",
            json={
                "model": "llama3.2",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "max_tokens": 300,
            },
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]

    def _build_agent_prompt(self, chunk_content: str, opposing_content: str) -> str:
        """Fill the debate agent prompt template.

        Parameters:
            chunk_content: Content of the chunk this agent defends.
            opposing_content: Content of the opposing chunk.

        Returns:
            Formatted prompt string.
        """
        return self._agent_prompt_template.format(
            chunk_content=chunk_content,
            opposing_chunk_content=opposing_content,
        )

    def _build_aggregator_prompt(
        self,
        agent1_argument: str,
        agent2_argument: str,
        conflict: ConflictPair,
    ) -> str:
        """Fill the aggregator prompt template.

        Parameters:
            agent1_argument: The argument from the agent defending chunk_i.
            agent2_argument: The argument from the agent defending chunk_j.
            conflict: The ConflictPair containing both chunks and their metadata.

        Returns:
            Formatted prompt string.
        """
        return self._aggregator_prompt_template.format(
            agent1_argument=agent1_argument,
            agent2_argument=agent2_argument,
            chunk_i_tier=conflict.chunk_i.metadata.authority_tier,
            chunk_i_created_at=str(conflict.chunk_i.metadata.created_at),
            chunk_j_tier=conflict.chunk_j.metadata.authority_tier,
            chunk_j_created_at=str(conflict.chunk_j.metadata.created_at),
        )

    @staticmethod
    def parse_winner(aggregator_output: str) -> tuple[str, str | None]:
        """Extract the winner decision and optional merged summary from aggregator output.

        Parameters:
            aggregator_output: Raw text from the aggregator LLM.

        Returns:
            A tuple of (winner, merged_summary). ``winner`` is one of
            ``"1"``, ``"2"``, or ``"MERGE"``. ``merged_summary`` is populated
            only when winner is ``"MERGE"``.
        """
        winner_match = re.search(
            r"WINNER:\s*(1|2|MERGE)", aggregator_output, re.IGNORECASE
        )
        if not winner_match:
            return "", None

        winner = winner_match.group(1).upper()

        merged_summary: str | None = None
        if winner == "MERGE":
            merge_match = re.search(
                r"MERGED_SUMMARY:\s*(.+)", aggregator_output, re.DOTALL
            )
            if merge_match:
                merged_summary = merge_match.group(1).strip()
                # Truncate to ~200 tokens (rough word approximation)
                words = merged_summary.split()
                if len(words) > 200:
                    merged_summary = " ".join(words[:200])

        return winner, merged_summary

    async def resolve(self, conflict: ConflictPair) -> RankedChunk:
        """Run the debate loop and return the winning or merged chunk.

        Parameters:
            conflict: A ``ConflictPair`` from the ``ConflictDetector``.

        Returns:
            The winning ``RankedChunk``, or a new ``RankedChunk`` containing
            the merged summary.

        Raises:
            E007KnowledgeConflictUnresolved: If no consensus is reached after
                ``max_debate_rounds`` iterations.
        """
        async with httpx.AsyncClient() as client:
            for _round in range(self._max_rounds):
                # Agent 1 defends chunk_i
                agent1_prompt = self._build_agent_prompt(
                    conflict.chunk_i.content, conflict.chunk_j.content
                )
                agent1_argument = await self._call_llm(agent1_prompt, client)

                # Agent 2 defends chunk_j
                agent2_prompt = self._build_agent_prompt(
                    conflict.chunk_j.content, conflict.chunk_i.content
                )
                agent2_argument = await self._call_llm(agent2_prompt, client)

                # Aggregator evaluates
                agg_prompt = self._build_aggregator_prompt(
                    agent1_argument, agent2_argument, conflict
                )
                aggregator_output = await self._call_llm(agg_prompt, client)

                winner, merged_summary = self.parse_winner(aggregator_output)

                if winner == "1":
                    return conflict.chunk_i
                elif winner == "2":
                    return conflict.chunk_j
                elif winner == "MERGE" and merged_summary:
                    return RankedChunk(
                        chunk_id=f"{conflict.chunk_i.chunk_id}-merged",
                        content=merged_summary,
                        metadata=ChunkMetadata(
                            chunk_id=f"{conflict.chunk_i.chunk_id}-merged",
                            doc_id=conflict.chunk_i.metadata.doc_id,
                            source_type=conflict.chunk_i.metadata.source_type,
                            source_uri=conflict.chunk_i.metadata.source_uri,
                            authority_tier=conflict.chunk_i.metadata.authority_tier,
                            created_at=conflict.chunk_i.metadata.created_at,
                            resolution_status="resolved",
                            is_accepted_answer=False,
                            recency_score=max(
                                conflict.chunk_i.metadata.recency_score,
                                conflict.chunk_j.metadata.recency_score,
                            ),
                        ),
                        score=max(conflict.chunk_i.score, conflict.chunk_j.score),
                    )
                # No valid winner token found — try next round

        raise E007KnowledgeConflictUnresolved(
            f"Conflict between chunks {conflict.chunk_i.chunk_id} and "
            f"{conflict.chunk_j.chunk_id} unresolved after {self._max_rounds} "
            f"debate rounds."
        )
