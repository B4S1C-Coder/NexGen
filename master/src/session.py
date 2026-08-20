import json
from typing import Any, Optional
from datetime import datetime

from pydantic import BaseModel, ConfigDict
from redis.asyncio import Redis
from nexgen_shared.schemas import UserQuery

class Message(BaseModel):
    """ Represents a single conversational turn/message. """
    role: str
    content: str

class SessionState(BaseModel):
    """ Per-session state persisted across user turns. """
    model_config = ConfigDict(extra="ignore")

    session_id: str
    query_history: list[UserQuery] = []
    active_context_window: list[Message] = []
    topology_graph: Optional[dict[str, Any]] = None
    iteration_count: int = 0


class InMemorySessionStore:
    """Process-local session backend used when Redis is unavailable.

    Mimics the subset of the redis-py asyncio API that SessionManager uses
    (``get`` / ``set``) so the orchestrator can boot without Docker.
    """

    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        """Return a previously stored JSON blob, or ``None``."""
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        """Persist a JSON blob. TTL is ignored in the in-memory store."""
        self.store[key] = value

    async def ping(self) -> bool:
        """Always succeed — the in-memory store is local to this process."""
        return True

import chromadb

class SessionManager:
    """ Manages Redis persistence and context pruning for multi-turn RCA sessions with Internal RAG. """

    def __init__(self, redis_url: str, ttl_seconds: int=7200, memory_only: bool = False):
        """Create a session manager.

        Args:
            redis_url: Redis connection URL used when a live store is available.
            ttl_seconds: Expiry applied to Redis keys.
            memory_only: When True, skip Redis entirely (dev / mock pathway).
        """
        self._redis_url = redis_url
        self.ttl = ttl_seconds
        self.redis: Any = InMemorySessionStore() if memory_only else None
        
        # Phase C: Production-Grade Internal RAG for chat history
        self.chroma = chromadb.PersistentClient(path="./.session_rag_db")
        self.collection = self.chroma.get_or_create_collection("session_history")
    
    async def _backend(self) -> Any:
        """Return Redis if reachable, otherwise a process-local dict store."""
        if self.redis is not None:
            return self.redis
        try:
            client = Redis.from_url(self._redis_url, decode_responses=True)
            await client.ping()
            self.redis = client
        except Exception:
            self.redis = InMemorySessionStore()
        return self.redis

    async def get(self, session_id: str) -> SessionState:
        """ Fetch session state. Returns a fresh state if none exists. """
        backend = await self._backend()
        data = await backend.get(f"nexgen:session:{session_id}")
        if data:
            return SessionState.model_validate_json(data)

        return SessionState(session_id=session_id)
    
    async def put(self, session_id: str, state: SessionState) -> None:
        """ Persist session state to Redis and index into Internal RAG. """
        # Index the latest context into ChromaDB
        for msg in state.active_context_window:
            # Create a deterministic ID to avoid duplication
            msg_id = f"{session_id}_{hash(msg.content)}"
            try:
                self.collection.add(
                    documents=[msg.content],
                    metadatas=[{"role": msg.role, "session_id": session_id}],
                    ids=[msg_id]
                )
            except Exception:
                pass # Document likely already exists
                
        backend = await self._backend()
        await backend.set(
            f"nexgen:session:{session_id}",
            state.model_dump_json(),
            ex=self.ttl
        )
    
    def trim_context(self, state: SessionState, current_query: str = "") -> SessionState:
        """
        Applies a semantic RAG approach combined with a sliding window to preserve long-term coherence.
        """
        if not state.active_context_window:
            return state
            
        # 1. Keep the most recent 6 messages (3 turns) for immediate contextual flow
        recent_messages = state.active_context_window[-6:]
        recent_contents = {m.content for m in recent_messages}
        
        retrieved_messages = []
        
        # 2. Semantic fetch for older relevant context using ChromaDB
        if current_query:
            try:
                results = self.collection.query(
                    query_texts=[current_query],
                    n_results=6,
                    where={"session_id": state.session_id}
                )
                if results and results["documents"] and results["documents"][0]:
                    for doc, meta in zip(results["documents"][0], results["metadatas"][0]):
                        msg = Message(role=meta["role"], content=doc)
                        if msg.content not in recent_contents:
                            retrieved_messages.append(msg)
                            recent_contents.add(msg.content)
            except Exception:
                pass # Fallback cleanly if Chroma fails
                
        # 3. Combine: RAG context first, then immediate recent flow
        combined = retrieved_messages + recent_messages
        
        new_state = state.model_copy(deep=True)
        new_state.active_context_window = combined
        
        return new_state
