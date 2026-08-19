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

class SessionManager:
    """ Manages Redis persistence and context pruning for multi-turn RCA sessions. """

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
        """ Persist session state to Redis. """
        backend = await self._backend()
        await backend.set(
            f"nexgen:session:{session_id}",
            state.model_dump_json(),
            ex=self.ttl
        )
    
    def trim_context(self, state: SessionState) -> SessionState:
        """
        Applies a sliding window pruning to the last 20 messages, 
        then applies LongContextReorder to fight the Lost-in-the-Middle effect.
        """
        trimmed_messages = state.active_context_window[-20:]
        
        if not trimmed_messages:
            return state

        # Rank by newest first (highest priority)
        ranked = list(reversed(trimmed_messages))
        
        # Use two pointers to fill the edges first, pushing lower priority to the middle
        reordered = [None] * len(ranked)
        left = 0
        right = len(ranked) - 1
        
        for i, msg in enumerate(ranked):
            if i % 2 == 0:
                reordered[left] = msg
                left += 1
            else:
                reordered[right] = msg
                right -= 1
                
        new_state = state.model_copy(deep=True)
        new_state.active_context_window = reordered
        
        return new_state
