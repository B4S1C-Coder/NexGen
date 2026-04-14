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

class SessionManager:
    """ Manages Redis persistence and context pruning for multi-turn RCA sessions. """

    def __init__(self, redis_url: str, ttl_seconds: int=7200):
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.ttl = ttl_seconds
    
    async def get(self, session_id: str) -> SessionState:
        """ Fetch session state. Returns a fresh state if none exists. """
        data = await self.redis.get(f"nexgen:session:{session_id}")
        if data:
            return SessionState.model_validate_json(data)

        return SessionState(session_id=session_id)
    
    async def put(self, session_id: str, state: SessionState) -> None:
        """ Persist session state to Redis. """
        await self.redis.set(
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
