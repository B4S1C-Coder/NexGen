"""Per-session chat history, stored in Redis (or in memory when Redis is not running)."""

from __future__ import annotations

import logging

from pydantic import BaseModel
from redis.asyncio import Redis
from redis.exceptions import RedisError

from nexgen_shared.schemas import UserQuery

logger = logging.getLogger(__name__)

MAX_MESSAGES = 20


class Message(BaseModel):
    """One chat turn."""

    role: str
    content: str


class SessionState(BaseModel):
    """Everything remembered about one session."""

    session_id: str
    query_history: list[UserQuery] = []
    messages: list[Message] = []


class SessionManager:
    """Saves and loads SessionState. Keeps only the last MAX_MESSAGES messages."""

    def __init__(self, redis_url: str, ttl_seconds: int = 7200) -> None:
        self.redis: Redis | None = Redis.from_url(redis_url, decode_responses=True)
        self.memory: dict[str, str] = {}
        self.ttl = ttl_seconds

    async def get(self, session_id: str) -> SessionState | None:
        """Return the stored session, or None if it does not exist."""
        key = f"nexgen:session:{session_id}"
        raw = self.memory.get(key)
        if self.redis is not None:
            try:
                raw = await self.redis.get(key)
            except (RedisError, OSError):
                self._fall_back_to_memory()
        return SessionState.model_validate_json(raw) if raw else None

    async def put(self, state: SessionState) -> None:
        """Store the session, trimming the history to the most recent messages."""
        state.messages = state.messages[-MAX_MESSAGES:]
        key = f"nexgen:session:{state.session_id}"
        if self.redis is not None:
            try:
                await self.redis.set(key, state.model_dump_json(), ex=self.ttl)
                return
            except (RedisError, OSError):
                self._fall_back_to_memory()
        self.memory[key] = state.model_dump_json()

    def _fall_back_to_memory(self) -> None:
        logger.warning("Redis unavailable, keeping sessions in memory")
        self.redis = None
