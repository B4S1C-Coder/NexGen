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