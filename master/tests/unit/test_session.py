from datetime import datetime, timezone

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from nexgen_shared.schemas import UserQuery

from src.session import MAX_MESSAGES, Message, SessionManager, SessionState


class FakeRedis:
    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ex=None):
        self.store[key] = value


class DownRedis:
    async def get(self, key):
        raise RedisConnectionError("down")

    async def set(self, key, value, ex=None):
        raise RedisConnectionError("down")


@pytest.fixture
def manager() -> SessionManager:
    m = SessionManager("redis://unused")
    m.redis = FakeRedis()
    return m


async def test_put_then_get_round_trip(manager):
    state = SessionState(session_id="s1")
    state.query_history.append(UserQuery(query_id="q1", raw_text="why?", session_id="s1",
                                         timestamp_utc=datetime.now(timezone.utc)))
    await manager.put(state)
    loaded = await manager.get("s1")
    assert loaded is not None and loaded.query_history[0].query_id == "q1"


async def test_missing_session_is_none(manager):
    assert await manager.get("nope") is None


async def test_history_keeps_last_messages_in_order(manager):
    state = SessionState(session_id="s1", messages=[Message(role="user", content=f"m{i}") for i in range(25)])
    await manager.put(state)
    loaded = await manager.get("s1")
    assert len(loaded.messages) == MAX_MESSAGES
    assert loaded.messages[0].content == "m5" and loaded.messages[-1].content == "m24"


async def test_falls_back_to_memory_when_redis_is_down():
    m = SessionManager("redis://unused")
    m.redis = DownRedis()
    await m.put(SessionState(session_id="s1"))
    assert m.redis is None
    assert (await m.get("s1")).session_id == "s1"
