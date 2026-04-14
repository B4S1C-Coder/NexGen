import pytest
from datetime import datetime, timezone
from src.session import SessionManager, SessionState, Message
from nexgen_shared.schemas import UserQuery

class MockRedis:
    def __init__(self):
        self.store = {}
    
    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ex=None):
        self.store[key] = value

@pytest.fixture
def session_manager():
    manager = SessionManager("redis://dummy", 7200)
    manager.redis = MockRedis()
    return manager

@pytest.mark.asyncio
async def test_session_put_get(session_manager):
    state = SessionState(session_id="123", iteration_count=2)
    query = UserQuery(
        query_id="q1",
        raw_text="Why did payments fail?",
        session_id="123",
        timestamp_utc=datetime.now(timezone.utc)
    )

    state.query_history.append(query)

    # Execute
    await session_manager.put("123", state)
    retrieved = await session_manager.get("123")

    # Verify
    assert retrieved.session_id == "123"
    assert retrieved.iteration_count == 2
    assert len(retrieved.query_history) == 1
    assert retrieved.query_history[0].query_id == "q1"

def test_trim_context(session_manager):
    state = SessionState(session_id="123")
    for i in range(25):
        state.active_context_window.append(
            Message(role="user", content=f"msg_{i}")
        )
    
    trimmed = session_manager.trim_context(state)

    assert len(trimmed.active_context_window) == 20
    assert trimmed.active_context_window[0].content == "msg_24"
    assert trimmed.active_context_window[-1].content == "msg_23"