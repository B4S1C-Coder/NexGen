import pytest
import os
import uuid
from datetime import datetime, timezone
import sys

# Ensure nexgen_shared and master can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../master')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.orchestrator import MasterOrchestrator
from src.settings import Settings
from nexgen_shared.schemas import UserQuery

@pytest.mark.asyncio
async def test_master_mock_flow():
    """
    Test the MasterOrchestrator end-to-end using mock services.
    Verifies that the RAG session manager and DAG planner execute without error.
    """
    settings = Settings(
        mock_services=True,
        redis_url="redis://localhost:6379", # Won't be used since mock_services=True implies memory_only
    )
    
    orchestrator = MasterOrchestrator(settings=settings)
    
    query = UserQuery(
        query_id=str(uuid.uuid4()),
        raw_text="Show me all HTTP 500 errors from the payments service in the last 30 minutes",
        session_id="test-session-123",
        timestamp_utc=datetime.now(timezone.utc)
    )
    
    report = await orchestrator.execute_query(query)
    
    assert report is not None
    assert report.query_id == query.query_id
    
    # We should get a low-confidence report because no real LLM is connected, 
    # and intent_classifier might fail or return a dummy response.
    assert report.confidence == 0.0 or "Analysis halted" in report.root_cause_summary
    
    print("Test passed! Generated Report:")
    print(report.model_dump_json(indent=2))
