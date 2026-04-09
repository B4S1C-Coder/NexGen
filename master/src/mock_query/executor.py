from datetime import datetime, timezone
from nexgen_shared.schemas import LogHit

class MockElasticsearchExecutor:
    def execute(self, kql: str) -> tuple[list[LogHit], int]:
        hits = [LogHit(timestamp=datetime.now(timezone.utc), service="mock-service", level="ERROR", message="Mock log entry")]
        return hits, len(hits)
