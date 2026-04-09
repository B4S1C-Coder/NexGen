from nexgen_shared.schemas import LogRetrievalResult, LogRetrievalStatus, LogHit

class MockResultFormatter:
    def format_result(self, query_id: str, status: LogRetrievalStatus, kql: str, valid: bool, attempts: int, hits: list[LogHit], count: int) -> LogRetrievalResult:
        return LogRetrievalResult(query_id=query_id, status=status, kql_generated=kql, syntax_valid=valid, refinement_attempts=attempts, hits=hits, hit_count=count, error=None)
