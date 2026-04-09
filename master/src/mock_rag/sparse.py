class MockSparseRetriever:
    def retrieve(self, query: str) -> list[dict]:
        return [{"id": "c2", "score": 0.5, "text": "Mock sparse chunk for " + query}]
