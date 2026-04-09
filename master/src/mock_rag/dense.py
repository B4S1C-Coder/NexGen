class MockDenseRetriever:
    def retrieve(self, query: str) -> list[dict]:
        return [{"id": "c1", "score": 0.8, "text": "Mock dense chunk relevant to " + query}]
