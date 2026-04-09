class MockCrossEncoderReranker:
    def rerank(self, query: str, docs: list[dict]) -> list[dict]:
        return docs
