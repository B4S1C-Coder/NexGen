class MockAuthorityScorer:
    def score(self, docs: list[dict]) -> list[dict]:
        return docs
