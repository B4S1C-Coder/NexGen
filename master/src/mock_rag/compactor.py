class MockLLMLingua2Compactor:
    def compress(self, docs: list[dict], budget: int) -> int:
        return sum(len(d["text"]) for d in docs)
