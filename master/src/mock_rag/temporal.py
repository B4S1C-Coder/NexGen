from nexgen_shared.schemas import KnowledgeTimeWindow
class MockTemporalFilter:
    def build_filter(self, window: KnowledgeTimeWindow) -> dict:
        return {"not_after": window.not_after}
