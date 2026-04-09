class MockSchemaLinker:
    def link_schema(self, nl_query: str) -> dict:
        return {"indices": ["mock-index-*"], "time_field": "@timestamp"}
