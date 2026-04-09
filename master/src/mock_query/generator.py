class MockKQLGenerator:
    def generate(self, nl_query: str, schema: dict, examples: list[str]) -> str:
        return "mock.field: \"error\" | where @timestamp >= now-1h"
