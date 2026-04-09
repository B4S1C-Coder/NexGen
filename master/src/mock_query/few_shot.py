class MockFewShotSelector:
    def get_examples(self, nl_query: str) -> list[str]:
        return ["NLQ: Mock\nKQL: mock_field: \"mock_value\""]
