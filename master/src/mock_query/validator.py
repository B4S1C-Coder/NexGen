class MockSyntaxValidator:
    def validate(self, kql: str) -> tuple[bool, list[str], dict]:
        return True, [], {"ast": "mock_ast"}
