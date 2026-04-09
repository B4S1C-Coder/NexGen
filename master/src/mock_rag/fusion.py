class MockWRRFFusion:
    def fuse(self, dense: list[dict], sparse: list[dict]) -> list[dict]:
        return dense + sparse
