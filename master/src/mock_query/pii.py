from nexgen_shared.schemas import LogHit

class MockPIIMasker:
    def mask(self, hits: list[LogHit]) -> list[LogHit]:
        for hit in hits:
            if hit.message:
                hit.message = hit.message.replace("secret", "<MASKED>")
        return hits
