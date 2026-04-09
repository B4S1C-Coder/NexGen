from nexgen_shared.schemas import LogRetrievalRequest, LogRetrievalResult
from .schema_linker import MockSchemaLinker
from .few_shot import MockFewShotSelector
from .generator import MockKQLGenerator
from .validator import MockSyntaxValidator
from .repair import MockRepairAgent
from .executor import MockElasticsearchExecutor
from .pii import MockPIIMasker
from .formatter import MockResultFormatter

class MockQueryPipeline:
    def __init__(self):
        self.linker = MockSchemaLinker()
        self.few_shot = MockFewShotSelector()
        self.generator = MockKQLGenerator()
        self.validator = MockSyntaxValidator()
        self.repair = MockRepairAgent()
        self.executor = MockElasticsearchExecutor()
        self.pii = MockPIIMasker()
        self.formatter = MockResultFormatter()

    async def retrieve(self, request: LogRetrievalRequest) -> LogRetrievalResult:
        schema = self.linker.link_schema(request.natural_language)
        examples = self.few_shot.get_examples(request.natural_language)
        kql = self.generator.generate(request.natural_language, schema, examples)
        valid, errors, ast = self.validator.validate(kql)
        if not valid:
            kql = self.repair.repair(kql, errors)
        raw_hits, total = self.executor.execute(kql)
        masked_hits = self.pii.mask(raw_hits)
        return self.formatter.format_result(
            query_id=request.query_id, status="success", kql=kql, valid=valid,
            attempts=0, hits=masked_hits, count=total
        )
