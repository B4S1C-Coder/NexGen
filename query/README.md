## Architecture

```mermaid
flowchart TD
    client["Master /query"] --> api[QueryFastAPIApp]
    api --> retrieveRoute["/retrieve"]
    api --> healthRoute["/health"]
    api --> schemaStatusRoute["/schema-cache/status"]

    retrieveRoute --> schemaLinker[SchemaLinker]
    retrieveRoute --> kqlGenerator[KQLGenerator]
    retrieveRoute --> esExecutor[ElasticsearchExecutor]
    retrieveRoute --> piiMasker[PIIMasker]
    retrieveRoute --> formatter[ResultFormatter]

    schemaLinker --> esMappings[ESIndexMappingsCache]
    esExecutor --> elasticsearchNode["Elasticsearch"]
```

This diagram shows how the NL-to-KQL service receives a `LogRetrievalRequest`, links it to index schemas, generates KQL, executes against Elasticsearch, masks PII, and returns a `LogRetrievalResult`.

