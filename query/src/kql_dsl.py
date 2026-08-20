"""KQL-to-DSL transpiler — converts Kibana Query Language strings into
Elasticsearch Query DSL dicts that the REST API _search endpoint accepts.

Uses `kaquel` to securely parse and render KQL into DSL.
"""

from __future__ import annotations
import logging
from kaquel.kql import parse_kql

logger = logging.getLogger(__name__)

def kql_to_dsl(kql: str) -> dict:
    """Translate a Kibana KQL query string into an Elasticsearch Query DSL dict.

    The returned dict contains a top-level 'query' key ready to pass
    directly to the elasticsearch-py 9.x search() method as query=...

    Args:
        kql: A KQL query string produced by the KQLGenerator, using
             standard Kibana KQL syntax. No pipe clauses.

    Returns:
        Dict with structure {"query": <ES DSL query dict>}.
    """
    kql = kql.strip()
    if not kql:
        return {"query": {"match_all": {}}}
        
    try:
        query_ast = parse_kql(kql)
        return {"query": query_ast.render()}
    except Exception as e:
        logger.error(f"Kaquel failed to parse KQL: {kql}. Error: {e}")
        # Fallback to multi_match if syntax is entirely unrecognizable
        return {"query": {"multi_match": {"query": kql, "fields": ["*"]}}}