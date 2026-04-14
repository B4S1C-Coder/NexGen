import os
import re
import json
from typing import Optional, List, Dict
from pydantic import BaseModel
from openai import AsyncOpenAI

class IntentResult(BaseModel):
    """ Routing schema outputted by the classifier based on query semantics. """
    logs_needed: bool
    docs_needed: bool
    is_quantitative: bool
    is_qualitative: bool
    time_range: Optional[Dict[str, str]] = None
    index_hints: List[str] = []

class IntentClassifier:
    def __init__(self, openai_client: Optional[AsyncOpenAI] = None, qdrant_client=None):
        self.llm = openai_client
        self.qdrant = qdrant_client

        # Keywords
        self.quantitative_patterns = [r"\bcount\b", r"\bhow many\b", r"\bsum\b", r"\baverage\b"]
        self.docs_only_patterns = [r"\bbest practice\b", r"\bhow to\b", r"\barchitecture\b"]
        self.troubleshooting_patterns = [r"\bwhy\b", r"\bfailed\b", r"\berror\b", r"\bcause\b"]

        try:
            with open("src/prompts/intent.txt", "r") as f:
                self.llm_prompt = f.read()
        except FileNotFoundError:
            try:
                with open("prompts/intent.txt", "r") as f:
                    self.llm_prompt = f.read()
            except FileNotFoundError:
                self.llm_prompt = "You are an intent classifier."
    
    async def classify(self, raw_text: str) -> IntentResult:
        """
        Classifies incoming queries across 3 performance-tiered stages:
        1. Regex / Keyword fast path 
        2. OATS Semantic similarity (Stubbed / prepared)
        3. Local LLM invocation over JSON mode
        """
        text_lower = raw_text.lower()
        
        # Regex
        is_docs_strict = any(re.search(p, text_lower) for p in self.docs_only_patterns)
        if is_docs_strict:
            return IntentResult(
                logs_needed=False, docs_needed=True, 
                is_quantitative=False, is_qualitative=True
            )
            
        is_quant = any(re.search(p, text_lower) for p in self.quantitative_patterns)
        is_troubleshoot = any(re.search(p, text_lower) for p in self.troubleshooting_patterns)
        
        if is_quant and not is_troubleshoot:
            return IntentResult(
                logs_needed=True, docs_needed=False, 
                is_quantitative=True, is_qualitative=False
            )
        
        #  Qdrant to be added later
        # if self.qdrant:
        #    hits = await self.qdrant.semantic_search(raw_text, threshold=0.85)
        #    if hits: return IntentResult(**hits[0].payload)

        # LLM fallback
        if self.llm:
            return await self._llm_classify(raw_text)
            
        # Failsafe default if LLM client isn't passed during early testing
        return IntentResult(
            logs_needed=True, docs_needed=True, 
            is_quantitative=False, is_qualitative=True
        )
    
    async def _llm_classify(self, raw_text: str) -> IntentResult:
        """Pings the local llama.cpp server and enforces the JSON output schema."""
        response = await self.llm.chat.completions.create(
            model=os.getenv("OPENAI_MODEL_NAME", "llama3.2"),  # Default local llama model name
            messages=[
                {"role": "system", "content": self.llm_prompt},
                {"role": "user", "content": raw_text}
            ],
            response_format={"type": "json_object"},
            temperature=0.1
        )

        raw = response.choices[0].message.content.strip()
        if raw.startswith("```json"): raw = raw[7:]
        if raw.startswith("```"): raw = raw[3:]
        if raw.endswith("```"): raw = raw[:-3]
        data = json.loads(raw.strip())
        return IntentResult(**data)