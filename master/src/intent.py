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
        pass