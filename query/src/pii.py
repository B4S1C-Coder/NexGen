"""PII Masker — Stage 7 of the NL-to-KQL pipeline.

Strips personally identifiable information from raw Elasticsearch
hits before returning them to the Master LLM. Uses compiled regex
patterns for performance.

Patterns masked:
  - IPv4 addresses        → <IP_ADDRESS>
  - IPv6 addresses        → <IP_ADDRESS>
  - Email addresses       → <EMAIL>
  - JWT tokens            → <JWT_TOKEN>
  - AWS access keys       → <AWS_KEY>
  - Credit card numbers   → <CREDIT_CARD>
  - Phone numbers         → <PHONE>
  - MD5 hashes            → <HASH>
  - SHA-256 hashes        → <HASH>

Trace IDs are preserved — they are needed for correlation.

Defined in TASKS.md P2-Q5.
"""

from __future__ import annotations

import re
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Compiled regex patterns — compiled once at import time
# ---------------------------------------------------------------------------

_PATTERNS: list[tuple[re.Pattern, str]] = [
    # JWT token (three dot-separated base64 segments)
    (re.compile(r"eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"), "<JWT_TOKEN>"),

    # AWS access key (starts with AKIA, 20 chars)
    (re.compile(r"AKIA[0-9A-Z]{16}"), "<AWS_KEY>"),

    # Email address
    (re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"), "<EMAIL>"),

    # Credit card (13-19 digits, optional dashes/spaces)
    (re.compile(r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{1,7}\b"), "<CREDIT_CARD>"),

    # IPv4 address
    (re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"), "<IP_ADDRESS>"),

    # IPv6 address (simplified — catches common formats)
    (re.compile(r"\b([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b"), "<IP_ADDRESS>"),

    # SHA-256 hash (64 hex chars)
    (re.compile(r"\b[a-fA-F0-9]{64}\b"), "<HASH>"),

    # MD5 hash (32 hex chars)
    (re.compile(r"\b[a-fA-F0-9]{32}\b"), "<HASH>"),

    # Phone number (international format)
    (re.compile(r"\+\d{1,3}[\s-]?\d{6,14}"), "<PHONE>"),
]


# ---------------------------------------------------------------------------
# PIIMasker
# ---------------------------------------------------------------------------

class PIIMasker:
    """Masks PII in Elasticsearch hit dicts using regex patterns.

    Walks every string value in every hit dict and applies all
    compiled patterns. Non-string values are left untouched.

    Usage:
        masker = PIIMasker()
        clean_hits = masker.mask(raw_hits)
    """

    def mask(self, hits: list[dict]) -> list[dict]:
        """Mask PII in a list of ES hit dicts.

        Args:
            hits: List of _source dicts from ExecutorResult.hits.

        Returns:
            New list of dicts with PII replaced by placeholder tokens.
            Original dicts are not modified.
        """
        masked = [self._mask_dict(hit) for hit in hits]
        logger.debug("Masked PII in %d hits.", len(masked))
        return masked

    def _mask_dict(self, d: dict) -> dict:
        """Recursively mask all string values in a dict.

        Args:
            d: A single ES hit _source dict.

        Returns:
            New dict with PII in string values replaced.
        """
        result = {}
        for key, value in d.items():
            if isinstance(value, str):
                result[key] = self._mask_string(value)
            elif isinstance(value, dict):
                result[key] = self._mask_dict(value)
            elif isinstance(value, list):
                result[key] = [
                    self._mask_dict(item) if isinstance(item, dict)
                    else self._mask_string(item) if isinstance(item, str)
                    else item
                    for item in value
                ]
            else:
                result[key] = value
        return result

    def _mask_string(self, text: str) -> str:
        """Apply all PII patterns to a single string.

        Args:
            text: Any string value from an ES hit.

        Returns:
            String with all PII matches replaced by placeholders.
        """
        for pattern, replacement in _PATTERNS:
            text = pattern.sub(replacement, text)
        return text