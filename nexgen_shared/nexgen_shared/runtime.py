"""Process-wide runtime flags shared by the three NexGen services."""

from __future__ import annotations

import os
from collections.abc import Mapping

_TRUTHY = frozenset({"1", "true", "yes", "on"})


def mock_services_enabled(environ: Mapping[str, str] | None = None) -> bool:
    """Return whether fixture-backed backends should replace live data stores.

    Args:
        environ: Optional environment mapping. Defaults to ``os.environ``.

    Returns:
        ``True`` when ``MOCK_SERVICES`` is a truthy string (``1``, ``true``,
        ``yes``, or ``on``), otherwise ``False``.
    """
    env = os.environ if environ is None else environ
    raw = str(env.get("MOCK_SERVICES", "false")).strip().lower()
    return raw in _TRUTHY
