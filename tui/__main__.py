"""Launch the NexGen operator TUI.

Usage (from the repository root, with services' dependencies installed)::

    MOCK_SERVICES=true python -m tui
"""

from __future__ import annotations

from tui.app import run


def main() -> None:
    """Entry point for ``python -m tui``."""
    run()


if __name__ == "__main__":
    main()
