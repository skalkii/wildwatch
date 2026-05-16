"""Prompt loader + formatter.

The four index prompts live in ``prompts/*.txt`` at the repo root.
Each prompt may reference per-stream context via ``str.format`` placeholders:

- ``species.txt``: ``{location_context}``, ``{species_list}``
- ``behavior.txt``: ``{location_context}``
- ``environment.txt``: none (static)
- ``audio.txt``: ``{location_context}``, ``{expected_sounds}``
"""

from __future__ import annotations

from pathlib import Path

PROMPT_DIR = Path(__file__).resolve().parent.parent / "prompts"


def load_prompt(name: str) -> str:
    """Return the raw prompt text for ``name`` (without ``.txt``)."""
    path = PROMPT_DIR / f"{name}.txt"
    return path.read_text(encoding="utf-8")


def format_prompt(name: str, **ctx: str) -> str:
    """Return the prompt for ``name`` with ``{placeholder}`` fields substituted.

    Raises ``KeyError`` if the prompt references a placeholder not in ``ctx`` —
    catches wiring bugs early instead of shipping a literal ``{species_list}``
    string to the VLM.
    """
    return load_prompt(name).format(**ctx)
