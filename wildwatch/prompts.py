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


# Shared "generic upload" prompt context.
#
# Live rtstreams plug per-stream context from ``config.STREAMS``;
# uploads don't carry any. Before this constant lived here,
# ``ingest.py`` and ``post_upload_analysis.py`` each kept their own
# near-duplicate dict — fixing common-name coverage in one place
# silently skipped the other. Callers that need a tweak override one
# field via ``{**DEFAULT_UPLOAD_PROMPT_CONTEXT, "expected_sounds": "..."}``.
DEFAULT_UPLOAD_PROMPT_CONTEXT: dict[str, str] = {
    "location_context": "uploaded clip (any environment)",
    "species_list": (
        "common wildlife — oryx, springbok, elephant, lion, giraffe, zebra, "
        "leopard, hyena, jackal, kudu, buffalo, hippo, crocodile, baboon, "
        "warthog, wild dog, various birds. If no wildlife, describe what is "
        "in the scene."
    ),
    "expected_sounds": "any ambient sound",
}


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
