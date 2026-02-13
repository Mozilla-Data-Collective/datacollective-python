from __future__ import annotations

import json
from pathlib import Path

_SLUGS_FILE = Path(__file__).parent / "data" / "slugs.json"


def _load_slugs() -> dict[str, str]:
    """Load the slugs from the JSON file."""
    if not _SLUGS_FILE.exists():
        return {}
    with open(_SLUGS_FILE) as f:
        return json.load(f)


def resolve_slug_or_id(slug_or_id: str) -> str:
    """
    Resolve a slug to an ID. If the input is already an ID, return it as is.
    """
    slugs = _load_slugs()
    return slugs.get(slug_or_id, slug_or_id)
