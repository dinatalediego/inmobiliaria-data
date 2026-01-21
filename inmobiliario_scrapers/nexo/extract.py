from __future__ import annotations

import json
from typing import Optional

from bs4 import BeautifulSoup


def extract_next_data(html: str) -> Optional[dict]:
    """Try extracting Next.js __NEXT_DATA__ JSON."""
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.select_one("script#__NEXT_DATA__")
    if not tag or not tag.string:
        return None
    try:
        return json.loads(tag.string)
    except Exception:
        return None


def extract_card_texts(html: str) -> list[str]:
    """Extract 'card' texts from HTML.

    This function tries common selectors first. If none found, it falls back
    to extracting blocks that contain 'Modelo' and 'Desde'.
    """
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        ".modelo-card",
        ".model-card",
        "[data-testid='model-card']",
        ".card-modelo",
        ".card",
    ]

    for sel in selectors:
        cards = soup.select(sel)
        if cards and len(cards) >= 2:
            return [" ".join(c.stripped_strings) for c in cards]

    # Fallback: find text-heavy blocks that match keywords
    blocks = []
    for tag in soup.find_all(["div", "section", "article", "li"]):
        txt = " ".join(tag.stripped_strings)
        if not txt:
            continue
        if "Modelo" in txt and "Desde" in txt and ("m" in txt or "m²" in txt or "mÂ²" in txt):
            blocks.append(txt)

    # Deduplicate while preserving order
    seen = set()
    out = []
    for b in blocks:
        if b in seen:
            continue
        seen.add(b)
        out.append(b)

    return out
