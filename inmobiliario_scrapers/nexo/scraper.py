from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Optional

from inmobiliario_scrapers.core.exceptions import FetchError
from inmobiliario_scrapers.core.utils import HttpConfig, download_html
from .extract import extract_card_texts
from .parser import NexoParser


class NexoScraper:
    def __init__(self, http_cfg: Optional[HttpConfig] = None):
        self.http_cfg = http_cfg or HttpConfig()

    def fetch(self, url: str) -> str:
        try:
            return download_html(url, self.http_cfg)
        except Exception as e:
            raise FetchError(f"Error fetching {url}: {str(e)}") from e

    def parse(self, html: str) -> dict:
        proj = NexoParser(html).parse()
        d = asdict(proj)
        # tipologias is list[dict] already via asdict
        return d

    def run(self, url: str, raw_dir: Optional[str] = None, slug: str = "nexo", debug: bool = False) -> dict:
        """Fetch + parse a URL.

        If raw_dir is provided, saves raw HTML. If debug=True, also saves extracted card texts.
        """
        html = self.fetch(url)

        card_texts: list[str] = []
        if raw_dir:
            p = Path(raw_dir)
            p.mkdir(parents=True, exist_ok=True)
            (p / f"{slug}.html").write_text(html, encoding="utf-8")

            if debug:
                card_texts = extract_card_texts(html)
                (p / f"{slug}__cards.txt").write_text("\n".join(card_texts), encoding="utf-8")

        data = self.parse(html)
        data["url"] = url

        # parsing diagnostics (useful for registry)
        tips = data.get("tipologias") or []
        parse_ok = sum(1 for t in tips if (t.get("parse_ok") if isinstance(t, dict) else getattr(t, "parse_ok", False)))
        data["tipologias_rows"] = len(tips)
        data["parse_ok_rows"] = int(parse_ok)
        data["parse_fail_rows"] = int(len(tips) - parse_ok)
        if debug:
            data["extracted_cards_count"] = len(card_texts) if card_texts else None

        return data
