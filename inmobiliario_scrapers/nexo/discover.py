from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from bs4 import BeautifulSoup

BASE = "https://nexoinmobiliario.pe"
ID_RE_END = re.compile(r"-(\d+)$")

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
}


def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return u
    u, _frag = urldefrag(u)
    # normalize trailing slash (avoid stripping domain)
    if u.endswith("/") and len(u) > len("https://"):
        u = u[:-1]
    return u


def read_urls_file(path: str) -> list[str]:
    p = Path(path)
    lines = p.read_text(encoding="utf-8").splitlines()
    out: list[str] = []
    for line in lines:
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(normalize_url(s))
    # dedupe preserving order
    seen = set()
    deduped: list[str] = []
    for u in out:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    return deduped


def write_urls_file(path: str | Path, urls: Iterable[str]) -> str:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    data = "\n".join([normalize_url(u) for u in urls if normalize_url(u)])
    p.write_text((data + "\n") if data else "", encoding="utf-8")
    return str(p)


def fetch_html(url: str, timeout: int = 30) -> str:
    r = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


@dataclass
class OtroProyecto:
    url: str
    project_id: str | None
    title: str | None
    price_text: str | None
    badge: str | None


def extract_otros_links(html: str, base_url: str = BASE) -> list[OtroProyecto]:
    """Extrae links de 'Otros Departamentos / Otros proyectos' desde el carrusel.

    Selectores (orden):
    - a.carousel-extra-section-otros-btn[href]
    - div.carousel-extra-section-otros-share[data-url]
    """
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div.carousel-extra-section-otros-card")

    items: list[OtroProyecto] = []
    for card in cards:
        a = card.select_one("a.carousel-extra-section-otros-btn[href]")
        href = a["href"].strip() if a and a.has_attr("href") else None
        url = urljoin(base_url, href) if href else None

        if not url:
            share = card.select_one("div.carousel-extra-section-otros-share[data-url]")
            if share and share.has_attr("data-url"):
                url = share["data-url"].strip()

        if not url:
            continue

        url = normalize_url(url)

        title_el = card.select_one("h3.carousel-extra-section-otros-title")
        price_el = card.select_one("div.carousel-extra-section-otros-price")
        badge_el = card.select_one("div.carousel-extra-section-otros-badge")

        title = title_el.get_text(strip=True) if title_el else None
        price_text = price_el.get_text(" ", strip=True) if price_el else None
        badge = badge_el.get_text(" ", strip=True) if badge_el else None

        project_id = None
        m = ID_RE_END.search(urlparse(url).path)
        if m:
            project_id = m.group(1)

        items.append(OtroProyecto(url=url, project_id=project_id, title=title, price_text=price_text, badge=badge))

    # dedupe by url, preserve order
    seen = set()
    out: list[OtroProyecto] = []
    for it in items:
        if it.url not in seen:
            seen.add(it.url)
            out.append(it)
    return out


def discover_otros_from_urls(
    urls: Iterable[str],
    *,
    sleep_s: float = 1.2,
    timeout: int = 30,
    debug: bool = False,
    raw_dir: str | None = None,
) -> tuple[list[str], list[dict]]:
    """Recorre urls (seeds) y devuelve:

    - list[str] de other_urls (dedupe global, orden de descubrimiento)
    - list[dict] edges con metadata (seed_url -> other_url)

    Si debug=True y raw_dir se provee, guarda html raw por cada seed.
    """
    seen_other: set[str] = set()
    other_urls: list[str] = []
    edges: list[dict] = []

    rawp = Path(raw_dir) if raw_dir else None
    if debug and rawp:
        rawp.mkdir(parents=True, exist_ok=True)

    for i, seed_url in enumerate(urls, start=1):
        seed = normalize_url(seed_url)
        if not seed:
            continue

        try:
            html = fetch_html(seed, timeout=timeout)

            if debug and rawp:
                slug = re.sub(r"[^a-zA-Z0-9]+", "-", seed).strip("-")
                slug = slug[-120:] if len(slug) > 120 else slug
                (rawp / f"{slug}__discover.html").write_text(html, encoding="utf-8")

            otros = extract_otros_links(html, base_url=BASE)
            for it in otros:
                edges.append(
                    {
                        "seed_url": seed,
                        "other_url": it.url,
                        "other_project_id": it.project_id,
                        "other_title": it.title,
                        "other_price_text": it.price_text,
                        "other_badge": it.badge,
                    }
                )
                if it.url not in seen_other:
                    seen_other.add(it.url)
                    other_urls.append(it.url)

        except Exception:
            # swallow here; pipeline/batch can log errors at higher layer
            pass

        if sleep_s and sleep_s > 0:
            time.sleep(sleep_s)

    return other_urls, edges
