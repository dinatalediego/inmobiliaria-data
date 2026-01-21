"""Shared utilities (HTTP fetch, text normalization, numeric parsing)."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_CLEAN_SPACES = re.compile(r"\s+")


def clean_text(s: str) -> str:
    return _CLEAN_SPACES.sub(" ", s or "").strip()


def fix_mojibake(s: str) -> str:
    """Fix common mojibake like 'NÃ¡poles' -> 'Nápoles'."""
    if not s:
        return s
    try:
        return s.encode("latin1").decode("utf-8")
    except Exception:
        return s


def parse_money_to_float(text: Optional[str]) -> Optional[float]:
    """Parse first numeric chunk from a money-like text and return float.

    Examples:
      - 'Desde S/ 327,550' -> 327550.0
      - 'US$ 120,500' -> 120500.0

    Note: for prices we typically want integer amounts, so we strip '.' and ',' as thousand separators.
    """
    if not text:
        return None

    t = fix_mojibake(clean_text(text))
    m = re.search(r"(\d[\d\.,]*)", t)
    if not m:
        return None

    num = m.group(1)
    num = num.replace(".", "").replace(",", "")
    try:
        return float(num)
    except ValueError:
        return None


def parse_float(text: Optional[str]) -> Optional[float]:
    """Parse float from text like '57.04 m²' or '44,63 m²'."""
    if not text:
        return None

    t = fix_mojibake(clean_text(text))
    m = re.search(r"(\d+[\.,]?\d*)", t)
    if not m:
        return None

    raw = m.group(1)
    # If comma is used as decimal separator, normalize.
    if "," in raw and "." not in raw:
        raw = raw.replace(",", ".")
    else:
        # remove thousands separators, keep last decimal if present
        raw = raw.replace(",", "")
    try:
        return float(raw)
    except ValueError:
        return None


@dataclass
class HttpConfig:
    timeout_s: int = 25
    total_retries: int = 3
    backoff_factor: float = 0.4
    rate_limit_s: float = 0.0
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )


def download_html(url: str, cfg: Optional[HttpConfig] = None) -> str:
    cfg = cfg or HttpConfig()

    if cfg.rate_limit_s and cfg.rate_limit_s > 0:
        time.sleep(cfg.rate_limit_s)

    session = requests.Session()
    retries = Retry(
        total=cfg.total_retries,
        backoff_factor=cfg.backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))

    resp = session.get(url, headers={"User-Agent": cfg.user_agent}, timeout=cfg.timeout_s)
    resp.raise_for_status()
    return resp.text
