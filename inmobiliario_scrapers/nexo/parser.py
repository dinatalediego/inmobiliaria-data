from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup

from inmobiliario_scrapers.core.exceptions import ParseError
from inmobiliario_scrapers.core.utils import clean_text, fix_mojibake, parse_float, parse_money_to_float
from .extract import extract_card_texts
from .model import Proyecto, Tipologia


# Regex tolerant to variations like:
#  - '27 unidades disponibles'
#  - '1 unidad disponible'
#  - 'Pisos 2 al 28' / 'Piso 2'
#  - mojibake 'NÃ¡poles', 'mÂ²', 'baÃ±os'
CARD_RE = re.compile(
    r"""
    (?P<prefix>Plano)?\s*
    (?P<proyecto>.+?)\s*
    (?P<unidades>\d+)\s+unidad(?:es)?\s+disponible(?:s)?\s*
    .*?Desde\s*(?P<moneda>S\/|US\$)\s*(?P<precio>[\d\.,]+)\s*
    .*?Modelo\s*(?P<modelo>[A-Za-z0-9\-_]+)\s*
    .*?(?P<area>[\d\.,]+)\s*m(?:²|2|Â²)\s*
    .*?Piso(?:s)?\s*(?P<piso_min>\d+)(?:\s*al\s*(?P<piso_max>\d+))?\s*
    .*?(?P<dorms>\d+)\s*dorms?\s*
    .*?(?P<banos>\d+)\s*bañ(?:o|os|Ã±o|Ã±os)\s*
    """,
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)


def _find_first_text(soup: BeautifulSoup, selectors: list[str]) -> Optional[str]:
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            return el.get_text(" ", strip=True)
    return None


def parse_card_text(txt: str) -> Tipologia:
    raw = fix_mojibake(clean_text(txt))
    m = CARD_RE.search(raw)
    if not m:
        return Tipologia(
            modelo="",
            area_m2=parse_float(raw),
            precio_desde=parse_money_to_float(raw),
            moneda=None,
            unidades_disponibles=None,
            piso_min=None,
            piso_max=None,
            dormitorios=None,
            banos=None,
            raw=raw,
            parse_ok=False,
        )

    g = m.groupdict()
    moneda = g.get("moneda")
    precio = parse_money_to_float(f"{moneda} {g.get('precio')}")
    area = parse_float(g.get("area"))

    piso_min = int(g["piso_min"]) if g.get("piso_min") else None
    piso_max = int(g["piso_max"]) if g.get("piso_max") else piso_min

    return Tipologia(
        modelo=g.get("modelo") or "",
        area_m2=area,
        precio_desde=precio,
        moneda=moneda,
        unidades_disponibles=int(g["unidades"]) if g.get("unidades") else None,
        piso_min=piso_min,
        piso_max=piso_max,
        dormitorios=int(g["dorms"]) if g.get("dorms") else None,
        banos=int(g["banos"]) if g.get("banos") else None,
        raw=raw,
        parse_ok=True,
    )


class NexoParser:
    def __init__(self, html: str):
        self.html = html
        self.soup = BeautifulSoup(html, "html.parser")

    def parse(self) -> Proyecto:
        try:
            nombre = _find_first_text(self.soup, ["h1", "h1 span", ".project-title", ".titulo"])
            if nombre:
                nombre = fix_mojibake(clean_text(nombre))

            direccion = _find_first_text(self.soup, [
                "[data-testid='project-address']",
                ".direccion",
                ".address",
                ".project-address",
            ])
            if direccion:
                direccion = fix_mojibake(clean_text(direccion))

            # Price 'Desde' often appears in many places; take first match from whole page
            full_text = fix_mojibake(clean_text(self.soup.get_text(" ", strip=True)))
            moneda = None
            precio_desde = None
            m = re.search(r"Desde\s*(S\/|US\$)\s*([\d\.,]+)", full_text, re.IGNORECASE)
            if m:
                moneda = m.group(1)
                precio_desde = parse_money_to_float(f"{moneda} {m.group(2)}")

            # Extract and parse cards
            card_texts = extract_card_texts(self.html)
            tipologias = [parse_card_text(t) for t in card_texts]

            return Proyecto(
                nombre=nombre,
                direccion=direccion,
                precio_desde=precio_desde,
                moneda=moneda,
                tipologias=tipologias,
            )

        except Exception as e:
            raise ParseError(f"Error parsing Nexo: {str(e)}") from e
