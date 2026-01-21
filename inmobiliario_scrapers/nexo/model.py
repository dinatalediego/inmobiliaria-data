from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class Tipologia:
    modelo: str
    area_m2: Optional[float]
    precio_desde: Optional[float]
    moneda: Optional[str]
    unidades_disponibles: Optional[int]
    piso_min: Optional[int]
    piso_max: Optional[int]
    dormitorios: Optional[int]
    banos: Optional[int]
    raw: Optional[str] = None
    parse_ok: bool = True


@dataclass
class Proyecto:
    nombre: Optional[str]
    direccion: Optional[str]
    precio_desde: Optional[float]
    moneda: Optional[str]
    tipologias: list[Tipologia]
