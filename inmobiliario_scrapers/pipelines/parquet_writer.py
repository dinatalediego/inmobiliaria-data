from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd


def write_project_and_tipologias(
    payload: dict,
    output_project_parquet: str,
    output_tipologias_parquet: Optional[str] = None,
) -> tuple[str, str]:
    """Write two parquet files:

    - project parquet: 1 row
    - tipologias parquet: N rows
    """
    out_proj = Path(output_project_parquet)
    out_proj.parent.mkdir(parents=True, exist_ok=True)

    tip_out = (
        Path(output_tipologias_parquet)
        if output_tipologias_parquet
        else out_proj.with_name(out_proj.stem + "_tipologias.parquet")
    )
    tip_out.parent.mkdir(parents=True, exist_ok=True)

    scraped_at = datetime.now(timezone.utc)

    tipologias = payload.get("tipologias") or []

    proj_row: dict[str, Any] = {
        "fuente": "nexo_inmobiliario",
        "url": payload.get("url"),
        "nombre": payload.get("nombre"),
        "direccion": payload.get("direccion"),
        "moneda": payload.get("moneda"),
        "precio_desde": payload.get("precio_desde"),
        "scraped_at": scraped_at,
        "n_tipologias": len(tipologias),
    }

    df_proj = pd.DataFrame([proj_row])
    df_proj.to_parquet(out_proj, index=False)

    rows = []
    for t in tipologias:
        row = dict(t)
        row["fuente"] = "nexo_inmobiliario"
        row["url"] = payload.get("url")
        row["proyecto"] = payload.get("nombre")
        row["scraped_at"] = scraped_at
        rows.append(row)

    df_tip = pd.DataFrame(rows)
    df_tip.to_parquet(tip_out, index=False)

    return str(out_proj), str(tip_out)
