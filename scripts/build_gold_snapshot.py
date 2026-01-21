# scripts/build_gold_snapshot.py
import glob, os, re, hashlib
from datetime import datetime
import pyarrow.parquet as pq
import pandas as pd

SILVER_DIR = os.getenv("SILVER_DIR", "data/silver")
OUT_GOLD_PROJECTS = os.getenv("OUT_GOLD_PROJECTS", "data/gold/projects_snapshot.parquet")
OUT_GOLD_TYPOLOGIES = os.getenv("OUT_GOLD_TYPOLOGIES", "data/gold/typologies_snapshot.parquet")

def read_parquet(path: str) -> pd.DataFrame:
    return pq.read_table(path).to_pandas()

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def parse_distrito_slug(url: str):
    # ejemplo: https://nexoinmobiliario.pe/departamentos/jesus-maria/san-felipe-3060
    m = re.search(r"/departamentos/([^/]+)/([^/?#]+)", url)
    if not m:
        return None, None
    return m.group(1), m.group(2)

def as_of_from_scraped(scraped_at):
    # scraped_at suele venir timestamp; si viene string iso, parsea
    if pd.isna(scraped_at):
        return pd.NaT
    if isinstance(scraped_at, str):
        try:
            dt = datetime.fromisoformat(scraped_at.replace("Z","+00:00"))
        except Exception:
            dt = pd.to_datetime(scraped_at, utc=True).to_pydatetime()
    else:
        dt = pd.to_datetime(scraped_at, utc=True).to_pydatetime()
    return dt.date()

def build():
    proj_paths = glob.glob(os.path.join(SILVER_DIR, "**/*__proyecto.parquet"), recursive=True)
    typ_paths  = glob.glob(os.path.join(SILVER_DIR, "**/*__tipologias.parquet"), recursive=True)

    if not proj_paths or not typ_paths:
        raise RuntimeError(f"No encontré parquets en {SILVER_DIR}")

    dfp = pd.concat([read_parquet(p) for p in proj_paths], ignore_index=True)
    dft = pd.concat([read_parquet(p) for p in typ_paths], ignore_index=True)

    # as_of_date (por defecto desde scraped_at)
    if "scraped_at" in dfp.columns:
        dfp["as_of_date"] = dfp["scraped_at"].apply(as_of_from_scraped)
    else:
        dfp["as_of_date"] = pd.Timestamp.utcnow().date()

    if "scraped_at" in dft.columns:
        dft["as_of_date"] = dft["scraped_at"].apply(as_of_from_scraped)
    else:
        dft["as_of_date"] = pd.Timestamp.utcnow().date()

    # IDs estables por día
    dfp["project_day_id"] = dfp.apply(lambda r: sha1(f"{r.get('url','')}|{r.get('as_of_date','')}"), axis=1)

    # distrito y slug por url (útil para filtros)
    dfp[["distrito","slug"]] = dfp["url"].apply(lambda u: pd.Series(parse_distrito_slug(u)))

    # JOIN: tipologías heredan project_day_id por url+as_of_date
    keep_proj = ["project_day_id","url","as_of_date","fuente","nombre","direccion","moneda","precio_desde","scraped_at","n_tipologias","distrito","slug"]
    dfp2 = dfp[keep_proj].drop_duplicates(["project_day_id"])

    dft = dft.merge(dfp2[["project_day_id","url","as_of_date","distrito","slug"]], on=["url","as_of_date"], how="left")

    # typology_day_id (url|modelo|as_of_date)
    dft["typology_day_id"] = dft.apply(lambda r: sha1(f"{r.get('url','')}|{r.get('modelo','')}|{r.get('as_of_date','')}"), axis=1)

    # Tipos numéricos (seguro)
    for c in ["area_m2","precio_desde","banos"]:
        if c in dft.columns:
            dft[c] = pd.to_numeric(dft[c], errors="coerce")
    for c in ["unidades_disponibles","piso_min","piso_max","dormitorios"]:
        if c in dft.columns:
            dft[c] = pd.to_numeric(dft[c], errors="coerce").astype("Int64")

    # Guardar gold local (opcional)
    os.makedirs(os.path.dirname(OUT_GOLD_PROJECTS), exist_ok=True)
    os.makedirs(os.path.dirname(OUT_GOLD_TYPOLOGIES), exist_ok=True)
    pq.write_table(pd.DataFrame(dfp2).to_arrow(), OUT_GOLD_PROJECTS)
    pq.write_table(pd.DataFrame(dft).to_arrow(), OUT_GOLD_TYPOLOGIES)

    return dfp2, dft

if __name__ == "__main__":
    build()
