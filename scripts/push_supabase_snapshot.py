# scripts/push_supabase_snapshot.py
import os, json, requests
import pandas as pd
from datetime import datetime, timezone

from build_gold_snapshot import build

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

HEADERS = {
  "apikey": SERVICE_KEY,
  "Authorization": f"Bearer {SERVICE_KEY}",
  "Content-Type": "application/json",
  "Prefer": "resolution=merge-duplicates"
}

def postgrest(table: str) -> str:
  return f"{SUPABASE_URL}/rest/v1/{table}"

def chunks(lst, n):
  for i in range(0, len(lst), n):
    yield lst[i:i+n]

def upsert(table: str, records: list[dict], batch_size=500):
  for batch in chunks(records, batch_size):
    r = requests.post(postgrest(table), headers=HEADERS, data=json.dumps(batch))
    r.raise_for_status()

def budget_bucket(x):
  if x is None or pd.isna(x): return "NA"
  x = float(x)
  if x <= 90000: return "<=90k"
  if x <= 120000: return "<=120k"
  if x <= 160000: return "<=160k"
  return ">160k"

def write_cache(dft: pd.DataFrame, as_of_date):
  # Segmento mínimo: distrito + bucket presupuesto + dormitorios
  df = dft.copy()
  df["precio_desde"] = pd.to_numeric(df.get("precio_desde"), errors="coerce")
  df["segment_key"] = df.apply(lambda r: f"{r.get('distrito','NA')}|{budget_bucket(r.get('precio_desde'))}|{int(r.get('dormitorios') or 0)}d", axis=1)

  rows = []
  for seg, g in df.groupby("segment_key"):
    top = g.sort_values(["unidades_disponibles","area_m2"], ascending=[False, False]).head(20)
    payload = top[[
      "typology_day_id","project_day_id","proyecto","modelo","distrito",
      "precio_desde","moneda","area_m2","dormitorios","banos","piso_min","piso_max","url"
    ]].to_dict(orient="records")
    rows.append({"as_of_date": str(as_of_date), "segment_key": seg, "payload": payload})

  upsert("reco_cache", rows, batch_size=200)

def main():
  dfp, dft = build()
  now = datetime.now(timezone.utc).isoformat()

  dfp["updated_at"] = now
  dft["updated_at"] = now

  # limpiar NaNs
  projects = dfp.where(pd.notnull(dfp), None).to_dict(orient="records")
  typos    = dft.where(pd.notnull(dft), None).to_dict(orient="records")

  # Upsert
  upsert("projects_snapshot", projects, batch_size=200)
  upsert("typologies_snapshot", typos, batch_size=500)

  # Cache (último día en el dataframe)
  as_of = dft["as_of_date"].max()
  write_cache(dft[dft["as_of_date"] == as_of], as_of)

  print(f"OK pushed projects={len(projects)} typologies={len(typos)} as_of={as_of}")

if __name__ == "__main__":
  main()
