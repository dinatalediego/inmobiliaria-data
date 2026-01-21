repo-etl/
  data/
    silver/
      nexo/
        ...__proyecto.parquet
        ...__tipologias.parquet
    gold/
      typologies_snapshot.parquet   (opcional como artefacto local)
  scripts/
    build_gold_snapshot.py
    push_supabase_snapshot.py
  .github/workflows/
    snapshot_to_supabase_daily.yml
