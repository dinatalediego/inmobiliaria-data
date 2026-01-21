# inmobiliario_scrapers (property-scrappers)

Librería reproducible para scraping inmobiliario (enfoque inicial: Nexo Inmobiliario).

## Instalación (editable)

```bash
python -m venv .venv
# Windows
.\.venv\Scripts\activate
# Linux/mac
source .venv/bin/activate

pip install -U pip
pip install -e .
```

## CLI

Scrapear un proyecto y guardar dos Parquet (proyecto + tipologías):

```bash
inmo-scrape-nexo "https://nexoinmobiliario.pe/departamentos/jesus-maria/torre-napoles-3900" data/silver/nexo_proyecto.parquet --raw-dir data/raw
```

Esto genera:
- `data/silver/nexo_proyecto.parquet` (1 fila)
- `data/silver/nexo_proyecto_tipologias.parquet` (N filas)
- `data/raw/<slug>.html` (HTML crudo)

### Batch por archivo (urls.txt)

1 URL por linea (soporta `#` comentarios):

```text
# Nexo batch
https://nexoinmobiliario.pe/departamentos/jesus-maria/torre-napoles-3900
```

Ejecuta:

```bash
inmo-scrape-nexo-batch --urls-file urls.txt --out-dir data/silver --raw-dir data/raw --debug --min-delay 1.5 --max-delay 3.0
```

Esto genera:
- 1 parquet de proyecto por URL: `data/silver/<slug>__proyecto.parquet`
- 1 parquet de tipologias por URL: `data/silver/<slug>__tipologias.parquet`
- `data/silver/run_<run_id>.txt` con el resumen del batch
- `data/registry/scraper_registry.db` con tracking de runs, artefactos y diffs (hash-based) vs el ultimo run exitoso

## ¿Por qué a veces sale vacío?

Nexo puede cambiar selectores o cargar contenido dinámicamente. Este paquete intenta:
1) extraer cards por selectores comunes
2) fallback por keywords (`Modelo`, `Desde`, `m²`)

Si igual sale vacío, revisa el HTML guardado en `--raw-dir` para confirmar si el contenido viene renderizado por JS.

## Dev

- Python >= 3.9
- Salidas: Parquet (usa `pyarrow`)

