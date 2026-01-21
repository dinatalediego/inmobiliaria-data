from __future__ import annotations

import re
from pathlib import Path

import click

from inmobiliario_scrapers.nexo.scraper import NexoScraper
from inmobiliario_scrapers.pipelines.parquet_writer import write_project_and_tipologias


def _slugify(url: str) -> str:
    # stable filename component
    s = re.sub(r"[^a-zA-Z0-9]+", "-", url).strip("-")
    return s[-80:] if len(s) > 80 else s


@click.command()
@click.argument("url")
@click.argument("output_project_parquet")
@click.option("--output-tipologias", default=None, help="Ruta parquet (tipologías).")
@click.option("--raw-dir", default=None, help="Directorio para guardar HTML crudo.")
@click.option("--rate-limit", default=0.0, type=float, help="Sleep (segundos) antes de cada request.")
def main(url: str, output_project_parquet: str, output_tipologias: str | None, raw_dir: str | None, rate_limit: float) -> None:
    """Scrapea un proyecto de Nexo Inmobiliario y guarda Parquet."""
    scraper = NexoScraper()
    scraper.http_cfg.rate_limit_s = rate_limit

    slug = _slugify(url)
    if raw_dir:
        Path(raw_dir).mkdir(parents=True, exist_ok=True)
        raw_dir = str(Path(raw_dir))

    payload = scraper.run(url, raw_dir=raw_dir, slug=slug)
    out_proj, out_tip = write_project_and_tipologias(payload, output_project_parquet, output_tipologias)

    click.echo(f"[OK] proyecto -> {out_proj}")
    click.echo(f"[OK] tipologias -> {out_tip}")
    click.echo(f"[INFO] tipologias extraidas: {payload.get('tipologias') and len(payload['tipologias']) or 0}")


if __name__ == "__main__":
    main()
