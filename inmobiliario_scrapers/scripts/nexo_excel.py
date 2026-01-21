from __future__ import annotations

import click
from inmobiliario_scrapers.reporting.excel_export import export_excel


@click.command()
@click.option("--in-dir", required=True, help="Carpeta con *__tipologias.parquet (ej: data/silver).")
@click.option("--out-xlsx", required=True, help="Ruta salida xlsx.")
@click.option("--per-project-sheets/--no-per-project-sheets", default=True, show_default=True)
def main(in_dir: str, out_xlsx: str, per_project_sheets: bool) -> None:
    path = export_excel(in_dir=in_dir, out_xlsx=out_xlsx, per_project_sheets=per_project_sheets)
    click.echo(f"[OK] Excel -> {path}")


if __name__ == "__main__":
    main()
