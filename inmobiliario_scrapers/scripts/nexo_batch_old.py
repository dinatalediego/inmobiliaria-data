from __future__ import annotations

import random
import re
from pathlib import Path

import click

from inmobiliario_scrapers.nexo.scraper import NexoScraper
from inmobiliario_scrapers.pipelines.parquet_writer import write_project_and_tipologias
from inmobiliario_scrapers.registry.sqlite_registry import RegistryDB, Timer, sha256_file


def _read_urls(urls_file: str) -> list[str]:
    p = Path(urls_file)
    lines = p.read_text(encoding="utf-8").splitlines()
    urls: list[str] = []
    for line in lines:
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        urls.append(s)
    return urls


def _slugify(url: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "-", url).strip("-")
    return s[-80:] if len(s) > 80 else s


@click.command()
@click.option("--urls-file", required=True, help="TXT con 1 URL por linea (soporta # comentarios).")
@click.option("--out-dir", required=True, help="Directorio destino para parquets.")
@click.option("--raw-dir", default=None, help="Directorio para snapshots raw (html/cards).")
@click.option("--registry-db", default="data/registry/scraper_registry.db", show_default=True, help="SQLite registry path.")
@click.option("--min-delay", default=0.0, type=float, help="Delay minimo entre URLs (segundos).")
@click.option("--max-delay", default=0.0, type=float, help="Delay maximo entre URLs (segundos).")
@click.option("--debug", is_flag=True, help="Guarda html + cards.txt por URL.")
def main(urls_file: str, out_dir: str, raw_dir: str | None, registry_db: str, min_delay: float, max_delay: float, debug: bool) -> None:
    """Scrape batch para Nexo: lee urls desde un archivo y genera outputs + registry + diffs."""
    urls = _read_urls(urls_file)
    if not urls:
        raise click.ClickException("urls_file no contiene URLs (o todas son comentarios/vacias).")

    outp = Path(out_dir)
    outp.mkdir(parents=True, exist_ok=True)
    rawp = Path(raw_dir) if raw_dir else (outp / "raw")
    if debug:
        rawp.mkdir(parents=True, exist_ok=True)

    reg = RegistryDB(registry_db)
    run = reg.start_run(
        source="nexo_inmobiliario",
        urls_count=len(urls),
        fetch_mode="requests",
        user_agent="",
        min_delay_s=min_delay,
        max_delay_s=max_delay,
        notes=f"urls_file={urls_file}",
    )

    run_log = outp / f"run_{run.run_id}.txt"

    scraper = NexoScraper()

    ok = 0
    fail = 0
    with run_log.open("w", encoding="utf-8") as f:
        f.write(f"RUN_ID: {run.run_id}\n")
        f.write(f"SOURCE: {run.source}\n")
        f.write(f"STARTED_AT: {run.started_at}\n")
        f.write(f"URLS_FILE: {urls_file}\n")
        f.write(f"OUT_DIR: {outp}\n")
        f.write(f"RAW_DIR: {rawp if debug else '(disabled)'}\n")
        f.write(f"DELAY: min={min_delay}s max={max_delay}s\n\n")

        reg.log_artifact(run.run_id, artifact_type="run_log", path=str(run_log), url=None, compute_hash=True)
        reg.log_artifact(run.run_id, artifact_type="registry_db", path=str(registry_db), url=None, compute_hash=False)

        for i, url in enumerate(urls, start=1):
            t = Timer()
            slug = _slugify(url)

            # polite delay before hitting server
            if max_delay > 0:
                d = random.uniform(min_delay, max_delay)
                if d > 0:
                    import time as _time

                    _time.sleep(d)
            elif min_delay > 0:
                import time as _time

                _time.sleep(min_delay)

            try:
                payload = scraper.run(url, raw_dir=str(rawp) if debug else None, slug=slug, debug=debug)

                proj_path = outp / f"{slug}__proyecto.parquet"
                tip_path = outp / f"{slug}__tipologias.parquet"
                out_proj, out_tip = write_project_and_tipologias(payload, str(proj_path), str(tip_path))

                # register artifacts
                reg.log_artifact(run.run_id, artifact_type="project_parquet", path=out_proj, url=url, rows=1)
                reg.log_artifact(run.run_id, artifact_type="tipologias_parquet", path=out_tip, url=url, rows=int(payload.get("tipologias_rows") or 0))

                if debug:
                    html_path = rawp / f"{slug}.html"
                    cards_path = rawp / f"{slug}__cards.txt"
                    if html_path.exists():
                        reg.log_artifact(run.run_id, artifact_type="raw_html", path=str(html_path), url=url)
                    if cards_path.exists():
                        reg.log_artifact(run.run_id, artifact_type="cards_txt", path=str(cards_path), url=url)

                # diff vs last success (hash-based)
                prev_run_id, prev_hash = reg.get_last_success_tipologias_hash(url)
                cur_hash = sha256_file(out_tip)
                if prev_hash is None:
                    reg.log_diff(run.run_id, url, prev_run_id, diff_status="new_url", diff={"cur_sha256": cur_hash})
                    diff_note = "new_url"
                elif prev_hash == cur_hash:
                    reg.log_diff(run.run_id, url, prev_run_id, diff_status="no_change", diff={"cur_sha256": cur_hash, "prev_sha256": prev_hash})
                    diff_note = "no_change"
                else:
                    reg.log_diff(run.run_id, url, prev_run_id, diff_status="changed", diff={"cur_sha256": cur_hash, "prev_sha256": prev_hash})
                    diff_note = "changed"

                reg.log_url_result(
                    run_id=run.run_id,
                    url=url,
                    status="ok",
                    duration_ms=t.ms(),
                    extracted_cards_count=payload.get("extracted_cards_count"),
                    tipologias_rows=payload.get("tipologias_rows"),
                    parse_ok_rows=payload.get("parse_ok_rows"),
                    parse_fail_rows=payload.get("parse_fail_rows"),
                )

                ok += 1
                f.write(f"[OK]   {i}/{len(urls)} {url}\n")
                f.write(f"       project={Path(out_proj).name} tipologias={Path(out_tip).name} diff={diff_note}\n")
                f.write(f"       rows={payload.get('tipologias_rows')} parse_ok={payload.get('parse_ok_rows')} parse_fail={payload.get('parse_fail_rows')}\n")

            except Exception as e:
                fail += 1
                reg.log_url_result(
                    run_id=run.run_id,
                    url=url,
                    status="fail",
                    error_type=type(e).__name__,
                    error_msg=str(e),
                    duration_ms=t.ms(),
                )
                f.write(f"[FAIL] {i}/{len(urls)} {url}\n")
                f.write(f"       error={type(e).__name__}: {e}\n")

        f.write("\n")
        f.write(f"SUMMARY ok={ok} fail={fail} total={len(urls)}\n")

    reg.finalize_run(run.run_id)
    reg.close()

    click.echo(f"[DONE] run_id={run.run_id} ok={ok} fail={fail} total={len(urls)}")
    click.echo(f"[LOG]  {run_log}")


if __name__ == "__main__":
    main()
