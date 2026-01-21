from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def file_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


@dataclass
class RunInfo:
    run_id: str
    started_at: str
    source: str
    fetch_mode: str
    user_agent: str
    min_delay_s: float
    max_delay_s: float
    urls_count: int


class RegistryDB:
    """A lightweight SQLite registry to track runs, per-URL results, and produced artifacts.

    Designed for reproducibility: every run has an ID; every artifact has a hash;
    and we can diff current outputs vs last successful run.
    """

    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def _init_schema(self) -> None:
        cur = self._conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
              run_id TEXT PRIMARY KEY,
              source TEXT NOT NULL,
              started_at TEXT NOT NULL,
              ended_at TEXT,
              urls_count INTEGER NOT NULL,
              ok_count INTEGER DEFAULT 0,
              fail_count INTEGER DEFAULT 0,
              fetch_mode TEXT,
              user_agent TEXT,
              min_delay_s REAL DEFAULT 0,
              max_delay_s REAL DEFAULT 0,
              notes TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS url_results (
              run_id TEXT NOT NULL,
              url TEXT NOT NULL,
              status TEXT NOT NULL,
              http_status INTEGER,
              error_type TEXT,
              error_msg TEXT,
              duration_ms INTEGER,
              html_sha256 TEXT,
              extracted_cards_count INTEGER,
              tipologias_rows INTEGER,
              parse_ok_rows INTEGER,
              parse_fail_rows INTEGER,
              created_at TEXT NOT NULL,
              PRIMARY KEY (run_id, url)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS artifacts (
              run_id TEXT NOT NULL,
              url TEXT,
              artifact_type TEXT NOT NULL,
              path TEXT NOT NULL,
              sha256 TEXT,
              bytes INTEGER,
              rows INTEGER,
              created_at TEXT NOT NULL,
              PRIMARY KEY (run_id, path)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS diffs (
              run_id TEXT NOT NULL,
              url TEXT NOT NULL,
              compare_to_run_id TEXT,
              diff_status TEXT NOT NULL,
              diff_json TEXT,
              created_at TEXT NOT NULL,
              PRIMARY KEY (run_id, url)
            )
            """
        )

        self._conn.commit()

    # ---------- Runs ----------
    def start_run(
        self,
        source: str,
        urls_count: int,
        fetch_mode: str = "requests",
        user_agent: str = "",
        min_delay_s: float = 0.0,
        max_delay_s: float = 0.0,
        notes: str | None = None,
    ) -> RunInfo:
        run_id = str(uuid.uuid4())
        started_at = utc_now().isoformat()
        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT INTO runs(run_id, source, started_at, urls_count, fetch_mode, user_agent, min_delay_s, max_delay_s, notes)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (run_id, source, started_at, int(urls_count), fetch_mode, user_agent, float(min_delay_s), float(max_delay_s), notes),
        )
        self._conn.commit()
        return RunInfo(
            run_id=run_id,
            started_at=started_at,
            source=source,
            fetch_mode=fetch_mode,
            user_agent=user_agent,
            min_delay_s=min_delay_s,
            max_delay_s=max_delay_s,
            urls_count=urls_count,
        )

    def finalize_run(self, run_id: str) -> None:
        cur = self._conn.cursor()
        cur.execute("SELECT status, COUNT(*) as c FROM url_results WHERE run_id=? GROUP BY status", (run_id,))
        rows = cur.fetchall()
        ok = 0
        fail = 0
        for r in rows:
            if r["status"] == "ok":
                ok = int(r["c"])
            elif r["status"] == "fail":
                fail = int(r["c"])

        cur.execute(
            """
            UPDATE runs
            SET ended_at=?, ok_count=?, fail_count=?
            WHERE run_id=?
            """,
            (utc_now().isoformat(), ok, fail, run_id),
        )
        self._conn.commit()

    # ---------- URL results ----------
    def log_url_result(
        self,
        run_id: str,
        url: str,
        status: str,
        http_status: int | None = None,
        error_type: str | None = None,
        error_msg: str | None = None,
        duration_ms: int | None = None,
        html_sha256: str | None = None,
        extracted_cards_count: int | None = None,
        tipologias_rows: int | None = None,
        parse_ok_rows: int | None = None,
        parse_fail_rows: int | None = None,
    ) -> None:
        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO url_results(
              run_id, url, status, http_status, error_type, error_msg, duration_ms,
              html_sha256, extracted_cards_count, tipologias_rows, parse_ok_rows, parse_fail_rows, created_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                run_id,
                url,
                status,
                http_status,
                error_type,
                error_msg,
                duration_ms,
                html_sha256,
                extracted_cards_count,
                tipologias_rows,
                parse_ok_rows,
                parse_fail_rows,
                utc_now().isoformat(),
            ),
        )
        self._conn.commit()

    # ---------- Artifacts ----------
    def log_artifact(
        self,
        run_id: str,
        artifact_type: str,
        path: str,
        url: str | None = None,
        rows: int | None = None,
        compute_hash: bool = True,
    ) -> None:
        p = str(path)
        h = sha256_file(p) if (compute_hash and os.path.exists(p)) else None
        b = file_size(p)
        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO artifacts(run_id, url, artifact_type, path, sha256, bytes, rows, created_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (run_id, url, artifact_type, p, h, b, rows, utc_now().isoformat()),
        )
        self._conn.commit()

    # ---------- Diffs ----------
    def get_last_success_tipologias_hash(self, url: str) -> tuple[str | None, str | None]:
        """Return (run_id, sha256) for the latest successful tipologias parquet for a URL."""
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT a.run_id, a.sha256
            FROM artifacts a
            JOIN url_results u ON u.run_id=a.run_id AND (u.url=a.url OR a.url IS NULL)
            WHERE a.artifact_type='tipologias_parquet' AND a.url=? AND u.status='ok'
            ORDER BY u.created_at DESC
            LIMIT 1
            """,
            (url,),
        )
        row = cur.fetchone()
        if not row:
            return None, None
        return row["run_id"], row["sha256"]

    def log_diff(self, run_id: str, url: str, compare_to_run_id: str | None, diff_status: str, diff: dict[str, Any] | None = None) -> None:
        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO diffs(run_id, url, compare_to_run_id, diff_status, diff_json, created_at)
            VALUES(?,?,?,?,?,?)
            """,
            (
                run_id,
                url,
                compare_to_run_id,
                diff_status,
                json.dumps(diff or {}, ensure_ascii=False),
                utc_now().isoformat(),
            ),
        )
        self._conn.commit()


class Timer:
    def __init__(self):
        self.t0 = time.time()

    def ms(self) -> int:
        return int((time.time() - self.t0) * 1000)
