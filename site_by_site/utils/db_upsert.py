# utils/db_upsert.py
from __future__ import annotations

import os
import json
import typing as t
from datetime import datetime

import sqlite3
import psycopg2
import psycopg2.extras


# -----------------------------
# Public API
# -----------------------------
def compute_dedupe_key(row: dict) -> str:
    """
    Mirror the in-memory dedupe order used by JobScraper.dedupe_records():
    Posting ID → Detail URL → Position Title.
    """
    return (
        str(row.get("Posting ID") or "")
        or str(row.get("Detail URL") or "")
        or str(row.get("Position Title") or "")
    )


def upsert_rows(
    db_url: str,
    table: str,
    rows: t.List[dict],
    *,
    extra_indexes: t.Sequence[str] = (),
) -> int:
    """
    Upsert canonical rows into SQLite or Postgres, creating the table if needed.
    Returns the number of rows attempted.

    - Uses a stable natural key: (Vendor, Dedupe Key).
    - Adds basic timestamps: first_seen (on insert), updated_at (always).
    - Stores unknown columns as JSON in _extra to avoid schema churn.
    """
    if not rows:
        return 0

    # Normalize & stamp
    now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    prepped: list[dict] = []
    for r in rows:
        r = dict(r)  # copy
        r["Dedupe Key"] = compute_dedupe_key(r)
        r["_updated_at"] = now_iso
        prepped.append(r)

    if db_url.startswith("sqlite://"):
        path = db_url.replace("sqlite:///", "", 1).replace("sqlite://", "", 1)
        return _sqlite_upsert(path, table, prepped, extra_indexes=extra_indexes)

    # Postgres: postgres:// or postgresql://
    if db_url.startswith(("postgres://", "postgresql://")):
        return _postgres_upsert(db_url, table, prepped, extra_indexes=extra_indexes)
    raise ValueError(
        f"Unsupported db_url scheme for {db_url!r} (use sqlite:/// or postgresql://)"
    )


# -----------------------------
# Column plan
# -----------------------------
# Keep a core set of typed columns (matches CANON_COLUMNS reasonably well),
# plus a JSON catch-all for anything unknown to keep this future-proof.
_CANON_TEXT = {
    "Vendor",
    "Position Title",
    "Detail URL",
    "Description",
    "Post Date",
    "Posting ID",
    "US Person Required",
    "Clearance Level Must Possess",
    "Clearance Level Must Obtain",
    "Relocation Available",
    "Salary Raw",
    "Remote Status",
    "Full Time Status",
    "Hours Per Week",
    "Travel Percentage",
    "Job Category",
    "Business Sector",
    "Business Area",
    "Industry",
    "Shift",
    "Required Education",
    "Preferred Education",
    "Career Level",
    "Required Skills",
    "Preferred Skills",
    "Raw Location",
    "Country",
    "State",
    "City",
    "Postal Code",
}
_CANON_NUM = {
    "Salary Min (USD/yr)",
    "Salary Max (USD/yr)",
    "Latitude",
    "Longitude",
    "Bonus",
}
_CORE_EXTRA = {"Dedupe Key", "_updated_at"}  # timestamps & key
_ALL_COLUMNS = _CANON_TEXT | _CANON_NUM | _CORE_EXTRA


# -----------------------------
# Public: existence check for incremental scraping
# -----------------------------
def get_existing_keys(
    db_url: str,
    table: str,
    vendor: str | None,
    keys: t.Iterable[str],
    *,
    chunk_size: int = 500,
) -> set[str]:
    """
    Return the subset of `keys` that already exist for this vendor in the DB.
    Keys are the same "Dedupe Key" (Posting ID → URL → Title) we upsert with.
    """
    ks = [k for k in set(keys) if k]
    if not ks:
        return set()

    if db_url.startswith("sqlite://"):
        path = db_url.replace("sqlite:///", "", 1).replace("sqlite://", "", 1)
        return _sqlite_exist_keys(path, table, vendor, ks, chunk_size=chunk_size)

    if db_url.startswith(("postgres://", "postgresql://")):
        if psycopg2 is None:
            raise RuntimeError(
                "psycopg2 is required for Postgres queries. Install: pip install psycopg2-binary"
            )
        return _postgres_exist_keys(db_url, table, vendor, ks, chunk_size=chunk_size)

    raise ValueError(
        f"Unsupported db_url scheme for {db_url!r} (use sqlite:/// or postgresql://)"
    )


def _split_known_unknown(row: dict) -> tuple[dict, dict]:
    known, unknown = {}, {}
    for k, v in row.items():
        if k in _ALL_COLUMNS:
            known[k] = v
        else:
            unknown[k] = v
    return known, unknown


# -----------------------------
# SQLite
# -----------------------------
_SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    "Vendor"                        TEXT NOT NULL,
    "Dedupe Key"                    TEXT NOT NULL,
    "Position Title"                TEXT,
    "Detail URL"                    TEXT,
    "Description"                   TEXT,
    "Post Date"                     TEXT,
    "Posting ID"                    TEXT,
    "US Person Required"            TEXT,
    "Clearance Level Must Possess"  TEXT,
    "Clearance Level Must Obtain"   TEXT,
    "Relocation Available"          TEXT,
    "Salary Raw"                    TEXT,
    "Salary Min (USD/yr)"           REAL,
    "Salary Max (USD/yr)"           REAL,
    "Bonus"                         REAL,
    "Remote Status"                 TEXT,
    "Full Time Status"              TEXT,
    "Hours Per Week"                TEXT,
    "Travel Percentage"             TEXT,
    "Job Category"                  TEXT,
    "Business Sector"               TEXT,
    "Business Area"                 TEXT,
    "Industry"                      TEXT,
    "Shift"                         TEXT,
    "Required Education"            TEXT,
    "Preferred Education"           TEXT,
    "Career Level"                  TEXT,
    "Required Skills"               TEXT,
    "Preferred Skills"              TEXT,
    "Raw Location"                  TEXT,
    "Country"                       TEXT,
    "State"                         TEXT,
    "City"                          TEXT,
    "Postal Code"                   TEXT,
    "Latitude"                      REAL,
    "Longitude"                     REAL,
    "_extra"                        TEXT, -- JSON dump of unknown fields
    "first_seen"                    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    "_updated_at"                   TEXT NOT NULL,
    PRIMARY KEY ("Vendor","Dedupe Key")
);
"""


def _sqlite_upsert(
    path: str, table: str, rows: list[dict], *, extra_indexes: t.Sequence[str]
) -> int:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    con = sqlite3.connect(path)
    try:
        con.execute(_SQLITE_DDL.format(table=table))
        # Helpful default indexes (opt-in + a few safe defaults)
        default_ix = {"Post Date", "Posting ID", "Detail URL"}
        for ix in set(extra_indexes) | default_ix:
            con.execute(
                f'CREATE INDEX IF NOT EXISTS idx_{table}_{ix.replace(" ", "_")} ON {table}("{ix}")'
            )

        # Build UPSERT param list
        cols = [
            *sorted(_CANON_TEXT),
            *sorted(_CANON_NUM),
            "Dedupe Key",
            "_extra",
            "_updated_at",
        ]
        placeholders = ",".join(["?"] * len(cols))
        col_csv = ",".join([f'"{c}"' for c in cols])

        # ON CONFLICT ... DO UPDATE
        update_cols = [c for c in cols if c not in ("Vendor", "Dedupe Key")]
        set_csv = ",".join([f'"{c}"=excluded."{c}"' for c in update_cols])

        sql = (
            f"INSERT INTO {table} ({col_csv}) VALUES ({placeholders}) "
            f'ON CONFLICT("Vendor","Dedupe Key") DO UPDATE SET {set_csv}'
        )

        batch = []
        for r in rows:
            known, unknown = _split_known_unknown(r)
            payload = []
            for c in sorted(_CANON_TEXT):
                payload.append(_as_text(known.get(c)))
            for c in sorted(_CANON_NUM):
                payload.append(_as_float(known.get(c)))
            payload.append(known.get("Dedupe Key"))
            payload.append(
                json.dumps(unknown, separators=(",", ":"), ensure_ascii=False)
                if unknown
                else None
            )
            payload.append(known.get("_updated_at"))
            batch.append(tuple(payload))

        con.executemany(sql, batch)
        con.commit()
        return len(rows)
    finally:
        con.close()


def _sqlite_exist_keys(
    path: str, table: str, vendor: str | None, keys: list[str], *, chunk_size: int
) -> set[str]:
    con = sqlite3.connect(path)
    try:
        out: set[str] = set()
        # If the table doesn't exist yet (first run), just return empty quietly.
        try:
            con.execute(f"SELECT 1 FROM {table} LIMIT 1")
        except sqlite3.OperationalError as e:
            if "no such table" in str(e).lower():
                return set()
            raise
        for i in range(0, len(keys), chunk_size):
            chunk = keys[i : i + chunk_size]
            qmarks = ",".join(["?"] * len(chunk))
            if vendor:
                sql = f'SELECT "Dedupe Key" FROM {table} WHERE "Vendor"=? AND "Dedupe Key" IN ({qmarks})'
                rows = con.execute(sql, (vendor, *chunk)).fetchall()
            else:
                sql = (
                    f'SELECT "Dedupe Key" FROM {table} WHERE "Dedupe Key" IN ({qmarks})'
                )
                rows = con.execute(sql, (*chunk,)).fetchall()
            out.update(r[0] for r in rows if r and r[0])
        return out
    finally:
        con.close()


# -----------------------------
# Postgres
# -----------------------------
_PG_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    "Vendor"                        TEXT NOT NULL,
    "Dedupe Key"                    TEXT NOT NULL,
    "Position Title"                TEXT,
    "Detail URL"                    TEXT,
    "Description"                   TEXT,
    "Post Date"                     TEXT,
    "Posting ID"                    TEXT,
    "US Person Required"            TEXT,
    "Clearance Level Must Possess"  TEXT,
    "Clearance Level Must Obtain"   TEXT,
    "Relocation Available"          TEXT,
    "Salary Raw"                    TEXT,
    "Salary Min (USD/yr)"           DOUBLE PRECISION,
    "Salary Max (USD/yr)"           DOUBLE PRECISION,
    "Bonus"                         DOUBLE PRECISION,
    "Remote Status"                 TEXT,
    "Full Time Status"              TEXT,
    "Hours Per Week"                TEXT,
    "Travel Percentage"             TEXT,
    "Job Category"                  TEXT,
    "Business Sector"               TEXT,
    "Business Area"                 TEXT,
    "Industry"                      TEXT,
    "Shift"                         TEXT,
    "Required Education"            TEXT,
    "Preferred Education"           TEXT,
    "Career Level"                  TEXT,
    "Required Skills"               TEXT,
    "Preferred Skills"              TEXT,
    "Raw Location"                  TEXT,
    "Country"                       TEXT,
    "State"                         TEXT,
    "City"                          TEXT,
    "Postal Code"                   TEXT,
    "Latitude"                      DOUBLE PRECISION,
    "Longitude"                     DOUBLE PRECISION,
    "_extra"                        JSONB,
    "first_seen"                    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "_updated_at"                   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY ("Vendor","Dedupe Key")
);
"""


def _postgres_upsert(
    db_url: str, table: str, rows: list[dict], *, extra_indexes: t.Sequence[str]
) -> int:
    conn = psycopg2.connect(db_url)  # type: ignore
    try:
        cur = conn.cursor()
        cur.execute(_PG_DDL.format(table=table))
        default_ix = {"Post Date", "Posting ID", "Detail URL"}
        for ix in set(extra_indexes) | default_ix:
            cur.execute(
                f'CREATE INDEX IF NOT EXISTS idx_{table}_{ix.replace(" ", "_")} ON {table}("{ix}")'
            )

        cols = [
            *sorted(_CANON_TEXT),
            *sorted(_CANON_NUM),
            "Dedupe Key",
            "_extra",
            "_updated_at",
        ]
        col_csv = ",".join([f'"{c}"' for c in cols])
        placeholders = ",".join([f"%({c})s" for c in cols])

        update_cols = [c for c in cols if c not in ("Vendor", "Dedupe Key")]
        set_csv = ",".join([f'"{c}"=EXCLUDED."{c}"' for c in update_cols])

        sql = (
            f"INSERT INTO {table} ({col_csv}) VALUES ({placeholders}) "
            f'ON CONFLICT ("Vendor","Dedupe Key") DO UPDATE SET {set_csv}'
        )

        prepped_dicts = []
        for r in rows:
            known, unknown = _split_known_unknown(r)
            d = {}
            for c in sorted(_CANON_TEXT):
                d[c] = _as_text(known.get(c))
            for c in sorted(_CANON_NUM):
                d[c] = _as_float(known.get(c))
            d["Dedupe Key"] = known.get("Dedupe Key")
            d["_extra"] = (
                json.dumps(unknown, separators=(",", ":"), ensure_ascii=False)
                if unknown
                else None
            )
            d["_updated_at"] = known.get("_updated_at")
            prepped_dicts.append(d)

        psycopg2.extras.execute_batch(cur, sql, prepped_dicts, page_size=500)  # type: ignore
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def _postgres_exist_keys(
    db_url: str, table: str, vendor: str | None, keys: list[str], *, chunk_size: int
) -> set[str]:
    conn = psycopg2.connect(db_url)  # type: ignore
    try:
        cur = conn.cursor()
        out: set[str] = set()
        # If table is missing (first run), return empty quietly.
        try:
            cur.execute(f"SELECT 1 FROM {table} LIMIT 1")
        except Exception as e:  # psycopg2.ProgrammingError / UndefinedTable
            # Check for undefined table without importing dialect-specific errors
            if (
                "undefined table" in str(e).lower()
                or "does not exist" in str(e).lower()
            ):
                return set()
            raise
        for i in range(0, len(keys), chunk_size):
            chunk = keys[i : i + chunk_size]
            placeholders = ",".join(["%s"] * len(chunk))
            if vendor:
                sql = f'SELECT "Dedupe Key" FROM {table} WHERE "Vendor"=%s AND "Dedupe Key" IN ({placeholders})'
                cur.execute(sql, (vendor, *chunk))
            else:
                sql = f'SELECT "Dedupe Key" FROM {table} WHERE "Dedupe Key" IN ({placeholders})'
                cur.execute(sql, (*chunk,))
            out.update(r[0] for r in cur.fetchall() if r and r[0])
        return out
    finally:
        conn.close()


def _as_text(v):
    if v is None:
        return None
    return str(v)


def _as_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None
