"""migrate_to_pg.py (v2.67.162)
================================

One-shot migrator: copy the worker's SQLite team_actions.db to a
Render Postgres instance, so that both the web service and the
worker can stop fighting over two physically separate disks and
read/write a single shared DB.

How it works
------------
1. Open the SQLite source (DB_PATH from data_paths.py)
2. Introspect every table via `sqlite_master` + `PRAGMA
   table_info` so the schema definition stays single-source-of-
   truth in db.py
3. Translate each column to its Postgres equivalent on the fly
4. CREATE TABLE IF NOT EXISTS in Postgres, plus CREATE INDEX IF
   NOT EXISTS for each index seen in sqlite_master
5. Copy rows in batches of 1000 per table
6. Verify counts match — log discrepancies

Idempotency
-----------
- Schema DDL uses IF NOT EXISTS so re-runs are safe
- Data copy uses ON CONFLICT DO NOTHING with the table's UNIQUE
  constraint columns where possible; otherwise it skips tables
  that already have rows unless --force-overwrite is passed
- --tables filter lets you migrate one table at a time during
  verification

Usage
-----
    # Dry-run: shows the DDL that would be applied, no writes
    python migrate_to_pg.py --dry-run

    # Apply schema only, no data copy
    python migrate_to_pg.py --schema-only

    # Full migration (schema + data)
    python migrate_to_pg.py

    # Re-migrate a specific table (overwrites if present)
    python migrate_to_pg.py --tables demand_signals \
        --force-overwrite

Env vars
--------
    DATABASE_URL    Postgres connection string. Render gives you
                    this when you provision a Postgres add-on.
                    Format: postgres://user:pass@host:port/db

Caveats
-------
- COLLATE NOCASE → Postgres uses CITEXT extension; this script
  drops the collation and relies on application-level UPPER()
  checks instead. Pre-existing UPPER() patterns in db.py work
  unchanged.
- AUTOINCREMENT → BIGSERIAL. After data copy, the sequence is
  bumped past the max id so future inserts don't collide.
- TIMESTAMP without TZ → TIMESTAMPTZ assuming UTC. Caller
  responsible if there's local-time data.
- REAL → DOUBLE PRECISION. Money columns may want NUMERIC for
  precision; convert manually post-migration if needed.

After migration
---------------
1. Run this script (once it succeeds, both DBs are in sync)
2. Add DATABASE_URL to BOTH Render services
3. Set DB_BACKEND=postgres on BOTH services
4. Restart both — they now share the Postgres DB
5. Roll back by unsetting DB_BACKEND (defaults to sqlite)
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

try:
    from data_paths import DB_PATH
except ImportError:
    DB_PATH = "team_actions.db"

log = logging.getLogger("migrate_to_pg")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s")


# ---------------------------------------------------------------------------
# Type mapping — SQLite declared types → Postgres column types
# ---------------------------------------------------------------------------
# SQLite is loosely typed (type affinity); we map the declared type
# string from PRAGMA table_info to a strict Postgres type.

def _pg_type(sqlite_type: str) -> str:
    t = (sqlite_type or "").upper().strip()
    if not t:
        return "TEXT"
    # Strip parens like VARCHAR(255) — Postgres TEXT has no limit
    base = re.sub(r"\(.*?\)", "", t).strip()
    if "INT" in base:
        return "BIGINT"
    if base in ("REAL", "FLOAT", "DOUBLE"):
        return "DOUBLE PRECISION"
    if "NUMERIC" in base or "DECIMAL" in base:
        return "NUMERIC"
    if "TIMESTAMP" in base or "DATETIME" in base:
        return "TIMESTAMPTZ"
    if base == "DATE":
        return "DATE"
    if base == "BOOLEAN":
        return "BOOLEAN"
    if "BLOB" in base:
        return "BYTEA"
    # Everything else collapses to TEXT — covers TEXT, VARCHAR,
    # CHAR, CLOB, JSON-as-text, etc.
    return "TEXT"


def _is_autoinc_pk(sqlite_type: str, pk: int, name: str) -> bool:
    """True if this column is an INTEGER PRIMARY KEY AUTOINCREMENT
    in SQLite — needs to become BIGSERIAL in Postgres."""
    if pk != 1:
        return False
    t = (sqlite_type or "").upper().strip()
    return "INT" in t


def _translate_default(default: Optional[str]) -> Optional[str]:
    """SQLite default expressions → Postgres equivalents."""
    if default is None:
        return None
    d = str(default).strip()
    # datetime('now') → NOW()
    if "datetime('now')" in d.lower():
        return "NOW()"
    # CURRENT_TIMESTAMP works in both
    if d.upper() == "CURRENT_TIMESTAMP":
        return "CURRENT_TIMESTAMP"
    # Pass through literals and other functions unchanged
    return d


# ---------------------------------------------------------------------------
# Schema introspection — SQLite → DDL
# ---------------------------------------------------------------------------

def _list_user_tables(sconn: sqlite3.Connection) -> List[str]:
    """All non-internal tables. Skip sqlite_*, WAL artefacts, and
    auto-generated FTS shadow tables."""
    rows = sconn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' "
        "AND name NOT LIKE '%_fts_%' "
        "ORDER BY name").fetchall()
    return [r[0] for r in rows]


def _table_columns(sconn: sqlite3.Connection,
                       table: str
                       ) -> List[Tuple[str, str, int, Optional[str],
                                           int, int]]:
    """PRAGMA table_info: returns
        (cid, name, type, notnull, dflt_value, pk)
    per column."""
    rows = sconn.execute(
        f'PRAGMA table_info("{table}")').fetchall()
    out = []
    for cid, name, typ, notnull, dflt, pk in rows:
        out.append((name, typ, notnull, dflt, pk, cid))
    return out


def _unique_constraints(sconn: sqlite3.Connection,
                              table: str
                              ) -> List[List[str]]:
    """Composite UNIQUE constraints from sqlite_master. Returns a
    list of column-name lists."""
    row = sconn.execute(
        "SELECT sql FROM sqlite_master "
        "WHERE type='table' AND name=?",
        (table,)).fetchone()
    if not row or not row[0]:
        return []
    sql = row[0]
    # Match UNIQUE(col1, col2, ...) at table level
    out = []
    for m in re.finditer(
            r"\bUNIQUE\s*\(([^)]+)\)", sql, re.IGNORECASE):
        cols = [c.strip().strip('"') for c in m.group(1).split(",")]
        out.append([c for c in cols if c])
    return out


def _indexes(sconn: sqlite3.Connection,
                table: str) -> List[Tuple[str, str]]:
    """User-defined indexes for `table`. Returns (index_name,
    create_sql) pairs. Auto-indexes created for UNIQUE constraints
    are filtered out (sqlite_autoindex_*)."""
    rows = sconn.execute(
        "SELECT name, sql FROM sqlite_master "
        "WHERE type='index' AND tbl_name=? "
        "AND name NOT LIKE 'sqlite_autoindex_%' "
        "AND sql IS NOT NULL",
        (table,)).fetchall()
    return [(r[0], r[1]) for r in rows]


def _emit_create_table(sconn: sqlite3.Connection,
                              table: str) -> str:
    """Build the Postgres CREATE TABLE statement for `table`."""
    cols = _table_columns(sconn, table)
    if not cols:
        raise RuntimeError(f"No columns found for table {table}")
    col_defs: List[str] = []
    # SQLite's PRAGMA table_info reports pk as 1, 2, 3, ... giving
    # the column's position within the PK (not just "is a PK
    # column"). We collect (pk_position, name) pairs and sort, so
    # composite PKs like (channel_id, ts) come out in the right
    # order.
    pk_entries: List[Tuple[int, str]] = []
    autoinc_emitted = False
    # First pass: detect whether the table has a single-col
    # INTEGER AUTOINCREMENT pk. If so, that column becomes
    # BIGSERIAL PRIMARY KEY inline. Composite PKs never use
    # AUTOINCREMENT in SQLite.
    pk_count = sum(1 for c in cols if c[4] > 0)
    has_single_autoinc_pk = (
        pk_count == 1
        and any(c[4] == 1 and _is_autoinc_pk(c[1], c[4], c[0])
                for c in cols))
    for name, typ, notnull, dflt, pk, _cid in cols:
        if has_single_autoinc_pk and pk == 1 \
                and _is_autoinc_pk(typ, pk, name):
            col_defs.append(f'"{name}" BIGSERIAL PRIMARY KEY')
            autoinc_emitted = True
            continue
        parts = [f'"{name}"', _pg_type(typ)]
        if notnull:
            parts.append("NOT NULL")
        d = _translate_default(dflt)
        if d is not None:
            parts.append(f"DEFAULT {d}")
        col_defs.append(" ".join(parts))
        if pk > 0:
            pk_entries.append((pk, name))
    if pk_entries and not autoinc_emitted:
        ordered = [f'"{n}"'
                      for _, n in sorted(pk_entries,
                                              key=lambda x: x[0])]
        col_defs.append(f"PRIMARY KEY ({', '.join(ordered)})")
    # Composite UNIQUE constraints
    for uniq in _unique_constraints(sconn, table):
        quoted = ", ".join(f'"{c}"' for c in uniq)
        col_defs.append(f"UNIQUE ({quoted})")
    body = ",\n    ".join(col_defs)
    return (f'CREATE TABLE IF NOT EXISTS "{table}" (\n    '
              f"{body}\n);")


def _emit_indexes(sconn: sqlite3.Connection,
                       table: str) -> List[str]:
    """Translate each user-defined SQLite index to Postgres."""
    out: List[str] = []
    for name, sql in _indexes(sconn, table):
        # Most SQLite index SQL is portable. Tweaks:
        # - 'CREATE INDEX' → 'CREATE INDEX IF NOT EXISTS'
        # - COLLATE NOCASE inside index spec: drop it (PG would
        #   need CITEXT to do equivalent)
        s = re.sub(r"^CREATE\s+(UNIQUE\s+)?INDEX\s+",
                       r"CREATE \1INDEX IF NOT EXISTS ",
                       sql, count=1, flags=re.IGNORECASE)
        s = re.sub(r"COLLATE\s+NOCASE", "", s, flags=re.IGNORECASE)
        if not s.rstrip().endswith(";"):
            s += ";"
        out.append(s)
    return out


# ---------------------------------------------------------------------------
# Data copy
# ---------------------------------------------------------------------------

def _copy_rows(sconn: sqlite3.Connection,
                  pconn,
                  table: str,
                  conflict_cols: Optional[List[str]],
                  force_overwrite: bool,
                  batch: int = 1000) -> Tuple[int, int]:
    """Copy rows from SQLite → Postgres. Returns (read, written).

    Conflict handling:
      - If conflict_cols is set, use ON CONFLICT DO NOTHING on
        those columns (UPSERT-skip mode).
      - If force_overwrite is set, TRUNCATE the PG table first.
      - Otherwise, skip the table entirely if PG already has
        rows (safer default for re-runs)."""
    cols = [c[0] for c in _table_columns(sconn, table)]
    if not cols:
        return (0, 0)

    pcur = pconn.cursor()

    if force_overwrite:
        pcur.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY')
        log.info("  %s: TRUNCATE applied (force-overwrite)", table)
    else:
        pcur.execute(f'SELECT COUNT(*) FROM "{table}"')
        existing = pcur.fetchone()[0]
        if existing > 0 and not conflict_cols:
            log.info(
                "  %s: SKIP (Postgres already has %d rows, no "
                "conflict-cols to dedupe on, --force-overwrite "
                "not set)", table, existing)
            return (0, 0)

    src_rows = sconn.execute(
        f'SELECT * FROM "{table}"').fetchall()
    if not src_rows:
        log.info("  %s: 0 rows in SQLite", table)
        return (0, 0)

    col_list = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    base_sql = (f'INSERT INTO "{table}" ({col_list}) '
                  f"VALUES ({placeholders})")
    if conflict_cols:
        conflict = ", ".join(f'"{c}"' for c in conflict_cols)
        base_sql += f" ON CONFLICT ({conflict}) DO NOTHING"

    written = 0
    for i in range(0, len(src_rows), batch):
        chunk = [tuple(row) for row in src_rows[i:i + batch]]
        pcur.executemany(base_sql, chunk)
        written += len(chunk)
    pconn.commit()

    # Bump BIGSERIAL sequence past max id so future inserts don't
    # collide with copied data.
    pk_col = next(
        (c[0] for c in _table_columns(sconn, table)
          if c[4] == 1), None)
    if pk_col:
        # Best-effort — only matters if it's a serial pk
        try:
            pcur.execute(
                f"SELECT pg_get_serial_sequence(%s, %s)",
                (table, pk_col))
            seq = pcur.fetchone()[0]
            if seq:
                pcur.execute(
                    f'SELECT setval(%s, (SELECT '
                    f'COALESCE(MAX("{pk_col}"), 1) FROM '
                    f'"{table}"))', (seq,))
                pconn.commit()
        except Exception:
            pconn.rollback()

    log.info("  %s: %d rows read, %d INSERTed",
                table, len(src_rows), written)
    return (len(src_rows), written)


# ---------------------------------------------------------------------------
# Conflict-column heuristic — pick natural unique key per table
# ---------------------------------------------------------------------------

def _conflict_cols_for(sconn: sqlite3.Connection,
                              table: str) -> Optional[List[str]]:
    """Prefer the first composite UNIQUE constraint declared.
    Else fall back to the primary key (single-col) — but only if
    it's the AUTOINCREMENT row id. Returns None if no suitable
    key (caller decides whether to overwrite/skip)."""
    uniqs = _unique_constraints(sconn, table)
    if uniqs:
        return uniqs[0]
    pks = [c[0] for c in _table_columns(sconn, table) if c[4] == 1]
    if len(pks) == 1:
        return pks
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=("Migrate team_actions.db SQLite → Render "
                        "Postgres for cross-service shared state."))
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the DDL and counts, no PG writes")
    parser.add_argument(
        "--schema-only", action="store_true",
        help="Apply schema in PG, skip data copy")
    parser.add_argument(
        "--tables", nargs="+",
        help="Subset of tables to migrate (default: all)")
    parser.add_argument(
        "--force-overwrite", action="store_true",
        help=("TRUNCATE each target table before copying. Use "
                "only when you know the PG copy is stale or wrong."))
    parser.add_argument(
        "--sqlite-path", default=str(DB_PATH),
        help=f"SQLite source path (default: {DB_PATH})")
    args = parser.parse_args()

    src_path = Path(args.sqlite_path)
    if not src_path.exists():
        log.error("SQLite source not found: %s", src_path)
        return 2

    # v2.67.164 — Accept either DATABASE_URL (the 12-factor
    # convention) or INTERNAL_DATABASE_URL (the prefix Render
    # uses when you save the internal URL with its dashboard
    # label). DATABASE_URL wins if both are set.
    pg_url = (os.environ.get("DATABASE_URL", "").strip()
                or os.environ.get(
                    "INTERNAL_DATABASE_URL", "").strip())
    if not pg_url and not args.dry_run:
        log.error("DATABASE_URL / INTERNAL_DATABASE_URL not set "
                      "— refusing to proceed.")
        log.error("Set the Render Postgres internal URL and retry.")
        return 2

    sconn = sqlite3.connect(str(src_path))
    sconn.row_factory = sqlite3.Row
    tables = _list_user_tables(sconn)
    if args.tables:
        wanted = set(args.tables)
        skipped = sorted(set(tables) - wanted)
        tables = [t for t in tables if t in wanted]
        log.info("Filter applied — %d tables, skipping %d "
                  "others: %s", len(tables), len(skipped),
                  ", ".join(skipped[:5]) + (" …" if len(skipped) > 5
                                                  else ""))

    log.info("Source: %s  (%d tables)", src_path, len(tables))

    if args.dry_run:
        for t in tables:
            ddl = _emit_create_table(sconn, t)
            idx = _emit_indexes(sconn, t)
            log.info("--- %s ---", t)
            print(ddl)
            for s in idx:
                print(s)
            cnt = sconn.execute(
                f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            log.info("  rows in SQLite: %d", cnt)
        sconn.close()
        return 0

    # Real connect to Postgres
    try:
        import psycopg  # type: ignore
    except ImportError:
        log.error("psycopg not installed — run "
                  "`pip install 'psycopg[binary]>=3.1'`")
        sconn.close()
        return 2
    pconn = psycopg.connect(pg_url)
    log.info("Connected to Postgres")

    # 1. Schema
    log.info("Applying schema (%d tables)…", len(tables))
    with pconn.cursor() as pcur:
        for t in tables:
            pcur.execute(_emit_create_table(sconn, t))
            for stmt in _emit_indexes(sconn, t):
                pcur.execute(stmt)
        pconn.commit()
    log.info("Schema applied.")

    if args.schema_only:
        log.info("--schema-only → done.")
        pconn.close()
        sconn.close()
        return 0

    # 2. Data
    log.info("Copying data…")
    total_read = total_written = 0
    for t in tables:
        conflict = _conflict_cols_for(sconn, t)
        r, w = _copy_rows(sconn, pconn, t, conflict,
                              args.force_overwrite)
        total_read += r
        total_written += w
    log.info("Data copy complete. Read=%d Written=%d",
                total_read, total_written)

    # 3. Verify counts (sanity check)
    log.info("Verifying row counts…")
    mismatches = 0
    with pconn.cursor() as pcur:
        for t in tables:
            sc = sconn.execute(
                f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            pcur.execute(f'SELECT COUNT(*) FROM "{t}"')
            pc = pcur.fetchone()[0]
            if sc != pc:
                log.warning("  %s: SQLite=%d Postgres=%d "
                              "(diff=%d)", t, sc, pc, pc - sc)
                mismatches += 1
    if mismatches:
        log.warning("%d tables had count mismatches — see above. "
                      "Could be benign (e.g. conflict-cols "
                      "deduped duplicates) or a real bug.",
                      mismatches)
    else:
        log.info("All %d tables match.", len(tables))

    pconn.close()
    sconn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
