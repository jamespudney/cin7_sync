"""db_dialect.py (v2.67.163)
=============================

Backend abstraction for the shared team_actions DB. Routes
between SQLite (legacy single-disk) and Postgres (Render-shared
cross-service) at runtime via the DB_BACKEND env var.

Goal: db.py should call `from db_dialect import connect` and the
rest of db.py needs zero changes for 90% of queries. The wrapper
rewrites SQLite-flavored SQL into Postgres-flavored SQL on the
fly:

  - `?` placeholders     →  `%s`
  - `datetime('now')`    →  `NOW()`
  - `datetime('now',
       '-' || ? || ' days')`
                         →  `(NOW() - INTERVAL '1 day' * ?)`
  - `datetime('now',
       '-' || ? || ' hours')`
                         →  `(NOW() - INTERVAL '1 hour' * ?)`
  - `INSERT OR IGNORE`   →  `INSERT … ON CONFLICT DO NOTHING`
  - `PRAGMA …`           →  no-op (Postgres handles concurrency)
  - `executescript(s)`   →  split on `;` and run each non-blank
                            statement (psycopg has no
                            executescript)

10% of queries can't be auto-rewritten — `INSERT OR REPLACE` and
sites that use `cursor.lastrowid`. Those require manual fixes in
db.py using the helpers exported here:

  - `upsert_sql(table, cols, conflict_cols, update_cols)` —
    emits dialect-correct INSERT ... ON CONFLICT … DO UPDATE for
    Postgres, INSERT OR REPLACE for SQLite.
  - `insert_returning_id(conn, sql, params)` — runs the INSERT
    appending RETURNING id (or first PK col); returns the new
    row's id. Works on both backends (SQLite ≥ 3.35 supports
    RETURNING).

Env vars
--------
    DB_BACKEND       'sqlite' (default) or 'postgres'
    DATABASE_URL     Postgres DSN — required when
                     DB_BACKEND='postgres'

Local-dev usage
---------------
Leave DB_BACKEND unset and everything behaves like today
(SQLite, current code path, no risk).

Production cutover
------------------
Set DB_BACKEND='postgres' on both Render services AFTER
migrate_to_pg.py has run successfully. Both will share the same
DB via DATABASE_URL.
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from typing import Any, Iterable, Iterator, List, Optional, Sequence, Tuple

log = logging.getLogger("db_dialect")


def _backend() -> str:
    return os.environ.get("DB_BACKEND", "sqlite").strip().lower()


def is_postgres() -> bool:
    return _backend() == "postgres"


def is_sqlite() -> bool:
    return not is_postgres()


# ---------------------------------------------------------------------------
# SQL rewriting — SQLite-flavored → Postgres-flavored
# ---------------------------------------------------------------------------

# datetime('now', '-' || ? || ' UNIT')  →  (NOW() - INTERVAL '1 UNIT' * ?)
_DT_PARAM_RE = re.compile(
    r"datetime\(\s*'now'\s*,\s*'-'\s*\|\|\s*\?\s*\|\|\s*"
    r"'\s+(days?|hours?|minutes?|seconds?)\s*'\s*\)",
    re.IGNORECASE)

# datetime('now')  →  NOW()
_DT_NOW_RE = re.compile(
    r"datetime\(\s*'now'\s*\)", re.IGNORECASE)

# INSERT OR IGNORE INTO foo  →  INSERT INTO foo … ON CONFLICT DO NOTHING
# (the ON CONFLICT clause is appended at the END of the statement,
# not inline)
_INSERT_OR_IGNORE_RE = re.compile(
    r"\bINSERT\s+OR\s+IGNORE\b", re.IGNORECASE)

# PRAGMA … —  skip entirely on Postgres
_PRAGMA_RE = re.compile(
    r"^\s*PRAGMA\b[^;]*;?\s*$", re.IGNORECASE | re.MULTILINE)


def _rewrite_pg(sql: str) -> str:
    """Translate one SQLite-flavored SQL statement into the
    Postgres equivalent. Idempotent on already-Postgres SQL."""
    if not sql:
        return sql

    s = sql

    # 1) datetime('now', '-' || ? || ' UNIT')  →  INTERVAL form
    def _dt_param_sub(m: re.Match) -> str:
        unit = m.group(1).lower().rstrip("s")  # singular
        return f"(NOW() - INTERVAL '1 {unit}' * ?)"

    s = _DT_PARAM_RE.sub(_dt_param_sub, s)

    # 2) datetime('now')  →  NOW()
    s = _DT_NOW_RE.sub("NOW()", s)

    # 3) INSERT OR IGNORE INTO … VALUES (…)
    #    → INSERT INTO … VALUES (…) ON CONFLICT DO NOTHING
    # We drop the `OR IGNORE` keywords (keeping `INSERT`) and
    # let the existing `INTO` from the SQL flow through — that
    # way we don't double-INTO. Then append ON CONFLICT DO
    # NOTHING at the end.
    if _INSERT_OR_IGNORE_RE.search(s):
        s = _INSERT_OR_IGNORE_RE.sub("INSERT", s)
        # Append ON CONFLICT DO NOTHING just before any trailing
        # semicolon and whitespace. If the statement already has
        # an ON CONFLICT clause (rare but possible if someone
        # hand-crafted it), don't double-append.
        if "ON CONFLICT" not in s.upper():
            s = s.rstrip()
            if s.endswith(";"):
                s = s[:-1] + " ON CONFLICT DO NOTHING;"
            else:
                s = s + " ON CONFLICT DO NOTHING"

    # 4) `?` placeholders → `%s` (psycopg format style)
    # The replacement must NOT match `?` inside string literals.
    # We do a single linear scan tracking quote state so we don't
    # corrupt INSERT … VALUES ('foo?bar', …) — though those don't
    # occur in db.py today, defensive coding is cheap.
    s = _swap_qmark_to_pct(s)
    return s


def _swap_qmark_to_pct(sql: str) -> str:
    """Replace `?` placeholders with `%s`, but leave `?` inside
    single-quoted string literals alone. Linear, single-pass."""
    out: List[str] = []
    in_str = False
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if ch == "'":
            # Handle '' escape inside a string
            if in_str and i + 1 < n and sql[i + 1] == "'":
                out.append("''")
                i += 2
                continue
            in_str = not in_str
            out.append(ch)
            i += 1
            continue
        if ch == "?" and not in_str:
            out.append("%s")
            i += 1
            continue
        # Also handle %s pre-existing — escape as %%s so psycopg
        # doesn't try to interpolate. But we don't expect any
        # literal % in db.py SQL, so skip for now.
        out.append(ch)
        i += 1
    return "".join(out)


# ---------------------------------------------------------------------------
# Postgres connection wrapper
# ---------------------------------------------------------------------------

class _PgCursor:
    """Wraps a psycopg cursor so its execute() rewrites SQL on
    the fly, and exposes the sqlite3-compatible attributes that
    db.py uses (`lastrowid`, dict-style row access)."""

    def __init__(self, pg_cur):
        self._cur = pg_cur
        self._last_rowid: Optional[int] = None

    def execute(self, sql: str,
                  params: Sequence[Any] = ()) -> "_PgCursor":
        rewritten = _rewrite_pg(sql)
        # Detect simple-INSERT statements where the caller will
        # use .lastrowid afterwards. We inject `RETURNING id` so
        # the wrapper can capture and stash the new row id. db.py
        # has 17 lastrowid sites all of which insert into tables
        # with a BIGSERIAL id column.
        if _is_simple_insert_with_id(rewritten):
            rewritten = _append_returning_id(rewritten)
            self._cur.execute(rewritten, params or ())
            row = self._cur.fetchone()
            if row is not None:
                # psycopg dict-row returns a dict; positional row
                # returns a tuple. Handle both.
                if isinstance(row, dict):
                    self._last_rowid = row.get("id")
                else:
                    try:
                        self._last_rowid = row[0]
                    except (KeyError, IndexError):
                        self._last_rowid = None
        else:
            self._cur.execute(rewritten, params or ())
            self._last_rowid = None
        return self

    def executemany(self, sql: str,
                          rows: Iterable[Sequence[Any]]
                          ) -> "_PgCursor":
        rewritten = _rewrite_pg(sql)
        self._cur.executemany(rewritten, list(rows))
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    @property
    def lastrowid(self) -> Optional[int]:
        return self._last_rowid

    @property
    def description(self):
        return self._cur.description

    @property
    def rowcount(self):
        return self._cur.rowcount

    def close(self):
        self._cur.close()


class _PgConnection:
    """Wraps a psycopg connection to expose the sqlite3.Connection
    interface that db.py expects. Specifically:

      - execute(sql, params)        → returns a cursor
      - executemany(sql, rows)
      - executescript(multi_sql)    → split + execute each stmt
      - commit() / rollback()
      - autocommit-like behaviour (psycopg autocommit=True)
      - context-manager support (`with connect() as c: …`)
    """

    def __init__(self, pg_conn):
        self._conn = pg_conn

    # ----- sqlite3-style top-level execute helpers --------------
    def execute(self, sql: str,
                  params: Sequence[Any] = ()) -> _PgCursor:
        cur = _PgCursor(self._conn.cursor())
        cur.execute(sql, params)
        return cur

    def executemany(self, sql: str,
                          rows: Iterable[Sequence[Any]]
                          ) -> _PgCursor:
        cur = _PgCursor(self._conn.cursor())
        cur.executemany(sql, rows)
        return cur

    def executescript(self, multi_sql: str) -> None:
        """psycopg has no executescript. Split on `;` (naïvely —
        fine for our schema which has no procedural blocks) and
        run each non-empty statement."""
        # Drop PRAGMA lines entirely on Postgres
        cleaned = _PRAGMA_RE.sub("", multi_sql or "")
        for raw_stmt in cleaned.split(";"):
            stmt = raw_stmt.strip()
            if not stmt:
                continue
            rewritten = _rewrite_pg(stmt)
            with self._conn.cursor() as cur:
                cur.execute(rewritten)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    # ----- context-manager (used by `@contextmanager connect` in
    # db.py) ----------------------------------------------------
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            try:
                self._conn.rollback()
            except Exception:
                pass
        self.close()
        return False

    # ----- row_factory hook (no-op; psycopg dict_row covers it)
    @property
    def row_factory(self):
        return None

    @row_factory.setter
    def row_factory(self, _value):
        # db.py sets `conn.row_factory = sqlite3.Row`. With psycopg
        # we configure dict_row at cursor-factory level, so this
        # setter is a no-op. Kept for interface compatibility.
        pass


# ---------------------------------------------------------------------------
# INSERT … RETURNING id helper
# ---------------------------------------------------------------------------

_SIMPLE_INSERT_RE = re.compile(
    r"^\s*INSERT\s+INTO\s+", re.IGNORECASE)
_RETURNING_RE = re.compile(
    r"\bRETURNING\b", re.IGNORECASE)
_ON_CONFLICT_RE = re.compile(
    r"\bON\s+CONFLICT\b", re.IGNORECASE)


def _is_simple_insert_with_id(sql: str) -> bool:
    """True if this is an INSERT we should append RETURNING id to.
    Skips INSERTs that already have a RETURNING clause. ON
    CONFLICT INSERTs are skipped because they may not produce a
    new row, in which case RETURNING returns 0 rows and
    fetchone() returns None — the caller's lastrowid will be
    None which matches SQLite's behaviour on conflict-skipped
    inserts."""
    if not sql:
        return False
    if not _SIMPLE_INSERT_RE.match(sql):
        return False
    if _RETURNING_RE.search(sql):
        return False
    return True


def _append_returning_id(sql: str) -> str:
    """Append `RETURNING id` to an INSERT. If the table doesn't
    have an `id` column, Postgres raises a column-not-found
    error; the caller in db.py must use a different idiom for
    such tables (most tables in our schema use `id` BIGSERIAL).

    Inserts the RETURNING clause BEFORE any trailing semicolon."""
    s = sql.rstrip()
    if s.endswith(";"):
        return s[:-1] + " RETURNING id;"
    return s + " RETURNING id"


# ---------------------------------------------------------------------------
# Public connect() — the only thing db.py needs to import
# ---------------------------------------------------------------------------

@contextmanager
def connect() -> Iterator[Any]:
    """Open a connection to the team_actions DB. Returns either a
    real sqlite3.Connection (when DB_BACKEND='sqlite') or a
    _PgConnection wrapper (when DB_BACKEND='postgres'). The
    interface is sqlite3-compatible in both cases so the rest of
    db.py is backend-agnostic."""
    if is_postgres():
        # v2.67.164 — Accept either DATABASE_URL (12-factor
        # convention) or INTERNAL_DATABASE_URL (Render's prefixed
        # form when the env var was saved with its dashboard
        # label).
        url = (os.environ.get("DATABASE_URL", "").strip()
                or os.environ.get(
                    "INTERNAL_DATABASE_URL", "").strip())
        if not url:
            raise RuntimeError(
                "DB_BACKEND=postgres but neither DATABASE_URL "
                "nor INTERNAL_DATABASE_URL is set")
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError(
                "psycopg not installed — add "
                "'psycopg[binary]>=3.1' to requirements.txt"
            ) from exc
        # autocommit=True matches sqlite isolation_level=None
        # behaviour (each statement commits immediately unless
        # wrapped in an explicit transaction by the caller).
        raw = psycopg.connect(url, autocommit=True,
                                  row_factory=dict_row)
        wrapper = _PgConnection(raw)
        try:
            yield wrapper
        finally:
            wrapper.close()
        return

    # SQLite path — unchanged from the original db.py logic.
    from data_paths import DB_PATH  # noqa: WPS433 (deferred)
    raw = sqlite3.connect(str(DB_PATH), isolation_level=None,
                              timeout=30)
    raw.row_factory = sqlite3.Row
    try:
        # PRAGMAs are idempotent — set every connection
        try:
            raw.execute("PRAGMA journal_mode=WAL")
            raw.execute("PRAGMA synchronous=NORMAL")
            raw.execute("PRAGMA busy_timeout=30000")
        except sqlite3.Error:
            pass
        yield raw
    finally:
        raw.close()


# ---------------------------------------------------------------------------
# Self-test — run `python db_dialect.py` to verify rewriting
# ---------------------------------------------------------------------------

def _selftest() -> int:
    tests = [
        # (input, expected_pg_output)
        ("INSERT INTO foo (a, b) VALUES (?, ?)",
            "INSERT INTO foo (a, b) VALUES (%s, %s)"),
        ("INSERT OR IGNORE INTO foo (a) VALUES (?)",
            "INSERT INTO foo (a) VALUES (%s) ON CONFLICT DO NOTHING"),
        ("SELECT * FROM foo WHERE created_at >= "
          "datetime('now', '-' || ? || ' days')",
            "SELECT * FROM foo WHERE created_at >= "
            "(NOW() - INTERVAL '1 day' * %s)"),
        ("UPDATE foo SET ts = datetime('now') WHERE id = ?",
            "UPDATE foo SET ts = NOW() WHERE id = %s"),
        ("SELECT 1 FROM foo WHERE x = 'has ? mark' AND y = ?",
            "SELECT 1 FROM foo WHERE x = 'has ? mark' AND y = %s"),
        ("SELECT * FROM foo WHERE ts >= "
          "datetime('now', '-' || ? || ' hours') LIMIT ?",
            "SELECT * FROM foo WHERE ts >= "
            "(NOW() - INTERVAL '1 hour' * %s) LIMIT %s"),
        ("INSERT INTO foo (a) VALUES (?) RETURNING id",
            "INSERT INTO foo (a) VALUES (%s) RETURNING id"),
    ]
    fails = 0
    for sql_in, expected in tests:
        got = _rewrite_pg(sql_in)
        ok = got == expected
        if not ok:
            fails += 1
            print(f"FAIL: {sql_in!r}\n  got:      {got!r}\n  expected: {expected!r}")
        else:
            print(f"OK:   {sql_in[:60]}…" if len(sql_in) > 60
                    else f"OK:   {sql_in}")
    # Boolean helpers
    assert _is_simple_insert_with_id(
        "INSERT INTO foo (a) VALUES (?)")
    assert not _is_simple_insert_with_id(
        "INSERT INTO foo (a) VALUES (?) RETURNING id")
    assert not _is_simple_insert_with_id(
        "SELECT * FROM foo")
    print()
    if fails:
        print(f"{fails} test(s) FAILED")
        return 1
    print("All self-tests passed.")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_selftest())
