"""
db.py — local SQLite for team actions (notes, flags, and future state)
======================================================================
All team-shared state that the web app writes (notes per SKU, flags for
review, approval history, policy overrides) lives in a single SQLite file
next to the app: team_actions.db.

Why SQLite: zero ops, single file, easy to back up, handles 10s of concurrent
Streamlit sessions on one PC comfortably. Swap for Postgres later if hosted.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

DB_PATH = Path(__file__).resolve().parent / "team_actions.db"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS notes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    sku         TEXT    NOT NULL,
    author      TEXT    NOT NULL,
    body        TEXT    NOT NULL,
    tags        TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS ix_notes_sku ON notes(sku);
CREATE INDEX IF NOT EXISTS ix_notes_created ON notes(created_at);

CREATE TABLE IF NOT EXISTS flags (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    sku         TEXT    NOT NULL,
    flag_type   TEXT    NOT NULL,
    set_by      TEXT    NOT NULL,
    set_at      TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    cleared_at  TIMESTAMP,
    cleared_by  TEXT,
    notes       TEXT
);
CREATE INDEX IF NOT EXISTS ix_flags_sku ON flags(sku);
CREATE INDEX IF NOT EXISTS ix_flags_active ON flags(cleared_at);

CREATE TABLE IF NOT EXISTS audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event       TEXT    NOT NULL,
    actor       TEXT    NOT NULL,
    target      TEXT,
    detail      TEXT,
    at          TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS ix_audit_at ON audit_log(at);

-- Migration map: retiring SKU -> successor SKU. Used when we're phasing
-- out a product line and want its historical demand rolled up under the
-- new line for forecasting.
CREATE TABLE IF NOT EXISTS sku_migrations (
    retiring_sku    TEXT    PRIMARY KEY,
    successor_sku   TEXT    NOT NULL,
    share_pct       REAL    NOT NULL DEFAULT 100.0,   -- % of demand migrating
    set_by          TEXT    NOT NULL,
    set_at          TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    note            TEXT
);

-- Supplier operational config — lead times, MOQ, MOV, freight preferences.
-- Drives the ABC / reorder math on the Ordering page.
CREATE TABLE IF NOT EXISTS supplier_config (
    supplier_name   TEXT    PRIMARY KEY,
    lead_time_sea_days  INTEGER,           -- typical sea/truck lead time
    lead_time_air_days  INTEGER,           -- NULL if air not offered
    air_eligible_default INTEGER DEFAULT 0, -- 0/1 — is air available at all
    air_max_length_mm   INTEGER,            -- NULL = any length; e.g. 2200 for Topmet UPS
    moq_units           REAL,               -- minimum qty per order
    mov_amount          REAL,               -- minimum order value
    mov_currency        TEXT,
    preferred_freight   TEXT,               -- 'sea' | 'air' | 'mixed'
    safety_pct_A        REAL DEFAULT 30.0,  -- safety factor for A-class
    safety_pct_B        REAL DEFAULT 20.0,
    safety_pct_C        REAL DEFAULT 15.0,
    review_days_A       INTEGER DEFAULT 14,
    review_days_B       INTEGER DEFAULT 30,
    review_days_C       INTEGER DEFAULT 45,
    set_by              TEXT    NOT NULL,
    set_at              TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    note                TEXT
);

-- Supplier pricing policy. Default is 'fixed_per_unit' which uses CIN7's
-- per-SKU AverageCost. Alternatives:
--   per_foot: price_per_ft × tube_length_ft (Reeves style - no color/length
--             variation; just cost per linear foot)
--   per_foot_tiered: same as per_foot but with quantity-break tiers.
--             tiers_json = [{"min_qty": 0, "price_per_ft": 2.40},
--                           {"min_qty": 100, "price_per_ft": 2.10},
--                           {"min_qty": 500, "price_per_ft": 1.85}, ...]
--             applied by PO-line qty (or aggregate supplier qty — config).
CREATE TABLE IF NOT EXISTS supplier_pricing (
    supplier_name   TEXT    PRIMARY KEY,
    pricing_model   TEXT    NOT NULL,             -- fixed_per_unit | per_foot | per_foot_tiered
    base_price      REAL,                          -- for fixed (rare) or flat per_foot price
    tiers_json      TEXT,                          -- JSON list for tiered
    tier_basis      TEXT    DEFAULT 'line_qty',    -- line_qty | supplier_total
    currency        TEXT,
    effective_from  TEXT,
    set_by          TEXT    NOT NULL,
    set_at          TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    note            TEXT
);

-- Family → default supplier assignment. Overrides the auto-inference
-- from purchase history when the 90-day PO window is thin. Example:
-- 'All SIERRA38 / SIERRA65 masters come from Reeves even if last PO was
-- 6 months ago'. Used in LED Tubes draft-PO workflow.
CREATE TABLE IF NOT EXISTS family_supplier_assignments (
    family          TEXT    PRIMARY KEY,
    supplier_name   TEXT    NOT NULL,
    set_by          TEXT    NOT NULL,
    set_at          TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    note            TEXT
);

-- Per-SKU supplier override. Rarer than family-level but sometimes needed
-- when one SKU in a family comes from a different source.
CREATE TABLE IF NOT EXISTS sku_supplier_overrides (
    sku             TEXT    PRIMARY KEY,
    supplier_name   TEXT    NOT NULL,
    set_by          TEXT    NOT NULL,
    set_at          TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    note            TEXT
);

-- Critical components per tube family. Team-designated components that
-- we want to track closely (e.g. Yukon mounting plate used across many
-- tubes and has long supplier lead time). Shown prominently on LED Tubes
-- page with consumption projections and days-of-cover.
CREATE TABLE IF NOT EXISTS family_critical_components (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    family          TEXT    NOT NULL,
    component_sku   TEXT    NOT NULL,
    role            TEXT,       -- e.g. 'Mounting plate', 'Heat plate', 'Diffuser'
    lead_time_days  INTEGER,    -- supplier lead time in days
    set_by          TEXT    NOT NULL,
    set_at          TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    note            TEXT,
    UNIQUE(family, component_sku)
);
CREATE INDEX IF NOT EXISTS ix_family_crit_family
    ON family_critical_components(family);

-- Per-SKU policy overrides. The ABC engine will compute a default target;
-- values here override it (null = use default). Set expires_at to
-- auto-revert after a date.
CREATE TABLE IF NOT EXISTS sku_policy_overrides (
    sku             TEXT    PRIMARY KEY,
    abc_class       TEXT,                       -- 'A' / 'B' / 'C' override
    target_min_units  REAL,                     -- manual min stock target
    target_max_units  REAL,                     -- manual max stock target
    target_days_of_cover REAL,                  -- alternative: days of cover
    default_freight_mode TEXT,                  -- 'sea' / 'air' / 'mixed'
    service_level_pct REAL,                     -- 0-100
    set_by          TEXT    NOT NULL,
    set_at          TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    expires_at      TIMESTAMP,
    reason          TEXT
);

-- UI preferences — per-user, per-view column layout (order + visibility).
-- Keyed by (user, view) so each teammate can have their own PO-editor
-- layout. view is a stable string like 'ordering_po_editor'.
-- columns_csv stores an ordered comma-separated list of column keys the
-- user wants visible, in the order they want to see them.
-- widths_csv stores per-column width preferences as "key=small,key=large,..."
-- Only Streamlit's preset widths ('small'/'medium'/'large') are supported.
CREATE TABLE IF NOT EXISTS ui_prefs (
    user        TEXT    NOT NULL,
    view        TEXT    NOT NULL,
    columns_csv TEXT    NOT NULL,
    widths_csv  TEXT,
    updated_at  TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (user, view)
);

-- User-named presets (snapshots). ui_prefs holds the "current live" view;
-- ui_presets holds as many named snapshots as the user wants, which appear
-- alongside the built-in presets in the Quick preset dropdown.
CREATE TABLE IF NOT EXISTS ui_presets (
    user        TEXT    NOT NULL,
    view        TEXT    NOT NULL,
    name        TEXT    NOT NULL,
    columns_csv TEXT    NOT NULL,
    widths_csv  TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (user, view, name)
);
"""


def _migrate_ui_prefs_widths(conn: sqlite3.Connection) -> None:
    """Add widths_csv to older ui_prefs tables that predate it."""
    try:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info('ui_prefs')").fetchall()}
        if "widths_csv" not in cols:
            conn.execute("ALTER TABLE ui_prefs ADD COLUMN widths_csv TEXT")
    except sqlite3.Error:
        pass


def _migrate_supplier_dropship(conn: sqlite3.Connection) -> None:
    """Add dropship_default to older supplier_config tables."""
    try:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info('supplier_config')").fetchall()}
        if "dropship_default" not in cols:
            conn.execute(
                "ALTER TABLE supplier_config ADD COLUMN "
                "dropship_default INTEGER DEFAULT 0")
    except sqlite3.Error:
        pass


def _migrate_supplier_stockout_recovery(conn: sqlite3.Connection) -> None:
    """Add stockout_min_cover_days to older supplier_config tables.
    Default 60 — how many days of stock to target after a PO arrives
    from a true stockout, on top of covering lead time."""
    try:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info('supplier_config')").fetchall()}
        if "stockout_min_cover_days" not in cols:
            conn.execute(
                "ALTER TABLE supplier_config ADD COLUMN "
                "stockout_min_cover_days INTEGER DEFAULT 60")
    except sqlite3.Error:
        pass

FLAG_TYPES = [
    "For review",
    "Reorder approved",
    "Slow mover — investigate",
    "Dead stock — liquidate",
    "Supplier issue",
    "Quality issue",
    "Pricing review",
    "Air-freight candidate",
    "Confirmed kit",           # force-include in Kit Management
    "Not actually a kit",      # force-exclude from Kit Management
    "Bought as kit",           # kit we buy whole from supplier (e.g. Topmet)
    "Built in-house",          # kit we assemble from components
    "Do not reorder",          # buyer-set exclusion — hidden from Ordering
    "Dropship",                # order-on-demand, we don't hold stock
    "Not dropship",            # override: CIN7 says dropship, user wants stocked
]


# ---------------------------------------------------------------------------
# "Do not reorder" helpers — thin wrappers around the flag machinery so the
# Ordering page doesn't have to know the flag_type string.
# ---------------------------------------------------------------------------
DNR_FLAG = "Do not reorder"


def all_do_not_reorder_skus() -> set:
    """Set of SKUs with an ACTIVE 'Do not reorder' flag.
    The Ordering page filters these out of the main reorder list."""
    with connect() as c:
        rows = c.execute(
            "SELECT DISTINCT sku FROM flags "
            "WHERE flag_type = ? AND cleared_at IS NULL",
            (DNR_FLAG,),
        ).fetchall()
    return {r["sku"] for r in rows}


def list_do_not_reorder(limit: int = 500) -> List[sqlite3.Row]:
    """Full list of active DNR flags with metadata for the reactivation
    screen (who set it, when, any reason/note)."""
    with connect() as c:
        return c.execute(
            "SELECT id, sku, set_by, set_at, notes "
            "FROM flags WHERE flag_type = ? AND cleared_at IS NULL "
            "ORDER BY set_at DESC LIMIT ?",
            (DNR_FLAG, int(limit)),
        ).fetchall()


def set_do_not_reorder(sku: str, set_by: str, reason: str = "") -> int:
    """Mark an SKU as 'Do not reorder'. No-op if already active."""
    # set_flag() creates a new row regardless, so dedupe first.
    with connect() as c:
        existing = c.execute(
            "SELECT id FROM flags WHERE sku = ? AND flag_type = ? "
            "AND cleared_at IS NULL",
            (sku, DNR_FLAG),
        ).fetchone()
        if existing:
            return int(existing["id"])
    return set_flag(sku, DNR_FLAG, set_by, reason)


def clear_do_not_reorder(sku: str, cleared_by: str) -> int:
    """Reactivate an SKU — clear any active 'Do not reorder' flag(s).
    Returns the count cleared (usually 1, occasionally 0)."""
    with connect() as c:
        rows = c.execute(
            "SELECT id FROM flags WHERE sku = ? AND flag_type = ? "
            "AND cleared_at IS NULL",
            (sku, DNR_FLAG),
        ).fetchall()
    for r in rows:
        clear_flag(int(r["id"]), cleared_by)
    return len(rows)


# ---------------------------------------------------------------------------
# Dropship helpers — same pattern as Do not reorder. A dropship item is
# one we order on demand from the supplier; we never hold stock. The
# Ordering page shows them with a badge, skips reorder math, and offers
# a "Promote to stocked" action when volume warrants it.
# ---------------------------------------------------------------------------
DROPSHIP_FLAG = "Dropship"


def all_dropship_skus() -> set:
    """Set of SKUs currently flagged as Dropship."""
    with connect() as c:
        rows = c.execute(
            "SELECT DISTINCT sku FROM flags "
            "WHERE flag_type = ? AND cleared_at IS NULL",
            (DROPSHIP_FLAG,),
        ).fetchall()
    return {r["sku"] for r in rows}


def set_dropship(sku: str, set_by: str, reason: str = "") -> int:
    with connect() as c:
        existing = c.execute(
            "SELECT id FROM flags WHERE sku = ? AND flag_type = ? "
            "AND cleared_at IS NULL",
            (sku, DROPSHIP_FLAG),
        ).fetchone()
        if existing:
            return int(existing["id"])
    return set_flag(sku, DROPSHIP_FLAG, set_by, reason)


def clear_dropship(sku: str, cleared_by: str) -> int:
    with connect() as c:
        rows = c.execute(
            "SELECT id FROM flags WHERE sku = ? AND flag_type = ? "
            "AND cleared_at IS NULL",
            (sku, DROPSHIP_FLAG),
        ).fetchall()
    for r in rows:
        clear_flag(int(r["id"]), cleared_by)
    return len(rows)


def list_dropship(limit: int = 500) -> List[sqlite3.Row]:
    with connect() as c:
        return c.execute(
            "SELECT id, sku, set_by, set_at, notes FROM flags "
            "WHERE flag_type = ? AND cleared_at IS NULL "
            "ORDER BY set_at DESC LIMIT ?",
            (DROPSHIP_FLAG, int(limit)),
        ).fetchall()


# ---------------------------------------------------------------------------
# "Not dropship" override — used when the user wants to promote a SKU that
# CIN7 currently marks as Always Drop Ship or tags Dropship. This is an
# app-side intent record; CIN7 remains untouched until the user clicks
# Write to CIN7 in the pending-writes expander.
# ---------------------------------------------------------------------------
NOT_DROPSHIP_FLAG = "Not dropship"


def all_not_dropship_skus() -> set:
    """SKUs the user has explicitly marked as 'not dropship' in the app,
    overriding CIN7. These are candidates for write-back."""
    with connect() as c:
        rows = c.execute(
            "SELECT DISTINCT sku FROM flags "
            "WHERE flag_type = ? AND cleared_at IS NULL",
            (NOT_DROPSHIP_FLAG,),
        ).fetchall()
    return {r["sku"] for r in rows}


def set_not_dropship(sku: str, set_by: str, reason: str = "") -> int:
    with connect() as c:
        existing = c.execute(
            "SELECT id FROM flags WHERE sku = ? AND flag_type = ? "
            "AND cleared_at IS NULL",
            (sku, NOT_DROPSHIP_FLAG),
        ).fetchone()
        if existing:
            return int(existing["id"])
    return set_flag(sku, NOT_DROPSHIP_FLAG, set_by, reason)


def clear_not_dropship(sku: str, cleared_by: str) -> int:
    with connect() as c:
        rows = c.execute(
            "SELECT id FROM flags WHERE sku = ? AND flag_type = ? "
            "AND cleared_at IS NULL",
            (sku, NOT_DROPSHIP_FLAG),
        ).fetchall()
    for r in rows:
        clear_flag(int(r["id"]), cleared_by)
    return len(rows)


# ---------------------------------------------------------------------------
# Latest-note-per-SKU — bulk lookup for the PO editor's Notes column so we
# don't hit SQLite once per row.
# ---------------------------------------------------------------------------

def latest_note_per_sku() -> dict:
    """Return {sku: body} for the most recent note on each SKU.
    Empty dict if no notes on record."""
    with connect() as c:
        rows = c.execute(
            """
            SELECT sku, body FROM notes
            WHERE id IN (
                SELECT MAX(id) FROM notes GROUP BY sku
            )
            """
        ).fetchall()
    return {r["sku"]: (r["body"] or "") for r in rows}


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(DB_PATH, isolation_level=None, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(_SCHEMA)
        _migrate_ui_prefs_widths(conn)
        _migrate_supplier_dropship(conn)
        _migrate_supplier_stockout_recovery(conn)
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Notes
# ---------------------------------------------------------------------------

def add_note(sku: str, author: str, body: str, tags: str = "") -> int:
    with connect() as c:
        cur = c.execute(
            "INSERT INTO notes (sku, author, body, tags) VALUES (?, ?, ?, ?)",
            (sku, author, body.strip(), tags.strip()),
        )
        nid = cur.lastrowid
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("note.add", author, sku, body[:200]),
        )
        return nid


def list_notes(sku: Optional[str] = None, limit: int = 500) -> List[sqlite3.Row]:
    with connect() as c:
        if sku:
            rows = c.execute(
                "SELECT * FROM notes WHERE sku = ? "
                "ORDER BY created_at DESC LIMIT ?",
                (sku, limit),
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM notes ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return rows


def delete_note(note_id: int, actor: str) -> None:
    with connect() as c:
        row = c.execute("SELECT sku FROM notes WHERE id = ?",
                        (note_id,)).fetchone()
        c.execute("DELETE FROM notes WHERE id = ?", (note_id,))
        if row:
            c.execute(
                "INSERT INTO audit_log (event, actor, target, detail) "
                "VALUES (?, ?, ?, ?)",
                ("note.delete", actor, row["sku"], f"note_id={note_id}"),
            )


# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------

def set_flag(sku: str, flag_type: str, set_by: str, notes: str = "") -> int:
    with connect() as c:
        # prevent duplicate active flag of same type
        existing = c.execute(
            "SELECT id FROM flags WHERE sku = ? AND flag_type = ? "
            "AND cleared_at IS NULL",
            (sku, flag_type),
        ).fetchone()
        if existing:
            return existing["id"]
        cur = c.execute(
            "INSERT INTO flags (sku, flag_type, set_by, notes) "
            "VALUES (?, ?, ?, ?)",
            (sku, flag_type, set_by, notes.strip()),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("flag.set", set_by, sku, flag_type),
        )
        return cur.lastrowid


def clear_flag(flag_id: int, cleared_by: str) -> None:
    with connect() as c:
        row = c.execute(
            "SELECT sku, flag_type FROM flags WHERE id = ?",
            (flag_id,),
        ).fetchone()
        c.execute(
            "UPDATE flags SET cleared_at = datetime('now'), cleared_by = ? "
            "WHERE id = ? AND cleared_at IS NULL",
            (cleared_by, flag_id),
        )
        if row:
            c.execute(
                "INSERT INTO audit_log (event, actor, target, detail) "
                "VALUES (?, ?, ?, ?)",
                ("flag.clear", cleared_by, row["sku"], row["flag_type"]),
            )


def list_flags(sku: Optional[str] = None, active_only: bool = True,
               limit: int = 500) -> List[sqlite3.Row]:
    with connect() as c:
        where = []
        params: list = []
        if sku:
            where.append("sku = ?")
            params.append(sku)
        if active_only:
            where.append("cleared_at IS NULL")
        sql = "SELECT * FROM flags"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY set_at DESC LIMIT ?"
        params.append(limit)
        return c.execute(sql, params).fetchall()


def flag_counts_by_sku() -> dict:
    """Return {sku: [flag_type, ...]} for active flags. Used by list pages
    to show a 🚩 indicator."""
    out: dict = {}
    with connect() as c:
        for row in c.execute(
            "SELECT sku, flag_type FROM flags WHERE cleared_at IS NULL"
        ):
            out.setdefault(row["sku"], []).append(row["flag_type"])
    return out


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

def recent_audit(limit: int = 200) -> List[sqlite3.Row]:
    with connect() as c:
        return c.execute(
            "SELECT * FROM audit_log ORDER BY at DESC LIMIT ?",
            (limit,),
        ).fetchall()


# ---------------------------------------------------------------------------
# SKU policy overrides
# ---------------------------------------------------------------------------

def set_policy_override(
    sku: str,
    set_by: str,
    *,
    abc_class: Optional[str] = None,
    target_min_units: Optional[float] = None,
    target_max_units: Optional[float] = None,
    target_days_of_cover: Optional[float] = None,
    default_freight_mode: Optional[str] = None,
    service_level_pct: Optional[float] = None,
    expires_at: Optional[str] = None,
    reason: Optional[str] = None,
) -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO sku_policy_overrides
                (sku, abc_class, target_min_units, target_max_units,
                 target_days_of_cover, default_freight_mode,
                 service_level_pct, set_by, expires_at, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sku) DO UPDATE SET
                abc_class = excluded.abc_class,
                target_min_units = excluded.target_min_units,
                target_max_units = excluded.target_max_units,
                target_days_of_cover = excluded.target_days_of_cover,
                default_freight_mode = excluded.default_freight_mode,
                service_level_pct = excluded.service_level_pct,
                set_by = excluded.set_by,
                set_at = datetime('now'),
                expires_at = excluded.expires_at,
                reason = excluded.reason
            """,
            (sku, abc_class, target_min_units, target_max_units,
             target_days_of_cover, default_freight_mode, service_level_pct,
             set_by, expires_at, reason),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("policy.set", set_by, sku,
             f"class={abc_class} min={target_min_units} max={target_max_units} "
             f"dos={target_days_of_cover} mode={default_freight_mode} "
             f"sl={service_level_pct}"),
        )


def get_policy_override(sku: str) -> Optional[sqlite3.Row]:
    with connect() as c:
        return c.execute(
            "SELECT * FROM sku_policy_overrides WHERE sku = ?",
            (sku,),
        ).fetchone()


def all_policy_overrides() -> List[sqlite3.Row]:
    with connect() as c:
        return c.execute(
            "SELECT * FROM sku_policy_overrides ORDER BY set_at DESC"
        ).fetchall()


def clear_policy_override(sku: str, actor: str) -> None:
    with connect() as c:
        c.execute("DELETE FROM sku_policy_overrides WHERE sku = ?", (sku,))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("policy.clear", actor, sku, ""),
        )


# ---------------------------------------------------------------------------
# SKU migrations (retiring -> successor)
# ---------------------------------------------------------------------------

def set_migration(retiring_sku: str, successor_sku: str,
                  actor: str, share_pct: float = 100.0,
                  note: str = "") -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO sku_migrations
                (retiring_sku, successor_sku, share_pct, set_by, note)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(retiring_sku) DO UPDATE SET
                successor_sku = excluded.successor_sku,
                share_pct = excluded.share_pct,
                set_by = excluded.set_by,
                set_at = datetime('now'),
                note = excluded.note
            """,
            (retiring_sku, successor_sku, share_pct, actor, note),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("migration.set", actor, retiring_sku,
             f"-> {successor_sku} @ {share_pct}%"),
        )


def all_migrations() -> List[sqlite3.Row]:
    with connect() as c:
        return c.execute(
            "SELECT * FROM sku_migrations ORDER BY retiring_sku"
        ).fetchall()


def clear_migration(retiring_sku: str, actor: str) -> None:
    with connect() as c:
        c.execute("DELETE FROM sku_migrations WHERE retiring_sku = ?",
                  (retiring_sku,))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("migration.clear", actor, retiring_sku, ""),
        )


# ---------------------------------------------------------------------------
# Family critical components
# ---------------------------------------------------------------------------

def add_critical_component(family: str, component_sku: str, actor: str,
                           role: str = "", lead_time_days: Optional[int] = None,
                           note: str = "") -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO family_critical_components
                (family, component_sku, role, lead_time_days, set_by, note)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(family, component_sku) DO UPDATE SET
                role = excluded.role,
                lead_time_days = excluded.lead_time_days,
                set_by = excluded.set_by,
                set_at = datetime('now'),
                note = excluded.note
            """,
            (family, component_sku, role, lead_time_days, actor, note),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("critical.add", actor, f"{family}/{component_sku}",
             f"role={role} lt={lead_time_days}d"),
        )


def list_critical_components(family: Optional[str] = None) -> List[sqlite3.Row]:
    with connect() as c:
        if family:
            return c.execute(
                "SELECT * FROM family_critical_components WHERE family = ? "
                "ORDER BY set_at DESC", (family,),
            ).fetchall()
        return c.execute(
            "SELECT * FROM family_critical_components ORDER BY family, component_sku"
        ).fetchall()


def set_supplier_config(
    supplier_name: str,
    *,
    lead_time_sea_days: Optional[int] = None,
    lead_time_air_days: Optional[int] = None,
    air_eligible_default: int = 0,
    air_max_length_mm: Optional[int] = None,
    moq_units: Optional[float] = None,
    mov_amount: Optional[float] = None,
    mov_currency: Optional[str] = None,
    preferred_freight: Optional[str] = None,
    safety_pct_A: float = 30.0,
    safety_pct_B: float = 20.0,
    safety_pct_C: float = 15.0,
    review_days_A: int = 14,
    review_days_B: int = 30,
    review_days_C: int = 45,
    dropship_default: int = 0,
    stockout_min_cover_days: int = 60,
    actor: str,
    note: str = "",
) -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO supplier_config
                (supplier_name, lead_time_sea_days, lead_time_air_days,
                 air_eligible_default, air_max_length_mm,
                 moq_units, mov_amount, mov_currency,
                 preferred_freight,
                 safety_pct_A, safety_pct_B, safety_pct_C,
                 review_days_A, review_days_B, review_days_C,
                 dropship_default, stockout_min_cover_days,
                 set_by, note)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(supplier_name) DO UPDATE SET
                lead_time_sea_days = excluded.lead_time_sea_days,
                lead_time_air_days = excluded.lead_time_air_days,
                air_eligible_default = excluded.air_eligible_default,
                air_max_length_mm = excluded.air_max_length_mm,
                moq_units = excluded.moq_units,
                mov_amount = excluded.mov_amount,
                mov_currency = excluded.mov_currency,
                preferred_freight = excluded.preferred_freight,
                safety_pct_A = excluded.safety_pct_A,
                safety_pct_B = excluded.safety_pct_B,
                safety_pct_C = excluded.safety_pct_C,
                review_days_A = excluded.review_days_A,
                review_days_B = excluded.review_days_B,
                review_days_C = excluded.review_days_C,
                dropship_default = excluded.dropship_default,
                stockout_min_cover_days = excluded.stockout_min_cover_days,
                set_by = excluded.set_by,
                set_at = datetime('now'),
                note = excluded.note
            """,
            (supplier_name, lead_time_sea_days, lead_time_air_days,
             int(bool(air_eligible_default)), air_max_length_mm,
             moq_units, mov_amount, mov_currency, preferred_freight,
             safety_pct_A, safety_pct_B, safety_pct_C,
             review_days_A, review_days_B, review_days_C,
             int(bool(dropship_default)),
             int(stockout_min_cover_days),
             actor, note),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("supplier_config.set", actor, supplier_name,
             f"sea={lead_time_sea_days}d air={lead_time_air_days}d "
             f"air_max_len={air_max_length_mm}mm moq={moq_units} "
             f"mov={mov_amount}"),
        )


def all_supplier_configs() -> dict:
    """Return {supplier_name: row_as_dict}."""
    with connect() as c:
        rows = c.execute("SELECT * FROM supplier_config").fetchall()
    return {r["supplier_name"]: dict(r) for r in rows}


def set_supplier_pricing(
    supplier_name: str,
    pricing_model: str,                 # fixed_per_unit | per_foot | per_foot_tiered
    *,
    base_price: Optional[float] = None,
    tiers_json: Optional[str] = None,
    tier_basis: str = "line_qty",
    currency: Optional[str] = None,
    effective_from: Optional[str] = None,
    actor: str,
    note: str = "",
) -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO supplier_pricing
                (supplier_name, pricing_model, base_price, tiers_json,
                 tier_basis, currency, effective_from, set_by, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(supplier_name) DO UPDATE SET
                pricing_model = excluded.pricing_model,
                base_price = excluded.base_price,
                tiers_json = excluded.tiers_json,
                tier_basis = excluded.tier_basis,
                currency = excluded.currency,
                effective_from = excluded.effective_from,
                set_by = excluded.set_by,
                set_at = datetime('now'),
                note = excluded.note
            """,
            (supplier_name, pricing_model, base_price, tiers_json,
             tier_basis, currency, effective_from, actor, note),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("supplier_pricing.set", actor, supplier_name,
             f"model={pricing_model} base={base_price}"),
        )


def all_supplier_pricing() -> dict:
    """Return {supplier_name: row_as_dict}."""
    with connect() as c:
        rows = c.execute("SELECT * FROM supplier_pricing").fetchall()
    return {r["supplier_name"]: dict(r) for r in rows}


def set_family_supplier(family: str, supplier_name: str, actor: str,
                         note: str = "") -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO family_supplier_assignments
                (family, supplier_name, set_by, note)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(family) DO UPDATE SET
                supplier_name = excluded.supplier_name,
                set_by = excluded.set_by,
                set_at = datetime('now'),
                note = excluded.note
            """,
            (family, supplier_name, actor, note),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("family_supplier.set", actor, family, supplier_name),
        )


def all_family_suppliers() -> List[sqlite3.Row]:
    with connect() as c:
        return c.execute(
            "SELECT * FROM family_supplier_assignments ORDER BY family"
        ).fetchall()


def clear_family_supplier(family: str, actor: str) -> None:
    with connect() as c:
        c.execute("DELETE FROM family_supplier_assignments WHERE family = ?",
                  (family,))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("family_supplier.clear", actor, family, ""),
        )


def set_sku_supplier(sku: str, supplier_name: str, actor: str,
                      note: str = "") -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO sku_supplier_overrides
                (sku, supplier_name, set_by, note)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(sku) DO UPDATE SET
                supplier_name = excluded.supplier_name,
                set_by = excluded.set_by,
                set_at = datetime('now'),
                note = excluded.note
            """,
            (sku, supplier_name, actor, note),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("sku_supplier.set", actor, sku, supplier_name),
        )


def all_sku_supplier_overrides() -> dict:
    """Return {sku: supplier_name} for all SKU-level overrides."""
    with connect() as c:
        rows = c.execute(
            "SELECT sku, supplier_name FROM sku_supplier_overrides"
        ).fetchall()
    return {r["sku"]: r["supplier_name"] for r in rows}


def clear_critical_component(cid: int, actor: str) -> None:
    with connect() as c:
        row = c.execute(
            "SELECT family, component_sku FROM family_critical_components "
            "WHERE id = ?", (cid,)).fetchone()
        c.execute("DELETE FROM family_critical_components WHERE id = ?", (cid,))
        if row:
            c.execute(
                "INSERT INTO audit_log (event, actor, target, detail) "
                "VALUES (?, ?, ?, ?)",
                ("critical.clear", actor,
                 f"{row['family']}/{row['component_sku']}", ""),
            )


# ---------------------------------------------------------------------------
# UI preferences — per-user, per-view column layout
# ---------------------------------------------------------------------------

def get_column_layout(user: str, view: str) -> Optional[List[str]]:
    """Return the saved column order for (user, view), or None if unset.

    Result is a list of column-key strings in the order the user wants them
    rendered. Columns not present in the list should be hidden by the caller.
    """
    user = (user or "").strip().lower() or "default"
    with connect() as c:
        row = c.execute(
            "SELECT columns_csv FROM ui_prefs WHERE user = ? AND view = ?",
            (user, view),
        ).fetchone()
    if not row or not row["columns_csv"]:
        return None
    cols = [c.strip() for c in row["columns_csv"].split(",") if c.strip()]
    return cols or None


def save_column_layout(user: str, view: str, columns: List[str]) -> None:
    """Save an ordered list of visible columns for (user, view).
    Preserves any existing widths_csv (width prefs live separately)."""
    user = (user or "").strip().lower() or "default"
    csv = ",".join(c.strip() for c in columns if c and c.strip())
    with connect() as c:
        c.execute(
            """
            INSERT INTO ui_prefs (user, view, columns_csv, updated_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(user, view) DO UPDATE SET
              columns_csv = excluded.columns_csv,
              updated_at  = datetime('now')
            """,
            (user, view, csv),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("ui_prefs.save", user, view, csv),
        )


# Width presets for PO editor columns. 'small', 'medium', 'large' are
# Streamlit's native presets; 'tiny' and 'huge' are extras we map to
# specific pixel widths at render time (requires Streamlit >=1.40).
# Kept single-sourced here so the UI and save path agree on what's valid.
VALID_WIDTHS = ("tiny", "small", "medium", "large", "huge")


def get_column_widths(user: str, view: str) -> dict:
    """Return {column_key: 'small'|'medium'|'large'} for the user's view,
    or {} if nothing saved."""
    user = (user or "").strip().lower() or "default"
    with connect() as c:
        row = c.execute(
            "SELECT widths_csv FROM ui_prefs WHERE user = ? AND view = ?",
            (user, view),
        ).fetchone()
    if not row or not row["widths_csv"]:
        return {}
    out: dict = {}
    for pair in row["widths_csv"].split(","):
        if "=" not in pair:
            continue
        k, v = pair.split("=", 1)
        k, v = k.strip(), v.strip().lower()
        if k and v in VALID_WIDTHS:
            out[k] = v
    return out


def save_column_widths(user: str, view: str, widths: dict) -> None:
    """Save per-column width presets for (user, view). Accepts a dict of
    {column_key: 'small'|'medium'|'large'}; entries with other values
    are silently dropped."""
    user = (user or "").strip().lower() or "default"
    pairs = []
    for k, v in (widths or {}).items():
        v = str(v or "").strip().lower()
        if k and v in VALID_WIDTHS:
            pairs.append(f"{k}={v}")
    csv = ",".join(pairs)
    with connect() as c:
        # Ensure row exists first; keep columns_csv untouched if it's already set
        existing = c.execute(
            "SELECT columns_csv FROM ui_prefs WHERE user = ? AND view = ?",
            (user, view),
        ).fetchone()
        cols_val = existing["columns_csv"] if existing else ""
        c.execute(
            """
            INSERT INTO ui_prefs (user, view, columns_csv, widths_csv, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(user, view) DO UPDATE SET
              widths_csv = excluded.widths_csv,
              updated_at = datetime('now')
            """,
            (user, view, cols_val, csv),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("ui_prefs.save_widths", user, view, csv),
        )


def reset_column_layout(user: str, view: str) -> None:
    """Forget the saved layout — next load will use app default."""
    user = (user or "").strip().lower() or "default"
    with connect() as c:
        c.execute(
            "DELETE FROM ui_prefs WHERE user = ? AND view = ?",
            (user, view),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("ui_prefs.reset", user, view, ""),
        )


# ---------------------------------------------------------------------------
# User-named column presets (snapshots of layout + widths)
# ---------------------------------------------------------------------------

def save_user_preset(user: str, view: str, name: str,
                     columns: List[str], widths: dict) -> None:
    """Save/overwrite a user-named preset for (user, view, name).
    `columns` is the ordered visible-column list. `widths` is a
    {col_key: 'small'|'medium'|'large'} dict."""
    user = (user or "").strip().lower() or "default"
    name = (name or "").strip()
    if not name:
        raise ValueError("preset name is required")
    cols_csv = ",".join(c.strip() for c in columns if c and c.strip())
    pairs = []
    for k, v in (widths or {}).items():
        v = str(v or "").strip().lower()
        if k and v in VALID_WIDTHS:
            pairs.append(f"{k}={v}")
    widths_csv = ",".join(pairs)
    with connect() as c:
        c.execute(
            """
            INSERT INTO ui_presets (user, view, name, columns_csv,
                                     widths_csv, created_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(user, view, name) DO UPDATE SET
              columns_csv = excluded.columns_csv,
              widths_csv  = excluded.widths_csv,
              created_at  = datetime('now')
            """,
            (user, view, name, cols_csv, widths_csv),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("ui_presets.save", user, f"{view}:{name}", cols_csv),
        )


def list_user_presets(user: str, view: str) -> List[dict]:
    """Return [{name, columns, widths, created_at}, …] for this user
    in this view, newest first."""
    user = (user or "").strip().lower() or "default"
    out: List[dict] = []
    with connect() as c:
        rows = c.execute(
            "SELECT name, columns_csv, widths_csv, created_at "
            "FROM ui_presets WHERE user = ? AND view = ? "
            "ORDER BY created_at DESC",
            (user, view),
        ).fetchall()
    for r in rows:
        cols = [c.strip() for c in (r["columns_csv"] or "").split(",")
                if c.strip()]
        widths = {}
        for pair in (r["widths_csv"] or "").split(","):
            if "=" not in pair:
                continue
            k, v = pair.split("=", 1)
            if k.strip() and v.strip().lower() in VALID_WIDTHS:
                widths[k.strip()] = v.strip().lower()
        out.append({
            "name": r["name"],
            "columns": cols,
            "widths": widths,
            "created_at": r["created_at"],
        })
    return out


def load_user_preset(user: str, view: str,
                     name: str) -> Optional[dict]:
    """Look up a specific preset by name. Returns None if not found."""
    for p in list_user_presets(user, view):
        if p["name"] == name:
            return p
    return None


def delete_user_preset(user: str, view: str, name: str) -> None:
    user = (user or "").strip().lower() or "default"
    with connect() as c:
        c.execute(
            "DELETE FROM ui_presets "
            "WHERE user = ? AND view = ? AND name = ?",
            (user, view, name),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("ui_presets.delete", user, f"{view}:{name}", ""),
        )
