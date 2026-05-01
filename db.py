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

# DB_PATH lives in DATA_DIR so the SQLite file follows the persistent
# disk on Render. data_paths.py defaults to the project folder locally.
from data_paths import DB_PATH  # noqa: E402


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

-- =========================================================================
-- Supplier Pricing — family-color tier model (Reeves-style)
-- =========================================================================
-- Some suppliers price by FAMILY (e.g., 'SIERRA38'), with quantity-break
-- tiers, where TOTAL footage across multiple colors qualifies for the
-- tier but each color is priced at its own per-tier rate. Plus a setup
-- fee triggers when a single PO contains more than one color.
--
-- Three coordinated tables capture the full model:
--   family_color_pricing       — the per-color tier price table
--   family_setup_fees          — setup / changeover fees
--   family_pricing_rules       — how tier qualification rolls up

-- One row per (family, color, supplier, tier_qty) — the per-color price
-- at each volume tier. tier_qty is the MINIMUM total qty (per the
-- aggregation rule below) at which this row's unit_price applies.
CREATE TABLE IF NOT EXISTS family_color_pricing (
    family          TEXT    NOT NULL,    -- 'SIERRA38'
    color           TEXT    NOT NULL,    -- 'White' | 'Black' | normalised label
    supplier        TEXT    NOT NULL,    -- 'Reeves'
    tier_qty        REAL    NOT NULL,    -- minimum qty triggering this tier
    unit_price      REAL    NOT NULL,    -- per-foot (or per-unit) price
    unit            TEXT    DEFAULT 'ft',-- 'ft' | 'unit' — what the qty/price is in
    currency        TEXT    DEFAULT 'USD',
    set_by          TEXT    NOT NULL,
    set_at          TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    note            TEXT,
    PRIMARY KEY (family, color, supplier, tier_qty)
);
CREATE INDEX IF NOT EXISTS ix_family_color_pricing_family
    ON family_color_pricing(family, supplier);

-- Setup / changeover fees that fire under specific PO-mix conditions.
-- fee_type is a free string today ('color_change' is the main one for
-- Reeves) but we leave it open for 'tooling_change', 'minimum_runtime'
-- etc. as more suppliers come online.
CREATE TABLE IF NOT EXISTS family_setup_fees (
    family          TEXT    NOT NULL,
    supplier        TEXT    NOT NULL,
    fee_type        TEXT    NOT NULL,    -- 'color_change' | future types
    fee_amount      REAL    NOT NULL,
    currency        TEXT    DEFAULT 'USD',
    description     TEXT,                 -- human-readable
    set_by          TEXT    NOT NULL,
    set_at          TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (family, supplier, fee_type)
);

-- How tier qualification rolls up. Two values today:
--   'sum_across_colors' — sum demand across all colors of the family
--                          (Reeves SIERRA38: White + Black qty combined
--                          qualifies the tier; color change fee applies)
--   'per_color'         — each color metered separately (default for
--                          most suppliers; no color-change fee)
-- Future rules can extend this enum.
CREATE TABLE IF NOT EXISTS family_pricing_rules (
    family          TEXT    NOT NULL,
    supplier        TEXT    NOT NULL,
    rule            TEXT    NOT NULL DEFAULT 'per_color',
    nag_threshold_savings  REAL DEFAULT 200.0,   -- $ above which buyer gets nudged
    nag_threshold_pct      REAL DEFAULT 25.0,    -- % of tier-gap to fire nudge
    auto_pad_threshold_savings REAL,             -- NULL = ask, set = auto-pad
    set_by          TEXT    NOT NULL,
    set_at          TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    note            TEXT,
    PRIMARY KEY (family, supplier)
);

-- Per-SKU pack quantity (e.g., MMA-M155-25A-M comes in packs of 10).
-- Reorder qty rounds UP to nearest multiple. Independent of family
-- pricing — a SKU may have a pack qty without being in any tier scheme.
CREATE TABLE IF NOT EXISTS sku_pack_settings (
    sku             TEXT    PRIMARY KEY,
    pack_qty        REAL    NOT NULL,
    moq             REAL,                 -- minimum order qty (overrides supplier default)
    note            TEXT,
    set_by          TEXT    NOT NULL,
    set_at          TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);

-- =========================================================================
-- PO draft persistence (legacy v1 — single anonymous draft per supplier)
-- =========================================================================
-- Kept for backward compatibility. Superseded by po_drafts + po_draft_lines
-- below which support multi-draft per supplier with explicit lifecycle
-- (editing → submitted → finalized) and pessimistic locking for multi-user.
CREATE TABLE IF NOT EXISTS po_draft_edits (
    supplier        TEXT    NOT NULL,
    sku             TEXT    NOT NULL,
    edited_qty      REAL    NOT NULL,
    edited_at       TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    set_by          TEXT,
    note            TEXT,
    PRIMARY KEY (supplier, sku)
);
CREATE INDEX IF NOT EXISTS ix_po_draft_supplier
    ON po_draft_edits(supplier);

-- =========================================================================
-- PO drafts v2 — multi-draft per supplier with status lifecycle
-- =========================================================================
-- Buyer workflow:
--   1. Create draft (status=editing) — only one buyer at a time can edit
--      via the locked_by column (pessimistic lock with 30-min auto-timeout)
--   2. Edit lines in po_draft_lines — qty changes saved to DB durably
--   3. Push to CIN7 → creates a Draft PO via API, captures cin7_po_number
--      and cin7_po_id, transitions our draft to status=submitted (locked
--      from further edits in our app — buyer goes into CIN7 to finalize)
--   4. Auto-finalize — when CIN7's PO status flips to ORDERED, sync
--      detects and transitions our draft to status=finalized (archived)
--
-- Why multi-draft per supplier: a buyer often wants to push two POs
-- simultaneously to the same supplier — e.g., one sea-freight bulk PO
-- and an urgent air-freight PO. Each is a separate draft with its own
-- name and freight_mode tag.
CREATE TABLE IF NOT EXISTS po_drafts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier        TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    freight_mode    TEXT,                  -- 'sea' | 'air' | 'mixed' | NULL
    status          TEXT    NOT NULL DEFAULT 'editing',
                                            -- editing | submitted | finalized | cancelled
    cin7_po_number  TEXT,                  -- assigned on submit
    cin7_po_id      TEXT,                  -- CIN7 internal UUID
    cin7_po_status  TEXT,                  -- last-seen CIN7 status
    -- Pessimistic lock — only the locker can write to draft lines.
    -- Auto-released after 30 minutes of inactivity (cleared on read
    -- if locked_at is older than threshold).
    locked_by       TEXT,
    locked_at       TIMESTAMP,
    -- Lifecycle
    created_at      TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    created_by      TEXT,
    submitted_at    TIMESTAMP,
    submitted_by    TEXT,
    finalized_at    TIMESTAMP,
    note            TEXT
);
CREATE INDEX IF NOT EXISTS ix_po_drafts_supplier
    ON po_drafts(supplier);
CREATE INDEX IF NOT EXISTS ix_po_drafts_status
    ON po_drafts(status);

CREATE TABLE IF NOT EXISTS po_draft_lines (
    draft_id        INTEGER NOT NULL,
    sku             TEXT    NOT NULL,
    edited_qty      REAL    NOT NULL,
    last_edited_by  TEXT,
    last_edited_at  TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    note            TEXT,
    PRIMARY KEY (draft_id, sku),
    FOREIGN KEY (draft_id) REFERENCES po_drafts(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ix_po_draft_lines_draft
    ON po_draft_lines(draft_id);

-- AI Q&A audit log. Every question the AI Assistant page processes
-- gets a row here: prompt, what tools it called, what it answered,
-- how confident it was, and any thumbs-up/down feedback the user gave.
-- This is the foundation for the "feedback loop" — over time we mine
-- the negatively-rated rows to refine prompts/tools/aliases.
CREATE TABLE IF NOT EXISTS ai_audit_logs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         TEXT,
    user_question   TEXT NOT NULL,
    parsed_intent   TEXT,                  -- short summary of what AI thought
    tools_called_json TEXT,                -- JSON list of {tool, args, result_summary}
    answer_returned TEXT,
    confidence_score REAL,                 -- 0.0-1.0; AI's self-assessed
    feedback        TEXT,                  -- 'positive' | 'negative' | NULL
    feedback_note   TEXT,
    duration_ms     INTEGER,
    model_used      TEXT,                  -- e.g. 'claude-sonnet-4-6'
    created_at      TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS ix_ai_audit_user
    ON ai_audit_logs(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_ai_audit_feedback
    ON ai_audit_logs(feedback);

-- Product alias learning table. When the AI matches a fuzzy phrase
-- ("warm strip", "black shallow channel") to a SKU or product family,
-- the resolution gets recorded here. Future questions with the same
-- phrase can short-circuit the LLM call and use this mapping directly.
-- approved_by lets us distinguish AI-guessed aliases from human-confirmed.
CREATE TABLE IF NOT EXISTS product_aliases (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    phrase          TEXT NOT NULL,         -- normalized lowercase
    sku             TEXT,                  -- exact SKU match (NULL if family-level)
    product_family  TEXT,                  -- e.g. 'SIERRA38'
    confidence      REAL,                  -- 0.0-1.0
    approved_by     TEXT,                  -- 'ai' or username
    times_used      INTEGER DEFAULT 1,
    created_at      TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    last_used_at    TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS ix_product_aliases_phrase
    ON product_aliases(phrase);

-- Generic feedback events. Designed for the long-term commercial-
-- intelligence vision: feedback comes from many sources (AI chats,
-- Slack reactions, Gorgias resolutions, buyer dashboard clicks,
-- weekly buyer summary emails). Source identifies origin; entity_type
-- + entity_id point at what's being rated (an audit log row, a
-- demand signal, a buyer warning, a SKU, a product family). Later
-- pipelines (alias learning, prompt tuning, demand-signal scoring)
-- mine this table.
CREATE TABLE IF NOT EXISTS feedback_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,         -- 'ai_chat' | 'slack' | 'gorgias' | 'buyer_dashboard' | 'email' | 'manual'
    entity_type     TEXT NOT NULL,         -- 'ai_audit_log' | 'demand_signal' | 'buyer_warning' | 'sku' | 'product_family' | etc
    entity_id       TEXT NOT NULL,         -- ID of the entity (string for flexibility)
    feedback        TEXT NOT NULL,         -- 'positive' | 'negative' | 'correction' | 'ignore' | 'useful' | etc
    note            TEXT,                  -- free-text; e.g. corrected SKU, reason for negative
    user_id         TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS ix_feedback_entity
    ON feedback_events(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS ix_feedback_source
    ON feedback_events(source, created_at DESC);

-- Demand signals — the heart of the proactive intelligence layer.
-- Captures every "someone is interested in this product" moment from
-- whatever source. Phase 1 (manual entry) only uses source='manual'.
-- Slack / Gorgias / SEO / Shopify integrations later add more values
-- to the source + signal_type columns without schema changes.
--
-- Why this matters: without a signal table, Slack messages and
-- customer chats are noise. With this table, every conversation
-- becomes a row the buyer can act on.
--
-- Reference: docs/demand-scoring.md (designed at end of build day).
CREATE TABLE IF NOT EXISTS demand_signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,

    -- WHERE the signal came from. Open-ended string so future sources
    -- (slack, gorgias, seo, shopify_search etc) plug in without a
    -- schema migration.
    source          TEXT    NOT NULL,        -- 'manual' | 'slack' | 'gorgias' | 'seo' | 'shopify_search' | 'shopify_abandoned' | 'web_form' | 'phone'
    source_ref      TEXT,                    -- e.g. Slack message URL, Gorgias ticket #, page URL

    -- WHAT the signal is about. SKU preferred; product_family is the
    -- fallback when we know the product line but not the variant.
    sku             TEXT,
    product_family  TEXT,
    raw_text        TEXT,                    -- original phrasing — preserved
                                              -- so future AI re-parsing can
                                              -- improve on initial extraction

    -- TYPE of signal — open-ended for the same reason as source.
    signal_type     TEXT    NOT NULL,        -- 'inquiry' | 'quote' | 'sold' | 'lost' | 'substitute_offered' | 'cancelled' | 'returned' | 'complaint' | 'seo_rank' | 'search_query' | 'abandoned_cart' | 'notify_me'

    quantity        REAL,                    -- units mentioned (NULL if unknown)

    -- WHO
    customer_id     TEXT,                    -- CIN7 customer ID if known
    customer_name   TEXT,                    -- free-text customer reference
    salesperson     TEXT,                    -- who logged or owns the signal

    -- HOW CONFIDENT we are about the parsed values. Manual entries
    -- default to 1.0 (the human said it). AI extractions later might
    -- start at 0.6 and need_review until corrected.
    confidence      REAL    DEFAULT 1.0,
    needs_review    INTEGER DEFAULT 0,       -- 1/0 boolean

    -- LIFECYCLE — set by buyer/sales after the fact when known
    outcome         TEXT,                    -- 'open' | 'converted' | 'lost' | 'duplicate' | 'invalid'
    note            TEXT,                    -- buyer/sales notes on this signal

    -- AUDIT
    created_at      TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    created_by      TEXT,
    updated_at      TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    updated_by      TEXT
);
CREATE INDEX IF NOT EXISTS ix_demand_signals_sku
    ON demand_signals(sku, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_demand_signals_family
    ON demand_signals(product_family, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_demand_signals_created_at
    ON demand_signals(created_at DESC);
CREATE INDEX IF NOT EXISTS ix_demand_signals_signal_type
    ON demand_signals(signal_type, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_demand_signals_source
    ON demand_signals(source, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_demand_signals_outcome
    ON demand_signals(outcome);
CREATE INDEX IF NOT EXISTS ix_demand_signals_review
    ON demand_signals(needs_review)
    WHERE needs_review = 1;
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


# =====================================================================
# Family-color tier pricing (Reeves-style)
# =====================================================================

def set_family_color_pricing(
    family: str,
    color: str,
    supplier: str,
    tier_qty: float,
    unit_price: float,
    actor: str,
    unit: str = "ft",
    currency: str = "USD",
    note: str = "",
) -> None:
    """Upsert one row of the family-color tier table."""
    with connect() as c:
        c.execute(
            """
            INSERT INTO family_color_pricing
                (family, color, supplier, tier_qty, unit_price,
                 unit, currency, set_by, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(family, color, supplier, tier_qty) DO UPDATE SET
                unit_price = excluded.unit_price,
                unit = excluded.unit,
                currency = excluded.currency,
                set_by = excluded.set_by,
                set_at = datetime('now'),
                note = excluded.note
            """,
            (family, color, supplier, tier_qty, unit_price,
             unit, currency, actor, note),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("family_pricing.set", actor,
             f"{family}/{color}/{supplier}@{tier_qty}",
             f"price={unit_price} {currency}/{unit}"),
        )


def delete_family_color_pricing(family: str, color: str, supplier: str,
                                  tier_qty: float, actor: str) -> None:
    with connect() as c:
        c.execute(
            "DELETE FROM family_color_pricing "
            "WHERE family = ? AND color = ? AND supplier = ? AND tier_qty = ?",
            (family, color, supplier, tier_qty),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("family_pricing.delete", actor,
             f"{family}/{color}/{supplier}@{tier_qty}", ""),
        )


def all_family_color_pricing(
    family: Optional[str] = None,
    supplier: Optional[str] = None,
) -> List[sqlite3.Row]:
    """Returns all tier rows. Filtered by family and/or supplier if given.
    Sorted by family, supplier, color, tier_qty (ascending)."""
    sql = "SELECT * FROM family_color_pricing"
    where, args = [], []
    if family:
        where.append("family = ?")
        args.append(family)
    if supplier:
        where.append("supplier = ?")
        args.append(supplier)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY family, supplier, color, tier_qty"
    with connect() as c:
        return c.execute(sql, args).fetchall()


def family_pricing_families() -> List[str]:
    """Distinct families that have any pricing rows configured."""
    with connect() as c:
        rows = c.execute(
            "SELECT DISTINCT family FROM family_color_pricing "
            "ORDER BY family").fetchall()
        return [r["family"] for r in rows]


def set_family_setup_fee(family: str, supplier: str, fee_type: str,
                          fee_amount: float, actor: str,
                          currency: str = "USD",
                          description: str = "") -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO family_setup_fees
                (family, supplier, fee_type, fee_amount,
                 currency, description, set_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(family, supplier, fee_type) DO UPDATE SET
                fee_amount = excluded.fee_amount,
                currency = excluded.currency,
                description = excluded.description,
                set_by = excluded.set_by,
                set_at = datetime('now')
            """,
            (family, supplier, fee_type, fee_amount,
             currency, description, actor),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("family_setup_fee.set", actor,
             f"{family}/{supplier}/{fee_type}",
             f"{fee_amount} {currency}"),
        )


def all_family_setup_fees(
    family: Optional[str] = None,
) -> List[sqlite3.Row]:
    sql = "SELECT * FROM family_setup_fees"
    args = []
    if family:
        sql += " WHERE family = ?"
        args.append(family)
    sql += " ORDER BY family, supplier, fee_type"
    with connect() as c:
        return c.execute(sql, args).fetchall()


def delete_family_setup_fee(family: str, supplier: str,
                              fee_type: str, actor: str) -> None:
    with connect() as c:
        c.execute(
            "DELETE FROM family_setup_fees "
            "WHERE family = ? AND supplier = ? AND fee_type = ?",
            (family, supplier, fee_type),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("family_setup_fee.delete", actor,
             f"{family}/{supplier}/{fee_type}", ""),
        )


def set_family_pricing_rule(
    family: str, supplier: str, rule: str, actor: str,
    nag_threshold_savings: float = 200.0,
    nag_threshold_pct: float = 25.0,
    auto_pad_threshold_savings: Optional[float] = None,
    note: str = "",
) -> None:
    if rule not in ("per_color", "sum_across_colors"):
        raise ValueError(
            f"rule must be 'per_color' or 'sum_across_colors', got: {rule}")
    with connect() as c:
        c.execute(
            """
            INSERT INTO family_pricing_rules
                (family, supplier, rule, nag_threshold_savings,
                 nag_threshold_pct, auto_pad_threshold_savings,
                 set_by, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(family, supplier) DO UPDATE SET
                rule = excluded.rule,
                nag_threshold_savings = excluded.nag_threshold_savings,
                nag_threshold_pct = excluded.nag_threshold_pct,
                auto_pad_threshold_savings = excluded.auto_pad_threshold_savings,
                set_by = excluded.set_by,
                set_at = datetime('now'),
                note = excluded.note
            """,
            (family, supplier, rule, nag_threshold_savings,
             nag_threshold_pct, auto_pad_threshold_savings, actor, note),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("family_pricing_rule.set", actor,
             f"{family}/{supplier}", rule),
        )


def get_family_pricing_rule(
    family: str, supplier: str,
) -> Optional[sqlite3.Row]:
    with connect() as c:
        return c.execute(
            "SELECT * FROM family_pricing_rules "
            "WHERE family = ? AND supplier = ?",
            (family, supplier),
        ).fetchone()


def all_family_pricing_rules() -> List[sqlite3.Row]:
    with connect() as c:
        return c.execute(
            "SELECT * FROM family_pricing_rules "
            "ORDER BY family, supplier").fetchall()


def delete_family_pricing_rule(family: str, supplier: str,
                                 actor: str) -> None:
    with connect() as c:
        c.execute(
            "DELETE FROM family_pricing_rules "
            "WHERE family = ? AND supplier = ?",
            (family, supplier),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("family_pricing_rule.delete", actor,
             f"{family}/{supplier}", ""),
        )


# ----- Per-SKU pack settings ------------------------------------------

def set_sku_pack(sku: str, pack_qty: float, actor: str,
                  moq: Optional[float] = None, note: str = "") -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO sku_pack_settings
                (sku, pack_qty, moq, note, set_by)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(sku) DO UPDATE SET
                pack_qty = excluded.pack_qty,
                moq = excluded.moq,
                note = excluded.note,
                set_by = excluded.set_by,
                set_at = datetime('now')
            """,
            (sku, pack_qty, moq, note, actor),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("sku_pack.set", actor, sku,
             f"pack={pack_qty} moq={moq}"),
        )


def get_sku_pack(sku: str) -> Optional[sqlite3.Row]:
    with connect() as c:
        return c.execute(
            "SELECT * FROM sku_pack_settings WHERE sku = ?",
            (sku,)).fetchone()


def all_sku_pack() -> List[sqlite3.Row]:
    with connect() as c:
        return c.execute(
            "SELECT * FROM sku_pack_settings ORDER BY sku").fetchall()


def clear_sku_pack(sku: str, actor: str) -> None:
    with connect() as c:
        c.execute("DELETE FROM sku_pack_settings WHERE sku = ?", (sku,))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("sku_pack.clear", actor, sku, ""),
        )


# ----- Tier resolution helper -----------------------------------------

def resolve_tier_for_qty(family: str, color: str, supplier: str,
                          qty: float) -> Optional[sqlite3.Row]:
    """Given a qty, return the tier row whose tier_qty is the highest
    value <= qty (i.e., the tier this qty qualifies for). Returns None
    if no tiers configured or qty is below all tiers' minimums."""
    with connect() as c:
        return c.execute(
            "SELECT * FROM family_color_pricing "
            "WHERE family = ? AND color = ? AND supplier = ? "
            "  AND tier_qty <= ? "
            "ORDER BY tier_qty DESC LIMIT 1",
            (family, color, supplier, qty),
        ).fetchone()


def next_tier_for_qty(family: str, color: str, supplier: str,
                       qty: float) -> Optional[sqlite3.Row]:
    """The next tier ABOVE the given qty (i.e., the cheaper-per-unit
    tier the buyer could reach by adding more). Returns None if already
    at the top tier."""
    with connect() as c:
        return c.execute(
            "SELECT * FROM family_color_pricing "
            "WHERE family = ? AND color = ? AND supplier = ? "
            "  AND tier_qty > ? "
            "ORDER BY tier_qty ASC LIMIT 1",
            (family, color, supplier, qty),
        ).fetchone()


# =====================================================================
# PO draft edits — persistent qty edits across sessions/restarts
# =====================================================================

def set_po_draft_edit(supplier: str, sku: str, edited_qty: float,
                       actor: str = "", note: str = "") -> None:
    """Save a per-supplier-per-SKU draft qty edit. Upserts."""
    with connect() as c:
        c.execute(
            """
            INSERT INTO po_draft_edits
                (supplier, sku, edited_qty, set_by, note)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(supplier, sku) DO UPDATE SET
                edited_qty = excluded.edited_qty,
                edited_at = datetime('now'),
                set_by = excluded.set_by,
                note = excluded.note
            """,
            (supplier, sku, edited_qty, actor, note),
        )


def clear_po_draft_edit(supplier: str, sku: str) -> None:
    """Remove one draft entry."""
    with connect() as c:
        c.execute(
            "DELETE FROM po_draft_edits "
            "WHERE supplier = ? AND sku = ?",
            (supplier, sku))


def clear_po_draft_edits_for_supplier(supplier: str, actor: str = "") -> int:
    """Wipe all drafts for a supplier. Returns count cleared."""
    with connect() as c:
        n = c.execute(
            "DELETE FROM po_draft_edits WHERE supplier = ?",
            (supplier,)).rowcount
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("po_draft.clear_all", actor or "unknown",
             supplier, f"cleared {n} draft edits"))
        return n


def get_po_draft_edits(supplier: str) -> dict:
    """Returns {sku: edited_qty} for a supplier."""
    with connect() as c:
        rows = c.execute(
            "SELECT sku, edited_qty FROM po_draft_edits "
            "WHERE supplier = ?",
            (supplier,)).fetchall()
        return {r["sku"]: float(r["edited_qty"]) for r in rows}


def all_po_draft_edits() -> List[sqlite3.Row]:
    """All drafts across all suppliers — for an admin view."""
    with connect() as c:
        return c.execute(
            "SELECT * FROM po_draft_edits "
            "ORDER BY supplier, sku").fetchall()


# =====================================================================
# PO drafts v2 — multi-draft per supplier with lifecycle + locking
# =====================================================================

# Auto-release a lock after this many minutes of inactivity. Buyers
# sometimes forget to release; without auto-timeout drafts get stuck.
PO_DRAFT_LOCK_TIMEOUT_MIN = 30


def create_po_draft(supplier: str, name: str, actor: str,
                     freight_mode: Optional[str] = None,
                     note: str = "") -> int:
    """Create a new draft for a supplier. Returns the new draft id."""
    with connect() as c:
        cur = c.execute(
            """
            INSERT INTO po_drafts
                (supplier, name, freight_mode, status,
                 created_by, locked_by, locked_at, note)
            VALUES (?, ?, ?, 'editing', ?, ?, datetime('now'), ?)
            """,
            (supplier, name, freight_mode, actor, actor, note),
        )
        draft_id = cur.lastrowid
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("po_draft.create", actor, f"{supplier}/{draft_id}",
             f"name={name!r} freight={freight_mode}"),
        )
        return draft_id


def get_po_draft(draft_id: int) -> Optional[sqlite3.Row]:
    """Fetch one draft (with auto-release if its lock has timed out)."""
    with connect() as c:
        row = c.execute(
            "SELECT * FROM po_drafts WHERE id = ?", (draft_id,)
        ).fetchone()
        if row is None:
            return None
        # Apply lock timeout
        if row["locked_by"] and row["locked_at"]:
            try:
                lock_t = datetime.fromisoformat(
                    str(row["locked_at"]).replace("Z", ""))
                age_min = (datetime.utcnow() - lock_t).total_seconds() / 60
                if age_min > PO_DRAFT_LOCK_TIMEOUT_MIN:
                    c.execute(
                        "UPDATE po_drafts SET locked_by = NULL, "
                        "locked_at = NULL WHERE id = ?", (draft_id,))
                    # Re-read
                    row = c.execute(
                        "SELECT * FROM po_drafts WHERE id = ?", (draft_id,)
                    ).fetchone()
            except (ValueError, TypeError):
                pass
        return row


def list_po_drafts(supplier: Optional[str] = None,
                    status: Optional[str] = None,
                    include_archived: bool = False) -> List[sqlite3.Row]:
    """List drafts, optionally filtered. By default hides finalized/cancelled
    (set include_archived=True to see them). First applies any pending
    lock auto-releases on the in-memory results."""
    sql = "SELECT * FROM po_drafts"
    where = []
    args: list = []
    if supplier:
        where.append("supplier = ?")
        args.append(supplier)
    if status:
        where.append("status = ?")
        args.append(status)
    elif not include_archived:
        where.append("status IN ('editing', 'submitted')")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC"
    with connect() as c:
        rows = c.execute(sql, args).fetchall()
    # Apply lock timeouts in a follow-up pass (not while iterating in
    # the first connect block — we reopen briefly to release stale ones).
    stale_ids = []
    for r in rows:
        if r["locked_by"] and r["locked_at"]:
            try:
                lock_t = datetime.fromisoformat(
                    str(r["locked_at"]).replace("Z", ""))
                age_min = (datetime.utcnow() - lock_t).total_seconds() / 60
                if age_min > PO_DRAFT_LOCK_TIMEOUT_MIN:
                    stale_ids.append(r["id"])
            except (ValueError, TypeError):
                pass
    if stale_ids:
        with connect() as c:
            c.executemany(
                "UPDATE po_drafts SET locked_by = NULL, "
                "locked_at = NULL WHERE id = ?",
                [(i,) for i in stale_ids])
        # Re-fetch with the same filter so callers see fresh state
        with connect() as c:
            rows = c.execute(sql, args).fetchall()
    return rows


def lock_po_draft(draft_id: int, actor: str) -> bool:
    """Attempt to lock a draft for editing by `actor`. Returns True if
    the lock is now held by actor; False if someone else holds it (and
    their lock isn't yet stale)."""
    with connect() as c:
        # Read current state
        row = c.execute(
            "SELECT locked_by, locked_at FROM po_drafts WHERE id = ?",
            (draft_id,)).fetchone()
        if row is None:
            return False
        cur_by = row["locked_by"]
        cur_at = row["locked_at"]
        # If held by someone else, check timeout
        if cur_by and cur_by != actor:
            try:
                lock_t = datetime.fromisoformat(
                    str(cur_at).replace("Z", "")) if cur_at else None
                age_min = ((datetime.utcnow() - lock_t).total_seconds() / 60
                            if lock_t else 0)
            except (ValueError, TypeError):
                age_min = 0
            if age_min < PO_DRAFT_LOCK_TIMEOUT_MIN and lock_t is not None:
                return False  # someone else holds it, not stale
        # Take/refresh the lock
        c.execute(
            "UPDATE po_drafts SET locked_by = ?, "
            "locked_at = datetime('now') WHERE id = ?",
            (actor, draft_id))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("po_draft.lock", actor, str(draft_id),
             "took/refreshed lock"))
        return True


def release_po_draft_lock(draft_id: int, actor: str,
                            force: bool = False) -> bool:
    """Release the lock. Default: only the current locker can release.
    Pass force=True to override (e.g., admin clearing a stuck lock)."""
    with connect() as c:
        row = c.execute(
            "SELECT locked_by FROM po_drafts WHERE id = ?",
            (draft_id,)).fetchone()
        if row is None:
            return False
        if row["locked_by"] and row["locked_by"] != actor and not force:
            return False
        c.execute(
            "UPDATE po_drafts SET locked_by = NULL, locked_at = NULL "
            "WHERE id = ?", (draft_id,))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("po_draft.release", actor, str(draft_id),
             "force=true" if force else "released"))
        return True


def upsert_po_draft_line(draft_id: int, sku: str, edited_qty: float,
                          actor: str, note: str = "") -> None:
    """Save (insert or update) one line in a draft. Caller is expected
    to have verified the lock is held by actor."""
    with connect() as c:
        c.execute(
            """
            INSERT INTO po_draft_lines
                (draft_id, sku, edited_qty, last_edited_by, note)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(draft_id, sku) DO UPDATE SET
                edited_qty = excluded.edited_qty,
                last_edited_by = excluded.last_edited_by,
                last_edited_at = datetime('now'),
                note = excluded.note
            """,
            (draft_id, sku, edited_qty, actor, note),
        )


def delete_po_draft_line(draft_id: int, sku: str) -> None:
    """Remove one line from a draft (e.g., user reverted to engine default)."""
    with connect() as c:
        c.execute(
            "DELETE FROM po_draft_lines WHERE draft_id = ? AND sku = ?",
            (draft_id, sku))


def get_po_draft_lines(draft_id: int) -> dict:
    """Returns {sku: edited_qty} for a draft."""
    with connect() as c:
        rows = c.execute(
            "SELECT sku, edited_qty FROM po_draft_lines "
            "WHERE draft_id = ?", (draft_id,)).fetchall()
        return {r["sku"]: float(r["edited_qty"]) for r in rows}


def list_po_draft_lines(draft_id: int) -> List[sqlite3.Row]:
    """Full row data for a draft's lines (for audit / admin view)."""
    with connect() as c:
        return c.execute(
            "SELECT * FROM po_draft_lines WHERE draft_id = ? "
            "ORDER BY sku", (draft_id,)).fetchall()


def mark_po_draft_submitted(draft_id: int, actor: str,
                              cin7_po_number: str = "",
                              cin7_po_id: str = "",
                              note: str = "") -> None:
    """Transition a draft from editing → submitted. Records CIN7 PO
    number/ID. Releases the lock."""
    with connect() as c:
        c.execute(
            """
            UPDATE po_drafts SET
                status = 'submitted',
                cin7_po_number = ?,
                cin7_po_id = ?,
                submitted_at = datetime('now'),
                submitted_by = ?,
                locked_by = NULL,
                locked_at = NULL,
                note = COALESCE(NULLIF(?, ''), note)
            WHERE id = ?
            """,
            (cin7_po_number, cin7_po_id, actor, note, draft_id),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("po_draft.submit", actor, str(draft_id),
             f"cin7_po_number={cin7_po_number}"),
        )


def set_po_draft_cin7_ids(draft_id: int, *,
                            cin7_po_id: str | None = None,
                            cin7_po_number: str | None = None,
                            cin7_status: str | None = None,
                            actor: str = "system") -> None:
    """Persist CIN7 identifiers on a draft without changing its status.
    Used by the push flow's partial-success path: if the master POST
    succeeds but the lines POST fails, we still want to remember the
    CIN7 PO ID locally so the buyer can find it. Only non-None fields
    are written, so this is also safe to call with just one of them."""
    sets = []
    params: list = []
    if cin7_po_id is not None:
        sets.append("cin7_po_id = ?")
        params.append(cin7_po_id)
    if cin7_po_number is not None:
        sets.append("cin7_po_number = ?")
        params.append(cin7_po_number)
    if cin7_status is not None:
        sets.append("cin7_po_status = ?")
        params.append(cin7_status)
    if not sets:
        return
    params.append(draft_id)
    with connect() as c:
        c.execute(
            f"UPDATE po_drafts SET {', '.join(sets)} WHERE id = ?",
            params,
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("po_draft.cin7_ids_set", actor, str(draft_id),
             f"id={cin7_po_id} num={cin7_po_number} status={cin7_status}"),
        )


def mark_po_draft_finalized(draft_id: int, actor: str = "auto-sync",
                              cin7_po_status: str = "ORDERED") -> None:
    """Transition submitted → finalized. Typically called by the sync
    job when it detects the CIN7 PO has been confirmed."""
    with connect() as c:
        c.execute(
            """
            UPDATE po_drafts SET
                status = 'finalized',
                cin7_po_status = ?,
                finalized_at = datetime('now')
            WHERE id = ?
            """,
            (cin7_po_status, draft_id),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("po_draft.finalize", actor, str(draft_id),
             f"cin7_po_status={cin7_po_status}"),
        )


def cancel_po_draft(draft_id: int, actor: str, reason: str = "") -> None:
    """Cancel a draft. If the draft was already submitted, this only
    clears our local state — the CIN7 PO must be cancelled separately
    in CIN7. Releases the pessimistic lock on the way out."""
    with connect() as c:
        c.execute(
            """
            UPDATE po_drafts SET
                status = 'cancelled',
                locked_by = NULL,
                locked_at = NULL,
                note = COALESCE(NULLIF(?, ''), note)
            WHERE id = ?
            """,
            (reason, draft_id),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("po_draft.cancel", actor, str(draft_id), reason),
        )


def rename_po_draft(draft_id: int, new_name: str, actor: str) -> None:
    """Rename a draft. UI helper only — doesn't touch CIN7."""
    with connect() as c:
        c.execute(
            "UPDATE po_drafts SET name = ? WHERE id = ?",
            (new_name, draft_id))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("po_draft.rename", actor, str(draft_id),
             f"-> {new_name!r}"),
        )


# ---------------------------------------------------------------------------
# AI Q&A logging + alias learning
# ---------------------------------------------------------------------------

def log_ai_query(*,
                  user_id: str,
                  user_question: str,
                  parsed_intent: Optional[str] = None,
                  tools_called_json: Optional[str] = None,
                  answer_returned: Optional[str] = None,
                  confidence_score: Optional[float] = None,
                  duration_ms: Optional[int] = None,
                  model_used: Optional[str] = None) -> int:
    """Record one AI Assistant interaction. Returns the row ID so the
    caller can attach feedback later when the user clicks thumbs-up/down."""
    with connect() as c:
        cur = c.execute(
            """
            INSERT INTO ai_audit_logs
                (user_id, user_question, parsed_intent, tools_called_json,
                 answer_returned, confidence_score, duration_ms, model_used)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, user_question, parsed_intent, tools_called_json,
             answer_returned, confidence_score, duration_ms, model_used),
        )
        return int(cur.lastrowid)


def record_ai_feedback(audit_id: int, feedback: str,
                        note: str = "",
                        user_id: str = "") -> None:
    """Attach thumbs-up/down feedback to an existing audit log row.
    Feedback is written to two places:
      - ai_audit_logs.feedback (quick filtering of a single chat)
      - feedback_events (generic feedback stream — same pattern used
        for Slack reactions, Gorgias resolutions, buyer dashboard
        clicks etc when those land later).
    Keeping both lets the AI page query is-this-row-rated quickly
    while the cross-source analytics live in feedback_events."""
    if feedback not in ("positive", "negative"):
        raise ValueError("feedback must be 'positive' or 'negative'")
    with connect() as c:
        c.execute(
            "UPDATE ai_audit_logs SET feedback = ?, feedback_note = ? "
            "WHERE id = ?",
            (feedback, note, audit_id),
        )
        c.execute(
            """
            INSERT INTO feedback_events
                (source, entity_type, entity_id, feedback, note, user_id)
            VALUES ('ai_chat', 'ai_audit_log', ?, ?, ?, ?)
            """,
            (str(audit_id), feedback, note, user_id),
        )


def record_feedback_event(*,
                            source: str,
                            entity_type: str,
                            entity_id: str,
                            feedback: str,
                            note: str = "",
                            user_id: str = "") -> int:
    """Generic feedback writer for non-AI sources (Slack reactions,
    Gorgias outcomes, buyer dashboard, etc.). Returns the row id."""
    with connect() as c:
        cur = c.execute(
            """
            INSERT INTO feedback_events
                (source, entity_type, entity_id, feedback, note, user_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (source, entity_type, str(entity_id), feedback, note, user_id),
        )
        return int(cur.lastrowid)


def list_ai_queries(user_id: Optional[str] = None,
                     limit: int = 50) -> List[sqlite3.Row]:
    """Recent AI queries, optionally filtered by user. Newest first."""
    sql = "SELECT * FROM ai_audit_logs"
    params: list = []
    if user_id:
        sql += " WHERE user_id = ?"
        params.append(user_id)
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    with connect() as c:
        return c.execute(sql, params).fetchall()


def upsert_product_alias(phrase: str, *,
                          sku: Optional[str] = None,
                          product_family: Optional[str] = None,
                          confidence: float = 0.5,
                          approved_by: str = "ai") -> None:
    """Store/update a phrase → SKU/family mapping. Phrase is lowercased.
    On collision (same phrase already mapped to same target), bump
    times_used + last_used_at instead of creating duplicate rows."""
    phrase_n = (phrase or "").strip().lower()
    if not phrase_n:
        return
    with connect() as c:
        existing = c.execute(
            "SELECT id, times_used FROM product_aliases "
            "WHERE phrase = ? AND COALESCE(sku, '') = COALESCE(?, '') "
            "AND COALESCE(product_family, '') = COALESCE(?, '')",
            (phrase_n, sku, product_family),
        ).fetchone()
        if existing:
            c.execute(
                "UPDATE product_aliases SET "
                "times_used = times_used + 1, "
                "last_used_at = datetime('now'), "
                "confidence = MAX(confidence, ?) "
                "WHERE id = ?",
                (confidence, existing["id"]),
            )
        else:
            c.execute(
                """
                INSERT INTO product_aliases
                    (phrase, sku, product_family, confidence, approved_by)
                VALUES (?, ?, ?, ?, ?)
                """,
                (phrase_n, sku, product_family, confidence, approved_by),
            )


def lookup_aliases(phrase: str,
                    min_confidence: float = 0.0) -> List[sqlite3.Row]:
    """Return any stored aliases matching the phrase (case-insensitive),
    ordered by times_used desc. Use this BEFORE calling the LLM to
    short-circuit common questions."""
    phrase_n = (phrase or "").strip().lower()
    if not phrase_n:
        return []
    with connect() as c:
        return c.execute(
            "SELECT * FROM product_aliases "
            "WHERE phrase = ? AND confidence >= ? "
            "ORDER BY times_used DESC, confidence DESC",
            (phrase_n, min_confidence),
        ).fetchall()


# ---------------------------------------------------------------------------
# Demand signals — proactive intelligence layer
# ---------------------------------------------------------------------------

def insert_demand_signal(*,
                          source: str,
                          signal_type: str,
                          sku: Optional[str] = None,
                          product_family: Optional[str] = None,
                          quantity: Optional[float] = None,
                          customer_id: Optional[str] = None,
                          customer_name: Optional[str] = None,
                          salesperson: Optional[str] = None,
                          raw_text: Optional[str] = None,
                          source_ref: Optional[str] = None,
                          confidence: float = 1.0,
                          needs_review: bool = False,
                          outcome: Optional[str] = None,
                          note: Optional[str] = None,
                          created_by: str = "system") -> int:
    """Record one demand signal. Returns the row id.

    Either `sku` or `product_family` should be provided — the buyer
    warning logic will skip rows that have neither (they're useless).
    """
    if not source:
        raise ValueError("source is required")
    if not signal_type:
        raise ValueError("signal_type is required")
    with connect() as c:
        cur = c.execute(
            """
            INSERT INTO demand_signals
                (source, source_ref, sku, product_family, raw_text,
                 signal_type, quantity, customer_id, customer_name,
                 salesperson, confidence, needs_review, outcome, note,
                 created_by, updated_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (source, source_ref, sku, product_family, raw_text,
             signal_type, quantity, customer_id, customer_name,
             salesperson, confidence, 1 if needs_review else 0,
             outcome, note, created_by, created_by),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("demand_signal.insert", created_by,
             sku or product_family or "?",
             f"source={source} type={signal_type} qty={quantity}"))
        return int(cur.lastrowid)


def update_demand_signal(signal_id: int, *,
                          outcome: Optional[str] = None,
                          note: Optional[str] = None,
                          quantity: Optional[float] = None,
                          customer_id: Optional[str] = None,
                          sku: Optional[str] = None,
                          product_family: Optional[str] = None,
                          needs_review: Optional[bool] = None,
                          updated_by: str = "system") -> None:
    """Update a signal's mutable fields. Only non-None args are
    written, so it's safe to call with just the fields you mean to
    change."""
    sets = []
    params: list = []
    if outcome is not None:
        sets.append("outcome = ?")
        params.append(outcome)
    if note is not None:
        sets.append("note = ?")
        params.append(note)
    if quantity is not None:
        sets.append("quantity = ?")
        params.append(quantity)
    if customer_id is not None:
        sets.append("customer_id = ?")
        params.append(customer_id)
    if sku is not None:
        sets.append("sku = ?")
        params.append(sku)
    if product_family is not None:
        sets.append("product_family = ?")
        params.append(product_family)
    if needs_review is not None:
        sets.append("needs_review = ?")
        params.append(1 if needs_review else 0)
    if not sets:
        return
    sets.append("updated_at = datetime('now')")
    sets.append("updated_by = ?")
    params.append(updated_by)
    params.append(signal_id)
    with connect() as c:
        c.execute(
            f"UPDATE demand_signals SET {', '.join(sets)} WHERE id = ?",
            params,
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("demand_signal.update", updated_by, str(signal_id),
             ",".join(sets[:-2])))   # exclude updated_at/updated_by


def list_demand_signals(*,
                         sku: Optional[str] = None,
                         product_family: Optional[str] = None,
                         signal_type: Optional[str] = None,
                         source: Optional[str] = None,
                         since: Optional[str] = None,   # ISO date
                         outcome: Optional[str] = None,
                         needs_review: Optional[bool] = None,
                         limit: int = 200) -> List[sqlite3.Row]:
    """Filtered list of signals, newest first. All filters are optional.
    Used by the buyer dashboard, the AI tools, and the warning column.
    """
    sql = "SELECT * FROM demand_signals"
    where = []
    params: list = []
    if sku:
        where.append("sku = ?")
        params.append(sku)
    if product_family:
        where.append("product_family = ?")
        params.append(product_family)
    if signal_type:
        where.append("signal_type = ?")
        params.append(signal_type)
    if source:
        where.append("source = ?")
        params.append(source)
    if since:
        where.append("created_at >= ?")
        params.append(since)
    if outcome:
        where.append("outcome = ?")
        params.append(outcome)
    if needs_review is not None:
        where.append("needs_review = ?")
        params.append(1 if needs_review else 0)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(int(limit))
    with connect() as c:
        return c.execute(sql, params).fetchall()


def count_demand_signals_by_sku(*,
                                  since: Optional[str] = None,
                                  signal_type: Optional[str] = None,
                                  ) -> dict:
    """Aggregate count of signals per SKU. Used by the reorder warning
    column to flag SKUs with rising/concentrated inquiries.
    Returns {sku: count}."""
    sql = ("SELECT sku, COUNT(*) AS n FROM demand_signals "
           "WHERE sku IS NOT NULL AND sku != ''")
    params: list = []
    if since:
        sql += " AND created_at >= ?"
        params.append(since)
    if signal_type:
        sql += " AND signal_type = ?"
        params.append(signal_type)
    sql += " GROUP BY sku"
    with connect() as c:
        return {r["sku"]: int(r["n"])
                for r in c.execute(sql, params).fetchall()}


def delete_demand_signal(signal_id: int, actor: str = "system") -> None:
    """Hard-delete a signal. Use sparingly — for genuinely-invalid
    rows. For wrong-but-real signals prefer update_demand_signal()
    with outcome='invalid' so the audit trail stays."""
    with connect() as c:
        c.execute("DELETE FROM demand_signals WHERE id = ?", (signal_id,))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("demand_signal.delete", actor, str(signal_id), ""))
