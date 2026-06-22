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

-- Users / profiles. v2.66.
-- Lightweight profile system — NOT per-user authentication. The shared
-- APP_PASSWORD gate stays. Once past the gate, the user picks (or types)
-- their name, and we load their profile so actions are tied to a real
-- team member and forms can read their defaults.
--
-- display_name is the human-friendly name shown everywhere ('James',
-- 'Sarah', 'Aiden'). UNIQUE COLLATE NOCASE so 'james' and 'James' don't
-- create duplicate rows.
--
-- role gates feature visibility (buyer / sales / admin / viewer). admin
-- can edit anyone's profile; everyone else only their own.
CREATE TABLE IF NOT EXISTS users (
    user_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    display_name    TEXT    NOT NULL UNIQUE COLLATE NOCASE,
    role            TEXT    NOT NULL DEFAULT 'sales',
    email           TEXT,
    active          INTEGER NOT NULL DEFAULT 1,
    default_page    TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    updated_at      TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS ix_users_active ON users(active, role);

CREATE TABLE IF NOT EXISTS audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event       TEXT    NOT NULL,
    actor       TEXT    NOT NULL,
    target      TEXT,
    detail      TEXT,
    at          TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS ix_audit_at ON audit_log(at);

-- v2.67.185 — per-user page permissions.
-- One row per (user_id, page_name). If a user has NO rows at all,
-- they see every page (backwards-compat — existing users don't
-- lose access until permissions are explicitly configured for
-- them). Once at least one row exists for a user, only pages
-- with allowed=1 are visible.
-- Admins (users.role='admin') always see every page regardless
-- of rows here. Set via the "User Permissions" admin page.
CREATE TABLE IF NOT EXISTS user_page_permissions (
    user_id     INTEGER NOT NULL,
    page_name   TEXT    NOT NULL,
    allowed     INTEGER NOT NULL DEFAULT 0,
    set_by      TEXT,
    set_at      TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (user_id, page_name)
);
CREATE INDEX IF NOT EXISTS ix_user_page_permissions_user
    ON user_page_permissions(user_id);

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
    order_cadence_days  INTEGER,            -- v2.67.283 — real reorder
                                            -- interval (e.g. 7 = weekly);
                                            -- overrides ABC review_days
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

-- v2.67.284 — supplier holiday / shutdown periods. Each row is a
-- known closure (e.g. summer shutdown, Chinese New Year, public
-- holidays). The reorder engine looks at the upcoming lead-time +
-- cadence window and adds any closed days to the target cover, so
-- an order placed before a shutdown automatically bridges it.
-- Multiple periods per supplier are supported (just add more rows).
CREATE TABLE IF NOT EXISTS supplier_holidays (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_name   TEXT    NOT NULL,
    start_date      DATE    NOT NULL,
    end_date        DATE    NOT NULL,
    label           TEXT,
    created_by      TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_supplier_holidays_supplier
    ON supplier_holidays(supplier_name);

-- v2.67.303 — Shopify Admin API discount totals by month. The
-- CIN7-derived discount line on Monthly Metrics Section 6 was a
-- proxy that undercounted by 60-70% per the May 2026 audit
-- (~$10k/mo vs Shopify's real ~$25-45k/mo). Shopify is the
-- source of truth for coupons / automatic promos / compare-at /
-- shipping discounts / draft adjustments — pulled daily via
-- shopify_discounts.py.
CREATE TABLE IF NOT EXISTS shopify_monthly_discounts (
    month             TEXT PRIMARY KEY,        -- 'YYYY-MM'
    total_discounts   REAL NOT NULL,
    order_count       INTEGER NOT NULL,
    synced_at         TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);

-- v2.67.302 — persistent user sessions. Pre-fix, every Render deploy
-- reset every Streamlit worker's session_state and forced staff to
-- re-pick their name and click Sign In again, multiple times per
-- day during active development. Sessions now live in Postgres
-- (survives deploys) and are keyed by a random token stored in the
-- browser URL. 24h sliding expiry — refreshed on every access, so
-- active users never have to re-sign in. Idle >24h triggers a
-- fresh sign-in. Stored token = URL-safe random 32 bytes.
CREATE TABLE IF NOT EXISTS user_sessions (
    token           TEXT PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    expires_at      TIMESTAMP NOT NULL,
    last_used_at    TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_user_sessions_expires
    ON user_sessions(expires_at);

-- v2.67.285 — observed actual lead times pulled from Inventory
-- Planner. Per-SKU; IP tracks the real elapsed time between PO
-- placement and receipt (avg_lead_time). The reorder engine
-- prefers this over the supplier_config default — observed beats
-- a stale 35-day default every time.
CREATE TABLE IF NOT EXISTS ip_lead_times (
    sku                       TEXT    PRIMARY KEY,
    observed_lead_time_days   INTEGER,           -- IP avg_lead_time
    configured_lead_time_days INTEGER,           -- IP lead_time setting
    vendor_name               TEXT,
    sales_velocity1           REAL,              -- IP daily velocity
    last_received_at          TEXT,
    synced_at                 TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_ip_lead_times_vendor
    ON ip_lead_times(vendor_name);

-- v2.67.292 — QuickBooks Online Profit & Loss data by month.
-- The Monthly Metrics page treats CIN7-derived figures as
-- "operational" and QB account values as canonical, because the
-- Viktor cross-system audit found:
--   • Shipping Charged inflated 27-218% vs QB acc 405 every month
--   • Historical COGS drifting up to 27% vs QB acc 500
--   • Dec 2025 sales gap of -$45k (journal entry not in CIN7)
-- Reading from this table lets the page show QB alongside CIN7 so
-- finance / commissions reference the reconciled financials.
CREATE TABLE IF NOT EXISTS qbo_monthly_pl (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    month           TEXT NOT NULL,        -- 'YYYY-MM'
    account_id      TEXT,                  -- QBO Id (preferred match)
    account_number  TEXT,                  -- chart-of-accounts num
                                           -- (e.g. '400', '500')
    account_name    TEXT NOT NULL,
    account_type    TEXT,                  -- Income/Expense/CoGS
    parent_account_id TEXT,
    amount          REAL NOT NULL,
    synced_at       TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    UNIQUE(month, account_id, account_name)
);
CREATE INDEX IF NOT EXISTS idx_qbo_monthly_pl_month
    ON qbo_monthly_pl(month);
CREATE INDEX IF NOT EXISTS idx_qbo_monthly_pl_acctnum
    ON qbo_monthly_pl(account_number);

-- v2.67.292 — canonical mapping from a Monthly Metrics "category"
-- (sales / cogs / shipping_charged / shipping_cost / etc.) to one
-- or more QBO accounts. Pre-seeded with W4S's chart of accounts
-- (per Viktor's audit) but editable in the Methodology UI so other
-- companies can adapt without code changes.
CREATE TABLE IF NOT EXISTS qbo_account_mappings (
    category        TEXT PRIMARY KEY,      -- e.g. 'sales', 'cogs'
    account_numbers TEXT,                  -- CSV: '400,401,402'
    account_names   TEXT,                  -- CSV fallback when no
                                           -- account number on chart
    notes           TEXT,
    set_by          TEXT,
    set_at          TIMESTAMP NOT NULL DEFAULT (datetime('now'))
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

    -- LIFECYCLE — set by buyer/sales after the fact when known.
    -- Canonical values (v2.59+): 'pending' | 'converted' | 'lost' |
    --                            'ignored' | 'duplicate' | 'wrong_sku'
    -- Legacy values still in old rows: 'open' (= pending), 'invalid'
    -- (= ignored). normalize_outcome() in this file maps them on read.
    outcome         TEXT,
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

-- v2.67.36 — Dormancy provenance log. Tracks SKUs that have been
-- flagged is_dormant=True at any engine run, so that when a salesman
-- successfully sells one (because the AI surfaced it as slow stock),
-- the buyer gets a "!" warning before reordering. The warning auto-
-- lifts after sustained recovery (90 days of post-dormancy active
-- demand) or buyer manual dismiss. Critical for the discounting/
-- promotion fly-wheel: we don't want sales-driven recoveries to
-- inflate reorder targets and re-stock items we're trying to
-- liquidate.
CREATE TABLE IF NOT EXISTS sku_dormancy_log (
    sku                     TEXT PRIMARY KEY,
    first_seen_dormant_at   TIMESTAMP,        -- earliest is_dormant=True observation
    last_seen_dormant_at    TIMESTAMP,        -- most recent is_dormant=True observation
    recovered_at            TIMESTAMP,        -- first observation post-dormancy
                                                -- where is_dormant=False AND demand>0;
                                                -- cleared if SKU goes dormant again
    warning_lifted_at       TIMESTAMP,        -- NULL = warning still active
    warning_lift_reason     TEXT,             -- 'auto_recovered_90d' | 'manual_dismiss'
    warning_lifted_by       TEXT,             -- user_id who dismissed (manual)
    last_engine_run_at      TIMESTAMP,        -- last time engine touched this row
    created_at              TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS ix_dormancy_active
    ON sku_dormancy_log(warning_lifted_at)
    WHERE warning_lifted_at IS NULL;

-- v2.67.42 — daily snapshot of slow-stock value on shelf. Used by
-- the Overview tile + Slow Movers page to show month-over-month
-- progress / regression. Snapshot key is the date so we get one
-- row per day; engine recomputes during the day overwrite the
-- same date's row (last-write-wins).
CREATE TABLE IF NOT EXISTS slow_mover_value_snapshots (
    snapshot_date         DATE PRIMARY KEY,
    skus_count            INTEGER,
    units_on_hand         REAL,
    value_on_shelf        REAL,    -- StockOnHand sum across the day's slow SKUs
    captured_at           TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);

-- ============================================================
-- v2.67.57 — Slack integration
-- ============================================================
-- The bot polls Slack `conversations.history` every ~60s and
-- writes every message it sees into slack_messages. Subsequent
-- listener pass classifies each unprocessed message (question /
-- trigger / chatter), and either responds (logging to
-- slack_bot_responses) or marks the message as skipped.
--
-- Why not just memory-cache: the listener and AI tools (e.g.
-- get_slack_messages for cross-channel grep) need a queryable
-- store, AND we need a durable audit log of every bot post for
-- accountability + retrospective tuning.

CREATE TABLE IF NOT EXISTS slack_messages (
    -- (channel_id, ts) is unique per Slack message
    channel_id      TEXT NOT NULL,
    ts              TEXT NOT NULL,    -- Slack timestamp (string, contains '.')
    user_id         TEXT,             -- e.g. U02ABCDEF
    user_name       TEXT,             -- resolved display name
    text            TEXT,             -- raw message text
    thread_ts       TEXT,             -- if this message is a thread reply, the parent ts
    is_bot          INTEGER DEFAULT 0,-- 1 if posted by a bot (incl. our own)
    is_our_bot      INTEGER DEFAULT 0,-- 1 if posted by THIS bot (so we never reply to self)
    permalink       TEXT,             -- direct link back to the Slack message
    raw_event       TEXT,             -- original JSON for debugging
    ingested_at     TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    -- Listener bookkeeping. NULL = not yet classified.
    -- 'question' / 'trigger' / 'chatter' / 'bot_self' / 'too_old'
    classification  TEXT,
    classified_at   TIMESTAMP,
    response_id     INTEGER,         -- FK into slack_bot_responses if we responded
    PRIMARY KEY (channel_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_slack_messages_unclassified
    ON slack_messages(classification, ingested_at)
    WHERE classification IS NULL;
CREATE INDEX IF NOT EXISTS idx_slack_messages_thread
    ON slack_messages(channel_id, thread_ts);

CREATE TABLE IF NOT EXISTS slack_bot_responses (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    in_channel      TEXT NOT NULL,        -- channel id where we replied
    in_ts           TEXT NOT NULL,        -- ts of the message we replied to
    in_thread_ts    TEXT NOT NULL,        -- thread_ts we posted into
    user_question   TEXT,                 -- truncated copy of the user's text
    response_text   TEXT,                 -- the full text we posted
    response_ts     TEXT,                 -- Slack ts of our reply (returned by chat.postMessage)
    tools_used      TEXT,                 -- comma-separated AI tool names invoked
    classification  TEXT,                 -- question/trigger/mention
    audit_posted    INTEGER DEFAULT 0,    -- 1 once mirrored to #ai-audit
    posted_at       TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    -- Feedback signals: team can react with 🛑/👎/⛔ to flag a bad response.
    -- Updated by the listener on subsequent polls.
    flag_count      INTEGER DEFAULT 0,
    flagged_at      TIMESTAMP,
    flag_reason     TEXT
);
CREATE INDEX IF NOT EXISTS idx_slack_bot_responses_pending_audit
    ON slack_bot_responses(audit_posted)
    WHERE audit_posted = 0;

-- Cursor table: per-channel, the latest Slack ts we've ingested.
-- Used by slack_sync to do incremental conversations.history pulls.
CREATE TABLE IF NOT EXISTS slack_channel_cursors (
    channel_id      TEXT PRIMARY KEY,
    channel_name    TEXT,                 -- friendly name when we resolve it
    last_ts         TEXT,                 -- highest ts ingested so far
    last_pulled_at  TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);

-- ============================================================
-- v2.67.66 — Feedback ingest + auto-improvement loop
-- ============================================================
-- The bot now reads its own audit trail (#ai-audit + thread replies
-- on its posts in any monitored channel) and the team's emoji
-- reactions on its replies. A daily summarizer (bot_self_improvement
-- .py) digests these signals into a markdown 'lessons learned'
-- snippet that gets prepended to every system prompt. Feedback loop
-- closes itself overnight.

CREATE TABLE IF NOT EXISTS slack_audit_feedback (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    -- FK to slack_bot_responses.id. NULL when feedback can't be
    -- linked to a specific response (rare — kept defensive).
    response_id     INTEGER,
    -- 'reaction' (emoji on bot post),
    -- 'thread_reply' (human posted in same thread as bot),
    -- 'audit_thread' (human posted in #ai-audit thread on bot mirror)
    feedback_type   TEXT NOT NULL,
    user_id         TEXT,
    user_name       TEXT,
    -- For reactions: emoji shortcode (e.g. '+1', '-1', 'no_entry')
    -- For thread replies: the message text
    content         TEXT NOT NULL,
    -- Polarity hint computed at ingest time:
    --   1 = positive (👍 ✅ 💯 🎉 ❤️ 🙏 etc.)
    --  -1 = negative (👎 🛑 ❌ ⛔ 😠 etc.)
    --   0 = neutral (eyes, thinking, etc., or text reply)
    is_positive     INTEGER DEFAULT 0,
    -- Slack ts of the feedback message (for dedup on re-poll)
    feedback_ts     TEXT,
    captured_at     TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_slack_audit_feedback_response
    ON slack_audit_feedback(response_id);
CREATE INDEX IF NOT EXISTS idx_slack_audit_feedback_recent
    ON slack_audit_feedback(captured_at);
-- Dedup guard for re-polled feedback events.
CREATE UNIQUE INDEX IF NOT EXISTS idx_slack_audit_feedback_unique
    ON slack_audit_feedback(response_id, feedback_type, user_id, content);

-- Daily summary of feedback patterns. Read by slack_listener at
-- compose time, prepended to the system prompt as 'TEAM FEEDBACK
-- CONTEXT'.
CREATE TABLE IF NOT EXISTS bot_lessons_learned (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    summary_date    DATE NOT NULL UNIQUE,    -- one row per day
    feedback_window_days  INTEGER NOT NULL,  -- how far back the
                                              -- summarizer looked
    feedback_count  INTEGER NOT NULL,        -- N feedback events
                                              -- considered
    summary_text    TEXT NOT NULL,           -- markdown bullets
    raw_feedback_json TEXT,                  -- the input the LLM
                                              -- saw, for audit
    generated_at    TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_bot_lessons_recent
    ON bot_lessons_learned(summary_date DESC);

-- v2.67.73 — vision-extracted dimensional data per Shopify product.
-- Most LED profiles have their cross-section dimensions baked into
-- spec-diagram PNGs in Shopify (see Slim8 example: 12.2mm × 7mm with
-- 8mm channel). CIN7's Length/Width/Height fields are largely empty.
-- extract_dimensions.py uses Claude vision to read those diagrams
-- once, caches the result here, and the AI Assistant + dimension
-- describer + Slack bot all read from this table.
CREATE TABLE IF NOT EXISTS product_dimensions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Identity (one row per Shopify product; SKUs join via handle).
    shopify_product_id  TEXT,
    shopify_handle  TEXT NOT NULL,
    family          TEXT,
    title           TEXT,
    -- Source diagram.
    source_image_url TEXT,
    source_image_position INTEGER,
    -- Cross-section dimensions (mm). Channel = LED-strip recess.
    outer_width_mm   REAL,
    outer_height_mm  REAL,
    channel_width_mm REAL,
    channel_depth_mm REAL,
    -- Wing geometry for mud-in / recessed profiles.
    wing_width_mm    REAL,
    wing_count       INTEGER,
    -- Mounting + strip-fit semantics.
    mounting_type    TEXT,   -- 'surface'|'recessed'|'mud-in'|
                              -- 'corner'|'pendant'|'unknown'
    profile_shape    TEXT,   -- 'U'|'square'|'angled'|'round'|'oval'|
                              -- 'wing'|'unknown'
    has_clip_lips    INTEGER, -- 0/1 — whether top edges grip a cover
    max_strip_width_mm REAL,
    extra_notes      TEXT,
    -- Extraction metadata.
    raw_response     TEXT,    -- full JSON from Claude vision
    confidence       TEXT,    -- 'high'|'medium'|'low' (model self-rating)
    has_diagram      INTEGER NOT NULL DEFAULT 0,
                              -- 0 if no spec diagram detected (so
                              -- we don't keep retrying empty products)
    model_used       TEXT,
    extracted_at     TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    UNIQUE(shopify_handle)
);
CREATE INDEX IF NOT EXISTS idx_product_dimensions_handle
    ON product_dimensions(shopify_handle);
CREATE INDEX IF NOT EXISTS idx_product_dimensions_family
    ON product_dimensions(family);

-- v2.67.90 — marketing intelligence tables.
-- Vision: bring SEO + email + ad + reviews data together so the
-- buyer can answer 'why did this SKU's sales spike/dip', and so
-- the AI bot can replace Triple Whale's Moby chat (cancel by
-- June 1).
--
-- Sources flowing in:
--   semrush_sync.py        -> seo_keyword_positions
--   klaviyo_sync.py        -> email_campaigns + email_campaign_skus
--   reviewsio_sync.py      -> product_reviews
--   ga4_sync.py            -> ga4_events_daily (Phase 2 after Elevar)
--   google_ads_sync.py     -> ad_campaigns_daily, ad_campaign_skus
--   meta_ads_sync.py       -> same shape as google_ads_sync (Phase 3)

-- SEO: per-keyword/URL ranking positions over time.
-- Pulled weekly from SEMrush (Guru plan API, ~10 units per
-- keyword). Also fed by per-URL Search Console data when wired up.
CREATE TABLE IF NOT EXISTS seo_keyword_positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword         TEXT NOT NULL,
    url             TEXT,                  -- the ranking URL on
                                            -- our domain
    sku             TEXT,                  -- mapped via Shopify
                                            -- handle, may be NULL
                                            -- for category pages
    family          TEXT,                  -- ditto
    position        REAL,                  -- 1.0 = top of page;
                                            -- decimals for averaged
                                            -- positions
    previous_position REAL,                -- last week's position
                                            -- for delta display
    search_volume   INTEGER,               -- monthly search volume
                                            -- per SEMrush
    serp_features   TEXT,                  -- json: featured snippet,
                                            -- people-also-ask, etc.
    source          TEXT NOT NULL DEFAULT 'semrush',
    captured_at     TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    UNIQUE(keyword, url, source, captured_at)
);
CREATE INDEX IF NOT EXISTS idx_seo_kw_sku
    ON seo_keyword_positions(sku);
CREATE INDEX IF NOT EXISTS idx_seo_kw_family
    ON seo_keyword_positions(family);
CREATE INDEX IF NOT EXISTS idx_seo_kw_recent
    ON seo_keyword_positions(captured_at DESC);

-- Email campaigns from Klaviyo.
-- Headline metrics per campaign. Tied to per-SKU click data via
-- email_campaign_skus.
CREATE TABLE IF NOT EXISTS email_campaigns (
    id              TEXT PRIMARY KEY,      -- klaviyo campaign id
    name            TEXT,
    subject         TEXT,
    sent_at         TIMESTAMP,
    list_name       TEXT,                  -- target list/segment
    recipients      INTEGER,
    delivered       INTEGER,
    opens_unique    INTEGER,
    clicks_unique   INTEGER,
    open_rate       REAL,
    click_rate      REAL,
    revenue         REAL,                  -- klaviyo's attributed
                                            -- revenue
    orders          INTEGER,               -- klaviyo's attributed
                                            -- order count
    raw_payload     TEXT,                  -- full klaviyo response
                                            -- for re-parsing
    captured_at     TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_email_campaigns_sent
    ON email_campaigns(sent_at DESC);

-- Per-SKU click data for each email campaign.
-- Klaviyo's "Clicked Email" event has the URL clicked; we resolve
-- URL -> Shopify handle -> CIN7 SKU and aggregate clicks per SKU
-- per campaign.
CREATE TABLE IF NOT EXISTS email_campaign_skus (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id     TEXT NOT NULL,         -- FK to email_campaigns
    sku             TEXT NOT NULL,
    family          TEXT,
    shopify_handle  TEXT,
    click_count     INTEGER NOT NULL DEFAULT 0,
    unique_clicks   INTEGER NOT NULL DEFAULT 0,
    attributed_revenue REAL,               -- if klaviyo attributes
                                            -- per-product revenue
    captured_at     TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    UNIQUE(campaign_id, sku),
    FOREIGN KEY(campaign_id) REFERENCES email_campaigns(id)
);
CREATE INDEX IF NOT EXISTS idx_email_camp_sku_sku
    ON email_campaign_skus(sku);
CREATE INDEX IF NOT EXISTS idx_email_camp_sku_family
    ON email_campaign_skus(family);

-- Reviews from Reviews.io, per product.
-- One row per review (so we can show recent ones / sentiment).
-- Aggregates rolled up at query time.
CREATE TABLE IF NOT EXISTS product_reviews (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id       TEXT NOT NULL UNIQUE,  -- reviews.io review id
    sku             TEXT,
    family          TEXT,
    shopify_handle  TEXT,
    shopify_product_id TEXT,
    rating          REAL NOT NULL,         -- 1.0-5.0
    title           TEXT,
    body            TEXT,
    reviewer_name   TEXT,
    reviewer_email  TEXT,
    review_date     TIMESTAMP,
    verified_buyer  INTEGER NOT NULL DEFAULT 0,
    helpful_count   INTEGER NOT NULL DEFAULT 0,
    images_json     TEXT,                  -- list of image URLs
    captured_at     TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_product_reviews_sku
    ON product_reviews(sku);
CREATE INDEX IF NOT EXISTS idx_product_reviews_family
    ON product_reviews(family);
CREATE INDEX IF NOT EXISTS idx_product_reviews_date
    ON product_reviews(review_date DESC);
CREATE INDEX IF NOT EXISTS idx_product_reviews_handle
    ON product_reviews(shopify_handle);

-- Daily ad-platform spend + outcomes per campaign.
-- Phase 2 onwards (post-Elevar). Will be populated by
-- google_ads_sync.py and meta_ads_sync.py.
CREATE TABLE IF NOT EXISTS ad_campaigns_daily (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    platform        TEXT NOT NULL,         -- 'google_ads' | 'meta'
    campaign_id     TEXT NOT NULL,
    campaign_name   TEXT,
    campaign_type   TEXT,                  -- 'search'|'shopping'|
                                            -- 'pmax'|'display'|
                                            -- 'meta_advantage'
    date            DATE NOT NULL,
    -- v2.67.107 — spend nullable so ga4_sync can INSERT rows
    -- without google_ads_sync data yet. COALESCE in upsert
    -- preserves existing value when google_ads later fills in.
    spend           REAL DEFAULT 0,
    impressions     INTEGER,
    clicks          INTEGER,
    conv_platform   REAL,                  -- platform's self-report
                                            -- conversion count
    conv_ga4        REAL,                  -- GA4-attributed conv
                                            -- (the trustworthy one)
    revenue_platform REAL,                 -- platform's self-report
    revenue_ga4     REAL,                  -- GA4-attributed
    captured_at     TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    UNIQUE(platform, campaign_id, date)
);
CREATE INDEX IF NOT EXISTS idx_ad_camp_daily_recent
    ON ad_campaigns_daily(date DESC);
CREATE INDEX IF NOT EXISTS idx_ad_camp_daily_platform
    ON ad_campaigns_daily(platform, campaign_id);

-- Per-SKU ad attribution. From GA4 ecommerce events with
-- campaign tagging.
CREATE TABLE IF NOT EXISTS ad_campaign_skus (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    platform        TEXT NOT NULL,
    campaign_id     TEXT NOT NULL,
    date            DATE NOT NULL,
    sku             TEXT NOT NULL,
    family          TEXT,
    item_views      INTEGER,
    add_to_carts    INTEGER,
    purchases       INTEGER,
    revenue         REAL,                 -- attributed revenue (GA4)
    spend           REAL,                 -- v2.67.105: per-SKU ad
                                            -- spend from Google Ads
                                            -- shopping_performance_view
    impressions     INTEGER,              -- v2.67.105
    clicks          INTEGER,              -- v2.67.105
    captured_at     TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    UNIQUE(platform, campaign_id, date, sku)
);
CREATE INDEX IF NOT EXISTS idx_ad_camp_sku_sku
    ON ad_campaign_skus(sku);
CREATE INDEX IF NOT EXISTS idx_ad_camp_sku_family
    ON ad_campaign_skus(family);

-- v2.67.152 Shipping margin alerts. When a shipment's
-- (customer-charge - actual-carrier-cost) is outside ±5% of cost
-- (with a $5 floor to ignore cheap items), the bot posts to
-- #shipping-issues asking the team to review. UNIQUE on
-- shipment_id ensures one alert per shipment regardless of how
-- many polling cycles re-scan it.
CREATE TABLE IF NOT EXISTS shipping_margin_alerts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    shipment_id         TEXT NOT NULL,    -- ShipmentID / OrderNumber
    order_number        TEXT,             -- INV-NNNN / SO-NNNN
    customer            TEXT,
    ship_date           TEXT,
    customer_charge     REAL,
    shipment_cost       REAL,
    margin_amount       REAL,             -- charge - cost (signed)
    margin_pct          REAL,             -- (charge-cost)/cost
    direction           TEXT,             -- 'under' | 'over'
    posted_channel      TEXT,
    posted_ts           TEXT,
    posted_at           TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    error_msg           TEXT,
    status              TEXT NOT NULL DEFAULT 'open',
                                              -- open | reviewed | resolved
    reviewed_by         TEXT,
    reviewed_at         TIMESTAMP,
    review_note         TEXT,
    UNIQUE(shipment_id)
);
CREATE INDEX IF NOT EXISTS idx_shipping_alerts_open
    ON shipping_margin_alerts(status, posted_at DESC)
    WHERE status = 'open';

-- v2.67.144 Stock issues tracker. When a query about stock
-- accuracy / supply lands in #stock-issues-queries, the bot
-- builds a structured intelligence block for the stock controller
-- to confirm/reject — NOT an answer to dispatch. Each issue has
-- a lifecycle: open → awaiting_response → resolved.
--
-- Two patterns we classify:
--   supply_query  — pre-dispatch question 'can we supply SO-X?'
--                   evidence sought: SO shipped via ShipStation
--   count_wrong   — discrepancy claim 'should be N, found M'
--                   evidence sought: stock_adjustments entry
--
-- Identity is the original Slack message (channel, ts) — the
-- raise. Per-SKU/per-SO detail is in stock_issue_items so a
-- single raise can track multiple items.
CREATE TABLE IF NOT EXISTS stock_issues (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    raise_channel       TEXT NOT NULL,
    raise_ts            TEXT NOT NULL,
    raise_thread_ts     TEXT,             -- the thread the bot replied into
    raised_by           TEXT,             -- Slack user_name of the raiser
    raised_text         TEXT,             -- truncated copy of the original message
    issue_type          TEXT NOT NULL,    -- 'supply_query' | 'count_wrong' | 'mixed'
    so_numbers          TEXT,             -- comma-separated SO-XXXXX
    skus                TEXT,             -- comma-separated SKUs mentioned
    families            TEXT,             -- comma-separated families
    status              TEXT NOT NULL DEFAULT 'open',
                                            -- open | awaiting_response | escalated | resolved | wont_fix
    awaiting_user       TEXT,             -- 'stockkeeper' | 'buyer' (who we DM'd)
    bot_thread_reply_ts TEXT,             -- ts of our intelligence-block reply
    dm_channel          TEXT,             -- where we DM'd for escalation
    dm_posted_ts        TEXT,
    dm_posted_at        TIMESTAMP,
    resolved_at         TIMESTAMP,
    resolved_by         TEXT,
    resolution_text     TEXT,
    created_at          TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    UNIQUE(raise_channel, raise_ts)
);
CREATE INDEX IF NOT EXISTS idx_stock_issues_open
    ON stock_issues(status, created_at DESC)
    WHERE status NOT IN ('resolved', 'wont_fix');
CREATE INDEX IF NOT EXISTS idx_stock_issues_thread
    ON stock_issues(raise_channel, raise_thread_ts);

-- v2.67.140 Back-in-stock ARRIVAL notifications. When a PO is
-- received and its line items match pending 'notify_me' demand
-- signals, the bot posts a reminder in #back-in-stock listing
-- the waiting customers. This table tracks which (PO, SKU/family,
-- demand_signal_id) combinations have already been notified
-- about so we don't spam the channel on subsequent polls.
CREATE TABLE IF NOT EXISTS back_in_stock_arrival_notifications (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    po_number           TEXT NOT NULL,
    sku                 TEXT,
    family              TEXT,
    demand_signal_id    INTEGER NOT NULL,
    posted_channel      TEXT,
    posted_ts           TEXT,
    posted_at           TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    error_msg           TEXT,
    UNIQUE(po_number, demand_signal_id),
    FOREIGN KEY (demand_signal_id) REFERENCES demand_signals(id)
);
CREATE INDEX IF NOT EXISTS idx_bis_arrivals_recent
    ON back_in_stock_arrival_notifications(posted_at DESC);

-- v2.67.138 Dropship backorder warnings. When a customer orders
-- a SKU flagged as DropShipMode='Always Drop Ship' in CIN7, the
-- system silently auto-creates a draft PO and waits for someone
-- to approve it. Without a notification, the draft sits idle and
-- the customer's order is stuck. This table records each warning
-- we've posted to #purchase-backorder so we don't double-notify.
-- Keyed by (so_number, sku) — a single SO with multiple dropship
-- lines generates one row per line. A repeat order from the same
-- customer for the same SKU is a NEW so_number → new warning.
CREATE TABLE IF NOT EXISTS dropship_backorder_warnings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    so_number           TEXT NOT NULL,
    sku                 TEXT NOT NULL,
    customer            TEXT,
    supplier            TEXT,
    quantity_ordered    REAL,
    quantity_on_hand    REAL,        -- snapshot at warning time
    posted_channel      TEXT,
    posted_ts           TEXT,
    posted_at           TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    error_msg           TEXT,
    UNIQUE(so_number, sku)
);
CREATE INDEX IF NOT EXISTS idx_dropship_warnings_recent
    ON dropship_backorder_warnings(posted_at DESC);

-- v2.67.130 PO dispatch reminders. When a PO transitions to
-- RECEIVED status and its line comments contain SO-numbers
-- (backorders the buyer flagged), we post a reminder to the
-- #fulfillment channel so the team dispatches as soon as the
-- stock arrives. PRIMARY KEY on po_number gives us idempotent
-- deduplication — even if the daily cycle runs multiple times
-- or the worker restarts mid-flight, we never double-notify.
CREATE TABLE IF NOT EXISTS po_dispatch_reminders (
    po_number           TEXT    PRIMARY KEY,
    supplier            TEXT,
    received_status     TEXT,
    so_numbers          TEXT,   -- comma-separated SO-XXXXX list
    n_sos               INTEGER DEFAULT 0,
    posted_channel      TEXT,
    posted_ts           TEXT,
    posted_at           TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    error_msg           TEXT,   -- populated if Slack post failed
    -- v2.67.131 escalation tracking. If by next day none of the
    -- SOs have shipped per ShipStation, post a follow-up reminder
    -- and stamp escalated_at so we don't escalate twice.
    escalated_at        TIMESTAMP,
    escalated_ts        TEXT,
    escalation_reason   TEXT
);
CREATE INDEX IF NOT EXISTS idx_po_dispatch_reminders_recent
    ON po_dispatch_reminders(posted_at DESC);

-- v2.67.126 Slack OAuth user tokens (for Viktor bridge from
-- dashboard). Each staff member who wants the dashboard to
-- forward marketing questions to Viktor on their behalf
-- authorises once via Slack OAuth; we store the user-scoped
-- access token here (encrypted at rest via Fernet using
-- SLACK_USER_TOKEN_ENCRYPTION_KEY env var). The dashboard then
-- posts to Slack AS the user, so Viktor sees a real human
-- message and responds (Slack apps universally filter bot
-- messages — see v2.67.125).
CREATE TABLE IF NOT EXISTS slack_user_tokens (
    user_id             INTEGER PRIMARY KEY,  -- our users.user_id
    slack_user_id       TEXT    NOT NULL,     -- their U-prefix
    slack_team_id       TEXT,
    -- Encrypted with Fernet. Empty string = revoked / cleared.
    access_token_enc    TEXT    NOT NULL,
    scopes              TEXT,                 -- comma-separated
    authed_at           TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    expires_at          TIMESTAMP,            -- null = no expiry
    last_used_at        TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
CREATE INDEX IF NOT EXISTS idx_slack_user_tokens_slack_uid
    ON slack_user_tokens(slack_user_id);

-- v2.67.211 QuickBooks Online connection. ONE row per connected
-- QBO company (realm). Tokens encrypted with Fernet. The access
-- token expires hourly; the refresh token lasts ~100 days and
-- ROTATES on every refresh — both columns get rewritten each
-- time qbo_oauth refreshes. Powers the Cashflow Management page.
CREATE TABLE IF NOT EXISTS qbo_connection (
    realm_id            TEXT PRIMARY KEY,     -- QBO company ID
    access_token_enc    TEXT NOT NULL,        -- Fernet-encrypted
    refresh_token_enc   TEXT NOT NULL,        -- Fernet-encrypted
    access_expires_at   TIMESTAMP,            -- ~1h out
    refresh_expires_at  TIMESTAMP,            -- ~100d out
    environment         TEXT DEFAULT 'sandbox',  -- sandbox|production
    connected_by        TEXT,
    connected_at        TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    updated_at          TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);

-- v2.67.257 Notion database IDs by logical name. find_or_create
-- used to look up databases by title only, which created
-- duplicates when the title search missed (rename, move, API
-- quirk). Storing the canonical ID here means we always reuse
-- the same DB once created.
CREATE TABLE IF NOT EXISTS notion_db_ids (
    name   TEXT PRIMARY KEY,    -- logical name e.g. 'slow_movers'
    db_id  TEXT NOT NULL,       -- Notion database ID
    set_at TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);

-- v2.67.250 Notion knowledge-base mirror. Operational playbooks
-- (and later: product FAQs, troubleshooting) live in Notion as
-- the team's editable source of truth; we mirror their contents
-- here so the AI Assistant can ground answers in them without
-- a slow round-trip to the Notion API on every question.
CREATE TABLE IF NOT EXISTS notion_kb_articles (
    page_id          TEXT PRIMARY KEY,   -- Notion page ID, no hyphens
    title            TEXT NOT NULL,
    content_md       TEXT,               -- rendered markdown body
    url              TEXT,
    category         TEXT,               -- e.g. 'playbook', 'product-faq'
    notion_edited_at TIMESTAMP,
    synced_at        TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);

-- v2.67.219 Cashflow Management — supplier-payables tracker.
-- One row per supplier bill. QBO-sourced rows are kept in sync
-- from QuickBooks Bills (qbo_bill_id set); manual rows (freight,
-- duty, anything not yet billed in QBO) have source='manual'.
-- James's overrides (amount_override, due_date_override) and the
-- approval workflow fields are NEVER clobbered by the QBO sync —
-- the sync only refreshes the qbo_* mirror columns.
CREATE TABLE IF NOT EXISTS cashflow_payables (
    payable_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    source            TEXT NOT NULL DEFAULT 'qbo',  -- qbo|manual
    qbo_bill_id       TEXT,                         -- QBO Bill.Id
    supplier          TEXT,
    reference         TEXT,                         -- DocNumber / PO ref
    description       TEXT,
    amount            REAL,                         -- QBO TotalAmt mirror
    currency          TEXT DEFAULT 'USD',
    invoice_date      TEXT,                         -- YYYY-MM-DD
    due_date          TEXT,                         -- YYYY-MM-DD
    qbo_balance       REAL,                         -- outstanding per QBO
    status            TEXT NOT NULL DEFAULT 'pending',
    -- status: pending | approved | scheduled | paid
    approved_by       TEXT,
    approved_at       TIMESTAMP,
    paid_date         TEXT,
    paid_amount       REAL,
    slack_ts          TEXT,                         -- approval post ts
    notes             TEXT,
    amount_override   REAL,                         -- James's override
    due_date_override TEXT,
    is_dismissed      INTEGER NOT NULL DEFAULT 0,
    created_at        TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    updated_at        TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    updated_by        TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cashflow_payables_bill
    ON cashflow_payables(qbo_bill_id);

-- v2.67.219 Cashflow Management — 53-week forecast grid. A
-- key-value cell store: one row per (week, line-item). row_key
-- is an app-defined category ('forecast_sales', 'google_ads',
-- 'rent', 'correction', ...). Editable in the dashboard.
CREATE TABLE IF NOT EXISTS cashflow_forecast (
    week_start  TEXT NOT NULL,    -- YYYY-MM-DD week anchor
    row_key     TEXT NOT NULL,
    amount      REAL,
    updated_at  TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    updated_by  TEXT,
    PRIMARY KEY (week_start, row_key)
);

-- v2.67.234 Cashflow scenarios — named what-if copies of the
-- forecast. The 'base' forecast lives in cashflow_forecast above;
-- each named scenario gets its own cell set here (cloned from
-- base on creation, then edited freely without touching base).
CREATE TABLE IF NOT EXISTS cashflow_scenarios (
    name        TEXT PRIMARY KEY,
    created_by  TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS cashflow_scenario_forecast (
    scenario    TEXT NOT NULL,
    week_start  TEXT NOT NULL,
    row_key     TEXT NOT NULL,
    amount      REAL,
    updated_at  TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    updated_by  TEXT,
    PRIMARY KEY (scenario, week_start, row_key)
);
-- Custom (user-added) forecast line items. scenario='base' for
-- rows added to the live forecast; a scenario name for rows that
-- exist only in that what-if.
CREATE TABLE IF NOT EXISTS cashflow_custom_rows (
    scenario    TEXT NOT NULL DEFAULT 'base',
    row_key     TEXT NOT NULL,
    label       TEXT NOT NULL,
    kind        TEXT NOT NULL DEFAULT 'outflow',  -- inflow|outflow
    sort_order  INTEGER NOT NULL DEFAULT 100,
    created_at  TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (scenario, row_key)
);

-- v2.67.235 Cashflow loans — private / term loan register. The
-- amortization schedule is computed deterministically from these
-- params by loan_amortization.py (Actual/365 simple interest).
CREATE TABLE IF NOT EXISTS cashflow_loans (
    loan_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    lender             TEXT NOT NULL,
    principal          REAL NOT NULL,
    apr                REAL NOT NULL,        -- annual %, e.g. 6.5
    start_date         TEXT NOT NULL,        -- YYYY-MM-DD
    first_payment_date TEXT NOT NULL,
    monthly_payment    REAL NOT NULL,
    forecast_row_key   TEXT,                 -- forecast row to feed
    notes              TEXT,
    active             INTEGER NOT NULL DEFAULT 1,
    created_at         TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    updated_at         TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);

-- v2.67.126 Viktor bridge sessions. When the dashboard's AI
-- Assistant posts to Slack on a user's behalf to forward a
-- marketing question to Viktor, we record the post here so we
-- can poll for Viktor's reply and return it to the dashboard.
CREATE TABLE IF NOT EXISTS viktor_bridge_sessions (
    session_id          TEXT PRIMARY KEY,     -- UUID, generated client-side
    user_id             INTEGER NOT NULL,     -- our user who asked
    question            TEXT NOT NULL,
    channel_id          TEXT NOT NULL,        -- where we posted
    posted_ts           TEXT,                 -- Slack ts of our post
    thread_ts           TEXT,                 -- thread Viktor will reply in
    viktor_reply_ts     TEXT,                 -- once detected
    viktor_reply_text   TEXT,                 -- pulled from slack_messages
    overlay_text        TEXT,                 -- our engine-signal addendum
    status              TEXT NOT NULL DEFAULT 'pending',
                                              -- pending / replied / timeout / error
    created_at          TIMESTAMP NOT NULL DEFAULT (datetime('now')),
    completed_at        TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_viktor_bridge_pending
    ON viktor_bridge_sessions(status)
    WHERE status = 'pending';

-- v2.67.118 Google Merchant Center: per-product feed status.
-- One row per SKU (offer_id) — overwrites each sync. Tracks
-- whether the SKU is approved, disapproved, or has warnings on
-- each destination (Shopping ads, free listings).
CREATE TABLE IF NOT EXISTS product_feed_status (
    sku                 TEXT PRIMARY KEY,
    offer_id            TEXT,
    family              TEXT,
    shopify_handle      TEXT,
    title               TEXT,
    -- Per-destination roll-ups: 'approved', 'disapproved',
    -- 'pending', 'eligible', 'not_eligible' or '' if absent.
    ads_status          TEXT,
    free_listings_status TEXT,
    -- JSON array of issue dicts:
    --   [{code, severity, destination, description, detail, url}]
    issues_json         TEXT,
    n_issues            INTEGER DEFAULT 0,
    n_errors            INTEGER DEFAULT 0,
    n_warnings          INTEGER DEFAULT 0,
    last_checked        TIMESTAMP NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_product_feed_status_family
    ON product_feed_status(family);
CREATE INDEX IF NOT EXISTS idx_product_feed_status_ads_status
    ON product_feed_status(ads_status);
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


def _migrate_supplier_cadence(conn: sqlite3.Connection) -> None:
    """v2.67.283 — add order_cadence_days to supplier_config. The
    real interval between reorders for this supplier (e.g. 7 when
    ordered weekly). When set, it overrides the ABC-class
    review_days in the reorder engine — each order then only needs
    to bridge to the NEXT order, not a generic 30-45 days, which is
    the single biggest lever for freeing cash tied up in stock."""
    try:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info('supplier_config')").fetchall()}
        if "order_cadence_days" not in cols:
            conn.execute(
                "ALTER TABLE supplier_config ADD COLUMN "
                "order_cadence_days INTEGER")
    except sqlite3.Error:
        pass


def _migrate_product_aliases_multi_target(conn: sqlite3.Connection) -> None:
    """v2.63: extend product_aliases beyond a single (sku XOR family)
    target so a phrase can map to multiple SKUs, multiple families, or
    a captured attribute filter.

    Adds (idempotent ADD COLUMN):
      - rule_type           TEXT   (values: 'sku' | 'sku_list' | 'family'
                                    | 'family_list' | 'attributes' |
                                    'mixed' | NULL for legacy single-target
                                    rows)
      - target_skus_json    TEXT   JSON list of SKUs (or NULL)
      - target_families_json TEXT  JSON list of families (or NULL)
      - attributes_json     TEXT   JSON object (or NULL) — captured for
                                    later use when product_attributes
                                    (A1) ships; alias-before-LLM hint
                                    surfaces it but the system can't
                                    enforce attribute filters yet.

    Backfill: any pre-v2.63 row keeps its original sku / product_family
    columns AND gets rule_type set to 'sku' (if sku is non-null) or
    'family' (if product_family is non-null). The new JSON columns are
    left NULL — readers consult sku/product_family first, then
    target_skus_json / target_families_json. find_alias_in_question
    handles both shapes transparently.
    """
    try:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info('product_aliases')").fetchall()}
        if "rule_type" not in cols:
            conn.execute(
                "ALTER TABLE product_aliases ADD COLUMN rule_type TEXT")
            # Backfill: classify legacy rows so future readers can
            # route uniformly on rule_type.
            conn.execute(
                "UPDATE product_aliases SET rule_type = 'sku' "
                "WHERE rule_type IS NULL AND sku IS NOT NULL "
                "AND TRIM(sku) != ''")
            conn.execute(
                "UPDATE product_aliases SET rule_type = 'family' "
                "WHERE rule_type IS NULL AND product_family IS NOT NULL "
                "AND TRIM(product_family) != ''")
        if "target_skus_json" not in cols:
            conn.execute(
                "ALTER TABLE product_aliases ADD COLUMN "
                "target_skus_json TEXT")
        if "target_families_json" not in cols:
            conn.execute(
                "ALTER TABLE product_aliases ADD COLUMN "
                "target_families_json TEXT")
        if "attributes_json" not in cols:
            conn.execute(
                "ALTER TABLE product_aliases ADD COLUMN "
                "attributes_json TEXT")
        # Provenance columns (v2.63 spec): track WHERE the rule came
        # from (manual / feedback / system) and WHO created it (separate
        # from approved_by which captures who signed off — usually the
        # same person but the model leaves room for future split).
        if "source" not in cols:
            conn.execute(
                "ALTER TABLE product_aliases ADD COLUMN source TEXT")
            # Backfill: any legacy row is assumed manual unless an AI
            # marker is detectable in approved_by.
            conn.execute(
                "UPDATE product_aliases "
                "SET source = CASE "
                "  WHEN LOWER(COALESCE(approved_by, '')) IN ('ai', "
                "       'system', 'alias_lookup') THEN 'system' "
                "  ELSE 'manual' END "
                "WHERE source IS NULL")
        if "created_by" not in cols:
            conn.execute(
                "ALTER TABLE product_aliases ADD COLUMN created_by TEXT")
            # Default created_by = approved_by for legacy rows.
            conn.execute(
                "UPDATE product_aliases SET created_by = approved_by "
                "WHERE created_by IS NULL")
        # v2.64: text_search rule type — phrase becomes a text filter
        # across product title / description / tags / product_type /
        # collections. Stored as JSON list of field names.
        if "search_fields_json" not in cols:
            conn.execute(
                "ALTER TABLE product_aliases ADD COLUMN "
                "search_fields_json TEXT")
    except sqlite3.Error:
        pass


def _migrate_demand_signal_match_columns(conn: sqlite3.Connection) -> None:
    """v2.61: split SKU lineage and persist auto-reconciler match metadata.

    Adds five nullable columns to demand_signals and backfills detected_sku
    from the existing sku for any pre-v2.61 row that doesn't have it yet.

    - detected_sku        TEXT  — what was originally captured (immutable
                                  source-of-truth for what the signal-capturer
                                  thought the SKU was)
    - matched_order_number TEXT — populated by reconcile_demand_signals when
                                  a HIGH-confidence sale match is found
    - matched_sale_date    TEXT — sale's OrderDate at the moment we matched
    - matched_sale_line_id TEXT — synthetic id (SaleID:ProductID) so we can
                                  later trace each conversion back to a sale
    - match_confidence     TEXT — 'high' / 'medium' / 'low' / NULL.
                                  HIGH = exact SKU + exact customer match
                                  MEDIUM = exact SKU + signal had no
                                           customer_id, sale exists to *some*
                                           customer (flagged needs_review,
                                           NOT auto-converted)
                                  LOW = future (family-only match)

    `sku` remains the canonical column the matcher reads — it equals the
    approved value if the buyer corrected it via the review page, otherwise
    a copy of detected_sku from insert time.
    """
    try:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info('demand_signals')").fetchall()}

        if "detected_sku" not in cols:
            conn.execute(
                "ALTER TABLE demand_signals ADD COLUMN detected_sku TEXT")
            # Backfill: original captured value == current sku for any row
            # that pre-dates this migration. (For rows captured post-v2.61
            # the insert helper sets both at the same time.)
            conn.execute(
                "UPDATE demand_signals SET detected_sku = sku "
                "WHERE detected_sku IS NULL AND sku IS NOT NULL")

        if "matched_order_number" not in cols:
            conn.execute(
                "ALTER TABLE demand_signals ADD COLUMN "
                "matched_order_number TEXT")
        if "matched_sale_date" not in cols:
            conn.execute(
                "ALTER TABLE demand_signals ADD COLUMN matched_sale_date TEXT")
        if "matched_sale_line_id" not in cols:
            conn.execute(
                "ALTER TABLE demand_signals ADD COLUMN "
                "matched_sale_line_id TEXT")
        if "match_confidence" not in cols:
            conn.execute(
                "ALTER TABLE demand_signals ADD COLUMN match_confidence TEXT")
    except sqlite3.Error:
        pass


def _migrate_ad_campaign_skus_spend(conn: sqlite3.Connection) -> None:
    """v2.67.105 — add per-SKU spend tracking columns to existing
    ad_campaign_skus tables. Original v2.67.90 schema only had
    revenue (from GA4). Now we also pull spend from Google Ads'
    shopping_performance_view so the dashboard can show per-SKU
    ROAS, not just per-SKU revenue."""
    try:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info('ad_campaign_skus')").fetchall()}
        if not cols:
            return  # table doesn't exist yet (fresh DB)
        if "spend" not in cols:
            conn.execute(
                "ALTER TABLE ad_campaign_skus ADD COLUMN spend REAL")
        if "impressions" not in cols:
            conn.execute(
                "ALTER TABLE ad_campaign_skus ADD COLUMN "
                "impressions INTEGER")
        if "clicks" not in cols:
            conn.execute(
                "ALTER TABLE ad_campaign_skus ADD COLUMN clicks "
                "INTEGER")
    except sqlite3.Error:
        pass


def _migrate_po_dispatch_reminders_escalation(
        conn: sqlite3.Connection) -> None:
    """v2.67.131 — add escalation columns to existing
    po_dispatch_reminders tables. Tracks whether we've sent a
    'STILL not shipped' follow-up message after 24h."""
    try:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info('po_dispatch_reminders')").fetchall()}
        if not cols:
            return  # table doesn't exist yet (fresh DB schema
                      # already includes the columns)
        if "escalated_at" not in cols:
            conn.execute(
                "ALTER TABLE po_dispatch_reminders ADD COLUMN "
                "escalated_at TIMESTAMP")
        if "escalated_ts" not in cols:
            conn.execute(
                "ALTER TABLE po_dispatch_reminders ADD COLUMN "
                "escalated_ts TEXT")
        if "escalation_reason" not in cols:
            conn.execute(
                "ALTER TABLE po_dispatch_reminders ADD COLUMN "
                "escalation_reason TEXT")
    except sqlite3.Error:
        pass


def _migrate_ad_campaign_skus_free_listings(
        conn: sqlite3.Connection) -> None:
    """v2.67.118 — add free-listing performance columns from Google
    Merchant Center to existing ad_campaign_skus tables. Free
    listings are Google Shopping's organic (non-paid) surface;
    Merchant Center reports clicks + impressions on them per
    offer_id (≈ SKU). google_ads_sync owns paid spend/clicks;
    merchant_sync owns free_listing_*."""
    try:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info('ad_campaign_skus')").fetchall()}
        if not cols:
            return  # table doesn't exist yet (fresh DB)
        if "free_listing_clicks" not in cols:
            conn.execute(
                "ALTER TABLE ad_campaign_skus ADD COLUMN "
                "free_listing_clicks INTEGER")
        if "free_listing_impressions" not in cols:
            conn.execute(
                "ALTER TABLE ad_campaign_skus ADD COLUMN "
                "free_listing_impressions INTEGER")
    except sqlite3.Error:
        pass


def _migrate_ad_campaigns_daily_drop_spend_notnull(
        conn: sqlite3.Connection) -> None:
    """v2.67.107 — rebuild ad_campaigns_daily without NOT NULL on
    spend. ga4_sync was failing with 'NOT NULL constraint failed:
    ad_campaigns_daily.spend' because it correctly passes None
    for fields it doesn't own (so COALESCE preserves
    google_ads_sync's value on UPDATE) — but the INSERT path was
    blocked by the constraint when no google_ads row existed yet.

    SQLite can't ALTER COLUMN; we rebuild the table preserving
    data + indexes."""
    try:
        cols_info = conn.execute(
            "PRAGMA table_info('ad_campaigns_daily')").fetchall()
        if not cols_info:
            return  # fresh DB, schema created without NOT NULL
        spend_col = next(
            (c for c in cols_info if c[1] == "spend"), None)
        if not spend_col or spend_col[3] == 0:
            return  # already nullable

        # Rebuild — preserve all existing data + indexes.
        conn.executescript("""
            BEGIN;
            CREATE TABLE ad_campaigns_daily_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                campaign_name TEXT,
                campaign_type TEXT,
                date DATE NOT NULL,
                spend REAL DEFAULT 0,
                impressions INTEGER,
                clicks INTEGER,
                conv_platform REAL,
                conv_ga4 REAL,
                revenue_platform REAL,
                revenue_ga4 REAL,
                captured_at TIMESTAMP NOT NULL DEFAULT
                  (datetime('now')),
                UNIQUE(platform, campaign_id, date)
            );
            INSERT INTO ad_campaigns_daily_new
              SELECT id, platform, campaign_id, campaign_name,
                     campaign_type, date, spend, impressions,
                     clicks, conv_platform, conv_ga4,
                     revenue_platform, revenue_ga4, captured_at
              FROM ad_campaigns_daily;
            DROP TABLE ad_campaigns_daily;
            ALTER TABLE ad_campaigns_daily_new
              RENAME TO ad_campaigns_daily;
            CREATE INDEX IF NOT EXISTS idx_ad_camp_daily_recent
                ON ad_campaigns_daily(date DESC);
            CREATE INDEX IF NOT EXISTS idx_ad_camp_daily_platform
                ON ad_campaigns_daily(platform, campaign_id);
            COMMIT;
        """)
    except sqlite3.Error:
        # Best-effort migration. If it fails, the next
        # connection will retry.
        try:
            conn.execute("ROLLBACK")
        except Exception:
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
# Vision-extracted product dimensions (v2.67.73)
# ---------------------------------------------------------------------------

def upsert_product_dimensions(row: dict) -> int:
    """Insert or replace one product_dimensions row keyed on
    shopify_handle. Returns the row id."""
    cols = (
        "shopify_product_id", "shopify_handle", "family", "title",
        "source_image_url", "source_image_position",
        "outer_width_mm", "outer_height_mm",
        "channel_width_mm", "channel_depth_mm",
        "wing_width_mm", "wing_count",
        "mounting_type", "profile_shape", "has_clip_lips",
        "max_strip_width_mm", "extra_notes",
        "raw_response", "confidence", "has_diagram",
        "model_used", "extracted_at",
    )
    values = [row.get(c) for c in cols]
    placeholders = ",".join("?" for _ in cols)
    col_list = ",".join(cols)
    sql = (
        f"INSERT INTO product_dimensions ({col_list}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT(shopify_handle) DO UPDATE SET "
        + ",".join(f"{c}=excluded.{c}" for c in cols)
    )
    with connect() as c:
        cur = c.execute(sql, values)
        return int(cur.lastrowid or 0)


def get_product_dimensions(shopify_handle: str) -> Optional[dict]:
    """Return the product_dimensions row for a Shopify handle, or
    None if no extraction has run yet."""
    with connect() as c:
        r = c.execute(
            "SELECT * FROM product_dimensions WHERE shopify_handle = ?",
            (shopify_handle,)
        ).fetchone()
    return dict(r) if r else None


def all_product_dimensions() -> list:
    """Return every product_dimensions row as list of dicts. Used by
    dimension_describer.py to enrich the per-SKU CSV."""
    with connect() as c:
        rows = c.execute("SELECT * FROM product_dimensions").fetchall()
    return [dict(r) for r in rows]


def search_product_dimensions(query: str, limit: int = 10) -> list:
    """Fuzzy lookup of product_dimensions by Shopify handle, product
    title, or SKU family. Powers the AI assistant's
    get_product_dimensions tool. Rows with a real spec diagram rank
    first so the best data surfaces at the top."""
    q = f"%{(query or '').strip().lower()}%"
    with connect() as c:
        rows = c.execute(
            "SELECT * FROM product_dimensions "
            "WHERE LOWER(shopify_handle) LIKE ? "
            "   OR LOWER(title) LIKE ? "
            "   OR LOWER(COALESCE(family, '')) LIKE ? "
            "ORDER BY has_diagram DESC, title "
            "LIMIT ?", (q, q, q, limit)).fetchall()
    return [dict(r) for r in rows]


def product_dimensions_handles() -> set:
    """Return set of Shopify handles already extracted (for skip-
    if-cached logic in extract_dimensions.py)."""
    with connect() as c:
        rows = c.execute(
            "SELECT shopify_handle FROM product_dimensions"
        ).fetchall()
    return {r["shopify_handle"] for r in rows}


def product_dimensions_no_diagram_handles() -> set:
    """Return set of Shopify handles where the first extraction
    found no diagram. Used by --retry-no-diagram to selectively
    re-process those rows (e.g. with more images per call)."""
    with connect() as c:
        rows = c.execute(
            "SELECT shopify_handle FROM product_dimensions "
            "WHERE has_diagram = 0"
        ).fetchall()
    return {r["shopify_handle"] for r in rows}


# ---------------------------------------------------------------------------
# Marketing intelligence helpers (v2.67.90)
# ---------------------------------------------------------------------------

def upsert_seo_keyword_position(row: dict) -> int:
    """Insert one SEMrush ranking observation. Idempotent on
    (keyword, url, source, captured_at)."""
    cols = ("keyword", "url", "sku", "family", "position",
              "previous_position", "search_volume", "serp_features",
              "source", "captured_at")
    values = [row.get(c) for c in cols]
    sql = (f"INSERT OR IGNORE INTO seo_keyword_positions "
             f"({','.join(cols)}) VALUES "
             f"({','.join('?' for _ in cols)})")
    with connect() as c:
        cur = c.execute(sql, values)
        return int(cur.lastrowid or 0)


def get_seo_signals_for_sku(sku: str, days: int = 30) -> list:
    """Return SEO ranking observations for a SKU in the last N days."""
    sql = (
        "SELECT * FROM seo_keyword_positions "
        "WHERE sku = ? "
        "  AND captured_at >= datetime('now', '-' || ? || ' days') "
        "ORDER BY captured_at DESC, position ASC")
    with connect() as c:
        rows = c.execute(sql, (sku, days)).fetchall()
    return [dict(r) for r in rows]


def get_seo_signals_for_family(family: str, days: int = 30) -> list:
    sql = (
        "SELECT * FROM seo_keyword_positions "
        "WHERE family = ? "
        "  AND captured_at >= datetime('now', '-' || ? || ' days') "
        "ORDER BY captured_at DESC, position ASC")
    with connect() as c:
        rows = c.execute(sql, (family, days)).fetchall()
    return [dict(r) for r in rows]


def upsert_email_campaign(row: dict) -> int:
    """Insert or replace an email campaign. Keyed on klaviyo id."""
    cols = ("id", "name", "subject", "sent_at", "list_name",
              "recipients", "delivered", "opens_unique",
              "clicks_unique", "open_rate", "click_rate",
              "revenue", "orders", "raw_payload", "captured_at")
    values = [row.get(c) for c in cols]
    sql = (
        f"INSERT INTO email_campaigns ({','.join(cols)}) "
        f"VALUES ({','.join('?' for _ in cols)}) "
        f"ON CONFLICT(id) DO UPDATE SET "
        + ",".join(f"{c}=excluded.{c}" for c in cols if c != "id"))
    with connect() as c:
        cur = c.execute(sql, values)
        return int(cur.lastrowid or 0)


def upsert_email_campaign_sku(row: dict) -> int:
    cols = ("campaign_id", "sku", "family", "shopify_handle",
              "click_count", "unique_clicks", "attributed_revenue",
              "captured_at")
    values = [row.get(c) for c in cols]
    sql = (
        f"INSERT INTO email_campaign_skus ({','.join(cols)}) "
        f"VALUES ({','.join('?' for _ in cols)}) "
        f"ON CONFLICT(campaign_id, sku) DO UPDATE SET "
        + ",".join(f"{c}=excluded.{c}"
                     for c in cols if c not in ("campaign_id", "sku")))
    with connect() as c:
        cur = c.execute(sql, values)
        return int(cur.lastrowid or 0)


def get_email_attribution_for_sku(sku: str, days: int = 90) -> list:
    """Return email campaigns that drove clicks/revenue on this SKU."""
    sql = (
        "SELECT ec.id, ec.name, ec.subject, ec.sent_at, "
        "       ec.recipients, ec.open_rate, ec.click_rate, "
        "       ec.revenue AS campaign_revenue, "
        "       ecs.click_count, ecs.unique_clicks, "
        "       ecs.attributed_revenue AS sku_revenue "
        "FROM email_campaign_skus ecs "
        "JOIN email_campaigns ec ON ec.id = ecs.campaign_id "
        "WHERE ecs.sku = ? "
        "  AND ec.sent_at >= datetime('now', '-' || ? || ' days') "
        "ORDER BY ec.sent_at DESC")
    with connect() as c:
        rows = c.execute(sql, (sku, days)).fetchall()
    return [dict(r) for r in rows]


def upsert_product_review(row: dict) -> int:
    """Insert one review. Idempotent on review_id."""
    cols = ("review_id", "sku", "family", "shopify_handle",
              "shopify_product_id", "rating", "title", "body",
              "reviewer_name", "reviewer_email", "review_date",
              "verified_buyer", "helpful_count", "images_json",
              "captured_at")
    values = [row.get(c) for c in cols]
    sql = (
        f"INSERT INTO product_reviews ({','.join(cols)}) "
        f"VALUES ({','.join('?' for _ in cols)}) "
        f"ON CONFLICT(review_id) DO UPDATE SET "
        + ",".join(f"{c}=excluded.{c}"
                     for c in cols if c != "review_id"))
    with connect() as c:
        cur = c.execute(sql, values)
        return int(cur.lastrowid or 0)


def get_reviews_summary_for_sku(sku: str) -> dict:
    """Aggregate review stats for a SKU."""
    sql = (
        "SELECT COUNT(*) as count, "
        "       AVG(rating) as avg_rating, "
        "       SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) "
        "         as low_count, "
        "       SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) "
        "         as high_count, "
        "       MAX(review_date) as latest_review "
        "FROM product_reviews WHERE sku = ?")
    with connect() as c:
        r = c.execute(sql, (sku,)).fetchone()
    return dict(r) if r else {}


def get_recent_reviews_for_sku(sku: str, limit: int = 5) -> list:
    sql = (
        "SELECT * FROM product_reviews WHERE sku = ? "
        "ORDER BY review_date DESC LIMIT ?")
    with connect() as c:
        rows = c.execute(sql, (sku, limit)).fetchall()
    return [dict(r) for r in rows]


def upsert_ad_campaign_daily(row: dict) -> int:
    """v2.67.101 — COALESCE so each sync only updates the fields
    it actually OWNS. Fixes the bug where ga4_sync was clobbering
    google_ads_sync's spend with 0.0 (spend was $287 instead of
    the real ~$45k for 30 days).

    Each sync passes None for fields owned by the other:
      google_ads_sync owns: spend, impressions, clicks,
                              conv_platform, revenue_platform
      ga4_sync owns:        conv_ga4, revenue_ga4

    Both can write campaign_name / type / captured_at.
    COALESCE(new, existing) keeps the existing value when new is
    NULL — exactly the merge semantics we need."""
    cols = ("platform", "campaign_id", "campaign_name", "campaign_type",
              "date", "spend", "impressions", "clicks",
              "conv_platform", "conv_ga4",
              "revenue_platform", "revenue_ga4", "captured_at")
    values = [row.get(c) for c in cols]
    # v2.67.172 — Qualify the existing-row column with the table
    # name (ad_campaigns_daily.col). Postgres rejects an
    # unqualified column reference in DO UPDATE SET because both
    # EXCLUDED.col and the target row's col share the same name
    # → ambiguous. SQLite tolerated it. Works in both backends
    # when qualified.
    sql = (
        f"INSERT INTO ad_campaigns_daily ({','.join(cols)}) "
        f"VALUES ({','.join('?' for _ in cols)}) "
        f"ON CONFLICT(platform, campaign_id, date) DO UPDATE SET "
        + ",".join(
            f"{c}=COALESCE(excluded.{c}, ad_campaigns_daily.{c})"
            for c in cols
            if c not in ("platform", "campaign_id", "date")))
    with connect() as c:
        cur = c.execute(sql, values)
        return int(cur.lastrowid or 0)


def upsert_ad_campaign_sku(row: dict) -> int:
    """v2.67.118 — adds free_listing_clicks/free_listing_impressions
    columns. v2.67.105 — adds spend/impressions/clicks columns +
    COALESCE so each sync only updates fields it owns:
      ga4_sync owns:        item_views, add_to_carts,
                              purchases, revenue
      google_ads_sync owns: spend, impressions, clicks (per-SKU
                              from shopping_performance_view)
      merchant_sync owns:   free_listing_clicks,
                              free_listing_impressions
    Both can write platform / family / captured_at.

    Merchant Center free-listing data is account-level (not tied
    to a Google Ads campaign), so merchant_sync writes rows with
    platform='google_merchant' and campaign_id='free_listings' —
    a synthetic identifier that keeps the per-SKU/date UNIQUE
    constraint intact and clearly distinguishes free-listing
    rows from paid Shopping rows."""
    cols = ("platform", "campaign_id", "date", "sku", "family",
              "item_views", "add_to_carts", "purchases", "revenue",
              "spend", "impressions", "clicks",
              "free_listing_clicks", "free_listing_impressions",
              "captured_at")
    values = [row.get(c) for c in cols]
    # v2.67.172 — see upsert_ad_campaign_daily; same Postgres
    # ambiguity rule applies here.
    sql = (
        f"INSERT INTO ad_campaign_skus ({','.join(cols)}) "
        f"VALUES ({','.join('?' for _ in cols)}) "
        f"ON CONFLICT(platform, campaign_id, date, sku) "
        f"DO UPDATE SET "
        + ",".join(
            f"{c}=COALESCE(excluded.{c}, ad_campaign_skus.{c})"
            for c in cols
            if c not in ("platform", "campaign_id",
                            "date", "sku")))
    with connect() as c:
        cur = c.execute(sql, values)
        return int(cur.lastrowid or 0)


def upsert_product_feed_status(row: dict) -> int:
    """v2.67.118 — Google Merchant Center feed status per SKU.
    Overwrites on every sync; we always want the latest state."""
    cols = ("sku", "offer_id", "family", "shopify_handle", "title",
              "ads_status", "free_listings_status", "issues_json",
              "n_issues", "n_errors", "n_warnings", "last_checked")
    values = [row.get(c) for c in cols]
    sql = (
        f"INSERT INTO product_feed_status ({','.join(cols)}) "
        f"VALUES ({','.join('?' for _ in cols)}) "
        f"ON CONFLICT(sku) DO UPDATE SET "
        + ",".join(f"{c}=excluded.{c}" for c in cols if c != "sku"))
    with connect() as c:
        cur = c.execute(sql, values)
        return int(cur.lastrowid or 0)


def get_disapproved_skus(limit: int = 100) -> list:
    """v2.67.118 — SKUs Google rejected from Shopping ads. The
    high-ROI list: every SKU here is paying $0 but COULD be ranking
    if the issue is fixed."""
    sql = (
        "SELECT sku, family, title, shopify_handle, ads_status, "
        "       free_listings_status, n_issues, n_errors, "
        "       issues_json, last_checked "
        "FROM product_feed_status "
        "WHERE ads_status = 'disapproved' "
        "   OR ads_status = 'not_eligible' "
        "ORDER BY n_errors DESC, sku ASC "
        "LIMIT ?")
    with connect() as c:
        rows = c.execute(sql, (limit,)).fetchall()
    return [dict(r) for r in rows]


def get_feed_status_summary() -> dict:
    """v2.67.118 — counts by ads_status. Used by the Ad-Umpire
    health header: 'X SKUs approved, Y disapproved, Z pending'."""
    sql = (
        "SELECT ads_status, COUNT(*) AS n "
        "FROM product_feed_status "
        "GROUP BY ads_status")
    with connect() as c:
        rows = c.execute(sql).fetchall()
    out = {"total": 0}
    for r in rows:
        status = (r["ads_status"] or "unknown").lower()
        out[status] = int(r["n"] or 0)
        out["total"] += out[status]
    return out


# ---------------------------------------------------------------------------
# Shipping margin alerts (v2.67.152)
# ---------------------------------------------------------------------------
def has_shipping_margin_alert(shipment_id: str) -> bool:
    """Idempotency check — UNIQUE constraint guarantees one row
    per shipment but this avoids the round-trip on duplicates."""
    if not shipment_id:
        return False
    with connect() as c:
        r = c.execute(
            "SELECT 1 FROM shipping_margin_alerts "
            "WHERE shipment_id = ?", (shipment_id,)).fetchone()
    return r is not None


def record_shipping_margin_alert(*,
                                          shipment_id: str,
                                          order_number: Optional[str],
                                          customer: Optional[str],
                                          ship_date: Optional[str],
                                          customer_charge: Optional[float],
                                          shipment_cost: Optional[float],
                                          margin_amount: Optional[float],
                                          margin_pct: Optional[float],
                                          direction: str,
                                          posted_channel: Optional[str],
                                          posted_ts: Optional[str],
                                          error_msg: Optional[str] = None
                                          ) -> None:
    with connect() as c:
        c.execute(
            "INSERT OR IGNORE INTO shipping_margin_alerts "
            "(shipment_id, order_number, customer, ship_date, "
            " customer_charge, shipment_cost, margin_amount, "
            " margin_pct, direction, posted_channel, posted_ts, "
            " error_msg) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (shipment_id, order_number, customer, ship_date,
              customer_charge, shipment_cost, margin_amount,
              margin_pct, direction, posted_channel, posted_ts,
              error_msg))


def list_open_shipping_margin_alerts(limit: int = 50) -> list:
    with connect() as c:
        rows = c.execute(
            "SELECT id, shipment_id, order_number, customer, "
            "       ship_date, customer_charge, shipment_cost, "
            "       margin_amount, margin_pct, direction, "
            "       posted_at, status "
            "FROM shipping_margin_alerts "
            "WHERE status = 'open' "
            "ORDER BY ABS(margin_amount) DESC, posted_at DESC "
            "LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]


def resolve_shipping_margin_alert(alert_id: int,
                                          reviewed_by: str,
                                          review_note: str) -> None:
    with connect() as c:
        c.execute(
            "UPDATE shipping_margin_alerts SET "
            "  status = 'reviewed', "
            "  reviewed_at = datetime('now'), "
            "  reviewed_by = ?, review_note = ? "
            "WHERE id = ?",
            (reviewed_by, (review_note or "")[:300], alert_id))


# ---------------------------------------------------------------------------
# Stock issues tracker (v2.67.144)
# ---------------------------------------------------------------------------
def upsert_stock_issue(*, raise_channel: str, raise_ts: str,
                            raise_thread_ts: Optional[str] = None,
                            raised_by: Optional[str] = None,
                            raised_text: Optional[str] = None,
                            issue_type: str = "supply_query",
                            so_numbers: Optional[List[str]] = None,
                            skus: Optional[List[str]] = None,
                            families: Optional[List[str]] = None,
                            ) -> int:
    """Create a new stock_issues row or return the existing id if
    one already exists for this (raise_channel, raise_ts) pair.
    Idempotent — listener can re-process the same message safely."""
    with connect() as c:
        existing = c.execute(
            "SELECT id FROM stock_issues "
            "WHERE raise_channel = ? AND raise_ts = ?",
            (raise_channel, raise_ts)).fetchone()
        if existing:
            return int(existing["id"])
        cur = c.execute(
            "INSERT INTO stock_issues "
            "(raise_channel, raise_ts, raise_thread_ts, raised_by, "
            " raised_text, issue_type, so_numbers, skus, families, "
            " status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')",
            (raise_channel, raise_ts, raise_thread_ts, raised_by,
              (raised_text or "")[:1000], issue_type,
              ",".join(so_numbers or []),
              ",".join(skus or []),
              ",".join(families or [])))
        return int(cur.lastrowid)


def update_stock_issue_bot_reply(issue_id: int,
                                          bot_thread_reply_ts: str
                                          ) -> None:
    with connect() as c:
        c.execute(
            "UPDATE stock_issues SET "
            "  bot_thread_reply_ts = ?, "
            "  status = 'awaiting_response' "
            "WHERE id = ?",
            (bot_thread_reply_ts, issue_id))


def update_stock_issue_dm(issue_id: int,
                                 dm_channel: str, dm_posted_ts: str,
                                 awaiting_user: str = "stockkeeper"
                                 ) -> None:
    with connect() as c:
        c.execute(
            "UPDATE stock_issues SET "
            "  dm_channel = ?, dm_posted_ts = ?, "
            "  dm_posted_at = datetime('now'), "
            "  awaiting_user = ?, status = 'escalated' "
            "WHERE id = ?",
            (dm_channel, dm_posted_ts, awaiting_user, issue_id))


def resolve_stock_issue(issue_id: int, resolved_by: str,
                              resolution_text: str) -> None:
    with connect() as c:
        c.execute(
            "UPDATE stock_issues SET "
            "  status = 'resolved', "
            "  resolved_at = datetime('now'), "
            "  resolved_by = ?, resolution_text = ? "
            "WHERE id = ?",
            (resolved_by, (resolution_text or "")[:500], issue_id))


def acknowledge_stock_issue(issue_id: int, ack_by: str,
                                  ack_text: str) -> None:
    """v2.67.247 — mark a stock issue ACKNOWLEDGED by a human
    reply that wasn't a strict resolution keyword. The morning
    summary excludes acknowledged items (the team is on it) but
    keeps them eligible for later 'fixed' / 'adjusted' style
    resolution. Only transitions from open / awaiting_response /
    escalated — won't overwrite a resolved row."""
    with connect() as c:
        c.execute(
            "UPDATE stock_issues SET "
            "  status = 'acknowledged', "
            "  resolved_at = datetime('now'), "
            "  resolved_by = ?, resolution_text = ? "
            "WHERE id = ? AND status IN "
            "  ('open', 'awaiting_response', 'escalated')",
            (ack_by, (ack_text or "")[:500], issue_id))


def list_open_stock_issues(limit: int = 100,
                                max_age_days: int = 30) -> list:
    """Open + escalated, ordered oldest first."""
    with connect() as c:
        rows = c.execute(
            "SELECT id, raise_channel, raise_ts, raise_thread_ts, "
            "       raised_by, raised_text, issue_type, "
            "       so_numbers, skus, families, status, "
            "       awaiting_user, dm_channel, dm_posted_at, "
            "       created_at "
            "FROM stock_issues "
            "WHERE status IN ('open', 'awaiting_response', "
            "                  'escalated') "
            "  AND created_at >= "
            "      datetime('now', '-' || ? || ' days') "
            "ORDER BY created_at ASC LIMIT ?",
            (max_age_days, limit)).fetchall()
    return [dict(r) for r in rows]


def find_stock_issue_by_thread(raise_channel: str,
                                       thread_ts: str) -> Optional[dict]:
    """Used when a reply lands in a thread — find the parent issue
    so we can pick up the staff's confirmation/resolution text.
    Includes 'acknowledged' so a later 'fixed' reply can promote
    an already-acknowledged issue to fully resolved."""
    if not raise_channel or not thread_ts:
        return None
    with connect() as c:
        r = c.execute(
            "SELECT id, raise_channel, raise_ts, "
            "       raise_thread_ts, status "
            "FROM stock_issues "
            "WHERE raise_channel = ? "
            "  AND raise_thread_ts = ? "
            "  AND status IN ('open', 'awaiting_response', "
            "                  'escalated', 'acknowledged') "
            "LIMIT 1",
            (raise_channel, thread_ts)).fetchone()
    return dict(r) if r else None


def list_stock_issues_pending_escalation(
        min_age_hours: int = 4) -> list:
    """awaiting_response issues older than min_age_hours that
    haven't been DM'd yet. Used by the escalation cycle."""
    with connect() as c:
        rows = c.execute(
            "SELECT id, raise_channel, raise_ts, raise_thread_ts, "
            "       raised_by, raised_text, issue_type, "
            "       so_numbers, skus, families, created_at "
            "FROM stock_issues "
            "WHERE status = 'awaiting_response' "
            "  AND dm_posted_ts IS NULL "
            "  AND created_at <= "
            "      datetime('now', '-' || ? || ' hours') "
            "ORDER BY created_at ASC",
            (min_age_hours,)).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Back-in-stock arrival notifications (v2.67.140)
# ---------------------------------------------------------------------------
def find_pending_back_in_stock_signals(
        skus: list = None,
        families: list = None,
        days: int = 365,
        ) -> list:
    """Return demand_signals where signal_type='notify_me' and
    outcome is NULL or 'pending', and the SKU OR family matches
    one of the lists provided. Used by check_arrivals to find
    customers waiting for stock that's about to land.

    Either `skus` or `families` (or both) must be provided.
    `days` bounds the lookback window — old subscriptions get
    aged out by the caller, not here."""
    if not skus and not families:
        return []
    parts = ["signal_type = 'notify_me'",
              "(outcome IS NULL OR outcome IN ('pending', 'open'))",
              ("created_at >= "
                "datetime('now', '-' || ? || ' days')"),
              ]
    params: list = [days]
    or_terms = []
    if skus:
        placeholders = ",".join("?" for _ in skus)
        or_terms.append(f"UPPER(sku) IN ({placeholders})")
        params.extend(s.upper() for s in skus)
    if families:
        # v2.67.161 — Whole-token LIKE match (instead of strict
        # IN) so an arrival family like 'SLIM8' matches a stored
        # subscription family like 'SLIM8 12V' or 'WHITE SLIM8'.
        # We wrap the stored value in spaces and convert dashes
        # to spaces so the LIKE pattern '% TOKEN %' captures
        # the family as a whole word — 'SLIM80' won't match
        # 'SLIM8'.
        fam_terms = []
        for f in families:
            fam_terms.append(
                "(' ' || UPPER(REPLACE(product_family, '-', "
                "' ')) || ' ') LIKE ('% ' || ? || ' %')")
            params.append(f.upper())
        or_terms.append("(" + " OR ".join(fam_terms) + ")")
    parts.append("(" + " OR ".join(or_terms) + ")")
    sql = (
        "SELECT id, sku, product_family, customer_name, "
        "       customer_id, raw_text, note, created_at, "
        "       source_ref "
        "FROM demand_signals "
        "WHERE " + " AND ".join(parts) + " "
        "ORDER BY created_at DESC")
    with connect() as c:
        rows = c.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def has_back_in_stock_arrival_notification(
        po_number: str, demand_signal_id: int) -> bool:
    """Idempotency check — already posted an arrival reminder for
    this (PO, demand_signal) pair?"""
    if not po_number or not demand_signal_id:
        return False
    with connect() as c:
        r = c.execute(
            "SELECT 1 FROM back_in_stock_arrival_notifications "
            "WHERE po_number = ? AND demand_signal_id = ?",
            (po_number, demand_signal_id)).fetchone()
    return r is not None


def record_back_in_stock_arrival_notification(
        po_number: str,
        sku: Optional[str],
        family: Optional[str],
        demand_signal_id: int,
        posted_channel: Optional[str],
        posted_ts: Optional[str],
        error_msg: Optional[str] = None) -> None:
    """Persist one (PO, demand_signal) notification. Multiple
    demand_signal IDs from the same PO each get their own row
    (one per customer notified). INSERT OR IGNORE handles
    concurrent writers."""
    with connect() as c:
        c.execute(
            "INSERT OR IGNORE INTO "
            "back_in_stock_arrival_notifications "
            "(po_number, sku, family, demand_signal_id, "
            " posted_channel, posted_ts, error_msg) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (po_number, sku, family, demand_signal_id,
              posted_channel, posted_ts, error_msg))


# ---------------------------------------------------------------------------
# Dropship backorder warnings (v2.67.138)
# ---------------------------------------------------------------------------
def has_dropship_warning(so_number: str, sku: str) -> bool:
    """Idempotency check — have we already warned about this
    (SO, SKU) pair? UNIQUE constraint guarantees one row per pair."""
    if not so_number or not sku:
        return False
    with connect() as c:
        r = c.execute(
            "SELECT 1 FROM dropship_backorder_warnings "
            "WHERE so_number = ? AND sku = ?",
            (so_number, sku)).fetchone()
    return r is not None


def record_dropship_warning(so_number: str, sku: str,
                                  customer: Optional[str],
                                  supplier: Optional[str],
                                  quantity_ordered: Optional[float],
                                  quantity_on_hand: Optional[float],
                                  posted_channel: Optional[str],
                                  posted_ts: Optional[str],
                                  error_msg: Optional[str] = None
                                  ) -> None:
    """Persist a dropship warning. INSERT OR IGNORE so concurrent
    workers can't duplicate. error_msg is set if the Slack post
    failed — row still inserted so we don't retry forever."""
    with connect() as c:
        c.execute(
            "INSERT OR IGNORE INTO dropship_backorder_warnings "
            "(so_number, sku, customer, supplier, "
            " quantity_ordered, quantity_on_hand, posted_channel, "
            " posted_ts, error_msg) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (so_number, sku, customer, supplier,
              quantity_ordered, quantity_on_hand, posted_channel,
              posted_ts, error_msg))


def list_recent_dropship_warnings(limit: int = 50) -> list:
    with connect() as c:
        rows = c.execute(
            "SELECT so_number, sku, customer, supplier, "
            "       quantity_ordered, quantity_on_hand, "
            "       posted_channel, posted_ts, posted_at, "
            "       error_msg "
            "FROM dropship_backorder_warnings "
            "ORDER BY posted_at DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# PO dispatch reminders (v2.67.130)
# ---------------------------------------------------------------------------
def has_notified_po_dispatch(po_number: str) -> bool:
    """Idempotency check — has this PO already triggered a
    fulfillment reminder? PRIMARY KEY guarantees one row per PO."""
    if not po_number:
        return False
    with connect() as c:
        r = c.execute(
            "SELECT 1 FROM po_dispatch_reminders WHERE po_number = ?",
            (po_number,)).fetchone()
    return r is not None


def record_po_dispatch_reminder(po_number: str,
                                       supplier: Optional[str],
                                       received_status: Optional[str],
                                       so_numbers: List[str],
                                       posted_channel: Optional[str],
                                       posted_ts: Optional[str],
                                       error_msg: Optional[str] = None
                                       ) -> None:
    """Persist that we've notified (or attempted to notify) about
    this PO. Pass error_msg if the post failed — keeps the row in
    place so we don't retry indefinitely; admins can manually
    clear it if they want a retry."""
    so_csv = ",".join(s.strip() for s in so_numbers if s)
    # v2.67.166 — ON CONFLICT DO UPDATE works on both SQLite
    # (>= 3.24) and Postgres. Previous INSERT OR REPLACE was
    # SQLite-only and crashed on the Postgres backend.
    with connect() as c:
        c.execute(
            "INSERT INTO po_dispatch_reminders "
            "(po_number, supplier, received_status, so_numbers, "
            " n_sos, posted_channel, posted_ts, error_msg, "
            " posted_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now')) "
            "ON CONFLICT(po_number) DO UPDATE SET "
            "  supplier=excluded.supplier, "
            "  received_status=excluded.received_status, "
            "  so_numbers=excluded.so_numbers, "
            "  n_sos=excluded.n_sos, "
            "  posted_channel=excluded.posted_channel, "
            "  posted_ts=excluded.posted_ts, "
            "  error_msg=excluded.error_msg, "
            "  posted_at=excluded.posted_at",
            (po_number, supplier, received_status, so_csv,
              len(so_numbers), posted_channel, posted_ts, error_msg))


def list_recent_po_dispatch_reminders(limit: int = 50) -> list:
    """For diagnostics — recent reminders we've posted."""
    with connect() as c:
        rows = c.execute(
            "SELECT po_number, supplier, received_status, "
            "       so_numbers, n_sos, posted_channel, posted_at, "
            "       escalated_at, escalated_ts, escalation_reason, "
            "       error_msg "
            "FROM po_dispatch_reminders "
            "ORDER BY posted_at DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]


def list_po_reminders_pending_escalation(
        min_age_hours: int = 24,
        max_age_hours: int = 168
        ) -> list:
    """v2.67.131 — reminders posted >= min_age_hours ago that have
    not yet been escalated and didn't error on the initial post.
    max_age_hours bounds how far back we look (default 7 days)
    so we don't endlessly chase stale reminders if shipments are
    just slow."""
    with connect() as c:
        rows = c.execute(
            "SELECT po_number, supplier, received_status, "
            "       so_numbers, n_sos, posted_channel, posted_ts, "
            "       posted_at "
            "FROM po_dispatch_reminders "
            "WHERE escalated_at IS NULL "
            "  AND (error_msg IS NULL OR error_msg = '') "
            "  AND posted_at <= datetime('now', '-' || ? || ' hours') "
            "  AND posted_at >= datetime('now', '-' || ? || ' hours') "
            "ORDER BY posted_at ASC",
            (min_age_hours, max_age_hours)).fetchall()
    return [dict(r) for r in rows]


def record_po_dispatch_escalation(po_number: str,
                                          posted_ts: Optional[str],
                                          reason: str,
                                          error_msg: Optional[str] = None
                                          ) -> None:
    """v2.67.131 — stamp a reminder as escalated. We never escalate
    twice; the escalated_at IS NULL filter above guarantees idempotence."""
    with connect() as c:
        c.execute(
            "UPDATE po_dispatch_reminders SET "
            "escalated_at = datetime('now'), "
            "escalated_ts = ?, "
            "escalation_reason = ? "
            "WHERE po_number = ?",
            (posted_ts, (reason + (f" | error: {error_msg}"
                                          if error_msg else "")),
              po_number))


# ---------------------------------------------------------------------------
# Viktor bridge — Slack user OAuth tokens (v2.67.126)
# ---------------------------------------------------------------------------
def upsert_slack_user_token(user_id: int, slack_user_id: str,
                                  access_token_enc: str,
                                  slack_team_id: Optional[str] = None,
                                  scopes: Optional[str] = None,
                                  expires_at: Optional[str] = None
                                  ) -> None:
    """Store / overwrite a user's encrypted Slack token. Used by
    the OAuth callback in app.py once the user authorises the
    dashboard to post to Slack on their behalf."""
    sql = (
        "INSERT INTO slack_user_tokens "
        "(user_id, slack_user_id, slack_team_id, access_token_enc, "
        " scopes, expires_at, authed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, datetime('now')) "
        "ON CONFLICT(user_id) DO UPDATE SET "
        "  slack_user_id = excluded.slack_user_id, "
        "  slack_team_id = excluded.slack_team_id, "
        "  access_token_enc = excluded.access_token_enc, "
        "  scopes = excluded.scopes, "
        "  expires_at = excluded.expires_at, "
        "  authed_at = datetime('now')")
    with connect() as c:
        c.execute(sql, (user_id, slack_user_id, slack_team_id,
                          access_token_enc, scopes, expires_at))


def get_slack_user_token_row(user_id: int) -> Optional[dict]:
    """Return the encrypted token row for a user (or None).
    Caller decrypts via slack_oauth.decrypt_token."""
    with connect() as c:
        r = c.execute(
            "SELECT user_id, slack_user_id, slack_team_id, "
            "       access_token_enc, scopes, expires_at, "
            "       authed_at, last_used_at "
            "FROM slack_user_tokens WHERE user_id = ?",
            (user_id,)).fetchone()
    return dict(r) if r else None


def touch_slack_user_token(user_id: int) -> None:
    """Bump last_used_at when the token is used. Cheap, no return."""
    try:
        with connect() as c:
            c.execute(
                "UPDATE slack_user_tokens SET "
                "last_used_at = datetime('now') WHERE user_id = ?",
                (user_id,))
    except sqlite3.Error:
        pass


def delete_slack_user_token(user_id: int) -> None:
    """Revoke a stored Slack token (user clicked 'Disconnect')."""
    with connect() as c:
        c.execute("DELETE FROM slack_user_tokens WHERE user_id = ?",
                    (user_id,))


# ---------------------------------------------------------------------------
# QuickBooks Online connection (v2.67.211)
# ---------------------------------------------------------------------------
# ONE QBO company at a time — Wired4Signs has a single books file.
# We model that with a fixed primary key so an UPSERT always
# targets the same row; a reconnect to a different realm simply
# overwrites it. Callers (qbo_oauth.py) decrypt the token columns.
def save_qbo_connection(realm_id: str,
                          access_token_enc: str,
                          refresh_token_enc: str,
                          access_expires_at: Optional[str],
                          refresh_expires_at: Optional[str],
                          environment: str = "sandbox",
                          connected_by: Optional[str] = None) -> None:
    """Insert or update the single QBO connection row. Token
    columns must already be Fernet-encrypted by the caller.
    Both tokens are rewritten on every refresh (QBO rotates the
    refresh token), so this doubles as the refresh-persist path."""
    sql = (
        "INSERT INTO qbo_connection "
        "(realm_id, access_token_enc, refresh_token_enc, "
        " access_expires_at, refresh_expires_at, environment, "
        " connected_by, connected_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now')) "
        "ON CONFLICT(realm_id) DO UPDATE SET "
        "  access_token_enc = excluded.access_token_enc, "
        "  refresh_token_enc = excluded.refresh_token_enc, "
        "  access_expires_at = excluded.access_expires_at, "
        "  refresh_expires_at = excluded.refresh_expires_at, "
        "  environment = excluded.environment, "
        "  connected_by = excluded.connected_by, "
        "  updated_at = datetime('now')")
    with connect() as c:
        c.execute(sql, (realm_id, access_token_enc, refresh_token_enc,
                          access_expires_at, refresh_expires_at,
                          environment, connected_by))


def get_qbo_connection() -> Optional[dict]:
    """Return the current QBO connection row (or None if not yet
    connected). Caller decrypts access/refresh tokens via
    qbo_oauth.decrypt_token. Only one realm is ever stored, so we
    return the most-recently-updated row defensively."""
    with connect() as c:
        r = c.execute(
            "SELECT realm_id, access_token_enc, refresh_token_enc, "
            "       access_expires_at, refresh_expires_at, "
            "       environment, connected_by, connected_at, "
            "       updated_at "
            "FROM qbo_connection "
            "ORDER BY updated_at DESC LIMIT 1").fetchone()
    return dict(r) if r else None


def clear_qbo_connection() -> None:
    """Disconnect QBO — drop all stored tokens (user clicked
    'Disconnect' or revoked access from the Intuit side)."""
    with connect() as c:
        c.execute("DELETE FROM qbo_connection")


# ---------------------------------------------------------------------------
# Cashflow Management — supplier payables (v2.67.219)
# ---------------------------------------------------------------------------
# Columns a caller is allowed to update via update_payable(). The
# QBO mirror columns are deliberately NOT in here — they are owned
# by upsert_qbo_payable (the sync). amount_override/due_date_
# override let James adjust a QBO bill without the sync undoing it.
_PAYABLE_UPDATABLE = (
    "supplier", "reference", "description", "amount", "currency",
    "invoice_date", "due_date", "status", "paid_date",
    "paid_amount", "slack_ts", "notes", "amount_override",
    "due_date_override", "is_dismissed",
)


def upsert_qbo_payable(qbo_bill_id: str, supplier: Optional[str],
                       reference: Optional[str],
                       description: Optional[str],
                       amount: Optional[float],
                       currency: Optional[str],
                       invoice_date: Optional[str],
                       due_date: Optional[str],
                       qbo_balance: Optional[float]) -> None:
    """Insert or refresh a QBO-sourced payable. ON CONFLICT this
    updates ONLY the QBO mirror columns — status, approval fields
    and James's overrides are preserved so the sync never undoes
    a human decision."""
    sql = (
        "INSERT INTO cashflow_payables "
        "(source, qbo_bill_id, supplier, reference, description, "
        " amount, currency, invoice_date, due_date, qbo_balance, "
        " created_at, updated_at) "
        "VALUES ('qbo', ?, ?, ?, ?, ?, ?, ?, ?, ?, "
        "        datetime('now'), datetime('now')) "
        "ON CONFLICT(qbo_bill_id) DO UPDATE SET "
        "  supplier = excluded.supplier, "
        "  reference = excluded.reference, "
        "  description = excluded.description, "
        "  amount = excluded.amount, "
        "  currency = excluded.currency, "
        "  invoice_date = excluded.invoice_date, "
        "  due_date = excluded.due_date, "
        "  qbo_balance = excluded.qbo_balance, "
        "  updated_at = datetime('now')")
    with connect() as c:
        c.execute(sql, (qbo_bill_id, supplier, reference,
                          description, amount, currency,
                          invoice_date, due_date, qbo_balance))


def mark_qbo_payables_closed_except(open_qbo_bill_ids: list[str]) -> int:
    """Mark mirrored QBO bills as paid when they are no longer open.

    The Cashflow page previously relied on each old bill being
    re-synced with Balance=0. That fails for bills whose TxnDate
    falls outside the recent sync window: they remain locally
    pending forever and pollute the forecast. QBO's open-bills list
    is the source of truth here; anything mirrored locally but not
    present there has been settled/closed in QBO.
    """
    ids = [str(i).strip() for i in (open_qbo_bill_ids or [])
           if str(i).strip()]
    paid_date = datetime.utcnow().date().isoformat()
    sql = (
        "UPDATE cashflow_payables SET "
        "  qbo_balance = 0, "
        "  status = 'paid', "
        "  paid_date = COALESCE(paid_date, ?), "
        "  paid_amount = COALESCE(paid_amount, amount), "
        "  updated_at = datetime('now'), "
        "  updated_by = 'auto:qbo_sync' "
        "WHERE source = 'qbo' "
        "  AND qbo_bill_id IS NOT NULL "
    )
    params: list = [paid_date]
    if ids:
        sql += "  AND qbo_bill_id NOT IN ("
        sql += ",".join("?" for _ in ids)
        sql += ") "
        params.extend(ids)
    sql += (
        "  AND (qbo_balance IS NULL OR qbo_balance > 0 "
        "       OR status != 'paid')"
    )
    with connect() as c:
        cur = c.execute(sql, params)
        return int(cur.rowcount or 0)


def add_manual_payable(supplier: Optional[str],
                       reference: Optional[str],
                       description: Optional[str],
                       amount: Optional[float],
                       currency: str = "USD",
                       invoice_date: Optional[str] = None,
                       due_date: Optional[str] = None,
                       updated_by: Optional[str] = None) -> int:
    """Add a non-QBO payable (freight, duty, anything not yet
    billed in QBO). Returns the new payable_id."""
    with connect() as c:
        cur = c.execute(
            "INSERT INTO cashflow_payables "
            "(source, supplier, reference, description, amount, "
            " currency, invoice_date, due_date, updated_by, "
            " created_at, updated_at) "
            "VALUES ('manual', ?, ?, ?, ?, ?, ?, ?, ?, "
            "        datetime('now'), datetime('now'))",
            (supplier, reference, description, amount, currency,
              invoice_date, due_date, updated_by))
        return int(cur.lastrowid)


def list_payables(include_dismissed: bool = False,
                  include_paid: bool = True) -> list:
    """Return cashflow payables as a list of dicts, ordered by
    due date (nulls last). 'include_paid=False' excludes BOTH
    rows whose local workflow status is 'paid' AND rows where
    QBO reports zero outstanding balance — without that second
    filter, paid QBO bills (status still 'pending' locally)
    would inflate the dashboard's outstanding-payables total."""
    sql = "SELECT * FROM cashflow_payables WHERE 1=1"
    if not include_dismissed:
        sql += " AND is_dismissed = 0"
    if not include_paid:
        sql += (" AND status != 'paid' "
                " AND NOT (qbo_balance IS NOT NULL "
                "          AND qbo_balance <= 0)")
    sql += (" ORDER BY CASE WHEN due_date IS NULL OR due_date = '' "
            "THEN 1 ELSE 0 END, due_date, payable_id")
    with connect() as c:
        rows = c.execute(sql).fetchall()
    return [dict(r) for r in rows]


def get_payable(payable_id: int) -> Optional[dict]:
    with connect() as c:
        r = c.execute(
            "SELECT * FROM cashflow_payables WHERE payable_id = ?",
            (payable_id,)).fetchone()
    return dict(r) if r else None


def update_payable(payable_id: int, fields: dict,
                   updated_by: Optional[str] = None) -> None:
    """Generic update — only whitelisted columns in
    _PAYABLE_UPDATABLE are written. Always bumps updated_at."""
    sets = []
    vals: list = []
    for col, val in (fields or {}).items():
        if col in _PAYABLE_UPDATABLE:
            sets.append(f"{col} = ?")
            vals.append(val)
    if not sets:
        return
    sets.append("updated_at = datetime('now')")
    sets.append("updated_by = ?")
    vals.append(updated_by)
    vals.append(payable_id)
    with connect() as c:
        c.execute(
            f"UPDATE cashflow_payables SET {', '.join(sets)} "
            f"WHERE payable_id = ?", vals)


def approve_payable(payable_id: int, approved_by: str,
                   slack_ts: Optional[str] = None) -> None:
    """Mark a payable approved for payment (James's go-ahead)."""
    with connect() as c:
        c.execute(
            "UPDATE cashflow_payables SET status = 'approved', "
            "approved_by = ?, approved_at = datetime('now'), "
            "slack_ts = COALESCE(?, slack_ts), "
            "updated_at = datetime('now'), updated_by = ? "
            "WHERE payable_id = ?",
            (approved_by, slack_ts, approved_by, payable_id))


def delete_manual_payable(payable_id: int) -> None:
    """Hard-delete a manual payable row. QBO-sourced rows should
    be dismissed (is_dismissed) instead, or they'd reappear on
    the next sync."""
    with connect() as c:
        c.execute(
            "DELETE FROM cashflow_payables "
            "WHERE payable_id = ? AND source = 'manual'",
            (payable_id,))


# ---------------------------------------------------------------------------
# Cashflow Management — 53-week forecast grid (v2.67.219)
# ---------------------------------------------------------------------------
def set_forecast_cell(week_start: str, row_key: str,
                     amount: Optional[float],
                     updated_by: Optional[str] = None,
                     scenario: str = "base") -> None:
    """Upsert one forecast cell. scenario='base' writes the live
    forecast; any other name writes that what-if scenario."""
    with connect() as c:
        if scenario == "base":
            c.execute(
                "INSERT INTO cashflow_forecast "
                "(week_start, row_key, amount, updated_at, "
                " updated_by) "
                "VALUES (?, ?, ?, datetime('now'), ?) "
                "ON CONFLICT(week_start, row_key) DO UPDATE SET "
                "  amount = excluded.amount, "
                "  updated_at = datetime('now'), "
                "  updated_by = excluded.updated_by",
                (week_start, row_key, amount, updated_by))
        else:
            c.execute(
                "INSERT INTO cashflow_scenario_forecast "
                "(scenario, week_start, row_key, amount, "
                " updated_at, updated_by) "
                "VALUES (?, ?, ?, ?, datetime('now'), ?) "
                "ON CONFLICT(scenario, week_start, row_key) "
                "DO UPDATE SET "
                "  amount = excluded.amount, "
                "  updated_at = datetime('now'), "
                "  updated_by = excluded.updated_by",
                (scenario, week_start, row_key, amount,
                  updated_by))


def bulk_set_forecast(cells: list,
                     updated_by: Optional[str] = None,
                     scenario: str = "base") -> None:
    """Upsert many forecast cells at once. `cells` is a list of
    (week_start, row_key, amount) tuples."""
    for week_start, row_key, amount in cells:
        set_forecast_cell(week_start, row_key, amount,
                          updated_by, scenario)


def get_forecast(scenario: str = "base") -> dict:
    """Return a forecast grid as a dict keyed by
    (week_start, row_key) -> amount. scenario='base' = the live
    forecast; any other name = that what-if scenario."""
    with connect() as c:
        if scenario == "base":
            rows = c.execute(
                "SELECT week_start, row_key, amount "
                "FROM cashflow_forecast").fetchall()
        else:
            rows = c.execute(
                "SELECT week_start, row_key, amount "
                "FROM cashflow_scenario_forecast "
                "WHERE scenario = ?", (scenario,)).fetchall()
    return {(r["week_start"], r["row_key"]): r["amount"]
            for r in rows}


def get_forecast_owners(scenario: str = "base") -> dict:
    """Return {(week_start, row_key): updated_by} for a forecast
    grid. Used by the sales-projection feature to tell a human-
    edited cell apart from an auto-projected one."""
    with connect() as c:
        if scenario == "base":
            rows = c.execute(
                "SELECT week_start, row_key, updated_by "
                "FROM cashflow_forecast").fetchall()
        else:
            rows = c.execute(
                "SELECT week_start, row_key, updated_by "
                "FROM cashflow_scenario_forecast "
                "WHERE scenario = ?", (scenario,)).fetchall()
    return {(r["week_start"], r["row_key"]): r["updated_by"]
            for r in rows}


# ---------------------------------------------------------------------------
# Cashflow scenarios — named what-if copies (v2.67.234)
# ---------------------------------------------------------------------------
def list_scenarios() -> list:
    """Return all named what-if scenarios (not 'base')."""
    with connect() as c:
        rows = c.execute(
            "SELECT name, created_by, created_at "
            "FROM cashflow_scenarios "
            "ORDER BY created_at").fetchall()
    return [dict(r) for r in rows]


def create_scenario(name: str,
                    created_by: Optional[str] = None) -> None:
    """Create a what-if scenario as a CLONE of the base forecast
    — copies every base cell and base custom row into the new
    scenario so the user starts from the live position."""
    with connect() as c:
        c.execute(
            "INSERT INTO cashflow_scenarios (name, created_by) "
            "VALUES (?, ?)", (name, created_by))
        base_cells = c.execute(
            "SELECT week_start, row_key, amount "
            "FROM cashflow_forecast").fetchall()
        for r in base_cells:
            c.execute(
                "INSERT INTO cashflow_scenario_forecast "
                "(scenario, week_start, row_key, amount, "
                " updated_by) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(scenario, week_start, row_key) "
                "DO NOTHING",
                (name, r["week_start"], r["row_key"],
                  r["amount"], created_by))
        base_rows = c.execute(
            "SELECT row_key, label, kind, sort_order "
            "FROM cashflow_custom_rows "
            "WHERE scenario = 'base'").fetchall()
        for r in base_rows:
            c.execute(
                "INSERT INTO cashflow_custom_rows "
                "(scenario, row_key, label, kind, sort_order) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(scenario, row_key) DO NOTHING",
                (name, r["row_key"], r["label"], r["kind"],
                  r["sort_order"]))


def delete_scenario(name: str) -> None:
    """Delete a what-if scenario and all its cells/custom rows.
    Refuses to touch 'base'."""
    if name == "base":
        return
    with connect() as c:
        c.execute("DELETE FROM cashflow_scenario_forecast "
                    "WHERE scenario = ?", (name,))
        c.execute("DELETE FROM cashflow_custom_rows "
                    "WHERE scenario = ?", (name,))
        c.execute("DELETE FROM cashflow_scenarios "
                    "WHERE name = ?", (name,))


# ---------------------------------------------------------------------------
# Cashflow custom rows — user-added forecast line items (v2.67.234)
# ---------------------------------------------------------------------------
def get_custom_rows(scenario: str = "base") -> list:
    """Return custom (user-added) forecast rows for a scenario,
    each a dict with row_key/label/kind/sort_order."""
    with connect() as c:
        rows = c.execute(
            "SELECT row_key, label, kind, sort_order "
            "FROM cashflow_custom_rows WHERE scenario = ? "
            "ORDER BY sort_order, row_key", (scenario,)).fetchall()
    return [dict(r) for r in rows]


def add_custom_row(scenario: str, row_key: str, label: str,
                  kind: str = "outflow") -> None:
    """Add (or relabel) a custom forecast line item."""
    with connect() as c:
        c.execute(
            "INSERT INTO cashflow_custom_rows "
            "(scenario, row_key, label, kind) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(scenario, row_key) DO UPDATE SET "
            "  label = excluded.label, kind = excluded.kind",
            (scenario, row_key, label, kind))


def delete_custom_row(scenario: str, row_key: str) -> None:
    """Remove a custom row and its cells from a scenario."""
    with connect() as c:
        c.execute(
            "DELETE FROM cashflow_custom_rows "
            "WHERE scenario = ? AND row_key = ?",
            (scenario, row_key))
        if scenario == "base":
            c.execute("DELETE FROM cashflow_forecast "
                        "WHERE row_key = ?", (row_key,))
        else:
            c.execute(
                "DELETE FROM cashflow_scenario_forecast "
                "WHERE scenario = ? AND row_key = ?",
                (scenario, row_key))


# ---------------------------------------------------------------------------
# Cashflow loans (v2.67.235)
# ---------------------------------------------------------------------------
_LOAN_UPDATABLE = (
    "lender", "principal", "apr", "start_date",
    "first_payment_date", "monthly_payment", "forecast_row_key",
    "notes", "active",
)


def add_loan(lender: str, principal: float, apr: float,
            start_date: str, first_payment_date: str,
            monthly_payment: float,
            forecast_row_key: Optional[str] = None,
            notes: Optional[str] = None) -> int:
    """Register a loan. The amortization schedule is computed on
    demand by loan_amortization.py — only the params are stored."""
    with connect() as c:
        cur = c.execute(
            "INSERT INTO cashflow_loans "
            "(lender, principal, apr, start_date, "
            " first_payment_date, monthly_payment, "
            " forecast_row_key, notes, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, "
            "        datetime('now'), datetime('now'))",
            (lender, principal, apr, start_date,
              first_payment_date, monthly_payment,
              forecast_row_key, notes))
        return int(cur.lastrowid)


def list_loans(active_only: bool = True) -> list:
    """Return registered loans."""
    sql = "SELECT * FROM cashflow_loans"
    if active_only:
        sql += " WHERE active = 1"
    sql += " ORDER BY loan_id"
    with connect() as c:
        rows = c.execute(sql).fetchall()
    return [dict(r) for r in rows]


def get_loan(loan_id: int) -> Optional[dict]:
    with connect() as c:
        r = c.execute(
            "SELECT * FROM cashflow_loans WHERE loan_id = ?",
            (loan_id,)).fetchone()
    return dict(r) if r else None


def update_loan(loan_id: int, fields: dict) -> None:
    """Update whitelisted loan columns."""
    sets = []
    vals: list = []
    for col, val in (fields or {}).items():
        if col in _LOAN_UPDATABLE:
            sets.append(f"{col} = ?")
            vals.append(val)
    if not sets:
        return
    sets.append("updated_at = datetime('now')")
    vals.append(loan_id)
    with connect() as c:
        c.execute(
            f"UPDATE cashflow_loans SET {', '.join(sets)} "
            f"WHERE loan_id = ?", vals)


def delete_loan(loan_id: int) -> None:
    with connect() as c:
        c.execute("DELETE FROM cashflow_loans WHERE loan_id = ?",
                    (loan_id,))


# ---------------------------------------------------------------------------
# Notion knowledge-base mirror (v2.67.250)
# ---------------------------------------------------------------------------
def upsert_kb_article(page_id: str, title: str,
                          content_md: Optional[str],
                          url: Optional[str],
                          notion_edited_at: Optional[str] = None,
                          category: Optional[str] = None
                          ) -> None:
    """Upsert one Notion KB article (page) by page_id."""
    sql = (
        "INSERT INTO notion_kb_articles "
        "(page_id, title, content_md, url, category, "
        " notion_edited_at, synced_at) "
        "VALUES (?, ?, ?, ?, ?, ?, datetime('now')) "
        "ON CONFLICT(page_id) DO UPDATE SET "
        "  title = excluded.title, "
        "  content_md = excluded.content_md, "
        "  url = excluded.url, "
        "  category = excluded.category, "
        "  notion_edited_at = excluded.notion_edited_at, "
        "  synced_at = datetime('now')")
    with connect() as c:
        c.execute(sql, (page_id, title, content_md, url, category,
                          notion_edited_at))


def list_kb_articles(limit: int = 500) -> list:
    """Return all mirrored KB articles, newest sync first."""
    with connect() as c:
        rows = c.execute(
            "SELECT page_id, title, content_md, url, category, "
            "       notion_edited_at, synced_at "
            "FROM notion_kb_articles "
            "ORDER BY synced_at DESC LIMIT ?",
            (limit,)).fetchall()
    return [dict(r) for r in rows]


def get_kb_article(page_id: str) -> Optional[dict]:
    with connect() as c:
        r = c.execute(
            "SELECT * FROM notion_kb_articles WHERE page_id = ?",
            (page_id,)).fetchone()
    return dict(r) if r else None


def search_kb_articles(query: str, limit: int = 5) -> list:
    """Case-insensitive substring search across title + content.
    Returns up to `limit` rows. Title matches are ranked above
    body matches via a CASE expression."""
    q = (query or "").strip().lower()
    if not q:
        return []
    like = f"%{q}%"
    with connect() as c:
        rows = c.execute(
            "SELECT page_id, title, content_md, url, category, "
            "       notion_edited_at "
            "FROM notion_kb_articles "
            "WHERE LOWER(title) LIKE ? "
            "   OR LOWER(content_md) LIKE ? "
            "ORDER BY CASE WHEN LOWER(title) LIKE ? "
            "              THEN 0 ELSE 1 END, title "
            "LIMIT ?",
            (like, like, like, limit)).fetchall()
    return [dict(r) for r in rows]


def delete_kb_article(page_id: str) -> None:
    with connect() as c:
        c.execute("DELETE FROM notion_kb_articles "
                    "WHERE page_id = ?", (page_id,))


# ---------------------------------------------------------------------------
# Notion database ID registry (v2.67.257)
# ---------------------------------------------------------------------------
def get_notion_db_id(name: str) -> Optional[str]:
    """Return the stored Notion database id for `name`, or None."""
    with connect() as c:
        r = c.execute(
            "SELECT db_id FROM notion_db_ids WHERE name = ?",
            (name,)).fetchone()
    return r["db_id"] if r else None


def set_notion_db_id(name: str, db_id: str) -> None:
    """Upsert the canonical database id for a logical name."""
    with connect() as c:
        c.execute(
            "INSERT INTO notion_db_ids (name, db_id, set_at) "
            "VALUES (?, ?, datetime('now')) "
            "ON CONFLICT(name) DO UPDATE SET "
            "  db_id = excluded.db_id, "
            "  set_at = datetime('now')",
            (name, db_id))


def clear_notion_db_id(name: str) -> None:
    """Forget the stored id (forces the next sync to look up or
    create the database fresh)."""
    with connect() as c:
        c.execute("DELETE FROM notion_db_ids WHERE name = ?",
                    (name,))


# ---------------------------------------------------------------------------
# Viktor bridge sessions (v2.67.126)
# ---------------------------------------------------------------------------
def create_viktor_bridge_session(session_id: str, user_id: int,
                                       question: str, channel_id: str
                                       ) -> None:
    """Record that the dashboard is forwarding a marketing question
    to Viktor on behalf of this user. Polling code looks up by
    session_id to fetch Viktor's reply when it arrives."""
    with connect() as c:
        c.execute(
            "INSERT INTO viktor_bridge_sessions "
            "(session_id, user_id, question, channel_id, status) "
            "VALUES (?, ?, ?, ?, 'pending')",
            (session_id, user_id, question, channel_id))


def update_viktor_bridge_post(session_id: str,
                                    posted_ts: str,
                                    thread_ts: str) -> None:
    """Store the Slack ts of our posted message + the thread_ts
    Viktor will reply in (Slack's chat.postMessage returns both)."""
    with connect() as c:
        c.execute(
            "UPDATE viktor_bridge_sessions SET "
            "posted_ts = ?, thread_ts = ? "
            "WHERE session_id = ?",
            (posted_ts, thread_ts, session_id))


def complete_viktor_bridge_session(session_id: str,
                                          viktor_reply_ts: str,
                                          viktor_reply_text: str,
                                          overlay_text: Optional[str]
                                          ) -> None:
    """Mark a bridge session as 'replied' once we've detected
    Viktor's reply and composed the overlay."""
    with connect() as c:
        c.execute(
            "UPDATE viktor_bridge_sessions SET "
            "viktor_reply_ts = ?, viktor_reply_text = ?, "
            "overlay_text = ?, status = 'replied', "
            "completed_at = datetime('now') "
            "WHERE session_id = ?",
            (viktor_reply_ts, viktor_reply_text, overlay_text,
              session_id))


def get_viktor_bridge_session(session_id: str) -> Optional[dict]:
    with connect() as c:
        r = c.execute(
            "SELECT * FROM viktor_bridge_sessions "
            "WHERE session_id = ?", (session_id,)).fetchone()
    return dict(r) if r else None


def poll_viktor_bridge_reply(session_id: str,
                                    viktor_slack_user_id: str
                                    ) -> Optional[dict]:
    """v2.67.126 — Check if Viktor has replied in the thread for
    this bridge session. Looks at slack_messages (populated by
    slack_sync's poll cycle) for a message from Viktor's user_id
    in the same thread, posted AFTER our forwarded message.

    Returns None while waiting; returns a dict with the reply
    fields when Viktor has replied."""
    sess = get_viktor_bridge_session(session_id)
    if not sess:
        return None
    posted_ts = sess.get("posted_ts")
    thread_ts = sess.get("thread_ts")
    channel_id = sess.get("channel_id")
    if not (posted_ts and thread_ts and channel_id):
        return None
    with connect() as c:
        r = c.execute(
            "SELECT ts, text FROM slack_messages "
            "WHERE channel_id = ? "
            "  AND user_id = ? "
            "  AND thread_ts = ? "
            "  AND ts > ? "
            "ORDER BY ts ASC LIMIT 1",
            (channel_id, viktor_slack_user_id,
              thread_ts, posted_ts)).fetchone()
    if not r:
        return None
    return {
        "ts": r["ts"],
        "text": r["text"] or "",
    }


def get_ad_attribution_for_sku(sku: str, days: int = 30) -> list:
    """Return ad campaigns that drove revenue on this SKU.
    v2.67.105 — adds per-SKU spend (acs.spend) so we can compute
    SKU-level ROAS, not just campaign-level."""
    sql = (
        "SELECT ad.platform, ad.campaign_id, "
        "       ad.campaign_name, ad.campaign_type, "
        "       SUM(ad.spend) AS campaign_spend, "
        "       SUM(ad.revenue_ga4) AS campaign_attributed_revenue, "
        "       SUM(acs.spend) AS sku_spend, "
        "       SUM(acs.clicks) AS sku_clicks, "
        "       SUM(acs.impressions) AS sku_impressions, "
        "       SUM(acs.revenue) AS sku_revenue, "
        "       SUM(acs.purchases) AS sku_purchases, "
        "       SUM(acs.add_to_carts) AS sku_atcs, "
        "       CASE WHEN SUM(acs.spend) > 0 THEN "
        "         ROUND(SUM(acs.revenue) / SUM(acs.spend), 2) "
        "       ELSE NULL END AS sku_roas "
        "FROM ad_campaign_skus acs "
        "LEFT JOIN ad_campaigns_daily ad "
        "  ON ad.platform = acs.platform "
        "  AND ad.campaign_id = acs.campaign_id "
        "  AND ad.date = acs.date "
        "WHERE acs.sku = ? "
        "  AND acs.date >= date('now', '-' || ? || ' days') "
        "GROUP BY ad.platform, ad.campaign_id "
        "ORDER BY sku_spend DESC NULLS LAST")
    with connect() as c:
        rows = c.execute(sql, (sku, days)).fetchall()
    return [dict(r) for r in rows]


def get_sku_ad_summary(sku: str, days: int = 30) -> dict:
    """v2.67.105 — single-row total for a SKU's ad performance.
    Used by the AI tool that answers 'what did we spend on
    advertising LED-Slim8 last month'."""
    sql = (
        "SELECT SUM(spend) AS total_spend, "
        "       SUM(revenue) AS total_revenue, "
        "       SUM(clicks) AS total_clicks, "
        "       SUM(impressions) AS total_impressions, "
        "       SUM(purchases) AS total_purchases, "
        "       COUNT(DISTINCT campaign_id) AS n_campaigns, "
        "       MIN(date) AS earliest, MAX(date) AS latest, "
        "       CASE WHEN SUM(spend) > 0 THEN "
        "         ROUND(SUM(revenue) / SUM(spend), 2) "
        "       ELSE NULL END AS roas, "
        "       CASE WHEN SUM(clicks) > 0 THEN "
        "         ROUND(SUM(spend) / SUM(clicks), 2) "
        "       ELSE NULL END AS cpc "
        "FROM ad_campaign_skus "
        "WHERE sku = ? "
        "  AND date >= date('now', '-' || ? || ' days')")
    with connect() as c:
        r = c.execute(sql, (sku, days)).fetchone()
    return dict(r) if r else {}


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

# v2.67.163 — backend routing. By default this is a no-op pass-
# through to the existing SQLite logic below. When DB_BACKEND=
# 'postgres' is set on the Render service, db_dialect.connect()
# returns a Postgres connection wrapper that rewrites SQLite-
# flavored SQL on the fly so the rest of db.py needs no changes.
import os as _os_for_backend  # noqa: E402

def _backend_is_postgres() -> bool:
    return (_os_for_backend.environ.get("DB_BACKEND", "sqlite")
              .strip().lower() == "postgres")


# v2.67.187 — Post-cutover Postgres migrations.
# The original migrate_to_pg.py introspected SQLite and built
# the Postgres schema from that snapshot. Tables ADDED to
# db.py's _SCHEMA after the cutover never make it to Postgres
# unless we ship dialect-correct DDL here.
#
# Each tuple is (table_name, postgres_ddl). The DDL must be
# IDEMPOTENT (use IF NOT EXISTS). _apply_pg_post_cutover()
# runs once per connect on the Postgres path; cheap because
# CREATE TABLE IF NOT EXISTS is a no-op when the table exists.
#
# Add a new entry every time db.py's _SCHEMA gets a new table.
# The keep-this-updated rule in intelligence_glossary.py covers
# engine columns; this is the equivalent rule for Postgres
# schema additions.
_PG_POST_CUTOVER_TABLES = [
    # v2.67.185 — User Permissions portal.
    ("user_page_permissions",
      """
      CREATE TABLE IF NOT EXISTS user_page_permissions (
          user_id   BIGINT NOT NULL,
          page_name TEXT   NOT NULL,
          allowed   BIGINT NOT NULL DEFAULT 0,
          set_by    TEXT,
          set_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY (user_id, page_name)
      );
      """),
    ("user_page_permissions_index",
      """
      CREATE INDEX IF NOT EXISTS ix_user_page_permissions_user
          ON user_page_permissions(user_id);
      """),
    # v2.67.199 — supplier_config mixed-case columns. The
    # original SQLite schema declared `safety_pct_A`, `_B`, `_C`
    # and `review_days_A`/`_B`/`_C` (mixed case for buyer
    # readability). SQLite is case-insensitive; Postgres is
    # case-folding-to-lowercase when identifiers are UNQUOTED
    # but case-preserving when QUOTED. My migrator quoted
    # column names, so Postgres now has columns literally
    # named `safety_pct_A` etc. with the capital letter
    # preserved. But the app's queries use the bare unquoted
    # form, which Postgres lowercases → "column safety_pct_a
    # does not exist". Rename to lowercase to match how the
    # app accesses them.
    #
    # Each rename runs in its own statement so a failure
    # (column already renamed on a re-run) doesn't poison the
    # rest. autocommit=True means each is its own transaction.
    ("supplier_config_rename_safety_pct_A",
      'ALTER TABLE supplier_config RENAME COLUMN '
      '"safety_pct_A" TO safety_pct_a;'),
    ("supplier_config_rename_safety_pct_B",
      'ALTER TABLE supplier_config RENAME COLUMN '
      '"safety_pct_B" TO safety_pct_b;'),
    ("supplier_config_rename_safety_pct_C",
      'ALTER TABLE supplier_config RENAME COLUMN '
      '"safety_pct_C" TO safety_pct_c;'),
    ("supplier_config_rename_review_days_A",
      'ALTER TABLE supplier_config RENAME COLUMN '
      '"review_days_A" TO review_days_a;'),
    ("supplier_config_rename_review_days_B",
      'ALTER TABLE supplier_config RENAME COLUMN '
      '"review_days_B" TO review_days_b;'),
    ("supplier_config_rename_review_days_C",
      'ALTER TABLE supplier_config RENAME COLUMN '
      '"review_days_C" TO review_days_c;'),
    # v2.67.283 — per-supplier reorder cadence (overrides the
    # ABC-class review days when set). IF NOT EXISTS keeps it
    # idempotent across re-runs.
    ("supplier_config_add_order_cadence_days",
      "ALTER TABLE supplier_config "
      "ADD COLUMN IF NOT EXISTS order_cadence_days INTEGER;"),
    # v2.67.284 — supplier holiday / shutdown periods. Multiple
    # rows per supplier; the reorder engine adds the overlap with
    # the upcoming lead-time + cadence window to the target cover.
    ("supplier_holidays_table",
      """
      CREATE TABLE IF NOT EXISTS supplier_holidays (
          id              SERIAL PRIMARY KEY,
          supplier_name   TEXT NOT NULL,
          start_date      DATE NOT NULL,
          end_date        DATE NOT NULL,
          label           TEXT,
          created_by      TEXT,
          created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      """),
    ("supplier_holidays_index",
      """
      CREATE INDEX IF NOT EXISTS idx_supplier_holidays_supplier
          ON supplier_holidays(supplier_name);
      """),
    # v2.67.303 — Shopify Admin API monthly discount totals.
    ("shopify_monthly_discounts_table",
      """
      CREATE TABLE IF NOT EXISTS shopify_monthly_discounts (
          month             TEXT PRIMARY KEY,
          total_discounts   DOUBLE PRECISION NOT NULL,
          order_count       INTEGER NOT NULL,
          synced_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      """),
    # v2.67.302 — persistent user sessions across worker deploys.
    ("user_sessions_table",
      """
      CREATE TABLE IF NOT EXISTS user_sessions (
          token         TEXT PRIMARY KEY,
          user_id       INTEGER NOT NULL,
          created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          expires_at    TIMESTAMPTZ NOT NULL,
          last_used_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      """),
    ("user_sessions_expires_idx",
      """
      CREATE INDEX IF NOT EXISTS idx_user_sessions_expires
          ON user_sessions(expires_at);
      """),
    # v2.67.285 — observed actual lead times from Inventory Planner.
    ("ip_lead_times_table",
      """
      CREATE TABLE IF NOT EXISTS ip_lead_times (
          sku                       TEXT PRIMARY KEY,
          observed_lead_time_days   INTEGER,
          configured_lead_time_days INTEGER,
          vendor_name               TEXT,
          sales_velocity1           REAL,
          last_received_at          TEXT,
          synced_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      """),
    ("ip_lead_times_index",
      """
      CREATE INDEX IF NOT EXISTS idx_ip_lead_times_vendor
          ON ip_lead_times(vendor_name);
      """),
    # v2.67.292 — QBO Profit & Loss by month + account-mapping
    # config. Canonical financial source for Monthly Metrics.
    ("qbo_monthly_pl_table",
      """
      CREATE TABLE IF NOT EXISTS qbo_monthly_pl (
          id              SERIAL PRIMARY KEY,
          month           TEXT NOT NULL,
          account_id      TEXT,
          account_number  TEXT,
          account_name    TEXT NOT NULL,
          account_type    TEXT,
          parent_account_id TEXT,
          amount          DOUBLE PRECISION NOT NULL,
          synced_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE(month, account_id, account_name)
      );
      """),
    ("qbo_monthly_pl_month_idx",
      """
      CREATE INDEX IF NOT EXISTS idx_qbo_monthly_pl_month
          ON qbo_monthly_pl(month);
      """),
    ("qbo_monthly_pl_acctnum_idx",
      """
      CREATE INDEX IF NOT EXISTS idx_qbo_monthly_pl_acctnum
          ON qbo_monthly_pl(account_number);
      """),
    ("qbo_account_mappings_table",
      """
      CREATE TABLE IF NOT EXISTS qbo_account_mappings (
          category        TEXT PRIMARY KEY,
          account_numbers TEXT,
          account_names   TEXT,
          notes           TEXT,
          set_by          TEXT,
          set_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      """),
    # v2.67.211 — QuickBooks Online connection table.
    ("qbo_connection",
      """
      CREATE TABLE IF NOT EXISTS qbo_connection (
          realm_id            TEXT PRIMARY KEY,
          access_token_enc    TEXT NOT NULL,
          refresh_token_enc   TEXT NOT NULL,
          access_expires_at   TIMESTAMPTZ,
          refresh_expires_at  TIMESTAMPTZ,
          environment         TEXT DEFAULT 'sandbox',
          connected_by        TEXT,
          connected_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      """),
    # v2.67.219 — Cashflow Management tables.
    ("cashflow_payables",
      """
      CREATE TABLE IF NOT EXISTS cashflow_payables (
          payable_id        SERIAL PRIMARY KEY,
          source            TEXT NOT NULL DEFAULT 'qbo',
          qbo_bill_id       TEXT,
          supplier          TEXT,
          reference         TEXT,
          description       TEXT,
          amount            DOUBLE PRECISION,
          currency          TEXT DEFAULT 'USD',
          invoice_date      TEXT,
          due_date          TEXT,
          qbo_balance       DOUBLE PRECISION,
          status            TEXT NOT NULL DEFAULT 'pending',
          approved_by       TEXT,
          approved_at       TIMESTAMPTZ,
          paid_date         TEXT,
          paid_amount       DOUBLE PRECISION,
          slack_ts          TEXT,
          notes             TEXT,
          amount_override   DOUBLE PRECISION,
          due_date_override TEXT,
          is_dismissed      INTEGER NOT NULL DEFAULT 0,
          created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_by        TEXT
      );
      """),
    ("cashflow_payables_idx",
      "CREATE UNIQUE INDEX IF NOT EXISTS "
      "idx_cashflow_payables_bill "
      "ON cashflow_payables(qbo_bill_id);"),
    ("cashflow_forecast",
      """
      CREATE TABLE IF NOT EXISTS cashflow_forecast (
          week_start  TEXT NOT NULL,
          row_key     TEXT NOT NULL,
          amount      DOUBLE PRECISION,
          updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_by  TEXT,
          PRIMARY KEY (week_start, row_key)
      );
      """),
    # v2.67.234 — Cashflow scenario planning tables.
    ("cashflow_scenarios",
      """
      CREATE TABLE IF NOT EXISTS cashflow_scenarios (
          name        TEXT PRIMARY KEY,
          created_by  TEXT,
          created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      """),
    ("cashflow_scenario_forecast",
      """
      CREATE TABLE IF NOT EXISTS cashflow_scenario_forecast (
          scenario    TEXT NOT NULL,
          week_start  TEXT NOT NULL,
          row_key     TEXT NOT NULL,
          amount      DOUBLE PRECISION,
          updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_by  TEXT,
          PRIMARY KEY (scenario, week_start, row_key)
      );
      """),
    ("cashflow_custom_rows",
      """
      CREATE TABLE IF NOT EXISTS cashflow_custom_rows (
          scenario    TEXT NOT NULL DEFAULT 'base',
          row_key     TEXT NOT NULL,
          label       TEXT NOT NULL,
          kind        TEXT NOT NULL DEFAULT 'outflow',
          sort_order  INTEGER NOT NULL DEFAULT 100,
          created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY (scenario, row_key)
      );
      """),
    # v2.67.257 Notion database ID registry.
    ("notion_db_ids",
      """
      CREATE TABLE IF NOT EXISTS notion_db_ids (
          name   TEXT PRIMARY KEY,
          db_id  TEXT NOT NULL,
          set_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      """),
    # v2.67.250 Notion KB mirror.
    ("notion_kb_articles",
      """
      CREATE TABLE IF NOT EXISTS notion_kb_articles (
          page_id          TEXT PRIMARY KEY,
          title            TEXT NOT NULL,
          content_md       TEXT,
          url              TEXT,
          category         TEXT,
          notion_edited_at TIMESTAMPTZ,
          synced_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      """),
    ("cashflow_loans",
      """
      CREATE TABLE IF NOT EXISTS cashflow_loans (
          loan_id            SERIAL PRIMARY KEY,
          lender             TEXT NOT NULL,
          principal          DOUBLE PRECISION NOT NULL,
          apr                DOUBLE PRECISION NOT NULL,
          start_date         TEXT NOT NULL,
          first_payment_date TEXT NOT NULL,
          monthly_payment    DOUBLE PRECISION NOT NULL,
          forecast_row_key   TEXT,
          notes              TEXT,
          active             INTEGER NOT NULL DEFAULT 1,
          created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      """),
]


# v2.67.202 — once-per-process gate. Without this, the post-
# cutover migrations re-run on EVERY connect — and once the
# rename migrations have succeeded their first time, every
# subsequent attempt fails with "column doesn't exist" because
# the column was already renamed. That's expected idempotent
# behaviour but it generated huge log spam (Cheran's morning
# diagnostic was hundreds of duplicate warning lines).
_pg_post_cutover_applied = False


def _apply_pg_post_cutover(conn) -> None:
    """Apply post-cutover schema migrations on Postgres. Runs
    once per worker process (gated by _pg_post_cutover_applied).
    Each DDL uses IF NOT EXISTS so re-runs would be no-ops, but
    the one-per-process gate keeps the worker logs clean.

    If a migration genuinely needs to re-run (e.g. you ship a
    new entry to _PG_POST_CUTOVER_TABLES), restart the worker —
    that resets the gate."""
    global _pg_post_cutover_applied
    if _pg_post_cutover_applied:
        return
    for label, ddl in _PG_POST_CUTOVER_TABLES:
        try:
            conn.execute(ddl)
        except Exception as exc:
            # Don't fail the connection over a migration hiccup
            # — log at DEBUG (was WARNING — too noisy when the
            # rename has already succeeded and the column the
            # ALTER wants to rename no longer exists).
            import logging as _lg
            _lg.getLogger("db").debug(
                "PG post-cutover migration %s failed: %s",
                label, exc)
    _pg_post_cutover_applied = True


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    # v2.67.163 — Postgres path delegates entirely to db_dialect
    # so the SQLite path below stays unchanged. The dialect
    # wrapper exposes the sqlite3-compatible interface db.py
    # expects (.execute/.executemany/.executescript/.commit/
    # row dict access/lastrowid).
    if _backend_is_postgres():
        from db_dialect import connect as _pg_connect
        with _pg_connect() as conn:
            # v2.67.167 — Postgres path does NOT re-apply the
            # SQLite-flavored _SCHEMA on every connection.
            # migrate_to_pg.py introspects + translates and is
            # the source of truth for the Postgres schema as of
            # the cutover.
            # v2.67.187 — Apply post-cutover schema migrations
            # for tables added AFTER the initial cutover.
            # Idempotent (CREATE TABLE IF NOT EXISTS).
            _apply_pg_post_cutover(conn)
            yield conn
        return

    # SQLite path — unchanged from prior versions.
    # v2.67.111 — 30s timeout (up from 5s) for write-lock contention
    # from the many concurrent sync writers we now run.
    conn = sqlite3.connect(DB_PATH, isolation_level=None,
                              timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        # v2.67.111 — WAL (Write-Ahead Logging) journal mode.
        # Default rollback journal blocks readers during a write.
        # With 5+ concurrent writers (klaviyo_sync, reviewsio_sync,
        # google_ads_sync, ga4_sync, slack_listener), we hit
        # 'database is locked' errors regularly.
        # WAL lets readers proceed concurrently with writers and
        # only serialises writer-vs-writer. Dramatic concurrency
        # improvement on SQLite for this workload.
        # synchronous=NORMAL is the recommended pairing with WAL —
        # safe (no corruption risk) and ~50% faster than FULL.
        # busy_timeout 30s applies when a writer must wait.
        # All PRAGMAs are idempotent — set every connection but
        # SQLite no-ops if already set.
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")
        except sqlite3.Error:
            pass
        conn.executescript(_SCHEMA)
        _migrate_ui_prefs_widths(conn)
        _migrate_supplier_dropship(conn)
        _migrate_supplier_stockout_recovery(conn)
        _migrate_supplier_cadence(conn)
        _migrate_demand_signal_match_columns(conn)
        _migrate_product_aliases_multi_target(conn)
        _migrate_ad_campaign_skus_spend(conn)  # v2.67.105
        _migrate_ad_campaigns_daily_drop_spend_notnull(conn)  # v2.67.107
        _migrate_ad_campaign_skus_free_listings(conn)  # v2.67.118
        _migrate_po_dispatch_reminders_escalation(conn)  # v2.67.131
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
    order_cadence_days: Optional[int] = None,
    dropship_default: int = 0,
    stockout_min_cover_days: int = 60,
    actor: str,
    note: str = "",
) -> None:
    # v2.67.349 — canonicalise the supplier_name on every save so the
    # DB never accumulates near-duplicates that lookups miss.
    supplier_name = _normalise_supplier_name(supplier_name)
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
                 order_cadence_days,
                 dropship_default, stockout_min_cover_days,
                 set_by, note)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                order_cadence_days = excluded.order_cadence_days,
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
             (int(order_cadence_days)
              if order_cadence_days else None),
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


def _normalise_supplier_name(name) -> str:
    """v2.67.349 — canonicalise supplier names so save/lookup paths
    can't be defeated by invisible whitespace mismatches (NBSP,
    trailing space, double space, leading space). str.split() with no
    args splits on ANY Unicode whitespace including NBSP, then
    " ".join() collapses runs of whitespace to single regular spaces.
    Symmetric: applied at save (set_supplier_config) AND lookup
    (all_supplier_configs returns normalised keys)."""
    return " ".join(str(name or "").split()).strip()


def all_supplier_configs() -> dict:
    """Return {supplier_name: row_as_dict} with normalised keys so
    callers can lookup with either the canonical or a not-quite-canonical
    string and still match."""
    with connect() as c:
        rows = c.execute("SELECT * FROM supplier_config").fetchall()
    return {
        _normalise_supplier_name(r["supplier_name"]): dict(r)
        for r in rows
    }


# ---------------------------------------------------------------------------
# Supplier holidays / shutdowns (v2.67.284)
# ---------------------------------------------------------------------------
def _iso_date(value) -> str:
    """Coerce a date or ISO string to YYYY-MM-DD for DB storage."""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    s = str(value)
    return s[:10]


def add_supplier_holiday(supplier_name: str,
                          start_date,
                          end_date,
                          label: str = "",
                          actor: str = "") -> int:
    """Insert a new supplier closure period. Returns the new row id."""
    with connect() as c:
        cur = c.execute(
            "INSERT INTO supplier_holidays "
            "(supplier_name, start_date, end_date, label, created_by) "
            "VALUES (?, ?, ?, ?, ?)",
            (supplier_name, _iso_date(start_date),
             _iso_date(end_date), label or "", actor or ""))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("supplier_holiday.add", actor, supplier_name,
             f"{_iso_date(start_date)} → {_iso_date(end_date)} "
             f"({label or ''})"))
        return int(cur.lastrowid or 0)


def delete_supplier_holiday(holiday_id: int,
                              actor: str = "") -> None:
    """Remove a closure period by id."""
    with connect() as c:
        c.execute("DELETE FROM supplier_holidays WHERE id = ?",
                   (int(holiday_id),))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("supplier_holiday.delete", actor,
             str(holiday_id), ""))


def get_supplier_holidays(supplier_name: str) -> list:
    """All closure periods for one supplier, oldest first."""
    with connect() as c:
        rows = c.execute(
            "SELECT * FROM supplier_holidays "
            "WHERE supplier_name = ? "
            "ORDER BY start_date", (supplier_name,)).fetchall()
    return [dict(r) for r in rows]


def all_supplier_holidays_by_supplier() -> dict:
    """{supplier_name: [closure_dicts]} — single fetch for the
    reorder engine to look up closures per-supplier without N
    queries."""
    with connect() as c:
        rows = c.execute(
            "SELECT * FROM supplier_holidays "
            "ORDER BY supplier_name, start_date").fetchall()
    out: dict = {}
    for r in rows:
        d = dict(r)
        out.setdefault(d["supplier_name"], []).append(d)
    return out


# ---------------------------------------------------------------------------
# IP observed lead times (v2.67.285)
# ---------------------------------------------------------------------------
def upsert_ip_lead_time(sku: str,
                         observed_lead_time_days: Optional[int],
                         configured_lead_time_days: Optional[int],
                         vendor_name: Optional[str] = None,
                         sales_velocity1: Optional[float] = None,
                         last_received_at: Optional[str] = None) -> None:
    """Insert/update one IP-observed lead-time record, keyed on SKU."""
    if not sku:
        return
    with connect() as c:
        c.execute(
            "INSERT INTO ip_lead_times "
            "(sku, observed_lead_time_days, "
            " configured_lead_time_days, vendor_name, "
            " sales_velocity1, last_received_at, synced_at) "
            "VALUES (?, ?, ?, ?, ?, ?, datetime('now')) "
            "ON CONFLICT(sku) DO UPDATE SET "
            "  observed_lead_time_days = "
            "    excluded.observed_lead_time_days, "
            "  configured_lead_time_days = "
            "    excluded.configured_lead_time_days, "
            "  vendor_name = excluded.vendor_name, "
            "  sales_velocity1 = excluded.sales_velocity1, "
            "  last_received_at = excluded.last_received_at, "
            "  synced_at = datetime('now')",
            (sku, observed_lead_time_days,
             configured_lead_time_days, vendor_name,
             sales_velocity1, last_received_at))


def get_ip_lead_times() -> dict:
    """{sku: dict(observed_lead_time_days, configured_lead_time_days,
    vendor_name, ...)} — the reorder engine's lookup for observed
    actual lead times."""
    with connect() as c:
        rows = c.execute(
            "SELECT * FROM ip_lead_times").fetchall()
    return {r["sku"]: dict(r) for r in rows}


# ---------------------------------------------------------------------------
# Shopify monthly discounts (v2.67.303)
# ---------------------------------------------------------------------------
def upsert_shopify_monthly_discounts(month: str,
                                       total_discounts: float,
                                       order_count: int) -> None:
    """Upsert one (month, total_discounts, order_count) row. Month
    is 'YYYY-MM'. total_discounts is the SUM of all
    Shopify `total_discounts` values on orders created that month
    excluding cancelled. Coupons + auto promos + shipping discounts
    + line-level discounts + draft adjustments all roll into this."""
    if not month:
        return
    with connect() as c:
        c.execute(
            "INSERT INTO shopify_monthly_discounts "
            "(month, total_discounts, order_count, synced_at) "
            "VALUES (?, ?, ?, datetime('now')) "
            "ON CONFLICT(month) DO UPDATE SET "
            "  total_discounts = excluded.total_discounts, "
            "  order_count = excluded.order_count, "
            "  synced_at = datetime('now')",
            (month, float(total_discounts), int(order_count)))


def get_shopify_monthly_discounts(month: str) -> Optional[Dict]:
    """Return one month's row or None."""
    with connect() as c:
        row = c.execute(
            "SELECT * FROM shopify_monthly_discounts "
            "WHERE month = ?", (month,)).fetchone()
    return dict(row) if row else None


def all_shopify_monthly_discounts() -> Dict[str, float]:
    """Return {month: total_discounts} — used by the Monthly
    Metrics page to populate Section 6's discount row."""
    with connect() as c:
        rows = c.execute(
            "SELECT month, total_discounts FROM "
            "shopify_monthly_discounts").fetchall()
    return {r["month"]: float(r["total_discounts"] or 0)
            for r in rows}


# ---------------------------------------------------------------------------
# QBO monthly P&L + account mappings (v2.67.292)
# ---------------------------------------------------------------------------
def upsert_qbo_monthly_pl(month: str,
                          account_id: Optional[str],
                          account_number: Optional[str],
                          account_name: str,
                          amount: float,
                          account_type: Optional[str] = None,
                          parent_account_id: Optional[str] = None) -> None:
    """Insert/update one (month, account) → amount row from QBO."""
    if not month or not account_name:
        return
    with connect() as c:
        c.execute(
            "INSERT INTO qbo_monthly_pl "
            "(month, account_id, account_number, account_name, "
            " account_type, parent_account_id, amount, synced_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now')) "
            "ON CONFLICT(month, account_id, account_name) DO UPDATE "
            "SET account_number = excluded.account_number, "
            "    account_type = excluded.account_type, "
            "    parent_account_id = excluded.parent_account_id, "
            "    amount = excluded.amount, "
            "    synced_at = datetime('now')",
            (month, account_id or "", account_number,
             account_name, account_type, parent_account_id,
             float(amount)))


def batch_upsert_qbo_monthly_pl(rows: list) -> int:
    """v2.67.293 — bulk upsert variant. Opens ONE DB connection and
    executes every upsert inside it instead of per-row connecting.
    The per-row variant above took 8 minutes for 868 rows on the
    Render Postgres because each `with connect()` paid connection
    setup latency; this version completes in seconds.

    `rows` is a list of dicts with keys: month, account_id,
    account_number, account_name, amount, account_type,
    parent_account_id. Returns the number of rows actually written
    (skipping any with missing month or account_name)."""
    if not rows:
        return 0
    sql = (
        "INSERT INTO qbo_monthly_pl "
        "(month, account_id, account_number, account_name, "
        " account_type, parent_account_id, amount, synced_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now')) "
        "ON CONFLICT(month, account_id, account_name) DO UPDATE "
        "SET account_number = excluded.account_number, "
        "    account_type = excluded.account_type, "
        "    parent_account_id = excluded.parent_account_id, "
        "    amount = excluded.amount, "
        "    synced_at = datetime('now')")
    n = 0
    with connect() as c:
        for r in rows:
            month = (r.get("month") or "").strip()
            name = (r.get("account_name") or "").strip()
            if not month or not name:
                continue
            try:
                c.execute(sql, (
                    month,
                    r.get("account_id") or "",
                    r.get("account_number"),
                    name,
                    r.get("account_type"),
                    r.get("parent_account_id"),
                    float(r.get("amount") or 0)))
                n += 1
            except Exception:  # noqa: BLE001
                # Single-row failure: continue with the rest.
                # The per-row helper above does the same; here we
                # don't want one bad row to blow up the batch.
                continue
    return n


def get_qbo_monthly_pl(start_month: Optional[str] = None,
                       end_month: Optional[str] = None) -> list:
    """Return all qbo_monthly_pl rows, optionally bounded by month
    range (inclusive). Months are 'YYYY-MM' strings."""
    sql = "SELECT * FROM qbo_monthly_pl"
    clauses: list = []
    params: list = []
    if start_month:
        clauses.append("month >= ?")
        params.append(start_month)
    if end_month:
        clauses.append("month <= ?")
        params.append(end_month)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY month, account_number, account_name"
    with connect() as c:
        rows = c.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def qbo_monthly_pl_summary_by_category(
        category_to_mapping: dict) -> dict:
    """Aggregate qbo_monthly_pl rows by category and month.

    category_to_mapping: {category_name: {'account_numbers': [...],
                                          'account_names': [...]}}
        — or for backward-compat, {category_name: [account_number...]}
        which is treated as account_numbers only.

    Returns: {month: {category_name: amount}}.

    v2.67.294 — extended to allow account-name matching too, so QB
    subtotal rows like 'Total Income' / 'Total Cost of Goods Sold'
    (which have no AcctNum) can be captured as their own categories.
    An account can match MULTIPLE categories (e.g. acc#500 can live
    in BOTH 'cogs' and 'total_cogs' if both mappings include it)."""
    if not category_to_mapping:
        return {}
    # Normalise each category's mapping into (number_set, name_set).
    cat_specs: dict = {}
    for cat, m in category_to_mapping.items():
        if isinstance(m, dict):
            nums = m.get("account_numbers") or []
            names = m.get("account_names") or []
        else:
            nums, names = list(m or []), []
        cat_specs[cat] = (
            {str(n).strip() for n in nums if str(n).strip()},
            {str(n).strip().lower() for n in names
             if str(n).strip()},
        )
    if not cat_specs:
        return {}
    out: dict = {}
    with connect() as c:
        rows = c.execute(
            "SELECT month, account_number, account_name, amount "
            "FROM qbo_monthly_pl").fetchall()
    for r in rows:
        d = dict(r)
        num = str(d.get("account_number") or "").strip()
        name = str(d.get("account_name") or "").strip().lower()
        amount = float(d.get("amount") or 0)
        month = d.get("month") or ""
        if not month:
            continue
        for cat, (num_set, name_set) in cat_specs.items():
            if (num and num in num_set) or (name and name in name_set):
                out.setdefault(month, {})
                out[month][cat] = out[month].get(cat, 0.0) + amount
    return out


def get_qbo_account_mappings() -> dict:
    """{category: {'account_numbers': [...], 'account_names': [...]}}
    — the canonical mapping config (editable per company)."""
    with connect() as c:
        rows = c.execute(
            "SELECT * FROM qbo_account_mappings").fetchall()
    out: dict = {}
    for r in rows:
        d = dict(r)
        nums = [x.strip() for x in (d.get("account_numbers") or ""
                                     ).split(",") if x.strip()]
        names = [x.strip() for x in (d.get("account_names") or ""
                                      ).split(",") if x.strip()]
        out[d["category"]] = {
            "account_numbers": nums,
            "account_names": names,
            "notes": d.get("notes") or "",
        }
    return out


def set_qbo_account_mapping(category: str,
                             account_numbers: Optional[list] = None,
                             account_names: Optional[list] = None,
                             notes: str = "",
                             actor: str = "") -> None:
    """Upsert a category → account mapping. Pass account_numbers and/
    or account_names as lists; they're stored as comma-separated."""
    nums_csv = ",".join(str(n).strip() for n in (account_numbers or [])
                        if str(n).strip())
    names_csv = ",".join(str(n).strip() for n in (account_names or [])
                         if str(n).strip())
    with connect() as c:
        c.execute(
            "INSERT INTO qbo_account_mappings "
            "(category, account_numbers, account_names, notes, "
            " set_by) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(category) DO UPDATE SET "
            "    account_numbers = excluded.account_numbers, "
            "    account_names = excluded.account_names, "
            "    notes = excluded.notes, "
            "    set_by = excluded.set_by, "
            "    set_at = datetime('now')",
            (category, nums_csv, names_csv, notes, actor))


# Pre-seed default mappings on first read so the page works out of
# the box for W4S's chart of accounts (per Viktor's audit).
# v2.67.294 — extended to include broader QB summary rows so the
# Monthly Metrics page can show both the narrow product-only view
# AND the QB-canonical "Total Income / Total COGS / Net Income"
# view side by side. Each entry below is (account_numbers,
# account_names, notes). Either matcher is sufficient.
_DEFAULT_QBO_MAPPINGS = {
    # Narrow product-only views (existing).
    "sales": (
        ["400"], [],
        "Product sales only (acc 400). Matches the buyer-team view."),
    "shipping_charged": (
        ["405"], [],
        "Acc 405 Sales - Shipping. Customer-facing freight income."),
    "cogs": (
        ["500"], [],
        "Acc 500 product COGS only. Excludes Amazon fees + "
        "inventory adjustments."),
    "shipping_cost": (
        ["694"], [],
        "Acc 694 Shipping-Out. Parcel + freight billed out."),
    # Broader QB-canonical views (v2.67.294, option C).
    "total_income": (
        [], ["Total Income"],
        "QB's own 'Total Income' summary row. Sales + shipping "
        "income + sundry / billable expense income."),
    "total_cogs": (
        ["500", "502", "550"], ["Total Cost of Goods Sold"],
        "QB's 'Total COGS' (acc 500 + 502 Amazon fees + 550 "
        "inventory adjustments). Matches QB's GP calculation."),
    "qb_gross_profit": (
        [], ["Gross Profit"],
        "QB's own 'Gross Profit' summary row (Total Income − "
        "Total COGS). Compare with our app's GP for variance."),
    "qb_total_expenses": (
        [], ["Total Expenses"],
        "QB's 'Total Expenses' summary row (all operating "
        "expenses, ex COGS)."),
    "qb_net_operating_income": (
        [], ["Net Operating Income"],
        "QB's 'Net Operating Income' summary row "
        "(Gross Profit − Total Expenses)."),
    "qb_net_income": (
        [], ["Net Income"],
        "QB's bottom-line 'Net Income' summary row."),
    # Granular cost / income components for visibility.
    "amazon_sales": (
        ["403"], [],
        "Acc 403 Sales - Amazon (separate from main sales line)."),
    "cogs_amazon_fees": (
        ["502"], [],
        "Acc 502 COGS - Amazon Fees."),
    "inventory_adjustment": (
        ["550"], [],
        "Acc 550 Inventory Adjustment — write-offs / write-ons."),
    "sundry_income": (
        [], ["Sundry Income - Billable Exps",
              "Billable Expense Income"],
        "Sundry billable-expense income (no account number on "
        "the P&L)."),
    "packaging_cost": (
        ["690"], [],
        "Acc 690 Shipping - Packaging & Consumables. Real cost "
        "of fulfilling but not on the carrier label."),
    "shipping_in": (
        ["692"], [],
        "Acc 692 Shipping-In. Inbound freight (usually "
        "capitalised into COGS but tracked separately for "
        "visibility)."),
}


def seed_default_qbo_account_mappings(actor: str = "system") -> int:
    """Insert default mappings for any category not yet set.
    Returns the count of mappings created."""
    existing = get_qbo_account_mappings()
    n = 0
    for cat, spec in _DEFAULT_QBO_MAPPINGS.items():
        if cat in existing:
            continue
        if isinstance(spec, tuple):
            nums, names, notes = spec
        else:
            # legacy: bare list of account numbers
            nums, names, notes = spec, [], "Default (W4S)"
        set_qbo_account_mapping(
            cat,
            account_numbers=nums,
            account_names=names,
            notes=notes,
            actor=actor)
        n += 1
    return n


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


def clear_sku_supplier(sku: str, actor: str = "") -> None:
    """v2.67.321 — remove a per-SKU supplier override so the SKU falls
    back to CIN7-native / family-rule / PO-history resolution."""
    with connect() as c:
        c.execute(
            "DELETE FROM sku_supplier_overrides WHERE sku = ?", (sku,))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("sku_supplier.clear", actor, sku, ""),
        )


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
            # v2.67.313 — `user` and `view` are reserved in Postgres;
            # double-quoted identifiers work in both SQLite & Postgres.
            'SELECT columns_csv FROM ui_prefs '
            'WHERE "user" = ? AND "view" = ?',
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
            INSERT INTO ui_prefs ("user", "view", columns_csv, updated_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT("user", "view") DO UPDATE SET
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
            'SELECT widths_csv FROM ui_prefs '
            'WHERE "user" = ? AND "view" = ?',
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
            'SELECT columns_csv FROM ui_prefs '
            'WHERE "user" = ? AND "view" = ?',
            (user, view),
        ).fetchone()
        cols_val = existing["columns_csv"] if existing else ""
        c.execute(
            """
            INSERT INTO ui_prefs ("user", "view", columns_csv, widths_csv, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT("user", "view") DO UPDATE SET
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
            'DELETE FROM ui_prefs WHERE "user" = ? AND "view" = ?',
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
            INSERT INTO ui_presets ("user", "view", name, columns_csv,
                                     widths_csv, created_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT("user", "view", name) DO UPDATE SET
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
            'SELECT name, columns_csv, widths_csv, created_at '
            'FROM ui_presets WHERE "user" = ? AND "view" = ? '
            'ORDER BY created_at DESC',
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
            'DELETE FROM ui_presets '
            'WHERE "user" = ? AND "view" = ? AND name = ?',
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


def list_ai_corrections(limit: int = 30) -> List[sqlite3.Row]:
    """v2.67.33 — return recent on-the-fly corrections users have
    written under AI answers. These accumulate over time as a
    'memory' the AI references in its system prompt — every
    correction sharpens future answers without requiring code
    changes or schema additions.

    Source: ai_audit_logs.feedback_note where the user explicitly
    typed a correction. Sorted newest first; the AI gets the most
    recent N.

    Excludes empty notes and rows where the user has explicitly
    archived the correction (feedback='archived')."""
    with connect() as c:
        rows = c.execute(
            """
            SELECT id,
                   user_question,
                   feedback_note,
                   feedback,
                   user_id,
                   created_at
              FROM ai_audit_logs
             WHERE feedback_note IS NOT NULL
               AND TRIM(feedback_note) != ''
               AND COALESCE(feedback, '') != 'archived'
             ORDER BY created_at DESC
             LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    return list(rows)


def archive_ai_correction(audit_id: int,
                            user_id: str = "") -> None:
    """v2.67.33 — flag a correction as archived so it stops being
    fed back into the system prompt. Sets feedback='archived' on
    the audit row. The original feedback_note text is preserved
    so we still have a record of what the user wanted."""
    with connect() as c:
        c.execute(
            "UPDATE ai_audit_logs SET feedback = 'archived' "
            "WHERE id = ?",
            (int(audit_id),),
        )
        c.execute(
            """
            INSERT INTO feedback_events
                (source, entity_type, entity_id, feedback, note, user_id)
            VALUES ('ai_chat', 'ai_audit_log', ?, 'archived',
                    'correction archived', ?)
            """,
            (str(audit_id), user_id),
        )


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
                          rule_type: Optional[str] = None,
                          target_skus: Optional[list] = None,
                          target_families: Optional[list] = None,
                          attributes: Optional[dict] = None,
                          search_fields: Optional[list] = None,
                          confidence: float = 0.5,
                          approved_by: str = "ai",
                          source: str = "manual",
                          created_by: Optional[str] = None) -> int:
    """Store/update a phrase → target mapping. Phrase is lowercased.
    On collision (same phrase + same target shape), bump times_used.
    Writes an audit_log row on every insert/update.

    Two compatible call shapes:

      Legacy single-target (pre-v2.63):
        upsert_product_alias(phrase, sku='LED-X', confidence=0.9)
        upsert_product_alias(phrase, product_family='SIERRA38')

      Multi-target (v2.63+):
        upsert_product_alias(phrase, rule_type='sku_list',
                             target_skus=['LED-A','LED-B'])
        upsert_product_alias(phrase, rule_type='family_list',
                             target_families=['SIERRA38','SMOKIES38'])
        upsert_product_alias(phrase, rule_type='attributes',
                             attributes={'product_type':'LED strip',
                                         'kelvin':[2200,2700]})

    Both shapes round-trip through the same row. The single-target
    fields (sku, product_family) stay populated when the rule is a
    single target so legacy readers keep working.

    Returns the id of the row (existing or newly inserted).
    """
    import json as _json

    phrase_n = (phrase or "").strip().lower()
    if not phrase_n:
        return 0

    # Normalise inputs. If caller passed a single sku/family, hoist
    # them into the corresponding rule_type/list shape so the row is
    # stored consistently going forward.
    target_skus = [s for s in (target_skus or []) if s and s.strip()]
    target_families = [f.strip().upper() for f in (target_families or [])
                        if f and f.strip()]
    search_fields = [str(f).strip().lower()
                      for f in (search_fields or [])
                      if f and str(f).strip()]
    if rule_type is None:
        if search_fields:
            rule_type = "text_search"
        elif attributes:
            rule_type = "attributes"
        elif len(target_skus) > 1:
            rule_type = "sku_list"
        elif len(target_families) > 1:
            rule_type = "family_list"
        elif sku and product_family:
            rule_type = "mixed"
        elif sku:
            rule_type = "sku"
        elif product_family:
            rule_type = "family"
        elif target_skus:
            rule_type = "sku"
            sku = sku or target_skus[0]
        elif target_families:
            rule_type = "family"
            product_family = product_family or target_families[0]

    target_skus_json = _json.dumps(target_skus) if target_skus else None
    target_families_json = (_json.dumps(target_families)
                              if target_families else None)
    attributes_json = (_json.dumps(attributes, sort_keys=True)
                        if attributes else None)
    search_fields_json = (_json.dumps(search_fields)
                           if search_fields else None)

    with connect() as c:
        # Collision detection: same phrase + same canonical target
        # shape. For multi-target rules we compare the JSON blobs so
        # two distinct lists don't collapse to one row.
        existing = c.execute(
            "SELECT id, times_used FROM product_aliases "
            "WHERE phrase = ? "
            "AND COALESCE(sku, '') = COALESCE(?, '') "
            "AND COALESCE(product_family, '') = COALESCE(?, '') "
            "AND COALESCE(target_skus_json, '') = COALESCE(?, '') "
            "AND COALESCE(target_families_json, '') = COALESCE(?, '') "
            "AND COALESCE(attributes_json, '') = COALESCE(?, '') "
            "AND COALESCE(search_fields_json, '') = COALESCE(?, '')",
            (phrase_n, sku, product_family,
             target_skus_json, target_families_json, attributes_json,
             search_fields_json),
        ).fetchone()
        if existing:
            c.execute(
                "UPDATE product_aliases SET "
                "times_used = times_used + 1, "
                "last_used_at = datetime('now'), "
                "confidence = MAX(confidence, ?), "
                "rule_type = COALESCE(rule_type, ?) "
                "WHERE id = ?",
                (confidence, rule_type, existing["id"]),
            )
            row_id = int(existing["id"])
            c.execute(
                "INSERT INTO audit_log (event, actor, target, detail) "
                "VALUES (?, ?, ?, ?)",
                ("product_alias.bump", approved_by, str(row_id),
                 f"phrase='{phrase_n}' rule_type={rule_type or ''} "
                 f"confidence={confidence}"))
            return row_id
        else:
            cur = c.execute(
                """
                INSERT INTO product_aliases
                    (phrase, sku, product_family, rule_type,
                     target_skus_json, target_families_json,
                     attributes_json, search_fields_json,
                     confidence, approved_by, source, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (phrase_n, sku, product_family, rule_type,
                 target_skus_json, target_families_json,
                 attributes_json, search_fields_json,
                 confidence, approved_by,
                 source, created_by or approved_by),
            )
            row_id = int(cur.lastrowid)
            c.execute(
                "INSERT INTO audit_log (event, actor, target, detail) "
                "VALUES (?, ?, ?, ?)",
                ("product_alias.insert", approved_by, str(row_id),
                 f"phrase='{phrase_n}' rule_type={rule_type or ''} "
                 f"sku={sku or ''} family={product_family or ''} "
                 f"n_skus={len(target_skus)} "
                 f"n_families={len(target_families)} "
                 f"has_attributes={bool(attributes)} "
                 f"search_fields={search_fields or ''} "
                 f"confidence={confidence}"))
            return row_id


def aliases_for_phrase(phrase: str) -> List[sqlite3.Row]:
    """Return ALL rules whose phrase exactly matches `phrase` (after
    normalising to lowercase). Used by the AI Feedback page to show
    'this phrase already has the following rules' BEFORE the user
    saves a new one — prevents accidental duplicates and lets the
    reviewer see what's there before adding more."""
    phrase_n = (phrase or "").strip().lower()
    if not phrase_n:
        return []
    with connect() as c:
        return c.execute(
            "SELECT * FROM product_aliases WHERE phrase = ? "
            "ORDER BY confidence DESC, times_used DESC",
            (phrase_n,),
        ).fetchall()


def list_product_aliases(*,
                          sku: Optional[str] = None,
                          product_family: Optional[str] = None,
                          phrase_contains: Optional[str] = None,
                          min_confidence: float = 0.0,
                          limit: int = 500) -> List[sqlite3.Row]:
    """Browse the alias table — used by the AI Feedback page to show
    what's already been learned. All filters optional."""
    sql = "SELECT * FROM product_aliases WHERE confidence >= ?"
    params: list = [min_confidence]
    if sku:
        sql += " AND sku = ?"
        params.append(sku)
    if product_family:
        sql += " AND product_family = ?"
        params.append(product_family)
    if phrase_contains:
        sql += " AND phrase LIKE ?"
        params.append(f"%{phrase_contains.strip().lower()}%")
    sql += " ORDER BY times_used DESC, last_used_at DESC LIMIT ?"
    params.append(int(limit))
    with connect() as c:
        return c.execute(sql, params).fetchall()


def delete_product_alias(alias_id: int, actor: str = "system") -> None:
    """Remove an alias mapping. Used by the AI Feedback page when a
    correction was wrong / no longer applies."""
    with connect() as c:
        c.execute("DELETE FROM product_aliases WHERE id = ?", (alias_id,))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("product_alias.delete", actor, str(alias_id), ""))


def find_alias_in_question(question: str,
                            min_confidence: float = 0.6) -> List[dict]:
    """Best-effort substring match of stored alias phrases against the
    user's question. Returns hits longest-phrase-first, deduped by
    target shape so a phrase mapped twice to the same target only
    surfaces once.

    Each hit dict (v2.63 multi-target shape):
        {
          "id": int,
          "phrase": str,
          "rule_type": str | None,    # 'sku'/'sku_list'/'family'/
                                        # 'family_list'/'mixed'/'attributes'
          "skus":         list[str],   # always populated; single rules
                                        # surface as a 1-element list,
                                        # multi-rules surface fully
          "families":     list[str],
          "attributes":   dict,
          "confidence":   float,
          "times_used":   int,
        }

    Callers that want the legacy single-target fields can read
    hit['skus'][0] / hit['families'][0] when len()==1.

    Used by the AI Assistant page to inject 'past corrections' hints
    into the system prompt before the LLM call. Substring match
    catches realistic phrasing (alias 'warm strip' inside 'do we have
    any warm strip in stock?') without requiring exact full-question
    matches.
    """
    import json as _json

    q = (question or "").strip().lower()
    if not q:
        return []
    with connect() as c:
        rows = c.execute(
            "SELECT * FROM product_aliases WHERE confidence >= ? "
            "ORDER BY length(phrase) DESC, times_used DESC LIMIT 2000",
            (min_confidence,),
        ).fetchall()

    def _decode_list(blob):
        if not blob:
            return []
        try:
            v = _json.loads(blob)
            return [str(x) for x in v] if isinstance(v, list) else []
        except (ValueError, TypeError):
            return []

    def _decode_dict(blob):
        if not blob:
            return {}
        try:
            v = _json.loads(blob)
            return dict(v) if isinstance(v, dict) else {}
        except (ValueError, TypeError):
            return {}

    hits: list[dict] = []
    seen_targets: set = set()
    for r in rows:
        phrase = (r["phrase"] or "").strip().lower()
        if not phrase or phrase not in q:
            continue

        # Compose unified multi-target view of this row. Single-target
        # legacy rows (sku XOR family populated) get hoisted into the
        # list shape so callers don't need to special-case.
        cols = r.keys() if hasattr(r, "keys") else []

        skus = (_decode_list(r["target_skus_json"])
                if "target_skus_json" in cols else [])
        families = (_decode_list(r["target_families_json"])
                    if "target_families_json" in cols else [])
        attrs = (_decode_dict(r["attributes_json"])
                  if "attributes_json" in cols else {})
        search_fields = (_decode_list(r["search_fields_json"])
                          if "search_fields_json" in cols else [])

        if not skus and r["sku"]:
            skus = [str(r["sku"])]
        if not families and r["product_family"]:
            families = [str(r["product_family"])]

        # Dedup key uses the FULL target shape so a phrase mapped to
        # different sets of SKUs surfaces twice (intended).
        key = (
            tuple(sorted(s.upper() for s in skus)),
            tuple(sorted(f.upper() for f in families)),
            tuple(sorted(attrs.items())),
            tuple(sorted(search_fields)),
        )
        if key in seen_targets:
            continue
        seen_targets.add(key)

        rule_type = (r["rule_type"]
                      if "rule_type" in cols and r["rule_type"]
                      else None)
        if rule_type is None:
            # Infer for fully legacy rows.
            if search_fields:
                rule_type = "text_search"
            elif attrs:
                rule_type = "attributes"
            elif len(skus) > 1:
                rule_type = "sku_list"
            elif len(families) > 1:
                rule_type = "family_list"
            elif skus and families:
                rule_type = "mixed"
            elif skus:
                rule_type = "sku"
            elif families:
                rule_type = "family"

        hits.append({
            "id": int(r["id"]),
            "phrase": phrase,
            "rule_type": rule_type,
            "skus": skus,
            "families": families,
            "attributes": attrs,
            "search_fields": search_fields,
            "confidence": float(r["confidence"] or 0),
            "times_used": int(r["times_used"] or 0),
        })
    return hits


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
# Users / profiles (v2.66)
# ---------------------------------------------------------------------------
#
# Lightweight profile lookup. After the shared password gate, the user
# picks (or types) their name; we load a row from `users` so audit logs
# and forms can show / use their display_name + role + defaults.
# Per-user authentication is deliberately NOT in scope — see schema
# comment on the users table.

# v2.67.207 — super_admin is the top tier. Only super_admins can
# open the User Permissions page, change anyone's role, or
# create/manage users. Regular 'admin' keeps full PAGE access
# but cannot escalate privileges or manage other users.
USER_ROLES = ("buyer", "sales", "admin", "super_admin", "viewer")
DEFAULT_NEW_USER_ROLE = "sales"

# Names listed in this env var are ALWAYS treated as super_admin,
# regardless of their stored users.role. This solves the
# bootstrap problem (you can't reach the super-admin-only User
# Permissions page to make yourself super_admin if nobody is one
# yet) and acts as a break-glass override that survives DB
# resets. Comma-separated display names, case-insensitive.
SUPER_ADMIN_ENV = "SUPER_ADMIN_NAMES"


def is_super_admin(display_name: str = "", role: str = "") -> bool:
    """v2.67.207 — True if the user is a super-admin via EITHER:
      1. their users.role == 'super_admin', OR
      2. their display_name appears in the SUPER_ADMIN_NAMES
         env var (bootstrap / break-glass).
    Case-insensitive on both."""
    import os as _os
    if (role or "").strip().lower() == "super_admin":
        return True
    name_lc = (display_name or "").strip().lower()
    if not name_lc:
        return False
    env_raw = _os.environ.get(SUPER_ADMIN_ENV, "")
    env_names = {n.strip().lower()
                    for n in env_raw.split(",") if n.strip()}
    return name_lc in env_names


# ---------------------------------------------------------------------------
# User sessions (v2.67.302) — survive worker restarts / deploys
# ---------------------------------------------------------------------------
def _new_session_token() -> str:
    """URL-safe random token (43 chars, 256 bits of entropy)."""
    import secrets
    return secrets.token_urlsafe(32)


def create_user_session(user_id: int,
                         ttl_hours: int = 24) -> str:
    """Create a fresh login session for `user_id` and return the
    token (caller stashes it in the browser URL via st.query_params).
    24-hour TTL by default; the validate_user_session call below
    extends it on every page view, so an active user stays signed
    in indefinitely."""
    token = _new_session_token()
    with connect() as c:
        c.execute(
            "INSERT INTO user_sessions "
            "(token, user_id, expires_at) VALUES "
            "(?, ?, datetime('now', '+' || ? || ' hours'))",
            (token, int(user_id), int(ttl_hours)))
    return token


def validate_user_session(token: str,
                           renew_hours: int = 24
                           ) -> Optional[Dict]:
    """Check the token. If valid and not expired, return the
    user dict and SLIDING-RENEW the expiry to (now + renew_hours).
    Returns None if the token is unknown or expired."""
    if not token:
        return None
    with connect() as c:
        row = c.execute(
            "SELECT s.token, s.user_id, s.expires_at, "
            "       u.display_name, u.role, u.email, "
            "       u.default_page, u.active "
            "FROM user_sessions s "
            "JOIN users u ON u.user_id = s.user_id "
            "WHERE s.token = ? "
            "  AND s.expires_at > datetime('now') "
            "  AND u.active = 1",
            (token,)).fetchone()
        if not row:
            return None
        # Sliding renewal — push expiry forward, refresh last_used.
        c.execute(
            "UPDATE user_sessions SET "
            "  expires_at = datetime('now', '+' || ? || ' hours'), "
            "  last_used_at = datetime('now') "
            "WHERE token = ?",
            (int(renew_hours), token))
    return {
        "user_id": int(row["user_id"]),
        "display_name": row["display_name"],
        "role": row["role"],
        "email": row["email"],
        "default_page": row["default_page"],
        "active": bool(row["active"]),
    }


def revoke_user_session(token: str) -> None:
    """Delete a session token (sign-out)."""
    if not token:
        return
    with connect() as c:
        c.execute("DELETE FROM user_sessions WHERE token = ?",
                  (token,))


def cleanup_expired_user_sessions() -> int:
    """Housekeeping — drop expired session rows. Safe to call
    on every page load (cheap; rows are removed by the index)."""
    try:
        with connect() as c:
            cur = c.execute(
                "DELETE FROM user_sessions "
                "WHERE expires_at <= datetime('now')")
            return int(cur.rowcount or 0)
    except Exception:  # noqa: BLE001
        return 0


def get_user_by_name(display_name: str) -> Optional[sqlite3.Row]:
    """Look up a user profile by display_name (case-insensitive). Returns
    None if no row matches. v2.67.168 — LOWER() on both sides works
    on SQLite and Postgres; the original `COLLATE NOCASE` is
    SQLite-only and breaks on Postgres."""
    name = (display_name or "").strip()
    if not name:
        return None
    with connect() as c:
        return c.execute(
            "SELECT * FROM users WHERE LOWER(display_name) = LOWER(?)",
            (name,),
        ).fetchone()


def list_users(active_only: bool = True) -> List[sqlite3.Row]:
    """All users in the system, ordered by display_name. Set
    active_only=False to include soft-deactivated rows (rare — used by
    the admin profile page). v2.67.168 — LOWER(col) ORDER BY works
    on both backends; COLLATE NOCASE is SQLite-only."""
    sql = "SELECT * FROM users"
    if active_only:
        sql += " WHERE active = 1"
    sql += " ORDER BY LOWER(display_name)"
    with connect() as c:
        return c.execute(sql).fetchall()


def upsert_user(*,
                  display_name: str,
                  role: str = DEFAULT_NEW_USER_ROLE,
                  email: Optional[str] = None,
                  active: bool = True,
                  default_page: Optional[str] = None,
                  actor: Optional[str] = None) -> int:
    """Create-or-update a user profile. Match is on display_name
    (case-insensitive). Returns user_id.

    The role argument is validated against USER_ROLES; unknown values
    fall back to DEFAULT_NEW_USER_ROLE rather than raising — the app
    should never crash because someone typed 'Buyer' instead of
    'buyer'.

    Audit-logged on every mutation. `actor` defaults to the
    display_name being upserted (self-edit) when not provided.
    """
    name = (display_name or "").strip()
    if not name:
        raise ValueError("display_name is required")
    role_norm = (role or DEFAULT_NEW_USER_ROLE).strip().lower()
    if role_norm not in USER_ROLES:
        role_norm = DEFAULT_NEW_USER_ROLE
    actor_eff = (actor or name).strip()

    with connect() as c:
        # v2.67.168 — LOWER() works on both backends; COLLATE
        # NOCASE is SQLite-only and Postgres has no equivalent
        # built-in collation.
        existing = c.execute(
            "SELECT user_id FROM users "
            "WHERE LOWER(display_name) = LOWER(?)",
            (name,),
        ).fetchone()
        if existing:
            uid = int(existing["user_id"])
            c.execute(
                "UPDATE users SET "
                "role = ?, email = ?, active = ?, default_page = ?, "
                "updated_at = datetime('now') "
                "WHERE user_id = ?",
                (role_norm, email, 1 if active else 0, default_page, uid),
            )
            c.execute(
                "INSERT INTO audit_log (event, actor, target, detail) "
                "VALUES (?, ?, ?, ?)",
                ("user.update", actor_eff, str(uid),
                 f"display_name='{name}' role={role_norm} "
                 f"active={int(bool(active))} "
                 f"default_page={default_page or ''}"))
            return uid
        cur = c.execute(
            """
            INSERT INTO users
                (display_name, role, email, active, default_page)
            VALUES (?, ?, ?, ?, ?)
            """,
            (name, role_norm, email, 1 if active else 0, default_page),
        )
        uid = int(cur.lastrowid)
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("user.insert", actor_eff, str(uid),
             f"display_name='{name}' role={role_norm}"))
        return uid


def get_or_create_user(display_name: str) -> sqlite3.Row:
    """Convenience for the sign-in flow: if the user exists, return
    them; if not, create a basic profile with the default role and
    return it. Used when the user types a name not yet in the system.
    """
    name = (display_name or "").strip()
    if not name:
        raise ValueError("display_name is required")
    existing = get_user_by_name(name)
    if existing is not None:
        return existing
    upsert_user(
        display_name=name, role=DEFAULT_NEW_USER_ROLE, active=True,
        actor="self-signin")
    new_row = get_user_by_name(name)
    assert new_row is not None  # just inserted
    return new_row


# ---------------------------------------------------------------------------
# v2.67.185 — Per-user page permissions
# ---------------------------------------------------------------------------
def get_user_page_permissions(user_id: int) -> dict:
    """Return {page_name: bool} of explicit permission rows for
    this user. Empty dict if no rows exist (= backwards-compat
    'see everything' mode)."""
    if not user_id:
        return {}
    with connect() as c:
        rows = c.execute(
            "SELECT page_name, allowed "
            "FROM user_page_permissions WHERE user_id = ?",
            (int(user_id),)).fetchall()
    return {r["page_name"]: bool(r["allowed"]) for r in rows}


def set_user_page_permissions(user_id: int,
                                 allowed_pages: list,
                                 all_pages: list,
                                 set_by: str) -> None:
    """Bulk-replace a user's page permissions. `allowed_pages` is
    the list of pages the user CAN access; everything else in
    `all_pages` gets stored with allowed=0.

    Storing the explicit-deny rows (rather than just absences) is
    deliberate: it disambiguates 'never configured' (no rows =
    see-everything default) from 'configured to see nothing' (all
    rows allowed=0). The admin UI shows checkboxes for every
    page; pressing Save writes rows for all of them.

    Audit-logged on every save."""
    allowed_set = {str(p).strip()
                      for p in (allowed_pages or [])
                      if str(p).strip()}
    all_set = {str(p).strip()
                  for p in (all_pages or [])
                  if str(p).strip()}
    if not all_set:
        return
    actor = (set_by or "").strip() or "system"
    with connect() as c:
        for page in sorted(all_set):
            allowed = 1 if page in allowed_set else 0
            c.execute(
                "INSERT INTO user_page_permissions "
                "(user_id, page_name, allowed, set_by) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(user_id, page_name) DO UPDATE SET "
                "  allowed = excluded.allowed, "
                "  set_by  = excluded.set_by, "
                "  set_at  = datetime('now')",
                (int(user_id), page, allowed, actor))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("user_permissions.set", actor, str(user_id),
              f"{len(allowed_set)} of {len(all_set)} pages "
              "allowed"))


def can_user_access_page(user_id: int, page_name: str,
                              role: str = "sales") -> bool:
    """Permission gate. Returns True when:
      • role == 'admin' (always — admins bypass the table)
      • OR the user has zero permission rows (= unconfigured,
        backwards-compat see-everything default)
      • OR the row for this page has allowed=1
    Returns False only when the row exists AND allowed=0."""
    if (role or "").strip().lower() == "admin":
        return True
    perms = get_user_page_permissions(user_id)
    if not perms:
        # Never configured — see everything (backwards compat).
        return True
    return bool(perms.get(page_name, False))


def clear_user_page_permissions(user_id: int,
                                       set_by: str) -> None:
    """Reset a user's permissions to 'unconfigured' (deletes all
    rows). They'll fall back to the see-everything default."""
    with connect() as c:
        c.execute(
            "DELETE FROM user_page_permissions WHERE user_id = ?",
            (int(user_id),))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("user_permissions.clear",
              (set_by or "").strip() or "system",
              str(user_id), "reset to unconfigured"))


# ---------------------------------------------------------------------------
# Demand signals — proactive intelligence layer
# ---------------------------------------------------------------------------

# Canonical outcome vocabulary as of v2.59. The DB column is just TEXT
# (no CHECK constraint), so legacy values can coexist — we map them on
# read for display, and the next edit on a row rewrites it to the new
# vocabulary. See `normalize_outcome()` below for the mapping.
OUTCOME_VALUES = [
    "pending",     # not yet resolved (was 'open' pre-v2.59)
    "converted",   # closed as a sale — feeds demand_scoring conversion factor
    "lost",        # customer didn't buy
    "ignored",     # not actionable / spam (was 'invalid' pre-v2.59)
    "duplicate",   # same demand already captured in another row
    "wrong_sku",   # captured SKU was wrong; corrected via 'approved_sku' edit
]

# Legacy → canonical map. Applied on display so old rows render under the
# new labels without a destructive migration.
_OUTCOME_LEGACY_MAP = {
    "open":    "pending",
    "invalid": "ignored",
}


def normalize_outcome(value):
    """Return the canonical outcome label for display. Maps legacy values
    ('open' → 'pending', 'invalid' → 'ignored') and treats NULL/empty as
    'pending'. Anything already canonical (or unrecognised) is returned
    as-is so we never silently drop information."""
    if value is None or str(value).strip() == "":
        return "pending"
    v = str(value).strip().lower()
    return _OUTCOME_LEGACY_MAP.get(v, v)


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
    # detected_sku locks in the originally-captured SKU at insert time so
    # later edits to `sku` (via the review page) don't destroy the lineage.
    # Both columns hold the same value at capture; only `sku` mutates after.
    with connect() as c:
        cur = c.execute(
            """
            INSERT INTO demand_signals
                (source, source_ref, sku, detected_sku, product_family,
                 raw_text, signal_type, quantity, customer_id,
                 customer_name, salesperson, confidence, needs_review,
                 outcome, note, created_by, updated_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (source, source_ref, sku, sku, product_family,
             raw_text, signal_type, quantity, customer_id,
             customer_name, salesperson, confidence,
             1 if needs_review else 0,
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
                          matched_order_number: Optional[str] = None,
                          matched_sale_date: Optional[str] = None,
                          matched_sale_line_id: Optional[str] = None,
                          match_confidence: Optional[str] = None,
                          updated_by: str = "system") -> None:
    """Update a signal's mutable fields. Only non-None args are
    written, so it's safe to call with just the fields you mean to
    change.

    The matched_* and match_confidence columns are populated by the
    auto-reconciler — manual edits via the review page typically only
    touch outcome / note / approved_sku / product_family / needs_review.
    """
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
    if matched_order_number is not None:
        sets.append("matched_order_number = ?")
        params.append(matched_order_number)
    if matched_sale_date is not None:
        sets.append("matched_sale_date = ?")
        params.append(matched_sale_date)
    if matched_sale_line_id is not None:
        sets.append("matched_sale_line_id = ?")
        params.append(matched_sale_line_id)
    if match_confidence is not None:
        sets.append("match_confidence = ?")
        params.append(match_confidence)
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


# ---------------------------------------------------------------------------
# Demand score wrappers - bridge demand_signals rows -> demand_scoring module.
# Kept here (not in demand_scoring.py) so the scoring module stays pure /
# DB-free / unit-testable. This is the only place that knows how to fetch
# rows for a SKU.
# ---------------------------------------------------------------------------

def compute_demand_score(sku,
                          *,
                          window_days=30,
                          conversion_window_days=90):
    """Compute the 0-100 demand score + confidence for a single SKU.

    Pulls signals from demand_signals within the window and hands them
    to demand_scoring.score_signals(). Returns the same dict shape that
    function returns (score, confidence, components, breakdown, why).
    Returns the empty/zero dict if no signals.
    """
    import demand_scoring as _ds
    from datetime import datetime, timedelta

    sku = (sku or "").strip()
    if not sku:
        return _ds.score_signals([], window_days=window_days)

    now = datetime.utcnow()
    window_since = (now - timedelta(days=window_days)).isoformat()
    conv_since = (now - timedelta(days=conversion_window_days)).isoformat()

    window_rows = list_demand_signals(
        sku=sku, since=window_since, limit=10000)
    conv_rows = list_demand_signals(
        sku=sku, since=conv_since, limit=10000)

    window_dicts = [dict(r) for r in window_rows]
    conv_dicts = [dict(r) for r in conv_rows]

    return _ds.score_signals(
        window_dicts,
        window_days=window_days,
        conversion_signals=conv_dicts,
        conversion_window_days=conversion_window_days,
        now=now,
    )


def compute_demand_scores_batch(*,
                                  window_days=30,
                                  conversion_window_days=90):
    """Compute scores for ALL SKUs that have at least one signal in the
    window, in one DB scan. Returns {sku: score_dict}.

    Used by the Ordering page warning column. The naive
    "loop compute_demand_score per sku" version was O(N) DB calls; this
    is O(2) (one for the window range, one for the conversion range).
    """
    import demand_scoring as _ds
    from datetime import datetime, timedelta

    now = datetime.utcnow()
    window_since = (now - timedelta(days=window_days)).isoformat()
    conv_since = (now - timedelta(days=conversion_window_days)).isoformat()

    with connect() as c:
        window_rows = c.execute(
            "SELECT * FROM demand_signals "
            "WHERE sku IS NOT NULL AND sku != '' AND created_at >= ?",
            (window_since,)).fetchall()
        conv_rows = c.execute(
            "SELECT * FROM demand_signals "
            "WHERE sku IS NOT NULL AND sku != '' AND created_at >= ?",
            (conv_since,)).fetchall()

    by_sku_window = {}
    for r in window_rows:
        by_sku_window.setdefault(r["sku"], []).append(dict(r))
    by_sku_conv = {}
    for r in conv_rows:
        by_sku_conv.setdefault(r["sku"], []).append(dict(r))

    out = {}
    for sku, sigs in by_sku_window.items():
        out[sku] = _ds.score_signals(
            sigs,
            window_days=window_days,
            conversion_signals=by_sku_conv.get(sku, sigs),
            conversion_window_days=conversion_window_days,
            now=now,
        )
    return out


# ---------------------------------------------------------------------------
# Demand-signal auto-reconciler — match pending signals to CIN7 sales.
# ---------------------------------------------------------------------------
#
# Reduces manual work on the Demand Signals review page: when a captured
# inquiry is followed by a real CIN7 sale to the same customer for the
# same SKU, we mark the signal `outcome='converted'` automatically.
#
# Caller passes in the sale_lines records (list of dicts with at minimum
# SKU, CustomerID, OrderDate, OrderNumber). Keeping db.py free of pandas
# imports — the page-side and the nightly cin7_sync hook each load the
# CSV in their own context and pass dicts down.
#
# Match rule:
#   - signal.outcome ∈ {pending, open, NULL}
#   - signal.sku and signal.customer_id both present
#   - sale's CustomerID matches signal.customer_id (case-insensitive)
#   - sale's SKU matches signal.sku (case-insensitive)
#   - sale's OrderDate >= signal.created_at AND <= created_at + window_days
#
# FIFO per (sku, customer_id) — the OLDEST pending signal claims the
# earliest unconsumed matching sale, so a single sale never converts two
# pendings.
#
# Per spec (v2.60): we do NOT auto-mark `lost`. That stays a manual
# decision until we trust the conversion side first.

def reconcile_demand_signals(sales_rows,
                              *,
                              window_days: int = 30,
                              actor: str = "auto_reconciler",
                              dry_run: bool = False,
                              cancelled_statuses=None) -> dict:
    """Auto-mark pending demand signals as converted when a matching CIN7
    sale exists, OR flag them for human review when the evidence is only
    partial.

    Confidence model (v2.61):
      HIGH    — exact SKU + exact customer_id, sale within window.
                AUTO-CONVERTS. Stores match metadata and writes a
                'demand_signal_auto_converted' audit row.
      MEDIUM  — exact SKU, signal had NO customer_id captured, sale
                exists to *some* customer in the window.
                FLAGS needs_review=1 and stores match metadata, but does
                NOT change outcome. Writes a 'demand_signal_needs_review'
                audit row. Reviewer decides via the page.
      LOW     — reserved for future family-only matches.

    Two-pass: HIGH consumes its sales first, then MEDIUM looks at
    whatever's left. So a HIGH and a MEDIUM never compete for the same
    sale row.

    Sales filter: rows whose Status is in `cancelled_statuses` are
    EXCLUDED from matching. Default excludes VOIDED / CANCELLED /
    CREDITED / LOST. Per the project rule "accuracy > speed" — better
    to miss a match than convert off a void.

    `dry_run=True` computes everything and returns the plan but writes
    nothing to demand_signals or audit_log (one summary row excepted —
    so the page can show that a dry-run was attempted).

    Returns (v2.61.1 — full bucket breakdown so checked balances)::
        {
            "checked": int,                  # pending signals considered
            "converted": int,                # auto-converted (HIGH)
            "needs_review": int,             # flagged for review (MEDIUM)
            "skipped_no_sku": int,           # signal had no SKU
            "skipped_no_customer": int,      # signal had SKU but no
                                              # customer_id, AND no
                                              # MEDIUM fallback fired
            "skipped_no_match": int,         # signal had SKU + customer_id
                                              # but no qualifying live sale
            "skipped_cancelled_voided": int, # the only candidate in window
                                              # was VOIDED / CANCELLED /
                                              # CREDITED / LOST
            "errors": int,
            "dry_run": bool,
            "would_convert": [...],   # only when dry_run=True
            "would_review": [...]     # only when dry_run=True
        }

    Invariant:
        checked == converted + needs_review + skipped_no_sku
                 + skipped_no_customer + skipped_no_match
                 + skipped_cancelled_voided + errors
    """
    from datetime import datetime as _dt, timedelta as _td

    if cancelled_statuses is None:
        cancelled_statuses = {"VOIDED", "CANCELLED", "CREDITED", "LOST"}
    cancelled_statuses = {str(s).upper() for s in cancelled_statuses}

    summary = {
        "checked": 0,
        "converted": 0,
        "needs_review": 0,
        "skipped_no_sku": 0,
        "skipped_no_customer": 0,
        "skipped_no_match": 0,
        "skipped_cancelled_voided": 0,
        "errors": 0,
        "dry_run": bool(dry_run),
        "would_convert": [],
        "would_review": [],
    }

    def _parse_dt(value):
        if value is None:
            return None
        if isinstance(value, _dt):
            return value.replace(tzinfo=None) if value.tzinfo else value
        s = str(value).strip()
        if not s or s.lower() in ("nan", "none", "nat"):
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f",
                    "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d"):
            try:
                return _dt.strptime(s.split("+")[0].rstrip("Z"), fmt)
            except ValueError:
                continue
        try:
            return _dt.fromisoformat(
                s.replace("Z", "+00:00")).replace(tzinfo=None)
        except (ValueError, AttributeError):
            return None

    def _record_run(extra=""):
        # Always write a run-marker row — even on dry-run — so the page
        # can show "Last reconciled at" honestly. Detail says (dry_run).
        with connect() as c:
            c.execute(
                "INSERT INTO audit_log (event, actor, target, detail) "
                "VALUES (?, ?, ?, ?)",
                ("demand_signal.reconcile_run", actor, "all",
                 (f"checked={summary['checked']} "
                  f"converted={summary['converted']} "
                  f"needs_review={summary['needs_review']} "
                  f"no_sku={summary['skipped_no_sku']} "
                  f"no_customer={summary['skipped_no_customer']} "
                  f"no_match={summary['skipped_no_match']} "
                  f"cancelled_voided={summary['skipped_cancelled_voided']} "
                  f"errors={summary['errors']} "
                  f"window_days={window_days} "
                  f"{'(dry_run)' if dry_run else ''} {extra}").strip()))

    sales_rows = list(sales_rows or [])
    if not sales_rows:
        _record_run("(no sales data)")
        return summary

    # Pull pending-equivalent signals (oldest first for FIFO).
    all_pending = list_demand_signals(limit=10000)
    pending = [r for r in all_pending
               if (r["outcome"] is None
                   or str(r["outcome"]).strip().lower() in
                   ("pending", "open", ""))]
    summary["checked"] = len(pending)

    # Bucket no-sku rows up front so they appear in the summary instead
    # of vanishing silently (the v2.61 bug James caught).
    eligible = []
    for s in pending:
        if not s["sku"] or not str(s["sku"]).strip():
            summary["skipped_no_sku"] += 1
        else:
            eligible.append(s)
    eligible.sort(key=lambda r: str(r["created_at"] or ""))

    # Build FOUR indexes from sales_rows so we can distinguish "no
    # match at all" from "only candidate was cancelled":
    #   live_exact   : (sku, cid) → [non-cancelled sales sorted asc]
    #   live_any     : sku        → [non-cancelled sales sorted asc]
    #   void_exact   : (sku, cid) → [cancelled-status sales]
    #   void_any     : sku        → [cancelled-status sales]
    # Same sale_record is shared between live_exact and live_any so
    # `consumed=True` set in pass 1 is visible in pass 2.
    live_exact: dict = {}
    live_any: dict = {}
    void_exact: dict = {}
    void_any: dict = {}
    for row in sales_rows:
        sku = str(row.get("SKU", "") or "").strip().upper()
        if not sku:
            continue
        order_date_dt = _parse_dt(
            row.get("OrderDate") or row.get("InvoiceDate"))
        if order_date_dt is None:
            continue
        status = str(row.get("Status", "") or "").strip().upper()
        sale_id = str(row.get("SaleID") or "").strip()
        product_id = str(row.get("ProductID") or "").strip()
        sale_record = {
            "OrderDate": order_date_dt,
            "OrderNumber": (row.get("OrderNumber")
                            or row.get("InvoiceNumber") or "?"),
            "SaleID": sale_id or None,
            "ProductID": product_id or None,
            "MatchedSaleLineID": (
                f"{sale_id}:{product_id}" if (sale_id and product_id)
                else (sale_id or product_id or "?")),
            "Status": status or None,
            "Quantity": row.get("Quantity"),
            "consumed": False,
        }
        cid = str(row.get("CustomerID", "") or "").strip().upper()
        if status in cancelled_statuses:
            if cid:
                void_exact.setdefault((sku, cid), []).append(sale_record)
            void_any.setdefault(sku, []).append(sale_record)
        else:
            if cid:
                live_exact.setdefault((sku, cid), []).append(sale_record)
            live_any.setdefault(sku, []).append(sale_record)

    for k in live_exact:
        live_exact[k].sort(key=lambda x: x["OrderDate"])
    for k in live_any:
        live_any[k].sort(key=lambda x: x["OrderDate"])
    for k in void_exact:
        void_exact[k].sort(key=lambda x: x["OrderDate"])
    for k in void_any:
        void_any[k].sort(key=lambda x: x["OrderDate"])

    def _exists_in_window(candidates, sig_dt, window_end):
        """Was there ANY (consumed or not) candidate in window? Used
        only to detect cancelled-only situations."""
        for c_sale in candidates:
            if sig_dt <= c_sale["OrderDate"] <= window_end:
                return True
        return False

    def _earliest_unconsumed(candidates, sig_dt, window_end):
        for c_sale in candidates:
            if c_sale["consumed"]:
                continue
            if sig_dt <= c_sale["OrderDate"] <= window_end:
                return c_sale
        return None

    def _apply(signal, match, *, confidence):
        """Persist the match. For confidence='high' we flip outcome and
        write the auto_converted audit row. For 'medium' we set
        needs_review=1, store the suspected match metadata, but DO NOT
        change outcome. dry_run short-circuits all writes."""
        sig_id = int(signal["id"])
        order_num = match["OrderNumber"] or "?"
        sale_date_str = match["OrderDate"].strftime("%Y-%m-%d")
        sale_line_id = match["MatchedSaleLineID"]
        existing_note = (signal["note"] or "").strip()
        if confidence == "high":
            new_note = ((existing_note + " | ") if existing_note else "") + (
                f"Auto-converted from CIN7 sale Order {order_num} on "
                f"{sale_date_str}.")
        else:  # medium
            new_note = ((existing_note + " | ") if existing_note else "") + (
                f"SUSPECTED match: CIN7 sale Order {order_num} on "
                f"{sale_date_str} (no customer_id on signal — please "
                f"verify).")

        if dry_run:
            bucket = (summary["would_convert"] if confidence == "high"
                      else summary["would_review"])
            bucket.append({
                "signal_id": sig_id,
                "sku": signal["sku"],
                "customer_id": signal["customer_id"],
                "order_number": order_num,
                "sale_date": sale_date_str,
                "confidence": confidence,
            })
            match["consumed"] = True
            return

        kwargs = {
            "note": new_note,
            "matched_order_number": str(order_num),
            "matched_sale_date": sale_date_str,
            "matched_sale_line_id": str(sale_line_id),
            "match_confidence": confidence,
            "updated_by": actor,
        }
        if confidence == "high":
            kwargs["outcome"] = "converted"
        else:  # medium
            kwargs["needs_review"] = True

        update_demand_signal(sig_id, **kwargs)
        match["consumed"] = True

        # Distinct, queryable audit event per match (in addition to the
        # generic demand_signal.update row written by update_demand_signal).
        evidence = (
            f"signal_id={sig_id} "
            f"sku={signal['sku']} "
            f"customer_id={signal['customer_id'] or ''} "
            f"order_number={order_num} "
            f"sale_date={sale_date_str} "
            f"sale_line_id={sale_line_id} "
            f"qty={match.get('Quantity')} "
            f"confidence={confidence}")
        event_name = ("demand_signal_auto_converted" if confidence == "high"
                      else "demand_signal_needs_review")
        with connect() as c:
            c.execute(
                "INSERT INTO audit_log (event, actor, target, detail) "
                "VALUES (?, ?, ?, ?)",
                (event_name, actor, str(sig_id), evidence))

    # ---- Pass 1: HIGH (exact SKU + exact customer_id) ----
    # Signals that lack customer_id are deferred to MEDIUM (pass 2).
    medium_candidates = []
    for s in eligible:
        sku_key = str(s["sku"]).strip().upper()
        cid_raw = (s["customer_id"] or "").strip()
        sig_dt = _parse_dt(s["created_at"])
        if sig_dt is None:
            # Treat un-parseable created_at as "no qualifying sale"
            # rather than inventing a fifth bucket.
            summary["skipped_no_match"] += 1
            continue
        window_end = sig_dt + _td(days=window_days)

        if cid_raw:
            cid_key = cid_raw.upper()
            live_candidates = live_exact.get((sku_key, cid_key), [])
            match = _earliest_unconsumed(
                live_candidates, sig_dt, window_end)
            if match:
                try:
                    _apply(s, match, confidence="high")
                    summary["converted"] += 1
                except Exception:
                    summary["errors"] += 1
            else:
                # Live miss. Was there a CANCELLED candidate in window?
                # If so, that's a different (more diagnostic) skip
                # category than "no match at all".
                void_candidates = void_exact.get((sku_key, cid_key), [])
                if _exists_in_window(
                        void_candidates, sig_dt, window_end):
                    summary["skipped_cancelled_voided"] += 1
                else:
                    summary["skipped_no_match"] += 1
        else:
            # No customer_id captured. Defer to MEDIUM pass.
            medium_candidates.append((s, sig_dt, window_end, sku_key))

    # ---- Pass 2: MEDIUM (exact SKU, signal had no customer_id) ----
    for (s, sig_dt, window_end, sku_key) in medium_candidates:
        live_candidates = live_any.get(sku_key, [])
        match = _earliest_unconsumed(
            live_candidates, sig_dt, window_end)
        if match:
            try:
                _apply(s, match, confidence="medium")
                summary["needs_review"] += 1
            except Exception:
                summary["errors"] += 1
        else:
            # No live MEDIUM match. Distinguish "only candidate was
            # cancelled" from "the missing customer_id is the bottleneck"
            # (i.e. there's literally no sale of this SKU in window).
            void_candidates = void_any.get(sku_key, [])
            if _exists_in_window(
                    void_candidates, sig_dt, window_end):
                summary["skipped_cancelled_voided"] += 1
            else:
                summary["skipped_no_customer"] += 1

    _record_run()
    return summary


def last_demand_signal_reconcile_at() -> Optional[str]:
    """ISO timestamp of the most recent reconcile_demand_signals run, or
    None if it's never been run. Used by the Demand Signals page to show
    a 'Last reconciled at:' caption."""
    with connect() as c:
        row = c.execute(
            "SELECT at FROM audit_log "
            "WHERE event = 'demand_signal.reconcile_run' "
            "ORDER BY at DESC LIMIT 1"
        ).fetchone()
    return row["at"] if row else None


# ---------------------------------------------------------------------------
# v2.67.36 — Dormancy provenance helpers.
# ---------------------------------------------------------------------------
# Track SKUs that have been flagged is_dormant=True so that when a
# salesman successfully sells one (because the AI surfaced it as slow
# stock), the buyer gets a "!" warning before reordering. The warning
# auto-lifts after 90 days of sustained post-dormancy activity, or
# can be manually dismissed.

# How long sustained activity (post-dormancy) must persist before the
# warning auto-lifts. Buyer can override via the dismiss button.
DORMANCY_RECOVERY_LIFT_DAYS = 90


def record_dormancy_snapshot(dormant_skus: set,
                              recovered_skus: set,
                              lift_after_days: int = DORMANCY_RECOVERY_LIFT_DAYS
                              ) -> dict:
    """v2.67.36 — write today's dormancy state into sku_dormancy_log.

    Called once per ABC engine recompute (which happens daily after
    the sync). Three flows:

      1. SKU is dormant NOW → upsert with last_seen_dormant_at=now,
         clear recovered_at (we're back to dormant), and set
         first_seen_dormant_at if this is the first observation.

      2. SKU was dormant before AND is now active+selling → set
         recovered_at if not yet set, and check whether it's been
         active long enough to auto-lift the warning.

      3. SKUs we don't see this run stay as-is. The warning persists
         indefinitely until lifted.

    Args:
        dormant_skus: set of SKU strings where is_dormant=True now.
        recovered_skus: set of SKU strings where is_dormant=False AND
            recent demand > 0 (i.e. genuinely active again).

    Returns a small summary dict {dormant_seen, recoveries_started,
    auto_lifted} for logging.
    """
    summary = {
        "dormant_seen": 0,
        "recoveries_started": 0,
        "auto_lifted": 0,
    }
    if not dormant_skus and not recovered_skus:
        return summary
    with connect() as c:
        # 1. Dormant SKUs — upsert "still / newly" dormant.
        for sku in dormant_skus:
            sku = str(sku).strip()
            if not sku:
                continue
            c.execute(
                """
                INSERT INTO sku_dormancy_log
                    (sku, first_seen_dormant_at, last_seen_dormant_at,
                     recovered_at, last_engine_run_at)
                VALUES
                    (?, datetime('now'), datetime('now'),
                     NULL, datetime('now'))
                ON CONFLICT(sku) DO UPDATE SET
                    last_seen_dormant_at = datetime('now'),
                    recovered_at = NULL,
                    last_engine_run_at = datetime('now'),
                    -- Re-dormancy clears any prior auto-lift so the
                    -- warning re-engages. Manual dismissals stay
                    -- (buyer's intent persists across re-dormancy).
                    -- v2.67.173 — qualify references to the
                    -- existing row's columns with the table name;
                    -- Postgres rejects unqualified ones in
                    -- ON CONFLICT DO UPDATE because both the
                    -- target row and EXCLUDED share the same
                    -- column names → ambiguous.
                    warning_lifted_at = CASE
                        WHEN sku_dormancy_log.warning_lift_reason
                             = 'manual_dismiss'
                        THEN sku_dormancy_log.warning_lifted_at
                        ELSE NULL
                    END,
                    warning_lift_reason = CASE
                        WHEN sku_dormancy_log.warning_lift_reason
                             = 'manual_dismiss'
                        THEN sku_dormancy_log.warning_lift_reason
                        ELSE NULL
                    END
                """,
                (sku,),
            )
            summary["dormant_seen"] += 1
        # 2. Recovered SKUs — record first sign of recovery and
        #    auto-lift if recovery is sustained.
        for sku in recovered_skus:
            sku = str(sku).strip()
            if not sku:
                continue
            # Set recovered_at if a prior dormancy exists and we
            # haven't yet recorded recovery. Use cursor explicitly
            # so we can read .rowcount (Connection has no rowcount).
            cur = c.execute(
                """
                UPDATE sku_dormancy_log
                   SET recovered_at = COALESCE(recovered_at,
                                                datetime('now')),
                       last_engine_run_at = datetime('now')
                 WHERE sku = ?
                   AND last_seen_dormant_at IS NOT NULL
                """,
                (sku,),
            )
            if cur.rowcount > 0:
                row = c.execute(
                    "SELECT recovered_at, last_seen_dormant_at, "
                    "       warning_lifted_at "
                    "  FROM sku_dormancy_log WHERE sku = ?",
                    (sku,),
                ).fetchone()
                if row and row["recovered_at"] and not row[
                        "warning_lifted_at"]:
                    # Auto-lift if the recovery has held for the
                    # threshold AND last_seen_dormant_at is older
                    # than the recovery (i.e. no re-dormancy).
                    auto_cur = c.execute(
                        """
                        UPDATE sku_dormancy_log
                           SET warning_lifted_at = datetime('now'),
                               warning_lift_reason = 'auto_recovered_'
                                                  || ?
                                                  || 'd'
                         WHERE sku = ?
                           AND warning_lifted_at IS NULL
                           AND recovered_at IS NOT NULL
                           AND last_seen_dormant_at < recovered_at
                           AND julianday('now')
                                 - julianday(recovered_at)
                               >= ?
                        """,
                        (int(lift_after_days), sku,
                         int(lift_after_days)),
                    )
                    if auto_cur.rowcount > 0:
                        summary["auto_lifted"] += 1
                    else:
                        summary["recoveries_started"] += 1
    return summary


def auto_lift_aclass_dormancy(active_aclass_skus: set,
                                 reason: str = "aclass_grace_v2_67_48"
                                 ) -> int:
    """v2.67.48 — auto-lift dormancy warnings on any SKU the engine
    no longer flags due to the A-class grace rule. Existing entries
    in `sku_dormancy_log` would otherwise wait the standard 90-day
    auto-lift window, leaving over-bought A-class items showing in
    the Slow Movers list for months after the engine fix landed.

    `active_aclass_skus` should be the set of SKU strings that the
    current engine recompute classifies as ABC=A with positive
    12mo activity (i.e. covered by the grace rule).

    Returns the number of warnings lifted by this call. Idempotent
    — already-lifted entries aren't re-touched."""
    if not active_aclass_skus:
        return 0
    skus = [str(s).strip() for s in active_aclass_skus
             if str(s).strip()]
    if not skus:
        return 0
    placeholders = ",".join("?" for _ in skus)
    with connect() as c:
        cur = c.execute(
            f"""
            UPDATE sku_dormancy_log
               SET warning_lifted_at = datetime('now'),
                   warning_lift_reason = ?
             WHERE warning_lifted_at IS NULL
               AND sku IN ({placeholders})
            """,
            (reason, *skus),
        )
        return int(cur.rowcount or 0)


def get_dormancy_warnings() -> dict:
    """v2.67.36 — return {sku: warning_info} for SKUs with an active
    once-slow warning. Used by the Ordering page to render a "!" in
    the Status column and an auto-note next to the buyer's manual
    notes."""
    with connect() as c:
        rows = c.execute(
            """
            SELECT sku,
                   first_seen_dormant_at,
                   last_seen_dormant_at,
                   recovered_at,
                   last_engine_run_at
              FROM sku_dormancy_log
             WHERE warning_lifted_at IS NULL
               AND last_seen_dormant_at IS NOT NULL
            """,
        ).fetchall()
    out = {}
    for r in rows:
        out[r["sku"]] = {
            "first_seen_dormant_at": r["first_seen_dormant_at"],
            "last_seen_dormant_at": r["last_seen_dormant_at"],
            "recovered_at": r["recovered_at"],
            "last_engine_run_at": r["last_engine_run_at"],
        }
    return out


def record_slow_mover_value_snapshot(skus_count: int,
                                        units_on_hand: float,
                                        value_on_shelf: float
                                        ) -> None:
    """v2.67.42 — daily snapshot of slow-stock totals. Called from
    the engine recompute right after dormancy log writes. One row
    per calendar date — same-day re-runs overwrite the row so the
    latest snapshot per day is kept (last-write-wins).

    Used by the Overview tile to show month-over-month progress
    (red caption with previous month's value) and by the Slow
    Movers page header for the same purpose."""
    with connect() as c:
        c.execute(
            """
            INSERT INTO slow_mover_value_snapshots
                (snapshot_date, skus_count, units_on_hand,
                 value_on_shelf, captured_at)
            VALUES
                (date('now'), ?, ?, ?, datetime('now'))
            ON CONFLICT(snapshot_date) DO UPDATE SET
                skus_count     = excluded.skus_count,
                units_on_hand  = excluded.units_on_hand,
                value_on_shelf = excluded.value_on_shelf,
                captured_at    = excluded.captured_at
            """,
            (int(skus_count), float(units_on_hand),
             float(value_on_shelf)),
        )


def get_previous_month_slow_mover_value() -> dict:
    """v2.67.42 — returns the most-recent snapshot from any date
    in the PREVIOUS calendar month, or {} if none. Used by the
    Overview slow-stock tile to render a small comparison caption.

    'Previous month' is defined as the calendar month preceding
    today's calendar month. So on 2026-05-15 we look at the
    latest snapshot dated between 2026-04-01 and 2026-04-30.

    v2.67.178 — compute the date boundaries in Python rather than
    using SQLite's `date('now', 'start of month', '-1 month')`
    function (which doesn't exist in Postgres). Portable both
    ways."""
    from datetime import date as _date
    today = _date.today()
    first_of_this_month = today.replace(day=1)
    if first_of_this_month.month == 1:
        first_of_prev_month = first_of_this_month.replace(
            year=first_of_this_month.year - 1, month=12)
    else:
        first_of_prev_month = first_of_this_month.replace(
            month=first_of_this_month.month - 1)
    with connect() as c:
        row = c.execute(
            """
            SELECT snapshot_date, skus_count, units_on_hand,
                   value_on_shelf
              FROM slow_mover_value_snapshots
             WHERE snapshot_date >= ?
               AND snapshot_date < ?
             ORDER BY snapshot_date DESC
             LIMIT 1
            """,
            (first_of_prev_month.isoformat(),
              first_of_this_month.isoformat()),
        ).fetchone()
    if not row:
        return {}
    return dict(row)


def list_slow_mover_snapshots(limit: int = 730) -> list:
    """v2.67.178 — return all rows from slow_mover_value_snapshots
    ordered oldest first, limited to `limit` rows (default 730 ≈
    two years of daily snapshots). Used by the Monthly Metrics
    page to plot EOM slow-stock value over the rolling window."""
    with connect() as c:
        rows = c.execute(
            "SELECT snapshot_date, skus_count, units_on_hand, "
            "       value_on_shelf "
            "FROM slow_mover_value_snapshots "
            "ORDER BY snapshot_date ASC "
            "LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def flag_sku_as_slow_mover(sku: str, user_id: str = "") -> None:
    """v2.67.40 — manually mark a SKU as a slow mover from the
    Slow Movers page UI. Inserts (or revives) a row in
    sku_dormancy_log so the SKU shows up everywhere the engine-
    driven warnings show up:
      - Overview slow-mover panel
      - Slow Movers detail table
      - Ordering page Status column (❗ prefix)
      - Notes column auto-prefix
      - Weekly digest email

    Use case: buyer/sales spots a slow-mover by eye that the engine
    didn't flag (e.g. seasonal item, project leftover, sample stock).
    The flag persists across engine recomputes and is sticky against
    auto-lifts (since the buyer set it deliberately)."""
    sku = str(sku).strip()
    if not sku:
        return
    with connect() as c:
        c.execute(
            """
            INSERT INTO sku_dormancy_log
                (sku, first_seen_dormant_at, last_seen_dormant_at,
                 recovered_at, last_engine_run_at)
            VALUES
                (?, datetime('now'), datetime('now'),
                 NULL, datetime('now'))
            ON CONFLICT(sku) DO UPDATE SET
                last_seen_dormant_at = datetime('now'),
                last_engine_run_at = datetime('now'),
                -- A manual flag re-engages a previously-lifted
                -- warning. Reasoning mirrors the SQL in
                -- record_dormancy_snapshot but with a manual
                -- intent marker.
                warning_lifted_at = NULL,
                warning_lift_reason = NULL,
                warning_lifted_by = NULL,
                recovered_at = NULL
            """,
            (sku,),
        )
        c.execute(
            """
            INSERT INTO feedback_events
                (source, entity_type, entity_id, feedback,
                 note, user_id)
            VALUES ('slow_movers_page', 'sku', ?,
                    'manual_flag_as_slow', '', ?)
            """,
            (sku, user_id or ""),
        )


def dismiss_dormancy_warning(sku: str, user_id: str = "",
                                reason: str = "manual_dismiss") -> None:
    """v2.67.36 — buyer override. Clears the once-slow warning so
    the SKU stops getting flagged in the Ordering page. The reason
    distinguishes manual dismissals from auto-lifts; manual ones
    persist across re-dormancy (a buyer's deliberate decision)."""
    sku = str(sku).strip()
    if not sku:
        return
    with connect() as c:
        c.execute(
            """
            UPDATE sku_dormancy_log
               SET warning_lifted_at = datetime('now'),
                   warning_lift_reason = ?,
                   warning_lifted_by = ?
             WHERE sku = ?
            """,
            (reason, user_id or "", sku),
        )
