"""
ai_tools.py
===========
Tool functions exposed to Claude via the Anthropic API's tool-use feature.

Why tool-use, not embedded data: rather than dumping the whole engine
into Claude's context window every query (expensive + truncates), we
register a small set of tools Claude can call to fetch exactly what it
needs to answer. Claude figures out which tool(s) to call based on the
user's question.

Each tool function in this module:
  - Takes a Python dict of arguments (Claude sends JSON)
  - Returns a Python dict (we serialize to JSON for Claude)
  - Pulls from the live engine_df / DB / CSVs — no stale snapshots
  - Returns small, structured results (not raw DataFrames)
  - Caps row counts so a "what's in stock" answer doesn't return 11k rows

The tools are deliberately narrow. Composability is Claude's job —
e.g., "what 2700K LED strips are slow moving?" is a `search_products`
call (filter by 2700K + LED strip family) followed by `get_dead_stock`
(filter to slow/dead from those results).

Adding new tools: register the spec in TOOL_SCHEMAS and add the
implementation in TOOL_HANDLERS. Both are required.
"""

from __future__ import annotations

import json
import time
from typing import Any, Optional

import pandas as pd

import db


# ---------------------------------------------------------------------------
# Tool schemas — these are what we send to Claude in the tools= argument.
# Schema follows Anthropic's tool spec: name, description, input_schema.
# ---------------------------------------------------------------------------
TOOL_SCHEMAS: list[dict] = [
    {
        "name": "search_products",
        "description": (
            "Find products matching a natural-language query and/or "
            "structured filters. Returns up to 25 SKUs with name, "
            "stock on hand, ABC class, classification (active/slow/"
            "dead/watchlist), and product family. Use this when the "
            "user asks about products by description, attribute, or "
            "category. Example: 'black recessed channel under 0.5 inch'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Free-text search across SKU + Name "
                                   "(case-insensitive substring match)",
                },
                "family": {
                    "type": "string",
                    "description": "Product family code, e.g. SIERRA38, "
                                   "CASCADE, KP24, etc. Optional.",
                },
                "classification": {
                    "type": "string",
                    "enum": ["active", "slow", "dead", "watchlist", "any"],
                    "description": "Filter to a specific stock "
                                   "classification. 'any' = no filter.",
                },
                "abc_class": {
                    "type": "string",
                    "enum": ["A", "B", "C", "any"],
                    "description": "Filter to A/B/C class. 'any' = no filter.",
                },
                "in_stock_only": {
                    "type": "boolean",
                    "description": "If true, only return SKUs with "
                                   "stock_on_hand > 0.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max rows to return (cap 50, default 25).",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_sku_details",
        "description": (
            "Get full details for a single SKU: name, stock on hand, "
            "ABC class, classification, recent sales velocity, last "
            "movement date, supplier, BOM info if applicable, and any "
            "migration mapping (predecessor/successor). Use when the "
            "user asks about a specific SKU."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {
                    "type": "string",
                    "description": "Exact SKU (case-sensitive). "
                                   "If unsure, use search_products first.",
                },
            },
            "required": ["sku"],
        },
    },
    {
        "name": "get_velocity",
        "description": (
            "Sales velocity / units sold / revenue for a SKU over the "
            "last N days. Returns totals AND optionally a daily/weekly/"
            "monthly breakdown that the UI will render as an inline "
            "chart. Use when user asks 'how fast does X sell', 'sales "
            "history for X', or 'show me the last 90 days of Y'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {"type": "string"},
                "days": {
                    "type": "integer",
                    "description": "Window in days (max 1825 = 5 years).",
                },
                "include_rolled_up": {
                    "type": "boolean",
                    "description": "If true, include sales of "
                                   "predecessor SKUs that migrated INTO "
                                   "this SKU (the engine's effective "
                                   "demand view).",
                },
                "granularity": {
                    "type": "string",
                    "enum": ["none", "day", "week", "month"],
                    "description": "If set to day/week/month, return a "
                                   "time-bucketed breakdown alongside "
                                   "the totals. The UI auto-renders "
                                   "this as a small line chart. Use "
                                   "when the user wants to SEE the "
                                   "trend, not just hear a single "
                                   "number.",
                },
            },
            "required": ["sku", "days"],
        },
    },
    {
        "name": "get_dead_stock",
        "description": (
            "List SKUs classified as dead, slow, or on the watchlist. "
            "Useful for sales team looking for products to push, or "
            "buyers reviewing what NOT to reorder. Returns SKU, name, "
            "stock on hand, stock value, classification, last "
            "movement date. Capped at 100 rows."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "classification": {
                    "type": "string",
                    "enum": ["dead", "slow", "watchlist", "all"],
                    "description": "Which class to return. 'all' = "
                                   "dead + slow + watchlist combined.",
                },
                "family": {
                    "type": "string",
                    "description": "Filter to a product family.",
                },
                "min_stock_value": {
                    "type": "number",
                    "description": "Only include SKUs whose total "
                                   "stock value (qty × cost) exceeds "
                                   "this threshold.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max rows (cap 100, default 25).",
                },
            },
            "required": ["classification"],
        },
    },
    {
        "name": "get_migration_chain",
        "description": (
            "Trace the predecessor/successor chain for a SKU. Returns "
            "the full retiring → successor lineage. Useful when user "
            "asks 'what replaced this SKU' or 'what did this SKU "
            "replace'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {"type": "string"},
            },
            "required": ["sku"],
        },
    },
    {
        "name": "get_sales_totals",
        "description": (
            "Aggregate sales totals across the WHOLE business — not "
            "per-SKU. Use when the user asks about company-wide sales: "
            "'what have our sales been this month?', 'how much did we "
            "sell last week?', 'monthly revenue for the last 6 months', "
            "'compare this month to last month', 'how did April do?', "
            "'Q1 2026 revenue'. Returns revenue (from "
            "order headers, includes shipping & tax — matches CIN7's "
            "Revenue tile), unit count (from line items), and order "
            "count for the requested range and granularity. "
            "Pass EITHER `period` (pre-defined) OR `start_date` + "
            "`end_date` (arbitrary historical range, ISO YYYY-MM-DD). "
            "If both are given, the explicit dates win."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "period": {
                    "type": "string",
                    "enum": ["today", "yesterday", "mtd",
                              "last_7_days", "last_30_days",
                              "last_90_days", "last_365_days",
                              "ytd", "last_year"],
                    "description": "Pre-defined period. Use 'mtd' "
                                   "for month-to-date, 'ytd' for year-"
                                   "to-date. Omit when passing "
                                   "start_date/end_date.",
                },
                "start_date": {
                    "type": "string",
                    "description": "Start of custom range, "
                                   "ISO YYYY-MM-DD (inclusive). "
                                   "Examples: April 2026 → "
                                   "'2026-04-01'; Q1 2026 → "
                                   "'2026-01-01'. Pair with end_date.",
                },
                "end_date": {
                    "type": "string",
                    "description": "End of custom range, "
                                   "ISO YYYY-MM-DD (inclusive). "
                                   "Examples: April 2026 → "
                                   "'2026-04-30'; Q1 2026 → "
                                   "'2026-03-31'. Pair with start_date.",
                },
                "group_by": {
                    "type": "string",
                    "enum": ["none", "day", "week", "month"],
                    "description": "How to bucket the results. 'none' "
                                   "returns one total for the whole "
                                   "period; 'month' breaks by calendar "
                                   "month etc.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_recent_signals",
        "description": (
            "List recent demand signals (customer inquiries, quotes, "
            "lost sales, returns, etc.) optionally filtered by SKU, "
            "product family, signal type, source, or time window. Use "
            "for questions like 'any inquiries about LED-XYZ "
            "recently?', 'what's been asked about this week?', "
            "'show me lost sales for SIERRA38 this month'. Returns up "
            "to 50 rows, newest first."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {"type": "string"},
                "product_family": {"type": "string"},
                "signal_type": {
                    "type": "string",
                    "enum": [
                        "inquiry", "quote", "sold", "lost",
                        "substitute_offered", "cancelled", "returned",
                        "complaint", "abandoned_cart", "notify_me",
                        "any",
                    ],
                },
                "source": {
                    "type": "string",
                    "enum": ["manual", "slack", "gorgias",
                              "shopify_search", "shopify_abandoned",
                              "seo", "web_form", "phone", "any"],
                },
                "days": {
                    "type": "integer",
                    "description": "Look back this many days. Default 30.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max rows (cap 50, default 25).",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_top_inquired_products",
        "description": (
            "Leaderboard of most-signaled SKUs over a period. Use for "
            "'what products are getting attention?', 'top inquiries "
            "this week', 'what's hot right now?'. Counts ALL signal "
            "types by default (inquiries, quotes, lost sales, etc.) "
            "but can be narrowed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Look back this many days. Default 30.",
                },
                "signal_type": {
                    "type": "string",
                    "description": "If set, only count signals of this "
                                   "type (e.g. 'inquiry' to see what "
                                   "people are asking about; 'lost' "
                                   "for what's slipping away).",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max rows (default 15).",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_demand_score",
        "description": (
            "Compute a 0-100 demand score for one SKU from its recent "
            "demand_signals. The score combines signal volume, signal "
            "type (inquiry vs quote vs cancelled), source credibility, "
            "recency, and conversion rate. Returns the score, a "
            "confidence band (0-1), the breakdown of which "
            "signal types/sources contributed, and a human-readable "
            "explanation. Use when the user asks 'what's the demand "
            "score for X?', 'is X really rising or just a one-off?', "
            "'should I trust the inquiries on X?'. Per "
            "docs/demand-scoring.md."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {"type": "string"},
                "window_days": {
                    "type": "integer",
                    "description": "Recent window (default 30).",
                },
            },
            "required": ["sku"],
        },
    },
    {
        "name": "get_rising_demand",
        "description": (
            "Compare signal counts in a recent window vs a prior "
            "window of the same length to find rising demand. Use for "
            "'what's increasing in demand?', 'what got hot this "
            "week?', 'what wasn't being asked about a month ago but "
            "is now?'. Returns SKUs ranked by signal-count growth."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "recent_days": {
                    "type": "integer",
                    "description": "Length of the 'recent' window. "
                                   "Default 7.",
                },
                "min_recent": {
                    "type": "integer",
                    "description": "Ignore SKUs with fewer than N "
                                   "signals in the recent window. "
                                   "Default 2.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max rows (default 15).",
                },
            },
            "required": [],
        },
    },
    {
        "name": "search_knowledge_base",
        "description": (
            "Search the company's app documentation, business rules, "
            "SOPs, and manuals. Use this when the user asks HOW or "
            "WHY something works, or asks about company conventions "
            "(e.g., 'why is this SKU marked slow-moving?', 'how does "
            "the reorder calculation work?', 'what's the LED tube "
            "family naming convention?'). Returns up to 5 relevant "
            "paragraphs with file path + line range so you can cite "
            "the source. If the search returns no results, tell the "
            "user the documentation needs to be added — do NOT "
            "guess or invent the rule."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural-language question or keywords. "
                                   "Be specific — 'slow-moving classification "
                                   "rule' beats 'slow stock'.",
                },
                "max_results": {
                    "type": "integer",
                    "description": "Max paragraphs to return (default 5, cap 10).",
                },
            },
            "required": ["query"],
        },
    },
    # v2.66.6: get_relevant_slow_stock schema REMOVED. See
    # TOOL_HANDLERS comment in this file for context.
    {
        "name": "get_compatible_accessories",
        "description": (
            "Look up compatible accessories (lenses, diffusers, end "
            "caps, clips, brackets, connectors) for a product / "
            "family using Shopify accessory collections as the "
            "source of truth. Call this for ANY question about "
            "compatibility, what-fits, what-works-with, or "
            "accessories for a product. Authoritative when a "
            "matching '<Family> Accessories' Shopify collection "
            "exists; falls back to text search across product "
            "titles (labelled confidence='lower') when one "
            "doesn't. NEVER guess compatibility from descriptions "
            "if a curated collection exists."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {
                    "type": "string",
                    "description": (
                        "Subject product SKU. Either sku or family "
                        "is required."),
                },
                "family": {
                    "type": "string",
                    "description": (
                        "Subject product family / parent code, e.g. "
                        "SLIM8, SIERRA38, KP24."),
                },
                "accessory_type": {
                    "type": "string",
                    "enum": ["lens", "diffuser", "cover", "end_cap",
                              "clip", "bracket", "connector"],
                    "description": (
                        "Optional filter: only return accessories of "
                        "this type. Title-keyword based — 'lens' "
                        "matches products whose title contains 'lens' "
                        "or 'lenses', etc."),
                },
                "limit": {
                    "type": "integer",
                    "description": (
                        "Max results (cap 50, default 25)."),
                },
            },
        },
    },
    {
        "name": "find_similar_products",
        "description": (
            "Find product alternatives to a given SKU or product "
            "family. Conservative: returns only families with the "
            "SAME nominal diameter (parsed from trailing digits in "
            "the family code, e.g. SIERRA38 → 38mm). Same-diameter "
            "families that don't exist are not invented — the tool "
            "returns no alternatives rather than guess. Call this "
            "when the user asks for 'similar', 'alternative', "
            "'equivalent', 'replace', 'substitute', or 'instead of' "
            "phrasing. Show alternatives FIRST in your answer, "
            "include the original family only as a reference."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {
                    "type": "string",
                    "description": (
                        "Subject SKU. Either sku or family is "
                        "required.")
                },
                "family": {
                    "type": "string",
                    "description": (
                        "Subject product family code, e.g. "
                        "SIERRA38. Required if sku not given.")
                },
                "limit": {
                    "type": "integer",
                    "description": (
                        "Max alternatives to return (cap 20, "
                        "default 8).")
                },
                "include_original_family": {
                    "type": "boolean",
                    "description": (
                        "If true, the response also names the "
                        "subject family for reference (NOT counted "
                        "as an alternative).")
                },
            },
        },
    },
    {
        "name": "get_incoming_stock",
        "description": (
            "List OPEN / incomplete CIN7 purchase orders for a SKU "
            "or family. Use this for questions about upcoming "
            "shipments — 'when's the next delivery of X?', 'how "
            "many SIERRA38 do we have on order?', 'what's the ETA "
            "on Y?'. Excludes received / closed / cancelled / "
            "voided POs and zero-quantity lines. If a line has no "
            "expected delivery date in CIN7, the tool returns "
            "'not available' rather than guessing. "
            "**v2.67.44** — every line now also returns "
            "`comments` (PO header free-text where the buyer "
            "typically notes airfreight vs seafreight) and "
            "`shipping_notes` (the 'Shipping notes' attribute under "
            "the 'Vendor purchase' attribute set, where the buyer "
            "logs progress like 'departed Shenzhen 2026-04-12, in "
            "customs'). When a user asks about an incoming "
            "shipment, INCLUDE both fields in the answer when "
            "they're non-empty — they're the most current "
            "human-curated signal about freight status. "
            "**v2.67.52** — every line ALSO returns `memo` (the "
            "'Purchase Order Memo' big text box on the PO form — "
            "what the buyer types about the entire order), `note` "
            "(separate top-level note CIN7 sometimes uses for "
            "reason / blame e.g. 'shipped in error'), and `terms` "
            "(payment terms). The buyer uses ALL FIVE fields for "
            "different purposes — surface every non-empty one. "
            "Format: `<expected_date> · qty <N> from <Supplier> · "
            "PO <number>  \\n  ✈/🚢 <comments> · 📍 <shipping_notes>"
            " · 📝 <memo> · ⚠ <note> · 💳 <terms>`."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {
                    "type": "string",
                    "description": (
                        "Exact SKU to look up. Either sku or "
                        "family is required.")
                },
                "family": {
                    "type": "string",
                    "description": (
                        "Product family / SKU prefix when looking "
                        "across variants — e.g. SIERRA38 will match "
                        "SIERRA38-* SKUs.")
                },
                "limit": {
                    "type": "integer",
                    "description": (
                        "Max PO lines to return (cap 50, "
                        "default 25).")
                },
            },
        },
    },
    {
        # v2.67.51 — open-ended PO lookup. get_incoming_stock filters
        # to OPEN POs and matches by SKU/family; this tool fetches the
        # full PO by number regardless of status (DRAFT, ORDERED,
        # PARTIAL, INVOICED, RECEIVED, CLOSED, VOIDED) so the user can
        # answer questions like 'what was on PO-7109?' or 'did we
        # receive PO-7042 yet?'.
        "name": "get_purchase_order",
        "description": (
            "Look up a specific CIN7 purchase order by number "
            "(e.g. PO-7109) and return its header + every line "
            "item plus EVERY freeform text field the buyer types "
            "into: `memo` (Purchase Order Memo box on the PO "
            "form — main instruction field), `comments` (header "
            "comments), `shipping_notes` (vendor-purchase attribute "
            "for freight progress), `note` (separate top-level "
            "note), `terms` (payment terms). v2.67.52 added the "
            "Memo / Note / Terms fields after the buyer pointed "
            "out the PO Memo wasn't being surfaced. Use this when "
            "the user asks about a SPECIFIC PO ('what's on PO-7109', "
            "'what did we order from Topmet', 'show me purchase "
            "7042'). Returns `matched`=0 with a note if the PO "
            "isn't in the local sync window — sync covers ~30 "
            "days for line detail."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "po_number": {
                    "type": "string",
                    "description": (
                        "PO number, with or without the 'PO-' "
                        "prefix. Case-insensitive. Either po_number "
                        "or supplier+date_from is required.")
                },
                "supplier": {
                    "type": "string",
                    "description": (
                        "Optional supplier name substring filter. "
                        "Used together with date_from when the user "
                        "doesn't have the PO number.")
                },
                "date_from": {
                    "type": "string",
                    "description": (
                        "Optional ISO date (YYYY-MM-DD). Returns "
                        "POs raised on/after this date. Pair with "
                        "supplier when the user is browsing.")
                },
                "include_received": {
                    "type": "boolean",
                    "description": (
                        "Include fully-received / closed POs. "
                        "Default true (this tool is for full lookup, "
                        "unlike get_incoming_stock which is for "
                        "open-only).")
                },
            },
        },
    },
    {
        # v2.67.51 — sale-order lookup. Mirrors get_purchase_order on
        # the sales side. Reads the sale_lines DataFrames the page
        # already merges (sale_lines_3d / sale_lines_30d / longest).
        "name": "get_sale_order",
        "description": (
            "Look up a specific CIN7 sale order by number / invoice "
            "and return its header + every line item PLUS every "
            "freeform text field the rep types into: `memo` (Sale "
            "Order Memo box — build/delivery instructions), "
            "`shipping_notes` (top-level shipping instructions on "
            "sales — different location from POs), `note` "
            "(top-level header note), `terms` (payment terms), "
            "`customer_reference` (customer's own PO# referencing "
            "this sale). v2.67.52 added the Memo / Note / "
            "ShippingNotes / Terms / CustomerReference fields. "
            "Use this when the user asks about a SPECIFIC sale "
            "('what did Acme buy on SO-12345', 'show me sale "
            "INV-9981', 'who ordered LED-V3060001-2 last week'). "
            "Searches by order_number, invoice_number, OR by "
            "customer name + date range when the user doesn't "
            "have a number. Returns `matched`=0 with a note if "
            "the sale isn't in the local sync window."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "order_number": {
                    "type": "string",
                    "description": (
                        "Sale order number (e.g. SO-12345 or "
                        "12345). Case-insensitive.")
                },
                "invoice_number": {
                    "type": "string",
                    "description": (
                        "Invoice number (e.g. INV-9981).")
                },
                "customer": {
                    "type": "string",
                    "description": (
                        "Customer name substring. Combine with "
                        "date_from / date_to to scope when no "
                        "number is known.")
                },
                "date_from": {
                    "type": "string",
                    "description": (
                        "ISO date (YYYY-MM-DD). Inclusive lower "
                        "bound on order_date.")
                },
                "date_to": {
                    "type": "string",
                    "description": (
                        "ISO date (YYYY-MM-DD). Inclusive upper "
                        "bound on order_date.")
                },
                "limit": {
                    "type": "integer",
                    "description": (
                        "Max sales to return (cap 25, default 10). "
                        "Each sale's full line list is included.")
                },
            },
        },
    },
    {
        # v2.67.179 — Live CIN7 fallback when get_sale_order
        # misses. Fixes the SO-56331-style case where a sale was
        # just created but isn't in the 30-day cache yet.
        # v2.67.196 — Live CIN7 fallback for fresh POs that
        # haven't synced into the local purchase_lines CSV yet.
        # Sister to get_sale_live. Fires when get_purchase_order
        # returns matched=0 on a PO-NNNN reference.
        "name": "get_purchase_live",
        "description": (
            "Fetch a purchase order's line items LIVE from CIN7 "
            "via the API (bypasses the 30-day local sync "
            "window). Use this when get_purchase_order returns "
            "matched=0 for a recent PO — the PO was likely "
            "created in the last few hours and hasn't synced "
            "yet. Returns each line's SKU, name, qty, current "
            "OnHand. Slower than get_purchase_order (one CIN7 "
            "API call) so ONLY call this when the cached "
            "lookup misses."),
        "input_schema": {
            "type": "object",
            "properties": {
                "po_number": {
                    "type": "string",
                    "description": (
                        "Purchase order number (e.g. PO-7160 "
                        "or 7160). Case-insensitive. Either "
                        "this OR purchase_id is required."),
                },
                "purchase_id": {
                    "type": "string",
                    "description": (
                        "CIN7 internal PurchaseID UUID. "
                        "Skip the by-OrderNumber search step "
                        "if you already have it. Optional."),
                },
            },
        },
    },
    {
        "name": "get_sale_live",
        "description": (
            "Fetch a sale's line items LIVE from CIN7 via the "
            "API (bypasses the 30-day local sync window). Use "
            "this when get_sale_order returns matched=0 for a "
            "recent order — the sale was likely created in the "
            "last few hours and hasn't synced yet. Returns each "
            "line's SKU, name, qty, current OnHand, and an "
            "'available' yes/no flag for fulfillment checks. "
            "Slower than get_sale_order (one CIN7 API call) so "
            "ONLY call this when the cached lookup misses."),
        "input_schema": {
            "type": "object",
            "properties": {
                "order_number": {
                    "type": "string",
                    "description": (
                        "Sale order number (e.g. SO-56331 or "
                        "56331). Case-insensitive. Required."),
                },
            },
            "required": ["order_number"],
        },
    },
    {
        # v2.67.54 — ShipStation lookup. Lets the AI answer
        # "where's order SO-12345 / tracking 1Z123...", "what
        # carrier did we use", "what's the shipping cost on this
        # sale". Reads the local shipments CSV pulled by
        # shipstation_sync.py.
        "name": "get_shipping_details",
        "description": (
            "Look up ShipStation shipments by order number, "
            "tracking number, customer name, or date range. "
            "Returns shipment cost, carrier, service, tracking "
            "number, ship-to address, weight, dimensions, item "
            "list, and any customer / internal notes. Use this "
            "when the user asks 'where is order SO-12345', "
            "'what's the tracking for INV-9981', 'who shipped "
            "this for Acme yesterday', 'what carriers did we "
            "use last week', 'what was the freight on this "
            "sale'. Returns matched=0 with a note when "
            "ShipStation isn't configured (env vars not set) "
            "or the order isn't in the local sync window."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "order_number": {
                    "type": "string",
                    "description": (
                        "Sale order number to look up "
                        "(SO-12345 or just 12345; case-"
                        "insensitive).")
                },
                "tracking_number": {
                    "type": "string",
                    "description": (
                        "Tracking number to look up.")
                },
                "customer": {
                    "type": "string",
                    "description": (
                        "Customer name substring. Combine with "
                        "date_from / date_to to scope.")
                },
                "carrier_code": {
                    "type": "string",
                    "description": (
                        "Filter by carrier (ups / fedex / "
                        "usps / dhl_express). Useful for "
                        "carrier-mix questions.")
                },
                "date_from": {
                    "type": "string",
                    "description": (
                        "ISO date (YYYY-MM-DD). Inclusive "
                        "lower bound on shipDate.")
                },
                "date_to": {
                    "type": "string",
                    "description": (
                        "ISO date (YYYY-MM-DD). Inclusive "
                        "upper bound on shipDate.")
                },
                "limit": {
                    "type": "integer",
                    "description": (
                        "Max shipments to return (cap 50, "
                        "default 25).")
                },
            },
        },
    },
    {
        # v2.67.55c — shipping P&L analysis tool. Built after the
        # SO-55451 / SO-55971 case study revealed ~$149k/year
        # shipping bleed across long-channel SKUs (DIM-weight not
        # being applied at storefront-quote time). Powers questions
        # like 'which SKUs are losing us money on shipping?',
        # 'what's our shipping P&L this month?', 'show me the
        # worst 20 loss-makers'.
        "name": "get_shipping_margin",
        "description": (
            "Shipping P&L analysis. Filters shipments by SKU / "
            "customer / carrier / date / margin threshold, "
            "computes margin = customer_charge - actual_cost per "
            "shipment, and rolls up to a summary. Use this for "
            "ANY question involving shipping profitability: "
            "'which SKUs lose us money', 'how much did we lose "
            "on shipping last month', 'is UPS Ground "
            "underpriced', 'show me free-shipping orders that "
            "cost us $X+'. Returns headline totals (revenue, "
            "cost, net margin, loss-maker count) plus per-row "
            "details for the worst N losses. Requires shipments "
            "synced post-v2.67.55c (with both customer_charge "
            "and actual_cost columns); reports a data_quality "
            "warning if the CSV is pre-v2.67.55c."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {
                    "type": "string",
                    "description": (
                        "Filter to shipments whose ItemSummary "
                        "contains this SKU substring (e.g. "
                        "'LEDKIT-NICHO' to scan one family).")
                },
                "customer": {
                    "type": "string",
                    "description": (
                        "Customer name substring filter.")
                },
                "carrier": {
                    "type": "string",
                    "description": (
                        "Carrier code (ups / usps / fedex). "
                        "Useful for carrier-mix analysis.")
                },
                "service": {
                    "type": "string",
                    "description": (
                        "Service code substring (e.g. "
                        "'2nd_day_air', 'ground'). Compares "
                        "service-tier P&L.")
                },
                "date_from": {
                    "type": "string",
                    "description": (
                        "ISO date (YYYY-MM-DD).")
                },
                "date_to": {
                    "type": "string",
                    "description": (
                        "ISO date (YYYY-MM-DD).")
                },
                "loss_only": {
                    "type": "boolean",
                    "description": (
                        "If true, only return shipments with "
                        "margin < 0. Default false.")
                },
                "margin_below": {
                    "type": "number",
                    "description": (
                        "Threshold: only return shipments with "
                        "margin < this value (e.g. -30 to find "
                        "shipments losing $30+).")
                },
                "limit": {
                    "type": "integer",
                    "description": (
                        "Max shipments to return in `worst_rows` "
                        "(cap 50, default 20). Summary stats "
                        "always cover the FULL filtered set.")
                },
            },
        },
    },
    {
        # v2.67.57 — Slack message lookup. The bot ingests team
        # chat in 5 channels (#purchase-backorders, #shipping-issues,
        # #fulfilment, #shopify-website-improvement, #saleschat).
        # When a question in another tool relates to a recent
        # discussion ("did Andrew approve the Topmet PO?", "what
        # did Mike say about INV-53104?"), this tool greps the
        # local slack_messages mirror.
        "name": "get_slack_messages",
        "description": (
            "Search the local mirror of Slack messages from "
            "channels the AI watches (#purchase-backorders, "
            "#shipping-issues, #fulfilment, #shopify-website-"
            "improvement, #saleschat). Use this when the user's "
            "question references a Slack discussion or when "
            "answering a question would benefit from team "
            "context — e.g. 'did anyone flag PO-7109?', 'what "
            "did the team say about INV-53104?', 'when was the "
            "last backorder discussion for SKU X'. Filter by "
            "channel name, search term, user name, or date range. "
            "Returns the message text, channel, user, and "
            "timestamp. Note: the bot only sees channels it's "
            "been invited to."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "search": {
                    "type": "string",
                    "description": (
                        "Substring to grep for in message text. "
                        "Case-insensitive. Useful for SKUs / PO# / "
                        "INV# / customer name.")
                },
                "channel": {
                    "type": "string",
                    "description": (
                        "Channel name (with or without #) or "
                        "channel_id. Filters to one channel.")
                },
                "user": {
                    "type": "string",
                    "description": (
                        "User display-name substring (e.g. "
                        "'andrew'). Case-insensitive.")
                },
                "since_hours": {
                    "type": "integer",
                    "description": (
                        "Only return messages from the last N "
                        "hours. Default 168 (7 days).")
                },
                "limit": {
                    "type": "integer",
                    "description": (
                        "Max messages to return (cap 50, "
                        "default 20).")
                },
            },
        },
    },
    {
        # v2.67.55 — Shopify order trace for conversion attribution.
        # CIN7 keeps the financial data on /sale (SourceChannel,
        # Customer, Total, lines) but DROPS the conversion fields
        # (landing_site, referring_site, source_name, UTM params in
        # note_attributes, discount codes redeemed). Those only live
        # on the Shopify order itself. This tool joins the two so
        # the AI can answer 'how did we get this conversion'.
        "name": "get_shopify_order",
        "description": (
            "Look up a Shopify order's conversion attribution and "
            "metadata: source_name (web / pos / shopify_draft / "
            "mobile_app), landing_site (first page hit), "
            "referring_site (where they came from — google, "
            "instagram, t.co, etc.), customer_locale, "
            "note_attributes (UTM params + custom theme keys), "
            "discount_codes redeemed, customer history (orders "
            "count, total spent, tags). Use when a CIN7 sale has "
            "SourceChannel='Shopify' AND the user asks 'how did we "
            "get this conversion', 'what was the traffic source', "
            "'what coupon did they use', 'is this a returning "
            "customer'. The AI Assistant should automatically "
            "follow up with this tool when asked attribution-flavoured "
            "questions about a Shopify-channel sale. Match by order "
            "Name (#1234), order_number (1234), email, or "
            "customer_name + date range. Returns matched=0 if the "
            "order isn't in the local sync window OR the integration "
            "isn't configured."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "order_name": {
                    "type": "string",
                    "description": (
                        "Shopify order name with or without # "
                        "(e.g. '#1234' or '1234'). The same "
                        "number CIN7 stores in OrderNumber for "
                        "Shopify-channel sales.")
                },
                "email": {
                    "type": "string",
                    "description": (
                        "Customer email — exact match.")
                },
                "customer": {
                    "type": "string",
                    "description": (
                        "Customer name substring. Combine with "
                        "date_from / date_to.")
                },
                "date_from": {
                    "type": "string",
                    "description": (
                        "ISO date (YYYY-MM-DD). Inclusive lower "
                        "bound on created_at.")
                },
                "date_to": {
                    "type": "string",
                    "description": (
                        "ISO date (YYYY-MM-DD). Inclusive upper "
                        "bound on created_at.")
                },
                "limit": {
                    "type": "integer",
                    "description": (
                        "Max orders to return (cap 25, default "
                        "10).")
                },
            },
        },
    },
    {
        # v2.67.51 — stock-adjustment lookup. The local sync currently
        # captures HEADERS only (TaskID, EffectiveDate, StocktakeNumber,
        # Status, Account, Reference). Line-level detail (which SKUs
        # were adjusted, by how much) requires a per-task detail call
        # which we don't sync yet. The tool returns whatever we have
        # locally + a note flagging the limitation so the AI doesn't
        # claim it sees line-level data.
        "name": "get_stock_adjustment",
        "description": (
            "Look up a specific CIN7 stock adjustment / stocktake "
            "by number (e.g. ST-12345) or list adjustments matching "
            "a date range / reference. Returns header detail "
            "(EffectiveDate, Status, Account, Reference). NOTE: "
            "line-level detail (per-SKU before/after qty) is NOT "
            "currently in the local sync — the tool tells the user "
            "to view the adjustment in CIN7 directly for line "
            "detail. Use this for 'show me ST-2034', 'what "
            "adjustments did we run last week', 'find the "
            "stocktake for warehouse 500'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "stocktake_number": {
                    "type": "string",
                    "description": (
                        "Stocktake / adjustment number (e.g. "
                        "ST-12345). Case-insensitive.")
                },
                "reference_substring": {
                    "type": "string",
                    "description": (
                        "Substring match against Reference field "
                        "(used for 'sales order 7103', 'cycle "
                        "count', etc.).")
                },
                "date_from": {
                    "type": "string",
                    "description": (
                        "ISO date (YYYY-MM-DD). Inclusive lower "
                        "bound on EffectiveDate.")
                },
                "date_to": {
                    "type": "string",
                    "description": (
                        "ISO date (YYYY-MM-DD). Inclusive upper "
                        "bound on EffectiveDate.")
                },
                "status": {
                    "type": "string",
                    "description": (
                        "Filter by Status (COMPLETED, VOIDED, "
                        "DRAFT). Default: all.")
                },
                "limit": {
                    "type": "integer",
                    "description": (
                        "Max adjustments to return (cap 50, "
                        "default 25).")
                },
            },
        },
    },
    {
        "name": "search_products_by_text",
        "description": (
            "PRIMARY STOCK-LISTING TOOL (v2.67.25). Use this for "
            "any 'what do we have', 'what's in stock', 'do we have "
            "X' question — CIN7 is the source of truth for stock "
            "quantities, classification (active/slow/dead/excess), "
            "and the trend rating (Stable / 📈 Trend / 🎯 Project / "
            "🔀 Mixed / 📉 Decline). Pass parents_only=true and "
            "in_stock_only=true for the buyer/sales-staff stock "
            "answer. Each row returns SKU + Name + OnHand qty + "
            "Classification + trend_flag, so the staff can see "
            "what's available AND what to prioritise selling. "
            "Substring search across product TEXT fields "
            "(title / description / tags / product_type / "
            "collections). Tokenized AND-match (every word in "
            "`query` must hit) + OR-match via `any_of_terms` (at "
            "least one — used for Kelvin alternatives) + "
            "`exclude_types` block list. For the catalog-discovery "
            "case ('tell me about the White Iris series'), use "
            "`find_products` instead — it unions Shopify "
            "descriptions and family info."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": (
                        "AND-matched search phrase. Tokenized on "
                        "whitespace; every token must appear in at "
                        "least one searched field. Use for required "
                        "concepts (e.g. 'led strip')."),
                },
                "any_of_terms": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "OR-matched terms. At least one must appear "
                        "in the searched fields. Use for Kelvin / "
                        "color-temp alternatives — e.g. for 'warm "
                        "white' pass ['warm white', '2200K', "
                        "'2400K', '2700K', '2800K', '3000K']. "
                        "Composes with `query`: row needs ALL query "
                        "tokens AND at least one any_of_terms hit."),
                },
                "exclude_types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Block list. Drop rows whose Name or Type "
                        "contains any of these keywords. Use to keep "
                        "'led strip' searches from returning "
                        "dimmers/controllers/power supplies/etc. — "
                        "e.g. ['dimmer', 'controller', 'power "
                        "supply', 'channel', 'profile', 'accessory', "
                        "'service', 'module']."),
                },
                "fields": {
                    "type": "array",
                    "items": {"type": "string",
                              "enum": ["title", "name", "description",
                                       "tags", "product_type", "type",
                                       "collections", "category",
                                       "family"]},
                    "description": (
                        "Which product fields to search across. "
                        "Defaults to ['title']. Pass the list from "
                        "the alias rule's search_fields."),
                },
                "classification": {
                    "type": "string",
                    "enum": ["active", "slow", "dead", "watchlist", "any"],
                    "description": (
                        "Optional secondary filter to a specific "
                        "stock classification."),
                },
                "in_stock_only": {
                    "type": "boolean",
                    "description": (
                        "If true, only return SKUs with on-hand > 0."),
                },
                "parents_only": {
                    "type": "boolean",
                    "description": (
                        "**Default TRUE (v2.67.31).** Hides child "
                        "SKUs (per-foot cuts, BOM derivatives, "
                        "fractional sources) so only supplier-"
                        "orderable parents and standalone products "
                        "are returned. Reuses the engine's "
                        "`is_non_master_tube` column. DO NOT pass "
                        "false for stock questions — children "
                        "duplicate their parent visually and "
                        "crowd out other families. Pass false ONLY "
                        "when the user explicitly asks for every "
                        "variant ('show me every length and "
                        "voltage of Iris 2700K')."),
                },
                "family": {
                    "type": "string",
                    "description": (
                        "Optional product-family code to narrow "
                        "further (e.g. SIERRA38)."),
                },
                "limit": {
                    "type": "integer",
                    "description": (
                        "Max rows to return (cap 200, default 25). "
                        "v2.67.28 — for STOCK-LISTING questions "
                        "('what warm white strips do we have', "
                        "'what's in stock', 'show me our slow "
                        "movers'), PASS LIMIT=200 to ensure every "
                        "parent/standalone family is represented. "
                        "Iris alone has ~9 warm-white parents (3 "
                        "kelvins × 3 densities); plus Lily, "
                        "Cardinal, Honey Suckle, Decor, Elite "
                        "Gold, Liatris, Sierra, Smokies → 40-60 "
                        "parents total. Default 25 is for narrow "
                        "lookups only."),
                },
            },
            "required": ["query"],
        },
    },
    {
        # v2.67 — unified product discovery across CIN7 inventory AND
        # the Shopify product knowledge base. Implementation lives in
        # product_search.py; ai_tools.find_products is a thin wrapper.
        "name": "find_products",
        "description": (
            "PRODUCT-KNOWLEDGE / CATALOG-DISCOVERY tool (v2.67.25). "
            "Use this when the user wants to UNDERSTAND the "
            "catalog — family relationships, customer-facing "
            "descriptions, what series exist, Shopify URLs, "
            "differences between product lines. NOT for stock "
            "questions — for 'what do we have in stock', use "
            "`search_products_by_text` with parents_only=true "
            "(CIN7 is the stock truth and find_products produces "
            "messy Shopify-only fallback rows when bulk-roll "
            "parents aren't in the customer-facing .md catalog). "
            "Use find_products for: 'tell me about the White Iris "
            "series', 'what families do we sell', 'what's the "
            "difference between Decor and Elite Gold', 'show me "
            "our high-CRI options'. Unions CIN7 + Shopify, marking "
            "each row with source ∈ {cin7, shopify, both}. "
            "Surfaces Shopify-only families (White Lily, etc.) "
            "that aren't yet in CIN7 — these come back with "
            "stock_status='unknown' and a `note` — surface them "
            "with the note, don't silently omit. Each row also "
            "carries `classification` (active/slow/dead/excess) "
            "and `trend_flag` (Stable/📈/🎯/🔀/📉) when CIN7 has "
            "the SKU. Pass any_of_terms for color-temp "
            "alternatives, e.g. ['warm white', '2200K', '2400K', "
            "'2700K', '2800K', '3000K']."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": (
                        "AND-matched search phrase. Tokenized on "
                        "whitespace; every token must appear in at "
                        "least one searched field on each result."),
                },
                "any_of_terms": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "OR-matched alternatives. At least one must "
                        "appear in the searched fields. Multi-word "
                        "phrases are supported (substring match), "
                        "e.g. ['warm white', '2200K', '2400K', "
                        "'2700K', '2800K', '3000K'] for warm-white "
                        "Kelvin range."),
                },
                "exclude_types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Block list — drop rows whose name/title "
                        "contains any of these. If omitted and "
                        "`query` includes 'strip', a default "
                        "accessories block list is applied "
                        "(dimmer, controller, profile, etc.)."),
                },
                "families": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Restrict results to these family codes. "
                        "Known families (v2.67): ELITE_GOLD, "
                        "WHITE_IRIS, WHITE_LILY, DECOR, "
                        "CARDINAL_FLOWER, LIATRIS, BALTIC_IVY, "
                        "HONEY_SUCKLE, SIERRA, SMOKIES, OSLO, "
                        "SLIM8, SLIM, PLW80, PLW70, DISA. The "
                        "family detector is a placeholder until "
                        "the product_attributes table ships."),
                },
                "in_stock_only": {
                    "type": "boolean",
                    "description": (
                        "Default true. Filters CIN7-side rows to "
                        "OnHand>0; Shopify-only rows are ALWAYS "
                        "returned with stock_status='unknown' so "
                        "the answer doesn't silently omit families "
                        "that aren't in CIN7."),
                },
                "parents_only": {
                    "type": "boolean",
                    "description": (
                        "Default true (v2.67.22). Hides child SKUs "
                        "that are derived from a parent via BOM "
                        "rules or sourcing-fraction rules — e.g. "
                        "the per-foot LEDIRIS2700-120-0305 is "
                        "hidden in favor of the supplier-orderable "
                        "parent LEDIRIS2700-120-100M. Mirrors the "
                        "Ordering page's behaviour: the buyer crew "
                        "asks 'what warm white strips do we have?' "
                        "to make stock decisions, and child variants "
                        "duplicate their parent visually while "
                        "crowding out other families. Pass false "
                        "ONLY when the user explicitly asks for "
                        "every variant ('show me every length and "
                        "voltage of Iris 2700K') or when they're "
                        "looking up a specific child SKU by name."),
                },
                "limit": {
                    "type": "integer",
                    "description": (
                        "Max rows to return (default 100, hard "
                        "max 200). v2.67.20 widened the budget so "
                        "comprehensive answers (e.g. 'all warm-white "
                        "strips') can surface every family × kelvin "
                        "× density (60/120/180 LEDs/m) parent. "
                        "Pass-1 emits up to 70% of limit for breadth "
                        "(family + kelvin variety); the remaining "
                        "30% drains the deferred queue in pass-2 so "
                        "density variants always come through."),
                },
            },
            "required": ["query"],
        },
    },
    # -----------------------------------------------------------------
    # v2.67.95 — Marketing intelligence tools.
    # These read from tables populated by klaviyo_sync, reviewsio_sync,
    # semrush_sync (and Phase 2: google_ads_sync, ga4_sync). They
    # answer 'why might this SKU be moving / not moving' by surfacing
    # the marketing context.
    # -----------------------------------------------------------------
    {
        "name": "get_email_attribution",
        "description": (
            "Return Klaviyo email campaigns that drove clicks or "
            "revenue on a given SKU/family/handle in the last N "
            "days. Use this when the user asks why a product spiked "
            "in sales, or wants to see the impact of a recent "
            "newsletter on a specific product."),
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {
                    "type": "string",
                    "description": (
                        "SKU to query — e.g. LED-V3060001-2390 or "
                        "the family code if SKU not known.")},
                "shopify_handle": {
                    "type": "string",
                    "description": (
                        "Shopify product handle, e.g. "
                        "'slim-led-channel-slim8-ac2-z'. Use this "
                        "if you have the handle but not the SKU.")},
                "days": {
                    "type": "integer",
                    "description": (
                        "Lookback window in days (default 90).")},
            },
        },
    },
    {
        "name": "get_seo_signals",
        "description": (
            "Return SEMrush keyword ranking observations for a "
            "SKU/family/handle in the last N days. Shows position, "
            "previous_position, search_volume, ranking URL. Use "
            "when the user asks why a product's traffic / sales "
            "shifted, or wants to see SEO performance on a "
            "specific item or family."),
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {
                    "type": "string",
                    "description": "SKU to query"},
                "family": {
                    "type": "string",
                    "description": (
                        "Family code (e.g. 'V3060001'). Use this "
                        "for category-level questions when SKU not "
                        "known.")},
                "days": {
                    "type": "integer",
                    "description": (
                        "Lookback window in days (default 30).")},
            },
        },
    },
    {
        "name": "get_product_reviews",
        "description": (
            "Return reviews.io review summary + recent reviews for "
            "a SKU. Includes average rating, count, low-star count, "
            "and the latest 5 reviews so the buyer can see "
            "qualitative feedback. Use this when the user asks "
            "about product quality, customer satisfaction, "
            "complaints, or whether to reorder based on reviews."),
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {
                    "type": "string",
                    "description": "SKU to query (required)"},
                "include_recent": {
                    "type": "boolean",
                    "description": (
                        "If true, include the 5 most recent "
                        "reviews verbatim (default true).")},
            },
            "required": ["sku"],
        },
    },
    {
        "name": "get_marketing_intelligence",
        "description": (
            "One-shot composite tool: returns ALL marketing signals "
            "for a SKU/family in one call — recent SEO ranks, email "
            "campaigns that touched it, review summary, and "
            "(Phase 2 onwards) ad-campaign attribution. Use this "
            "when the user asks an open-ended 'what's happening "
            "with this product' question. Cheaper than calling "
            "get_email_attribution + get_seo_signals + "
            "get_product_reviews separately."),
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {"type": "string"},
                "family": {"type": "string"},
                "shopify_handle": {"type": "string"},
                "days": {
                    "type": "integer",
                    "description": (
                        "Lookback window in days (default 30).")},
            },
        },
    },
    # -----------------------------------------------------------------
    # v2.67.102 — Campaign-level Moby-replacement tools.
    # These query ad_campaigns_daily + ad_campaign_skus (populated by
    # google_ads_sync.py + ga4_sync.py) and replicate the analyses
    # Triple Whale's Moby chat used to surface.
    # -----------------------------------------------------------------
    {
        "name": "get_ad_overview",
        "description": (
            "Top-level paid-marketing summary for a date window: "
            "total spend, GA4-attributed revenue, platform self-"
            "reported revenue, computed ROAS for both attribution "
            "models, # of campaigns active, top 5 spending "
            "campaigns. Use this for 'how are our Google Ads doing "
            "this month?' style questions. Reads the cin7_sync "
            "ad_campaigns_daily table (populated nightly from "
            "Google Ads API + GA4 Data API)."),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": (
                        "Lookback window (default 30).")},
                "platform": {
                    "type": "string",
                    "description": (
                        "Filter by platform: 'google_ads' (default) "
                        "or 'meta' or 'all'. Currently only "
                        "google_ads is populated."),
                    "enum": ["google_ads", "meta", "all"]},
            },
        },
    },
    {
        "name": "get_campaign_performance",
        "description": (
            "Per-campaign performance table for a date window. "
            "Returns each campaign's spend, clicks, conversions, "
            "revenue (both platform self-report AND GA4-attributed), "
            "ROAS, CPA. Use this for 'show me all my Search "
            "campaigns' / 'sort campaigns by ROAS' / 'top spending "
            "campaigns'."),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Lookback window (default 30)"},
                "platform": {
                    "type": "string",
                    "enum": ["google_ads", "meta", "all"]},
                "campaign_type": {
                    "type": "string",
                    "description": (
                        "Filter by campaign type substring "
                        "(case-insensitive): 'shopping', 'search', "
                        "'pmax', 'display'. Empty = all types.")},
                "sort_by": {
                    "type": "string",
                    "enum": ["spend", "ga4_roas",
                              "platform_roas", "ga4_revenue"],
                    "description": "Sort order (default 'spend')"},
                "limit": {
                    "type": "integer",
                    "description": (
                        "Max campaigns to return (default 25, "
                        "max 100).")},
            },
        },
    },
    {
        "name": "find_campaigns_to_cut",
        "description": (
            "Campaigns underperforming against a ROAS threshold. "
            "Use when the user asks 'which campaigns should I "
            "pause / cut'. Returns campaigns with GA4 ROAS below "
            "threshold AND meaningful spend (so we don't flag "
            "$5/day campaigns as cuts)."),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Lookback (default 30)"},
                "min_roas": {
                    "type": "number",
                    "description": (
                        "GA4 ROAS threshold; campaigns BELOW this "
                        "are flagged. Default 2.0 (i.e. spending $1 "
                        "to make less than $2).")},
                "min_spend": {
                    "type": "number",
                    "description": (
                        "Minimum total spend in window to consider "
                        "(filters out trivial-budget campaigns). "
                        "Default 100.")},
            },
        },
    },
    {
        "name": "find_campaigns_to_scale",
        "description": (
            "Campaigns over-performing against a ROAS threshold "
            "with budget headroom. Use when the user asks 'which "
            "campaigns can I increase budget on'. Returns campaigns "
            "with GA4 ROAS above threshold AND consistent "
            "high spend."),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Lookback (default 30)"},
                "min_roas": {
                    "type": "number",
                    "description": (
                        "GA4 ROAS threshold; campaigns ABOVE this "
                        "are flagged. Default 4.0.")},
                "min_spend": {
                    "type": "number",
                    "description": (
                        "Minimum total spend (so we don't surface "
                        "low-volume noise). Default 500.")},
            },
        },
    },
    {
        "name": "attribution_sanity_check",
        "description": (
            "Compares platform self-reported revenue vs GA4 "
            "attribution per campaign. Surfaces campaigns where "
            "the platform's number diverges significantly from "
            "GA4 (typical sign of view-through inflation). This is "
            "the same diagnostic Triple Whale's Moby chat surfaced. "
            "Returns each campaign with platform_revenue, "
            "ga4_revenue, and the ratio (platform/ga4)."),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Lookback (default 30)"},
                "campaign_id": {
                    "type": "string",
                    "description": (
                        "Optional: drill into one campaign. "
                        "Otherwise returns top inflators.")},
                "min_inflation_ratio": {
                    "type": "number",
                    "description": (
                        "Only surface campaigns where "
                        "platform/ga4 >= this. Default 1.5 "
                        "(50% inflation).")},
            },
        },
    },
    {
        "name": "get_sku_ad_spend",
        "description": (
            "Returns total Google Ads spend, attributed revenue, "
            "clicks, impressions, purchases, ROAS for a specific "
            "SKU over a date window. Plus the per-campaign "
            "breakdown showing which campaigns drove which spend "
            "on this product. Use for 'what did we spend on "
            "advertising LED-Slim8' / 'which campaigns target "
            "this product' / 'is this SKU profitable to advertise'."
            " Source: shopping_performance_view (Shopping + PMax "
            "shopping component)."),
        "input_schema": {
            "type": "object",
            "properties": {
                "sku": {"type": "string"},
                "days": {
                    "type": "integer",
                    "description": "Lookback (default 30)"},
            },
            "required": ["sku"],
        },
    },
    {
        "name": "compare_ad_periods",
        "description": (
            "Compares two date windows side-by-side: spend, "
            "revenue, ROAS, # campaigns. Use for 'how is May "
            "tracking vs April' or 'last 7 days vs prior 7 "
            "days'. Returns deltas + percentage changes."),
        "input_schema": {
            "type": "object",
            "properties": {
                "current_days": {
                    "type": "integer",
                    "description": (
                        "Length of the current window (default 30)")},
                "compare_to_days_ago": {
                    "type": "integer",
                    "description": (
                        "How many days back the COMPARE window "
                        "starts (default 60 = 'last 30 days vs "
                        "prior 30 days'). Set to 7+current_days "
                        "for week-over-week.")},
            },
        },
    },
    # v2.67.250 — Notion-backed knowledge-base search.
    # v2.67.261 — renamed from search_knowledge_base to
    # search_team_playbooks: it collided with the on-disk
    # ai_kb docs tool of the same name, and the Claude API
    # rejects a tool list with duplicate names ("Tool names
    # must be unique") — which silently broke EVERY AI call.
    {
        "name": "search_team_playbooks",
        "description": (
            "Search the team's internal knowledge base "
            "(operational playbooks, processes, escalation "
            "rules, FAQs) mirrored from Notion. Use this for "
            "'how do we…' / 'what's our process for…' style "
            "questions — e.g. 'how do we handle drop-ship "
            "backorders?', 'what's our PO approval process?', "
            "'rules for stock-issue escalation', 'supplier "
            "payment terms'. Returns matching articles with "
            "title, content excerpt and the Notion URL — "
            "ALWAYS cite the URL when you ground an answer in "
            "an article. Returns empty when nothing matches; "
            "fall back to your normal reasoning if so."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": (
                        "Free-text search across article titles "
                        "and bodies. Keep it short — 2-6 "
                        "keywords work best. Examples: "
                        "'drop-ship backorder', 'PO approval', "
                        "'stock issue escalation'."),
                },
            },
            "required": ["query"],
        },
    },
    # v2.67.281 — product cross-section dimensions, sourced from
    # the Notion "Product Dimensions" page (mirrored locally).
    {
        "name": "get_product_dimensions",
        "description": (
            "Look up the physical cross-section dimensions of an "
            "LED channel / profile product: outer width & height, "
            "the LED-strip channel (recess) width & depth, the max "
            "strip width that fits, mounting type (surface / "
            "mud-in / recessed / corner / pendant), profile shape, "
            "and wing geometry. Use this whenever someone asks how "
            "big a profile is, what size strip fits a channel, how "
            "deep a channel is, or how a profile mounts. Search by "
            "product name, model, Shopify handle, or SKU family. "
            "All values are in millimetres. If nothing matches, "
            "tell the user dimensions haven't been catalogued for "
            "that product — do NOT guess or estimate dimensions."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": (
                        "Product name, model, Shopify handle, or "
                        "SKU family. Examples: 'Hide10', 'Slim8', "
                        "'mud-in drywall channel', 'H7000200'."),
                },
            },
            "required": ["query"],
        },
    },
]


# ---------------------------------------------------------------------------
# Tool implementations.
# Each takes (engine_df, sale_lines_df, args_dict) and returns a dict.
# engine_df is the cached ABC engine output passed in by the Streamlit
# page; we don't recompute it per-tool-call (would be too slow).
# ---------------------------------------------------------------------------

def _serialise_row(row: dict) -> dict:
    """Make a row JSON-friendly: convert NaN/None, dates to strings.
    v2.67.51 — list/dict values pass through unchanged so nested
    structures (e.g. a PO record with `lines: [...]` already
    serialised) don't get collapsed to `str(v)`."""
    out = {}
    for k, v in row.items():
        if v is None:
            out[k] = None
        elif isinstance(v, float):
            if pd.isna(v):
                out[k] = None
            else:
                out[k] = round(v, 2)
        elif isinstance(v, (pd.Timestamp, )):
            out[k] = v.strftime("%Y-%m-%d") if not pd.isna(v) else None
        elif isinstance(v, (int, str, bool)):
            out[k] = v
        elif isinstance(v, (list, tuple, dict)):
            # Pass nested structures through. They were either built
            # by the caller as JSON-safe primitives, or they're a
            # list of already-serialised dicts (the get_purchase_order
            # / get_sale_order pattern: per-line dicts stored under
            # `lines`).
            out[k] = v
        else:
            out[k] = str(v)
    return out


def search_products(engine_df: pd.DataFrame,
                     sale_lines_df: pd.DataFrame,
                     args: dict) -> dict:
    query = (args.get("query") or "").strip().lower()
    family = (args.get("family") or "").strip().upper()
    classification = (args.get("classification") or "any").strip().lower()
    abc_class = (args.get("abc_class") or "any").strip().upper()
    in_stock_only = bool(args.get("in_stock_only", False))
    parents_only = bool(args.get("parents_only", True))
    limit = min(int(args.get("limit", 25) or 25), 50)

    # v2.67.27 — read engine_df as-is. Earlier versions tried to
    # derive a synthesised Classification column, but that:
    #   (a) caused a "shape mismatch" runtime error in production,
    #   (b) duplicated information the engine already computes
    #       (is_dormant, excess_units, trend_flag).
    # The AI now reads those raw engine signals instead.
    df = engine_df.copy()
    if parents_only and "is_non_master_tube" in df.columns:
        df = df[~df["is_non_master_tube"].fillna(False)]
    if query:
        mask_sku = df["SKU"].astype(str).str.lower().str.contains(
            query, na=False)
        mask_name = df["Name"].astype(str).str.lower().str.contains(
            query, na=False)
        df = df[mask_sku | mask_name]
    if family and "Family" in df.columns:
        df = df[df["Family"].astype(str).str.upper() == family]
    # v2.67.27 — classification arg maps to engine columns directly
    # (same logic as search_products_by_text). See that function for
    # the mapping rationale.
    if classification == "slow" and "is_dormant" in df.columns:
        _onh = df["OnHand"].fillna(0) if "OnHand" in df.columns else 0
        df = df[df["is_dormant"].fillna(False) & (_onh > 0)]
    elif classification == "dead" and "effective_units_12mo" in df.columns:
        _onh = df["OnHand"].fillna(0) if "OnHand" in df.columns else 0
        df = df[(_onh > 0)
                & (df["effective_units_12mo"].fillna(0) == 0)]
    elif classification == "excess" and "excess_units" in df.columns:
        df = df[df["excess_units"].fillna(0) > 0]
    elif classification != "any" and "Classification" in df.columns:
        # Fallback: literal Classification column match (legacy path
        # for any engine output that does populate the column).
        df = df[df["Classification"].astype(str).str.lower()
                  == classification]
    if abc_class != "ANY" and "ABC" in df.columns:
        df = df[df["ABC"].astype(str).str.upper() == abc_class]
    if in_stock_only and "OnHand" in df.columns:
        df = df[df["OnHand"].fillna(0) > 0]

    # v2.67.27 — surface the engine's actual signal columns so the
    # AI can read them directly instead of hoping for a synthesised
    # Classification field.
    # v2.67.274 — include Bin (warehouse shelf location) so the AI
    # always reports WHERE a SKU is stored alongside how much is on hand.
    cols_we_want = [c for c in [
        "SKU", "Name", "Family", "ABC", "Classification",
        "OnHand", "Bin", "TargetStock", "ReorderSuggested",
        "trend_flag", "is_dormant", "excess_units",
        "effective_units_12mo",
    ] if c in df.columns]
    df = df.head(limit)[cols_we_want]
    rows = [_serialise_row(r._asdict() if hasattr(r, "_asdict") else dict(r))
            for r in df.to_dict(orient="records")]
    # Pandas to_dict already gives plain dicts, but _serialise_row
    # normalises NaN/dates.
    rows = [_serialise_row(r) for r in df.to_dict(orient="records")]
    return {
        "matched": len(rows),
        "results": rows,
        "note": (
            f"Showing first {limit} of potentially many. Refine "
            "query if you need a narrower set."
            if len(rows) == limit else None),
    }


def get_sku_details(engine_df: pd.DataFrame,
                     sale_lines_df: pd.DataFrame,
                     args: dict) -> dict:
    sku = (args.get("sku") or "").strip()
    if not sku:
        return {"error": "sku is required"}
    row = engine_df[engine_df["SKU"].astype(str) == sku]
    if row.empty:
        return {"error": f"SKU {sku!r} not found in engine_df."}
    row = row.iloc[0]
    detail = _serialise_row(dict(row))
    # Add migration mapping if any
    mig_chain = _get_migration_chain_for_sku(sku)
    if mig_chain:
        detail["migration_chain"] = mig_chain
    # v2.67.308 — guidance for the assistant on which demand field to
    # quote. `units_12mo` is direct invoice-line sum; `effective_units_
    # 12mo` adds tube-rollup-in + migrated-in and is the CANONICAL
    # demand signal (what ABC ranking, target_stock, reorder math all
    # use). For non-master tube variants `units_12mo` reads near-zero
    # by design (demand sits on the master) — assistant must use
    # effective_units_12mo OR walk to the master via migration_chain.
    _dir = float(detail.get("units_12mo") or 0)
    _eff = float(detail.get("effective_units_12mo") or 0)
    _is_nmt = bool(detail.get("is_non_master_tube") or False)
    _notes = []
    if _is_nmt:
        _notes.append(
            "is_non_master_tube=True — direct units_12mo is near-zero by "
            "design (demand rolled up to master). Report demand from the "
            "master SKU (see migration_chain) instead.")
    if _eff > 0 and _dir > 0:
        _ratio = max(_eff, _dir) / max(min(_eff, _dir), 1)
        if _ratio >= 2.0:
            _notes.append(
                f"effective_units_12mo ({_eff:.0f}) and units_12mo "
                f"({_dir:.0f}) disagree by {_ratio:.1f}x — use the "
                f"`effective_units_12mo` field as the canonical demand. "
                f"`units_12mo` is the raw direct-sale-line sum only.")
    _abc = str(detail.get("ABC") or "").upper()
    _rev = float(detail.get("rev_12mo") or 0)
    if _abc == "A" and _rev < 5000:
        _notes.append(
            f"ABC=A but rev_12mo=${_rev:.0f}. A-class typically means "
            f"top-80% revenue rank — this is internally inconsistent. "
            f"Don't quote ABC=A as a fact without verifying.")
    if _notes:
        detail["assistant_notes"] = _notes
    return detail


def get_velocity(engine_df: pd.DataFrame,
                  sale_lines_df: pd.DataFrame,
                  args: dict) -> dict:
    sku = (args.get("sku") or "").strip()
    days = min(int(args.get("days", 90) or 90), 1825)
    granularity = (args.get("granularity") or "none").strip().lower()
    if not sku:
        return {"error": "sku is required"}
    if sale_lines_df is None or sale_lines_df.empty:
        return {"error": "Sale lines not loaded yet."}
    sl = sale_lines_df.copy()
    if "InvoiceDate" not in sl.columns:
        return {"error": "Sale lines missing InvoiceDate column."}
    sl["InvoiceDate"] = pd.to_datetime(sl["InvoiceDate"], errors="coerce")
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=days)
    # v2.67.308 — exclude CREDITED/VOIDED/CANCELLED so this matches the
    # engine's NET demand calc (engine drops these at app.py:5459-5462;
    # get_velocity historically did not, so a SKU with a big credit memo
    # could read inflated here vs. the engine's units_12mo).
    in_window = sl[(sl["SKU"].astype(str) == sku)
                    & (sl["InvoiceDate"] >= cutoff)]
    if "Status" in in_window.columns:
        _excluded = ("CREDITED", "VOIDED", "CANCELLED")
        in_window = in_window[
            ~in_window["Status"].astype(str).str.upper().isin(_excluded)
        ]
    result = {
        "sku": sku,
        "window_days": days,
        "units_sold": float(pd.to_numeric(
            in_window.get("Quantity", pd.Series(dtype=float)),
            errors="coerce").sum()),
        "revenue": float(pd.to_numeric(
            in_window.get("Total", pd.Series(dtype=float)),
            errors="coerce").sum()),
        "order_count": int(in_window.get(
            "SaleID", pd.Series(dtype=str)).nunique()),
        "first_sale": (in_window["InvoiceDate"].min().strftime("%Y-%m-%d")
                       if not in_window.empty else None),
        "last_sale": (in_window["InvoiceDate"].max().strftime("%Y-%m-%d")
                      if not in_window.empty else None),
    }

    # v2.67.308 — surface the engine's canonical demand signals so the
    # assistant can never report a raw invoice-line sum without seeing
    # what the engine thinks. Andrew flagged LED-G2000620-2 as "94 in May
    # vs ~200/mo avg" — assistant replied "60 units in 12mo · A-class ·
    # Trend" which is internally inconsistent (60 units × ~$15 ≠ A-class
    # revenue rank). Most likely causes when raw-sum and engine signals
    # disagree:
    #   • Tube/MP variant → demand rolled up to master SKU
    #   • Retiring SKU → demand migrated to successor
    #   • CREDITED/VOIDED lines (now excluded above, but historical data
    #     may have edge cases)
    #   • UOM mismatch between invoice and stock
    # The engine's effective_units_12mo is the CANONICAL demand number.
    if engine_df is not None and not engine_df.empty:
        eng_row = engine_df[engine_df["SKU"].astype(str) == sku]
        if not eng_row.empty:
            eng = eng_row.iloc[0]
            _eng_eff = float(eng.get("effective_units_12mo") or 0)
            _eng_dir = float(eng.get("units_12mo") or 0)
            result["engine_signals"] = {
                "ABC": str(eng.get("ABC") or ""),
                "trend_flag": str(eng.get("trend_flag") or ""),
                "is_dormant": bool(eng.get("is_dormant", False)),
                "is_non_master_tube": bool(
                    eng.get("is_non_master_tube", False)),
                "units_12mo_direct": _eng_dir,
                "effective_units_12mo": _eng_eff,
                "units_45d": float(eng.get("units_45d") or 0),
                "units_prior_45d": float(eng.get("units_prior_45d") or 0),
                "excess_units": float(eng.get("excess_units") or 0),
                "migrated_in": float(eng.get("migrated_in") or 0),
                "migrated_out": float(eng.get("migrated_out") or 0),
                "tube_rollup_in": float(eng.get("tube_rollup_in") or 0),
            }
            # Consistency check — only meaningful when window is ~12mo.
            if 300 <= days <= 400 and _eng_eff > 0:
                _sold = result["units_sold"]
                _ratio_hi = max(_eng_eff, _sold) / max(min(_eng_eff, _sold), 1)
                if _ratio_hi >= 2.0:
                    result["consistency_warning"] = (
                        f"Raw invoice-line sum for this SKU "
                        f"({_sold:.0f} units in last {days}d) disagrees "
                        f"with the engine's effective_units_12mo "
                        f"({_eng_eff:.0f}) by {_ratio_hi:.1f}x. The "
                        f"engine number is canonical — it accounts for "
                        f"tube-rollup ({eng.get('tube_rollup_in') or 0:g}), "
                        f"migration in/out "
                        f"({eng.get('migrated_in') or 0:g}/"
                        f"{eng.get('migrated_out') or 0:g}), and status "
                        f"exclusions. Report effective_units_12mo as the "
                        f"demand figure; mention the raw sum only if the "
                        f"user is asking specifically about invoiced lines."
                    )
            # Internal-consistency check on the engine row itself.
            # A-class with very low effective demand is a red flag — report
            # it so the assistant doesn't repeat both numbers as fact.
            _abc = str(eng.get("ABC") or "").upper()
            _rev = float(eng.get("rev_12mo") or 0)
            if _abc == "A" and _rev < 5000:
                result.setdefault("internal_warnings", []).append(
                    f"ABC=A but rev_12mo=${_rev:.0f} — A-class typically "
                    f"means top-80% revenue rank, so this likely indicates "
                    f"stale ABC computation or a data anomaly. Don't "
                    f"present ABC=A as a fact without checking."
                )

    # Time-bucketed breakdown — the UI looks for `chart_data` and
    # renders an inline st.line_chart when present.
    if granularity != "none" and not in_window.empty:
        df = in_window.copy()
        df["__qty"] = pd.to_numeric(
            df.get("Quantity", 0), errors="coerce").fillna(0)
        if granularity == "day":
            df["__bkt"] = df["InvoiceDate"].dt.strftime("%Y-%m-%d")
            label = "Daily units sold"
        elif granularity == "week":
            df["__bkt"] = (df["InvoiceDate"].dt.to_period("W")
                            .apply(lambda p: f"{p.start_time:%Y-%m-%d}"))
            label = "Weekly units sold"
        else:  # month
            df["__bkt"] = df["InvoiceDate"].dt.strftime("%Y-%m")
            label = "Monthly units sold"
        bucketed = df.groupby("__bkt")["__qty"].sum().sort_index()
        # Fill in missing buckets so the chart line is continuous,
        # not a series of dots with gaps.
        if granularity == "day":
            full_idx = pd.date_range(cutoff.normalize(),
                                       pd.Timestamp.now().normalize(),
                                       freq="D").strftime("%Y-%m-%d")
            bucketed = bucketed.reindex(full_idx, fill_value=0)
        result["chart_data"] = {
            "label": label,
            "x_label": granularity,
            "y_label": "Units",
            "series": [{
                "x": str(k),
                "y": float(v),
            } for k, v in bucketed.items()],
        }
    return result


def get_dead_stock(engine_df: pd.DataFrame,
                    sale_lines_df: pd.DataFrame,
                    args: dict) -> dict:
    classification = (args.get("classification") or "all").strip().lower()
    family = (args.get("family") or "").strip().upper()
    min_value = float(args.get("min_stock_value", 0) or 0)
    limit = min(int(args.get("limit", 25) or 25), 100)

    if "Classification" not in engine_df.columns:
        return {"error": "engine_df missing Classification column."}
    df = engine_df.copy()
    if classification == "all":
        df = df[df["Classification"].astype(str).str.lower().isin(
            ["dead", "slow", "watchlist"])]
    else:
        df = df[df["Classification"].astype(str).str.lower()
                  == classification]
    if family and "Family" in df.columns:
        df = df[df["Family"].astype(str).str.upper() == family]
    if min_value > 0:
        if "StockValue" in df.columns:
            df = df[df["StockValue"].fillna(0) >= min_value]
        elif "OnHand" in df.columns and "EffectiveUnitCost" in df.columns:
            df["__sv"] = (df["OnHand"].fillna(0)
                          * df["EffectiveUnitCost"].fillna(0))
            df = df[df["__sv"] >= min_value]
    cols = [c for c in [
        "SKU", "Name", "Family", "Classification",
        "OnHand", "Bin", "StockValue", "ABC",
    ] if c in df.columns]
    df = df.sort_values(
        by=cols[0] if "OnHand" not in cols else "OnHand",
        ascending=False).head(limit)[cols]
    return {
        "matched": len(df),
        "results": [_serialise_row(r) for r
                     in df.to_dict(orient="records")],
    }


def _get_migration_chain_for_sku(sku: str) -> Optional[dict]:
    """Walks db.sku_migrations to build the predecessor/successor
    chain for a SKU. Returns None if no migration touches this SKU."""
    migs = [dict(m) for m in db.all_migrations()]
    predecessors = [m for m in migs if m.get("successor_sku") == sku]
    successors = [m for m in migs if m.get("retiring_sku") == sku]
    if not predecessors and not successors:
        return None
    return {
        "predecessors": [
            {"sku": m["retiring_sku"],
             "share_pct": m.get("share_pct"),
             "set_by": m.get("set_by")}
            for m in predecessors],
        "successors": [
            {"sku": m["successor_sku"],
             "share_pct": m.get("share_pct"),
             "set_by": m.get("set_by")}
            for m in successors],
    }


def get_migration_chain(engine_df: pd.DataFrame,
                         sale_lines_df: pd.DataFrame,
                         args: dict) -> dict:
    sku = (args.get("sku") or "").strip()
    if not sku:
        return {"error": "sku is required"}
    chain = _get_migration_chain_for_sku(sku)
    if chain is None:
        return {"sku": sku, "chain": None,
                 "note": "No migration mapping recorded for this SKU."}
    return {"sku": sku, "chain": chain}


def get_sales_totals(engine_df: pd.DataFrame,
                       sale_lines_df: pd.DataFrame,
                       args: dict) -> dict:
    """Aggregate company-wide sales for a period, optionally grouped
    by day/week/month. Pulls revenue from sales_full (headers, includes
    shipping/tax) when available; falls back to sale_lines.Total.
    Units come from sale_lines.Quantity.

    NB: this tool needs the headers DataFrame, not just sale_lines.
    The Streamlit page passes both into the dispatcher via the
    `sale_lines_df` slot AND we look up sales_full from a process-level
    cache populated by the page on first call. To keep the dispatch
    signature uniform, we use the module-level _SALES_FULL hook below.
    """
    raw_period = args.get("period")
    raw_start = (args.get("start_date") or "").strip()
    raw_end = (args.get("end_date") or "").strip()
    group_by = (args.get("group_by") or "none").strip().lower()

    today = pd.Timestamp.now().normalize()

    # v2.67.19 — custom date range branch. If start_date/end_date are
    # provided, use those (they win over period). This makes arbitrary
    # historical ranges like 'April 2026' or 'Q1 2026' first-class
    # without forcing Claude into the breakdown-inference workaround.
    if raw_start or raw_end:
        if not (raw_start and raw_end):
            return {"error": "start_date and end_date must both be "
                              "provided (ISO YYYY-MM-DD)."}
        try:
            start = pd.Timestamp(raw_start).normalize()
            end = pd.Timestamp(raw_end).normalize()
        except (ValueError, TypeError) as exc:
            return {"error": f"Could not parse start_date/end_date "
                             f"({exc}). Expect ISO YYYY-MM-DD."}
        if start > end:
            return {"error": f"start_date {raw_start!r} is after "
                             f"end_date {raw_end!r}."}
        period = f"custom ({raw_start} to {raw_end})"
    else:
        period = (raw_period or "mtd").strip().lower()
        if period == "today":
            start, end = today, today
        elif period == "yesterday":
            start = end = today - pd.Timedelta(days=1)
        elif period == "mtd":
            start, end = today.replace(day=1), today
        elif period == "last_7_days":
            start, end = today - pd.Timedelta(days=7), today
        elif period == "last_30_days":
            start, end = today - pd.Timedelta(days=30), today
        elif period == "last_90_days":
            start, end = today - pd.Timedelta(days=90), today
        elif period == "last_365_days":
            start, end = today - pd.Timedelta(days=365), today
        elif period == "ytd":
            start = pd.Timestamp(year=today.year, month=1, day=1)
            end = today
        elif period == "last_year":
            start = pd.Timestamp(year=today.year - 1, month=1, day=1)
            end = pd.Timestamp(year=today.year - 1, month=12, day=31)
        else:
            return {"error": f"Unknown period {period!r}"}

    # Headers (revenue) — order-level, includes shipping/tax.
    rev_total = 0.0
    rev_by_bucket: dict = {}
    headers = _SALES_FULL_HOLDER.get("df")
    if headers is not None and not headers.empty:
        h = headers.copy()
        if "InvoiceDate" in h.columns:
            h["InvoiceDate"] = pd.to_datetime(
                h["InvoiceDate"], errors="coerce")
            h = h.dropna(subset=["InvoiceDate"])
            rev_col = next(
                (c for c in ("InvoiceAmount", "GrandTotal", "Total")
                  if c in h.columns), None)
            if rev_col:
                h["__rev"] = pd.to_numeric(
                    h[rev_col], errors="coerce").fillna(0)
                # Status filter — exclude voided/credited
                if "Status" in h.columns:
                    h = h[~h["Status"].astype(str).str.upper()
                          .isin(["VOIDED", "CREDITED",
                                 "CANCELLED", "CANCELED"])]
                h = h[(h["InvoiceDate"] >= start)
                       & (h["InvoiceDate"] <= end + pd.Timedelta(days=1))]
                rev_total = float(h["__rev"].sum())
                if group_by != "none" and not h.empty:
                    if group_by == "day":
                        h["__bkt"] = h["InvoiceDate"].dt.strftime("%Y-%m-%d")
                    elif group_by == "week":
                        h["__bkt"] = (
                            h["InvoiceDate"].dt.to_period("W")
                            .apply(lambda p: f"{p.start_time:%Y-%m-%d}"))
                    elif group_by == "month":
                        h["__bkt"] = h["InvoiceDate"].dt.strftime("%Y-%m")
                    grouped = h.groupby("__bkt")["__rev"].sum().to_dict()
                    rev_by_bucket = {k: round(v, 2)
                                       for k, v in grouped.items()}

    # Lines (units, orders)
    units = 0.0
    orders = 0
    sl = sale_lines_df.copy() if sale_lines_df is not None else pd.DataFrame()
    if not sl.empty and "InvoiceDate" in sl.columns:
        sl["InvoiceDate"] = pd.to_datetime(
            sl["InvoiceDate"], errors="coerce")
        sl = sl.dropna(subset=["InvoiceDate"])
        if "Status" in sl.columns:
            sl = sl[~sl["Status"].astype(str).str.upper()
                     .isin(["VOIDED", "CREDITED",
                            "CANCELLED", "CANCELED"])]
        sl = sl[(sl["InvoiceDate"] >= start)
                  & (sl["InvoiceDate"] <= end + pd.Timedelta(days=1))]
        if "Quantity" in sl.columns:
            units = float(pd.to_numeric(
                sl["Quantity"], errors="coerce").sum())
        if "SaleID" in sl.columns:
            orders = int(sl["SaleID"].nunique())

    return {
        "period": period,
        "start": start.strftime("%Y-%m-%d"),
        "end": end.strftime("%Y-%m-%d"),
        "revenue": round(rev_total, 2),
        "units": round(units, 2),
        "orders": orders,
        "group_by": group_by,
        "buckets": rev_by_bucket,
        "revenue_source": ("headers (includes shipping + tax)"
                            if rev_total > 0 else "no header data"),
    }


# Module-level holder for the headers DataFrame. The Streamlit page
# populates this once per session via set_sales_full_headers() so
# every tool call sees the same headers without repeatedly loading.
_SALES_FULL_HOLDER: dict = {"df": None}
_PURCHASE_LINES_HOLDER: dict = {"df": None}
# v2.67.51 — additional holders for the new transaction-lookup tools.
# Purchase headers carry InvoiceAmount, RequiredBy, Status, etc. that
# aren't on the line rows. Sale lines (longest window) drive
# get_sale_order's line listing. Stock-adjustment headers feed
# get_stock_adjustment.
_PURCHASE_HEADERS_HOLDER: dict = {"df": None}
_SALE_LINES_LONGEST_HOLDER: dict = {"df": None}
_STOCK_ADJUSTMENTS_HOLDER: dict = {"df": None}
# v2.67.54 — ShipStation shipments holder.
_SHIPMENTS_HOLDER: dict = {"df": None}
# v2.67.55 — Shopify orders holder for conversion-attribution
# lookups.
_SHOPIFY_ORDERS_HOLDER: dict = {"df": None}


def set_sales_full_headers(headers_df: pd.DataFrame) -> None:
    """Called by the Streamlit page on AI Assistant page load. Stores
    the merged sales-headers DataFrame so get_sales_totals can read
    it without recomputing per-tool-call."""
    _SALES_FULL_HOLDER["df"] = headers_df


def set_purchase_lines(purchase_lines_df: pd.DataFrame) -> None:
    """Called by the Streamlit AI Assistant page on load. Stashes the
    purchase-lines DataFrame (longest available window post-v2.67.51)
    so get_incoming_stock + get_purchase_order can scan POs without
    re-loading the CSV per tool call."""
    _PURCHASE_LINES_HOLDER["df"] = purchase_lines_df


def set_purchase_headers(purchase_headers_df: pd.DataFrame) -> None:
    """v2.67.51 — stash purchase headers for get_purchase_order so it
    can return supplier-level metadata (InvoiceAmount, BaseCurrency,
    SupplierCurrency, RequiredBy) that the line CSV doesn't carry."""
    _PURCHASE_HEADERS_HOLDER["df"] = purchase_headers_df


def set_sale_lines_longest(sale_lines_df: pd.DataFrame) -> None:
    """v2.67.51 — stash the merged longest-window sale-lines DataFrame
    so get_sale_order can return full line detail per sale without
    each tool call having to merge windows itself."""
    _SALE_LINES_LONGEST_HOLDER["df"] = sale_lines_df


def set_stock_adjustments(stock_adjustments_df: pd.DataFrame) -> None:
    """v2.67.51 — stash stock-adjustment headers for
    get_stock_adjustment. CIN7's adjustment endpoint only returns
    headers in the bulk pull; per-line detail isn't synced today."""
    _STOCK_ADJUSTMENTS_HOLDER["df"] = stock_adjustments_df


def set_shipments(shipments_df: pd.DataFrame) -> None:
    """v2.67.54 — stash the merged ShipStation shipments DataFrame
    so get_shipping_details can scan without re-loading the CSV per
    tool call."""
    _SHIPMENTS_HOLDER["df"] = shipments_df


def set_shopify_orders(shopify_orders_df: pd.DataFrame) -> None:
    """v2.67.55 — stash the Shopify orders DataFrame so
    get_shopify_order (and any future Shopify-side tool) can read it
    without re-loading."""
    _SHOPIFY_ORDERS_HOLDER["df"] = shopify_orders_df


def _signal_row_to_dict(row) -> dict:
    """Make a demand_signals row JSON-friendly for tool returns."""
    d = dict(row)
    return {
        "id": d.get("id"),
        "source": d.get("source"),
        "sku": d.get("sku"),
        "product_family": d.get("product_family"),
        "signal_type": d.get("signal_type"),
        "quantity": d.get("quantity"),
        "customer_name": d.get("customer_name"),
        "salesperson": d.get("salesperson"),
        "raw_text": d.get("raw_text"),
        "note": d.get("note"),
        "outcome": d.get("outcome"),
        "confidence": d.get("confidence"),
        "created_at": d.get("created_at"),
        "created_by": d.get("created_by"),
    }


def get_recent_signals(engine_df: pd.DataFrame,
                        sale_lines_df: pd.DataFrame,
                        args: dict) -> dict:
    days = max(1, min(int(args.get("days", 30) or 30), 365))
    sku = (args.get("sku") or "").strip() or None
    family = (args.get("product_family") or "").strip().upper() or None
    sig_type = (args.get("signal_type") or "").strip().lower()
    if sig_type in ("any", ""):
        sig_type = None
    source = (args.get("source") or "").strip().lower()
    if source in ("any", ""):
        source = None
    limit = max(1, min(int(args.get("limit", 25) or 25), 50))

    since_dt = (pd.Timestamp.now() - pd.Timedelta(days=days)).strftime(
        "%Y-%m-%d")
    rows = db.list_demand_signals(
        sku=sku,
        product_family=family,
        signal_type=sig_type,
        source=source,
        since=since_dt,
        limit=limit,
    )
    return {
        "matched": len(rows),
        "window_days": days,
        "filters_applied": {
            "sku": sku,
            "product_family": family,
            "signal_type": sig_type,
            "source": source,
        },
        "results": [_signal_row_to_dict(r) for r in rows],
    }


def get_top_inquired_products(engine_df: pd.DataFrame,
                                sale_lines_df: pd.DataFrame,
                                args: dict) -> dict:
    days = max(1, min(int(args.get("days", 30) or 30), 365))
    sig_type = (args.get("signal_type") or "").strip().lower() or None
    if sig_type in ("any", ""):
        sig_type = None
    limit = max(1, min(int(args.get("limit", 15) or 15), 50))

    since_dt = (pd.Timestamp.now() - pd.Timedelta(days=days)).strftime(
        "%Y-%m-%d")
    by_sku = db.count_demand_signals_by_sku(
        since=since_dt, signal_type=sig_type)
    if not by_sku:
        return {
            "matched": 0,
            "window_days": days,
            "results": [],
            "note": ("No signals in the window. Either nothing has "
                      "been logged or the period is too narrow."),
        }
    # Decorate with name + on-hand from engine for readability
    name_lookup: dict = {}
    onhand_lookup: dict = {}
    if not engine_df.empty and "SKU" in engine_df.columns:
        for r in engine_df.to_dict(orient="records"):
            sku_v = str(r.get("SKU"))
            if "Name" in r:
                name_lookup[sku_v] = str(r.get("Name") or "")[:100]
            if "OnHand" in r:
                onhand_lookup[sku_v] = r.get("OnHand")
    ranked = sorted(by_sku.items(), key=lambda x: -x[1])[:limit]
    return {
        "matched": len(ranked),
        "window_days": days,
        "signal_type_filter": sig_type,
        "results": [{
            "sku": s,
            "name": name_lookup.get(s, ""),
            "signal_count": n,
            "on_hand": onhand_lookup.get(s),
        } for s, n in ranked],
    }


def get_rising_demand(engine_df: pd.DataFrame,
                       sale_lines_df: pd.DataFrame,
                       args: dict) -> dict:
    recent_days = max(1, min(int(args.get("recent_days", 7) or 7), 90))
    min_recent = max(1, int(args.get("min_recent", 2) or 2))
    limit = max(1, min(int(args.get("limit", 15) or 15), 30))

    now = pd.Timestamp.now()
    recent_since = (now - pd.Timedelta(days=recent_days)).strftime(
        "%Y-%m-%d")
    prior_since = (now - pd.Timedelta(days=2 * recent_days)).strftime(
        "%Y-%m-%d")
    prior_until = recent_since   # exclusive of recent window

    # Recent counts via the helper
    recent = db.count_demand_signals_by_sku(since=recent_since)
    # Prior window — fetch all signals in [prior_since, recent_since)
    # via list_demand_signals, group manually
    rows = db.list_demand_signals(since=prior_since, limit=10000)
    prior: dict = {}
    for r in rows:
        d = dict(r)
        if d.get("created_at", "") >= recent_since:
            continue   # in the recent window, not prior
        sku_v = d.get("sku")
        if not sku_v:
            continue
        prior[sku_v] = prior.get(sku_v, 0) + 1

    # Compute deltas
    rows_out = []
    for sku_v, n_recent in recent.items():
        if n_recent < min_recent:
            continue
        n_prior = prior.get(sku_v, 0)
        delta = n_recent - n_prior
        ratio = (n_recent / n_prior) if n_prior > 0 else None
        rows_out.append({
            "sku": sku_v,
            "recent_count": n_recent,
            "prior_count": n_prior,
            "delta": delta,
            "ratio": (round(ratio, 2) if ratio is not None else None),
        })
    rows_out.sort(key=lambda r: (-r["delta"], -r["recent_count"]))
    rows_out = rows_out[:limit]

    # Decorate with names
    name_lookup: dict = {}
    if not engine_df.empty and "SKU" in engine_df.columns:
        for r in engine_df.to_dict(orient="records"):
            name_lookup[str(r.get("SKU"))] = str(r.get("Name") or "")[:100]
    for r in rows_out:
        r["name"] = name_lookup.get(r["sku"], "")

    return {
        "matched": len(rows_out),
        "recent_window_days": recent_days,
        "prior_window_days": recent_days,
        "min_recent_threshold": min_recent,
        "results": rows_out,
        "note": (
            "delta = recent_count - prior_count. ratio = recent/prior. "
            "ratio is null when prior_count was 0 (totally new "
            "interest)."
        ),
    }


def get_demand_score(engine_df: pd.DataFrame,
                       sale_lines_df: pd.DataFrame,
                       args: dict) -> dict:
    """Compute the 0-100 demand score for a single SKU. Wraps
    db.compute_demand_score and adds Claude-friendly explanation
    text via demand_scoring.explain_score()."""
    import demand_scoring
    sku = (args.get("sku") or "").strip()
    if not sku:
        return {"error": "sku is required"}
    window = max(1, min(int(args.get("window_days", 30) or 30), 365))
    score_dict = db.compute_demand_score(sku, window_days=window)
    score_dict["sku"] = sku
    score_dict["explanation"] = demand_scoring.explain_score(
        score_dict)
    return score_dict


def search_knowledge_base(engine_df: pd.DataFrame,
                            sale_lines_df: pd.DataFrame,
                            args: dict) -> dict:
    """Searches the on-disk knowledge base (markdown docs in docs/ +
    a curated set of top-level .md files). Returns top paragraphs.
    NOTE: we accept engine_df/sale_lines_df even though we don't use
    them, so the tool dispatch signature stays uniform."""
    import ai_kb
    query = (args.get("query") or "").strip()
    if not query:
        return {"error": "query is required"}
    max_results = min(int(args.get("max_results", 5) or 5), 10)
    results = ai_kb.search_knowledge_base(query, max_results=max_results)
    if not results:
        return {
            "matched": 0,
            "results": [],
            "note": (
                "No paragraphs in the knowledge base matched this query. "
                "Tell the user the documentation needs to be added or "
                "expanded — do NOT guess the answer."
            ),
        }
    return {
        "matched": len(results),
        "results": [{
            "source": p.source,
            "title": p.title,
            "lines": f"{p.start_line}-{p.end_line}",
            "score": p.score,
            "text": p.text[:1500],   # cap so a giant paragraph
                                       # doesn't blow the context.
        } for p in results],
    }


# ---------------------------------------------------------------------------
# Dispatch table — maps tool name (from Claude) to implementation.
# ---------------------------------------------------------------------------
def search_products_by_text(engine_df: pd.DataFrame,
                             sale_lines_df: pd.DataFrame,
                             args: dict) -> dict:
    """v2.64 — text-search rule executor.
    v2.65.1 — tokenized AND match, plus exclude_types blocklist.

    Driven by an alias rule with rule_type='text_search'. The user
    typed a phrase like 'warm white' that's been mapped to a search
    across product fields (title / description / tags / product_type /
    collections). This tool runs the contains-match across whichever
    of those fields exist in the products DataFrame.

    Tokenization (v2.65.1): the query is split on whitespace and
    EVERY token must appear (substring) in at least one of the
    searched fields for the row to match. So 'warm white led strip'
    becomes 4 tokens, all required, but they can appear in different
    fields (warm + white in title, led + strip in description). This
    is far less brittle than the old single-substring match which
    only hit rows containing the phrase end-to-end.

    Exclusions (v2.65.1): exclude_types is a list of forbidden
    keywords; any row whose Name or Type column contains any of
    them (case-insensitive substring) is filtered out. Used to keep
    'LED strip' queries from returning controllers/dimmers/etc.

    Combinable with classification + in_stock_only filters so a
    question like 'show me warm white LED strips that are slow movers'
    resolves to a single tool call.
    """
    query = (args.get("query") or "").strip().lower()
    if not query:
        return {"error": "query is required"}
    # Tokenize on whitespace — empty tokens dropped.
    query_tokens = [t for t in query.split() if t]

    # Map our canonical field names to whichever columns happen to be
    # in the products / engine DataFrame today. Some live in CIN7
    # masters (Name, Type), some only after shopify_sync has merged
    # (Description, Tags, Collections, ProductType). Fields the user
    # asks for that aren't present in the DF get reported as missing
    # — we don't pretend to have searched them.
    field_aliases = {
        "title":         ["Name"],
        "name":          ["Name"],
        "description":   ["Description", "Body", "Body_html"],
        "tags":          ["Tags", "Tags_csv"],
        "product_type":  ["ProductType", "Type"],
        "type":          ["ProductType", "Type"],
        "collections":   ["Collections", "Categories", "Category"],
        "category":      ["Collections", "Categories", "Category"],
        "family":        ["Family", "AdditionalAttribute1",
                          "ProductFamily"],
    }

    requested = args.get("fields") or ["title"]
    if isinstance(requested, str):
        requested = [requested]
    requested = [str(f).strip().lower() for f in requested if f]
    if not requested:
        requested = ["title"]

    # v2.67.27 — read engine_df as-is. The synthesised Classification
    # column from v2.67.22 was buggy AND duplicated work the engine
    # already does. The AI now reads is_dormant, excess_units,
    # effective_units_12mo, and trend_flag directly to determine
    # slow/dead/excess intent — these are the authoritative signals.
    df = engine_df.copy()
    searched_cols: list = []
    missing_fields: list = []
    actual_columns: list = []   # the real DataFrame columns we'll
                                  # search; stays parallel with searched_cols.
    for f in requested:
        candidate_cols = field_aliases.get(f, [f.capitalize()])
        col_used = None
        for c in candidate_cols:
            if c in df.columns:
                col_used = c
                break
        if col_used is None:
            missing_fields.append(f)
            continue
        actual_columns.append(col_used)
        searched_cols.append({"requested": f, "actual_column": col_used})

    if not actual_columns:
        return {
            "error": (f"None of the requested fields exist in the "
                       f"product data right now: {requested}. "
                       f"Available columns: {list(df.columns)[:20]}"),
            "missing_fields": missing_fields,
            "available_columns": list(df.columns),
        }

    # v2.65.1 tokenized AND-match. For each token: OR across fields
    # (any field can contain it). Then AND across tokens (every token
    # must hit somewhere). 'warm white led strip' becomes 4 tokens
    # that together require a row that mentions all four somewhere
    # in the searched columns.
    combined = None
    for tok in query_tokens:
        tok_mask = None
        for col in actual_columns:
            colvals = df[col].fillna("").astype(str).str.lower()
            m = colvals.str.contains(tok, na=False, regex=False)
            tok_mask = m if tok_mask is None else (tok_mask | m)
        if tok_mask is None:
            continue
        combined = tok_mask if combined is None else (combined & tok_mask)
    if combined is not None:
        df = df[combined]

    # v2.65.1 any_of_terms: OR-match. At least ONE of these substrings
    # must appear in the searched fields for the row to qualify. This
    # is how the alias rule for 'warm white' encodes Kelvin
    # alternatives: any_of_terms=['warm white', '2200K', '2400K',
    # '2700K', '2800K', '3000K'] — any one hit is enough. Composes
    # with the AND-match above: a row must satisfy ALL tokens in
    # `query` AND at least one term in `any_of_terms`.
    any_of_terms = args.get("any_of_terms") or []
    if isinstance(any_of_terms, str):
        any_of_terms = [any_of_terms]
    any_of_terms = [str(t).strip().lower() for t in any_of_terms if t]
    if any_of_terms and actual_columns:
        any_mask = None
        for term in any_of_terms:
            for col in actual_columns:
                colvals = df[col].fillna("").astype(str).str.lower()
                m = colvals.str.contains(term, na=False, regex=False)
                any_mask = m if any_mask is None else (any_mask | m)
        if any_mask is not None:
            df = df[any_mask]

    # v2.65.1 exclude_types: drop rows whose Name or Type contains any
    # of these keywords. Used to keep 'LED strip' searches from
    # surfacing controllers/dimmers/power supplies/etc.
    exclude_types = args.get("exclude_types") or []
    if isinstance(exclude_types, str):
        exclude_types = [exclude_types]
    exclude_types = [str(e).strip().lower() for e in exclude_types if e]
    # v2.67.28 — for strip queries, UNION the caller's exclude_types
    # with the strip-accessory defaults from product_search.py so
    # connectors / drivers / dimmers / channels / etc. don't eat
    # limit slots and crowd out actual strip families. Mirrors
    # find_products' behaviour (which already auto-adds these). The
    # symptom was: ai assistant called search_products_by_text with
    # limit=50, accessories ate ~25 slots, and major families like
    # White Iris / White Lily fell off the bottom of the result.
    #
    # v2.67.34 — same treatment for profile/channel/extrusion
    # queries. "What slow-moving profiles do we have" was returning
    # mounting brackets and fixing kits because those rows contain
    # "profile" in their descriptions; the user wants the profile
    # extrusions themselves. Strip and profile excludes are
    # mutually exclusive (a query is one or the other), so we pick
    # which list to apply based on which keyword fires.
    _qlower = query.lower() if isinstance(query, str) else ""
    if "strip" in _qlower:
        from product_search import _DEFAULT_EXCLUDES_FOR_STRIPS
        _existing_lower = set(exclude_types)
        for _default in _DEFAULT_EXCLUDES_FOR_STRIPS:
            if _default.lower() not in _existing_lower:
                exclude_types.append(_default.lower())
                _existing_lower.add(_default.lower())
    elif any(_kw in _qlower for _kw in
              ("profile", "channel", "extrusion")):
        from product_search import _DEFAULT_EXCLUDES_FOR_PROFILES
        _existing_lower = set(exclude_types)
        for _default in _DEFAULT_EXCLUDES_FOR_PROFILES:
            if _default.lower() not in _existing_lower:
                exclude_types.append(_default.lower())
                _existing_lower.add(_default.lower())
    excluded_count = 0
    if exclude_types:
        ex_mask = pd.Series(False, index=df.index)
        for excl_col in ("Type", "Name"):
            if excl_col in df.columns:
                col_lower = (df[excl_col].fillna("").astype(str)
                              .str.lower())
                for kw in exclude_types:
                    ex_mask = ex_mask | col_lower.str.contains(
                        kw, na=False, regex=False)
        excluded_count = int(ex_mask.sum())
        df = df[~ex_mask]

    # Optional secondary filters Claude can stack on top.
    # v2.67.27 — `classification` arg now maps to engine columns
    # (no synthesised Classification column). Mappings:
    #   'slow'     → is_dormant=True  (dormant = 90d activity dropped
    #                                  vs 12mo baseline; the engine's
    #                                  authoritative slow-mover flag)
    #   'dead'     → OnHand>0 AND effective_units_12mo==0
    #                                 (RULES.md §4.3 — holding stock
    #                                 with zero 12mo demand)
    #   'excess'   → excess_units>0   (RULES.md §4.1 — over-target)
    #   'active'   → none of the above
    #   'watchlist' → reserved; same as 'slow' until we wire it
    classification = (args.get("classification") or "any").strip().lower()
    if classification == "slow" and "is_dormant" in df.columns:
        _onh = df["OnHand"].fillna(0) if "OnHand" in df.columns else 0
        df = df[df["is_dormant"].fillna(False) & (_onh > 0)]
    elif classification == "dead" and "effective_units_12mo" in df.columns:
        _onh = df["OnHand"].fillna(0) if "OnHand" in df.columns else 0
        df = df[(_onh > 0)
                & (df["effective_units_12mo"].fillna(0) == 0)]
    elif classification == "excess" and "excess_units" in df.columns:
        df = df[df["excess_units"].fillna(0) > 0]
    elif classification == "active" and "is_dormant" in df.columns:
        _onh = df["OnHand"].fillna(0) if "OnHand" in df.columns else 0
        _eff = (df["effective_units_12mo"].fillna(0)
                 if "effective_units_12mo" in df.columns else 0)
        _exc = (df["excess_units"].fillna(0)
                 if "excess_units" in df.columns else 0)
        df = df[~df["is_dormant"].fillna(False)
                & ~((_onh > 0) & (_eff == 0))
                & (_exc == 0)]
    elif classification == "watchlist" and "is_dormant" in df.columns:
        # Treat watchlist same as slow until the column ships.
        _onh = df["OnHand"].fillna(0) if "OnHand" in df.columns else 0
        df = df[df["is_dormant"].fillna(False) & (_onh > 0)]
    in_stock_only = bool(args.get("in_stock_only", False))
    if in_stock_only and "OnHand" in df.columns:
        df = df[df["OnHand"].fillna(0) > 0]
    family = (args.get("family") or "").strip().upper()
    if family and "Family" in df.columns:
        df = df[df["Family"].astype(str).str.upper() == family]

    # v2.67.29 — default flipped back to True. v2.67.23 set it to
    # False to fix find_products' Shopify scoring loop, but
    # find_products now explicitly passes parents_only=False to
    # search_products_by_text (handling the filter at emission
    # time). Direct callers of search_products_by_text — including
    # the AI assistant — should get parents-only by default so
    # child SKUs (per-foot cuts, BOM derivatives) are hidden
    # automatically. The AI was inconsistently passing this arg,
    # so making it the default eliminates that as a source of
    # failure. Pass parents_only=false explicitly when you actually
    # want every variant.
    parents_only = bool(args.get("parents_only", True))
    if parents_only and "is_non_master_tube" in df.columns:
        df = df[~df["is_non_master_tube"].fillna(False)]

    # v2.67.11 — internal cap raised 500 → 2000 after observing that
    # even with 500 (per v2.67.5) and find_products passing limit=200
    # (since bumped to 1000), LEDIRIS-* and LED-WL-* warm-white SKUs
    # were still being silently truncated. CIN7 has thousands of
    # warm-white "led strip" matches; LED-31.* and LED-DECOR-*
    # variants alone consumed the first ~200-300 of them in CSV
    # order, leaving everything alphabetically later off the bottom.
    # Bumping to 2000 lets find_products pull the full warm-white
    # candidate pool into cin7_matched_skus so per-Shopify-hit
    # variant matching works for every family, not just the
    # alphabetically-early ones. The schema description for direct
    # LLM callers still says "cap 50, default 25" so Claude doesn't
    # over-fetch on simple SKU lookups; the higher cap only matters
    # when find_products explicitly asks for it.
    limit = min(int(args.get("limit", 25) or 25), 2000)
    # v2.67.22 — include the columns that feed derived Classification
    # (is_dormant, effective_units_12mo, excess_units, OnHand) so the
    # post-process below can compute slow/dead/active flags even when
    # engine_df doesn't have a literal `Classification` column.
    # v2.67.23 — also include `trend_flag` (Stable / 📈 Trend / 🎯
    # Project / 🔀 Mixed / 📉 Decline). The Ordering page uses this
    # as a sales-staff rating: a 🎯 Project SKU has concentrated
    # demand from 1-2 buyers, a 📉 Decline SKU has falling momentum.
    # Sales staff want this rating inline so they can prioritise
    # selling slow movers / declining stock.
    cols_we_want = [c for c in [
        "SKU", "Name", "Family", "ABC", "Classification",
        "OnHand", "Bin", "TargetStock", "ReorderSuggested",
        "is_dormant", "effective_units_12mo", "excess_units",
        "is_non_master_tube", "trend_flag",
    ] if c in df.columns]
    df = df.head(limit)[cols_we_want] if cols_we_want else df.head(limit)
    rows = [_serialise_row(r) for r in df.to_dict(orient="records")]
    return {
        "matched": len(rows),
        "results": rows,
        "searched": searched_cols,
        "tokens": query_tokens,
        "missing_fields": missing_fields,
        "excluded_count": excluded_count,
        "exclude_types_applied": exclude_types,
        "parents_only_applied": parents_only,
        "note": (
            f"Showing first {limit} of potentially many. Refine the "
            "query (or add more filters) if you need a narrower set."
            if len(rows) == limit else None),
    }


def find_similar_products(engine_df: pd.DataFrame,
                           sale_lines_df: pd.DataFrame,
                           args: dict) -> dict:
    """v2.64 — conservative similarity search.

    Tube-only ranking for now (the only category with a reliable
    naming convention right now: family code with trailing digits =
    nominal diameter in mm, e.g. SIERRA38 → 38mm).

    Resolution order:
      1. If sku given → look up its family from engine_df.
      2. If family given → use that.
      3. Parse trailing digits from family code as nominal diameter.

    Ranking (per spec — accuracy > speed, so weak matches are
    deliberately suppressed):
      - Same diameter (parsed from family code)         + strong match
      - Same product_type / Type if column exists       + bonus
      - Stock availability (OnHand > 0)                 + bonus
      - Material similarity (best-effort — only if
        the products DF has a Material column)          + bonus
    Other-diameter families are NOT returned by default — they're
    'maybe similar' at best and the spec says fewer accurate >
    many weak.

    If no trailing digits in the family code, returns
    {"diameter": "unknown"} and an empty list rather than guessing.
    Fallback to title/description regex (1.50&quot;, 38mm) is captured
    in the result with confidence='lower' when used.
    """
    import re
    sku = (args.get("sku") or "").strip()
    family = (args.get("family") or "").strip().upper()
    limit = min(int(args.get("limit", 8) or 8), 20)
    include_original_family = bool(
        args.get("include_original_family", False))

    if engine_df is None or engine_df.empty:
        return {"error": "engine_df is empty — products not loaded"}

    # Resolve family
    if not family and sku:
        _row = engine_df[engine_df["SKU"].astype(str) == sku]
        if not _row.empty and "Family" in _row.columns:
            family = str(_row.iloc[0].get("Family") or "").strip().upper()

    if not family:
        return {
            "error": ("Could not resolve a product family. Pass "
                      "either sku= or family=. For tubes the family "
                      "is the part code without the variant suffix "
                      "(e.g. SIERRA38, SMOKIES38).")
        }

    # Diameter from trailing digits — primary signal.
    _m = re.search(r"(\d{2,3})$", family)
    nominal_diameter = int(_m.group(1)) if _m else None
    diameter_source = "family_code" if nominal_diameter else "unknown"

    # Fallback: try to parse a diameter from the family-name title.
    # Lower confidence — used only when family code didn't yield one.
    fallback_used = False
    if nominal_diameter is None and "Name" in engine_df.columns:
        sample_row = engine_df[engine_df["Family"].astype(str).str.upper()
                                 == family]
        if not sample_row.empty:
            _name = str(sample_row.iloc[0].get("Name") or "")
            # 38mm / 38 mm
            m_mm = re.search(r"(\d{2,3})\s*mm", _name, re.IGNORECASE)
            if m_mm:
                nominal_diameter = int(m_mm.group(1))
                diameter_source = "title_mm"
                fallback_used = True
            else:
                # 1.5" / 1-1/2" — convert inches to mm (rough)
                m_inch = re.search(
                    r"(\d+(?:\.\d+)?)\s*[\"”]", _name)
                if m_inch:
                    inches = float(m_inch.group(1))
                    nominal_diameter = int(round(inches * 25.4))
                    diameter_source = "title_inch"
                    fallback_used = True

    if nominal_diameter is None:
        return {
            "subject_family": family,
            "diameter": "unknown",
            "alternatives": [],
            "note": ("Could not determine a diameter for this family "
                     "(no trailing digits in the family code, no "
                     "explicit mm/inch in the product name). Returning "
                     "no alternatives rather than guessing — per the "
                     "'accuracy > speed' rule."),
        }

    # Find candidate families with the same trailing diameter.
    if "Family" not in engine_df.columns:
        return {"error": "engine_df has no 'Family' column to compare"}

    fam_series = engine_df["Family"].fillna("").astype(str).str.upper()
    diameter_re = re.compile(rf"(\d{{2,3}})$")
    same_diameter_families: list = []
    for f in fam_series.unique():
        if not f or f == family:
            continue
        m = diameter_re.search(f)
        if m and int(m.group(1)) == nominal_diameter:
            same_diameter_families.append(f)

    # Build ranked alternatives. For each family, pick a representative
    # SKU (prefer one with stock; otherwise just the first).
    alternatives = []
    for f in same_diameter_families:
        rows = engine_df[fam_series == f]
        if rows.empty:
            continue
        rep = None
        if "OnHand" in rows.columns:
            in_stock = rows[rows["OnHand"].fillna(0) > 0]
            if not in_stock.empty:
                rep = in_stock.iloc[0]
        if rep is None:
            rep = rows.iloc[0]
        rep_dict = _serialise_row(dict(rep))
        why_parts = [f"same nominal diameter ({nominal_diameter}mm)"]
        differences = []
        # Material similarity — only if a material column exists.
        material_col = next(
            (c for c in ("Material", "Substrate") if c in rows.columns),
            None)
        if material_col:
            subject_rows = engine_df[fam_series == family]
            if not subject_rows.empty:
                _subj_mat = str(subject_rows.iloc[0]
                                  .get(material_col) or "").strip()
                _alt_mat = str(rep.get(material_col) or "").strip()
                if _subj_mat and _alt_mat and _subj_mat != _alt_mat:
                    differences.append(
                        f"different material ({_subj_mat} vs "
                        f"{_alt_mat})")
                elif _subj_mat and _alt_mat:
                    why_parts.append(f"same material ({_alt_mat})")
        # Stock note
        on_hand = rep_dict.get("OnHand")
        stock_note = (f"in stock ({on_hand})"
                      if on_hand and float(on_hand) > 0
                      else "out of stock")
        alternatives.append({
            "family": f,
            "representative_sku": rep_dict.get("SKU"),
            "name": rep_dict.get("Name"),
            "on_hand": on_hand,
            "classification": rep_dict.get("Classification"),
            "why_similar": "; ".join(why_parts),
            "differences": "; ".join(differences) or None,
            "stock_note": stock_note,
        })

    # Conservative ranking: in-stock first, then by family code.
    alternatives.sort(
        key=lambda a: (
            0 if (a["on_hand"] and float(a["on_hand"]) > 0) else 1,
            a["family"],
        ))
    alternatives = alternatives[:limit]

    result = {
        "subject_family": family,
        "diameter": nominal_diameter,
        "diameter_source": diameter_source,
        "diameter_confidence": (
            "lower (parsed from product name, not family code)"
            if fallback_used else "high (parsed from family code)"),
        "alternatives": alternatives,
        "note": (
            "Conservative result — only families with the SAME "
            "trailing-digit nominal diameter are listed. Other "
            "diameters are NOT returned automatically; ask "
            "specifically if you want them."
            + (" Diameter inferred from product name; treat with "
               "caution." if fallback_used else "")),
    }
    if include_original_family:
        result["subject_family_reference"] = {
            "family": family,
            "note": "Listed as reference; not an alternative.",
        }
    return result


# v2.67.127 — per-foot child SKU suffixes (from worker_engine.py's
# is_non_master_tube heuristic). When a user asks about a child
# SKU's POs, we need to look up the parent's POs instead because
# CIN7 stores purchase lines against the master roll, not the
# per-foot cut variants.
_PER_FOOT_SUFFIXES = ("0305", "0610", "0915", "1220", "1525",
                          "1830", "2135", "2440", "2745", "3050")
_MASTER_SUFFIX_CANDIDATES = ("100M", "50M", "5M", "10M", "25M",
                                  "MASTER")


def _find_parent_sku(child_sku: str,
                          engine_df: pd.DataFrame
                          ) -> Optional[str]:
    """v2.67.128 — Return the parent (master roll) SKU for a child.

    PRIMARY source: CIN7 BOM via bom_lookup.parent_sku(). Every per-
    foot cut is an Assembly built from one Component (the master
    roll); CIN7 stores this explicitly. cin7_sync writes nightly
    BOM CSVs into DATA_DIR; bom_lookup caches them in-process.

    FALLBACK: if the BOM hasn't been synced yet (fresh DB) or
    contains no entry for this SKU, fall back to the v2.67.127
    suffix heuristic — strip per-foot length (-0305, -0610, ...)
    and substitute master-roll suffix (-100M, -50M, ...). Brittle
    but better than nothing while waiting for the BOM sync."""
    if not child_sku:
        return None

    # 1. Try the BOM lookup (canonical source).
    try:
        from bom_lookup import parent_sku as _bom_parent
        bom_parent = _bom_parent(child_sku)
        if bom_parent:
            return bom_parent
    except Exception:  # noqa: BLE001
        # bom_lookup module missing or BOM CSV unreadable — fall
        # through to the heuristic rather than failing outright.
        pass

    # 2. Heuristic fallback (kept for resilience when BOM is stale
    #    / missing). Recognises the standard per-foot suffix
    #    pattern even when the BOM doesn't know about the SKU yet.
    s = child_sku.upper()
    suffix_idx = -1
    matched_suffix = None
    for suff in _PER_FOOT_SUFFIXES:
        if s.endswith(f"-{suff}"):
            suffix_idx = len(s) - len(suff) - 1
            matched_suffix = suff
            break
    if not matched_suffix:
        return None
    base = s[:suffix_idx]
    if engine_df is None or engine_df.empty or "SKU" not in engine_df.columns:
        return None
    catalog = engine_df["SKU"].astype(str).str.upper()
    for master_suff in _MASTER_SUFFIX_CANDIDATES:
        candidate = f"{base}-{master_suff}"
        if (catalog == candidate).any():
            return candidate
    if (catalog == base).any():
        return base
    return None


def get_incoming_stock(engine_df: pd.DataFrame,
                        sale_lines_df: pd.DataFrame,
                        args: dict) -> dict:
    """v2.64 — list open / incomplete CIN7 purchase order lines for a
    SKU or family. Powers questions like 'when's the next shipment of
    LED-XYZ?' and 'do we have any SIERRA38 incoming?'.

    Per spec, we only return OPEN POs:
      - Status NOT IN (DRAFT, RECEIVED, CLOSED, COMPLETED, CANCELLED,
        VOIDED, ORDERED-Received and the like)
      - Quantity > 0 (zero-qty lines suppressed)

    Expected delivery date — we use whichever of the standard CIN7
    fields exists in the schema today (`RequiredBy` is the canonical
    one in cin7_sync._extract_purchase_lines as of v2.64). Field name
    is reported in the output so the caller can audit.

    If no open lines match, returns matched=0 with a reason. If no
    expected date is recorded for an open line, the line is included
    with expected_date='not available'."""
    sku = (args.get("sku") or "").strip()
    family = (args.get("family") or "").strip().upper()
    limit = min(int(args.get("limit", 25) or 25), 50)

    purchase_lines = _PURCHASE_LINES_HOLDER.get("df")
    if purchase_lines is None or purchase_lines.empty:
        return {
            "error": ("Purchase lines not loaded for this session. "
                      "An admin needs to call "
                      "ai_tools.set_purchase_lines() once at AI "
                      "Assistant page boot."),
        }

    df = purchase_lines.copy()

    # Pick a date column from whichever of the candidates is present.
    date_col_candidates = (
        "RequiredBy", "ExpectedDate", "DeliveryDate",
        "RequiredDate", "DateRequired", "ETA")
    date_col = next(
        (c for c in date_col_candidates if c in df.columns), None)

    # Filter to OPEN POs. CIN7 statuses include AUTHORISED / ORDERED /
    # PARTIAL / RECEIVED / CLOSED / VOIDED. We exclude the closed /
    # cancelled / fully-received tail. Status containing 'Received'
    # (e.g. 'ORDERED-Received') is the synthetic stock-received row
    # written by _extract_purchase_lines — exclude that too.
    closed_keywords = ("RECEIVED", "CLOSED", "COMPLETED",
                        "CANCELLED", "VOIDED", "DRAFT")
    if "Status" in df.columns:
        status_u = df["Status"].fillna("").astype(str).str.upper()
        keep_mask = ~status_u.apply(
            lambda s: any(k in s for k in closed_keywords))
        df = df[keep_mask]

    # Suppress zero-qty lines.
    if "Quantity" in df.columns:
        df = df[pd.to_numeric(
            df["Quantity"], errors="coerce").fillna(0) > 0]

    # Match by SKU or family.
    if sku and "SKU" in df.columns:
        df = df[df["SKU"].astype(str).str.upper() == sku.upper()]
    elif family:
        # No Family column on purchase_lines (CIN7 doesn't set it on
        # the line). Fall back to substring against SKU prefix or Name.
        sku_match = (df["SKU"].astype(str).str.upper().str.startswith(
            family) if "SKU" in df.columns else False)
        name_match = (df["Name"].astype(str).str.upper().str.contains(
            family, na=False) if "Name" in df.columns else False)
        df = df[sku_match | name_match]

    if df.empty:
        # v2.67.127 — Per-foot child SKU fallback. If the user
        # asked about a per-foot cut (e.g. ...-0305) and no PO
        # exists in that exact SKU, CIN7 almost certainly tracks
        # the master roll instead (...-100M). Auto-look up the
        # parent and retry the open-PO query, surfacing the
        # parent-level POs with a note explaining the relationship.
        parent_result = None
        if sku:
            parent_sku = _find_parent_sku(sku, engine_df)
            if parent_sku and parent_sku.upper() != sku.upper():
                # Re-run the same query against the parent SKU.
                # Hand-rolled retry rather than recursion to avoid
                # surprise behaviour if parent itself also has no
                # POs.
                _pdf = purchase_lines.copy()
                if "Status" in _pdf.columns:
                    _status_u = (_pdf["Status"].fillna("")
                                  .astype(str).str.upper())
                    _keep = ~_status_u.apply(
                        lambda s: any(
                            k in s for k in closed_keywords))
                    _pdf = _pdf[_keep]
                if "Quantity" in _pdf.columns:
                    _pdf = _pdf[pd.to_numeric(
                        _pdf["Quantity"],
                        errors="coerce").fillna(0) > 0]
                if "SKU" in _pdf.columns:
                    _pdf = _pdf[
                        _pdf["SKU"].astype(str).str.upper()
                        == parent_sku.upper()]
                if not _pdf.empty:
                    parent_result = {
                        "parent_sku": parent_sku,
                        "child_sku_queried": sku,
                        "lines": [],
                    }
                    for _, _r in _pdf.head(limit).iterrows():
                        parent_result["lines"].append({
                            "sku": _r.get("SKU"),
                            "name": _r.get("Name"),
                            "quantity_on_order": _r.get("Quantity"),
                            "quantity_remaining":
                                _r.get("QuantityRemaining"),
                            "expected_date": (_r.get(date_col)
                                                if date_col
                                                else None),
                            "po_status": _r.get("Status"),
                            "po_id": _r.get("PurchaseID"),
                            "po_reference": _r.get("Reference"),
                        })
        if parent_result:
            return {
                "matched": len(parent_result["lines"]),
                "subject": sku,
                "child_sku_queried": parent_result["child_sku_queried"],
                "parent_sku": parent_result["parent_sku"],
                "is_parent_fallback": True,
                "date_field_used": date_col,
                "open_purchase_lines": parent_result["lines"],
                "note": (
                    f"No POs found for child SKU {sku}, but the "
                    f"master roll {parent_result['parent_sku']} has "
                    f"{len(parent_result['lines'])} open PO line(s). "
                    f"Per-foot variants are CUT FROM the master roll "
                    f"so incoming master-roll stock will become "
                    f"available as cuts of this child once received. "
                    f"Report these PO lines as the answer to the "
                    f"user's question, and explicitly mention that "
                    f"they're against the master roll, not the "
                    f"per-foot child."),
            }

        # v2.67.51 — cross-check engine_df.OnOrder before declaring
        # "nothing on order". The CIN7 stock-on-hand row carries an
        # OnOrder field that aggregates ALL outstanding PO lines for
        # the SKU regardless of when the PO was raised. The
        # purchase_lines DataFrame only carries lines from whatever
        # window we last synced (default 30d as of v2.67.51). Stale
        # sync windows produced false-negative answers ("no open POs
        # for LED-V3060001-2") even when OnOrder=190 said otherwise.
        # If the discrepancy shows up, surface it explicitly so the
        # AI tells the user "stock record says X on order but the
        # line detail isn't in our local sync window — admin should
        # run a wider sync".
        gap_hint = None
        try:
            if engine_df is not None and not engine_df.empty and sku:
                _eng = engine_df
                if "SKU" in _eng.columns:
                    _row = _eng[
                        _eng["SKU"].astype(str).str.upper()
                        == sku.upper()]
                    if not _row.empty and "OnOrder" in _row.columns:
                        _on_order = pd.to_numeric(
                            _row.iloc[0].get("OnOrder"),
                            errors="coerce")
                        if pd.notna(_on_order) and _on_order > 0:
                            gap_hint = (
                                f"DATA GAP: CIN7 stock record shows "
                                f"{int(_on_order)} units on order for "
                                f"{sku}, but no matching open PO line "
                                f"is in our local sync window. The PO "
                                f"may have been raised before the "
                                f"current purchase-lines window "
                                f"(daily sync = 30d). Tell the user "
                                f"to either look up the PO directly "
                                f"in CIN7 OR ask an admin to widen "
                                f"the sync window. Do not say 'no PO "
                                f"exists' — say 'the local sync "
                                f"doesn't cover the PO'.")
        except Exception:
            pass
        return {
            "matched": 0,
            "subject": sku or family,
            "date_field_used": date_col,
            "data_gap": gap_hint,
            "note": ("No open / incomplete purchase orders match. "
                      "Either the SKU has nothing on order, or all "
                      "matching POs are already received / closed / "
                      "cancelled. Per spec we don't return those."),
        }

    # v2.67.181 — build a SKU → OnHand map from engine_df once, so
    # we can enrich each line with current stock on the shelf.
    # Lets the AI answer "are these incoming on top of what we
    # already have?" without a second tool call.
    _on_hand_map: dict = {}
    if engine_df is not None and not engine_df.empty:
        if ("SKU" in engine_df.columns
                and "OnHand" in engine_df.columns):
            _on_hand_map = {
                str(k).upper(): float(v or 0)
                for k, v in engine_df.set_index(
                    "SKU")["OnHand"].dropna().items()}

    out_rows = []
    for _, r in df.head(limit).iterrows():
        # v2.67.181 — line dollar value (Quantity × Price OR the
        # CIN7 Total field if it carries the post-discount/tax
        # subtotal). Prefer Total when present and >0 (it's the
        # canonical line value).
        qty_n = pd.to_numeric(r.get("Quantity"), errors="coerce")
        price_n = pd.to_numeric(r.get("Price"), errors="coerce")
        total_n = pd.to_numeric(r.get("Total"), errors="coerce")
        if pd.notna(total_n) and total_n > 0:
            line_total = float(total_n)
        elif pd.notna(qty_n) and pd.notna(price_n):
            line_total = float(qty_n * price_n)
        else:
            line_total = None
        sku_u = str(r.get("SKU") or "").upper()
        on_hand_now = _on_hand_map.get(sku_u)
        rec = {
            "sku": r.get("SKU"),
            "name": r.get("Name"),
            "quantity_on_order": r.get("Quantity"),
            "quantity_remaining": (
                r.get("QuantityRemaining")
                if "QuantityRemaining" in df.columns
                else None),
            # v2.67.181 — per-line dollar value so the AI can
            # report "$3,702" rather than just unit counts.
            "unit_price": (float(price_n)
                              if pd.notna(price_n) else None),
            "line_total_value": line_total,
            "uom": (r.get("UOM")
                       if "UOM" in df.columns else None),
            # v2.67.181 — current OnHand for the same SKU. Helps
            # the AI compose "75 incoming + 49 already on shelf
            # = 124 total" style answers.
            "on_hand_now": on_hand_now,
            "expected_date": (
                str(r.get(date_col)) if (date_col
                                         and pd.notna(r.get(date_col)))
                else "not available"),
            # v2.67.181 — order_date so the AI can report
            # "Ordered Apr 29" alongside the ETA.
            "order_date": (
                str(r.get("OrderDate"))
                if "OrderDate" in df.columns
                and pd.notna(r.get("OrderDate"))
                else None),
            "supplier": r.get("Supplier"),
            "po_number": r.get("OrderNumber"),
            "status": r.get("Status"),
            # v2.67.44 — freight-signal fields. Buyer uses these
            # to log shipment mode (air/sea) in `comments` and
            # progress detail (e.g. "departed Shenzhen 2026-04-12,
            # in customs") in `shipping_notes`. Surface both so
            # AI shipment-status answers tell the staff what's
            # actually happening with the freight, not just the
            # required-by date.
            "comments": (
                str(r.get("Comments")).strip()
                if "Comments" in df.columns and pd.notna(r.get("Comments"))
                and str(r.get("Comments")).strip()
                else None),
            "shipping_notes": (
                str(r.get("ShippingNotes")).strip()
                if "ShippingNotes" in df.columns
                and pd.notna(r.get("ShippingNotes"))
                and str(r.get("ShippingNotes")).strip()
                else None),
            # v2.67.52 — surface every freeform PO text field. The
            # buyer types DIFFERENT things into Memo vs Note vs
            # Comments — surfacing only one of them hides what they
            # said. Memo is the big text box on the PO form
            # ('Purchase Order Memo'); Note is a separate top-level
            # note; Terms is payment terms.
            "memo": (
                str(r.get("Memo")).strip()
                if "Memo" in df.columns and pd.notna(r.get("Memo"))
                and str(r.get("Memo")).strip()
                else None),
            "note": (
                str(r.get("Note")).strip()
                if "Note" in df.columns and pd.notna(r.get("Note"))
                and str(r.get("Note")).strip()
                else None),
            "terms": (
                str(r.get("Terms")).strip()
                if "Terms" in df.columns and pd.notna(r.get("Terms"))
                and str(r.get("Terms")).strip()
                else None),
            # v2.67.55b — extra positional attributes (any
            # AdditionalAttributeN content beyond the canonical
            # Shipping Notes position).
            "attribute_notes": (
                str(r.get("AttributeNotes")).strip()
                if "AttributeNotes" in df.columns
                and pd.notna(r.get("AttributeNotes"))
                and str(r.get("AttributeNotes")).strip()
                else None),
        }
        out_rows.append(_serialise_row(rec))

    # v2.67.181 — supplier history. For each unique supplier in
    # the open POs, find the most recent FULLY-RECEIVED PO and
    # compute its lead time (received_date − order_date). Lets
    # the AI add an "ETA expectation" line like "previous Reeves
    # PO took 24 days; air freight on this one may be faster".
    suppliers_in_result = {
        str(r.get("supplier") or "").strip()
        for r in out_rows
        if r.get("supplier")}
    supplier_history: dict = {}
    if suppliers_in_result:
        try:
            pl_all = purchase_lines  # full window, not pre-filter
            for supplier_name in suppliers_in_result:
                if not supplier_name:
                    continue
                recv_mask = (
                    pl_all["Supplier"].astype(str)
                    == supplier_name)
                if "Status" in pl_all.columns:
                    recv_mask = recv_mask & (
                        pl_all["Status"].astype(str)
                        .str.upper().str.contains(
                            "RECEIVED", na=False))
                recv = pl_all[recv_mask]
                if recv.empty:
                    continue
                # Best record per PO — most recent ReceivedDate
                if ("ReceivedDate" not in recv.columns
                        or "OrderDate" not in recv.columns):
                    continue
                _rec = recv.copy()
                _rec["_rd"] = pd.to_datetime(
                    _rec["ReceivedDate"], errors="coerce")
                _rec["_od"] = pd.to_datetime(
                    _rec["OrderDate"], errors="coerce")
                _rec = _rec.dropna(subset=["_rd", "_od"])
                if _rec.empty:
                    continue
                # Group by PO; one row per PO with first ordered +
                # last received.
                grp = (_rec.groupby("OrderNumber")
                            .agg(order_date=("_od", "min"),
                                  received_date=("_rd", "max"))
                            .reset_index())
                grp = grp.sort_values(
                    "received_date", ascending=False).head(1)
                if grp.empty:
                    continue
                row = grp.iloc[0]
                lead_days = int(
                    (row["received_date"]
                      - row["order_date"]).days)
                supplier_history[supplier_name] = {
                    "previous_po": str(row["OrderNumber"]),
                    "previous_ordered": row["order_date"]
                        .strftime("%Y-%m-%d"),
                    "previous_received": row["received_date"]
                        .strftime("%Y-%m-%d"),
                    "lead_time_days": lead_days,
                }
        except Exception:
            # Supplier history is enrichment only — never block
            # the main result on a lookup failure.
            supplier_history = {}

    # v2.67.181 — explicit formatting guidance so the AI surfaces
    # the rich fields well (matches the quality bar Viktor sets).
    formatting_guidance = (
        "When composing the reply: (1) Lead with the PO header "
        "(PO-NNNN, supplier, status, ordered date). (2) Format "
        "each line as `<qty>× <friendly Name>` (NOT the raw SKU "
        "code) `— $<line_total_value>`. (3) After the lines, "
        "show current OnHand per SKU using `on_hand_now` so the "
        "user sees what's already on the shelf. (4) If "
        "`supplier_history` is present, add a sentence like "
        "'Previous PO from <supplier> took ~<lead_time_days> "
        "days, ordered <prev_ordered> → received "
        "<prev_received>' so the user has an ETA reference. "
        "(5) Be terse with internal notes (Memo/Note/Comments) — "
        "show only the freight mode and any progress detail; "
        "skip 'Draft', 'test', 'Generated by' style internal "
        "metadata.")

    return {
        "matched": len(out_rows),
        "subject": sku or family,
        "date_field_used": date_col,
        "lines": out_rows,
        "supplier_history": supplier_history or None,
        "formatting_guidance": formatting_guidance,
        "note": (
            f"Showing first {limit} of potentially many open POs."
            if len(out_rows) == limit else None),
    }


# ---------------------------------------------------------------------------
# v2.67.51 — transaction lookup tools.
#
# Why three separate tools instead of one generic "get_transaction":
#   - Each transaction type lives in its own CSV with a different
#     column shape (purchase headers vs sale lines vs stock-adjustment
#     headers). A unified tool would have to either return a wildly
#     polymorphic shape OR collapse the data — both lose information
#     the AI needs to answer the user's actual question.
#   - The user's question vocabulary is naturally different per type
#     ("what's on PO-X" vs "what did Acme buy" vs "show me the
#     stocktake"). Distinct schemas let Claude pick the right tool
#     instead of guessing flags.
#   - get_purchase_order overlaps but doesn't replace get_incoming_stock
#     — get_incoming_stock filters to OPEN POs and matches by SKU;
#     get_purchase_order is a full lookup including received/closed,
#     keyed on PO number.
# ---------------------------------------------------------------------------


def _normalise_po_number(raw: str) -> str:
    """Strip optional 'PO-' prefix and uppercase. PO-7109 → 7109."""
    s = (raw or "").strip().upper()
    if s.startswith("PO-"):
        s = s[3:]
    elif s.startswith("PO"):
        s = s[2:].lstrip("-")
    return s


def get_purchase_order(engine_df: pd.DataFrame,
                        sale_lines_df: pd.DataFrame,
                        args: dict) -> dict:
    """Look up a CIN7 purchase order by number and return header +
    every line item. v2.67.51 — built to answer 'what's on PO-7109',
    'show me the Topmet order from April', etc.

    Strategy: match purchase_lines first (it's where SKU detail
    lives), then enrich the header summary from purchase_headers if
    that DataFrame is also loaded. Status filtering is OFF by default
    — this is a full lookup tool, distinct from get_incoming_stock."""
    po_raw = (args.get("po_number") or "").strip()
    supplier_filter = (args.get("supplier") or "").strip().upper()
    date_from = (args.get("date_from") or "").strip()
    include_received = bool(args.get("include_received", True))

    purchase_lines = _PURCHASE_LINES_HOLDER.get("df")
    purchase_headers = _PURCHASE_HEADERS_HOLDER.get("df")

    if purchase_lines is None or purchase_lines.empty:
        return {
            "error": ("Purchase lines not loaded for this session. "
                      "An admin needs to call "
                      "ai_tools.set_purchase_lines() at AI page boot."),
        }

    if not po_raw and not supplier_filter and not date_from:
        return {
            "error": ("Specify either po_number, OR supplier + "
                      "date_from. With no filter the tool would "
                      "return every PO in the sync window."),
        }

    df = purchase_lines.copy()

    # PO number match (with or without PO- prefix). The line CSV
    # carries OrderNumber as the PO identifier.
    if po_raw and "OrderNumber" in df.columns:
        po_norm = _normalise_po_number(po_raw)
        order_norm = (df["OrderNumber"].astype(str).str.upper()
                      .str.replace("PO-", "", regex=False)
                      .str.replace("PO", "", regex=False)
                      .str.lstrip("-"))
        df = df[order_norm == po_norm]

    # Supplier substring filter.
    if supplier_filter and "Supplier" in df.columns:
        df = df[df["Supplier"].astype(str).str.upper()
                .str.contains(supplier_filter, na=False)]

    # date_from filter.
    if date_from:
        date_cols = [c for c in ("OrderDate", "InvoiceDate", "RequiredBy")
                     if c in df.columns]
        if date_cols:
            primary = date_cols[0]
            try:
                cutoff = pd.Timestamp(date_from)
                parsed = pd.to_datetime(df[primary], errors="coerce")
                df = df[parsed >= cutoff]
            except Exception:
                pass

    # Optional "open only" filter (default OFF — this tool is for
    # full lookup, unlike get_incoming_stock).
    if not include_received and "Status" in df.columns:
        closed_keywords = ("RECEIVED", "CLOSED", "COMPLETED",
                            "CANCELLED", "VOIDED")
        status_u = df["Status"].fillna("").astype(str).str.upper()
        df = df[~status_u.apply(
            lambda s: any(k in s for k in closed_keywords))]

    if df.empty:
        return {
            "matched": 0,
            "po_number": po_raw or None,
            "supplier_filter": supplier_filter or None,
            "date_from": date_from or None,
            "note": ("No purchase order matches the filter in the "
                     "local sync window. The line-detail sync covers "
                     "the last ~30 days. v2.67.196 — if a po_number "
                     "was given and you suspect this is a freshly-"
                     "created PO (created in the last few hours), "
                     "call get_purchase_live with the same po_number "
                     "to fetch the lines via CIN7's live API. Only "
                     "fall back to 'check CIN7 directly' if "
                     "get_purchase_live also returns matched=0."),
        }

    # Group by PO so each unique PO becomes one record with its
    # full line list. Most queries match exactly one PO; a supplier
    # / date browse may return several.
    pos_out = []
    if "OrderNumber" in df.columns:
        po_groups = df.groupby("OrderNumber", sort=False)
    else:
        po_groups = [(None, df)]

    for po_num, gdf in po_groups:
        # Header fields — same per group, take the first row.
        head_row = gdf.iloc[0]
        # Optional enrichment from purchase_headers (richer header
        # data than the line CSV holds).
        invoice_amount = None
        invoice_date = None
        order_status = None
        if (purchase_headers is not None
                and not purchase_headers.empty
                and "OrderNumber" in purchase_headers.columns):
            hdr_match = purchase_headers[
                purchase_headers["OrderNumber"].astype(str).str.upper()
                == str(po_num or "").upper()]
            if not hdr_match.empty:
                hdr = hdr_match.iloc[0]
                invoice_amount = hdr.get("InvoiceAmount")
                invoice_date = hdr.get("InvoiceDate")
                order_status = hdr.get("OrderStatus")

        line_rows = []
        for _, lr in gdf.iterrows():
            line_rows.append(_serialise_row({
                "sku": lr.get("SKU"),
                "name": lr.get("Name"),
                "quantity": lr.get("Quantity"),
                "price": lr.get("Price"),
                "tax": lr.get("Tax"),
                "discount": lr.get("Discount"),
                "total": lr.get("Total"),
                "uom": lr.get("UOM"),
            }))

        po_record = {
            "po_number": po_num,
            "supplier": head_row.get("Supplier"),
            "order_date": str(head_row.get("OrderDate"))
                if pd.notna(head_row.get("OrderDate")) else None,
            "invoice_date": (
                str(invoice_date)
                if invoice_date is not None
                and pd.notna(invoice_date)
                else (str(head_row.get("InvoiceDate"))
                      if "InvoiceDate" in gdf.columns
                      and pd.notna(head_row.get("InvoiceDate"))
                      else None)),
            "required_by": (
                str(head_row.get("RequiredBy"))
                if "RequiredBy" in gdf.columns
                and pd.notna(head_row.get("RequiredBy"))
                else None),
            "status": head_row.get("Status"),
            "order_status": order_status,
            "invoice_amount": (
                float(invoice_amount)
                if invoice_amount is not None
                and pd.notna(invoice_amount)
                else None),
            "comments": (
                str(head_row.get("Comments")).strip()
                if "Comments" in gdf.columns
                and pd.notna(head_row.get("Comments"))
                and str(head_row.get("Comments")).strip()
                else None),
            "shipping_notes": (
                str(head_row.get("ShippingNotes")).strip()
                if "ShippingNotes" in gdf.columns
                and pd.notna(head_row.get("ShippingNotes"))
                and str(head_row.get("ShippingNotes")).strip()
                else None),
            # v2.67.52 — full freeform-text field map.
            "memo": (
                str(head_row.get("Memo")).strip()
                if "Memo" in gdf.columns
                and pd.notna(head_row.get("Memo"))
                and str(head_row.get("Memo")).strip()
                else None),
            "note": (
                str(head_row.get("Note")).strip()
                if "Note" in gdf.columns
                and pd.notna(head_row.get("Note"))
                and str(head_row.get("Note")).strip()
                else None),
            "terms": (
                str(head_row.get("Terms")).strip()
                if "Terms" in gdf.columns
                and pd.notna(head_row.get("Terms"))
                and str(head_row.get("Terms")).strip()
                else None),
            "attribute_notes": (
                str(head_row.get("AttributeNotes")).strip()
                if "AttributeNotes" in gdf.columns
                and pd.notna(head_row.get("AttributeNotes"))
                and str(head_row.get("AttributeNotes")).strip()
                else None),
            "line_count": len(line_rows),
            "lines": line_rows,
        }
        pos_out.append(_serialise_row(po_record))

    return {
        "matched": len(pos_out),
        "po_number": po_raw or None,
        "purchase_orders": pos_out,
        "note": (
            "When showing the user, lead with PO number + supplier "
            "+ status, then list each line as `<qty> × <SKU> – "
            "<Name> @ <price>`. v2.67.52 — FIVE freeform text "
            "fields are populated independently: `memo` (PO Memo "
            "box — buyer's instructions for the whole order), "
            "`comments` (header comments), `shipping_notes` "
            "(vendor-purchase attribute — freight progress), "
            "`note` (top-level note), `terms` (payment terms). "
            "Surface EVERY non-null one — the buyer uses each "
            "for different purposes; suppressing any of them "
            "hides what they recorded."),
    }


def get_sale_order(engine_df: pd.DataFrame,
                    sale_lines_df: pd.DataFrame,
                    args: dict) -> dict:
    """Look up a specific sale order by number / invoice / customer +
    date range. v2.67.51 — answers 'what did Acme buy on SO-12345',
    'show me sale INV-9981', 'who ordered LED-V3060001-2 last week'.

    Reads from _SALE_LINES_LONGEST_HOLDER (set at page boot to the
    merged longest-window sale-lines DataFrame), falling back to
    sale_lines_df arg if the holder wasn't populated."""
    order_number = (args.get("order_number") or "").strip()
    invoice_number = (args.get("invoice_number") or "").strip()
    customer_filter = (args.get("customer") or "").strip().upper()
    date_from = (args.get("date_from") or "").strip()
    date_to = (args.get("date_to") or "").strip()
    limit = max(1, min(int(args.get("limit", 10) or 10), 25))

    sl = _SALE_LINES_LONGEST_HOLDER.get("df")
    if sl is None or sl.empty:
        sl = sale_lines_df
    if sl is None or sl.empty:
        return {
            "error": ("Sale lines not available. An admin needs to "
                      "call ai_tools.set_sale_lines_longest() at AI "
                      "page boot, or the local sync hasn't run."),
        }

    if not (order_number or invoice_number or customer_filter
            or date_from or date_to):
        return {
            "error": ("Specify at least one filter: order_number, "
                      "invoice_number, customer, OR a date range. "
                      "An empty filter would dump every sale line."),
        }

    df = sl.copy()

    if order_number and "OrderNumber" in df.columns:
        order_norm = order_number.upper().lstrip("SO-").lstrip("SO")
        col = (df["OrderNumber"].astype(str).str.upper()
               .str.replace("SO-", "", regex=False)
               .str.replace("SO", "", regex=False))
        df = df[col == order_norm]

    if invoice_number and "InvoiceNumber" in df.columns:
        inv_norm = invoice_number.upper()
        df = df[df["InvoiceNumber"].astype(str).str.upper() == inv_norm]

    if customer_filter and "Customer" in df.columns:
        df = df[df["Customer"].astype(str).str.upper()
                .str.contains(customer_filter, na=False)]

    if date_from and "OrderDate" in df.columns:
        try:
            cutoff = pd.Timestamp(date_from)
            parsed = pd.to_datetime(df["OrderDate"], errors="coerce")
            df = df[parsed >= cutoff]
        except Exception:
            pass

    if date_to and "OrderDate" in df.columns:
        try:
            cutoff = pd.Timestamp(date_to) + pd.Timedelta(days=1)
            parsed = pd.to_datetime(df["OrderDate"], errors="coerce")
            df = df[parsed < cutoff]
        except Exception:
            pass

    if df.empty:
        return {
            "matched": 0,
            "filters": {
                "order_number": order_number or None,
                "invoice_number": invoice_number or None,
                "customer": customer_filter or None,
                "date_from": date_from or None,
                "date_to": date_to or None,
            },
            "note": ("No sale matches in the local sync window. "
                     "Sale-line detail covers the last ~30 days. "
                     "v2.67.179 — if an order_number was provided "
                     "and you suspect the sale was created in the "
                     "last few hours, call get_sale_live with the "
                     "same order_number to bypass the cache via a "
                     "live CIN7 API fetch."),
        }

    # Group by sale (SaleID preferred — order_number can be reused
    # across credit notes / amendments).
    group_col = ("SaleID" if "SaleID" in df.columns
                 else "OrderNumber" if "OrderNumber" in df.columns
                 else None)
    if group_col is None:
        return {"error": "No SaleID / OrderNumber column on sale_lines."}

    grouped = list(df.groupby(group_col, sort=False))
    grouped = grouped[:limit]

    sales_out = []
    for sale_id, gdf in grouped:
        head = gdf.iloc[0]
        lines = []
        line_total = 0.0
        for _, lr in gdf.iterrows():
            qty = pd.to_numeric(lr.get("Quantity"), errors="coerce")
            total = pd.to_numeric(lr.get("Total"), errors="coerce")
            if pd.notna(total):
                line_total += float(total)
            lines.append(_serialise_row({
                "sku": lr.get("SKU"),
                "name": lr.get("Name"),
                "quantity": (float(qty) if pd.notna(qty) else None),
                "price": lr.get("Price"),
                "discount": lr.get("Discount"),
                "tax": lr.get("Tax"),
                "total": (float(total) if pd.notna(total) else None),
                "uom": lr.get("UOM"),
            }))

        # v2.67.52 — sale-side freeform text fields. Each is independently
        # populated by sales reps for different purposes (build
        # instructions in Memo, customer PO# in CustomerReference,
        # delivery quirks in ShippingNotes, header note in Note,
        # payment terms in Terms). Surface them all when present.
        def _txt(col):
            if (col not in gdf.columns
                    or not pd.notna(head.get(col))
                    or not str(head.get(col)).strip()):
                return None
            return str(head.get(col)).strip()

        sales_out.append(_serialise_row({
            "sale_id": sale_id,
            "order_number": head.get("OrderNumber"),
            "invoice_number": head.get("InvoiceNumber"),
            "order_date": (str(head.get("OrderDate"))
                            if pd.notna(head.get("OrderDate"))
                            else None),
            "invoice_date": (str(head.get("InvoiceDate"))
                              if "InvoiceDate" in gdf.columns
                              and pd.notna(head.get("InvoiceDate"))
                              else None),
            "status": head.get("Status"),
            "sale_type": head.get("SaleType"),
            "source_channel": head.get("SourceChannel"),
            "customer": head.get("Customer"),
            "memo": _txt("Memo"),
            "note": _txt("Note"),
            "shipping_notes": _txt("ShippingNotes"),
            "terms": _txt("Terms"),
            "customer_reference": _txt("CustomerReference"),
            "line_count": len(lines),
            "line_total": round(line_total, 2),
            "lines": lines,
        }))

    return {
        "matched": len(sales_out),
        "limit_applied": limit,
        "sales": sales_out,
        "note": (
            "Show the user: order number + customer + date + status, "
            "then each line as `<qty> × <SKU> – <Name> @ <price> = "
            "$<total>`. line_total is the sum of line `Total` values "
            "(excludes header-level shipping / tax adjustments). "
            "v2.67.52 — FIVE freeform text fields are populated "
            "independently: `memo` (Sale Order Memo — rep's "
            "build/delivery instructions), `note` (top-level "
            "header note), `shipping_notes` (top-level shipping "
            "instructions on sales — different location from POs), "
            "`terms` (payment terms), `customer_reference` "
            "(customer's own PO# referencing this sale). Surface "
            "EVERY non-null one — sales reps use each for different "
            "purposes."),
    }


def get_sale_live(engine_df: pd.DataFrame,
                    sale_lines_df: pd.DataFrame,
                    args: dict) -> dict:
    """v2.67.179 — Live CIN7 fallback when get_sale_order misses.

    Use case: a sale was just created in CIN7 (e.g. SO-56331) and
    isn't in the local 30-day sale_lines cache yet. The local
    SO→Shopify lookup may still find the sale header (from a
    different CSV with a longer window) but the line items aren't
    available. This tool hits CIN7's GET /sale endpoint directly
    via Cin7Client.get_sale() and returns the live lines + header,
    so the AI can answer 'what SKUs are on SO-56331' even when
    sync hasn't caught up.

    Two-step resolution:
      1. Find SaleID (UUID) from local sales_last_*d_*.csv via
         so_lookup.lookup_so()
      2. GET /sale?ID={uuid} via Cin7Client
    """
    order_number = (args.get("order_number") or "").strip()
    if not order_number:
        return {"error": "order_number is required"}

    # 1. Resolve order_number → SaleID
    try:
        from so_lookup import lookup_so
        info = lookup_so(order_number)
    except Exception as exc:
        return {"error": f"so_lookup failed: {exc}"}
    # so_lookup stores the SaleID UUID under the key "cin7_id".
    sale_id = (info or {}).get("cin7_id") or ""
    if not sale_id:
        return {
            "matched": 0,
            "note": (f"Couldn't resolve {order_number} to a CIN7 "
                      f"SaleID in the local sales CSV. The sale "
                      f"may pre-date even the longest local sync "
                      f"window. Direct the user to check CIN7."),
        }

    # 2. Live CIN7 GET /sale
    import os
    try:
        from cin7_sync import Cin7Client
    except Exception as exc:
        return {"error": f"cin7_sync import failed: {exc}"}
    account_id = os.environ.get("CIN7_ACCOUNT_ID", "").strip()
    app_key = os.environ.get("CIN7_APPLICATION_KEY", "").strip()
    if not (account_id and app_key):
        return {
            "error": ("CIN7 credentials not in env on this "
                          "service — can't do a live fetch."),
        }
    try:
        client = Cin7Client(account_id, app_key)
        sale = client.get_sale(sale_id)
    except Exception as exc:
        return {"error": f"CIN7 GET /sale failed: {exc}"}
    if not isinstance(sale, dict) or not sale:
        return {
            "matched": 0,
            "note": "CIN7 returned an empty sale object.",
        }

    # Extract line items and enrich with current OnHand from
    # engine_df so the AI can immediately answer
    # "are all SKUs available".
    order_obj = sale.get("Order") or {}
    lines = order_obj.get("Lines") or sale.get("Lines") or []
    on_hand_map: dict = {}
    if engine_df is not None and not engine_df.empty:
        if "SKU" in engine_df.columns and "OnHand" in engine_df.columns:
            on_hand_map = (
                engine_df.set_index(
                    engine_df["SKU"].astype(str).str.upper())[
                    "OnHand"].apply(
                    lambda v: float(v or 0)).to_dict())
    line_rows = []
    for line in lines:
        sku = str(line.get("SKU") or "").strip()
        qty = line.get("Quantity")
        try:
            qty_n = float(qty) if qty is not None else None
        except (TypeError, ValueError):
            qty_n = None
        oh = on_hand_map.get(sku.upper())
        available = (
            None if oh is None or qty_n is None
            else "yes" if oh >= qty_n
            else "no" if oh < qty_n
            else None)
        line_rows.append({
            "sku": sku,
            "name": line.get("Name") or "",
            "quantity": qty_n,
            "on_hand": oh,
            "available": available,
            "price": line.get("Price"),
            "discount": line.get("Discount"),
            "total": line.get("Total"),
            "tax": line.get("Tax"),
            "comment": line.get("Comment") or "",
        })
    return {
        "matched": 1,
        "source": "cin7_live_api",
        "sale_id": sale_id,
        "order_number": sale.get("OrderNumber"),
        "invoice_number": sale.get("InvoiceNumber"),
        "customer": sale.get("Customer"),
        "customer_reference": sale.get("CustomerReference"),
        "status": sale.get("Status"),
        "order_date": sale.get("OrderDate"),
        "invoice_date": sale.get("InvoiceDate"),
        "memo": sale.get("Memo"),
        "shipping_notes": sale.get("ShippingNotes"),
        "note": sale.get("Note"),
        "terms": sale.get("Terms"),
        "lines": line_rows,
        "guidance": (
            "Data fetched LIVE from CIN7 (bypasses the 30-day "
            "local sync window). For each line, "
            "`available` = yes/no based on current OnHand vs "
            "requested qty. Surface every line with availability "
            "status so the user gets a per-SKU answer."),
    }


def get_purchase_live(engine_df: pd.DataFrame,
                        sale_lines_df: pd.DataFrame,
                        args: dict) -> dict:
    """v2.67.196 — Live CIN7 fallback when get_purchase_order
    misses a freshly-created PO. Mirrors get_sale_live (v2.67.179).

    Use case: PO commentary in #buyer-review fires off a brand-new
    PO link (PO-7160 today). The PO was created in CIN7 minutes
    before the buyer posted, so the local purchase_lines_*.csv
    hasn't synced it yet. get_purchase_order returns matched=0.
    This tool hits CIN7's GET /purchase endpoint directly to
    fetch the lines.

    Looks up by either PO number ("PO-7160" or "7160") or the
    UUID if available. Returns each line's SKU/name/qty + current
    engine OnHand so the AI can immediately compose per-SKU
    commentary."""
    po_ref = ((args.get("po_number") or "")
                  or (args.get("purchase_id") or "")).strip()
    if not po_ref:
        return {"error": "po_number or purchase_id is required"}

    import os
    try:
        from cin7_sync import Cin7Client
    except Exception as exc:
        return {"error": f"cin7_sync import failed: {exc}"}
    account_id = os.environ.get("CIN7_ACCOUNT_ID", "").strip()
    app_key = os.environ.get("CIN7_APPLICATION_KEY", "").strip()
    if not (account_id and app_key):
        return {
            "error": ("CIN7 credentials not in env on this "
                          "service — can't do a live fetch."),
        }
    try:
        client = Cin7Client(account_id, app_key)
        purchase = client.get_purchase(po_ref)
    except Exception as exc:
        return {"error": f"CIN7 GET /purchase failed: {exc}"}
    if not isinstance(purchase, dict) or not purchase:
        return {
            "matched": 0,
            "note": (
                f"CIN7 /purchaseList?Search={po_ref} returned no "
                f"match. v2.67.312 — this lookup uses the correct "
                f"endpoint and DOES return DRAFT POs (the prior "
                f"endpoint silently skipped drafts), so a missing "
                f"result now genuinely means the PO doesn't exist "
                f"in CIN7. Don't tell the user to 'wait 2-3 "
                f"minutes for propagation' — that was a wrong "
                f"guess in the old code. Instead, ask them to "
                f"double-check the PO number, confirm it was "
                f"actually saved (not just opened-but-not-saved "
                f"in CIN7's UI), or check whether it was voided/"
                f"deleted."),
        }

    # Extract lines + enrich with current OnHand.
    order_obj = purchase.get("Order") or {}
    lines = order_obj.get("Lines") or purchase.get("Lines") or []
    on_hand_map: dict = {}
    if engine_df is not None and not engine_df.empty:
        if ("SKU" in engine_df.columns
                and "OnHand" in engine_df.columns):
            on_hand_map = (
                engine_df.set_index(
                    engine_df["SKU"].astype(str).str.upper())[
                    "OnHand"].apply(
                    lambda v: float(v or 0)).to_dict())

    # v2.67.355 — Storage dim check (James 2026-06-03). Warehouse
    # needs `Storage L x W x H In` populated on every product so
    # bin assignment works when a shipment lands. Fetch each line's
    # product detail and surface the dim value (or flag it missing).
    # Defensive parser — CIN7 returns custom attributes in either:
    #   (a) AdditionalAttributes: [{Name: "...", Value: "..."}, ...]
    #   (b) Positional flat dict: AdditionalAttribute1..10 keys
    #       with the name-to-position map coming from AttributeSet
    # We probe both. If neither yields a value, treat as missing.
    _DIM_FIELD = "Storage L x W x H In"

    def _extract_storage_dim(product_detail: dict) -> str:
        """Return the raw Storage L x W x H In value (trimmed), or empty
        string if the field is absent entirely.  We return whatever CIN7
        has — including partials like '___ x 2.756" x 1.969"' — so the
        team can see the current state and fill in what's missing over time.
        Tries the array form first, then the positional form."""
        if not isinstance(product_detail, dict):
            return ""
        attrs = product_detail.get("AdditionalAttributes")
        # Form (a): list of {Name, Value}
        if isinstance(attrs, list):
            for a in attrs:
                if not isinstance(a, dict):
                    continue
                name = str(a.get("Name") or "").strip()
                if name.lower() == _DIM_FIELD.lower():
                    return str(a.get("Value") or "").strip()
        # Form (b): positional flat dict (sometimes nested under
        # AdditionalAttributes, sometimes top-level on the product).
        # Names live in AttributeSet config; without that mapping we
        # scan all 10 slots and check if any non-empty value LOOKS
        # like a dim string (contains at least one 'x' separator with a
        # number on one side). We accept partials — ___ x 2.5 x 1.9 is
        # still useful — so we just need at least one numeric segment.
        import re as _re
        _dim_loose = _re.compile(
            r"(?:\d+(?:\.\d+)?|_+)\s*x\s*(?:\d+(?:\.\d+)?|_+)",
            _re.IGNORECASE)
        positional = (
            attrs if isinstance(attrs, dict)
            else product_detail)
        for i in range(1, 11):
            v = positional.get(f"AdditionalAttribute{i}")
            if v and isinstance(v, str) and _dim_loose.search(v):
                return v.strip()
        return ""

    import re as _re_dim
    _incomplete_pat = _re_dim.compile(r"_{2,}|^x\s|^\s*x", _re_dim.IGNORECASE)

    def _dim_is_incomplete(dim: str) -> bool:
        """True if the dim value exists but has placeholders (___) or
        missing segments — e.g. '___ x 2.756" x 1.969"'."""
        if not dim:
            return False
        # Has ___ placeholder anywhere
        if "___" in dim or "__" in dim:
            return True
        # Doesn't contain two 'x' separators (not all 3 dimensions present)
        parts = [p.strip() for p in _re_dim.split(r"\s*x\s*", dim, flags=_re_dim.IGNORECASE)]
        if len(parts) < 3:
            return True
        # Any segment is empty or non-numeric
        for part in parts[:3]:
            clean = part.replace('"', '').replace("'", "").strip()
            try:
                float(clean)
            except ValueError:
                return True
        return False

    def _fetch_product_dim(sku_val: str) -> dict:
        """Fetch the product detail for a single SKU and pull the
        Storage L x W x H In value. Returns {dim, missing, error}.
        Swallows per-SKU failures so one bad lookup doesn't sink the
        whole PO commentary — buyer can still see the rest of the
        analysis."""
        if not sku_val:
            return {"dim": "", "missing": True, "error": "no SKU"}
        try:
            resp = client.get(
                "product",
                params={"SKU": sku_val, "IncludeAttributes": "true"})
        except Exception as exc:
            return {
                "dim": "", "missing": True,
                "error": f"product lookup failed: {exc}"[:200]}
        # CIN7 returns either a single product dict OR a list wrapped
        # in {Products: [...]}. Handle both.
        if isinstance(resp, dict):
            prods = resp.get("Products")
            if isinstance(prods, list) and prods:
                p = prods[0]
            else:
                p = resp
        else:
            return {
                "dim": "", "missing": True,
                "error": "unexpected response shape"}
        dim = _extract_storage_dim(p)
        incomplete = _dim_is_incomplete(dim) if dim else False
        return {
            "dim": dim,
            "missing": not bool(dim),
            "incomplete": incomplete,
        }

    # v2.67.366 — build a SKU→engine-row lookup so every PO line gets
    # the real ABC / trend / demand / available / on_order data from the
    # engine instead of leaving the AI to hallucinate it from OnHand alone.
    eng_map: dict = {}
    if engine_df is not None and not engine_df.empty:
        for _, _erow in engine_df.iterrows():
            _esku = str(_erow.get("SKU") or "").strip().upper()
            if _esku:
                eng_map[_esku] = _erow

    def _eng(sku: str, col: str, default=None):
        row = eng_map.get(sku.upper())
        if row is None:
            return default
        v = row.get(col)
        return default if v is None or (isinstance(v, float) and __import__("math").isnan(v)) else v

    line_rows = []
    missing_dim_skus: list = []
    incomplete_dim_skus: list = []
    for line in lines:
        sku = str(line.get("SKU") or "").strip()
        qty = line.get("Quantity")
        try:
            qty_n = float(qty) if qty is not None else None
        except (TypeError, ValueError):
            qty_n = None
        oh = on_hand_map.get(sku.upper())
        dim_info = _fetch_product_dim(sku)
        if dim_info.get("missing") and sku:
            missing_dim_skus.append(sku)
        elif dim_info.get("incomplete") and sku:
            incomplete_dim_skus.append(sku)
        line_rows.append({
            "sku": sku,
            "name": line.get("Name") or "",
            "quantity": qty_n,
            "on_hand_now": oh,
            "price": line.get("Price"),
            "discount": line.get("Discount"),
            "total": line.get("Total"),
            "tax": line.get("Tax"),
            "uom": line.get("UOM"),
            "supplier_sku": line.get("SupplierSKU"),
            "comment": line.get("Comment") or "",
            "storage_dim": dim_info.get("dim", ""),
            "storage_dim_missing": dim_info.get("missing", True),
            "storage_dim_incomplete": dim_info.get("incomplete", False),
            # Engine signals — real data, not AI inference
            "abc_class": _eng(sku, "ABC", ""),
            "trend_flag": _eng(sku, "trend_flag", ""),
            "is_dormant": bool(_eng(sku, "is_dormant", False)),
            "units_12mo": _eng(sku, "units_12mo", 0),
            "effective_units_12mo": _eng(sku, "effective_units_12mo", 0),
            "units_45d": _eng(sku, "units_45d", 0),
            "available": _eng(sku, "Available", None),
            "on_order": _eng(sku, "OnOrder", None),
            "allocated": _eng(sku, "Allocated", None),
            "excess_units": _eng(sku, "excess_units", 0),
            "suggested_reorder": _eng(sku, "suggested_reorder", None),
            "reorder_status": _eng(sku, "reorder_status", ""),
        })

    # Header-level metadata. v2.67.197 — surface draft status
    # explicitly so the AI can include a 📝 DRAFT badge in the
    # reply (the commentary use case is largely about deciding
    # whether to approve drafts, so this is the most important
    # signal on the header).
    status_raw = (purchase.get("Status") or "").upper()
    is_draft = "DRAFT" in status_raw
    return {
        "matched": 1,
        "source": "cin7_live_api",
        "purchase_id": purchase.get("ID"),
        "po_number": purchase.get("OrderNumber"),
        "supplier": purchase.get("Supplier"),
        "supplier_id": purchase.get("SupplierID"),
        "status": purchase.get("Status"),
        "is_draft": is_draft,
        "order_date": purchase.get("OrderDate"),
        "required_by": purchase.get("RequiredBy"),
        "comments": purchase.get("Comments"),
        "shipping_notes": purchase.get("ShippingNotes"),
        "memo": purchase.get("Memo"),
        "note": purchase.get("Note"),
        "terms": purchase.get("Terms"),
        "lines": line_rows,
        "storage_dims_missing_skus": missing_dim_skus,
        "storage_dims_missing_count": len(missing_dim_skus),
        "storage_dims_incomplete_skus": incomplete_dim_skus,
        "storage_dims_incomplete_count": len(incomplete_dim_skus),
        "guidance": (
            "Data fetched LIVE from CIN7 (bypasses the 30-day "
            "local sync window). For each line, compose engine "
            "commentary per the existing po_review prompt: "
            "OnHand is in `on_hand_now`; for ABC / trend / "
            "12mo demand / dormancy, call get_sku_details or "
            "search_products_by_text with the SKU. Use the "
            "✅ / ⚠️ / 🪫 / 📦 / 💼 flags per the po_review "
            "system prompt. If is_draft=True, lead with "
            "'📝 *DRAFT* — pending approval. Commentary "
            "follows so you can decide whether to approve.' "
            "Draft commentary is the high-value path — "
            "decisions get made off it.\n\n"
            "v2.67.366 — ENGINE SIGNALS NOW ON EVERY LINE. "
            "Each line now carries real engine data: `abc_class`, "
            "`trend_flag`, `is_dormant`, `units_12mo`, "
            "`effective_units_12mo`, `units_45d`, `available`, "
            "`on_order`, `allocated`, `excess_units`, "
            "`suggested_reorder`, `reorder_status`. "
            "ALWAYS use these fields for your commentary — NEVER "
            "infer or guess demand/classification from OnHand alone. "
            "If `units_12mo` is 0 but `effective_units_12mo` > 0, "
            "report the effective figure. "
            "If engine data is missing for a SKU (fields are null/0), "
            "say 'not in engine — verify manually' rather than "
            "calling it dead or dormant. "
            "v2.67.361 — STORAGE DIMS (always surface, flag gaps). "
            "Each line carries `storage_dim` (raw value of CIN7's "
            "`Storage L x W x H In` field — always show it even if "
            "partial, e.g. '___ x 2.756\" x 1.969\"'), "
            "`storage_dim_missing` (true = field is blank), and "
            "`storage_dim_incomplete` (true = field has a value but "
            "contains placeholders or fewer than 3 dimensions). "
            "For EVERY line: append the dim value to that line's "
            "commentary as `📐 <value>` if present, or "
            "`📐 dims not set` if missing. "
            "After all per-line output, if any SKUs have missing "
            "OR incomplete dims, include a summary section:\\n"
            "```\\n"
            "📐 *Storage dims incomplete/missing — update in CIN7:*\\n"
            "<SKU1> (___ x 2.756 x 1.969 — length missing), "
            "<SKU2> (not set)\\n"
            "@warehouse please capture before shipment lands.\\n"
            "```\\n"
            "The team updates dims over time so always show what's "
            "there. Only omit the summary section if ALL lines have "
            "complete dims (storage_dims_missing_count == 0 AND "
            "storage_dims_incomplete_count == 0)."),
    }


def get_stock_adjustment(engine_df: pd.DataFrame,
                          sale_lines_df: pd.DataFrame,
                          args: dict) -> dict:
    """Look up CIN7 stock adjustments / stocktakes. v2.67.51 — answers
    'show me ST-2034', 'what adjustments ran last week', 'find the
    cycle count for warehouse 500'.

    LIMITATION: the local sync only captures adjustment HEADERS
    (TaskID, EffectiveDate, StocktakeNumber, Status, Account,
    Reference). Per-line SKU detail (which SKUs were adjusted and by
    how much) requires a per-task detail call we don't sync today.
    The tool surfaces what we have + tells the AI to direct the user
    to CIN7 for line detail."""
    stocktake_number = (args.get("stocktake_number") or "").strip()
    ref_substring = (args.get("reference_substring") or "").strip().upper()
    date_from = (args.get("date_from") or "").strip()
    date_to = (args.get("date_to") or "").strip()
    status = (args.get("status") or "").strip().upper()
    limit = max(1, min(int(args.get("limit", 25) or 25), 50))

    adj = _STOCK_ADJUSTMENTS_HOLDER.get("df")
    if adj is None or adj.empty:
        return {
            "error": ("Stock adjustments not loaded for this "
                      "session. An admin needs to call "
                      "ai_tools.set_stock_adjustments() at AI page "
                      "boot."),
        }

    df = adj.copy()

    if stocktake_number and "StocktakeNumber" in df.columns:
        st_norm = stocktake_number.upper()
        df = df[df["StocktakeNumber"].astype(str).str.upper() == st_norm]

    if ref_substring and "Reference" in df.columns:
        df = df[df["Reference"].astype(str).str.upper()
                .str.contains(ref_substring, na=False)]

    if status and "Status" in df.columns:
        df = df[df["Status"].astype(str).str.upper() == status]

    if date_from and "EffectiveDate" in df.columns:
        try:
            cutoff = pd.Timestamp(date_from)
            parsed = pd.to_datetime(df["EffectiveDate"], errors="coerce")
            df = df[parsed >= cutoff]
        except Exception:
            pass

    if date_to and "EffectiveDate" in df.columns:
        try:
            cutoff = pd.Timestamp(date_to) + pd.Timedelta(days=1)
            parsed = pd.to_datetime(df["EffectiveDate"], errors="coerce")
            df = df[parsed < cutoff]
        except Exception:
            pass

    if df.empty:
        return {
            "matched": 0,
            "filters": {
                "stocktake_number": stocktake_number or None,
                "reference_substring": ref_substring or None,
                "status": status or None,
                "date_from": date_from or None,
                "date_to": date_to or None,
            },
            "note": ("No stock adjustments match. The local sync "
                     "covers the last 30 days; older adjustments "
                     "won't be in this snapshot."),
        }

    # Sort newest first.
    if "EffectiveDate" in df.columns:
        df = df.assign(_d=pd.to_datetime(
            df["EffectiveDate"], errors="coerce"))
        df = df.sort_values("_d", ascending=False).drop(columns="_d")

    out_rows = []
    for _, r in df.head(limit).iterrows():
        out_rows.append(_serialise_row({
            "task_id": r.get("TaskID"),
            "stocktake_number": r.get("StocktakeNumber"),
            "effective_date": (str(r.get("EffectiveDate"))
                                if pd.notna(r.get("EffectiveDate"))
                                else None),
            "status": r.get("Status"),
            "account": r.get("Account"),
            "reference": (str(r.get("Reference")).strip()
                           if pd.notna(r.get("Reference"))
                           and str(r.get("Reference")).strip()
                           else None),
        }))

    return {
        "matched": len(out_rows),
        "limit_applied": limit,
        "adjustments": out_rows,
        "line_detail_available": False,
        "note": (
            "IMPORTANT: per-SKU line detail (which SKUs were "
            "adjusted by how much) is NOT in our local sync. Tell "
            "the user the header data shown here, then point them "
            "to CIN7 → Inventory → Stock adjustments → "
            "<StocktakeNumber> for the line breakdown."),
    }


def get_shipping_details(engine_df: pd.DataFrame,
                           sale_lines_df: pd.DataFrame,
                           args: dict) -> dict:
    """v2.67.54 — ShipStation lookup. Reads the local shipments
    DataFrame (merged longest-window pattern) and returns matching
    shipment records.

    Filter precedence: tracking_number > order_number > carrier +
    customer + date range. The first specific filter wins because
    the user usually knows ONE of those identifiers, and falling
    through to broad filters when a specific one is set produces
    confusing 'multiple shipments matched' answers."""
    order_number = (args.get("order_number") or "").strip()
    tracking_number = (args.get("tracking_number") or "").strip()
    customer_filter = (args.get("customer") or "").strip().upper()
    carrier_filter = (args.get("carrier_code") or "").strip().lower()
    date_from = (args.get("date_from") or "").strip()
    date_to = (args.get("date_to") or "").strip()
    limit = max(1, min(int(args.get("limit", 25) or 25), 50))

    df = _SHIPMENTS_HOLDER.get("df")
    if df is None or df.empty:
        return {
            "error": ("ShipStation shipments not loaded. Either the "
                      "ShipStation env vars (SHIPSTATION_API_KEY + "
                      "SHIPSTATION_API_SECRET) aren't configured, "
                      "or the shipstation_sync hasn't run yet. The "
                      "first sync can take 30-60 minutes for a "
                      "5-year backfill; subsequent syncs are "
                      "incremental."),
        }

    if not (order_number or tracking_number or customer_filter
            or carrier_filter or date_from or date_to):
        return {
            "error": ("Specify at least one filter: order_number, "
                      "tracking_number, customer, carrier_code, "
                      "OR a date range. Empty filter would dump "
                      "thousands of shipments."),
        }

    work = df.copy()

    # Tracking number is most specific — try it first.
    if tracking_number and "TrackingNumber" in work.columns:
        work = work[work["TrackingNumber"].astype(str).str.upper()
                     == tracking_number.upper()]
    # Order number. ShipStation v2's `shipment_number` field is
    # the CIN7 invoice number (INV-XXXXX), so we strip both SO-
    # and INV- prefixes to handle however the user typed it.
    # v2.67.55+ — match against the same normalised column so
    # 'INV-53141', '53141', 'inv-53141' all hit the same row.
    elif order_number and "OrderNumber" in work.columns:
        norm = order_number.upper()
        for pfx in ("SO-", "SO", "INV-", "INV"):
            if norm.startswith(pfx):
                norm = norm[len(pfx):].lstrip("-")
                break
        col = (work["OrderNumber"].astype(str).str.upper()
                .str.replace("SO-", "", regex=False)
                .str.replace("SO", "", regex=False)
                .str.replace("INV-", "", regex=False)
                .str.replace("INV", "", regex=False)
                .str.lstrip("-"))
        work = work[col == norm]
    else:
        if customer_filter and "CustomerName" in work.columns:
            work = work[work["CustomerName"].astype(str).str.upper()
                         .str.contains(customer_filter, na=False)]
        if carrier_filter and "CarrierCode" in work.columns:
            work = work[work["CarrierCode"].astype(str).str.lower()
                         == carrier_filter]
        if date_from and "ShipDate" in work.columns:
            try:
                cutoff = pd.Timestamp(date_from)
                parsed = pd.to_datetime(work["ShipDate"], errors="coerce")
                work = work[parsed >= cutoff]
            except Exception:
                pass
        if date_to and "ShipDate" in work.columns:
            try:
                cutoff = pd.Timestamp(date_to) + pd.Timedelta(days=1)
                parsed = pd.to_datetime(work["ShipDate"], errors="coerce")
                work = work[parsed < cutoff]
            except Exception:
                pass

    if work.empty:
        return {
            "matched": 0,
            "filters": {
                "order_number": order_number or None,
                "tracking_number": tracking_number or None,
                "customer": customer_filter or None,
                "carrier_code": carrier_filter or None,
                "date_from": date_from or None,
                "date_to": date_to or None,
            },
            "note": ("No shipments match. Either the order hasn't "
                     "shipped yet (ShipStation only records "
                     "shipments AFTER carrier label is created), "
                     "or it's outside the local sync window. The "
                     "AI should tell the user to check ShipStation "
                     "directly OR wait for the next sync."),
        }

    # Sort newest first.
    if "ShipDate" in work.columns:
        work = work.assign(_d=pd.to_datetime(
            work["ShipDate"], errors="coerce"))
        work = work.sort_values("_d", ascending=False).drop(columns="_d")

    out = []
    for _, r in work.head(limit).iterrows():
        # v2.67.55c — distinguish customer_charge (revenue, what we
        # billed the customer for shipping) from actual_cost (what
        # UPS billed us). Compute margin from both. Pre-v2.67.55c
        # CSVs have only the legacy ShipmentCost column with the
        # WRONG semantics (it held customer-charge); detect that
        # case and label it cleanly.
        cust_charge = r.get("CustomerShippingCharge")
        actual_cost = r.get("ShipmentCost")
        margin = r.get("ShippingMargin")
        # Backwards compat: legacy CSVs (pre-v2.67.55c) have
        # ShipmentCost = customer charge and no
        # CustomerShippingCharge column. Detect by absence of the
        # new column. In that case we can't compute margin without
        # an additional /labels API call — flag it.
        legacy_csv = "CustomerShippingCharge" not in work.columns
        if legacy_csv:
            cust_charge = r.get("ShipmentCost")  # legacy field
            actual_cost = None
            margin = None
        out.append(_serialise_row({
            "shipment_id": r.get("ShipmentID"),
            "order_number": r.get("OrderNumber"),
            "customer": r.get("CustomerName"),
            "ship_date": (str(r.get("ShipDate"))
                            if pd.notna(r.get("ShipDate"))
                            else None),
            "voided": bool(r.get("Voided"))
                       if pd.notna(r.get("Voided")) else False,
            "tracking_number": r.get("TrackingNumber"),
            "tracking_url": r.get("TrackingURL"),
            "carrier": r.get("CarrierCode"),
            "service": r.get("ServiceCode"),
            # v2.67.55c — three distinct fields. Customer charge
            # (revenue), actual cost (what UPS billed us), and
            # margin (revenue - cost). Negative margin = we lost
            # money on this shipment.
            "customer_charge": (
                float(cust_charge)
                if cust_charge is not None and pd.notna(cust_charge)
                else None),
            "actual_cost": (
                float(actual_cost)
                if actual_cost is not None and pd.notna(actual_cost)
                else None),
            "shipping_margin": (
                float(margin)
                if margin is not None and pd.notna(margin)
                else None),
            "insurance_cost": (
                float(r.get("InsuranceCost"))
                if pd.notna(r.get("InsuranceCost")) else None),
            "ship_to_city": r.get("ShipToCity"),
            "ship_to_state": r.get("ShipToState"),
            "ship_to_country": r.get("ShipToCountry"),
            "ship_to_postal": r.get("ShipToPostal"),
            "weight_value": r.get("WeightValue"),
            "weight_units": r.get("WeightUnits"),
            "dim_length": r.get("DimensionsLength"),
            "dim_width": r.get("DimensionsWidth"),
            "dim_height": r.get("DimensionsHeight"),
            "dim_units": r.get("DimensionsUnits"),
            "item_count": r.get("ItemCount"),
            "item_summary": r.get("ItemSummary"),
            "customer_notes": (
                str(r.get("CustomerNotes")).strip()
                if pd.notna(r.get("CustomerNotes"))
                and str(r.get("CustomerNotes")).strip()
                else None),
            "internal_notes": (
                str(r.get("InternalNotes")).strip()
                if pd.notna(r.get("InternalNotes"))
                and str(r.get("InternalNotes")).strip()
                else None),
        }))

    return {
        "matched": len(out),
        "limit_applied": limit,
        "shipments": out,
        "data_quality_note": (
            "Pre-v2.67.55c CSVs only carry customer_charge (no "
            "actual_cost or margin). If actual_cost is null "
            "across all rows, the data was synced before the "
            "cost-semantics fix — re-run shipstation_sync to "
            "backfill."
            if "CustomerShippingCharge" not in work.columns
            else None),
        "note": (
            "Show the user: ship_date · order_number · customer · "
            "carrier+service · tracking_number · customer_charge "
            "vs actual_cost. If shipping_margin < 0, flag explicitly "
            "as a LOSS-MAKING shipment with the dollar amount. "
            "Voided shipments → mark 'VOIDED'. Surface "
            "customer_notes / internal_notes if non-null. If "
            "tracking_url is set, include it as a clickable "
            "link."),
    }


def get_shopify_order(engine_df: pd.DataFrame,
                       sale_lines_df: pd.DataFrame,
                       args: dict) -> dict:
    """v2.67.55 — fetch Shopify-side conversion attribution for a
    sale that came through Shopify. CIN7's /sale endpoint records
    SourceChannel='Shopify' but NOT the channel sub-attribution
    (landing page, referrer, UTM, locale, discount codes). This
    tool reads the Shopify Admin API mirror that shopify_sync.py
    drops as `shopify_orders_full.csv` + rolling-window patches.

    Filter precedence: order_name > email > customer + date.
    Same first-specific-wins logic as get_shipping_details so the
    answer doesn't accidentally widen when the user provided a
    specific identifier."""
    order_name = (args.get("order_name") or "").strip()
    email = (args.get("email") or "").strip()
    customer_filter = (args.get("customer") or "").strip().upper()
    date_from = (args.get("date_from") or "").strip()
    date_to = (args.get("date_to") or "").strip()
    limit = max(1, min(int(args.get("limit", 10) or 10), 25))

    df = _SHOPIFY_ORDERS_HOLDER.get("df")
    if df is None or df.empty:
        return {
            "error": ("Shopify orders not loaded. Either the "
                      "SHOPIFY_DOMAIN / SHOPIFY_ACCESS_TOKEN env "
                      "vars aren't set, or the shopify-orders "
                      "sync hasn't run yet. To enable: set the "
                      "env vars, then run `python shopify_sync.py "
                      "--orders-full 1825` once for backfill."),
        }

    if not (order_name or email or customer_filter or date_from
            or date_to):
        return {
            "error": ("Specify at least one filter: order_name, "
                      "email, customer, OR a date range. Empty "
                      "filter would dump every Shopify order."),
        }

    work = df.copy()

    if order_name:
        # Match against both Name (#1234) and OrderNumber (1234).
        norm = order_name.lstrip("#").strip()
        candidates = []
        if "OrderNumber" in work.columns:
            candidates.append(
                work["OrderNumber"].astype(str).str.lstrip("#") == norm)
        if "Name" in work.columns:
            candidates.append(
                work["Name"].astype(str).str.lstrip("#") == norm)
        if candidates:
            mask = candidates[0]
            for c in candidates[1:]:
                mask = mask | c
            work = work[mask]
    elif email and "Email" in work.columns:
        work = work[work["Email"].astype(str).str.lower()
                     == email.lower()]
    else:
        if customer_filter:
            cols = [c for c in
                      ("CustomerFirstName", "CustomerLastName")
                      if c in work.columns]
            if cols:
                full_name = work[cols[0]].fillna("").astype(str)
                for c in cols[1:]:
                    full_name = full_name + " " + work[c].fillna("").astype(str)
                work = work[full_name.str.upper().str.contains(
                    customer_filter, na=False)]
        if date_from and "CreatedAt" in work.columns:
            try:
                cutoff = pd.Timestamp(date_from)
                parsed = pd.to_datetime(work["CreatedAt"],
                                            errors="coerce", utc=True)
                # Drop tz to compare with naive cutoff.
                parsed = parsed.dt.tz_convert(None)
                work = work[parsed >= cutoff]
            except Exception:
                pass
        if date_to and "CreatedAt" in work.columns:
            try:
                cutoff = pd.Timestamp(date_to) + pd.Timedelta(days=1)
                parsed = pd.to_datetime(work["CreatedAt"],
                                            errors="coerce", utc=True)
                parsed = parsed.dt.tz_convert(None)
                work = work[parsed < cutoff]
            except Exception:
                pass

    if work.empty:
        return {
            "matched": 0,
            "filters": {
                "order_name": order_name or None,
                "email": email or None,
                "customer": customer_filter or None,
                "date_from": date_from or None,
                "date_to": date_to or None,
            },
            "note": ("No Shopify order matches in the local sync "
                     "window. Either the order isn't from "
                     "Shopify (CIN7 SourceChannel may say "
                     "Shopify but the local mirror could be "
                     "stale — check shopify_orders_full.csv "
                     "freshness), or the filters are too "
                     "narrow. The AI should tell the user what "
                     "was searched and suggest a wider window "
                     "or different identifier."),
        }

    # Newest first.
    if "CreatedAt" in work.columns:
        work = work.assign(_d=pd.to_datetime(
            work["CreatedAt"], errors="coerce", utc=True))
        work = work.sort_values("_d", ascending=False).drop(columns="_d")

    out = []
    for _, r in work.head(limit).iterrows():
        def _txt(col):
            if (col not in work.columns
                    or not pd.notna(r.get(col))
                    or not str(r.get(col)).strip()):
                return None
            return str(r.get(col)).strip()
        out.append(_serialise_row({
            "shopify_order_id": r.get("ShopifyOrderID"),
            "name": r.get("Name"),
            "order_number": r.get("OrderNumber"),
            "created_at": _txt("CreatedAt"),
            "processed_at": _txt("ProcessedAt"),
            "financial_status": r.get("FinancialStatus"),
            "fulfillment_status": r.get("FulfillmentStatus"),
            "total_price": (
                float(r.get("TotalPrice"))
                if pd.notna(r.get("TotalPrice")) else None),
            "subtotal": (
                float(r.get("Subtotal"))
                if pd.notna(r.get("Subtotal")) else None),
            "total_tax": (
                float(r.get("TotalTax"))
                if pd.notna(r.get("TotalTax")) else None),
            "total_shipping": (
                float(r.get("TotalShipping"))
                if pd.notna(r.get("TotalShipping")) else None),
            "currency": r.get("Currency"),
            "customer": (
                f"{r.get('CustomerFirstName') or ''} "
                f"{r.get('CustomerLastName') or ''}").strip()
                or None,
            "email": r.get("Email"),
            "customer_orders_count": (
                int(r.get("CustomerOrdersCount"))
                if pd.notna(r.get("CustomerOrdersCount")) else None),
            "customer_total_spent": (
                float(r.get("CustomerTotalSpent"))
                if pd.notna(r.get("CustomerTotalSpent")) else None),
            "customer_tags": _txt("CustomerTags"),
            "tags": _txt("Tags"),
            "note": _txt("Note"),
            # CONVERSION ATTRIBUTION — the headline use case.
            "source_name": r.get("SourceName"),
            "landing_site": _txt("LandingSite"),
            "referring_site": _txt("ReferringSite"),
            "customer_locale": r.get("CustomerLocale"),
            "note_attributes": _txt("NoteAttributes"),
            "discount_codes": _txt("DiscountCodes"),
            "item_summary": _txt("ItemSummary"),
        }))

    return {
        "matched": len(out),
        "limit_applied": limit,
        "orders": out,
        "note": (
            "Show the user: order_name + customer + date + "
            "total_price as the headline. THEN surface "
            "conversion-attribution fields: source_name (where "
            "Shopify thinks it came from), referring_site (where "
            "the customer was BEFORE Shopify — google, "
            "instagram, etc.), landing_site (first storefront "
            "page), discount_codes (coupon redeemed), and "
            "note_attributes (UTM params + theme keys). "
            "customer_orders_count >= 2 means returning "
            "customer — flag that. If note_attributes contains "
            "'utm_source' or 'utm_campaign' substrings, those "
            "are the marketing-attribution params; surface "
            "them."),
    }


def get_shipping_margin(engine_df: pd.DataFrame,
                          sale_lines_df: pd.DataFrame,
                          args: dict) -> dict:
    """v2.67.55c — Shipping P&L analysis. Reads the merged
    shipments DataFrame (set via set_shipments), filters,
    computes margin per shipment, returns headline totals + worst
    rows.

    Why a dedicated tool vs reusing get_shipping_details: this one
    operates over the WHOLE filtered set (returns aggregate stats
    across thousands of shipments), whereas get_shipping_details
    is for ONE specific lookup. Different intent, different
    output shape — separate tool.
    """
    sku = (args.get("sku") or "").strip().upper()
    customer = (args.get("customer") or "").strip().upper()
    carrier = (args.get("carrier") or "").strip().lower()
    service = (args.get("service") or "").strip().lower()
    date_from = (args.get("date_from") or "").strip()
    date_to = (args.get("date_to") or "").strip()
    loss_only = bool(args.get("loss_only", False))
    margin_below = args.get("margin_below")
    limit = max(1, min(int(args.get("limit", 20) or 20), 50))

    df = _SHIPMENTS_HOLDER.get("df")
    if df is None or df.empty:
        return {
            "error": ("Shipments not loaded. ShipStation env vars "
                      "not configured OR the first sync hasn't run."),
        }

    # Detect data quality — pre-v2.67.55c CSVs lack the new column.
    if "CustomerShippingCharge" not in df.columns:
        return {
            "error": ("Shipments CSV is pre-v2.67.55c — only "
                      "carries customer_charge (mislabelled as "
                      "ShipmentCost). Margin analysis requires "
                      "true cost from /labels. Re-run "
                      "`shipstation_sync.py full --days 1825` "
                      "OR wait for the next daily sync."),
            "data_quality": "stale",
        }

    work = df.copy()
    if sku and "ItemSummary" in work.columns:
        work = work[work["ItemSummary"].astype(str).str.upper()
                     .str.contains(sku, na=False)]
    if customer and "CustomerName" in work.columns:
        work = work[work["CustomerName"].astype(str).str.upper()
                     .str.contains(customer, na=False)]
    if carrier and "CarrierCode" in work.columns:
        work = work[work["CarrierCode"].astype(str).str.lower()
                     .str.contains(carrier, na=False)]
    if service and "ServiceCode" in work.columns:
        work = work[work["ServiceCode"].astype(str).str.lower()
                     .str.contains(service, na=False)]
    if date_from and "ShipDate" in work.columns:
        try:
            cutoff = pd.Timestamp(date_from)
            parsed = pd.to_datetime(work["ShipDate"], errors="coerce",
                                       utc=True).dt.tz_convert(None)
            work = work[parsed >= cutoff]
        except Exception:
            pass
    if date_to and "ShipDate" in work.columns:
        try:
            cutoff = pd.Timestamp(date_to) + pd.Timedelta(days=1)
            parsed = pd.to_datetime(work["ShipDate"], errors="coerce",
                                       utc=True).dt.tz_convert(None)
            work = work[parsed < cutoff]
        except Exception:
            pass
    # Drop shipments without both numbers — can't compute margin.
    if "CustomerShippingCharge" in work.columns and "ShipmentCost" in work.columns:
        work = work.dropna(subset=["CustomerShippingCharge",
                                       "ShipmentCost"])
    # Drop voided.
    if "Voided" in work.columns:
        work = work[~work["Voided"].fillna(False).astype(bool)]
    # Compute margin (recompute defensively even though it's stored).
    work = work.copy()
    work["_margin"] = (
        pd.to_numeric(work["CustomerShippingCharge"], errors="coerce")
        - pd.to_numeric(work["ShipmentCost"], errors="coerce"))
    if loss_only:
        work = work[work["_margin"] < 0]
    if margin_below is not None:
        try:
            work = work[work["_margin"] < float(margin_below)]
        except (TypeError, ValueError):
            pass

    if work.empty:
        return {
            "matched": 0,
            "filters": {
                "sku": sku or None, "customer": customer or None,
                "carrier": carrier or None, "service": service or None,
                "date_from": date_from or None, "date_to": date_to or None,
                "loss_only": loss_only, "margin_below": margin_below,
            },
            "note": "No shipments match — try widening the filter.",
        }

    total_revenue = float(
        pd.to_numeric(work["CustomerShippingCharge"],
                       errors="coerce").sum())
    total_cost = float(
        pd.to_numeric(work["ShipmentCost"], errors="coerce").sum())
    net_margin = total_revenue - total_cost
    losses = work[work["_margin"] < -5]
    loss_count = int(len(losses))
    total_loss_dollars = float(losses["_margin"].sum()) if loss_count else 0.0

    # Worst rows.
    worst = work.sort_values("_margin").head(limit)
    worst_rows = []
    for _, r in worst.iterrows():
        worst_rows.append(_serialise_row({
            "order_number": r.get("OrderNumber"),
            "customer": r.get("CustomerName"),
            "ship_date": (str(r.get("ShipDate"))
                            if pd.notna(r.get("ShipDate")) else None),
            "carrier": r.get("CarrierCode"),
            "service": r.get("ServiceCode"),
            "customer_charge": (
                float(r.get("CustomerShippingCharge"))
                if pd.notna(r.get("CustomerShippingCharge")) else None),
            "actual_cost": (
                float(r.get("ShipmentCost"))
                if pd.notna(r.get("ShipmentCost")) else None),
            "margin": float(r.get("_margin")),
            "weight_value": r.get("WeightValue"),
            "weight_units": r.get("WeightUnits"),
            "dim_lwh": (
                f"{r.get('DimensionsLength')}×"
                f"{r.get('DimensionsWidth')}×"
                f"{r.get('DimensionsHeight')} "
                f"{r.get('DimensionsUnits') or ''}"
                if (r.get("DimensionsLength") and r.get("DimensionsWidth")
                    and r.get("DimensionsHeight"))
                else None),
            "tracking_number": r.get("TrackingNumber"),
            "item_summary": str(r.get("ItemSummary") or "")[:200],
        }))

    return {
        "matched": int(len(work)),
        "summary": {
            "shipments_analysed": int(len(work)),
            "total_revenue": round(total_revenue, 2),
            "total_cost": round(total_cost, 2),
            "net_margin": round(net_margin, 2),
            "loss_count": loss_count,
            "total_loss_dollars": round(total_loss_dollars, 2),
            "avg_margin": (round(net_margin / len(work), 2)
                            if len(work) else 0),
        },
        "worst_rows": worst_rows,
        "filters": {
            "sku": sku or None, "customer": customer or None,
            "carrier": carrier or None, "service": service or None,
            "date_from": date_from or None, "date_to": date_to or None,
            "loss_only": loss_only, "margin_below": margin_below,
        },
        "note": (
            "Lead with net_margin (positive = profit, negative = "
            "loss). If loss_count > 0, mention that N shipments "
            "are losing money totalling $X. Then list the worst "
            "individual losses by SKU. If the user asked about a "
            "specific SKU/family, focus on that subset."),
    }


def get_slack_messages(engine_df: pd.DataFrame,
                          sale_lines_df: pd.DataFrame,
                          args: dict) -> dict:
    """v2.67.57 — query the local mirror of ingested Slack messages.
    Same shape as get_sale_order / get_shipping_details — substring
    search + filters → list of recent messages.

    Why we need this: when staff ask the AI a question that
    references a recent Slack discussion ('did Andrew approve...?'),
    or when the AI is composing a response in Slack and could
    benefit from in-channel context, the AI calls this tool to
    grep its own message log. Same idea as cross-calling
    get_sale_order from a shipping question — adds team-knowledge
    context to the answer."""
    search = (args.get("search") or "").strip()
    channel = (args.get("channel") or "").strip().lstrip("#")
    user = (args.get("user") or "").strip().lower()
    since_hours = int(args.get("since_hours") or 168)
    limit = max(1, min(int(args.get("limit", 20) or 20), 50))

    if not (search or channel or user):
        return {
            "error": ("Specify at least one filter: search, "
                      "channel, OR user. Empty filter would dump "
                      "thousands of messages."),
        }

    since_unix = time.time() - (since_hours * 3600)

    # Build SQL.
    where_parts = ["CAST(ts AS REAL) >= ?"]
    params: list = [since_unix]
    if search:
        where_parts.append("LOWER(text) LIKE ?")
        params.append(f"%{search.lower()}%")
    if user:
        where_parts.append("LOWER(user_name) LIKE ?")
        params.append(f"%{user}%")
    if channel:
        # Match either channel_id or resolved channel name.
        # Use alias m.channel_id — the outer query aliases
        # slack_messages as m, so the bare table name isn't
        # visible inside the correlated subquery.
        where_parts.append(
            "(m.channel_id = ? OR EXISTS ("
            "SELECT 1 FROM slack_channel_cursors c "
            "WHERE c.channel_id = m.channel_id "
            "  AND LOWER(c.channel_name) = LOWER(?)))")
        params.extend([channel, channel.lstrip("#")])

    sql = (
        "SELECT m.channel_id, m.ts, m.user_name, m.text, "
        "       m.thread_ts, m.is_bot, "
        "       (SELECT channel_name FROM slack_channel_cursors c "
        "         WHERE c.channel_id = m.channel_id) AS channel_name "
        "FROM slack_messages m "
        f"WHERE {' AND '.join(where_parts)} "
        "ORDER BY ts DESC "
        "LIMIT ?"
    )
    params.append(limit)

    try:
        with db.connect() as c:
            rows = c.execute(sql, params).fetchall()
    except Exception as exc:
        return {"error": f"Slack query failed: {exc}"}

    msgs = []
    for r in rows:
        msgs.append({
            "channel": r["channel_name"] or r["channel_id"],
            "user": r["user_name"],
            "text": (r["text"] or "")[:600],
            "ts": r["ts"],
            "thread_ts": r["thread_ts"],
            "is_bot": bool(r["is_bot"]),
        })
    return {
        "matched": len(msgs),
        "search": search or None,
        "channel_filter": channel or None,
        "user_filter": user or None,
        "since_hours": since_hours,
        "messages": msgs,
        "note": (
            "Slack messages from the channels the bot watches. "
            "Use this for team context — when answering a "
            "question, mentioning that '#purchase-backorders had "
            "a thread on this last week' adds value over a pure "
            "data answer. Don't quote bot messages back at the "
            "user (is_bot=true rows are usually our own past "
            "responses)."),
    }


def get_compatible_accessories(engine_df: pd.DataFrame,
                                 sale_lines_df: pd.DataFrame,
                                 args: dict) -> dict:
    """v2.65 — compatibility lookup using Shopify accessory collections
    as the source of truth.

    The expectation: for each parent product family there's a matching
    Shopify collection titled '<Family> Accessories' (e.g. 'Slim8
    Accessories') that lists the lenses / diffusers / clips / brackets
    / connectors / end caps the team has explicitly designated as
    compatible. That curated list is far more reliable than guessing
    from product descriptions.

    Resolution:
      1. Subject = sku → resolve family from engine_df
                  family → use directly (uppercased).
      2. Read collections_index.csv (written by shopify_sync.py).
      3. Filter rows where collection_title contains both the family
         (case-insensitive) and the word 'accessories'.
      4. Group by product_handle, dedup SKUs, join with engine_df
         for stock + classification.
      5. Optional accessory_type filter classifies each row by
         keyword match in product_title (lens / diffuser / cover /
         end cap / clip / bracket / connector) and filters to the
         requested type.
      6. If no collection matched → fall back to broad text search
         across product titles for `<family> + <accessory_type>` and
         label the result `confidence: lower / source:
         text_fallback`. Per spec, NEVER guess silently.
      7. If collections_index.csv is missing entirely → return a
         clear error pointing the user at shopify_sync.py.
    """
    import csv as _csv
    from data_paths import DATA_DIR  # local import keeps ai_tools.py
                                       # decoupled from cin7 paths if
                                       # someone reuses the module.

    sku = (args.get("sku") or args.get("product")
           or args.get("product_or_family") or "").strip()
    family = (args.get("family") or "").strip().upper()
    accessory_type = (args.get("accessory_type") or "").strip().lower()
    limit = min(int(args.get("limit", 25) or 25), 50)

    # Resolve family
    if not family and sku:
        if engine_df is not None and not engine_df.empty:
            row = engine_df[engine_df["SKU"].astype(str) == sku]
            if not row.empty and "Family" in row.columns:
                family = str(row.iloc[0].get("Family") or "").strip().upper()
            elif row.empty:
                # Maybe sku is actually a family typed by the user
                family = sku.upper()
    if not family:
        return {
            "error": (
                "Need a SKU (we'll resolve to its family) or a "
                "family code directly. Pass sku= or family=. For "
                "compatibility on Slim8 try family='SLIM8'."),
        }

    # Locate the index. If it doesn't exist the whole tool can't
    # function — that's a data-availability state, not a bug.
    index_path = DATA_DIR / "shopify" / "collections_index.csv"
    if not index_path.exists():
        return {
            "error": (
                "Shopify collections_index.csv hasn't been built yet. "
                f"Expected at {index_path}. Run "
                "`python shopify_sync.py` (with collections enabled) "
                "to populate it. Until then, accessory lookup falls "
                "back to text search — explicitly call "
                "search_products_by_text if you need that path now."),
            "fallback": "text_search",
        }

    # Load + filter index. Matching rule: collection_title must
    # contain BOTH the family (case-insensitive) and the word
    # 'accessories'. So 'Slim8 Accessories' matches; 'Slim8
    # Compatible Parts' does NOT (could expand later if naming
    # convention drifts).
    fam_lower = family.lower()
    matches: list = []
    matching_collections: set = set()
    seen_skus: set = set()
    try:
        with index_path.open("r", encoding="utf-8", newline="") as fh:
            reader = _csv.DictReader(fh)
            for row in reader:
                col_title = (row.get("collection_title") or "").lower()
                if (fam_lower in col_title
                        and "accessor" in col_title):
                    sku_val = (row.get("product_sku") or "").strip()
                    if sku_val and sku_val in seen_skus:
                        continue
                    if sku_val:
                        seen_skus.add(sku_val)
                    matching_collections.add(
                        row.get("collection_title") or "")
                    matches.append({
                        "collection_title":
                            row.get("collection_title"),
                        "collection_handle":
                            row.get("collection_handle"),
                        "product_title":
                            row.get("product_title"),
                        "product_handle":
                            row.get("product_handle"),
                        "sku": sku_val,
                    })
    except Exception as exc:
        return {
            "error": f"Could not read collections_index.csv: {exc}",
        }

    # Optional accessory_type classifier — keyword match on title.
    # Returns BOTH the classification (so the AI can group/cite) and
    # uses it to filter when accessory_type was specified.
    type_keywords = {
        "lens":      ("lens", "lenses"),
        "diffuser":  ("diffuser", "diffusing"),
        "cover":     ("cover", "click cover"),
        "end_cap":   ("end cap", "endcap", "end-cap"),
        "clip":      ("clip", "mounting clip"),
        "bracket":   ("bracket", "bracketry", "mount"),
        "connector": ("connector", "connecting", "joiner",
                      "coupler"),
    }

    def _classify(title: str) -> str:
        t = (title or "").lower()
        for k, kws in type_keywords.items():
            if any(kw in t for kw in kws):
                return k
        return "other"

    for m in matches:
        m["accessory_type"] = _classify(m["product_title"])

    if accessory_type:
        # Map common synonyms onto our keys
        synonym = {
            "lenses": "lens", "diffusers": "diffuser",
            "covers": "cover", "end caps": "end_cap",
            "endcaps": "end_cap", "clips": "clip",
            "mounting clips": "clip", "brackets": "bracket",
            "connectors": "connector", "joiners": "connector",
        }
        target = synonym.get(accessory_type, accessory_type)
        matches = [m for m in matches
                    if m["accessory_type"] == target]

    # Fallback: no curated match found.
    if not matches:
        if engine_df is None or engine_df.empty:
            return {
                "subject_family": family,
                "matched": 0,
                "source": "none",
                "note": (
                    f"No '{family}' Accessories collection found in "
                    "Shopify and no engine_df to fall back on."),
            }
        # Conservative text fallback: search product titles for
        # 'family' AND (accessory_type if specified) — labelled
        # confidence=lower so the AI doesn't present this as
        # authoritative.
        df = engine_df.copy()
        if "Name" in df.columns:
            mask = df["Name"].fillna("").astype(str).str.lower(
                ).str.contains(fam_lower, na=False)
            if accessory_type:
                kws = type_keywords.get(
                    accessory_type, (accessory_type,))
                kw_mask = df["Name"].fillna("").astype(str).str.lower(
                    ).apply(lambda s: any(k in s for k in kws))
                mask = mask & kw_mask
            df = df[mask]
        else:
            df = df.head(0)
        df = df.head(limit)
        rows_out = []
        for _, r in df.iterrows():
            rec = _serialise_row(dict(r))
            rec["accessory_type"] = _classify(rec.get("Name"))
            rec["why_match"] = (
                f"product title contains '{fam_lower}'"
                + (f" + '{accessory_type}'" if accessory_type else ""))
            rows_out.append(rec)
        return {
            "subject_family": family,
            "matched": len(rows_out),
            "source": "text_fallback",
            "confidence": "lower",
            "results": rows_out,
            "note": (
                f"No '{family} Accessories' Shopify collection found. "
                "Falling back to text-search across product titles. "
                "Treat with caution — these are NOT curated "
                "compatibility designations. Recommend the buyer "
                "create a Shopify accessory collection so future "
                "lookups are authoritative."),
        }

    # Enrich curated matches with stock + classification from engine_df.
    matches = matches[:limit]
    if engine_df is not None and not engine_df.empty and "SKU" in engine_df.columns:
        eng_by_sku = {
            str(r["SKU"]): r for _, r in engine_df.iterrows()
            if pd.notna(r.get("SKU"))
        }
        for m in matches:
            if not m["sku"]:
                continue
            r = eng_by_sku.get(m["sku"])
            if r is not None:
                m["on_hand"] = (
                    None if pd.isna(r.get("OnHand"))
                    else float(r.get("OnHand")))
                m["classification"] = (
                    None if pd.isna(r.get("Classification"))
                    else str(r.get("Classification")))
                m["family"] = (
                    None if pd.isna(r.get("Family"))
                    else str(r.get("Family")))

    return {
        "subject_family": family,
        "matched": len(matches),
        "source": "shopify_collection",
        "confidence": "high",
        "matched_collections": sorted(matching_collections),
        "results": matches,
        "note": (
            "These are explicit compatibility designations from a "
            "Shopify accessory collection — authoritative."),
    }


def get_relevant_slow_stock(engine_df: pd.DataFrame,
                              sale_lines_df: pd.DataFrame,
                              args: dict) -> dict:
    """v2.66 — slow-moving / dead-stock promotion layer.

    Designed to be called AFTER the main answer of a product question.
    The system prompt tells Claude to pass the same filters used for
    the primary search (family, exclude_types, any_of_terms,
    sku_candidates) so the slow stock surfaced is genuinely related.

    Rules:
      - Only return classification IN ('slow', 'dead').
      - Only return rows with OnHand > 0 (in stock).
      - Apply the same exclude_types blocks the primary search did so
        we don't promote off-category items.
      - sku_candidates (optional) limits the slow-stock pool to a
        family-related set when the caller wants tight relevance.
      - Each returned row carries `reason_matched` + `classification`
        so the AI can render a 'Why relevant' line.
      - Caution flag set when the SKU has feedback_events of type
        'cancellation' / 'return' / 'complaint' / 'negative' (best-
        effort — non-blocking if those tables are empty).

    Returns up to limit rows. If none match, returns matched=0 — the
    caller should OMIT the slow-stock section entirely rather than
    forcing it.
    """
    family = (args.get("family") or "").strip().upper()
    family_list = args.get("family_list") or []
    if isinstance(family_list, str):
        family_list = [family_list]
    family_list = [f.strip().upper() for f in family_list if f]
    if family and family not in family_list:
        family_list = [family] + family_list

    exclude_types = args.get("exclude_types") or []
    if isinstance(exclude_types, str):
        exclude_types = [exclude_types]
    exclude_types = [str(e).strip().lower() for e in exclude_types if e]

    any_of_terms = args.get("any_of_terms") or []
    if isinstance(any_of_terms, str):
        any_of_terms = [any_of_terms]
    any_of_terms = [str(t).strip().lower() for t in any_of_terms if t]

    sku_candidates = args.get("sku_candidates") or []
    if isinstance(sku_candidates, str):
        sku_candidates = [sku_candidates]
    sku_candidates = [str(s).strip() for s in sku_candidates if s]

    intent = (args.get("intent") or "").strip()
    limit = min(int(args.get("limit", 10) or 10), 25)

    if engine_df is None or engine_df.empty:
        return {
            "matched": 0,
            "results": [],
            "note": "engine_df not loaded; cannot scan for slow stock.",
        }

    df = engine_df.copy()

    # Hard filters: classification + stock.
    if "Classification" not in df.columns:
        return {
            "matched": 0,
            "results": [],
            "note": ("Classification column not present in engine_df. "
                      "Slow-stock promotion requires the ABC engine "
                      "to have run on this dataset."),
        }
    cls_lower = df["Classification"].fillna("").astype(str).str.lower()
    df = df[cls_lower.isin(("slow", "dead"))]
    if "OnHand" in df.columns:
        df = df[df["OnHand"].fillna(0) > 0]
    if df.empty:
        return {
            "matched": 0,
            "results": [],
            "note": "No slow/dead in-stock rows in the catalog at all.",
        }

    # Family filter — accept any family in the list.
    if family_list and "Family" in df.columns:
        df = df[df["Family"].fillna("").astype(str).str.upper()
                  .isin(family_list)]

    # SKU candidate filter — most restrictive when supplied.
    if sku_candidates and "SKU" in df.columns:
        df = df[df["SKU"].astype(str).isin(sku_candidates)]

    # any_of_terms — at least one must hit Name (or Description if
    # present). Composes AND with the family / candidate filters.
    if any_of_terms:
        cols_to_search = [c for c in ("Name", "Description")
                            if c in df.columns]
        if cols_to_search:
            ok = pd.Series(False, index=df.index)
            for col in cols_to_search:
                vals = df[col].fillna("").astype(str).str.lower()
                for term in any_of_terms:
                    ok = ok | vals.str.contains(
                        term, na=False, regex=False)
            df = df[ok]

    # exclude_types — drop rows whose Name or Type matches any blocker.
    if exclude_types:
        ex_mask = pd.Series(False, index=df.index)
        for excl_col in ("Type", "Name"):
            if excl_col in df.columns:
                col_lower = (df[excl_col].fillna("").astype(str)
                              .str.lower())
                for kw in exclude_types:
                    ex_mask = ex_mask | col_lower.str.contains(
                        kw, na=False, regex=False)
        df = df[~ex_mask]

    if df.empty:
        return {
            "matched": 0,
            "results": [],
            "note": ("No relevant slow/dead in-stock items match "
                      "the supplied filters. Per the slow-stock-"
                      "promotion rule, OMIT the section from the "
                      "answer."),
        }

    # Caution check: any feedback_events flagged as cancellation /
    # return / complaint for the SKU. Best-effort — if the table is
    # empty or query fails, we skip rather than block.
    flagged_skus: set = set()
    try:
        sku_list = df["SKU"].astype(str).tolist() if "SKU" in df.columns else []
        if sku_list:
            with db.connect() as c:
                placeholders = ",".join(["?"] * len(sku_list))
                rows = c.execute(
                    f"SELECT entity_id FROM feedback_events "
                    f"WHERE entity_type = 'sku' "
                    f"AND feedback IN ('cancellation','return',"
                    f"'complaint','negative','quality_issue') "
                    f"AND entity_id IN ({placeholders})",
                    sku_list).fetchall()
                flagged_skus = {r["entity_id"] for r in rows}
    except Exception:
        pass

    df = df.head(limit)
    out_rows = []
    for _, r in df.iterrows():
        rec = _serialise_row(dict(r))
        sku = str(rec.get("SKU") or "")
        rec["caution"] = (
            ("Past return/cancellation/complaint feedback "
             "logged — handle with care")
            if sku in flagged_skus else None)
        # reason_matched is a brief human-readable note for the AI to
        # cite in 'Why relevant'.
        why_parts = []
        if rec.get("Classification"):
            why_parts.append(
                f"classified {rec['Classification']}")
        if rec.get("Family"):
            if family_list:
                why_parts.append(f"same family ({rec['Family']})")
            else:
                why_parts.append(f"family {rec['Family']}")
        if any_of_terms and rec.get("Name"):
            name_l = str(rec["Name"]).lower()
            for t in any_of_terms:
                if t in name_l:
                    why_parts.append(f"name contains '{t}'")
                    break
        rec["reason_matched"] = "; ".join(why_parts) or "filter match"
        out_rows.append(rec)

    return {
        "matched": len(out_rows),
        "results": out_rows,
        "intent": intent or None,
        "filters": {
            "family_list": family_list,
            "any_of_terms": any_of_terms,
            "exclude_types": exclude_types,
            "sku_candidates": sku_candidates,
        },
        "note": (
            "Slow-stock promotion candidates. Caller should render "
            "these in a SEPARATE 'Slow-moving stock worth offering' "
            "section AFTER the main answer, never replacing it. Each "
            "row's `reason_matched` gives the relevance rationale; "
            "`caution` (if non-null) should be surfaced to the user "
            "alongside the SKU."),
    }


# v2.67 — find_products lives in product_search.py to keep ai_tools.py
# focused on schemas + dispatch. Imported lazily inside the handler so
# a parse error in product_search doesn't crash this whole module at
# import time.
def find_products(engine_df: pd.DataFrame,
                   sale_lines_df: pd.DataFrame,
                   args: dict) -> dict:
    """Thin wrapper around product_search.find_products (v2.67)."""
    from product_search import find_products as _impl
    return _impl(engine_df, sale_lines_df, args)


# ---------------------------------------------------------------------------
# v2.67.95 — Marketing intelligence handlers
# ---------------------------------------------------------------------------
def _resolve_sku_family(args: dict) -> tuple:
    """Common helper: pull sku/family/handle from args. Returns
    (sku, family, handle). Either may be None."""
    sku = (args.get("sku") or "").strip() or None
    family = (args.get("family") or "").strip().upper() or None
    handle = (args.get("shopify_handle") or "").strip() or None
    # If SKU given but no family, derive family from SKU prefix.
    if sku and not family:
        s = sku.upper()
        if s.startswith("LED-"):
            parts = s.split("-")
            if len(parts) >= 2:
                family = parts[1]
        elif s.startswith("LEDKIT-"):
            parts = s.split("-")
            if len(parts) >= 2:
                family = f"KIT-{parts[1]}"
    return sku, family, handle


def get_email_attribution(engine_df: pd.DataFrame,
                              sale_lines_df: pd.DataFrame,
                              args: dict) -> dict:
    """Surface Klaviyo email campaigns that touched a SKU."""
    sku, family, handle = _resolve_sku_family(args)
    days = int(args.get("days") or 90)
    if not (sku or family or handle):
        return {
            "error": "Specify sku, family, or shopify_handle.",
        }

    # email_campaign_skus stores by sku (which today is shopify_handle
    # for klaviyo since we don't have variant-level mapping). Try
    # multiple keys.
    rows = []
    keys_tried = []
    for key in (sku, handle):
        if not key:
            continue
        keys_tried.append(key)
        try:
            r = db.get_email_attribution_for_sku(key, days=days)
            if r:
                rows.extend(r)
        except Exception as exc:
            return {"error": f"DB query failed: {exc}"}

    if not rows and family:
        # Fall back: any campaign click on a product in this family
        try:
            with db.connect() as c:
                fam_rows = c.execute(
                    "SELECT ec.id, ec.name, ec.subject, ec.sent_at, "
                    "       ec.recipients, ec.open_rate, "
                    "       ec.click_rate, "
                    "       ec.revenue AS campaign_revenue, "
                    "       ecs.click_count, ecs.unique_clicks, "
                    "       ecs.attributed_revenue AS sku_revenue, "
                    "       ecs.sku, ecs.shopify_handle "
                    "FROM email_campaign_skus ecs "
                    "JOIN email_campaigns ec "
                    "  ON ec.id = ecs.campaign_id "
                    "WHERE ecs.family = ? "
                    "  AND ec.sent_at >= datetime('now', "
                    "                                '-' || ? || ' days') "
                    "ORDER BY ec.sent_at DESC",
                    (family, days)).fetchall()
            rows = [dict(r) for r in fam_rows]
        except Exception as exc:
            return {"error": f"DB query failed: {exc}"}

    return {
        "matched": len(rows),
        "sku": sku,
        "family": family,
        "shopify_handle": handle,
        "lookback_days": days,
        "campaigns": rows[:25],
        "note": (
            "Klaviyo-attributed campaigns. click_count is total "
            "clicks on the product link; unique_clicks is unique "
            "recipients who clicked. attributed_revenue may be null "
            "if Klaviyo didn't surface per-product revenue for that "
            "campaign."),
    }


def get_seo_signals(engine_df: pd.DataFrame,
                       sale_lines_df: pd.DataFrame,
                       args: dict) -> dict:
    """Surface SEMrush ranking observations for a SKU/family."""
    sku, family, _handle = _resolve_sku_family(args)
    days = int(args.get("days") or 30)
    if not (sku or family):
        return {
            "error": "Specify sku or family.",
        }

    rows = []
    try:
        if sku:
            rows = db.get_seo_signals_for_sku(sku, days=days)
        if not rows and family:
            rows = db.get_seo_signals_for_family(family, days=days)
    except Exception as exc:
        return {"error": f"DB query failed: {exc}"}

    # For nicer output, group by keyword and show position trend
    by_keyword: dict = {}
    for r in rows:
        kw = r.get("keyword") or ""
        existing = by_keyword.setdefault(kw, {
            "keyword": kw,
            "url": r.get("url"),
            "search_volume": r.get("search_volume"),
            "observations": [],
        })
        existing["observations"].append({
            "captured_at": r.get("captured_at"),
            "position": r.get("position"),
            "previous_position": r.get("previous_position"),
        })

    keywords = list(by_keyword.values())
    keywords.sort(key=lambda k: (
        min((o.get("position") or 999.0)
              for o in k["observations"]),
        -(k.get("search_volume") or 0)))

    return {
        "matched": len(keywords),
        "sku": sku,
        "family": family,
        "lookback_days": days,
        "keywords": keywords[:25],
        "note": (
            "Position 1 = top of organic results. Lower number = "
            "better. previous_position is from the last week's "
            "pull. Sorted by best current position then volume."),
    }


def get_product_reviews(engine_df: pd.DataFrame,
                          sale_lines_df: pd.DataFrame,
                          args: dict) -> dict:
    """Surface review summary + recent reviews for a SKU."""
    sku = (args.get("sku") or "").strip()
    include_recent = args.get("include_recent")
    if include_recent is None:
        include_recent = True
    if not sku:
        return {"error": "sku is required"}

    try:
        summary = db.get_reviews_summary_for_sku(sku)
        recent = (db.get_recent_reviews_for_sku(sku, limit=5)
                    if include_recent else [])
    except Exception as exc:
        return {"error": f"DB query failed: {exc}"}

    if not summary or not summary.get("count"):
        return {
            "sku": sku,
            "matched": 0,
            "note": "No reviews on file for this SKU.",
        }

    # Trim review bodies for compact response
    trimmed = []
    for r in recent:
        rev = dict(r)
        if rev.get("body") and len(rev["body"]) > 400:
            rev["body"] = rev["body"][:400] + "..."
        trimmed.append(rev)

    return {
        "sku": sku,
        "summary": {
            "count": summary.get("count"),
            "avg_rating": summary.get("avg_rating"),
            "low_star_count": summary.get("low_count"),
            "high_star_count": summary.get("high_count"),
            "latest_review": summary.get("latest_review"),
        },
        "recent_reviews": trimmed,
        "note": (
            "low_star_count = ratings 1-2 (warning signs); "
            "high_star_count = ratings 4-5. Recent reviews shown "
            "verbatim (truncated to 400 chars). Use these to "
            "qualify a buyer recommendation."),
    }


def get_marketing_intelligence(engine_df: pd.DataFrame,
                                  sale_lines_df: pd.DataFrame,
                                  args: dict) -> dict:
    """Composite: SEO + email + reviews + (Phase 2) ads in one call.
    Cheaper than 4 separate tool calls when the user asks an
    open-ended 'what's happening with this SKU' question."""
    sku, family, handle = _resolve_sku_family(args)
    days = int(args.get("days") or 30)

    if not (sku or family or handle):
        return {
            "error": "Specify sku, family, or shopify_handle.",
        }

    out: dict = {
        "sku": sku,
        "family": family,
        "shopify_handle": handle,
        "lookback_days": days,
    }

    # SEO signals
    try:
        if sku:
            seo_rows = db.get_seo_signals_for_sku(sku, days=days)
        elif family:
            seo_rows = db.get_seo_signals_for_family(family,
                                                          days=days)
        else:
            seo_rows = []
        out["seo"] = {
            "observations": len(seo_rows),
            "top_movements": seo_rows[:10],
        }
    except Exception as exc:
        out["seo"] = {"error": f"{exc}"}

    # Email attribution
    try:
        email_rows = []
        for k in (sku, handle):
            if not k:
                continue
            email_rows.extend(
                db.get_email_attribution_for_sku(k, days=days * 3))
        out["email"] = {
            "campaigns": len(email_rows),
            "recent": email_rows[:5],
        }
    except Exception as exc:
        out["email"] = {"error": f"{exc}"}

    # Reviews
    try:
        if sku:
            summary = db.get_reviews_summary_for_sku(sku)
            recent = db.get_recent_reviews_for_sku(sku, limit=3)
        else:
            summary = {}
            recent = []
        if summary and summary.get("count"):
            out["reviews"] = {
                "count": summary.get("count"),
                "avg_rating": summary.get("avg_rating"),
                "low_star_count": summary.get("low_count"),
                "high_star_count": summary.get("high_count"),
                "recent": [
                    {"rating": r.get("rating"),
                     "title": r.get("title"),
                     "body": (r.get("body") or "")[:200],
                     "review_date": r.get("review_date")}
                    for r in recent],
            }
        else:
            out["reviews"] = {"count": 0,
                               "note": "no reviews on file"}
    except Exception as exc:
        out["reviews"] = {"error": f"{exc}"}

    # Ad attribution (Phase 2 onwards — populated by
    # google_ads_sync.py + ga4_sync.py)
    try:
        if sku:
            ads = db.get_ad_attribution_for_sku(sku, days=days)
            out["ads"] = {
                "campaigns": len(ads),
                "rows": ads[:5],
                "note": ("populated by google_ads + ga4 syncs "
                            "(Phase 2)"),
            }
    except Exception as exc:
        out["ads"] = {"error": f"{exc}"}

    return out


# ---------------------------------------------------------------------------
# v2.67.102 — Campaign-level Moby-replacement handlers
# ---------------------------------------------------------------------------

def _ad_table_diagnostics(platform: str = "google_ads") -> dict:
    """v2.67.122 — when a get_ad_overview call returns 0 rows, the
    bot used to parrot a misleading note ('sync isn't configured').
    That's wrong — empty can mean (a) the table doesn't exist,
    (b) the table is empty, (c) data exists but for a different
    date window than queried, OR (d) data exists for a different
    platform string. This helper surfaces the truth so the bot can
    give an accurate answer to the user."""
    out: dict = {
        "table_exists": False,
        "total_rows_all_time": 0,
        "latest_date": None,
        "earliest_date": None,
        "platforms_in_table": [],
    }
    try:
        with db.connect() as c:
            # Check existence first; a fresh DB on a service that
            # hasn't run the sync yet won't have the table.
            t = c.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='ad_campaigns_daily'"
            ).fetchone()
            if not t:
                return out
            out["table_exists"] = True
            r = c.execute(
                "SELECT COUNT(*) AS n, "
                "       MAX(date) AS latest, "
                "       MIN(date) AS earliest "
                "FROM ad_campaigns_daily"
            ).fetchone()
            out["total_rows_all_time"] = int((r["n"] or 0))
            out["latest_date"] = r["latest"]
            out["earliest_date"] = r["earliest"]
            plats = c.execute(
                "SELECT DISTINCT platform FROM ad_campaigns_daily"
            ).fetchall()
            out["platforms_in_table"] = sorted(
                {(p["platform"] or "").strip() for p in plats
                  if (p["platform"] or "").strip()})
    except Exception:
        # Returning the partial dict is still useful — the bot can
        # at least say 'I can't see the table' instead of 'sync
        # is not configured'.
        pass
    return out


def _ad_overview_empty_note(diag: dict, days: int,
                                  platform: str) -> str:
    """v2.67.122 — pick the most accurate explanation for an empty
    result. Replaces the old single-string note."""
    if not diag.get("table_exists"):
        return (f"The ad_campaigns_daily table doesn't exist in "
                  f"this database. Google Ads data is collected on "
                  f"the worker; if you're querying from the "
                  f"dashboard, the worker's data may not have "
                  f"propagated yet (cross-service drift — pending "
                  f"v2.68 Postgres migration). Try the Slack bot "
                  f"for ad questions until then.")
    if diag.get("total_rows_all_time", 0) == 0:
        return ("Table exists but is empty — google_ads_sync has "
                "not written any rows yet. Check the worker logs.")
    latest = diag.get("latest_date")
    plats = diag.get("platforms_in_table") or []
    if platform != "all" and platform not in plats:
        return (f"No rows match platform='{platform}'. Platforms "
                  f"present in the table: {plats}. Possible name "
                  f"mismatch (case, underscore vs space, etc.).")
    return (f"Data exists through {latest} but no rows match the "
              f"last {days}-day window for platform='{platform}'. "
              f"Earliest row: {diag.get('earliest_date')}. Sync "
              f"may have stopped — check worker logs.")


def _ad_summary_query(days: int, platform: str = "google_ads") -> dict:
    """Common base aggregator for ad-campaign analytics."""
    p_filter = "" if platform == "all" else "AND platform = ?"
    params = [days]
    if platform != "all":
        params.append(platform)
    sql = (
        "SELECT platform, "
        "       COUNT(DISTINCT campaign_id) AS n_campaigns, "
        "       ROUND(SUM(spend), 2) AS total_spend, "
        "       ROUND(SUM(revenue_ga4), 2) AS ga4_revenue, "
        "       ROUND(SUM(revenue_platform), 2) AS platform_revenue, "
        "       ROUND(SUM(conv_ga4), 0) AS ga4_conversions, "
        "       ROUND(SUM(conv_platform), 0) AS platform_conversions, "
        "       SUM(impressions) AS impressions, "
        "       SUM(clicks) AS clicks "
        "FROM ad_campaigns_daily "
        "WHERE date >= date('now', '-' || ? || ' days') "
        f"  {p_filter} "
        "GROUP BY platform")
    with db.connect() as c:
        rows = c.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_ad_overview(engine_df: pd.DataFrame,
                       sale_lines_df: pd.DataFrame,
                       args: dict) -> dict:
    """Top-level paid-marketing summary."""
    days = int(args.get("days") or 30)
    platform = (args.get("platform") or "google_ads").strip().lower()
    rows = _ad_summary_query(days, platform=platform)
    if not rows:
        # v2.67.122 — surface table diagnostics instead of a
        # single misleading note. The bot can now distinguish
        # 'sync never ran' from 'data exists for a different
        # window' from 'platform name mismatch'.
        diag = _ad_table_diagnostics(platform=platform)
        return {
            "matched": 0,
            "diagnostics": diag,
            "queried_window_days": days,
            "queried_platform": platform,
            "note": _ad_overview_empty_note(diag, days, platform),
        }

    # Compute ROAS and add top campaigns per platform
    out_rows = []
    for r in rows:
        spend = r.get("total_spend") or 0
        ga4 = r.get("ga4_revenue") or 0
        plat = r.get("platform_revenue") or 0
        out_rows.append({
            **r,
            "ga4_roas": round(ga4 / spend, 2) if spend else None,
            "platform_roas": round(plat / spend, 2) if spend else None,
            "platform_inflation_ratio": (
                round(plat / ga4, 2) if ga4 else None),
        })

    # Top 5 campaigns by spend
    top_sql = (
        "SELECT platform, campaign_id, campaign_name, campaign_type, "
        "       ROUND(SUM(spend), 2) AS spend, "
        "       ROUND(SUM(revenue_ga4), 2) AS ga4_revenue, "
        "       ROUND(SUM(revenue_platform), 2) AS platform_revenue "
        "FROM ad_campaigns_daily "
        "WHERE date >= date('now', '-' || ? || ' days') "
        + ("AND platform = ?" if platform != "all" else "")
        + " GROUP BY platform, campaign_id "
        "ORDER BY spend DESC LIMIT 5")
    p2 = [days]
    if platform != "all":
        p2.append(platform)
    with db.connect() as c:
        top_rows = [dict(r) for r in c.execute(top_sql, p2).fetchall()]

    for tr in top_rows:
        sp = tr.get("spend") or 0
        g4 = tr.get("ga4_revenue") or 0
        tr["ga4_roas"] = round(g4 / sp, 2) if sp else None

    return {
        "lookback_days": days,
        "platform_filter": platform,
        "by_platform": out_rows,
        "top_5_by_spend": top_rows,
        "note": (
            "ga4_roas uses GA4 data-driven attribution (the "
            "trustworthy ROAS for budget decisions). "
            "platform_inflation_ratio > 1.5 means the platform's "
            "self-report is meaningfully more optimistic than "
            "GA4."),
    }


def get_campaign_performance(engine_df: pd.DataFrame,
                                sale_lines_df: pd.DataFrame,
                                args: dict) -> dict:
    days = int(args.get("days") or 30)
    platform = (args.get("platform") or "google_ads").strip().lower()
    campaign_type = (args.get("campaign_type") or "").strip().upper()
    sort_by = (args.get("sort_by") or "spend").strip().lower()
    limit = max(1, min(int(args.get("limit") or 25), 100))

    where_parts = ["date >= date('now', '-' || ? || ' days')"]
    params = [days]
    if platform != "all":
        where_parts.append("platform = ?")
        params.append(platform)
    if campaign_type:
        where_parts.append("UPPER(campaign_type) LIKE ?")
        params.append(f"%{campaign_type}%")

    sort_sql = {
        "spend": "spend DESC",
        "ga4_roas": "ga4_roas DESC NULLS LAST",
        "platform_roas": "platform_roas DESC NULLS LAST",
        "ga4_revenue": "ga4_revenue DESC",
    }.get(sort_by, "spend DESC")

    sql = (
        "SELECT platform, campaign_id, campaign_name, campaign_type, "
        "       ROUND(SUM(spend), 2) AS spend, "
        "       SUM(impressions) AS impressions, "
        "       SUM(clicks) AS clicks, "
        "       ROUND(SUM(conv_ga4), 0) AS ga4_conversions, "
        "       ROUND(SUM(conv_platform), 0) AS platform_conversions, "
        "       ROUND(SUM(revenue_ga4), 2) AS ga4_revenue, "
        "       ROUND(SUM(revenue_platform), 2) AS platform_revenue, "
        "       CASE WHEN SUM(spend) > 0 THEN "
        "         ROUND(SUM(revenue_ga4) / SUM(spend), 2) "
        "       ELSE NULL END AS ga4_roas, "
        "       CASE WHEN SUM(spend) > 0 THEN "
        "         ROUND(SUM(revenue_platform) / SUM(spend), 2) "
        "       ELSE NULL END AS platform_roas, "
        "       CASE WHEN SUM(clicks) > 0 THEN "
        "         ROUND(SUM(spend) / SUM(clicks), 2) "
        "       ELSE NULL END AS cpc "
        "FROM ad_campaigns_daily "
        f"WHERE {' AND '.join(where_parts)} "
        "GROUP BY platform, campaign_id "
        f"ORDER BY {sort_sql} "
        "LIMIT ?")
    params.append(limit)

    with db.connect() as c:
        rows = c.execute(sql, params).fetchall()

    return {
        "matched": len(rows),
        "lookback_days": days,
        "filters": {
            "platform": platform,
            "campaign_type": campaign_type or None,
            "sort_by": sort_by,
        },
        "campaigns": [dict(r) for r in rows],
        "note": (
            "ga4_roas is the trustworthy number (data-driven "
            "attribution). platform_roas often inflates due to "
            "view-through credit. CPC = cost per click."),
    }


def find_campaigns_to_cut(engine_df: pd.DataFrame,
                              sale_lines_df: pd.DataFrame,
                              args: dict) -> dict:
    days = int(args.get("days") or 30)
    min_roas = float(args.get("min_roas") or 2.0)
    min_spend = float(args.get("min_spend") or 100.0)

    sql = (
        "SELECT platform, campaign_id, campaign_name, campaign_type, "
        "       ROUND(SUM(spend), 2) AS spend, "
        "       ROUND(SUM(revenue_ga4), 2) AS ga4_revenue, "
        "       ROUND(SUM(revenue_platform), 2) AS platform_revenue, "
        "       CASE WHEN SUM(spend) > 0 THEN "
        "         ROUND(SUM(revenue_ga4) / SUM(spend), 2) "
        "       ELSE NULL END AS ga4_roas "
        "FROM ad_campaigns_daily "
        "WHERE date >= date('now', '-' || ? || ' days') "
        "GROUP BY platform, campaign_id "
        "HAVING SUM(spend) >= ? "
        "   AND (SUM(revenue_ga4) / NULLIF(SUM(spend), 0)) < ? "
        "ORDER BY ga4_roas ASC, spend DESC")

    with db.connect() as c:
        rows = c.execute(sql, (days, min_spend, min_roas)).fetchall()

    return {
        "matched": len(rows),
        "lookback_days": days,
        "filters": {
            "min_roas_threshold": min_roas,
            "min_spend_floor": min_spend,
        },
        "underperformers": [dict(r) for r in rows],
        "recommendation": (
            f"Consider pausing or reducing budget on these "
            f"{len(rows)} campaigns. ROAS below {min_roas}x means "
            f"every $1 spent earns less than ${min_roas:.2f} back. "
            f"Sorted worst-first."),
    }


def find_campaigns_to_scale(engine_df: pd.DataFrame,
                                sale_lines_df: pd.DataFrame,
                                args: dict) -> dict:
    days = int(args.get("days") or 30)
    min_roas = float(args.get("min_roas") or 4.0)
    min_spend = float(args.get("min_spend") or 500.0)

    sql = (
        "SELECT platform, campaign_id, campaign_name, campaign_type, "
        "       ROUND(SUM(spend), 2) AS spend, "
        "       ROUND(SUM(revenue_ga4), 2) AS ga4_revenue, "
        "       ROUND(SUM(revenue_platform), 2) AS platform_revenue, "
        "       CASE WHEN SUM(spend) > 0 THEN "
        "         ROUND(SUM(revenue_ga4) / SUM(spend), 2) "
        "       ELSE NULL END AS ga4_roas, "
        "       COUNT(DISTINCT date) AS active_days "
        "FROM ad_campaigns_daily "
        "WHERE date >= date('now', '-' || ? || ' days') "
        "GROUP BY platform, campaign_id "
        "HAVING SUM(spend) >= ? "
        "   AND (SUM(revenue_ga4) / NULLIF(SUM(spend), 0)) >= ? "
        "ORDER BY ga4_roas DESC, spend DESC")

    with db.connect() as c:
        rows = c.execute(sql, (days, min_spend, min_roas)).fetchall()

    return {
        "matched": len(rows),
        "lookback_days": days,
        "filters": {
            "min_roas_threshold": min_roas,
            "min_spend_floor": min_spend,
        },
        "overperformers": [dict(r) for r in rows],
        "recommendation": (
            f"These {len(rows)} campaigns have GA4 ROAS above "
            f"{min_roas}x AND consistent spend (>${min_spend:.0f} "
            f"in window). Candidates for budget increases. Test "
            f"+20% increments and re-measure ROAS over a 14-day "
            f"window before committing further."),
    }


def attribution_sanity_check(engine_df: pd.DataFrame,
                                 sale_lines_df: pd.DataFrame,
                                 args: dict) -> dict:
    days = int(args.get("days") or 30)
    campaign_id = (args.get("campaign_id") or "").strip()
    min_ratio = float(args.get("min_inflation_ratio") or 1.5)

    where_parts = ["date >= date('now', '-' || ? || ' days')"]
    params = [days]
    if campaign_id:
        where_parts.append("campaign_id = ?")
        params.append(campaign_id)

    sql = (
        "SELECT platform, campaign_id, campaign_name, campaign_type, "
        "       ROUND(SUM(spend), 2) AS spend, "
        "       ROUND(SUM(revenue_platform), 2) AS platform_revenue, "
        "       ROUND(SUM(revenue_ga4), 2) AS ga4_revenue, "
        "       CASE WHEN SUM(revenue_ga4) > 0 THEN "
        "         ROUND(SUM(revenue_platform) / SUM(revenue_ga4), 2) "
        "       ELSE NULL END AS inflation_ratio "
        "FROM ad_campaigns_daily "
        f"WHERE {' AND '.join(where_parts)} "
        "GROUP BY platform, campaign_id "
        "HAVING SUM(spend) > 0 ")
    if not campaign_id:
        sql += ("AND (SUM(revenue_platform) / NULLIF(SUM(revenue_ga4), "
                 "0)) >= ? ")
        params.append(min_ratio)
    sql += "ORDER BY inflation_ratio DESC LIMIT 25"

    with db.connect() as c:
        rows = c.execute(sql, params).fetchall()

    return {
        "matched": len(rows),
        "lookback_days": days,
        "min_inflation_ratio": min_ratio,
        "campaigns": [dict(r) for r in rows],
        "note": (
            "inflation_ratio > 1.0 means the platform reports more "
            "revenue than GA4. >1.5 = 50% inflation, often from "
            "view-through credits. Use GA4 as the truthful baseline "
            "for budget decisions; the platform's number is for "
            "context only."),
    }


def get_sku_ad_spend(engine_df: pd.DataFrame,
                        sale_lines_df: pd.DataFrame,
                        args: dict) -> dict:
    """v2.67.105 — per-SKU Google Ads spend + ROAS.
    Answers: 'what did we spend on ads for LED-X', 'which campaigns
    target this SKU', 'is this SKU profitable to advertise'."""
    sku = (args.get("sku") or "").strip()
    days = int(args.get("days") or 30)
    if not sku:
        return {"error": "sku is required"}

    try:
        summary = db.get_sku_ad_summary(sku, days=days)
        per_campaign = db.get_ad_attribution_for_sku(sku, days=days)
    except Exception as exc:
        return {"error": f"DB query failed: {exc}"}

    if not summary or not summary.get("total_spend"):
        return {
            "sku": sku,
            "lookback_days": days,
            "total_spend": 0,
            "total_revenue": 0,
            "matched": 0,
            "note": (
                "No advertising spend on this SKU in window. "
                "Either it's not in any active Shopping/PMax "
                "campaign, or shopping_performance_view hasn't "
                "been synced yet (run "
                "google_ads_sync.py per-sku-backfill)."),
        }

    return {
        "sku": sku,
        "lookback_days": days,
        "summary": {
            "total_spend": round(summary.get("total_spend") or 0, 2),
            "total_revenue":
                round(summary.get("total_revenue") or 0, 2),
            "roas": summary.get("roas"),
            "cpc": summary.get("cpc"),
            "total_clicks": summary.get("total_clicks") or 0,
            "total_impressions":
                summary.get("total_impressions") or 0,
            "total_purchases":
                summary.get("total_purchases") or 0,
            "n_campaigns_targeting":
                summary.get("n_campaigns") or 0,
            "earliest_date": summary.get("earliest"),
            "latest_date": summary.get("latest"),
        },
        "by_campaign": per_campaign[:25],
        "note": (
            "spend / clicks / impressions are from Google Ads' "
            "shopping_performance_view (per-SKU). revenue / "
            "purchases are from GA4's per-SKU attribution. ROAS "
            "uses both. >2x = profitable, <1x = losing money. "
            "If spend > 0 but revenue = 0, the campaign is "
            "burning budget on a SKU customers aren't buying."),
    }


def compare_ad_periods(engine_df: pd.DataFrame,
                          sale_lines_df: pd.DataFrame,
                          args: dict) -> dict:
    cur_days = int(args.get("current_days") or 30)
    back_days = int(args.get("compare_to_days_ago") or (cur_days * 2))

    # Current window: last cur_days
    sql_cur = (
        "SELECT platform, "
        "       ROUND(SUM(spend), 2) AS spend, "
        "       ROUND(SUM(revenue_ga4), 2) AS ga4_revenue, "
        "       ROUND(SUM(revenue_platform), 2) AS platform_revenue, "
        "       COUNT(DISTINCT campaign_id) AS n_campaigns "
        "FROM ad_campaigns_daily "
        "WHERE date >= date('now', '-' || ? || ' days') "
        "GROUP BY platform")
    # Compare window: from -back_days to -(back_days - cur_days)
    older = back_days
    newer = back_days - cur_days
    sql_compare = (
        "SELECT platform, "
        "       ROUND(SUM(spend), 2) AS spend, "
        "       ROUND(SUM(revenue_ga4), 2) AS ga4_revenue, "
        "       ROUND(SUM(revenue_platform), 2) AS platform_revenue, "
        "       COUNT(DISTINCT campaign_id) AS n_campaigns "
        "FROM ad_campaigns_daily "
        "WHERE date >= date('now', '-' || ? || ' days') "
        "  AND date < date('now', '-' || ? || ' days') "
        "GROUP BY platform")
    with db.connect() as c:
        cur_rows = c.execute(sql_cur, (cur_days,)).fetchall()
        cmp_rows = c.execute(sql_compare, (older, newer)).fetchall()

    cur_dict = {r["platform"]: dict(r) for r in cur_rows}
    cmp_dict = {r["platform"]: dict(r) for r in cmp_rows}

    out = []
    for plat in sorted(set(list(cur_dict.keys()) + list(cmp_dict.keys()))):
        a = cur_dict.get(plat, {})
        b = cmp_dict.get(plat, {})

        def _delta(field):
            av = a.get(field) or 0
            bv = b.get(field) or 0
            d = av - bv
            pct = (d / bv * 100) if bv else None
            return {"current": av, "compare": bv,
                      "delta": round(d, 2),
                      "pct_change": round(pct, 1) if pct is not None
                                       else None}
        out.append({
            "platform": plat,
            "spend": _delta("spend"),
            "ga4_revenue": _delta("ga4_revenue"),
            "platform_revenue": _delta("platform_revenue"),
            "n_campaigns": _delta("n_campaigns"),
        })

    return {
        "current_window_days": cur_days,
        "compare_window_days_ago": back_days,
        "by_platform": out,
        "note": (
            "current = last N days. compare = N days ending "
            "'compare_to_days_ago' days ago. pct_change is "
            "(current-compare)/compare. Negative pct_change in "
            "spend with positive in revenue = improving "
            "efficiency."),
    }


def search_team_playbooks(engine_df: pd.DataFrame,
                            sale_lines_df: pd.DataFrame,
                            args: dict) -> dict:
    """v2.67.250 — search the local mirror of Notion playbooks +
    FAQs. Returns up to 5 matching articles with content excerpts
    + the Notion URL so the AI can cite source.

    v2.67.261 — renamed from search_knowledge_base (collided with
    the on-disk ai_kb tool; duplicate tool names break the API)."""
    query = (args.get("query") or "").strip()
    if not query:
        return {"matched": 0,
                "note": "Provide a 'query' string."}
    try:
        articles = db.search_kb_articles(query, limit=5)
    except Exception as exc:  # noqa: BLE001
        return {"error": f"KB search failed: {exc}"}
    if not articles:
        return {
            "matched": 0,
            "query": query,
            "note": ("No knowledge-base article matched. "
                     "Fall back to your normal reasoning, or "
                     "ask the user to add a Notion page for "
                     "this topic."),
        }
    out = []
    for a in articles:
        content = (a.get("content_md") or "")[:2000]
        out.append({
            "title": a.get("title"),
            "url": a.get("url"),
            "category": a.get("category"),
            "content": content,
            "notion_edited_at": a.get("notion_edited_at"),
            "truncated": (
                bool((a.get("content_md") or "")[2000:])),
        })
    return {
        "matched": len(out),
        "query": query,
        "results": out,
        "note": ("ALWAYS cite the article URL when you ground an "
                 "answer in one of these results. Content is "
                 "truncated to 2000 chars per result — the URL "
                 "is the full source."),
    }


def get_product_dimensions(engine_df: pd.DataFrame,
                            sale_lines_df: pd.DataFrame,
                            args: dict) -> dict:
    """v2.67.281 — look up LED channel / profile cross-section
    dimensions from the product_dimensions table. That table is
    the local mirror of Notion's 'Product Dimensions' page (the
    source of truth, refreshed by notion_sync pull-product-
    dimensions). Accepts engine_df/sale_lines_df for a uniform
    tool signature even though they're unused."""
    query = (args.get("query") or "").strip()
    if not query:
        return {"error": "query is required"}
    try:
        rows = db.search_product_dimensions(query, limit=10)
    except Exception as exc:  # noqa: BLE001
        return {"error": f"dimension lookup failed: {exc}"}
    if not rows:
        return {
            "matched": 0,
            "query": query,
            "note": ("No product matched. Dimensions are "
                     "catalogued only for LED channels/profiles "
                     "whose Shopify spec diagram was extracted — "
                     "do NOT guess or estimate dimensions; tell "
                     "the user it hasn't been catalogued."),
        }
    out = []
    for r in rows:
        out.append({
            "product": r.get("title"),
            "handle": r.get("shopify_handle"),
            "family": r.get("family"),
            "outer_width_mm": r.get("outer_width_mm"),
            "outer_height_mm": r.get("outer_height_mm"),
            "channel_width_mm": r.get("channel_width_mm"),
            "channel_depth_mm": r.get("channel_depth_mm"),
            "max_strip_width_mm": r.get("max_strip_width_mm"),
            "wing_count": r.get("wing_count"),
            "wing_width_mm": r.get("wing_width_mm"),
            "mounting_type": r.get("mounting_type"),
            "profile_shape": r.get("profile_shape"),
            "notes": r.get("extra_notes"),
        })
    return {
        "matched": len(out),
        "query": query,
        "results": out,
        "note": ("All values in millimetres. Outer = total "
                 "external profile size; channel = the LED-strip "
                 "recess. Source: the Notion 'Product Dimensions' "
                 "page. If a value is null it wasn't on the spec "
                 "diagram — say so rather than guessing."),
    }


TOOL_HANDLERS = {
    "search_products": search_products,
    "search_products_by_text": search_products_by_text,
    "find_products": find_products,
    "find_similar_products": find_similar_products,
    "get_incoming_stock": get_incoming_stock,
    # v2.67.51 — transaction-lookup tools.
    "get_purchase_order": get_purchase_order,
    "get_sale_order": get_sale_order,
    # v2.67.179 — live CIN7 fallback for fresh orders.
    "get_sale_live": get_sale_live,
    # v2.67.196 — live CIN7 fallback for fresh POs.
    "get_purchase_live": get_purchase_live,
    "get_stock_adjustment": get_stock_adjustment,
    # v2.67.54 — ShipStation lookup.
    "get_shipping_details": get_shipping_details,
    # v2.67.55c — Shipping P&L analysis.
    "get_shipping_margin": get_shipping_margin,
    # v2.67.55 — Shopify conversion attribution.
    "get_shopify_order": get_shopify_order,
    # v2.67.57 — Slack team-context lookup.
    "get_slack_messages": get_slack_messages,
    "get_compatible_accessories": get_compatible_accessories,
    # v2.66.6: get_relevant_slow_stock UNREGISTERED. Slow-stock
    # promotion was breaking product-list queries. Function stays in
    # the file for tomorrow's rebuild as a separate post-answer call.
    # "get_relevant_slow_stock": get_relevant_slow_stock,
    "get_sku_details": get_sku_details,
    "get_velocity": get_velocity,
    "get_dead_stock": get_dead_stock,
    "get_migration_chain": get_migration_chain,
    "get_sales_totals": get_sales_totals,
    "get_recent_signals": get_recent_signals,
    "get_top_inquired_products": get_top_inquired_products,
    "get_rising_demand": get_rising_demand,
    "get_demand_score": get_demand_score,
    "search_knowledge_base": search_knowledge_base,
    # v2.67.95 — marketing intelligence layer
    "get_email_attribution": get_email_attribution,
    "get_seo_signals": get_seo_signals,
    "get_product_reviews": get_product_reviews,
    "get_marketing_intelligence": get_marketing_intelligence,
    # v2.67.102 — campaign-level Moby-replacement
    "get_ad_overview": get_ad_overview,
    "get_campaign_performance": get_campaign_performance,
    "find_campaigns_to_cut": find_campaigns_to_cut,
    "find_campaigns_to_scale": find_campaigns_to_scale,
    "attribution_sanity_check": attribution_sanity_check,
    "compare_ad_periods": compare_ad_periods,
    "get_sku_ad_spend": get_sku_ad_spend,  # v2.67.105
    # v2.67.250 — Notion-backed playbook / FAQ search.
    # v2.67.261 — renamed from search_knowledge_base (name clash).
    "search_team_playbooks": search_team_playbooks,
    # v2.67.281 — product cross-section dimensions.
    "get_product_dimensions": get_product_dimensions,
}


def call_tool(tool_name: str,
               engine_df: pd.DataFrame,
               sale_lines_df: pd.DataFrame,
               args: dict) -> str:
    """Call the named tool and return a JSON string Claude can consume.
    Wraps errors so a buggy tool never kills the conversation."""
    handler = TOOL_HANDLERS.get(tool_name)
    if handler is None:
        return json.dumps({
            "error": f"Unknown tool {tool_name!r}",
            "available_tools": list(TOOL_HANDLERS.keys()),
        })
    try:
        result = handler(engine_df, sale_lines_df, args)
    except Exception as exc:  # noqa: BLE001
        result = {
            "error": f"{type(exc).__name__}: {exc}",
            "tool": tool_name,
            "args": args,
        }
    return json.dumps(result, default=str)
