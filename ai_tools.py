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
            "'compare this month to last month'. Returns revenue (from "
            "order headers, includes shipping & tax — matches CIN7's "
            "Revenue tile), unit count (from line items), and order "
            "count for the requested period and granularity."
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
                                   "to-date.",
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
            "required": ["period"],
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
            "'not available' rather than guessing."
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
        "name": "search_products_by_text",
        "description": (
            "Substring search across one or more product TEXT fields "
            "(title / description / tags / product_type / collections). "
            "Use this when an alias rule of type='text_search' fires "
            "in the system-prompt addendum, OR when the user asks for "
            "products matching a descriptive phrase that isn't tied "
            "to a specific SKU or family — e.g. 'warm white', "
            "'diffused lens', 'IP67 outdoor'. Combinable with "
            "classification + in_stock_only filters so 'show me warm "
            "white LED strips that are slow movers' is one tool call. "
            "If a requested field doesn't exist in the catalog data "
            "yet (e.g. tags before the Shopify merge ships), the tool "
            "reports it in `missing_fields` rather than silently "
            "skipping it."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": (
                        "The phrase to search for, e.g. 'warm white'."),
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
                "family": {
                    "type": "string",
                    "description": (
                        "Optional product-family code to narrow "
                        "further (e.g. SIERRA38)."),
                },
                "limit": {
                    "type": "integer",
                    "description": (
                        "Max rows to return (cap 50, default 25)."),
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
    """Make a row JSON-friendly: convert NaN/None, dates to strings."""
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
    limit = min(int(args.get("limit", 25) or 25), 50)

    df = engine_df.copy()
    if query:
        mask_sku = df["SKU"].astype(str).str.lower().str.contains(
            query, na=False)
        mask_name = df["Name"].astype(str).str.lower().str.contains(
            query, na=False)
        df = df[mask_sku | mask_name]
    if family and "Family" in df.columns:
        df = df[df["Family"].astype(str).str.upper() == family]
    if classification != "any" and "Classification" in df.columns:
        df = df[df["Classification"].astype(str).str.lower()
                  == classification]
    if abc_class != "ANY" and "ABC" in df.columns:
        df = df[df["ABC"].astype(str).str.upper() == abc_class]
    if in_stock_only and "OnHand" in df.columns:
        df = df[df["OnHand"].fillna(0) > 0]

    cols_we_want = [c for c in [
        "SKU", "Name", "Family", "ABC", "Classification",
        "OnHand", "TargetStock", "ReorderSuggested",
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
    in_window = sl[(sl["SKU"].astype(str) == sku)
                    & (sl["InvoiceDate"] >= cutoff)]
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
        "OnHand", "StockValue", "ABC",
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
    period = (args.get("period") or "mtd").strip().lower()
    group_by = (args.get("group_by") or "none").strip().lower()

    today = pd.Timestamp.now().normalize()
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


def set_sales_full_headers(headers_df: pd.DataFrame) -> None:
    """Called by the Streamlit page on AI Assistant page load. Stores
    the merged sales-headers DataFrame so get_sales_totals can read
    it without recomputing per-tool-call."""
    _SALES_FULL_HOLDER["df"] = headers_df


def set_purchase_lines(purchase_lines_df: pd.DataFrame) -> None:
    """Called by the Streamlit AI Assistant page on load. Stashes the
    purchase_lines_last_90d DataFrame so get_incoming_stock can scan
    open POs without re-loading the CSV per tool call."""
    _PURCHASE_LINES_HOLDER["df"] = purchase_lines_df


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

    Driven by an alias rule with rule_type='text_search'. The user
    typed a phrase like 'warm white' that's been mapped to a search
    across product fields (title / description / tags / product_type /
    collections). This tool runs the contains-match across whichever
    of those fields exist in the products DataFrame.

    Combinable with classification + in_stock_only filters so a
    question like 'show me warm white LED strips that are slow movers'
    resolves to a single tool call.
    """
    query = (args.get("query") or "").strip().lower()
    if not query:
        return {"error": "query is required"}

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

    df = engine_df.copy()
    searched_cols: list = []
    missing_fields: list = []
    masks = []
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
        mask = df[col_used].fillna("").astype(str).str.lower().str.contains(
            query, na=False, regex=False)
        masks.append((f, col_used, mask))
        searched_cols.append({"requested": f, "actual_column": col_used})

    if not masks:
        return {
            "error": (f"None of the requested fields exist in the "
                       f"product data right now: {requested}. "
                       f"Available columns: {list(df.columns)[:20]}"),
            "missing_fields": missing_fields,
            "available_columns": list(df.columns),
        }

    combined = masks[0][2]
    for _, _, m in masks[1:]:
        combined = combined | m
    df = df[combined]

    # Optional secondary filters Claude can stack on top.
    classification = (args.get("classification") or "any").strip().lower()
    if classification != "any" and "Classification" in df.columns:
        df = df[df["Classification"].astype(str).str.lower()
                  == classification]
    in_stock_only = bool(args.get("in_stock_only", False))
    if in_stock_only and "OnHand" in df.columns:
        df = df[df["OnHand"].fillna(0) > 0]
    family = (args.get("family") or "").strip().upper()
    if family and "Family" in df.columns:
        df = df[df["Family"].astype(str).str.upper() == family]

    limit = min(int(args.get("limit", 25) or 25), 50)
    cols_we_want = [c for c in [
        "SKU", "Name", "Family", "ABC", "Classification",
        "OnHand", "TargetStock", "ReorderSuggested",
    ] if c in df.columns]
    df = df.head(limit)[cols_we_want] if cols_we_want else df.head(limit)
    rows = [_serialise_row(r) for r in df.to_dict(orient="records")]
    return {
        "matched": len(rows),
        "results": rows,
        "searched": searched_cols,
        "missing_fields": missing_fields,
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
        return {
            "matched": 0,
            "subject": sku or family,
            "date_field_used": date_col,
            "note": ("No open / incomplete purchase orders match. "
                      "Either the SKU has nothing on order, or all "
                      "matching POs are already received / closed / "
                      "cancelled. Per spec we don't return those."),
        }

    out_rows = []
    for _, r in df.head(limit).iterrows():
        rec = {
            "sku": r.get("SKU"),
            "name": r.get("Name"),
            "quantity_on_order": r.get("Quantity"),
            "quantity_remaining": (
                r.get("QuantityRemaining")
                if "QuantityRemaining" in df.columns
                else None),
            "expected_date": (
                str(r.get(date_col)) if (date_col
                                         and pd.notna(r.get(date_col)))
                else "not available"),
            "supplier": r.get("Supplier"),
            "po_number": r.get("OrderNumber"),
            "status": r.get("Status"),
        }
        out_rows.append(_serialise_row(rec))

    return {
        "matched": len(out_rows),
        "subject": sku or family,
        "date_field_used": date_col,
        "lines": out_rows,
        "note": (
            f"Showing first {limit} of potentially many open POs."
            if len(out_rows) == limit else None),
    }


TOOL_HANDLERS = {
    "search_products": search_products,
    "search_products_by_text": search_products_by_text,
    "find_similar_products": find_similar_products,
    "get_incoming_stock": get_incoming_stock,
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
