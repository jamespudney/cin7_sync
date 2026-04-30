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
            "last N days. Returns daily breakdown if requested, "
            "otherwise totals. Use when user asks 'how fast does X "
            "sell' or 'how many of Y did we sell last month'."
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
    return {
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


def set_sales_full_headers(headers_df: pd.DataFrame) -> None:
    """Called by the Streamlit page on AI Assistant page load. Stores
    the merged sales-headers DataFrame so get_sales_totals can read
    it without recomputing per-tool-call."""
    _SALES_FULL_HOLDER["df"] = headers_df


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
TOOL_HANDLERS = {
    "search_products": search_products,
    "get_sku_details": get_sku_details,
    "get_velocity": get_velocity,
    "get_dead_stock": get_dead_stock,
    "get_migration_chain": get_migration_chain,
    "get_sales_totals": get_sales_totals,
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
