"""dropship_backorder.py (v2.67.138)
======================================

Drop-ship backorder warning to #purchase-backorder.

When a customer orders a SKU flagged as `DropShipMode = "Always
Drop Ship"` in CIN7, CIN7 silently auto-creates a draft purchase
order to the supplier. The team has no idea it's there until
someone happens to look at the draft POs list. Without approval,
the draft sits forever and the customer's order is stuck.

This module scans new sales lines every 5 minutes, identifies
dropship SKUs, and posts a warning to #purchase-backorder telling
the team to go approve the draft PO. Idempotent via the
dropship_backorder_warnings table — one warning per (SO, SKU)
pair.

CLI:
  python dropship_backorder.py daily   # scan + post
  python dropship_backorder.py dryrun  # scan + print, no Slack
  python dropship_backorder.py one --so SO-12345 --sku LED-X

Env vars
--------
  SLACK_BOT_TOKEN                       Standard bot token
  SLACK_PURCHASE_BACKORDER_CHANNEL_ID   Channel for warnings
  DROPSHIP_LOOKBACK_HOURS               How far back to scan (default 48)
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402

try:
    from data_paths import OUTPUT_DIR
except ImportError:
    OUTPUT_DIR = SCRIPT_DIR / "output"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
log = logging.getLogger("dropship_backorder")

# CIN7's DropShipMode values. "Always Drop Ship" auto-creates a
# draft PO on every sale. "Optional Drop Ship" is configurable per
# order — we only auto-warn on "Always" to avoid noise.
_DROPSHIP_MODES = ("ALWAYS DROP SHIP",)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def _find_latest_csv(pattern: str) -> Optional[Path]:
    matches = glob.glob(str(OUTPUT_DIR / pattern))
    if not matches:
        return None
    return Path(max(matches, key=os.path.getmtime))


def _load_products() -> Optional[pd.DataFrame]:
    """Load the products master with DropShipMode column."""
    path = _find_latest_csv("products_*.csv")
    if not path:
        log.error("No products_*.csv found in %s", OUTPUT_DIR)
        return None
    try:
        return pd.read_csv(path)
    except Exception as exc:
        log.error("Failed to read products CSV %s: %s", path, exc)
        return None


def _load_sale_lines() -> Optional[pd.DataFrame]:
    """Load the freshest sale_lines CSV. NearSync writes the 1-day
    window every ~15 min, so latency is bounded."""
    # Try shorter windows first — they're freshest.
    for pat in ("sale_lines_last_1d_*.csv",
                 "sale_lines_last_7d_*.csv",
                 "sale_lines_last_*d_*.csv"):
        path = _find_latest_csv(pat)
        if path:
            log.info("Loading sale lines from %s", path)
            try:
                return pd.read_csv(path)
            except Exception as exc:
                log.error("Failed to read sale lines CSV %s: %s",
                            path, exc)
    log.error("No sale_lines CSV found in %s", OUTPUT_DIR)
    return None


def _load_stock_on_hand() -> Optional[pd.DataFrame]:
    """Load the freshest stock_on_hand CSV to report OnHand at
    warning time. Optional — the warning still posts without it."""
    path = _find_latest_csv("stock_on_hand_*.csv")
    if not path:
        return None
    try:
        return pd.read_csv(path)
    except Exception as exc:
        log.warning("Stock-on-hand load failed (non-fatal): %s",
                      exc)
        return None


# ---------------------------------------------------------------------------
# Dropship classification
# ---------------------------------------------------------------------------
def _build_dropship_sku_index(
        products: pd.DataFrame) -> Dict[str, dict]:
    """Return a dict keyed by uppercase SKU for every product
    where DropShipMode contains 'Always Drop Ship'. Value is
    {dropship_mode, primary_supplier, product_name} for use in
    the warning message."""
    if products is None or products.empty:
        return {}
    # Find the DropShipMode column. CIN7 v2 uses 'DropShipMode';
    # some accounts use 'Drop Ship Mode' or 'DropshipMode'. Be
    # tolerant of casing/spacing variants.
    mode_col = None
    for cand in ("DropShipMode", "DropshipMode", "Drop Ship Mode",
                   "DropShipModeName"):
        if cand in products.columns:
            mode_col = cand
            break
    if not mode_col:
        log.error(
            "Products CSV has no DropShipMode column. Columns: %s",
            list(products.columns)[:30])
        return {}
    sku_col = None
    for cand in ("SKU", "Sku", "ProductCode"):
        if cand in products.columns:
            sku_col = cand
            break
    if not sku_col:
        log.error("Products CSV has no SKU column.")
        return {}

    mode_u = (products[mode_col].fillna("")
                  .astype(str).str.upper().str.strip())
    mask = mode_u.isin(_DROPSHIP_MODES)
    matched = products[mask]
    log.info("Found %d products flagged 'Always Drop Ship' "
              "out of %d total", len(matched), len(products))

    name_col = next(
        (c for c in ("Name", "ProductName")
          if c in products.columns), None)
    supplier_col = next(
        (c for c in ("Supplier", "PrimarySupplier")
          if c in products.columns), None)

    out: Dict[str, dict] = {}
    for _, row in matched.iterrows():
        sku = str(row.get(sku_col) or "").strip().upper()
        if not sku:
            continue
        out[sku] = {
            "dropship_mode": row.get(mode_col),
            "supplier": (row.get(supplier_col)
                          if supplier_col else None),
            "name": (row.get(name_col)
                      if name_col else None),
        }
    return out


def _build_onhand_index(
        stock_df: Optional[pd.DataFrame]) -> Dict[str, float]:
    """SKU → OnHand float lookup. Returns empty dict if stock CSV
    isn't loadable (warning still posts, just without OnHand)."""
    if stock_df is None or stock_df.empty:
        return {}
    sku_col = next((c for c in ("SKU", "ProductCode")
                      if c in stock_df.columns), None)
    onhand_col = next((c for c in ("OnHand", "Stock")
                          if c in stock_df.columns), None)
    if not (sku_col and onhand_col):
        return {}
    out: Dict[str, float] = {}
    for _, r in stock_df.iterrows():
        sku = str(r.get(sku_col) or "").strip().upper()
        if not sku:
            continue
        try:
            out[sku] = float(r.get(onhand_col) or 0)
        except (TypeError, ValueError):
            continue
    return out


# ---------------------------------------------------------------------------
# Sale-line filtering
# ---------------------------------------------------------------------------
def _recent_dropship_lines(sale_lines: pd.DataFrame,
                                  dropship_skus: Dict[str, dict],
                                  lookback_hours: int) -> pd.DataFrame:
    """Filter sale_lines down to recent rows whose SKU is in the
    dropship index. Returns an empty frame if no matches."""
    if sale_lines is None or sale_lines.empty or not dropship_skus:
        return pd.DataFrame()

    sku_col = next((c for c in ("SKU", "ProductCode")
                      if c in sale_lines.columns), None)
    if not sku_col:
        log.error("Sale lines CSV has no SKU column.")
        return pd.DataFrame()

    # Date filter — find a sensible date column.
    date_col = None
    for cand in ("OrderDate", "SaleDate", "CreatedDate", "Date"):
        if cand in sale_lines.columns:
            date_col = cand
            break

    cutoff = (datetime.now(timezone.utc)
                - timedelta(hours=lookback_hours))
    if date_col:
        dates = pd.to_datetime(
            sale_lines[date_col], errors="coerce", utc=True)
        date_mask = dates >= pd.Timestamp(cutoff)
    else:
        date_mask = pd.Series(True, index=sale_lines.index)
        log.warning(
            "No date column on sale_lines — scanning ALL rows")

    sku_upper = (sale_lines[sku_col].fillna("")
                  .astype(str).str.upper().str.strip())
    dropship_set = set(dropship_skus.keys())
    sku_mask = sku_upper.isin(dropship_set)

    result = sale_lines[date_mask & sku_mask].copy()
    log.info("Found %d dropship sale lines in last %dh",
              len(result), lookback_hours)
    return result


# ---------------------------------------------------------------------------
# Warning composition + posting
# ---------------------------------------------------------------------------
def _compose_warning(so_number: str, customer: Optional[str],
                          sku: str, product_name: Optional[str],
                          supplier: Optional[str],
                          quantity_ordered: Optional[float],
                          quantity_on_hand: Optional[float]) -> str:
    name = product_name or "(name not loaded)"
    qty_str = (f"{int(quantity_ordered)}"
                if quantity_ordered is not None
                and not pd.isna(quantity_ordered)
                else "?")
    onhand_str = (f"{int(quantity_on_hand)}"
                    if quantity_on_hand is not None
                    else "?")
    lines = [
        f"⚠️ *Drop-ship backorder — needs PO approval*",
        "",
        f"• Customer: {customer or '(unknown)'} · Order: "
        f"*{so_number}*",
        f"• SKU: `{sku}` — {name}",
        f"• Ordered: {qty_str} · OnHand: {onhand_str}",
    ]
    if supplier:
        lines.append(f"• Drop-ship supplier: *{supplier}*")
    lines.extend([
        "",
        "_CIN7 has auto-created a draft PO. "
        "Please approve it so the supplier dispatches._",
    ])
    return "\n".join(lines)


def _post_to_slack(channel_id: str, text: str
                      ) -> Tuple[Optional[str], Optional[str]]:
    try:
        import slack_sync
    except ImportError as exc:
        return None, f"slack_sync import failed: {exc}"
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not token:
        return None, "SLACK_BOT_TOKEN not set"
    try:
        session = slack_sync._build_session(token)
        body = slack_sync._slack_post(session, "chat.postMessage", {
            "channel": channel_id,
            "text": text,
            "unfurl_links": False,
            "unfurl_media": False,
        })
        if not body.get("ok"):
            return None, f"slack returned ok=false: {body}"
        return body.get("ts"), None
    except Exception as exc:
        return None, f"post error: {exc}"


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------
def scan_and_warn(dryrun: bool = False,
                       lookback_hours: int = 48) -> dict:
    """Top-level pass. Returns summary dict."""
    channel = os.environ.get(
        "SLACK_PURCHASE_BACKORDER_CHANNEL_ID", "").strip()
    if not dryrun and not channel:
        log.warning(
            "SLACK_PURCHASE_BACKORDER_CHANNEL_ID not set — no "
            "warnings will post.")
        return {"posted": 0, "skipped_no_channel": True}

    products = _load_products()
    if products is None:
        return {"posted": 0, "error": "products_load_failed"}
    sale_lines = _load_sale_lines()
    if sale_lines is None:
        return {"posted": 0, "error": "sale_lines_load_failed"}
    stock = _load_stock_on_hand()
    onhand_idx = _build_onhand_index(stock)

    dropship_skus = _build_dropship_sku_index(products)
    if not dropship_skus:
        log.info("No dropship products in catalog — nothing to "
                  "scan against.")
        return {"posted": 0, "dropship_skus_in_catalog": 0}

    eligible = _recent_dropship_lines(
        sale_lines, dropship_skus, lookback_hours)
    if eligible.empty:
        return {"posted": 0,
                  "dropship_skus_in_catalog": len(dropship_skus),
                  "eligible": 0}

    so_col = next((c for c in ("OrderNumber", "SaleOrderNumber",
                                  "SaleNumber", "SoNumber")
                      if c in eligible.columns), None)
    qty_col = next((c for c in ("Quantity", "Qty")
                       if c in eligible.columns), None)
    cust_col = next((c for c in ("Customer", "CustomerName",
                                    "BillingName")
                        if c in eligible.columns), None)
    sku_col = next((c for c in ("SKU", "ProductCode")
                       if c in eligible.columns), None)

    if not so_col:
        log.error("Sale lines CSV has no order-number column. "
                    "Columns: %s",
                    list(eligible.columns)[:20])
        return {"posted": 0, "error": "missing_order_number_col"}

    n_posted = 0
    n_already = 0
    n_errors = 0

    for _, row in eligible.iterrows():
        so_number = str(row.get(so_col) or "").strip()
        sku = str(row.get(sku_col) or "").strip().upper()
        if not (so_number and sku):
            continue
        if db.has_dropship_warning(so_number, sku):
            n_already += 1
            continue
        info = dropship_skus.get(sku, {})
        customer = (str(row.get(cust_col))
                      if cust_col and pd.notna(row.get(cust_col))
                      else None)
        qty = row.get(qty_col) if qty_col else None
        try:
            qty = (float(qty) if qty is not None
                    and not pd.isna(qty) else None)
        except (TypeError, ValueError):
            qty = None
        onhand = onhand_idx.get(sku)

        msg = _compose_warning(
            so_number=so_number,
            customer=customer,
            sku=sku,
            product_name=info.get("name"),
            supplier=info.get("supplier"),
            quantity_ordered=qty,
            quantity_on_hand=onhand,
        )
        log.info("Dropship warning %s/%s %s",
                  so_number, sku,
                  "[DRYRUN]" if dryrun else "")

        if dryrun:
            print(f"\n--- {so_number} / {sku} ---\n{msg}\n")
            continue

        posted_ts, error = _post_to_slack(channel, msg)
        if error:
            log.error("Post failed for %s/%s: %s",
                        so_number, sku, error)
            db.record_dropship_warning(
                so_number=so_number, sku=sku,
                customer=customer,
                supplier=info.get("supplier"),
                quantity_ordered=qty,
                quantity_on_hand=onhand,
                posted_channel=channel,
                posted_ts=None,
                error_msg=error,
            )
            n_errors += 1
            continue
        db.record_dropship_warning(
            so_number=so_number, sku=sku,
            customer=customer,
            supplier=info.get("supplier"),
            quantity_ordered=qty,
            quantity_on_hand=onhand,
            posted_channel=channel,
            posted_ts=posted_ts,
        )
        n_posted += 1

    return {
        "dropship_skus_in_catalog": len(dropship_skus),
        "eligible": len(eligible),
        "posted": n_posted,
        "skipped_already_warned": n_already,
        "errors": n_errors,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _setup_log(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=LOG_FORMAT, stream=sys.stdout, force=True)


def cmd_daily(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    hours = int(os.environ.get(
        "DROPSHIP_LOOKBACK_HOURS", "48") or 48)
    result = scan_and_warn(dryrun=False, lookback_hours=hours)
    log.info("DONE: %s", result)
    return 0


def cmd_dryrun(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    hours = int(args.hours or 48)
    result = scan_and_warn(dryrun=True, lookback_hours=hours)
    log.info("DONE [DRYRUN]: %s", result)
    return 0


def cmd_one(args: argparse.Namespace) -> int:
    """Debug: build the warning message for a specific (SO, SKU)
    without posting."""
    _setup_log(args.verbose)
    products = _load_products()
    sale_lines = _load_sale_lines()
    stock = _load_stock_on_hand()
    if products is None or sale_lines is None:
        return 1
    dropship_skus = _build_dropship_sku_index(products)
    onhand_idx = _build_onhand_index(stock)
    sku_u = args.sku.upper()
    info = dropship_skus.get(sku_u)
    if not info:
        log.info("SKU %s is NOT flagged dropship in CIN7. Modes "
                  "found for this SKU:", args.sku)
        mode_col = next((c for c in ("DropShipMode",
                                         "Drop Ship Mode")
                            if c in products.columns), None)
        if mode_col:
            row = products[products["SKU"].astype(str)
                                .str.upper() == sku_u]
            for _, r in row.iterrows():
                log.info("  %s", r.get(mode_col))
        return 0
    so_col = next((c for c in ("OrderNumber", "SaleNumber")
                      if c in sale_lines.columns), None)
    sku_col = next((c for c in ("SKU", "ProductCode")
                       if c in sale_lines.columns), None)
    match = sale_lines[
        (sale_lines[so_col].astype(str) == args.so)
        & (sale_lines[sku_col].astype(str).str.upper() == sku_u)
    ]
    if match.empty:
        log.error("No sale line found for SO=%s SKU=%s",
                    args.so, sku_u)
        return 1
    row = match.iloc[0]
    qty_col = next((c for c in ("Quantity", "Qty")
                       if c in match.columns), None)
    cust_col = next((c for c in ("Customer", "CustomerName")
                        if c in match.columns), None)
    msg = _compose_warning(
        so_number=args.so,
        customer=(row.get(cust_col) if cust_col else None),
        sku=sku_u,
        product_name=info.get("name"),
        supplier=info.get("supplier"),
        quantity_ordered=(row.get(qty_col) if qty_col else None),
        quantity_on_hand=onhand_idx.get(sku_u),
    )
    print(msg)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Warn #purchase-backorder when a customer "
                      "orders a dropship-flagged SKU.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_d = sub.add_parser("daily",
                            help="Scan + post (called from slack_loop).")
    p_d.add_argument("--verbose", action="store_true")
    p_d.set_defaults(func=cmd_daily)

    p_dr = sub.add_parser("dryrun",
                              help="Scan + print, no Slack post.")
    p_dr.add_argument("--hours", type=int, default=48)
    p_dr.add_argument("--verbose", action="store_true")
    p_dr.set_defaults(func=cmd_dryrun)

    p_o = sub.add_parser("one",
                            help="Inspect one (SO, SKU) without "
                                  "posting.")
    p_o.add_argument("--so", required=True)
    p_o.add_argument("--sku", required=True)
    p_o.add_argument("--verbose", action="store_true")
    p_o.set_defaults(func=cmd_one)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
