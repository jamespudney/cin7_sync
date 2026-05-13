"""shipping_margin_monitor.py (v2.67.152)
=============================================

Proactive shipping-margin monitoring.

When CIN7+Shopify quote customer shipping and ShipStation actually
ships, the actual carrier cost often diverges from what the
customer paid. Long-but-light items get hit with dimensional
weight; expedited services (2nd Day Air / Next Day) on
short-but-heavy items can be over-quoted. Without proactive
monitoring, these losses (and the rare gains) only surface when
someone happens to look at the dashboard.

This module scans recent ShipStation shipments every ~30 min,
computes margin = (CustomerCharge - ShipmentCost), and alerts to
#shipping-issues when EITHER:

  - the margin is OUTSIDE ±MARGIN_PCT_THRESHOLD (default 5%) of
    the actual cost, AND
  - the absolute margin is OVER MARGIN_AMOUNT_FLOOR (default $5)
    so 5%-of-pocket-change doesn't blow up the channel

Each alert is idempotent per shipment_id. The team can mark it
'reviewed' by replying in-thread; that's handled by a
slack_listener path (separate small wire-up).

CLI:
  python shipping_margin_monitor.py daily
  python shipping_margin_monitor.py dryrun [--hours 168]

Env vars:
  SLACK_BOT_TOKEN
  SLACK_SHIPPING_ISSUES_CHANNEL_ID    e.g. C08NC4ZCX4K
  SHIPPING_MARGIN_PCT_THRESHOLD       default 0.05 (5%)
  SHIPPING_MARGIN_AMOUNT_FLOOR        default 5 (USD)
  SHIPPING_MARGIN_LOOKBACK_HOURS      default 168 (7 days)
  SHIPPING_POLICY_URL                 default
    https://www.wired4signsusa.com/policies/shipping-policy
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402

try:
    from data_paths import OUTPUT_DIR
except ImportError:
    OUTPUT_DIR = SCRIPT_DIR / "output"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
log = logging.getLogger("shipping_margin_monitor")


def _load_latest_shipments() -> Optional[pd.DataFrame]:
    """Reuse the freshest shipments CSV (NearSync rolling window
    gives ~15-min freshness)."""
    candidates: List[Path] = []
    for pat in ("shipments_last_*d_*.csv", "shipments_full.csv"):
        for p in glob.glob(str(OUTPUT_DIR / pat)):
            candidates.append(Path(p))
    if not candidates:
        log.error("No shipments CSV found in %s", OUTPUT_DIR)
        return None
    latest = max(candidates, key=os.path.getmtime)
    log.info("Loading shipments from %s", latest)
    try:
        return pd.read_csv(latest, low_memory=False)
    except Exception as exc:
        log.error("Failed to read %s: %s", latest, exc)
        return None


def _looks_like_pickup(charge: float, cost: float) -> bool:
    """A shipment with $0 cost and $0 charge isn't a margin event
    worth flagging — pickup orders, in-store handoffs etc."""
    return abs(charge or 0) < 0.01 and abs(cost or 0) < 0.01


def _compose_alert(*, order: str, customer: Optional[str],
                       ship_date: Optional[str],
                       charge: float, cost: float,
                       margin_amt: float, margin_pct: float,
                       direction: str,
                       carrier: Optional[str],
                       service: Optional[str],
                       tracking: Optional[str],
                       skus: Optional[List[str]] = None
                       ) -> str:
    """Compose the #shipping-issues alert message."""
    policy_url = os.environ.get(
        "SHIPPING_POLICY_URL",
        "https://www.wired4signsusa.com/policies/shipping-policy")
    icon = "🔴" if direction == "under" else "⚠️"
    direction_label = (
        f"Under-charged by ${abs(margin_amt):.2f}"
        if direction == "under"
        else f"Over-charged by ${abs(margin_amt):.2f}")
    lines: List[str] = [
        f"{icon} *Shipping margin alert — {direction_label}*",
        "",
        f"• Order: *{order}*"
        + (f" · Customer: {customer}" if customer else ""),
    ]
    if ship_date:
        lines.append(f"• Ship date: {ship_date[:10]}")
    if carrier or service:
        bits = []
        if carrier:
            bits.append(str(carrier))
        if service:
            bits.append(str(service))
        lines.append(f"• Service: {' · '.join(bits)}")
    lines.append(
        f"• Customer paid: *${charge:.2f}* · "
        f"We paid carrier: *${cost:.2f}* · "
        f"Margin: *${margin_amt:+.2f}* "
        f"({margin_pct*100:+.1f}%)")
    if skus:
        # Cap to 3 SKUs — context, not a full list.
        skus_str = ", ".join(f"`{s}`" for s in skus[:3])
        if len(skus) > 3:
            skus_str += f", + {len(skus) - 3} more"
        lines.append(f"• Items: {skus_str}")
    if tracking:
        lines.append(f"• Tracking: `{tracking}`")
    lines.append("")
    if direction == "under":
        lines.append(
            f"_Per <{policy_url}|shipping policy>, consider "
            f"re-quoting or refunding. Reply 'reviewed' in this "
            f"thread to close, with a brief note on the cause._")
    else:
        lines.append(
            f"_Customer was charged more than carrier cost. "
            f"Check if a refund / goodwill credit is "
            f"appropriate. Reply 'reviewed' to close._")
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


def _sale_lines_for_invoice(inv_or_order: str) -> List[str]:
    """Return up to N SKUs on the matching sale, for context in
    the alert. Best-effort — empty list if we can't resolve."""
    if not inv_or_order:
        return []
    try:
        import glob
        paths = sorted(glob.glob(
            str(OUTPUT_DIR / "sale_lines_last_*d_*.csv")),
            key=os.path.getmtime)
        if not paths:
            return []
        df = pd.read_csv(paths[-1], low_memory=False)
        sku_col = next(
            (c for c in ("SKU", "ProductCode")
              if c in df.columns), None)
        match_cols = [c for c in ("OrderNumber", "InvoiceNumber",
                                       "SaleNumber")
                          if c in df.columns]
        if not sku_col or not match_cols:
            return []
        norm = str(inv_or_order).upper().lstrip("#")
        # Strip prefixes for normalised matching
        for prefix in ("SO-", "INV-"):
            if norm.startswith(prefix):
                norm = norm[len(prefix):]
        for mc in match_cols:
            tail = (df[mc].astype(str).str.upper()
                      .str.replace("SO-", "", regex=False)
                      .str.replace("INV-", "", regex=False)
                      .str.lstrip("#"))
            m = df[tail == norm]
            if not m.empty:
                return [str(s).strip().upper()
                          for s in m[sku_col].dropna().unique()[:5]
                          if str(s).strip()]
    except Exception:
        pass
    return []


def scan_and_alert(dryrun: bool = False,
                       lookback_hours: int = 168) -> dict:
    """Top-level scan. Returns summary dict.
    - Loads shipments CSV
    - Filters to those within lookback_hours
    - Computes margin per row
    - Filters to abs(pct) > threshold AND abs($) > floor
    - Posts to #shipping-issues for each new one (idempotent)
    """
    channel = os.environ.get(
        "SLACK_SHIPPING_ISSUES_CHANNEL_ID", "").strip()
    if not dryrun and not channel:
        return {"posted": 0, "skipped_no_channel": True}

    pct_thresh = float(os.environ.get(
        "SHIPPING_MARGIN_PCT_THRESHOLD", "0.05") or 0.05)
    amount_floor = float(os.environ.get(
        "SHIPPING_MARGIN_AMOUNT_FLOOR", "5") or 5)

    df = _load_latest_shipments()
    if df is None or df.empty:
        return {"posted": 0, "error": "no_shipments_csv"}

    # Map columns tolerantly — different shipstation_sync versions
    # have varied column names slightly.
    charge_col = next(
        (c for c in ("CustomerShippingCharge",
                       "CustomerShipmentCharge", "Charge")
          if c in df.columns), None)
    cost_col = next(
        (c for c in ("ShipmentCost", "Cost", "TotalCost")
          if c in df.columns), None)
    date_col = next(
        (c for c in ("ShipDate", "ShipmentDate", "Date")
          if c in df.columns), None)
    order_col = next(
        (c for c in ("OrderNumber", "InvoiceNumber", "SaleNumber")
          if c in df.columns), None)
    ship_id_col = next(
        (c for c in ("ShipmentID", "ID", "ShipStationShipmentID")
          if c in df.columns), None)
    customer_col = next(
        (c for c in ("Customer", "CustomerName", "BillingName",
                        "ShipToName")
          if c in df.columns), None)
    carrier_col = next(
        (c for c in ("CarrierCode", "Carrier", "CarrierName")
          if c in df.columns), None)
    service_col = next(
        (c for c in ("ServiceCode", "Service", "ServiceName")
          if c in df.columns), None)
    tracking_col = next(
        (c for c in ("TrackingNumber", "Tracking")
          if c in df.columns), None)
    voided_col = next(
        (c for c in ("Voided", "IsVoided", "Status")
          if c in df.columns), None)

    if not (charge_col and cost_col and ship_id_col):
        return {"posted": 0,
                  "error": "missing_required_cols",
                  "have_charge": bool(charge_col),
                  "have_cost": bool(cost_col),
                  "have_id": bool(ship_id_col)}

    # Lookback filter.
    cutoff = (datetime.now(timezone.utc)
                - timedelta(hours=lookback_hours))
    if date_col:
        dates = pd.to_datetime(
            df[date_col], errors="coerce", utc=True)
        date_mask = dates >= pd.Timestamp(cutoff)
    else:
        date_mask = pd.Series(True, index=df.index)

    # Voided filter — exclude obvious cancellations.
    if voided_col == "Voided" and "Voided" in df.columns:
        voided_mask = ~df["Voided"].fillna(False).astype(bool)
    elif voided_col == "IsVoided" and "IsVoided" in df.columns:
        voided_mask = ~df["IsVoided"].fillna(False).astype(bool)
    elif voided_col == "Status" and "Status" in df.columns:
        voided_mask = ~(df["Status"].fillna("")
                          .astype(str).str.upper()
                          .str.contains("VOID|CANCEL",
                                            regex=True, na=False))
    else:
        voided_mask = pd.Series(True, index=df.index)

    eligible = df[date_mask & voided_mask].copy()

    n_posted = 0
    n_already = 0
    n_within_band = 0
    n_below_floor = 0
    n_pickup = 0
    n_errors = 0

    for _, row in eligible.iterrows():
        try:
            charge = float(row.get(charge_col) or 0)
            cost = float(row.get(cost_col) or 0)
        except (TypeError, ValueError):
            continue
        if _looks_like_pickup(charge, cost):
            n_pickup += 1
            continue
        if cost <= 0:
            # Can't compute pct — skip (also a bit suspicious)
            continue
        margin = charge - cost
        pct = margin / cost
        if abs(pct) <= pct_thresh:
            n_within_band += 1
            continue
        if abs(margin) < amount_floor:
            n_below_floor += 1
            continue
        ship_id = str(row.get(ship_id_col) or "").strip()
        if not ship_id:
            continue
        if db.has_shipping_margin_alert(ship_id):
            n_already += 1
            continue
        order = (str(row.get(order_col) or "").strip()
                  if order_col else "")
        ship_date = (str(row.get(date_col) or "").strip()
                      if date_col else "")
        customer = (str(row.get(customer_col) or "").strip()
                      if customer_col else None)
        carrier = (str(row.get(carrier_col) or "").strip()
                    if carrier_col else None)
        service = (str(row.get(service_col) or "").strip()
                    if service_col else None)
        tracking = (str(row.get(tracking_col) or "").strip()
                      if tracking_col else None)
        direction = "under" if margin < 0 else "over"
        skus = _sale_lines_for_invoice(order)
        msg = _compose_alert(
            order=order, customer=customer,
            ship_date=ship_date, charge=charge,
            cost=cost, margin_amt=margin, margin_pct=pct,
            direction=direction, carrier=carrier,
            service=service, tracking=tracking, skus=skus,
        )
        log.info("Margin alert %s/%s · ${%.2f} (%.1f%%) %s",
                  order, ship_id, margin, pct * 100,
                  "[DRYRUN]" if dryrun else "")
        if dryrun:
            print(f"\n--- {order} / {ship_id} ---\n{msg}\n")
            continue
        posted_ts, err = _post_to_slack(channel, msg)
        if err:
            log.error("Post failed for %s: %s", ship_id, err)
            db.record_shipping_margin_alert(
                shipment_id=ship_id, order_number=order,
                customer=customer, ship_date=ship_date,
                customer_charge=charge, shipment_cost=cost,
                margin_amount=margin, margin_pct=pct,
                direction=direction, posted_channel=channel,
                posted_ts=None, error_msg=err)
            n_errors += 1
            continue
        db.record_shipping_margin_alert(
            shipment_id=ship_id, order_number=order,
            customer=customer, ship_date=ship_date,
            customer_charge=charge, shipment_cost=cost,
            margin_amount=margin, margin_pct=pct,
            direction=direction, posted_channel=channel,
            posted_ts=posted_ts)
        n_posted += 1

    return {
        "scanned": len(eligible),
        "posted": n_posted,
        "skipped_within_band": n_within_band,
        "skipped_below_floor": n_below_floor,
        "skipped_pickup": n_pickup,
        "skipped_already_alerted": n_already,
        "errors": n_errors,
        "threshold_pct": pct_thresh,
        "amount_floor": amount_floor,
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
        "SHIPPING_MARGIN_LOOKBACK_HOURS", "168") or 168)
    result = scan_and_alert(dryrun=False, lookback_hours=hours)
    log.info("DONE: %s", result)
    return 0


def cmd_dryrun(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    hours = int(args.hours or 168)
    result = scan_and_alert(dryrun=True, lookback_hours=hours)
    log.info("DONE [DRYRUN]: %s", result)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scan ShipStation shipments for margin "
                      "anomalies; alert #shipping-issues.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_d = sub.add_parser("daily")
    p_d.add_argument("--verbose", action="store_true")
    p_d.set_defaults(func=cmd_daily)
    p_dr = sub.add_parser("dryrun")
    p_dr.add_argument("--hours", type=int, default=168)
    p_dr.add_argument("--verbose", action="store_true")
    p_dr.set_defaults(func=cmd_dryrun)
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
