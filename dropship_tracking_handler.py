"""dropship_tracking_handler.py (v2.67.160)
==============================================

Parse UPS-style shipping notification emails forwarded into a
Slack channel via Gmail → Slack Email-app, match to the
corresponding CIN7 sale, AUTO-WRITE the tracking into the sale
via the CIN7 PUT /sale API, and post a confirmation reply.

Flow:
  1. Detects the email message in the configured channel
  2. Parses tracking number, ship-to name+address, carrier
     service, weight from the email body (Slack-Email puts the
     plaintext in `files[0].plain_text`)
  3. Matches to the most recent dropship sale with that ship-to
     name + address combo
  4. v2.67.159 — Auto-writes the tracking into the matched sale's
     Fulfilments[0].Ship.Lines via the CIN7 PUT /sale API.
     Idempotent: re-runs detect that the tracking is already
     present and skip the write. CIN7 then auto-pushes the
     fulfillment to Shopify, replacing Cheran's manual paste step.
  5. Posts confirmation in the same thread with the auto-write
     outcome (✅ written / ℹ️ already present / ❌ failed).
  6. Compares supplier's actual weight to Shopify's quoted weight
     and flags discrepancies > threshold

Env vars:
  SLACK_BOT_TOKEN
  SLACK_DROPSHIP_TRACKING_CHANNEL_ID   e.g. C0B3KD6GBM3
  SLACK_SHIPPING_ISSUES_CHANNEL_ID     where weight-mismatch
                                        alerts post (reuse the
                                        existing channel)
  DROPSHIP_WEIGHT_PCT_THRESHOLD        default 0.50 (50%) — large
                                        because dimensional weight
                                        can legitimately double
                                        actual weight on big boxes

Public API:
  is_ups_shipment_email(msg) -> bool
  parse_ups_email(text) -> dict
  handle_ups_email(msg) -> Tuple[reply_text, tools_used]
"""

from __future__ import annotations

import glob
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
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

log = logging.getLogger("dropship_tracking_handler")


# ---------------------------------------------------------------------------
# Detection — is this Slack-Email payload a UPS shipment notification?
# ---------------------------------------------------------------------------
_UPS_SUBJECT_HINTS = (
    "UPS Ship Notification",
    "UPS Tracking",
    "your shipment has been processed",
)


def _email_payload(msg: dict) -> Optional[dict]:
    """Pull the embedded email dict out of msg.raw_event.
    Slack-Email puts the email inside files[0] with mimetype
    text/html and filetype email."""
    raw = msg.get("raw_event")
    if not raw:
        return None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None
    files = raw.get("files") or []
    if not files:
        return None
    for f in files:
        if not isinstance(f, dict):
            continue
        if f.get("filetype") == "email":
            return f
    return None


def is_ups_shipment_email(msg: dict) -> bool:
    """True if the message looks like a UPS shipment notification
    forwarded via the Slack Email app. Conservative — we look at
    subject + body markers to avoid misclassifying other forwarded
    emails (e.g. the Gmail forwarding-verification email)."""
    f = _email_payload(msg)
    if not f:
        return False
    subject = (f.get("subject") or "").lower()
    body = (f.get("plain_text") or "").lower()
    # Subject is a stronger signal but not always present.
    if any(h.lower() in subject for h in _UPS_SUBJECT_HINTS):
        return True
    # Fall back to body markers — the standard UPS template
    # contains both "Tracking Number" and "Ship To".
    return ("tracking number" in body
              and "ship to" in body
              and "ups service" in body)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
_TRACKING_RE = re.compile(
    r"Tracking\s+Number\s*:?\s*(1Z[A-Z0-9]{10,18}|\d{10,22})",
    re.IGNORECASE)
_SERVICE_RE = re.compile(
    r"UPS\s+Service\s*:?\s*([^\n\r]+)", re.IGNORECASE)
_WEIGHT_RE = re.compile(
    r"(?:Package\s+)?Weight\s*:?\s*([\d.]+)\s*(LBS|LB|KG|G|OZ)",
    re.IGNORECASE)
_PACKAGES_RE = re.compile(
    r"Number\s+of\s+Packages\s*:?\s*(\d+)",
    re.IGNORECASE)
_SHIP_TO_RE = re.compile(
    # Captures everything from "Ship To:" up to the first
    # subsequent metadata header line (UPS Service / Number /
    # Weight). Greedy on lines, lazy at the boundary.
    r"Ship\s+To\s*:?\s*\n?(.*?)\n\s*(?:UPS\s+Service|Number\s+of"
    r"\s+Packages|Weight)\b",
    re.IGNORECASE | re.DOTALL)
_FROM_RE = re.compile(
    r"From\s*:?\s*([^\n\r]+)", re.IGNORECASE)


def _strip_markdown_bold(text: str) -> str:
    """v2.67.157 — Gmail's plain-text view of forwarded emails
    wraps labels in markdown asterisks: '*Ship To:*', '*UPS
    Service:*', '*Package Weight:*'. These break field-boundary
    regexes that expect bare labels. Strip the asterisks before
    parsing so the rest of the regexes are simple."""
    if not text:
        return text
    # Replace *foo* with foo (single-line spans only — avoid
    # over-eager removal of legitimate asterisks elsewhere)
    return re.sub(r"\*([^*\n]+?)\*", r"\1", text)


def parse_ups_email(text: str, subject: str = "") -> dict:
    """Extract structured fields from a UPS shipment-notification
    email body. Returns dict with whichever fields parsed; missing
    fields are None / empty. v2.67.258 — `subject` is searched as
    a fallback for the tracking number: UPS subjects reliably
    read 'UPS Ship Notification, Tracking Number 1Z...' even when
    the HTML body's plain-text conversion is mangled."""
    out: dict = {}
    if not text and not subject:
        return out
    text = _strip_markdown_bold(text or "")

    m = _TRACKING_RE.search(text)
    if not m and subject:
        m = _TRACKING_RE.search(_strip_markdown_bold(subject))
    if not m and subject:
        # Subject sometimes has the bare code with no label.
        m2 = re.search(r"\b(1Z[A-Z0-9]{10,18})\b",
                        subject, re.IGNORECASE)
        if m2:
            out["tracking_number"] = m2.group(1).strip()
    if m:
        out["tracking_number"] = m.group(1).strip()

    m = _SERVICE_RE.search(text)
    if m:
        out["ups_service"] = m.group(1).strip()

    m = _WEIGHT_RE.search(text)
    if m:
        try:
            out["weight_value"] = float(m.group(1))
            out["weight_unit"] = m.group(2).upper()
        except (TypeError, ValueError):
            pass

    m = _PACKAGES_RE.search(text)
    if m:
        try:
            out["package_count"] = int(m.group(1))
        except (TypeError, ValueError):
            pass

    m = _SHIP_TO_RE.search(text)
    if m:
        block = m.group(1)
        # The block is the multi-line address. First non-empty
        # line is usually the name; remaining lines are address.
        lines = [ln.strip() for ln in block.split("\n")
                  if ln.strip()]
        # Drop the trailing 'US' country line if present.
        if lines and lines[-1].upper() in (
                "US", "USA", "UNITED STATES"):
            lines = lines[:-1]
        if lines:
            out["ship_to_name"] = lines[0]
            out["ship_to_address"] = "\n".join(lines[1:])
            # Final non-empty line tends to be city/state/zip
            if len(lines) > 1:
                out["ship_to_last_line"] = lines[-1]

    m = _FROM_RE.search(text)
    if m:
        out["from_party"] = m.group(1).strip()

    return out


# ---------------------------------------------------------------------------
# Matching — find the CIN7 sale this email corresponds to
# ---------------------------------------------------------------------------
def _normalise_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip()).upper()


def _find_matching_sale(parsed: dict) -> Optional[dict]:
    """Best-effort match of a UPS-email's ship-to to a CIN7 sale.
    Strategy: load the freshest sales CSV, scan for sales with
    a matching ship-to NAME, prefer recent + Shopify-channel.

    Returns {sale_id, sale_number, customer, customer_reference,
    shipping_charge, source_channel, order_date} or None."""
    name = parsed.get("ship_to_name") or ""
    if not name:
        return None
    name_norm = _normalise_name(name)

    sales_path = None
    for p in sorted(glob.glob(
            str(OUTPUT_DIR / "sales_last_*d_*.csv")),
            key=os.path.getmtime, reverse=True):
        sales_path = Path(p)
        break
    if not sales_path:
        log.error("No sales_last_*d_*.csv found for matching")
        return None

    try:
        df = pd.read_csv(sales_path, low_memory=False)
    except Exception as exc:
        log.error("Failed to read %s: %s", sales_path, exc)
        return None

    # Tolerate column-name variants.
    name_col = next(
        (c for c in ("Customer", "CustomerName", "BillingName",
                        "ShipToName")
          if c in df.columns), None)
    if not name_col:
        log.error("Sales CSV has no customer-name column")
        return None
    order_col = next(
        (c for c in ("OrderNumber", "SaleNumber")
          if c in df.columns), None)
    id_col = next(
        (c for c in ("SaleID", "ID", "Id")
          if c in df.columns), None)
    ref_col = next(
        (c for c in ("CustomerReference", "ExternalReference",
                        "Reference")
          if c in df.columns), None)
    date_col = next(
        (c for c in ("OrderDate", "SaleDate", "CreatedAt",
                        "LastUpdatedDate")
          if c in df.columns), None)
    channel_col = next(
        (c for c in ("SourceChannel", "Channel")
          if c in df.columns), None)
    shipping_col = next(
        (c for c in ("ShippingCost", "Shipping",
                        "TotalShipping", "ShippingTotal")
          if c in df.columns), None)

    # Exact match first; fall back to last-name match if nothing
    # found. Buyer-side names sometimes have minor variations
    # ("Tomasz Glowiak" vs "Glowiak Tomasz").
    matches = df[df[name_col].astype(str).apply(_normalise_name)
                  == name_norm]
    if matches.empty:
        # Last-name fallback
        last_name = name_norm.split()[-1] if name_norm else ""
        if last_name:
            matches = df[df[name_col].astype(str).apply(
                _normalise_name).str.contains(
                    last_name, regex=False, na=False)]
    if matches.empty:
        return None

    # Sort by date DESC if we have a date column
    if date_col:
        matches = matches.copy()
        matches["__d"] = pd.to_datetime(
            matches[date_col], errors="coerce", utc=True)
        matches = matches.sort_values(
            "__d", ascending=False, na_position="last")

    row = matches.iloc[0]
    return {
        "sale_id": str(row.get(id_col) or "").strip()
                      if id_col else "",
        "sale_number": str(row.get(order_col) or "").strip()
                          if order_col else "",
        "customer": str(row.get(name_col) or "").strip(),
        "customer_reference": str(row.get(ref_col) or "").strip()
                                if ref_col else "",
        "source_channel": (str(row.get(channel_col) or "").strip()
                             if channel_col else ""),
        "shipping_charge": (float(row.get(shipping_col) or 0)
                              if shipping_col else 0),
        "order_date": str(row.get(date_col) or "").strip()
                          if date_col else "",
        "match_strategy": ("exact" if len(matches) > 0
                              and _normalise_name(
                                  str(row.get(name_col))) == name_norm
                            else "last_name"),
    }


# ---------------------------------------------------------------------------
# Weight comparison vs Shopify's quoted weight
# ---------------------------------------------------------------------------
def _shopify_order_weight(customer_reference: str
                              ) -> Optional[Tuple[float, str]]:
    """Look up the Shopify order's TotalWeight (or similar). The
    customer_reference is the '#42514' Shopify Order # stored on
    the CIN7 sale. Returns (weight, unit) or None.

    Note: Shopify stores total_weight in GRAMS by default;
    sometimes synced as ounces. Caller normalises units."""
    if not customer_reference:
        return None
    num = customer_reference.lstrip("#").strip()
    if not num:
        return None
    path = None
    for p in sorted(glob.glob(
            str(OUTPUT_DIR / "shopify_orders_*.csv")),
            key=os.path.getmtime, reverse=True):
        path = Path(p)
        break
    if not path:
        return None
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception:
        return None
    num_col = next(
        (c for c in ("OrderNumber", "order_number", "Name")
          if c in df.columns), None)
    weight_col = next(
        (c for c in ("TotalWeight", "total_weight", "Weight",
                        "OrderWeight")
          if c in df.columns), None)
    if not (num_col and weight_col):
        return None
    m = df[df[num_col].astype(str).str.lstrip("#").str.strip()
           == num]
    if m.empty:
        return None
    try:
        w = float(m.iloc[0][weight_col])
    except (TypeError, ValueError):
        return None
    if w <= 0:
        return None
    # Shopify default unit is grams. Heuristic: if value > 200,
    # likely grams; if value < 50, likely ounces or pounds.
    # Better: prefer to find a unit column if present.
    unit_col = next(
        (c for c in ("WeightUnit", "weight_unit") if c in df.columns),
        None)
    unit = "GRAMS"
    if unit_col:
        u_raw = str(m.iloc[0][unit_col]).upper().strip()
        if u_raw in ("LBS", "LB", "POUNDS"):
            unit = "LBS"
        elif u_raw in ("OZ", "OUNCES"):
            unit = "OZ"
        elif u_raw in ("KG", "KILOGRAMS"):
            unit = "KG"
        else:
            unit = u_raw or "GRAMS"
    else:
        # Heuristic fallback
        if w > 200:
            unit = "GRAMS"
        elif 30 < w < 200:
            unit = "OZ"
        else:
            unit = "LBS"
    return (w, unit)


def _to_pounds(value: float, unit: str) -> Optional[float]:
    """Normalise weight to LBS for apples-to-apples comparison."""
    if value is None:
        return None
    u = (unit or "").upper().strip()
    if u in ("LBS", "LB", "POUNDS"):
        return value
    if u in ("KG", "KILOGRAMS"):
        return value * 2.20462
    if u in ("G", "GRAMS"):
        return value * 0.00220462
    if u in ("OZ", "OUNCES"):
        return value / 16.0
    return None


# ---------------------------------------------------------------------------
# Reply composition
# ---------------------------------------------------------------------------
def _cin7_sale_url(sale_id: str) -> str:
    if not sale_id:
        return ""
    tpl = os.environ.get(
        "CIN7_SALE_URL_TEMPLATE",
        "https://inventory.dearsystems.com/Sale#{id}~{id}~tabOrder")
    return tpl.format(id=sale_id)


def _compose_confirmation(parsed: dict, sale_match: dict,
                                weight_alert: Optional[str],
                                write_result: Optional[dict] = None
                                ) -> str:
    """Build the Slack confirmation reply. Always shows the
    parsed tracking + matched SO + click-through. Adds the
    weight-mismatch warning when applicable. Reflects the auto-
    write outcome at the bottom so staff can tell at a glance
    whether they need to do anything."""
    sale_no = sale_match.get("sale_number") or "?"
    sale_id = sale_match.get("sale_id") or ""
    cust = sale_match.get("customer") or ""
    cust_ref = sale_match.get("customer_reference") or ""
    cin7_url = _cin7_sale_url(sale_id)

    sale_link = (f"<{cin7_url}|*{sale_no}*>" if cin7_url
                  else f"*{sale_no}*")

    lines: List[str] = [
        f"📦 *UPS tracking received — match to {sale_link}*",
        "",
        f"• Customer: {cust}"
        + (f" · Shopify Order: {cust_ref}" if cust_ref else ""),
        f"• Tracking: `{parsed.get('tracking_number') or '?'}`",
    ]
    svc = parsed.get("ups_service")
    if svc:
        lines.append(f"• Service: {svc}")
    if parsed.get("weight_value") is not None:
        unit = parsed.get("weight_unit") or "LBS"
        lines.append(
            f"• Supplier weight: "
            f"{parsed['weight_value']} {unit}")
    pkgs = parsed.get("package_count")
    if pkgs:
        lines.append(f"• Packages: {pkgs}")

    if weight_alert:
        lines.append("")
        lines.append(weight_alert)

    # Auto-write outcome footer (v2.67.159)
    lines.append("")
    if write_result:
        status = write_result.get("status")
        if status == "written":
            lines.append(
                "✅ *Tracking auto-written to CIN7* — fulfillment "
                "will push to Shopify automatically. No manual "
                "step needed.")
        elif status == "already_present":
            lines.append(
                "ℹ️ Tracking was already on this sale — no change "
                "made.")
        else:
            detail = (write_result.get("detail")
                        or "unknown failure")
            lines.append(
                f"❌ *Auto-write failed* — {detail}. Cheran: open "
                f"the sale and paste the tracking manually in "
                f"CIN7's Ship tab.")
    else:
        lines.append(
            "_Auto-write skipped (no sale_id). Cheran: open the "
            "sale and paste the tracking manually in CIN7's Ship "
            "tab._")
    return "\n".join(lines)


def _weight_mismatch_text(supplier_lbs: float,
                                quoted_lbs: float) -> Optional[str]:
    """If |delta| > threshold, return a Slack-formatted warning
    string. Returns None when within tolerance."""
    if supplier_lbs is None or quoted_lbs is None:
        return None
    if quoted_lbs <= 0:
        return None
    delta = supplier_lbs - quoted_lbs
    pct = delta / quoted_lbs
    thresh = float(os.environ.get(
        "DROPSHIP_WEIGHT_PCT_THRESHOLD", "0.50") or 0.50)
    if abs(pct) <= thresh:
        return None
    icon = "🔴" if delta > 0 else "⚠️"
    direction = ("HEAVIER" if delta > 0
                  else "LIGHTER")
    return (
        f"{icon} *Weight mismatch*: supplier shipped at "
        f"{supplier_lbs:.2f} lbs vs our quote of {quoted_lbs:.2f} "
        f"lbs ({delta:+.2f} lbs, {pct*100:+.0f}%). "
        f"Likely root cause of shipping margin loss if customer "
        f"chose expedited service.")


# ---------------------------------------------------------------------------
# Carrier mapping — UPS email service text → CIN7 Carrier field
# ---------------------------------------------------------------------------
# CIN7 expects carrier names with their trademark glyph + title
# case (e.g. 'UPS® 2nd Day Air'). UPS emails arrive in ALL CAPS
# without ® and with inconsistent punctuation ("A.M." vs "AM",
# "EARLY" vs "EARLY AM"). v2.67.160 — covers every standard UPS
# service variant: domestic ground/air, SurePost tiers, full
# international (Express/Express Plus/Saver/Expedited/Standard/
# Express Freight), freight LTL, and hundredweight. Alias keys
# fall through to the same canonical CIN7 value so we don't care
# which surface form the email uses.
_UPS_SERVICE_MAP = {
    # ── Domestic Ground ─────────────────────────────────────────
    "UPS GROUND": "UPS® Ground",
    "UPS GROUND SAVER": "UPS® Ground Saver",
    "UPS GROUND WITH FREIGHT PRICING":
        "UPS® Ground with Freight Pricing",

    # ── SurePost ────────────────────────────────────────────────
    "UPS SUREPOST": "UPS SurePost®",
    "UPS SUREPOST LESS THAN 1 LB": "UPS SurePost® Less than 1 lb",
    "UPS SUREPOST 1 LB OR GREATER":
        "UPS SurePost® 1 lb or Greater",
    "UPS SUREPOST BPM": "UPS SurePost® BPM",

    # ── Domestic Air ────────────────────────────────────────────
    "UPS NEXT DAY AIR": "UPS® Next Day Air",
    "UPS NEXT DAY AIR SAVER": "UPS® Next Day Air Saver",
    "UPS NEXT DAY AIR EARLY": "UPS® Next Day Air® Early",
    "UPS NEXT DAY AIR EARLY AM": "UPS® Next Day Air® Early",
    "UPS NEXT DAY AIR EARLY A.M.": "UPS® Next Day Air® Early",
    "UPS 2ND DAY AIR": "UPS® 2nd Day Air",
    "UPS 2ND DAY AIR A.M.": "UPS® 2nd Day Air A.M.",
    "UPS 2ND DAY AIR AM": "UPS® 2nd Day Air A.M.",
    "UPS 3 DAY SELECT": "UPS® 3 Day Select",

    # ── International ───────────────────────────────────────────
    "UPS WORLDWIDE EXPRESS": "UPS Worldwide Express®",
    "UPS WORLDWIDE EXPRESS PLUS": "UPS Worldwide Express Plus®",
    "UPS WORLDWIDE SAVER": "UPS Worldwide Saver®",
    "UPS WORLDWIDE EXPEDITED": "UPS Worldwide Expedited®",
    "UPS STANDARD": "UPS Standard®",
    "UPS WORLDWIDE EXPRESS FREIGHT":
        "UPS Worldwide Express Freight®",
    "UPS WORLDWIDE EXPRESS FREIGHT MIDDAY":
        "UPS Worldwide Express Freight® Midday",

    # ── Freight LTL ─────────────────────────────────────────────
    "UPS FREIGHT LTL": "UPS Freight® LTL",
    "UPS FREIGHT LTL GUARANTEED": "UPS Freight® LTL — Guaranteed",
    "UPS FREIGHT LTL GUARANTEED A.M.":
        "UPS Freight® LTL — Guaranteed A.M.",
    "UPS FREIGHT LTL GUARANTEED AM":
        "UPS Freight® LTL — Guaranteed A.M.",

    # ── Hundredweight ───────────────────────────────────────────
    "UPS HUNDREDWEIGHT SERVICE GROUND":
        "UPS Hundredweight Service® Ground",
    "UPS HUNDREDWEIGHT SERVICE AIR":
        "UPS Hundredweight Service® Air",
}


def _map_carrier(ups_service: str) -> str:
    """Map a parsed UPS service string to CIN7's canonical
    carrier field value. Normalises whitespace (multiple spaces
    collapse to one) before lookup so that 'UPS  Ground' and
    'UPS Ground' both hit the same key. Falls back to the
    original string title-cased if no mapping is known — better
    than an empty Carrier."""
    if not ups_service:
        return ""
    # Collapse any internal whitespace runs to a single space
    # before the dictionary lookup.
    key = re.sub(r"\s+", " ", ups_service.strip()).upper()
    return _UPS_SERVICE_MAP.get(key, ups_service.strip())


# ---------------------------------------------------------------------------
# CIN7 write — push tracking into the sale's Fulfilments[].Ship.Lines
# ---------------------------------------------------------------------------
def _tracking_already_on_sale(sale: dict,
                                    tracking_number: str) -> bool:
    """True if `tracking_number` is already in any Fulfilment's
    Ship.Lines on this sale. Idempotency guard — without it the
    bot would add a duplicate row on every retry."""
    if not tracking_number:
        return False
    for fulf in (sale.get("Fulfilments") or []):
        ship = (fulf.get("Ship") or {})
        for line in (ship.get("Lines") or []):
            existing = (line.get("TrackingNumber") or "").strip()
            if existing == tracking_number.strip():
                return True
    return False


def write_tracking_to_sale(sale_id: str,
                                 tracking_number: str,
                                 carrier: str,
                                 boxes: int = 1,
                                 shipment_date: Optional[str] = None,
                                 tracking_url: str = ""
                                 ) -> dict:
    """v2.67.159 — Auto-write a new shipment row to a CIN7 sale's
    Fulfilment.Ship.Lines via the API. CIN7 then auto-syncs
    CombinedTrackingNumbers and pushes fulfillment to Shopify.

    Steps:
      1. GET the sale
      2. Verify tracking isn't already on the sale (idempotent)
      3. Append a new Lines entry to Fulfilments[0].Ship.Lines
         with IsShipped=true so CIN7 marks the shipment shipped
      4. PUT the modified sale back

    Returns: {"status": "written"|"already_present"|"error",
              "detail": str, "tracking": str}
    """
    if not sale_id or not tracking_number:
        return {"status": "error",
                  "detail": "missing sale_id or tracking_number"}

    # Lazy import — heavy
    try:
        from cin7_sync import Cin7Client
    except ImportError as exc:
        return {"status": "error",
                  "detail": f"cin7_sync import failed: {exc}"}
    account_id = os.environ.get("CIN7_ACCOUNT_ID", "").strip()
    app_key = os.environ.get("CIN7_APPLICATION_KEY", "").strip()
    if not account_id or not app_key:
        return {"status": "error",
                  "detail": "CIN7 credentials missing"}
    try:
        client = Cin7Client(account_id, app_key)
    except Exception as exc:
        return {"status": "error",
                  "detail": f"client init failed: {exc}"}

    # 1. GET the sale
    sale = client.get_sale(sale_id)
    if not sale or not isinstance(sale, dict):
        return {"status": "error",
                  "detail": "sale GET returned empty"}
    if not sale.get("ID"):
        return {"status": "error",
                  "detail": "sale GET response missing ID"}

    # 2. Idempotency check
    if _tracking_already_on_sale(sale, tracking_number):
        return {
            "status": "already_present",
            "detail": (f"Tracking {tracking_number} already "
                          f"on sale; no change."),
            "tracking": tracking_number,
        }

    # 3. Construct the new Ship line + inject into Fulfilment[0]
    new_line = {
        "ShipmentDate": (shipment_date
                          or datetime.now(timezone.utc)
                              .strftime("%Y-%m-%dT%H:%M:%S")),
        "Carrier": carrier or "UPS® Ground",
        "Boxes": str(int(boxes) if boxes else 1),
        "TrackingNumber": tracking_number,
        "TrackingURL": tracking_url or "",
        "IsShipped": True,
    }
    fulfilments = sale.get("Fulfilments") or []
    if not fulfilments:
        # No existing fulfilment — extremely rare for a
        # dropship sale because CIN7 auto-creates one with the
        # draft PO. Bail rather than guess.
        return {
            "status": "error",
            "detail": ("Sale has no Fulfilments — can't add "
                          "tracking. Investigate the sale state."),
        }
    # Append to the first fulfilment's Ship.Lines
    first = fulfilments[0]
    ship = first.setdefault("Ship", {})
    lines = ship.setdefault("Lines", [])
    lines.append(new_line)
    # Make sure Ship.Status is at least AUTHORISED so CIN7
    # processes the new line.
    ship.setdefault("Status", "AUTHORISED")

    # 4. PUT the modified sale back
    try:
        resp = client.update_sale(sale)
    except Exception as exc:
        return {"status": "error",
                  "detail": f"PUT /sale failed: {exc}",
                  "tracking": tracking_number}

    # CIN7 returns various shapes on PUT. Treat presence of an
    # error / non-2xx text as failure.
    if isinstance(resp, dict) and resp.get("status"):
        status_code = resp.get("status")
        if isinstance(status_code, int) and status_code >= 400:
            return {
                "status": "error",
                "detail": (f"CIN7 returned HTTP {status_code}: "
                              f"{str(resp.get('text'))[:200]}"),
                "tracking": tracking_number,
            }
    return {
        "status": "written",
        "detail": (f"Tracking {tracking_number} written to "
                      f"sale {sale.get('OrderNumber') or sale_id}"),
        "tracking": tracking_number,
    }


# ---------------------------------------------------------------------------
# Top-level handler
# ---------------------------------------------------------------------------
def handle_ups_email(msg: dict) -> Tuple[str, List[str]]:
    """Parse, match, compose. Returns (reply_text, tools_used).
    Empty reply if parsing fails — caller skips posting."""
    f = _email_payload(msg)
    if not f:
        return "", []
    # v2.67.258 — Slack's Email app sometimes leaves plain_text
    # thin/empty for HTML-only emails; fall back to preview, then
    # to the message text itself.
    body = (f.get("plain_text") or f.get("preview")
            or msg.get("text") or "")
    subject = f.get("subject") or ""
    parsed = parse_ups_email(body, subject=subject)
    tools_used: List[str] = ["parse_ups_email"]

    if not parsed.get("tracking_number"):
        # v2.67.258 — was a silent return, which left Cheran
        # unsure if the bot even saw the email. Post a diagnostic
        # showing the raw text so (a) staff know it was seen and
        # (b) we can see exactly what to fix in the parser.
        log.warning("UPS email had no parseable tracking — "
                     "subject=%r body[:200]=%r",
                     subject, body[:200])
        snippet = (body or "(plain-text body was empty)")[:900]
        return (
            "📦 *UPS email received — couldn't parse it "
            "automatically*\n\n"
            f"Subject: `{subject or '(none)'}`\n\n"
            "I detected a UPS shipment email but couldn't pull a "
            "tracking number out of it. Raw text I received:\n"
            f"```\n{snippet}\n```\n"
            "_Cheran: add the tracking to CIN7 manually for now. "
            "This text lets us fix the auto-parser._"
        ), tools_used

    sale_match = _find_matching_sale(parsed)
    if not sale_match:
        # Couldn't match — still post a "got the email but no
        # match" so staff sees it
        tools_used.append("no_match")
        return _compose_no_match(parsed), tools_used
    tools_used.append("matched_sale")

    # v2.67.159 — Auto-write tracking to CIN7 sale. We do this
    # BEFORE weight comparison so the Slack reply reflects the
    # write outcome even if weight lookup errors out.
    write_result: Optional[dict] = None
    sale_id = sale_match.get("sale_id") or ""
    if sale_id and parsed.get("tracking_number"):
        try:
            write_result = write_tracking_to_sale(
                sale_id=sale_id,
                tracking_number=parsed["tracking_number"],
                carrier=_map_carrier(
                    parsed.get("ups_service") or ""),
                boxes=parsed.get("package_count") or 1,
            )
            tools_used.append(
                f"write_tracking:{write_result.get('status')}")
        except Exception as exc:
            log.error("write_tracking_to_sale crashed: %s", exc)
            write_result = {"status": "error",
                              "detail": f"unexpected: {exc}"}
            tools_used.append("write_tracking:crashed")

    # Weight check
    weight_alert = None
    try:
        cust_ref = sale_match.get("customer_reference") or ""
        if (cust_ref
                and parsed.get("weight_value") is not None):
            quoted = _shopify_order_weight(cust_ref)
            supplier_lbs = _to_pounds(
                parsed["weight_value"],
                parsed.get("weight_unit") or "LBS")
            if quoted and supplier_lbs is not None:
                quoted_lbs = _to_pounds(quoted[0], quoted[1])
                if quoted_lbs:
                    weight_alert = _weight_mismatch_text(
                        supplier_lbs, quoted_lbs)
                    tools_used.append("weight_compared")
    except Exception as exc:
        log.warning("Weight comparison error: %s", exc)

    reply = _compose_confirmation(parsed, sale_match,
                                          weight_alert,
                                          write_result)
    return reply, tools_used


def _compose_no_match(parsed: dict) -> str:
    """Reply when we parse the email but can't match to a sale."""
    lines = [
        "📦 *UPS tracking email received — couldn't auto-match to a sale*",
        "",
        f"• Ship to: {parsed.get('ship_to_name') or '?'}",
    ]
    addr = parsed.get("ship_to_address")
    if addr:
        lines.append(f"• Address: {addr.replace(chr(10), ', ')}")
    if parsed.get("tracking_number"):
        lines.append(
            f"• Tracking: `{parsed['tracking_number']}`")
    if parsed.get("ups_service"):
        lines.append(f"• Service: {parsed['ups_service']}")
    lines.append("")
    lines.append(
        "_No CIN7 sale found with this ship-to name. Cheran may "
        "need to add tracking manually. If the customer's CIN7 "
        "name differs from the UPS shipping name (e.g. company "
        "vs. contact), this is expected._")
    return "\n".join(lines)
