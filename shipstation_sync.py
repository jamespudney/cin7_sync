"""ShipStation sync (v2.67.54).

Pulls shipment records from the ShipStation v1 API into local CSVs
that the Streamlit app + AI Assistant can read. Three modes:

  python shipstation_sync.py recent --days 1     # NearSync window
  python shipstation_sync.py recent --days 7     # Daily catch-up
  python shipstation_sync.py full --days 1825    # First-time backfill
                                                   (5 years; can take
                                                   hours on big stores)

Output:
  output/shipments_last_<N>d_<timestamp>.csv  (rolling-window pulls)
  output/shipments_full.csv                   (full-history dump)

Rolling-window files are merged the same way `purchase_lines_*` are
merged in app.py — the longest window file is the base, newer
shorter-window files patch it. So the AI's get_shipping_details
tool (and Monthly Metrics' shipping-cost row) sees a continuously
fresh dataset without a per-tool reload.

Auth: ShipStation v1 uses HTTP Basic with API key + secret. Set
SHIPSTATION_API_KEY and SHIPSTATION_API_SECRET in .env. Gracefully
no-ops when env vars aren't set so the daily sync chain doesn't
fail in dev / pre-config environments.

Why v1 not v2: the v1 API is the well-documented one with stable
shipment-list endpoints and predictable pagination. v2 is the new
"Marketplace" API and not all accounts have access. v1 is going to
keep working for the foreseeable future per ShipStation's own
deprecation notes.

Rate limiting: ShipStation returns RateLimit-Remaining / RateLimit-
Reset headers. We respect them — sleep until reset when remaining
hits zero.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv

# Project paths — same module the cin7_sync uses, so OUTPUT_DIR
# resolves to the right disk on Render (/data/output) AND on Windows
# (C:/Tools/cin7_sync/output) without per-environment conditionals.
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from data_paths import OUTPUT_DIR  # noqa: E402


BASE_URL = os.environ.get("SHIPSTATION_BASE_URL",
                            "https://ssapi.shipstation.com").rstrip("/")
DEFAULT_PAGE_SIZE = 500       # ShipStation max per page is 500
DEFAULT_TIMEOUT = 30.0
MAX_RETRIES = 5

LOG_FORMAT = "%(asctime)s  %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("shipstation_sync")


# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------


def _build_session(api_key: str, api_secret: str) -> requests.Session:
    """Build a requests.Session preconfigured with Basic auth + JSON
    headers. Why a session: keeps connection pooling alive across the
    hundreds of paginated calls a backfill makes."""
    if not api_key or not api_secret:
        raise RuntimeError(
            "Missing ShipStation credentials. Set "
            "SHIPSTATION_API_KEY and SHIPSTATION_API_SECRET in .env.")
    s = requests.Session()
    token = b64encode(f"{api_key}:{api_secret}".encode("utf-8")).decode("ascii")
    s.headers.update({
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "cin7_sync-shipstation/2.67.54",
    })
    return s


def _respect_rate_limits(resp: requests.Response) -> None:
    """ShipStation returns X-Rate-Limit-Remaining + X-Rate-Limit-Reset
    headers. When Remaining hits zero, sleep until Reset. Why we
    don't just naïvely sleep N seconds per call: ShipStation's quota
    is much more generous than CIN7's (40 req/min for most plans),
    and burning the budget on artificial delays would slow a
    multi-thousand-page backfill 10x. Honouring the headers is the
    right behaviour."""
    try:
        remaining = int(resp.headers.get("X-Rate-Limit-Remaining", "1"))
        reset_in = int(resp.headers.get("X-Rate-Limit-Reset", "0"))
    except (TypeError, ValueError):
        return
    if remaining <= 1 and reset_in > 0:
        log.info("  hit rate limit; sleeping %ss for reset", reset_in)
        time.sleep(reset_in + 1)


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


def _paginate_shipments(session: requests.Session,
                         params: Dict[str, Any]
                         ) -> Iterable[Dict[str, Any]]:
    """Yield every shipment matching the given query params. Handles
    ShipStation's `page` + `pages` response shape. Retries on 429 /
    5xx with exponential backoff."""
    page = 1
    while True:
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = session.get(
                    f"{BASE_URL}/shipments",
                    params={**params, "page": page,
                            "pageSize": DEFAULT_PAGE_SIZE},
                    timeout=DEFAULT_TIMEOUT)
            except requests.RequestException as exc:
                if attempt >= MAX_RETRIES:
                    raise
                wait = 2 ** attempt
                log.warning("Network error %s — retrying in %ds",
                              exc, wait)
                time.sleep(wait)
                continue

            if resp.status_code == 429:
                # Honour Retry-After header if present, otherwise 60s.
                wait = int(resp.headers.get("Retry-After", "60"))
                log.warning("429 rate limit — sleeping %ss", wait)
                time.sleep(wait)
                continue

            if 500 <= resp.status_code < 600 and attempt < MAX_RETRIES:
                wait = 2 ** attempt
                log.warning("Server %s — retrying in %ds",
                              resp.status_code, wait)
                time.sleep(wait)
                continue

            if not resp.ok:
                raise RuntimeError(
                    f"ShipStation API {resp.status_code} on /shipments: "
                    f"{resp.text[:300]}")

            _respect_rate_limits(resp)
            payload = resp.json() or {}
            break

        shipments = payload.get("shipments") or []
        for s in shipments:
            yield s

        total_pages = payload.get("pages") or 1
        if page >= total_pages or not shipments:
            log.info("  Page %d/%d done (%d shipments).",
                       page, total_pages, len(shipments))
            return
        log.info("  Page %d/%d (%d shipments).",
                   page, total_pages, len(shipments))
        page += 1


# ---------------------------------------------------------------------------
# Row flattening
# ---------------------------------------------------------------------------


def _flatten_shipment(s: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a ShipStation /shipments response row into the column
    shape we want in CSV. Ship-to address is folded into a few
    flat columns rather than nested — pandas reads CSVs better
    that way and the AI tool builds clean responses without
    walking nested structures."""
    if not isinstance(s, dict):
        return {}
    ship_to = s.get("shipTo") or {}
    if not isinstance(ship_to, dict):
        ship_to = {}
    weight = s.get("weight") or {}
    if not isinstance(weight, dict):
        weight = {}
    dims = s.get("dimensions") or {}
    if not isinstance(dims, dict):
        dims = {}
    items = s.get("shipmentItems") or []
    item_summary = ""
    if isinstance(items, list) and items:
        # Compact summary: "qty × SKU (name)" per line, semicolon-joined.
        parts = []
        for it in items:
            if not isinstance(it, dict):
                continue
            qty = it.get("quantity")
            sku = it.get("sku") or ""
            name = (it.get("name") or "")[:60]
            parts.append(f"{qty}× {sku} ({name})")
        item_summary = "; ".join(parts)
    return {
        "ShipmentID": s.get("shipmentId"),
        "OrderID": s.get("orderId"),
        "OrderNumber": s.get("orderNumber"),
        "OrderKey": s.get("orderKey"),
        "UserID": s.get("userId"),
        "CustomerEmail": s.get("customerEmail"),
        "CustomerName": ship_to.get("name") or s.get("customerName"),
        "ShipDate": s.get("shipDate"),
        "CreateDate": s.get("createDate"),
        "VoidDate": s.get("voidDate"),
        "Voided": s.get("voided"),
        "MarketplaceNotified": s.get("marketplaceNotified"),
        "TrackingNumber": s.get("trackingNumber"),
        "CarrierCode": s.get("carrierCode"),
        "ServiceCode": s.get("serviceCode"),
        "PackageCode": s.get("packageCode"),
        "Confirmation": s.get("confirmation"),
        "WarehouseID": s.get("warehouseId"),
        "ShipmentCost": s.get("shipmentCost"),
        "InsuranceCost": s.get("insuranceCost"),
        "Notes": (s.get("customerNotes") or s.get("internalNotes")
                   or s.get("gift") and "GIFT" or ""),
        "InternalNotes": s.get("internalNotes"),
        "CustomerNotes": s.get("customerNotes"),
        "GiftMessage": s.get("giftMessage"),
        "ShipToCity": ship_to.get("city"),
        "ShipToState": ship_to.get("state"),
        "ShipToPostal": ship_to.get("postalCode"),
        "ShipToCountry": ship_to.get("country"),
        "ShipToCompany": ship_to.get("company"),
        "ShipToStreet1": ship_to.get("street1"),
        "WeightValue": weight.get("value"),
        "WeightUnits": weight.get("units"),
        "DimensionsLength": dims.get("length"),
        "DimensionsWidth": dims.get("width"),
        "DimensionsHeight": dims.get("height"),
        "DimensionsUnits": dims.get("units"),
        "ItemCount": len(items) if isinstance(items, list) else 0,
        "ItemSummary": item_summary,
    }


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------


def _write_csv(name: str, rows: List[Dict[str, Any]]) -> Path:
    """Write rows out as a timestamped CSV in OUTPUT_DIR. Same
    naming convention cin7_sync uses, so the existing
    `_dir_fingerprint` cache-key pattern in app.py picks up new
    files automatically."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    out_path = OUTPUT_DIR / f"{name}_{ts}.csv"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if not rows:
        # Write an empty CSV with a header so the merge loader in
        # app.py doesn't trip on a missing-column DataFrame.
        with out_path.open("w", encoding="utf-8", newline="") as f:
            f.write("ShipmentID,OrderNumber,ShipDate,ShipmentCost\n")
        log.info("Wrote empty file %s (0 shipments)", out_path.name)
        return out_path
    fieldnames = list(rows[0].keys())
    # Union of all keys across rows for safety (rows can have
    # different shapes if some shipments are missing fields).
    seen = set(fieldnames)
    for r in rows[1:]:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames,
                                  extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    log.info("Wrote %s (%d shipments)", out_path.name, len(rows))
    return out_path


# ---------------------------------------------------------------------------
# Sync modes
# ---------------------------------------------------------------------------


def sync_recent(session: requests.Session, days: int) -> Path:
    """Pull shipments with createDate within the last N days. This is
    what NearSync (--days 1) and Daily Sync (--days 7) call. The
    rolling-window file (shipments_last_<N>d_*.csv) is what
    `_load_longest_shipments()` in app.py merges with the full dump."""
    since_dt = datetime.now(timezone.utc) - timedelta(days=days)
    since_iso = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    log.info("Pulling ShipStation shipments since %s ...", since_iso)
    rows = []
    for s in _paginate_shipments(
            session,
            {"createDateStart": since_iso,
             "includeShipmentItems": "true"}):
        rows.append(_flatten_shipment(s))
    return _write_csv(f"shipments_last_{days}d", rows)


def sync_full(session: requests.Session, days: int = 1825) -> Path:
    """Full backfill — pull every shipment in the last N days
    (default ~5 years). This is the 'big sync' the user mentioned;
    expect 30-60 minutes on a busy account. After this completes,
    the daily NearSync keeps it current.

    Output: shipments_full.csv (NOT a rolling window — single
    historical dump). Subsequent runs overwrite it. The rolling
    `shipments_last_<N>d` files written by sync_recent get merged
    on top via the longest-window loader pattern in app.py."""
    since_dt = datetime.now(timezone.utc) - timedelta(days=days)
    since_iso = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    log.info("FULL BACKFILL — shipments since %s ...", since_iso)
    rows = []
    for s in _paginate_shipments(
            session,
            {"createDateStart": since_iso,
             "includeShipmentItems": "true"}):
        rows.append(_flatten_shipment(s))
    # Full file written without timestamp suffix so callers can find
    # it deterministically. Backup the previous one first.
    out_path = OUTPUT_DIR / "shipments_full.csv"
    if out_path.exists():
        backup = out_path.with_suffix(
            f".bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        out_path.rename(backup)
        log.info("Backed up previous shipments_full.csv to %s",
                   backup.name)
    if not rows:
        with out_path.open("w", encoding="utf-8", newline="") as f:
            f.write("ShipmentID,OrderNumber,ShipDate,ShipmentCost\n")
        log.info("Empty backfill written.")
        return out_path
    fieldnames = list(rows[0].keys())
    seen = set(fieldnames)
    for r in rows[1:]:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames,
                                  extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    log.info("Wrote %s (%d shipments)", out_path.name, len(rows))
    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Pull ShipStation shipments into local CSVs.")
    sub = p.add_subparsers(dest="cmd", required=True)
    rp = sub.add_parser("recent",
                          help="Rolling-window sync (NearSync / Daily).")
    rp.add_argument("--days", type=int, default=1,
                     help="Window in days. Default 1 (NearSync).")
    fp = sub.add_parser("full", help="Full backfill (slow).")
    fp.add_argument("--days", type=int, default=1825,
                     help="Backfill window in days (default ~5y).")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv(SCRIPT_DIR / ".env")
    args = build_arg_parser().parse_args(argv)

    api_key = os.environ.get("SHIPSTATION_API_KEY", "").strip()
    api_secret = os.environ.get("SHIPSTATION_API_SECRET", "").strip()
    if not api_key or not api_secret:
        log.warning("ShipStation env vars not set — skipping. Set "
                      "SHIPSTATION_API_KEY + SHIPSTATION_API_SECRET "
                      "to enable.")
        return 0

    session = _build_session(api_key, api_secret)

    if args.cmd == "recent":
        sync_recent(session, args.days)
    elif args.cmd == "full":
        sync_full(session, args.days)
    return 0


if __name__ == "__main__":
    sys.exit(main())
