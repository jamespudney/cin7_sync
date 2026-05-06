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


# v2.67.55+ — Two ShipStation APIs are supported. Auto-detect which
# one to use based on env vars:
#   v1 (legacy): HTTP Basic with SHIPSTATION_API_KEY + SHIPSTATION_API_SECRET.
#                Base: https://ssapi.shipstation.com.
#   v2 (newer):  Single token in `API-Key` header.
#                Base: https://api.shipstation.com/v2.
# v2's `/shipments` endpoint returns the same shape we want (with
# slightly renamed fields) — we normalise to the v1 column set in
# _flatten_shipment_v2 so the downstream CSV / AI tool layer doesn't
# need to know which API the data came from.
BASE_URL_V1 = os.environ.get("SHIPSTATION_BASE_URL",
                                "https://ssapi.shipstation.com").rstrip("/")
BASE_URL_V2 = os.environ.get("SHIPSTATION_V2_BASE_URL",
                                "https://api.shipstation.com/v2").rstrip("/")
# Backwards compat — old code references BASE_URL.
BASE_URL = BASE_URL_V1
DEFAULT_PAGE_SIZE = 500       # ShipStation max per page is 500
DEFAULT_TIMEOUT = 30.0
MAX_RETRIES = 5

LOG_FORMAT = "%(asctime)s  %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("shipstation_sync")


# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------


def _build_session(api_key: str,
                    api_secret: str = "",
                    api_version: str = "auto") -> Tuple[
                        requests.Session, str]:
    """Build a requests.Session for whichever ShipStation API is
    configured. Returns (session, version) where version is 'v1' or
    'v2' so callers can route to the right endpoint paths.

    Auto-detect rule: if api_secret is provided → v1 Basic auth
    (SHIPSTATION_API_KEY + SHIPSTATION_API_SECRET). Otherwise → v2
    API-Key header (SHIPSTATION_API_KEY only). Caller can force
    via api_version='v1' or api_version='v2'.

    Why two APIs: v1 (ssapi.shipstation.com) is the legacy
    well-documented one, basic-auth, broad endpoint coverage. v2
    (api.shipstation.com/v2) is ShipStation's newer API with a
    single token. As of 2026, both are supported by ShipStation
    but new accounts often only get v2 keys — hence auto-detect."""
    if not api_key:
        raise RuntimeError(
            "Missing SHIPSTATION_API_KEY. Set it in .env or Render "
            "env vars. v1 also needs SHIPSTATION_API_SECRET; v2 "
            "uses just the single API-Key.")
    s = requests.Session()
    if api_version == "auto":
        api_version = "v1" if api_secret else "v2"
    if api_version == "v1":
        token = b64encode(
            f"{api_key}:{api_secret}".encode("utf-8")).decode("ascii")
        s.headers.update({
            "Authorization": f"Basic {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "cin7_sync-shipstation/2.67.55",
        })
    else:  # v2
        s.headers.update({
            "API-Key": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "cin7_sync-shipstation/2.67.55",
        })
    return s, api_version


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
                         params: Dict[str, Any],
                         api_version: str = "v1"
                         ) -> Iterable[Dict[str, Any]]:
    """Yield every shipment matching the given query params. Handles
    both v1 and v2 response shapes. Retries on 429 / 5xx with
    exponential backoff.

    v1 response shape: `{shipments: [...], total, page, pages}`.
    v2 response shape: `{shipments: [...], links: {next: {href}},
    total, page, pages}` (ShipStation v2 still includes page/pages
    so we can use the same loop)."""
    base = BASE_URL_V2 if api_version == "v2" else BASE_URL_V1
    page = 1
    while True:
        attempt = 0
        while True:
            attempt += 1
            try:
                # v2 uses page_size (snake_case); v1 uses pageSize.
                page_param = ("page_size"
                              if api_version == "v2" else "pageSize")
                resp = session.get(
                    f"{base}/shipments",
                    params={**params, "page": page,
                            page_param: DEFAULT_PAGE_SIZE},
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


def _flatten_shipment_v2(s: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a ShipStation v2 /shipments response. v2 uses
    snake_case + slightly different field names. We normalise to
    the v1 column set so downstream consumers see a single shape.

    Notable v2 quirks confirmed against the live API on 2026-05-06:
      - shipment_number IS the CIN7 invoice number (e.g.
        'INV-52959'). Critical join key. Maps to OrderNumber so
        existing AI tool filters by order_number still work.
      - shipping_paid is a {currency, amount} dict, not a scalar.
        We extract amount.
      - tracking_number lives on /labels, NOT /shipments. The
        ENRICHMENT step in sync_recent/sync_full pulls /labels too
        and merges tracking_number / tracking_url into the row by
        shipment_id. _flatten_shipment_v2 returns a placeholder
        None — _merge_label_data fills it in.
      - customer_email is nested under ship_to.email (no top-level
        customer_email field as in v1).
      - shipment_status replaces v1's voided boolean; we map
        'cancelled' → voided=True for downstream compatibility."""
    if not isinstance(s, dict):
        return {}
    ship_to = s.get("ship_to") or {}
    if not isinstance(ship_to, dict):
        ship_to = {}
    weight = s.get("total_weight") or s.get("weight") or {}
    if not isinstance(weight, dict):
        weight = {}
    items = s.get("items") or s.get("shipment_items") or []
    item_summary = ""
    if isinstance(items, list) and items:
        parts = []
        for it in items:
            if not isinstance(it, dict):
                continue
            qty = it.get("quantity")
            sku = it.get("sku") or ""
            name = (it.get("name") or "")[:60]
            parts.append(f"{qty}× {sku} ({name})")
        item_summary = "; ".join(parts)

    # shipping_paid is a {currency, amount} dict in v2.
    shipping_paid = s.get("shipping_paid") or {}
    if not isinstance(shipping_paid, dict):
        shipping_paid = {}
    shipment_cost = shipping_paid.get("amount")
    amount_paid = s.get("amount_paid") or {}
    if not isinstance(amount_paid, dict):
        amount_paid = {}
    tax_paid = s.get("tax_paid") or {}
    if not isinstance(tax_paid, dict):
        tax_paid = {}

    # Map v2 status to v1 voided boolean.
    status = s.get("shipment_status") or ""
    voided = status in ("cancelled", "voided")

    return {
        "ShipmentID": s.get("shipment_id"),
        "OrderID": s.get("external_order_id"),
        "OrderNumber": s.get("shipment_number"),  # INV-XXX = CIN7 invoice
        "OrderKey": s.get("external_shipment_id"),
        "UserID": s.get("assigned_user"),
        "CustomerEmail": ship_to.get("email"),
        "CustomerName": ship_to.get("name"),
        "ShipDate": s.get("ship_date"),
        "CreateDate": s.get("created_at"),
        "VoidDate": (s.get("modified_at") if voided else None),
        "Voided": voided,
        "ShipmentStatus": status,            # v2-only column,
                                                # AI tool surfaces it
        "MarketplaceNotified": None,
        "TrackingNumber": None,              # filled by label merge
        "TrackingURL": None,                  # filled by label merge
        "CarrierCode": s.get("carrier_id"),
        "ServiceCode": s.get("service_code"),
        "RequestedService": s.get("requested_shipment_service"),
        "PackageCode": None,
        "Confirmation": s.get("confirmation"),
        "WarehouseID": s.get("warehouse_id"),
        "StoreID": s.get("store_id"),
        "ShipmentCost": shipment_cost,
        "AmountPaid": amount_paid.get("amount"),
        "TaxPaid": tax_paid.get("amount"),
        "Currency": (shipping_paid.get("currency")
                       or amount_paid.get("currency")),
        "InsuranceCost": None,
        "Notes": (s.get("internal_notes")
                   or s.get("notes_from_buyer") or ""),
        "InternalNotes": s.get("internal_notes"),
        "CustomerNotes": s.get("notes_from_buyer"),
        "GiftMessage": s.get("notes_for_gift"),
        "ShipToCity": (ship_to.get("city_locality")
                          or ship_to.get("city")),
        "ShipToState": (ship_to.get("state_province")
                          or ship_to.get("state")),
        "ShipToPostal": ship_to.get("postal_code"),
        "ShipToCountry": ship_to.get("country_code"),
        "ShipToCompany": ship_to.get("company_name"),
        "ShipToStreet1": ship_to.get("address_line1"),
        "WeightValue": weight.get("value"),
        "WeightUnits": weight.get("unit"),
        "Zone": s.get("zone"),
        "DimensionsLength": None,
        "DimensionsWidth": None,
        "DimensionsHeight": None,
        "DimensionsUnits": None,
        "ItemCount": len(items) if isinstance(items, list) else 0,
        "ItemSummary": item_summary,
    }


def _fetch_labels_index(session: requests.Session,
                          since_iso: str
                          ) -> Dict[str, Dict[str, Any]]:
    """Pull ShipStation v2 labels in the same date window and index
    them by shipment_id. The shipments endpoint doesn't carry
    tracking — it lives on labels. We fetch labels separately, then
    merge into the shipment rows.

    Why a separate fetch instead of per-shipment label calls: that
    would be 2× the API hits. v2's /labels endpoint accepts the
    same date filter and paginates the same way, so for ~equal
    label-per-shipment ratio we save half the rate-limit budget.

    Index value carries: tracking_number, tracking_url,
    tracking_status, carrier_code, label_id, voided, batch_id."""
    log.info("  Fetching labels for tracking-number enrichment...")
    idx: Dict[str, Dict[str, Any]] = {}
    # /labels has the same query shape as /shipments but a
    # different result_key. Inline pagination — the shared
    # paginator wouldn't help.
    # v2.67.55+ — /labels is materially slower than /shipments
    # (observed 30s timeout on first attempt with page_size=500).
    # Use a smaller page size + longer timeout + retries on
    # network errors. 100 per page is the v2 default and we've
    # seen it return reliably; 500 likely triggers a server-side
    # query that exceeds the request budget.
    LABELS_TIMEOUT = 90.0
    LABELS_PAGE_SIZE = 100
    LABELS_MAX_RETRIES = 3
    page = 1
    while True:
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = session.get(
                    f"{BASE_URL_V2}/labels",
                    params={"created_at_start": since_iso,
                              "page": page,
                              "page_size": LABELS_PAGE_SIZE},
                    timeout=LABELS_TIMEOUT)
                break
            except requests.RequestException as exc:
                if attempt >= LABELS_MAX_RETRIES:
                    log.warning("Label fetch network error %s "
                                  "after %d tries — abandoning label "
                                  "enrichment for this run",
                                  exc, attempt)
                    return idx
                wait = 2 ** attempt
                log.warning("Label fetch attempt %d failed (%s) — "
                              "retrying in %ds", attempt, exc, wait)
                time.sleep(wait)
                continue
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", "60"))
            log.warning("Label fetch 429 — sleeping %ss", wait)
            time.sleep(wait)
            continue
        if not resp.ok:
            log.warning("Label fetch %d on /labels: %s",
                          resp.status_code, resp.text[:200])
            return idx
        _respect_rate_limits(resp)
        payload = resp.json() or {}
        labels = payload.get("labels") or []
        for L in labels:
            if not isinstance(L, dict):
                continue
            sid = L.get("shipment_id")
            if not sid:
                continue
            # Skip voided labels — they have a tracking_number that's
            # no longer valid; we want the active label per shipment.
            if L.get("voided"):
                continue
            idx[sid] = {
                "TrackingNumber": L.get("tracking_number"),
                "TrackingURL": L.get("tracking_url"),
                "TrackingStatus": L.get("tracking_status"),
                "CarrierCode": L.get("carrier_code")
                                 or L.get("carrier_id"),
                "LabelID": L.get("label_id"),
                "BatchID": L.get("batch_id"),
            }
        total_pages = payload.get("pages") or 1
        log.info("    Labels page %d/%d (%d labels, %d indexed)",
                   page, total_pages, len(labels), len(idx))
        if page >= total_pages or not labels:
            return idx
        page += 1


def _merge_label_data(shipment_row: Dict[str, Any],
                        label_idx: Dict[str, Dict[str, Any]]
                        ) -> Dict[str, Any]:
    """Layer label fields onto a shipment row keyed by ShipmentID.
    No-op when the shipment hasn't been labelled yet. Carrier code
    from the label takes precedence over the shipment's carrier_id
    (which is an internal SE-XXXX ID; the label gives the actual
    carrier name like 'ups' / 'usps')."""
    sid = shipment_row.get("ShipmentID")
    if not sid:
        return shipment_row
    label = label_idx.get(sid)
    if not label:
        return shipment_row
    out = dict(shipment_row)
    if label.get("TrackingNumber"):
        out["TrackingNumber"] = label["TrackingNumber"]
    if label.get("TrackingURL"):
        out["TrackingURL"] = label["TrackingURL"]
    if label.get("CarrierCode"):
        # Replace the SE-XXXX carrier_id with a friendly carrier
        # code (ups/usps/fedex/dhl_express/etc).
        out["CarrierCode"] = label["CarrierCode"]
    out["TrackingStatus"] = label.get("TrackingStatus")
    out["LabelID"] = label.get("LabelID")
    return out


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


def _query_params(since_iso: str, api_version: str) -> Dict[str, Any]:
    """Build the version-appropriate filter for /shipments. v1 uses
    `createDateStart` + `includeShipmentItems`; v2 uses
    `created_at_start` + items are returned by default."""
    if api_version == "v2":
        return {"created_at_start": since_iso}
    return {"createDateStart": since_iso,
            "includeShipmentItems": "true"}


def sync_recent(session: requests.Session, days: int,
                  api_version: str = "v1") -> Path:
    """Pull shipments with createDate within the last N days. This is
    what NearSync (--days 1) and Daily Sync (--days 7) call. The
    rolling-window file (shipments_last_<N>d_*.csv) is what
    `_load_longest_shipments()` in app.py merges with the full dump.

    v2 only: also pulls /labels in the same date window and merges
    tracking_number / tracking_url / tracking_status onto each
    shipment row. v1 has tracking on /shipments directly so no
    enrichment step is needed there."""
    since_dt = datetime.now(timezone.utc) - timedelta(days=days)
    since_iso = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    log.info("Pulling ShipStation %s shipments since %s ...",
              api_version.upper(), since_iso)
    flatten = (_flatten_shipment_v2
               if api_version == "v2" else _flatten_shipment)
    rows = []
    for s in _paginate_shipments(
            session,
            _query_params(since_iso, api_version),
            api_version=api_version):
        rows.append(flatten(s))
    if api_version == "v2" and rows:
        label_idx = _fetch_labels_index(session, since_iso)
        rows = [_merge_label_data(r, label_idx) for r in rows]
    return _write_csv(f"shipments_last_{days}d", rows)


def sync_full(session: requests.Session, days: int = 1825,
                api_version: str = "v1") -> Path:
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
    log.info("FULL %s BACKFILL — shipments since %s ...",
              api_version.upper(), since_iso)
    flatten = (_flatten_shipment_v2
               if api_version == "v2" else _flatten_shipment)
    rows = []
    for s in _paginate_shipments(
            session,
            _query_params(since_iso, api_version),
            api_version=api_version):
        rows.append(flatten(s))
    # v2 — enrich with tracking from /labels.
    if api_version == "v2" and rows:
        label_idx = _fetch_labels_index(session, since_iso)
        rows = [_merge_label_data(r, label_idx) for r in rows]
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
    if not api_key:
        log.warning("ShipStation env var not set — skipping. Set "
                      "SHIPSTATION_API_KEY (v2) or "
                      "SHIPSTATION_API_KEY+SHIPSTATION_API_SECRET "
                      "(v1 legacy) to enable.")
        return 0

    session, api_version = _build_session(api_key, api_secret)
    log.info("Authenticated with ShipStation API %s", api_version.upper())

    if args.cmd == "recent":
        sync_recent(session, args.days, api_version=api_version)
    elif args.cmd == "full":
        sync_full(session, args.days, api_version=api_version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
