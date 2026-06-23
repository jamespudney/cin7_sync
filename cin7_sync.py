"""
cin7_sync.py
============
Local bridge between CIN7 Core (DEAR) and your Cowork workspace.

Pulls data from the CIN7 Core v2 API on your own machine using credentials
stored locally in a .env file (never shared with the cloud), and writes the
results as CSV and JSON files into the ./output folder. From there, Claude
(or the Streamlit app) can read, analyse, and report on them.

Quick reference
---------------
    python cin7_sync.py test                    # verify credentials
    python cin7_sync.py products                # product master
    python cin7_sync.py stock                   # current stock on hand
    python cin7_sync.py customers
    python cin7_sync.py suppliers
    python cin7_sync.py sales --days 365        # sale headers (last 365 days)
    python cin7_sync.py purchases --days 365    # PO headers (last 365 days)
    python cin7_sync.py salelines --days 90     # sale LINE ITEMS (slow, loops)
    python cin7_sync.py purchaselines --days 365
    python cin7_sync.py stockadjustments --days 365
    python cin7_sync.py stocktransfers --days 365
    python cin7_sync.py movements --days 365    # all movement types in one go
    python cin7_sync.py quick --days 30         # fast daily refresh (headers only)
    python cin7_sync.py full --days 365         # everything, including lines

Outputs
-------
    output/<endpoint>_<YYYY-MM-DD_HHMMSS>.csv
    output/<endpoint>_<YYYY-MM-DD_HHMMSS>.json
    output/cin7_sync.log
    output/.checkpoint_<endpoint>.json          # resume support for long pulls
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://inventory.dearsystems.com/ExternalApi/v2"
PAGE_SIZE = 1000           # CIN7 v2 allows up to 1000 per page on most endpoints

# Rate limit is a shared 60 calls/minute across ALL apps on the CIN7 account.
# Default 2.5s = 24 calls/min, leaves ~36/min of headroom for Inventory Planner,
# Shopify sync, Xero etc. Override with CIN7_RATE_SECONDS in .env for quiet
# windows (e.g. overnight you can drop to 1.5 for faster sync).
DEFAULT_RATE_LIMIT_SECONDS = 2.5

REQUEST_TIMEOUT = 60
MAX_RETRIES = 5

SCRIPT_DIR = Path(__file__).resolve().parent
# OUTPUT_DIR follows DATA_DIR env var (set to /data on Render).
# Defaults to project folder locally. See data_paths.py.
from data_paths import OUTPUT_DIR  # noqa: E402
from storage_dimensions import extract_storage_dim  # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "cin7_sync.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("cin7_sync")

# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------


def _parse_retry_after(value: Any, default: int = 10) -> int:
    """Parse a Retry-After header robustly. CIN7 sometimes returns
    non-numeric strings like '60 Seconds' or '30 sec' instead of the
    RFC-compliant integer seconds. Extract the first integer; fall
    back to the default on any failure."""
    if value is None:
        return default
    try:
        return max(1, int(value))
    except (ValueError, TypeError):
        import re as _re
        m = _re.search(r"\d+", str(value))
        if m:
            try:
                return max(1, int(m.group()))
            except ValueError:
                pass
    return default


class Cin7Client:
    """Minimal CIN7 Core v2 API client with pagination + polite rate limiting."""

    def __init__(self, account_id: str, application_key: str,
                 rate_seconds: float = DEFAULT_RATE_LIMIT_SECONDS) -> None:
        if not account_id or not application_key:
            raise ValueError(
                "Missing credentials. Set CIN7_ACCOUNT_ID and CIN7_APPLICATION_KEY "
                "in your .env file."
            )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "api-auth-accountid": account_id,
                "api-auth-applicationkey": application_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        self.rate_seconds = max(0.5, float(rate_seconds))
        self._last_call_ts = 0.0
        log.info(
            "CIN7 client rate-limited to %.2fs between calls (~%.0f calls/min). "
            "Full CIN7 account cap is ~60/min shared across all integrations.",
            self.rate_seconds, 60.0 / self.rate_seconds,
        )

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_call_ts
        if elapsed < self.rate_seconds:
            time.sleep(self.rate_seconds - elapsed)
        self._last_call_ts = time.monotonic()

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{BASE_URL}/{path.lstrip('/')}"
        attempt = 0
        while True:
            attempt += 1
            self._throttle()
            try:
                resp = self.session.get(url, params=params or {}, timeout=REQUEST_TIMEOUT)
            except requests.RequestException as exc:
                if attempt >= MAX_RETRIES:
                    raise
                wait = 2 ** attempt
                log.warning("Network error (%s). Retrying in %ds...", exc, wait)
                time.sleep(wait)
                continue

            if resp.status_code == 429:
                wait = _parse_retry_after(
                    resp.headers.get("Retry-After"), default=10)
                log.warning("429 rate limit hit. Sleeping %ds...", wait)
                time.sleep(wait)
                continue

            if 500 <= resp.status_code < 600 and attempt < MAX_RETRIES:
                wait = 2 ** attempt
                log.warning("Server %s. Retrying in %ds...", resp.status_code, wait)
                time.sleep(wait)
                continue

            if not resp.ok:
                raise RuntimeError(
                    f"CIN7 API error {resp.status_code} on {path}: {resp.text[:500]}"
                )

            try:
                return resp.json()
            except ValueError:
                body = resp.text or ""
                # CIN7 serves an HTML 404 page with 200 status for unknown endpoints
                if body.lstrip().lower().startswith("<!doctype") or "<html" in body[:200].lower():
                    raise RuntimeError(f"Endpoint not found (HTML 404 page): {path}")
                snippet = body[:200].replace("\n", " ").replace("\r", " ")
                raise RuntimeError(f"Non-JSON response from {path}: {snippet}")

    def put(self, path: str,
             body: Dict[str, Any]) -> Dict[str, Any]:
        """PUT a JSON body to CIN7. Used for product updates (dropship
        write-back, etc.). Same rate-limit + retry policy as GET. Returns
        the parsed JSON response on success, raises on 4xx/5xx."""
        url = f"{BASE_URL}/{path.lstrip('/')}"
        attempt = 0
        while True:
            attempt += 1
            self._throttle()
            try:
                resp = self.session.put(
                    url, json=body, timeout=REQUEST_TIMEOUT)
            except requests.RequestException as exc:
                if attempt >= MAX_RETRIES:
                    raise
                wait = 2 ** attempt
                log.warning("Network error (%s). Retrying in %ds...",
                            exc, wait)
                time.sleep(wait)
                continue
            if resp.status_code == 429:
                wait = _parse_retry_after(
                    resp.headers.get("Retry-After"), default=10)
                log.warning("429 rate limit on PUT. Sleeping %ds...", wait)
                time.sleep(wait)
                continue
            if 500 <= resp.status_code < 600 and attempt < MAX_RETRIES:
                wait = 2 ** attempt
                log.warning("Server %s on PUT. Retrying in %ds...",
                            resp.status_code, wait)
                time.sleep(wait)
                continue
            if not resp.ok:
                raise RuntimeError(
                    f"CIN7 PUT error {resp.status_code} on {path}: "
                    f"{resp.text[:500]}"
                )
            try:
                return resp.json()
            except ValueError:
                return {"status": resp.status_code, "text": resp.text}

    def update_product(self, product_id: str,
                       fields: Dict[str, Any]) -> Dict[str, Any]:
        """Update specific fields on a CIN7 product. `fields` should be
        the minimum set of keys to change plus the ID (auto-added).
        Common keys: DropShipMode ('No Drop Ship' / 'Always Drop Ship' /
        'Optional Drop Ship'), Tags (comma-separated string)."""
        body = {"ID": product_id, **fields}
        return self.put("product", body)

    def get_sale(self, sale_id: str
                  ) -> Optional[Dict[str, Any]]:
        """v2.67.159 — Fetch the full Sale object by ID (UUID).
        Returns the parsed JSON or None on failure. Used by the
        dropship-tracking auto-writer: GET the sale → modify
        Fulfilments[0].Ship.Lines → PUT the modified sale."""
        try:
            return self.get(f"sale", params={"ID": sale_id})
        except Exception as exc:
            log.error("CIN7 get_sale(%s) failed: %s",
                        sale_id, exc)
            return None

    def update_sale(self, sale_body: Dict[str, Any]
                      ) -> Dict[str, Any]:
        """v2.67.159 — PUT a full Sale object back to CIN7. The
        body MUST include "ID"; otherwise CIN7 treats it as a
        new sale (which fails validation). Used by the dropship
        tracking writer to push tracking back to the sale's
        Fulfilment.Ship.Lines."""
        if not sale_body.get("ID"):
            raise ValueError(
                "update_sale: sale_body must include 'ID'")
        return self.put("sale", sale_body)

    def _purchase_detail_by_id(self,
                               purchase_id: str) -> Optional[Dict[str, Any]]:
        """Fetch full purchase detail by CIN7 task ID.

        Prefer /advanced-purchase because CIN7's docs say it supports
        Simple, Advanced, and Service purchases, and PurchaseAdvanced UI
        links point at that object. Fall back to deprecated /purchase for
        legacy/simple-account compatibility.
        """
        if not purchase_id:
            return None
        errors = []
        for path in ("advanced-purchase", "purchase"):
            try:
                detail = self.get(path, params={"ID": purchase_id})
            except Exception as exc:
                errors.append(f"{path}: {exc}")
                continue
            if isinstance(detail, dict) and detail:
                detail["_cin7_detail_endpoint"] = path
                return detail
        if errors:
            log.warning(
                "CIN7 purchase detail lookup failed for %s via %s",
                purchase_id, "; ".join(errors))
        return None

    @staticmethod
    def _normalise_po_ref(ref: str) -> str:
        text = str(ref or "").strip().upper()
        return text[3:] if text.startswith("PO-") else text

    def get_purchase(self, ref: str) -> Optional[Dict[str, Any]]:
        """Fetch a full Purchase object live from CIN7.
        Accepts either:
          • a UUID (queried via /advanced-purchase?ID=<uuid>, with
            /purchase fallback)
          • a PO number like "PO-7213" or "7213" (looked up via
            /purchaseList?Search=PO-7213 to find the UUID, then fetched
            via /advanced-purchase?ID=<uuid> for full Order+Lines)

        BUG HISTORY: v2.67.196-v2.67.311 passed OrderNumber directly
        to /purchase, but per CIN7 docs (dearinventory.apib §Purchase
        line 13604) the /purchase endpoint ONLY accepts the ID
        parameter — OrderNumber is undocumented and silently returns
        nothing useful. Code then looked for the wrong response key
        (PurchaseOrderList / Purchases) when the actual key in the
        /purchaseList response is `PurchaseList`. Net effect: PO-by-
        number lookups always failed silently, with the
        get_purchase_live AI tool blaming "propagation lag".

        James 2026-05-27 — the bot replied to PO-7213 commentary with
        "PO isn't returning from CIN7, wait 2-3 minutes" because of
        this. The PO existed as a DRAFT; we just never queried the
        right endpoint to see drafts. /purchaseList returns ALL
        statuses including DRAFT, so this fix unlocks draft-PO
        commentary which is the most valuable moment to intervene
        (before AUTHORISED is hit).

        v2.67.372 — PurchaseAdvanced URLs must use /advanced-purchase
        first. /purchase is deprecated and may miss Advanced Purchase
        draft links copied from the CIN7 UI.

        Used by get_purchase_live AI tool when the local
        purchase_lines CSV doesn't have a freshly-created PO yet.
        Returns None on failure (logs the error so the caller can
        surface a useful message)."""
        if not ref:
            return None
        # UUIDs are 36 chars with hyphens; PO numbers are short or
        # start with 'PO-'.
        is_uuid = (len(ref) >= 32 and "-" in ref
                      and not ref.upper().startswith("PO-"))
        try:
            if is_uuid:
                return self._purchase_detail_by_id(ref)
            # PO-number lookup: /purchaseList?Search=<PO-NNNN> to
            # find the UUID, then /purchase?ID=<uuid> for full
            # Order+Lines detail. /purchaseList returns headers
            # only and the response key is `PurchaseList`. Search
            # matches OrderNumber substring (per CIN7 docs at line
            # 13298) across all statuses including DRAFT.
            ref_norm = self._normalise_po_ref(ref)
            search_term = f"PO-{ref_norm}"
            resp = self.get(
                "purchaseList",
                params={"Search": search_term, "Limit": 50})
            items = []
            if isinstance(resp, dict):
                # `PurchaseList` is the documented key. Defensive
                # fallbacks kept for any account shape variation.
                items = (resp.get("PurchaseList")
                            or resp.get("PurchaseOrderList")
                            or resp.get("Purchases")
                            or [])
            elif isinstance(resp, list):
                items = resp
            for it in items:
                if not isinstance(it, dict):
                    continue
                ord_n = str(it.get("OrderNumber") or "").upper()
                if self._normalise_po_ref(ord_n) == ref_norm:
                    pid = it.get("ID")
                    if pid:
                        # Always follow up with detail by ID for the
                        # full Order+Lines structure.
                        # /purchaseList returns header summary only.
                        detail = self._purchase_detail_by_id(pid)
                        if detail:
                            return detail
                    # Defensive: missing ID — return the thin
                    # header so the caller at least gets the
                    # status/supplier/dates, even without lines.
                    return it
            return None
        except Exception as exc:
            log.error("CIN7 get_purchase(%s) failed: %s",
                        ref, exc)
            return None


    def paginate(
        self,
        path: str,
        result_key: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Iterable[Dict[str, Any]]:
        page = 1
        params = dict(params or {})
        params.setdefault("Limit", PAGE_SIZE)
        total_yielded = 0
        while True:
            params["Page"] = page
            data = self.get(path, params=params)
            batch = data.get(result_key) or []
            for row in batch:
                yield row
            total_yielded += len(batch)
            log.info("  page %d -> %d records (running total %d)",
                     page, len(batch), total_yielded)
            if len(batch) < PAGE_SIZE:
                return
            page += 1


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def _flatten(row: Dict[str, Any], parent: str = "", sep: str = ".") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in row.items():
        column = f"{parent}{sep}{key}" if parent else key
        if isinstance(value, dict):
            out.update(_flatten(value, column, sep))
        elif isinstance(value, list):
            out[column] = json.dumps(value, ensure_ascii=False, default=str)
        else:
            out[column] = value
    return out


# v2.67.238 — how many timestamped snapshots to KEEP per output
# name (per extension). Older ones are pruned after every write.
# Without this the /data disk fills: nearsync runs every 15 min
# and write_outputs left every snapshot behind forever — the disk
# filled and ALL syncs started failing with ENOSPC.
_OUTPUT_KEEP = int(os.environ.get("OUTPUT_SNAPSHOTS_KEEP", "6") or 6)


def _prune_old_outputs(name: str, keep: int = _OUTPUT_KEEP) -> None:
    """Keep only the newest `keep` timestamped files for this
    output name (each extension separately); delete the rest.
    Best-effort — never raises."""
    for ext in ("csv", "json"):
        try:
            files = sorted(
                OUTPUT_DIR.glob(f"{name}_*.{ext}"),
                key=lambda p: p.stat().st_mtime,
                reverse=True)
        except OSError:
            continue
        for old in files[keep:]:
            try:
                old.unlink()
            except OSError:
                pass


def write_outputs(name: str, rows: List[Dict[str, Any]]) -> Path:
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    json_path = OUTPUT_DIR / f"{name}_{stamp}.json"
    csv_path = OUTPUT_DIR / f"{name}_{stamp}.csv"

    # v2.67.238 — compact JSON (no indent) to roughly halve the
    # raw-dump size; the CSV is the file the app actually reads.
    json_path.write_text(
        json.dumps(rows, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    if rows:
        flat_rows = [_flatten(r) for r in rows]
        columns: List[str] = []
        seen = set()
        for r in flat_rows:
            for k in r.keys():
                if k not in seen:
                    seen.add(k)
                    columns.append(k)
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for r in flat_rows:
                writer.writerow(r)
    else:
        csv_path.write_text("", encoding="utf-8")

    log.info("Wrote %d rows -> %s", len(rows), csv_path.name)
    log.info("          and -> %s", json_path.name)
    # v2.67.238 — prune old snapshots so /data can't fill up.
    _prune_old_outputs(name)
    return csv_path


# ---------------------------------------------------------------------------
# Checkpointing for long-running line-item pulls
# ---------------------------------------------------------------------------


def _checkpoint_path(name: str) -> Path:
    return OUTPUT_DIR / f".checkpoint_{name}.json"


def _load_checkpoint(name: str) -> Dict[str, Any]:
    p = _checkpoint_path(name)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_checkpoint(name: str, state: Dict[str, Any]) -> None:
    _checkpoint_path(name).write_text(
        json.dumps(state, indent=2, default=str), encoding="utf-8"
    )


def _clear_checkpoint(name: str) -> None:
    p = _checkpoint_path(name)
    if p.exists():
        p.unlink()


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------


def _iso_days_ago(days: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return dt.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Sync jobs — master data
# ---------------------------------------------------------------------------


def sync_test(client: Cin7Client) -> None:
    log.info("Testing connection with GET /me ...")
    me = client.get("me")
    log.info("  OK. Company: %s  |  Country: %s",
             me.get("Company"), me.get("Country"))
    write_outputs("me", [me])


def sync_products(client: Cin7Client) -> None:
    """Pull the product master, including Suppliers + ReorderLevels +
    AdditionalAttributes. v2.67.369 — IncludeAttributes added so the
    Storage L x W x H In dim field is present in the products CSV for
    use by the AI commentary tools without live per-SKU API calls."""
    log.info("Pulling product master (with Suppliers + ReorderLevels + Attributes)...")
    rows = list(client.paginate(
        "product", result_key="Products",
        params={
            "IncludeSuppliers": "true",
            "IncludeReorderLevels": "true",
            "IncludeAttributes": "true",
        },
    ))
    # Extract CIN7's Storage L x W x H In additional attribute into a
    # top-level storage_dim column so the engine and AI tools can join
    # it without live per-SKU API calls.
    for row in rows:
        row["storage_dim"] = extract_storage_dim(row)
    write_outputs("products", rows)


def sync_stock(client: Cin7Client) -> None:
    log.info("Pulling stock on hand (productavailability)...")
    rows = list(client.paginate("ref/productavailability", result_key="ProductAvailabilityList"))
    if not rows:
        rows = list(client.paginate("productavailability", result_key="ProductAvailabilityList"))
    write_outputs("stock_on_hand", rows)

    # Also append today's total FIFO inventory value to a cumulative
    # history file. Idempotent — only appends once per day. After 30
    # days of running this, we have real historical inventory snapshots
    # and the Monthly Metrics page can drop its walk-back reconstruction.
    _append_inventory_snapshot(rows)


def _append_inventory_snapshot(stock_rows: List[dict]) -> None:
    """Append today's FIFO inventory value to inventory_value_history.csv
    if today's date isn't already present. Uses CIN7's StockOnHand field
    (FIFO-based dollar value) — matches how we value inventory elsewhere.
    Safe to call multiple times a day; only the first call per date writes."""
    import csv
    today = datetime.utcnow().strftime("%Y-%m-%d")
    hist_path = OUTPUT_DIR / "inventory_value_history.csv"

    # Short-circuit if today already logged
    if hist_path.exists():
        try:
            with hist_path.open(newline="", encoding="utf-8") as f:
                for row in csv.reader(f):
                    if row and row[0] == today:
                        return   # already snapshotted today
        except Exception:
            pass  # treat unreadable file as "no record yet"

    # Compute today's FIFO total from the productavailability rows
    total_fifo = 0.0
    row_count = 0
    for r in stock_rows:
        try:
            total_fifo += float(r.get("StockOnHand") or 0)
            row_count += 1
        except (ValueError, TypeError):
            continue

    # Append
    header_needed = not hist_path.exists()
    try:
        with hist_path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if header_needed:
                w.writerow(["date", "total_fifo_value", "sku_rows"])
            w.writerow([today, f"{total_fifo:.2f}", row_count])
        log.info("  Inventory snapshot logged: %s = $%.2f (%d rows)",
                 today, total_fifo, row_count)
    except Exception as exc:
        log.warning("  Could not append inventory snapshot: %s", exc)


def sync_customers(client: Cin7Client) -> None:
    log.info("Pulling customer master...")
    rows = list(client.paginate("customer", result_key="CustomerList"))
    write_outputs("customers", rows)


def sync_suppliers(client: Cin7Client) -> None:
    log.info("Pulling supplier master...")
    rows = list(client.paginate("supplier", result_key="SupplierList"))
    write_outputs("suppliers", rows)


# ---------------------------------------------------------------------------
# Sync jobs — transaction headers
# ---------------------------------------------------------------------------


def sync_sales(client: Cin7Client, days: int) -> None:
    since = _iso_days_ago(days)
    log.info("Pulling sale list updated since %s ...", since)
    rows = list(client.paginate(
        "saleList",
        result_key="SaleList",
        params={"UpdatedSince": since},
    ))
    write_outputs(f"sales_last_{days}d", rows)


def sync_purchases(client: Cin7Client, days: int) -> None:
    since = _iso_days_ago(days)
    log.info("Pulling purchase list updated since %s ...", since)
    rows = list(client.paginate(
        "purchaseList",
        result_key="PurchaseList",
        params={"UpdatedSince": since},
    ))
    write_outputs(f"purchases_last_{days}d", rows)


# ---------------------------------------------------------------------------
# Sync jobs — line items (slow: one detail call per order)
# ---------------------------------------------------------------------------


def _extract_sale_lines(detail: Dict[str, Any], header: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Pull invoice lines (shipped qty) out of a /sale response, flattened
    with the header context we need for analysis.
    Falls back to order lines if no invoice is present yet."""
    sid = header.get("SaleID") or detail.get("ID")
    cust = header.get("Customer") or detail.get("Customer")
    cust_id = header.get("CustomerID") or detail.get("CustomerID")
    loc_id = header.get("OrderLocationID") or detail.get("OrderLocationID")
    order_num = header.get("OrderNumber") or detail.get("OrderNumber")
    status = header.get("Status") or detail.get("Status")
    order_date = header.get("OrderDate") or detail.get("OrderDate")
    source = header.get("SourceChannel") or detail.get("SourceChannel")
    sale_type = header.get("Type") or detail.get("Type")
    # v2.67.295 — capture SalesRepresentative for marketplace
    # segmentation. CIN7 Core sets this to 'AMAZON' / 'EBAY' /
    # 'SHOPIFY' / a real staff name on the sale header, depending
    # on origin. Denormalised onto every line so the Monthly
    # Metrics page can group revenue by rep without joining back
    # to the headers CSV. Real Amazon revenue lives here (in CIN7
    # / QB acc 400), NOT in QB acc 403 (which is a small misc
    # ledger). Reference: Joe Caballero SO-56856 screenshot
    # (Amazon marketplace order with SalesRep = "AMAZON").
    sales_rep = (header.get("SalesRepresentative")
                 or detail.get("SalesRepresentative"))
    # v2.67.52 — capture freeform sale-side text fields so the AI's
    # get_sale_order tool can surface what the rep typed (build
    # instructions, customer PO #, delivery requirements). Same
    # pattern as the PO side — same value across every line of the
    # same sale.
    text_fields = _extract_sale_text_fields(detail, header)

    out: List[Dict[str, Any]] = []

    # Invoices usually hold what actually shipped
    for invoice in (detail.get("Invoices") or []):
        if not isinstance(invoice, dict):
            continue
        # v2.67.330 — skip VOIDED invoices. When an invoice is voided
        # and re-issued (a correction/revision), CIN7 KEEPS the voided
        # invoice in the Invoices array alongside the live one. Summing
        # lines across all of them double-counts demand. James
        # 2026-05-28: LED-NEON-FLEX-SUPER-SLIM-ST showed 12 + 12 across
        # two months — one live invoice plus one voided revision of the
        # same 12-unit line. Only count finalised invoices; if the
        # Status field is absent we keep the line (no regression).
        _inv_status = str(invoice.get("Status") or "").strip().upper()
        if _inv_status in ("VOIDED", "NOT AVAILABLE"):
            continue
        inv_date = invoice.get("InvoiceDate")
        inv_num = invoice.get("InvoiceNumber")
        for line in (invoice.get("Lines") or []):
            if not isinstance(line, dict):
                continue
            out.append({
                "SaleID": sid,
                "OrderNumber": order_num,
                "OrderDate": order_date,
                "InvoiceNumber": inv_num,
                "InvoiceDate": inv_date,
                "Status": status,
                "SaleType": sale_type,
                "SourceChannel": source,
                # v2.67.295 — SalesRepresentative denormalised
                # so the Monthly Metrics page can split revenue
                # by AMAZON / EBAY / SHOPIFY / individual reps.
                "SalesRepresentative": sales_rep,
                "Customer": cust,
                "CustomerID": cust_id,
                "LocationID": loc_id,
                "ProductID": line.get("ProductID"),
                "SKU": line.get("SKU"),
                "Name": line.get("Name"),
                "Quantity": line.get("Quantity"),
                "Price": line.get("Price"),
                "Discount": line.get("Discount"),
                "Tax": line.get("Tax"),
                "Total": line.get("Total"),
                "UOM": line.get("UOM"),
                "AverageCost": line.get("AverageCost"),
                # v2.67.52 — sale-side freeform text fields. Same
                # value on every line of the same sale.
                **text_fields,
            })

        # Emit a synthetic line for each AdditionalCharges entry on the
        # invoice. CIN7 Core stores shipping, handling, surcharges etc.
        # in this array — NOT in a single ShippingTotal field. Each
        # entry has Description, Quantity, Price, Tax, Total. The
        # Description usually starts with "Shipping - " for shipping
        # charges (e.g. "Shipping - UPS Ground", "Shipping - Free
        # shipping"), which means our existing "Shipping - " regex
        # detection in the Monthly Metrics page picks them up without
        # additional plumbing.
        #
        # We skip zero-total charges (e.g. "Free shipping" entries)
        # to keep the file tidy.
        for charge in (invoice.get("AdditionalCharges") or []):
            if not isinstance(charge, dict):
                continue
            c_total = float(charge.get("Total") or 0)
            if c_total == 0:
                continue
            desc = str(charge.get("Description") or "").strip()
            out.append({
                "SaleID": sid,
                "OrderNumber": order_num,
                "OrderDate": order_date,
                "InvoiceNumber": inv_num,
                "InvoiceDate": inv_date,
                "Status": status,
                "SaleType": sale_type,
                "SourceChannel": source,
                # v2.67.295 — propagate SalesRep onto charge lines
                # too so per-rep totals include freight/handling.
                "SalesRepresentative": sales_rep,
                "Customer": cust,
                "CustomerID": cust_id,
                "LocationID": loc_id,
                "ProductID": None,
                "SKU": desc or "AdditionalCharge",
                "Name": desc or "Additional charge",
                "Quantity": float(charge.get("Quantity") or 1),
                "Price": float(charge.get("Price") or 0),
                "Discount": float(charge.get("Discount") or 0),
                "Tax": float(charge.get("Tax") or 0),
                "Total": c_total,
                "UOM": "charge",
                "AverageCost": 0,
                **text_fields,
            })

    # If no invoice yet, fall back to order lines (what was booked) —
    # plus any Order-level AdditionalCharges (shipping etc. booked at
    # quote/order time).
    if not out:
        order = detail.get("Order") or {}
        if not isinstance(order, dict):
            order = {}
        for line in (order.get("Lines") or []):
            if not isinstance(line, dict):
                continue
            out.append({
                "SaleID": sid,
                "OrderNumber": order_num,
                "OrderDate": order_date,
                "InvoiceNumber": None,
                "InvoiceDate": None,
                "Status": status,
                "SaleType": sale_type,
                "SourceChannel": source,
                # v2.67.295 — SalesRep on the order-only fallback.
                "SalesRepresentative": sales_rep,
                "Customer": cust,
                "CustomerID": cust_id,
                "LocationID": loc_id,
                "ProductID": line.get("ProductID"),
                "SKU": line.get("SKU"),
                "Name": line.get("Name"),
                "Quantity": line.get("Quantity"),
                "Price": line.get("Price"),
                "Discount": line.get("Discount"),
                "Tax": line.get("Tax"),
                "Total": line.get("Total"),
                "UOM": line.get("UOM"),
                "AverageCost": line.get("AverageCost"),
                **text_fields,
            })
        # Order-level AdditionalCharges (same structure as invoice-level)
        for charge in (order.get("AdditionalCharges") or []):
            if not isinstance(charge, dict):
                continue
            c_total = float(charge.get("Total") or 0)
            if c_total == 0:
                continue
            desc = str(charge.get("Description") or "").strip()
            out.append({
                "SaleID": sid,
                "OrderNumber": order_num,
                "OrderDate": order_date,
                "InvoiceNumber": None,
                "InvoiceDate": None,
                "Status": status,
                "SaleType": sale_type,
                "SourceChannel": source,
                # v2.67.295 — SalesRep on order-level charges too.
                "SalesRepresentative": sales_rep,
                "Customer": cust,
                "CustomerID": cust_id,
                "LocationID": loc_id,
                "ProductID": None,
                "SKU": desc or "AdditionalCharge",
                "Name": desc or "Additional charge",
                "Quantity": float(charge.get("Quantity") or 1),
                "Price": float(charge.get("Price") or 0),
                "Discount": float(charge.get("Discount") or 0),
                "Tax": float(charge.get("Tax") or 0),
                "Total": c_total,
                "UOM": "charge",
                "AverageCost": 0,
                **text_fields,
            })

    return out


def _extract_sale_text_fields(detail: Dict[str, Any],
                                header: Dict[str, Any]) -> Dict[str, Any]:
    """v2.67.52 — sale-side mirror of _extract_po_freight_signals.
    The CIN7 /sale endpoint exposes the same family of freeform
    fields but at slightly different paths:

      - Memo              ← detail.Order.Memo  (THE 'Sale Order Memo'
                            text box — what the sales rep types)
      - Note              ← detail.Note  (top-level)
      - ShippingNotes     ← detail.ShippingNotes  (TOP-LEVEL on
                            sales, unlike POs where it's an
                            additional attribute)
      - Terms             ← detail.Terms
      - CustomerReference ← detail.CustomerReference  (the
                            customer's PO number against this sale)

    These are typed by sales reps to flag custom build instructions,
    delivery requirements, customer PO numbers, etc. The user
    explicitly asked to surface them in the AI.
    """
    order_block = detail.get("Order") if isinstance(
        detail.get("Order"), dict) else {}
    memo = (order_block.get("Memo") or detail.get("Memo")
            or detail.get("OrderMemo") or "")
    note = detail.get("Note") or header.get("Note") or ""
    shipping_notes = (detail.get("ShippingNotes")
                      or header.get("ShippingNotes") or "")
    terms = detail.get("Terms") or header.get("Terms") or ""
    customer_ref = (detail.get("CustomerReference")
                    or header.get("CustomerReference") or "")
    return {
        "Memo": str(memo).strip() if memo else "",
        "Note": str(note).strip() if note else "",
        "ShippingNotes": (str(shipping_notes).strip()
                          if shipping_notes else ""),
        "Terms": str(terms).strip() if terms else "",
        "CustomerReference": (str(customer_ref).strip()
                              if customer_ref else ""),
    }


def _extract_po_freight_signals(detail: Dict[str, Any],
                                  header: Dict[str, Any]
                                  ) -> Dict[str, Any]:
    """Pull every freeform text field on a PO that staff might type
    into. v2.67.52 — expanded from 2 fields to 5 after the user
    flagged 'Purchase Order Memo' as a distinct field that the buyer
    actually uses (the existing Comments + ShippingNotes capture
    didn't cover it).

    Field map (per CIN7 /advanced-purchase response):
      - Comments     ← detail.Comments / Comment / InternalComments
                       (top-level — used inconsistently across accounts)
      - ShippingNotes ← AdditionalAttributes 'shipping notes'
                       (where the buyer logs freight progress)
      - Memo         ← detail.Order.Memo  (THE 'Purchase Order Memo'
                       field on the CIN7 PO form — what the buyer
                       sees as a big text box)
      - Note         ← detail.Note  (separate top-level note;
                       sometimes used for status / blame
                       e.g. 'SHIPPED IN ERROR BY TOPMET')
      - Terms        ← detail.Terms  (payment terms, e.g. 'Net 30')

    Every field is independently captured so the AI can surface them
    individually — the buyer types DIFFERENT things into each one."""
    # Comments — same logic as before.
    comments = (
        detail.get("Comments")
        or detail.get("Comment")
        or detail.get("InternalComments")
        or header.get("Comments")
        or header.get("Comment")
        or "")
    if not isinstance(comments, str):
        comments = str(comments or "")

    # ShippingNotes — CIN7 stores attribute values POSITIONALLY in a
    # `AdditionalAttribute1`-`AdditionalAttribute10` flat dict. The
    # label-to-position mapping comes from the `AttributeSet`
    # definition, which is configured per CIN7 account. For Wired4
    # Signs as of v2.67.55b (verified via /ref/attributeSet on
    # 2026-05-06), the relevant sets for purchases are:
    #   - "Vendor Purchase" set: AdditionalAttribute2 = Shipping Notes
    #     (also AdditionalAttribute1 = Trello Card)
    #   - "PO special instructions" set: AdditionalAttribute1/2/3 =
    #     Note 1 / Note 2 / Note 3
    # In practice, position 2 has carried shipping-tracking content
    # ("Ship 5/4 UPS 1Z5WE3350454840014 (ERROR!)" on PO-7109 confirmed
    # the Vendor Purchase set is what's attached to active POs). We
    # capture position 2 as ShippingNotes, plus any other non-empty
    # positional value in AttributeNotes (concatenated) so nothing
    # is silently dropped if the buyer types into a different slot.
    # Defensive against the legacy LIST-of-dicts shape too just in
    # case CIN7 changes the response format on us.
    shipping_notes = ""
    other_attr_parts = []
    attrs_blob = (detail.get("AdditionalAttributes")
                   or detail.get("AttributeSet")
                   or header.get("AdditionalAttributes"))
    if isinstance(attrs_blob, dict):
        # Positional dict — the live CIN7 shape as of 2026-05.
        for i in range(1, 11):
            v = attrs_blob.get(f"AdditionalAttribute{i}")
            if v in (None, "", False):
                continue
            sv = str(v).strip()
            if not sv or sv.lower() == "false":
                continue
            if i == 2 and not shipping_notes:
                # Vendor Purchase set: position 2 = Shipping Notes.
                shipping_notes = sv
            else:
                other_attr_parts.append(f"attr{i}: {sv}")
    elif isinstance(attrs_blob, list):
        # Defensive fallback: legacy list-of-{Name,Value}-dicts shape.
        for a in attrs_blob:
            if not isinstance(a, dict):
                continue
            name = str(a.get("Name") or a.get("name") or "")
            value = str(a.get("Value") or a.get("value") or "").strip()
            if not value:
                continue
            if name.lower().strip() == "shipping notes":
                shipping_notes = value
            else:
                other_attr_parts.append(f"{name}: {value}")
    # Top-level flat field fallback (some accounts surface
    # ShippingNotes at the top of the response).
    if not shipping_notes:
        for k in ("ShippingNotes", "Shipping_Notes", "shipping_notes"):
            v = detail.get(k) or header.get(k)
            if v:
                shipping_notes = str(v)
                break

    # v2.67.52 — Memo lives under detail.Order.Memo (nested). Defend
    # against detail.Memo too just in case some accounts surface it
    # at the top level.
    memo = ""
    order_block = detail.get("Order") if isinstance(
        detail.get("Order"), dict) else {}
    memo_raw = (order_block.get("Memo") or detail.get("Memo")
                or detail.get("OrderMemo") or "")
    if memo_raw:
        memo = str(memo_raw)

    # v2.67.52 — Note (top-level, distinct from Memo).
    note_raw = detail.get("Note") or header.get("Note") or ""
    note = str(note_raw) if note_raw else ""

    # v2.67.52 — Terms (payment terms).
    terms_raw = detail.get("Terms") or header.get("Terms") or ""
    terms = str(terms_raw) if terms_raw else ""

    # v2.67.55b — also surface any non-empty AdditionalAttributeN
    # values that weren't position 2 (the canonical Shipping Notes
    # slot). Concatenated into AttributeNotes column so the AI
    # tool can show them when the buyer used a different slot than
    # we expect (e.g. moved Trello Card content into Note 3).
    attribute_notes = "; ".join(other_attr_parts) if other_attr_parts else ""
    return {
        "Comments": comments.strip() if comments else "",
        "ShippingNotes": shipping_notes.strip() if shipping_notes else "",
        "Memo": memo.strip() if memo else "",
        "Note": note.strip() if note else "",
        "Terms": terms.strip() if terms else "",
        "AttributeNotes": attribute_notes,
    }


def _extract_purchase_lines(detail: Dict[str, Any], header: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Pull order + invoice lines from a /purchase response."""
    pid = header.get("ID") or detail.get("ID")
    supp = header.get("Supplier") or detail.get("Supplier")
    supp_id = header.get("SupplierID") or detail.get("SupplierID")
    order_num = header.get("OrderNumber") or detail.get("OrderNumber")
    order_date = header.get("OrderDate") or detail.get("OrderDate")
    required_by = header.get("RequiredBy") or detail.get("RequiredBy")
    status = header.get("Status") or detail.get("Status")
    # v2.67.44 — pull buyer's freight-signal fields once per PO.
    freight_signals = _extract_po_freight_signals(detail, header)

    out: List[Dict[str, Any]] = []

    order = detail.get("Order") or {}
    if not isinstance(order, dict):
        order = {}
    for line in (order.get("Lines") or []):
        if not isinstance(line, dict):
            continue
        out.append({
            "PurchaseID": pid,
            "OrderNumber": order_num,
            "OrderDate": order_date,
            "RequiredBy": required_by,
            "Status": status,
            "Supplier": supp,
            "SupplierID": supp_id,
            "ProductID": line.get("ProductID"),
            "SKU": line.get("SKU"),
            "Name": line.get("Name"),
            "Quantity": line.get("Quantity"),
            "Price": line.get("Price"),
            "Discount": line.get("Discount"),
            "Tax": line.get("Tax"),
            "Total": line.get("Total"),
            "UOM": line.get("UOM"),
            "Supplier SKU": line.get("SupplierSKU"),
            # v2.67.44 — freight-signal fields on every line of the
            # PO. Same value across lines from the same PO.
            "Comments": freight_signals["Comments"],
            "ShippingNotes": freight_signals["ShippingNotes"],
            # v2.67.52 — additional freeform fields the buyer types
            # into. Memo is the PO Memo box visible on the CIN7 PO
            # form; Note is a separate top-level note (sometimes used
            # for status); Terms is payment terms.
            "Memo": freight_signals["Memo"],
            "Note": freight_signals["Note"],
            "Terms": freight_signals["Terms"],
            "AttributeNotes": freight_signals.get("AttributeNotes", ""),
        })

    # Received qty from stock-received blocks, if present.
    # Shape varies by account: sometimes list of dicts, sometimes list of strings/IDs.
    for rec in (detail.get("StockReceived") or []):
        if not isinstance(rec, dict):
            continue
        rec_date = rec.get("Date") or rec.get("StockReceivedDate")
        rec_lines = rec.get("Lines") or []
        for line in rec_lines:
            if not isinstance(line, dict):
                continue
            out.append({
                "PurchaseID": pid,
                "OrderNumber": order_num,
                "OrderDate": order_date,
                "RequiredBy": required_by,
                "Status": (status + "-Received") if status else "Received",
                "Supplier": supp,
                "SupplierID": supp_id,
                "ProductID": line.get("ProductID"),
                "SKU": line.get("SKU"),
                "Name": line.get("Name"),
                "Quantity": line.get("ReceivedQuantity") or line.get("Quantity"),
                "ReceivedDate": rec_date,
                "UOM": line.get("UOM"),
                # Freight signals carried through to received rows
                # too so the AI can show progress notes that were
                # captured on the original PO.
                "Comments": freight_signals["Comments"],
                "ShippingNotes": freight_signals["ShippingNotes"],
                # v2.67.52 — same expansion on stock-received rows.
                "Memo": freight_signals["Memo"],
                "Note": freight_signals["Note"],
                "Terms": freight_signals["Terms"],
            })

    return out


def _fetch_lines_by_header(
    client: Cin7Client,
    headers: List[Dict[str, Any]],
    detail_path,          # str or callable(header) -> str
    id_field: str,
    extractor,
    checkpoint_name: str,
    output_name: str,
) -> None:
    """Loop through a list of header records, fetch each detail, extract lines,
    checkpoint progress, and write consolidated output at the end.
    `detail_path` may be a plain endpoint string, or a callable that takes a
    header and returns the right endpoint (for per-record routing)."""
    state = _load_checkpoint(checkpoint_name)
    processed: set = set(state.get("processed_ids") or [])
    all_lines: List[Dict[str, Any]] = state.get("lines") or []

    total = len(headers)
    label = detail_path if isinstance(detail_path, str) else "detail"
    log.info("Fetching %d %s details (resuming from %d already processed)...",
             total, label, len(processed))

    errors = 0
    try:
        for i, header in enumerate(headers, 1):
            record_id = header.get(id_field) or header.get("ID")
            if not record_id or record_id in processed:
                continue
            path = detail_path(header) if callable(detail_path) else detail_path
            try:
                detail = client.get(path, params={"ID": record_id})
            except Exception as exc:
                errors += 1
                log.warning("  Failed to fetch %s=%s (via %s): %s",
                            id_field, record_id, path, exc)
                if errors > 100:
                    raise RuntimeError("Too many detail fetch errors. Aborting.")
                # Still mark as processed to avoid retry-storm on deprecated records
                processed.add(record_id)
                continue

            lines = extractor(detail, header)
            all_lines.extend(lines)
            processed.add(record_id)

            if i % 25 == 0 or i == total:
                _save_checkpoint(checkpoint_name, {
                    "processed_ids": list(processed),
                    "lines": all_lines,
                    "updated": datetime.now().isoformat(),
                })
                log.info("  progress %d/%d   lines collected: %d   errors: %d",
                         i, total, len(all_lines), errors)
    except KeyboardInterrupt:
        log.warning("Interrupted. Checkpoint saved — re-run the same command to resume.")
        _save_checkpoint(checkpoint_name, {
            "processed_ids": list(processed),
            "lines": all_lines,
            "updated": datetime.now().isoformat(),
        })
        raise

    write_outputs(output_name, all_lines)
    _clear_checkpoint(checkpoint_name)


def sync_salelines(client: Cin7Client, days: int) -> None:
    since = _iso_days_ago(days)
    log.info("Listing sales updated since %s (for line detail)...", since)
    headers = list(client.paginate(
        "saleList", result_key="SaleList",
        params={"UpdatedSince": since},
    ))
    from collections import Counter
    type_counts = Counter((h.get("Type") or "Unknown") for h in headers)
    log.info("Sale types in window: %s", dict(type_counts))

    # /sale endpoint handles Simple, Advanced, AND Service sale types.
    # The Type field is captured per-line so we can classify in the warehouse.
    log.info("Will fetch %d sale details (~%.1f hours at %.2fs/call).",
             len(headers), len(headers) * client.rate_seconds / 3600,
             client.rate_seconds)
    _fetch_lines_by_header(
        client, headers,
        detail_path="sale",
        id_field="SaleID",
        extractor=_extract_sale_lines,
        checkpoint_name=f"salelines_{days}d",
        output_name=f"sale_lines_last_{days}d",
    )

    # Auto-reconcile pending demand signals against the freshly-written
    # sale lines. Best-effort; failure here must not crash the sync.
    _run_demand_reconcile_after_salelines(days)


def _run_demand_reconcile_after_salelines(days: int) -> None:
    """Load the just-written sale_lines_last_{days}d CSV and ask db.py to
    auto-mark matching pending demand signals as 'converted'. Logged but
    never raised — the demand-signal layer is a non-critical add-on; if
    it breaks we don't want it to take the nightly CIN7 sync down."""
    try:
        import db as _db   # local import keeps the cin7_sync top-level
                            # imports clean and avoids a hard dependency.
    except Exception as exc:
        log.info("  demand reconcile skipped: db import failed (%s)", exc)
        return
    try:
        latest = sorted(
            OUTPUT_DIR.glob(f"sale_lines_last_{days}d_*.csv")
        )
        if not latest:
            log.info("  demand reconcile: no sale_lines_last_%dd file found",
                     days)
            return
        import csv
        with open(latest[-1], encoding="utf-8", newline="") as fh:
            sales_records = list(csv.DictReader(fh))
        summary = _db.reconcile_demand_signals(
            sales_records, window_days=30,
            actor=f"auto_reconciler ({Path(__file__).name})")
        log.info(
            "  demand reconcile: checked=%d → converted=%d "
            "needs_review=%d no_sku=%d no_customer=%d "
            "no_match=%d cancelled_voided=%d errors=%d",
            summary.get("checked", 0),
            summary.get("converted", 0),
            summary.get("needs_review", 0),
            summary.get("skipped_no_sku", 0),
            summary.get("skipped_no_customer", 0),
            summary.get("skipped_no_match", 0),
            summary.get("skipped_cancelled_voided", 0),
            summary.get("errors", 0))
    except Exception as exc:
        log.warning("  demand reconcile failed: %s", exc)


def _purchase_detail_path(header: Dict[str, Any]) -> str:
    """Route based on PO Type. Simple -> /purchase,
    Advanced or Service -> /advanced-purchase (hyphenated, per CIN7)."""
    t = (header.get("Type") or "").lower()
    if "advanced" in t or "service" in t:
        return "advanced-purchase"
    return "purchase"


def sync_purchaselines(client: Cin7Client, days: int) -> None:
    since = _iso_days_ago(days)
    log.info("Listing purchases updated since %s (for line detail)...", since)
    headers = list(client.paginate(
        "purchaseList", result_key="PurchaseList",
        params={"UpdatedSince": since},
    ))
    from collections import Counter
    type_counts = Counter((h.get("Type") or "Unknown") for h in headers)
    log.info("Purchase types in window: %s", dict(type_counts))
    log.info("Will fetch %d purchase details (~%.1f hours at %.2fs/call).",
             len(headers), len(headers) * client.rate_seconds / 3600,
             client.rate_seconds)
    _fetch_lines_by_header(
        client, headers,
        detail_path=_purchase_detail_path,
        id_field="ID",
        extractor=_extract_purchase_lines,
        checkpoint_name=f"purchaselines_{days}d",
        output_name=f"purchase_lines_last_{days}d",
    )


# ---------------------------------------------------------------------------
# Sync jobs — stock movements (adjustments + transfers)
# ---------------------------------------------------------------------------


def _try_paginate(
    client: Cin7Client,
    candidates: List[tuple],
    params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Try several (path, result_key) pairs until one returns data.
    CIN7 endpoint names vary slightly between accounts/plans."""
    last_err = None
    for path, key in candidates:
        try:
            rows = list(client.paginate(path, result_key=key, params=dict(params)))
            if rows is not None:
                log.info("  (used endpoint %s -> %s)", path, key)
                return rows
        except RuntimeError as exc:
            log.warning("  endpoint %s failed: %s", path, str(exc)[:200])
            last_err = exc
            continue
    if last_err:
        log.warning("All candidate endpoints failed; returning empty.")
    return []


def sync_stockadjustments(client: Cin7Client, days: int) -> None:
    """Stock adjustment list endpoint usually already returns line-level rows."""
    since = _iso_days_ago(days)
    log.info("Pulling stock adjustments since %s ...", since)
    rows = _try_paginate(
        client,
        candidates=[
            ("stockAdjustmentList", "StockAdjustmentList"),
            ("stockadjustmentList", "StockAdjustmentList"),
            ("ref/stockAdjustment", "StockAdjustmentList"),
        ],
        params={"UpdatedSince": since},
    )
    write_outputs(f"stock_adjustments_last_{days}d", rows)


def sync_stocktransfers(client: Cin7Client, days: int) -> None:
    """Stock transfer list endpoint usually already returns line-level rows."""
    since = _iso_days_ago(days)
    log.info("Pulling stock transfers since %s ...", since)
    rows = _try_paginate(
        client,
        candidates=[
            ("stockTransferList", "StockTransferList"),
            ("stocktransferList", "StockTransferList"),
            ("ref/stockTransfer", "StockTransferList"),
        ],
        params={"UpdatedSince": since},
    )
    write_outputs(f"stock_transfers_last_{days}d", rows)


def sync_movements(client: Cin7Client, days: int) -> None:
    """Run all stock movement syncs back-to-back."""
    sync_stockadjustments(client, days)
    sync_stocktransfers(client, days)


def sync_assemblies(client: "Cin7Client", days: int) -> None:
    """Pull completed Finished Goods (FG-XXXX) tasks for the last N days
    and flatten per-component pick-line consumption into a CSV.

    v2.67.334 — James 2026-06-01: components that are mostly consumed
    via assemblies (kits being built) were dramatically under-forecast
    because the engine only counted direct sales + BOM rollup. For
    SKUs like LED-NEON-FLEX-NICHO-3000K-2 we saw ~5 direct sales but
    ~90 FG- consumptions per month — the BOM rollup picked up only a
    fraction. This sync captures the ground truth: every component
    line CIN7 actually decremented during an assembly task.

    Output: assemblies_last_{days}d_<timestamp>.csv with one row per
    (TaskID, ComponentSKU) pair:
        TaskID, AssemblyNumber, Date, CompletionDate, Status,
        ParentProductID, ParentSKU, ParentName, ParentQuantity,
        ComponentProductID, ComponentSKU, ComponentName, Quantity,
        Unit, Cost, BinID, Bin

    The finishedGoodsList endpoint has no date/UpdatedSince filter.
    Its list-level Date is not always the completion date the engine
    needs, so short-window syncs keep a wider candidate buffer and
    filter again after fetching detail.CompletionDate."""
    log.info("Pulling Finished Goods (assembly tasks) for last %d days...",
             days)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    buffer_days = int(os.environ.get(
        "CIN7_ASSEMBLY_LIST_BUFFER_DAYS", "180") or "180")
    candidate_cutoff = (
        cutoff - timedelta(days=buffer_days)
        if days <= 45 and buffer_days > 0 else cutoff)

    def _parse_dt(s):
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            return None

    def _row_dates(row: Dict[str, Any]) -> List[datetime]:
        out: List[datetime] = []
        for key in (
            "CompletionDate", "Date", "Updated", "LastUpdated",
            "Created", "CreatedDate", "ModifiedDate",
        ):
            dt = _parse_dt(row.get(key))
            if dt is not None:
                out.append(dt)
        return out

    # v2.67.336 — paginate FULLY and filter client-side. The earlier
    # streak-break optimisation assumed CIN7 returns the list newest-
    # first, but the v2.67.335 backfill log showed "Found 0 in window"
    # in 0.7s — i.e. page 1 had 25 consecutive OLD rows and we aborted.
    # Empirically CIN7 sorts finishedGoodsList oldest-first (or by some
    # field unrelated to Date), so the only safe strategy is to walk
    # every page.
    #
    # v2.67.371 — for short 30d pulls, keep a wider candidate buffer
    # because list-level Date can be task creation date while detail
    # CompletionDate is the actual movement date. We filter again after
    # the detail call before writing pick lines.
    tasks: List[Dict[str, Any]] = []
    total_scanned = 0
    for row in client.paginate(
        "finishedGoodsList", result_key="FinishedGoods",
        params={"Status": "COMPLETED"},
    ):
        total_scanned += 1
        dates = _row_dates(row)
        if not dates or any(dt >= candidate_cutoff for dt in dates):
            tasks.append(row)
        if total_scanned % 500 == 0:
            log.info("  Scanned %d list rows (%d in window so far)...",
                     total_scanned, len(tasks))

    log.info("  Scanned %d total rows; %d candidate tasks "
             "(candidate cutoff %s; final cutoff %s).",
             total_scanned, len(tasks),
             candidate_cutoff.date().isoformat(),
             cutoff.date().isoformat())

    # v2.67.337 — process NEWEST tasks first. CIN7's list sorts oldest-
    # first, but recent assemblies are far more demand-relevant — they
    # drive the 45d / momentum signals immediately. Reversing means
    # within the first hour the engine has actionable recent data even
    # if the full 365-day backfill takes many hours.
    tasks.reverse()

    # v2.67.337 — checkpoint + incremental write so a 14k-task backfill
    # survives Render container restarts and feeds the engine
    # progressively. Without this the CSV is written ONLY at the very
    # end, so any restart loses hours of work and the engine sees
    # nothing until completion.
    ckpt_name = f"assemblies_{days}d_v2_completion"
    state = _load_checkpoint(ckpt_name)
    processed_ids: set = set(state.get("processed_ids") or [])
    rows: List[Dict[str, Any]] = []
    if processed_ids:
        # Try to recover already-flushed rows from the most recent
        # same-window CSV so we don't start the rows array empty.
        prior_files = sorted(
            OUTPUT_DIR.glob(f"assemblies_last_{days}d_*.csv"),
            key=lambda p: p.stat().st_mtime,
        )
        if prior_files:
            latest = prior_files[-1]
            try:
                with latest.open(newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for r in reader:
                        rows.append(r)
                log.info("  Resuming: %d rows loaded from %s",
                         len(rows), latest.name)
            except Exception as exc:  # noqa: BLE001
                log.warning("  Could not read prior CSV (%s); "
                            "starting rows fresh.", exc)
                rows = []
        log.info("  Resuming from checkpoint: %d tasks already done.",
                 len(processed_ids))

    detail_errors = 0
    flushed_at = len(processed_ids)
    FLUSH_EVERY = 100

    for i, task in enumerate(tasks, 1):
        tid = task.get("TaskID")
        if not tid:
            continue
        tid_s = str(tid)
        if tid_s in processed_ids:
            continue
        try:
            detail = client.get("finishedGoods", params={"TaskID": tid})
        except Exception as exc:  # noqa: BLE001
            detail_errors += 1
            log.warning("  finishedGoods detail %s failed: %s",
                        task.get("AssemblyNumber"), exc)
            if detail_errors > 50:
                log.error("Too many detail errors; aborting assembly sync.")
                break
            continue

        if not isinstance(detail, dict):
            continue
        pick_lines = detail.get("PickLines") or []
        if not isinstance(pick_lines, list):
            continue

        parent_sku = task.get("ProductCode")
        parent_name = task.get("ProductName")
        parent_qty = task.get("Quantity")
        completion = detail.get("CompletionDate") or task.get("Date")
        completion_dt = _parse_dt(completion)
        if completion_dt is not None and completion_dt < cutoff:
            processed_ids.add(tid_s)
            continue
        status = detail.get("Status") or task.get("Status")
        for pl in pick_lines:
            if not isinstance(pl, dict):
                continue
            rows.append({
                "TaskID": tid,
                "AssemblyNumber": task.get("AssemblyNumber"),
                "Date": task.get("Date"),
                "CompletionDate": completion,
                "Status": status,
                "ParentProductID": task.get("ProductID"),
                "ParentSKU": parent_sku,
                "ParentName": parent_name,
                "ParentQuantity": parent_qty,
                "ComponentProductID": pl.get("ProductID"),
                "ComponentSKU": pl.get("ProductCode"),
                "ComponentName": pl.get("Name"),
                "Quantity": pl.get("Quantity"),
                "Unit": pl.get("Unit"),
                "Cost": pl.get("Cost"),
                "BinID": pl.get("BinID"),
                "Bin": pl.get("Bin"),
            })

        processed_ids.add(tid_s)

        if i % 50 == 0:
            log.info("  Processed %d/%d assemblies (skipped %d "
                     "already-done)...", i, len(tasks),
                     i - (len(processed_ids) - flushed_at))

        # v2.67.337 — incremental flush. Engine picks up new data on
        # next refresh, and a Render restart loses at most FLUSH_EVERY
        # tasks of progress.
        if (len(processed_ids) - flushed_at) >= FLUSH_EVERY:
            log.info("  Flushing: %d tasks processed, %d rows so far...",
                     len(processed_ids), len(rows))
            write_outputs(f"assemblies_last_{days}d", rows)
            _save_checkpoint(ckpt_name, {
                "processed_ids": sorted(processed_ids),
            })
            flushed_at = len(processed_ids)

    log.info("  Final write: %d component-consumption rows from %d tasks.",
             len(rows), len(processed_ids))
    write_outputs(f"assemblies_last_{days}d", rows)
    _clear_checkpoint(ckpt_name)


# ---------------------------------------------------------------------------
# BOM structure (parent-child relationships)
# ---------------------------------------------------------------------------


def sync_boms(client: Cin7Client, days: int = 0) -> None:
    """Fetch BOM structure for every BOM-flagged product.

    The product list endpoint doesn't return BillOfMaterialsProducts, so we
    have to call /product?ID={id} for each BOM product and extract the
    parent-child-quantity rows. Run once when BOMs are stable, then rerun
    only when new BOM products are added or structures change.

    Output: boms_YYYY-MM-DD.csv with rows of
    (AssemblySKU, ComponentSKU, Quantity, BOMType, AutoAssembly,
     AutoDisassembly, AssemblyName, ComponentName).
    `days` arg is accepted for uniform CLI but ignored.
    """
    log.info("Loading latest products to find BOM candidates...")
    # Try to read a recent local products export first (saves API calls)
    local_files = sorted(OUTPUT_DIR.glob("products_*.json"))
    products: List[Dict[str, Any]] = []
    if local_files:
        try:
            products = json.loads(local_files[-1].read_text(encoding="utf-8"))
            log.info("  Using %d products from %s",
                     len(products), local_files[-1].name)
        except Exception as exc:
            log.warning("  Could not read local products file: %s", exc)
            products = []
    if not products:
        log.info("  No local products file; fetching from CIN7...")
        products = list(client.paginate("product", result_key="Products"))

    bom_products = [
        p for p in products
        if str(p.get("BillOfMaterial")).lower() == "true"
    ]
    log.info("Found %d BOM-flagged products (of %d total).",
             len(bom_products), len(products))
    log.info("Will fetch detail for each (~%.1f hours at %.2fs/call).",
             len(bom_products) * client.rate_seconds / 3600,
             client.rate_seconds)

    # Build a fast lookup for component names
    by_id = {p.get("ID"): p for p in products}
    by_sku = {p.get("SKU"): p for p in products}

    all_rows: List[Dict[str, Any]] = []
    state = _load_checkpoint("boms")
    processed: set = set(state.get("processed_ids") or [])
    all_rows = state.get("rows") or []

    errors = 0
    try:
        for i, prod in enumerate(bom_products, 1):
            pid = prod.get("ID")
            sku = prod.get("SKU")
            if not pid or pid in processed:
                continue
            try:
                # IncludeBOM=true is required or CIN7 omits BillOfMaterials* !
                detail = client.get(
                    "product",
                    params={
                        "ID": pid,
                        "IncludeBOM": "true",
                        "IncludeSuppliers": "true",
                        "IncludeReorderLevels": "true",
                    },
                )
            except Exception as exc:
                errors += 1
                log.warning("  Failed to fetch %s: %s", sku, exc)
                processed.add(pid)
                if errors > 50:
                    raise RuntimeError("Too many BOM fetch errors. Aborting.")
                continue

            # CIN7 returns a Products array with one element when querying by ID
            rec = detail
            if isinstance(detail, dict) and "Products" in detail:
                ps = detail.get("Products") or []
                if ps and isinstance(ps[0], dict):
                    rec = ps[0]

            comps = rec.get("BillOfMaterialsProducts") or []
            if not isinstance(comps, list):
                comps = []

            assembly_name = rec.get("Name") or prod.get("Name")
            bom_type = rec.get("BOMType") or prod.get("BOMType")
            auto_asm = rec.get("AutoAssembly") or prod.get("AutoAssembly")
            auto_dis = rec.get("AutoDisassembly") or prod.get("AutoDisassembly")

            for c in comps:
                if not isinstance(c, dict):
                    continue
                # CIN7's BillOfMaterialsProducts uses these exact field names:
                #   ProductCode         — the component's SKU (string)
                #   ComponentProductID  — the component's product UUID
                # Older/alternative names kept as fallbacks in case CIN7
                # ever changes the schema or plan tier.
                comp_sku = (
                    c.get("ProductCode")
                    or c.get("SKU")
                    or by_id.get(c.get("ComponentProductID"), {}).get("SKU")
                    or by_id.get(c.get("ProductID"), {}).get("SKU")
                )
                comp_name = (
                    c.get("Name")
                    or by_id.get(c.get("ComponentProductID"), {}).get("Name")
                    or by_id.get(c.get("ProductID"), {}).get("Name")
                    or by_sku.get(comp_sku, {}).get("Name")
                )
                all_rows.append({
                    "AssemblySKU": sku,
                    "AssemblyName": assembly_name,
                    "ComponentSKU": comp_sku,
                    "ComponentName": comp_name,
                    "Quantity": c.get("Quantity"),
                    "BOMType": bom_type,
                    "AutoAssembly": auto_asm,
                    "AutoDisassembly": auto_dis,
                })
            processed.add(pid)

            if i % 25 == 0 or i == len(bom_products):
                _save_checkpoint("boms", {
                    "processed_ids": list(processed),
                    "rows": all_rows,
                    "updated": datetime.now().isoformat(),
                })
                log.info("  progress %d/%d   rows: %d   errors: %d",
                         i, len(bom_products), len(all_rows), errors)
    except KeyboardInterrupt:
        log.warning("Interrupted. Progress saved to checkpoint — rerun to resume.")
        _save_checkpoint("boms", {
            "processed_ids": list(processed),
            "rows": all_rows,
            "updated": datetime.now().isoformat(),
        })
        raise

    write_outputs("boms", all_rows)
    _clear_checkpoint("boms")


# ---------------------------------------------------------------------------
# Composite commands
# ---------------------------------------------------------------------------


def sync_quick(client: Cin7Client, days: int) -> None:
    """Fast daily refresh — masters + headers only. No detail loops."""
    sync_test(client)
    sync_products(client)
    sync_stock(client)
    sync_customers(client)
    sync_suppliers(client)
    sync_sales(client, days)
    sync_purchases(client, days)
    # v2.67.334 — pull assembly (FG-XXXX) consumption so the engine
    # can attribute demand to components that are mostly built into
    # kits rather than sold directly. Daily sync now runs a dedicated
    # 30-day assembly pull, so it sets CIN7_QUICK_SKIP_ASSEMBLIES=1 to
    # avoid scanning finishedGoods twice in one run.
    if os.environ.get("CIN7_QUICK_SKIP_ASSEMBLIES", "").strip() == "1":
        log.info("Skipping quick assembly pull "
                 "(CIN7_QUICK_SKIP_ASSEMBLIES=1).")
    else:
        sync_assemblies(client, days)


def sync_nearsync(client: Cin7Client, days: int = 1) -> None:
    """Near-real-time sync — stock snapshot + last-day movements + recent
    sale/purchase headers + sale/purchase LINE items. Designed to be run
    every 5-15 minutes from Task Scheduler. Skips masters
    (products/customers/suppliers) which are refreshed by the daily
    `quick` sync.

    NOTE: sale_lines and purchase_lines ARE included here — the Streamlit
    Overview's "Today" tile computes $/units from line-level data, so
    skipping them means the dashboard lags reality by hours. The extra
    API cost for a 1-day window is usually a handful of calls (<10 sale
    line fetches for a typical day)."""
    log.info("== NEAR-REALTIME SYNC ==")
    sync_stock(client)
    sync_stockadjustments(client, days)
    sync_stocktransfers(client, days)
    sync_sales(client, days)
    sync_salelines(client, days)
    sync_purchases(client, days)
    sync_purchaselines(client, days)
    # v2.67.336 — assemblies were briefly included in nearsync (v2.67.334)
    # but the finishedGoodsList endpoint has no date filter, so each
    # call has to scan ALL completed FG tasks (potentially thousands of
    # pages × 2.5s rate limit = many minutes). Far too slow for the
    # 15-min cadence — it would just keep falling behind. Daily quick
    # sync runs it instead; assembly demand changes slowly enough that
    # daily refresh is plenty.
    # Also trim old timestamped files for these prefixes to keep the output
    # folder manageable. Keep the last 24 per prefix (~6 hours at 15 min).
    _trim_old_files([
        "stock_on_hand",
        "stock_adjustments_last_",
        "stock_transfers_last_",
        "sales_last_",
        "sale_lines_last_1d",
        "purchases_last_",
        "purchase_lines_last_1d",
    ], keep_n=24)


def _trim_old_files(prefixes: List[str], keep_n: int = 24) -> None:
    """Keep the N most recent files per prefix, delete older ones."""
    for prefix in prefixes:
        for ext in ("csv", "json"):
            files = sorted(OUTPUT_DIR.glob(f"{prefix}*_*.{ext}"))
            for old in files[:-keep_n]:
                try:
                    old.unlink()
                except Exception as exc:
                    log.warning("  Could not delete %s: %s", old.name, exc)


def sync_full(client: Cin7Client, days: int) -> None:
    """Full sync including line items — can take hours on first run."""
    sync_quick(client, days)
    sync_salelines(client, days)
    sync_purchaselines(client, days)
    sync_movements(client, days)


# Aliases for backwards compatibility
def sync_all(client: Cin7Client, days: int) -> None:
    sync_quick(client, days)


COMMANDS = {
    "test": ("no-days", sync_test),
    "products": ("no-days", sync_products),
    "stock": ("no-days", sync_stock),
    "customers": ("no-days", sync_customers),
    "suppliers": ("no-days", sync_suppliers),
    "sales": ("days", sync_sales),
    "purchases": ("days", sync_purchases),
    "salelines": ("days", sync_salelines),
    "purchaselines": ("days", sync_purchaselines),
    "stockadjustments": ("days", sync_stockadjustments),
    "stocktransfers": ("days", sync_stocktransfers),
    "movements": ("days", sync_movements),
    "assemblies": ("days", sync_assemblies),
    "boms": ("no-days", lambda client: sync_boms(client)),
    "quick": ("days", sync_quick),
    "nearsync": ("days", sync_nearsync),
    "full": ("days", sync_full),
    "all": ("days", sync_all),
}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="CIN7 Core -> local CSV/JSON bridge.")
    p.add_argument(
        "command",
        choices=list(COMMANDS.keys()),
        help="Which sync to run.",
    )
    p.add_argument(
        "--days",
        type=int,
        default=30,
        help="Look-back window in days (for commands that filter by UpdatedSince).",
    )
    return p


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv(SCRIPT_DIR / ".env")
    args = build_arg_parser().parse_args(argv)

    account_id = os.environ.get("CIN7_ACCOUNT_ID", "").strip()
    application_key = os.environ.get("CIN7_APPLICATION_KEY", "").strip()
    rate_env = os.environ.get("CIN7_RATE_SECONDS", "").strip()
    try:
        rate_seconds = float(rate_env) if rate_env else DEFAULT_RATE_LIMIT_SECONDS
    except ValueError:
        log.warning("CIN7_RATE_SECONDS=%r is not a number; using default %.2f",
                    rate_env, DEFAULT_RATE_LIMIT_SECONDS)
        rate_seconds = DEFAULT_RATE_LIMIT_SECONDS

    try:
        client = Cin7Client(account_id, application_key, rate_seconds=rate_seconds)
    except ValueError as exc:
        log.error(str(exc))
        return 2

    try:
        kind, fn = COMMANDS[args.command]
        if kind == "days":
            fn(client, args.days)
        else:
            fn(client)
    except KeyboardInterrupt:
        log.warning("Interrupted by user.")
        return 130
    except Exception as exc:  # noqa: BLE001
        log.exception("Sync failed: %s", exc)
        return 1

    log.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
