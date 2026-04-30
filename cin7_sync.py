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


def write_outputs(name: str, rows: List[Dict[str, Any]]) -> Path:
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    json_path = OUTPUT_DIR / f"{name}_{stamp}.json"
    csv_path = OUTPUT_DIR / f"{name}_{stamp}.csv"

    json_path.write_text(
        json.dumps(rows, indent=2, ensure_ascii=False, default=str),
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
    """Pull the product master, including Suppliers + ReorderLevels.
    Without these Include* flags, CIN7 returns empty arrays for both,
    which breaks supplier auto-detection downstream."""
    log.info("Pulling product master (with Suppliers + ReorderLevels)...")
    rows = list(client.paginate(
        "product", result_key="Products",
        params={
            "IncludeSuppliers": "true",
            "IncludeReorderLevels": "true",
        },
    ))
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

    out: List[Dict[str, Any]] = []

    # Invoices usually hold what actually shipped
    for invoice in (detail.get("Invoices") or []):
        if not isinstance(invoice, dict):
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
            })

    return out


def _extract_purchase_lines(detail: Dict[str, Any], header: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Pull order + invoice lines from a /purchase response."""
    pid = header.get("ID") or detail.get("ID")
    supp = header.get("Supplier") or detail.get("Supplier")
    supp_id = header.get("SupplierID") or detail.get("SupplierID")
    order_num = header.get("OrderNumber") or detail.get("OrderNumber")
    order_date = header.get("OrderDate") or detail.get("OrderDate")
    required_by = header.get("RequiredBy") or detail.get("RequiredBy")
    status = header.get("Status") or detail.get("Status")

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
