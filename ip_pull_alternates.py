"""
ip_pull_alternates.py
=====================
Comprehensive one-shot extraction of everything our team has curated
inside Inventory Planner. Designed for the long-term plan of
decommissioning IP — captures all API-accessible buyer expertise into
portable CSVs that survive even after IP is gone.

What we extract:
  1. "Combine sales/stock" relationships (the `merged` array) — the
     authoritative alternatives map.
  2. Replenishment notes — per-warehouse free-text where buyers write
     things like "E60L24DC REPLACEMENT", "BWF - MOQ 1000m/profile",
     "Keep max 50 meter SOH - Use 5 meter SK". Years of buyer expertise.
  3. Variant tags — operational labels like "overstock11/01".
  4. Per-warehouse curated settings — lead time, review period,
     replenishment qty, minimum stock, MOQ flag, assembly time/cycle,
     ABC segment, forecast method, has_forecast_override flag,
     prices/costs.
  5. Velocities — IP's computed sales rates (30d/1d), historical
     stockout days, forward stockout forecast, days-of-cover estimates,
     and pre-computed sales windows (7/30/90/180/365 days).
  6. Vendor data — per-variant per-vendor cost prices and currencies.
  7. Forecasts — IP's 18-month forward forecast as JSON, including the
     forecasting methodology description.

What we CANNOT extract (UI-only fields, gone if IP is decommissioned):
  - "Max stock" column from the Replenishment view
  - Explicit MOQ quantity (we get above_moq boolean only)
  - Forecast-period manual overrides (only the boolean flag is exposed)
  - Saved buyer reports / dashboard configurations

For these, do a one-time CSV export from IP's UI before pulling the plug.

Output
------
    output/ip_alternates_<stamp>.csv
        MasterSKU, MasterID, AlternativeSKU, AlternativeID,
        Percent, Source, AlternativeTitle, AlternativeBarcode

    output/ip_notes_<stamp>.csv
        SKU, VariantID, WarehouseID, Note, Tags

    output/ip_variant_settings_<stamp>.csv
        SKU, VariantID, WarehouseID, LeadTime, ReviewPeriod,
        Replenishment, MinimumStock, AboveMOQ, AssemblyTime,
        AssemblyCycle, Segment, HasForecastOverride, ForecastMethod,
        InventoryManagement, RegularPrice, CostPrice, LandingCostPrice,
        Tags

    output/ip_velocities_<stamp>.csv
        SKU, VariantID, WarehouseID, CurrentSales, SalesVelocity30/1,
        OOSlast60days, TotalDaysOOS, ForecastedStockoutsDoS,
        ForecastStockCover (day/week/mo), Last7/30/90/180/365 days
        sales + revenue, MinimumStock, Replenishment, LeadTime

    output/ip_vendors_<stamp>.csv
        SKU, VariantID, Vendor, CostPrice, CostPriceCurrency,
        LandingCostPrice

    output/ip_forecasts_<stamp>.csv
        SKU, VariantID, WarehouseID, ForecastByPeriod_JSON,
        ForecastRevenue_JSON, ForecastDescription_JSON,
        Last90DaysSales/Revenue, ForecastedLost (revenue/sales) lead time

    output/ip_variants_summary_<stamp>.csv
        SKU, ID, MergeCount, NoteCount, TagCount, HasForecastOverride

    output/ip_alternates.log
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
from dotenv import load_dotenv

BASE_URL = "https://app.inventory-planner.com/api/v1"
PAGE_SIZE = 1000  # IP allows up to 1000 per docs
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 cin7-sync/1.0"
)

# We restrict the response to just the fields we need. Smaller payload =
# faster pull = less risk of socket timeouts on a 12k-variant walk.
#   id           — variant's IP-internal id
#   connections  — canonical SKU lives at connections[0].sku
#   merged       — curated alternatives ("Combine sales/stock")
#   warehouse    — per-warehouse settings: lead_time, replenishment,
#                   minimum_stock, above_moq, replenishment_notes
#   tags         — variant-level tags (operational labels)
FIELDS = "id,connections,merged,warehouse,tags"

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _setup_log() -> logging.Logger:
    log = logging.getLogger("ip_alternates")
    log.setLevel(logging.INFO)
    if not log.handlers:
        fh = logging.FileHandler(OUTPUT_DIR / "ip_alternates.log",
                                  encoding="utf-8")
        fh.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-8s %(message)s"))
        log.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("%(message)s"))
        log.addHandler(sh)
    return log


def _master_sku(variant: Dict[str, Any]) -> Optional[str]:
    """Return the canonical SKU from variant.connections[0].sku."""
    conns = variant.get("connections") or []
    if conns and isinstance(conns[0], dict):
        return conns[0].get("sku")
    return None


def fetch_all_variants(headers: Dict[str, str],
                        rate: float,
                        log: logging.Logger,
                        ) -> Iterable[Dict[str, Any]]:
    """Paginate through /variants, yielding one variant dict at a time."""
    page = 0
    total_yielded = 0
    last_call = 0.0
    while True:
        # Throttle (IP doesn't publish a rate limit but we play nice).
        elapsed = time.time() - last_call
        if elapsed < rate:
            time.sleep(rate - elapsed)

        params = {
            "page": page,
            "limit": PAGE_SIZE,
            "fields": FIELDS,
        }
        try:
            resp = requests.get(f"{BASE_URL}/variants",
                                  headers=headers, params=params,
                                  timeout=60)
            last_call = time.time()
        except requests.RequestException as exc:
            log.warning("  page %d network error: %s — retry in 5s", page, exc)
            time.sleep(5)
            continue

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", "30"))
            log.warning("  429 rate limit on page %d. Sleeping %ds", page, wait)
            time.sleep(wait)
            continue

        if resp.status_code != 200:
            log.error("  page %d failed status=%d body=%s",
                       page, resp.status_code, resp.text[:300])
            return

        try:
            data = resp.json()
        except ValueError:
            log.error("  page %d returned non-JSON: %s",
                       page, resp.text[:300])
            return

        variants = data.get("variants") or []
        meta = data.get("meta") or {}
        total = meta.get("total")
        log.info("  page %d -> %d records (total=%s, running=%d)",
                  page, len(variants), total,
                  total_yielded + len(variants))
        for v in variants:
            yield v
        total_yielded += len(variants)
        if len(variants) < PAGE_SIZE:
            return
        page += 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pull IP variants + curated alternatives into CSV")
    parser.add_argument(
        "--rate", type=float, default=None,
        help="Seconds between API calls (default: from IP_RATE_SECONDS or 1.0)")
    parser.add_argument(
        "--limit-pages", type=int, default=None,
        help="Stop after N pages (sanity test). Omit to pull everything.")
    args = parser.parse_args()

    load_dotenv()
    key = os.environ.get("IP_API_KEY")
    account = os.environ.get("IP_ACCOUNT")
    if not key or not account:
        print("ERROR: IP_API_KEY / IP_ACCOUNT not set in .env")
        return 1
    rate = (
        args.rate
        if args.rate is not None
        else float(os.environ.get("IP_RATE_SECONDS", "1.0"))
    )

    headers = {
        "Authorization": key,
        "Account": account,
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }

    log = _setup_log()
    log.info("Pulling IP variants (rate=%.1fs, page_size=%d)", rate, PAGE_SIZE)

    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    alt_csv = OUTPUT_DIR / f"ip_alternates_{stamp}.csv"
    sum_csv = OUTPUT_DIR / f"ip_variants_summary_{stamp}.csv"
    notes_csv = OUTPUT_DIR / f"ip_notes_{stamp}.csv"
    settings_csv = OUTPUT_DIR / f"ip_variant_settings_{stamp}.csv"
    velocities_csv = OUTPUT_DIR / f"ip_velocities_{stamp}.csv"
    vendors_csv = OUTPUT_DIR / f"ip_vendors_{stamp}.csv"
    forecasts_csv = OUTPUT_DIR / f"ip_forecasts_{stamp}.csv"

    n_variants = 0
    n_with_merged = 0
    n_alt_rows = 0
    n_with_notes = 0
    n_note_rows = 0
    n_with_tags = 0
    n_with_overrides = 0

    with alt_csv.open("w", newline="", encoding="utf-8") as af, \
         sum_csv.open("w", newline="", encoding="utf-8") as sf, \
         notes_csv.open("w", newline="", encoding="utf-8") as nf, \
         settings_csv.open("w", newline="", encoding="utf-8") as gf, \
         velocities_csv.open("w", newline="", encoding="utf-8") as vf, \
         vendors_csv.open("w", newline="", encoding="utf-8") as ven_f, \
         forecasts_csv.open("w", newline="", encoding="utf-8") as fc_f:
        alt_w = csv.writer(af)
        sum_w = csv.writer(sf)
        notes_w = csv.writer(nf)
        set_w = csv.writer(gf)
        vel_w = csv.writer(vf)
        ven_w = csv.writer(ven_f)
        fc_w = csv.writer(fc_f)
        alt_w.writerow([
            "MasterSKU", "MasterID",
            "AlternativeSKU", "AlternativeID",
            "Percent", "Source",
            "AlternativeTitle", "AlternativeBarcode",
        ])
        sum_w.writerow([
            "SKU", "ID", "MergeCount", "NoteCount", "TagCount",
            "HasForecastOverride"])
        notes_w.writerow([
            "SKU", "VariantID", "WarehouseID", "Note", "Tags"])
        # Settings: per-warehouse curated dimensions. NB: maximum_stock is
        # NOT in IP's API surface (UI-only); we capture everything else
        # the API exposes. Cost/price columns go here too because they're
        # part of "what was IP showing the buyer".
        set_w.writerow([
            "SKU", "VariantID", "WarehouseID",
            "LeadTime", "ReviewPeriod", "Replenishment",
            "MinimumStock", "AboveMOQ",
            "AssemblyTime", "AssemblyCycle",
            "Segment", "HasForecastOverride", "ForecastMethod",
            "InventoryManagement",
            "RegularPrice", "CostPrice", "LandingCostPrice",
            "Tags",
        ])
        # Velocities: IP's computed sales rates and stockout signals.
        # These are derived metrics — useful to compare against our own
        # engine's rates as a sanity check.
        vel_w.writerow([
            "SKU", "VariantID", "WarehouseID",
            "CurrentSales", "SalesVelocity30", "SalesVelocity1",
            "OOSlast60days", "TotalDaysOOS",
            "ForecastedStockoutsDoS", "ForecastStockCoverDays",
            "ForecastStockCoverWeeks", "ForecastStockCoverMonths",
            "Last7DaysSales", "Last30DaysSales", "Last90DaysSales",
            "Last180DaysSales", "Last365DaysSales",
            "Last7DaysRevenue", "Last30DaysRevenue", "Last90DaysRevenue",
            "Last180DaysRevenue", "Last365DaysRevenue",
            "MinimumStock", "Replenishment", "LeadTime",
        ])
        # Vendors: per-variant per-vendor cost data from connections[0].vendors.
        ven_w.writerow([
            "SKU", "VariantID",
            "Vendor", "CostPrice", "CostPriceCurrency", "LandingCostPrice",
        ])
        # Forecasts: 18-month forward forecast as a JSON-encoded mapping
        # of {year: {month: units}}. Stored as JSON string per row so the
        # CSV stays one-row-per-variant-per-warehouse but the structure
        # is preserved.
        fc_w.writerow([
            "SKU", "VariantID", "WarehouseID",
            "ForecastByPeriod_JSON", "ForecastRevenue_JSON",
            "ForecastDescription_JSON",
            "Last90DaysSales", "Last90DaysRevenue",
            "ForecastedLostRevenueLeadTime", "ForecastedLostSalesLeadTime",
        ])

        for v in fetch_all_variants(headers, rate, log):
            n_variants += 1
            master_sku = _master_sku(v)
            master_id = v.get("id")
            merged = v.get("merged") or []
            tags_raw = v.get("tags") or []
            tags_str = (",".join(str(t) for t in tags_raw)
                         if isinstance(tags_raw, list) else "")
            warehouses = v.get("warehouse") or []

            # --- Per-variant alternates (Combine sales/stock) ----------
            if merged:
                n_with_merged += 1
                for m in merged:
                    if not isinstance(m, dict):
                        continue
                    alt_w.writerow([
                        master_sku or "",
                        master_id or "",
                        m.get("sku") or "",
                        m.get("id") or "",
                        m.get("percent"),
                        m.get("source") or "",
                        (m.get("title") or "")[:200],
                        m.get("barcode") or "",
                    ])
                    n_alt_rows += 1

            # --- Per-variant vendor data (from connections[0].vendors) -
            # Captured once per variant — vendors aren't warehouse-specific.
            connections = v.get("connections") or []
            if connections and isinstance(connections[0], dict):
                vendors = connections[0].get("vendors") or []
                for vd in vendors:
                    if not isinstance(vd, dict):
                        continue
                    ven_w.writerow([
                        master_sku or "",
                        master_id or "",
                        vd.get("vendor") or "",
                        vd.get("cost_price"),
                        vd.get("cost_price_currency") or "",
                        vd.get("landing_cost_price"),
                    ])

            # --- Per-warehouse notes + curated settings + velocities ----
            note_count = 0
            had_override = False
            for wh in warehouses:
                if not isinstance(wh, dict):
                    continue
                wh_id = wh.get("warehouse") or ""
                note = (wh.get("replenishment_notes") or "").strip()

                # Settings dimensions
                lead_time = wh.get("lead_time")
                review_period = wh.get("review_period")
                replen = wh.get("replenishment")
                min_stock = wh.get("minimum_stock")
                above_moq = wh.get("above_moq")
                assembly_time = wh.get("assembly_time")
                assembly_cycle = wh.get("assembly_cycle")
                segment = wh.get("segment") or ""
                has_override = bool(wh.get("has_forecast_override"))
                if has_override:
                    had_override = True
                inv_mgmt = wh.get("inventory_management")
                regular_price = wh.get("regular_price")
                cost_price = wh.get("cost_price")
                landing_cost = wh.get("landing_cost_price")

                fdesc = wh.get("forecast_description") or {}
                forecast_method = (
                    fdesc.get("method") if isinstance(fdesc, dict) else "")

                # Notes CSV — only emit rows that actually have a note
                if note:
                    notes_w.writerow([
                        master_sku or "",
                        master_id or "",
                        wh_id,
                        note[:500],
                        tags_str,
                    ])
                    n_note_rows += 1
                    note_count += 1

                # Settings CSV — one row per warehouse, even if defaults
                set_w.writerow([
                    master_sku or "",
                    master_id or "",
                    wh_id,
                    lead_time, review_period, replen,
                    min_stock, above_moq,
                    assembly_time, assembly_cycle,
                    segment, has_override, forecast_method,
                    inv_mgmt,
                    regular_price, cost_price, landing_cost,
                    tags_str,
                ])

                # Velocities CSV — IP's computed metrics
                vel_w.writerow([
                    master_sku or "",
                    master_id or "",
                    wh_id,
                    wh.get("cur_sales"),
                    fdesc.get("sales_velocity30") if isinstance(fdesc, dict) else "",
                    fdesc.get("sales_velocity1") if isinstance(fdesc, dict) else "",
                    wh.get("oos_last_60_days"),
                    fdesc.get("total_days_oos") if isinstance(fdesc, dict) else "",
                    wh.get("forecasted_stockouts_dos"),
                    wh.get("forecast_stock_cover_day"),
                    wh.get("forecast_stock_cover_week"),
                    wh.get("forecast_stock_cover_mo"),
                    wh.get("last_7_days_sales"),
                    wh.get("last_30_days_sales"),
                    wh.get("last_90_days_sales"),
                    wh.get("last_180_days_sales"),
                    wh.get("last_365_days_sales"),
                    wh.get("last_7_days_revenue"),
                    wh.get("last_30_days_revenue"),
                    wh.get("last_90_days_revenue"),
                    wh.get("last_180_days_revenue"),
                    wh.get("last_365_days_revenue"),
                    min_stock, replen, lead_time,
                ])

                # Forecasts CSV — 18-month forward forecast (JSON for
                # structure preservation)
                fc_w.writerow([
                    master_sku or "",
                    master_id or "",
                    wh_id,
                    json.dumps(wh.get("forecast_by_period") or {}),
                    json.dumps(wh.get("forecast_revenue") or {}),
                    json.dumps(fdesc) if isinstance(fdesc, dict) else "",
                    wh.get("last_90_days_sales"),
                    wh.get("last_90_days_revenue"),
                    wh.get("forecasted_lost_revenue_lead_time"),
                    wh.get("forecasted_lost_sales_lead_time"),
                ])

            if note_count:
                n_with_notes += 1
            if tags_str:
                n_with_tags += 1
            if had_override:
                n_with_overrides += 1

            sum_w.writerow([
                master_sku or "",
                master_id or "",
                len(merged),
                note_count,
                len(tags_raw) if isinstance(tags_raw, list) else 0,
                had_override,
            ])

            if args.limit_pages and (n_variants // PAGE_SIZE) >= args.limit_pages:
                break

    log.info("=" * 60)
    log.info("Done.")
    log.info("  Variants pulled            : %d", n_variants)
    log.info("  Variants with merges       : %d (%.1f%%)",
              n_with_merged,
              (100.0 * n_with_merged / n_variants) if n_variants else 0)
    log.info("  Alternate links            : %d", n_alt_rows)
    log.info("  Variants with notes        : %d (%.1f%%)",
              n_with_notes,
              (100.0 * n_with_notes / n_variants) if n_variants else 0)
    log.info("  Note rows                  : %d", n_note_rows)
    log.info("  Variants with tags         : %d (%.1f%%)",
              n_with_tags,
              (100.0 * n_with_tags / n_variants) if n_variants else 0)
    log.info("  Variants with forecast override: %d (%.1f%%)",
              n_with_overrides,
              (100.0 * n_with_overrides / n_variants) if n_variants else 0)
    log.info("-" * 60)
    log.info("  Alternates CSV         : %s", alt_csv)
    log.info("  Notes CSV              : %s", notes_csv)
    log.info("  Settings CSV           : %s", settings_csv)
    log.info("  Velocities CSV         : %s", velocities_csv)
    log.info("  Vendors CSV            : %s", vendors_csv)
    log.info("  Forecasts CSV          : %s", forecasts_csv)
    log.info("  Summary CSV            : %s", sum_csv)
    log.info("-" * 60)
    log.info("Note: 'Max stock' and exact MOQ qty are NOT in IP's API")
    log.info("(UI-only fields). For those, use IP's CSV export feature")
    log.info("once before decommissioning.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
