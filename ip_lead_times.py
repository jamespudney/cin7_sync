"""
ip_lead_times.py (v2.67.285)
============================

Pull observed actual lead times from Inventory Planner and store them
in the local ip_lead_times table so the reorder engine prefers them
over a stale supplier_config default.

The motivation
--------------
Our supplier_config defaults sea lead time to 35 days when the
supplier hasn't been configured. IP literally measures the real time
between PO placement and receipt (`avg_lead_time` on each warehouse
block) and uses it for its own replenishment math. For a steady
supplier the observed actual is far more honest than a one-size
default — and using it is the single biggest "free cash" lever in the
reorder engine.

What we store
-------------
Per SKU (the natural key) we capture:
  observed_lead_time_days   IP's avg_lead_time (measured actual)
  configured_lead_time_days IP's lead_time setting (the curated value)
  vendor_name               best-effort vendor for context
  sales_velocity1           IP's daily velocity (so we know which
                             warehouse the lead time came from)
  last_received_at          most recent PO receipt date

The reorder engine prefers observed if it's in a sane range (3-120
days), else configured, else the supplier_config default.

CLI
---
    python ip_lead_times.py sync                       # full pull
    python ip_lead_times.py sync --limit-pages 1       # smoke test
    python ip_lead_times.py sync --dry-run --verbose   # log only

Env: IP_API_KEY, IP_ACCOUNT (same as ip_fetch_one.py / ip_probe.py).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402

BASE_URL = "https://app.inventory-planner.com/api/v1"
PAGE_SIZE = 1000  # IP allows up to 1000 per docs
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 cin7-sync/1.0"
)
# Minimal fields — smaller payload, fewer socket timeouts on a 12k-
# variant walk. IP's field allowlist needs dot-notation to pull
# nested warehouse sub-fields; otherwise `warehouse` returns just
# the warehouse-id stub and lead_time/avg_lead_time are missing.
FIELDS = (
    "id,connections,"
    "warehouse.lead_time,"
    "warehouse.avg_lead_time,"
    "warehouse.forecast_description,"
    "warehouse.last_received_at_time"
)

log = logging.getLogger("ip_lead_times")


def _setup_log(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        stream=sys.stdout, force=True)


# ---------------------------------------------------------------------------
# Per-variant field extraction (defensive — IP returns slightly
# different shapes across endpoints/versions)
# ---------------------------------------------------------------------------
def _master_sku(variant: dict) -> Optional[str]:
    """Canonical SKU. Lives at connections[0].sku per IP convention."""
    conns = variant.get("connections") or []
    if conns and isinstance(conns[0], dict):
        return conns[0].get("sku")
    return None


def _vendor_name(variant: dict) -> Optional[str]:
    """Best-effort vendor name from connections[0]."""
    conns = variant.get("connections") or []
    if conns and isinstance(conns[0], dict):
        return (conns[0].get("vendor_name")
                or conns[0].get("vendor")
                or None)
    return None


def _collect_warehouse_blocks(variant: dict) -> List[dict]:
    """Every warehouse-like dict on a variant, regardless of where IP
    nests them. Variant-level `warehouse`/`warehouses` and inside each
    connection's `warehouse`/`warehouses` are all picked up."""
    out: List[dict] = []
    for key in ("warehouse", "warehouses"):
        v = variant.get(key)
        if isinstance(v, list):
            out.extend(w for w in v if isinstance(w, dict))
        elif isinstance(v, dict):
            out.append(v)
    for conn in variant.get("connections") or []:
        if not isinstance(conn, dict):
            continue
        for key in ("warehouse", "warehouses"):
            v = conn.get(key)
            if isinstance(v, list):
                out.extend(w for w in v if isinstance(w, dict))
            elif isinstance(v, dict):
                out.append(v)
    return out


def _wh_velocity(wh: dict) -> float:
    """Daily velocity from forecast_description.sales_velocity1, or
    a top-level sales_velocity1 if IP put it there."""
    fd = wh.get("forecast_description") or {}
    try:
        return float(fd.get("sales_velocity1")
                     or wh.get("sales_velocity1")
                     or 0)
    except (TypeError, ValueError):
        return 0.0


def _coerce_days(v: Any) -> Optional[int]:
    """Numeric → int days; treat zero/null/garbage as None."""
    if v in (None, "", 0, 0.0):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f <= 0:
        return None
    return int(round(f))


def _wh_lead_times(wh: dict) -> Tuple[Optional[int], Optional[int]]:
    """Return (avg_lead_time, lead_time) for a warehouse block."""
    return _coerce_days(wh.get("avg_lead_time")), \
            _coerce_days(wh.get("lead_time"))


def _pick_lead_times(variant: dict
                      ) -> Tuple[Optional[int], Optional[int], float,
                                  Optional[str]]:
    """Pick the most active warehouse with lead-time data.
    Returns (observed, configured, velocity, last_received_at)."""
    candidates = _collect_warehouse_blocks(variant)
    if not candidates:
        return None, None, 0.0, None
    # Score: higher velocity wins; tiebreak by having ANY lead time;
    # then by observed value (so a real avg beats 0 / missing).
    def _score(wh: dict):
        avg, conf = _wh_lead_times(wh)
        return (
            _wh_velocity(wh),
            1 if (avg or conf) else 0,
            float(avg or 0),
        )
    candidates.sort(key=_score, reverse=True)
    best = candidates[0]
    avg, conf = _wh_lead_times(best)
    vel = _wh_velocity(best)
    last_received = best.get("last_received_at_time")
    return avg, conf, vel, last_received


# ---------------------------------------------------------------------------
# IP /variants pagination
# ---------------------------------------------------------------------------
def fetch_variants(headers: Dict[str, str],
                    rate: float,
                    page_limit: Optional[int] = None
                    ) -> Iterable[dict]:
    """Paginate /variants, yielding one variant dict at a time. Stops
    when a page returns fewer than PAGE_SIZE records (end-of-data)."""
    page = 0
    last_call = 0.0
    while True:
        elapsed = time.time() - last_call
        if elapsed < rate:
            time.sleep(rate - elapsed)
        params = {"page": page, "limit": PAGE_SIZE, "fields": FIELDS}
        try:
            r = requests.get(f"{BASE_URL}/variants",
                              headers=headers, params=params,
                              timeout=60)
            last_call = time.time()
        except requests.RequestException as exc:
            log.warning("page %d network err %s — retry in 5s",
                          page, exc)
            time.sleep(5)
            continue
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "30"))
            log.warning("429 on page %d, sleeping %ds", page, wait)
            time.sleep(wait)
            continue
        if r.status_code != 200:
            log.error("page %d HTTP %d: %s",
                       page, r.status_code, r.text[:200])
            return
        try:
            body = r.json()
        except ValueError:
            log.error("page %d returned non-JSON: %s",
                       page, r.text[:200])
            return
        variants = body.get("variants") or []
        log.info("page %d -> %d variants", page, len(variants))
        for v in variants:
            yield v
        if len(variants) < PAGE_SIZE:
            return
        page += 1
        if page_limit is not None and page >= page_limit:
            log.info("hit --limit-pages %d, stopping", page_limit)
            return


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def cmd_sync(args) -> int:
    _setup_log(args.verbose)
    load_dotenv()
    key = os.environ.get("IP_API_KEY")
    account = os.environ.get("IP_ACCOUNT")
    if not key or not account:
        log.error("IP_API_KEY / IP_ACCOUNT not set in .env")
        return 1
    headers = {
        "Authorization": key,
        "Account": account,
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    rate = float(os.environ.get("IP_RATE_SECONDS", "1.0"))
    log.info("Pulling IP variants and observed lead times "
              "(account=%s, rate=%.2fs/req)...", account, rate)

    n_total = 0
    n_with_lt = 0
    n_written = 0
    obs_values: List[int] = []
    conf_values: List[int] = []
    for v in fetch_variants(headers, rate, args.limit_pages):
        n_total += 1
        sku = _master_sku(v)
        if not sku:
            continue
        obs, conf, vel, last_rx = _pick_lead_times(v)
        if not (obs or conf):
            continue
        n_with_lt += 1
        if obs:
            obs_values.append(obs)
        if conf:
            conf_values.append(conf)
        if args.dry_run:
            log.debug("  %s: observed=%s configured=%s vel=%.3f",
                       sku, obs, conf, vel)
            continue
        try:
            db.upsert_ip_lead_time(
                sku=sku,
                observed_lead_time_days=obs,
                configured_lead_time_days=conf,
                vendor_name=_vendor_name(v),
                sales_velocity1=vel,
                last_received_at=last_rx)
            n_written += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("upsert failed for %s: %s", sku, exc)

    log.info("=" * 60)
    log.info("Variants scanned:        %d", n_total)
    log.info("With lead-time data:     %d", n_with_lt)
    log.info("Written to ip_lead_times: %d (dry-run=%s)",
              n_written, args.dry_run)
    if obs_values:
        obs_values.sort()
        log.info(
            "Observed lead times — min %d, median %d, max %d, n=%d",
            obs_values[0], obs_values[len(obs_values) // 2],
            obs_values[-1], len(obs_values))
    if conf_values:
        conf_values.sort()
        log.info(
            "Configured lead times — min %d, median %d, max %d, n=%d",
            conf_values[0], conf_values[len(conf_values) // 2],
            conf_values[-1], len(conf_values))
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Sync IP observed lead times into the local DB.")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser(
        "sync",
        help="Pull all IP variants and upsert their observed "
              "and configured lead times into ip_lead_times.")
    s.add_argument("--dry-run", action="store_true",
                     help="Walk IP but don't write to the DB.")
    s.add_argument("--verbose", action="store_true")
    s.add_argument("--limit-pages", type=int, default=None,
                     help="Stop after N pages (smoke test).")
    s.set_defaults(func=cmd_sync)
    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
