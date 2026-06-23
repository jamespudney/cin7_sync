#!/usr/bin/env python3
"""
Read-only CIN7 demand audit for one SKU.

This is deliberately separate from the dashboard and AI prompt path. Run it
inside Render, where CIN7_ACCOUNT_ID and CIN7_APPLICATION_KEY are set, to
prove the source records behind a disputed Ordering / AI demand number.

Examples:
    python audit_live_cin7_demand.py LED-NEON-FLEX-NICHO-3000K-2
    python audit_live_cin7_demand.py LED-NEON-FLEX-NICHO-3000K-2 --from 2026-06-01 --to 2026-06-23 --live-product-movements
    python audit_live_cin7_demand.py LED-NEON-FLEX-NICHO-3000K-2 --live-assemblies --live-sales
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv

from cin7_sync import Cin7Client, OUTPUT_DIR, _extract_sale_lines


BAD_SALE_STATUSES = {"CREDITED", "VOIDED", "CANCELLED", "CANCELED"}
BAD_ASSEMBLY_STATUSES = {"VOIDED", "CANCELLED", "CANCELED", "DRAFT"}


def _parse_day(value: Any) -> Optional[date]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _qty(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _status(row: Dict[str, Any]) -> str:
    return str(row.get("Status") or "").strip().upper()


def _latest(prefixes: Iterable[str]) -> Optional[Path]:
    files: List[Path] = []
    for prefix in prefixes:
        files.extend(OUTPUT_DIR.glob(f"{prefix}_*.csv"))
    if not files:
        return None
    return sorted(files, key=lambda p: p.stat().st_mtime)[-1]


def _read_csv(path: Optional[Path]) -> List[Dict[str, Any]]:
    if path is None or not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _in_period(day: Optional[date], start: date, end: date) -> bool:
    return day is not None and start <= day <= end


def _summarise(rows: List[Dict[str, Any]], date_key: str) -> None:
    if not rows:
        print("    (no rows)")
        return
    for row in rows[:40]:
        day = row.get(date_key) or row.get("InvoiceDate") or row.get("CompletionDate")
        bits = [
            str(day or ""),
            str(row.get("OrderNumber") or row.get("AssemblyNumber") or ""),
            str(row.get("SKU") or row.get("ComponentSKU") or ""),
            str(row.get("ParentSKU") or ""),
            str(row.get("Customer") or ""),
            f"qty={_qty(row.get('Quantity')):g}",
            str(row.get("Status") or ""),
        ]
        print("    " + " | ".join(bit for bit in bits if bit))
    if len(rows) > 40:
        print(f"    ... {len(rows) - 40} more rows")


def local_direct_sales(
    sku: str,
    start: date,
    end: date,
) -> Tuple[float, List[Dict[str, Any]], Optional[Path]]:
    path = _latest([
        "sale_lines_last_30d",
        "sale_lines_last_45d",
        "sale_lines_last_90d",
        "sale_lines_last_365d",
        "sale_lines_last_730d",
        "sale_lines",
    ])
    rows = []
    total = 0.0
    for row in _read_csv(path):
        if str(row.get("SKU") or "") != sku:
            continue
        if _status(row) in BAD_SALE_STATUSES:
            continue
        day = _parse_day(row.get("InvoiceDate"))
        if not _in_period(day, start, end):
            continue
        qty = _qty(row.get("Quantity"))
        total += qty
        rows.append(row)
    return total, rows, path


def local_bom_rollup_estimate(
    sku: str,
    start: date,
    end: date,
) -> Tuple[float, List[Dict[str, Any]], Optional[Path], Optional[Path]]:
    bom_path = _latest(["boms"])
    sl_path = _latest([
        "sale_lines_last_30d",
        "sale_lines_last_45d",
        "sale_lines_last_90d",
        "sale_lines_last_365d",
        "sale_lines_last_730d",
        "sale_lines",
    ])
    boms = _read_csv(bom_path)
    sale_lines = _read_csv(sl_path)
    children = []
    ratios: Dict[str, float] = {}
    for row in boms:
        if str(row.get("ComponentSKU") or "") != sku:
            continue
        child = str(row.get("AssemblySKU") or "")
        if not child:
            continue
        ratios[child] = _qty(row.get("Quantity"))
        children.append(row)
    if not ratios:
        return 0.0, [], bom_path, sl_path

    total = 0.0
    audit_rows: List[Dict[str, Any]] = []
    for row in sale_lines:
        child = str(row.get("SKU") or "")
        if child not in ratios:
            continue
        if _status(row) in BAD_SALE_STATUSES:
            continue
        day = _parse_day(row.get("InvoiceDate"))
        if not _in_period(day, start, end):
            continue
        qty = _qty(row.get("Quantity"))
        contrib = qty * ratios[child]
        total += contrib
        out = dict(row)
        out["ParentSKU"] = child
        out["Quantity"] = contrib
        out["SourceLineQty"] = qty
        out["BOMRatio"] = ratios[child]
        audit_rows.append(out)
    return total, audit_rows, bom_path, sl_path


def local_assembly_consumption(
    sku: str,
    start: date,
    end: date,
) -> Tuple[float, List[Dict[str, Any]], Optional[Path]]:
    path = _latest([
        "assemblies_last_30d",
        "assemblies_last_45d",
        "assemblies_last_90d",
        "assemblies_last_365d",
        "assemblies",
    ])
    rows = []
    total = 0.0
    for row in _read_csv(path):
        if str(row.get("ComponentSKU") or "") != sku:
            continue
        if _status(row) in BAD_ASSEMBLY_STATUSES:
            continue
        day = _parse_day(row.get("CompletionDate")) or _parse_day(row.get("Date"))
        if not _in_period(day, start, end):
            continue
        qty = _qty(row.get("Quantity"))
        if qty <= 0:
            continue
        total += qty
        rows.append(row)
    return total, rows, path


def _cin7_client(rate: float) -> Cin7Client:
    load_dotenv()
    account_id = os.environ.get("CIN7_ACCOUNT_ID", "")
    app_key = os.environ.get("CIN7_APPLICATION_KEY", "")
    if not account_id or not app_key:
        raise RuntimeError(
            "CIN7_ACCOUNT_ID / CIN7_APPLICATION_KEY are not set here. "
            "Run this inside Render Shell for wired4signs-app."
        )
    return Cin7Client(account_id, app_key, rate_seconds=rate)


def live_sales(
    client: Cin7Client,
    sku: str,
    start: date,
    end: date,
    max_headers: int = 0,
) -> Tuple[float, List[Dict[str, Any]]]:
    since = (start - timedelta(days=7)).isoformat()
    print(f"\n[Live CIN7 sales] listing saleList UpdatedSince={since}")
    headers = list(client.paginate(
        "saleList", result_key="SaleList", params={"UpdatedSince": since}))
    if max_headers > 0:
        headers = headers[:max_headers]
        print(f"  Limited to first {max_headers} headers for debug.")
    print(f"  Fetching sale detail for {len(headers)} sale headers...")

    rows: List[Dict[str, Any]] = []
    total = 0.0
    for i, header in enumerate(headers, 1):
        sale_id = header.get("SaleID")
        if not sale_id:
            continue
        detail = client.get("sale", params={"ID": sale_id})
        for line in _extract_sale_lines(detail, header):
            if str(line.get("SKU") or "") != sku:
                continue
            if _status(line) in BAD_SALE_STATUSES:
                continue
            day = _parse_day(line.get("InvoiceDate"))
            if not _in_period(day, start, end):
                continue
            qty = _qty(line.get("Quantity"))
            total += qty
            rows.append(line)
        if i % 50 == 0:
            print(f"  processed {i}/{len(headers)} sale details")
    return total, rows


def live_assemblies(
    client: Cin7Client,
    sku: str,
    start: date,
    end: date,
    buffer_days: int,
    max_tasks: int = 0,
) -> Tuple[float, List[Dict[str, Any]]]:
    candidate_start = start - timedelta(days=buffer_days)
    print("\n[Live CIN7 FG assemblies] scanning finishedGoodsList")
    print(f"  Candidate list-date cutoff: {candidate_start.isoformat()}")
    headers: List[Dict[str, Any]] = []
    scanned = 0
    for task in client.paginate(
        "finishedGoodsList",
        result_key="FinishedGoods",
        params={"Status": "COMPLETED"},
    ):
        scanned += 1
        dates = [
            _parse_day(task.get("CompletionDate")),
            _parse_day(task.get("Date")),
            _parse_day(task.get("Updated")),
            _parse_day(task.get("LastUpdated")),
            _parse_day(task.get("Created")),
            _parse_day(task.get("CreatedDate")),
            _parse_day(task.get("ModifiedDate")),
        ]
        known_dates = [d for d in dates if d is not None]
        if not known_dates or any(d >= candidate_start for d in known_dates):
            headers.append(task)
        if scanned % 500 == 0:
            print(f"  scanned {scanned} list rows; candidates {len(headers)}")

    headers.reverse()
    if max_tasks > 0:
        headers = headers[:max_tasks]
        print(f"  Limited to first {max_tasks} candidate tasks for debug.")
    print(f"  Fetching detail for {len(headers)} candidate FG tasks...")

    total = 0.0
    rows: List[Dict[str, Any]] = []
    for i, task in enumerate(headers, 1):
        task_id = task.get("TaskID")
        if not task_id:
            continue
        detail = client.get("finishedGoods", params={"TaskID": task_id})
        status = str(detail.get("Status") or task.get("Status") or "")
        if status.strip().upper() in BAD_ASSEMBLY_STATUSES:
            continue
        completion = detail.get("CompletionDate") or task.get("Date")
        completion_day = _parse_day(completion)
        if not _in_period(completion_day, start, end):
            continue
        for line in detail.get("PickLines") or []:
            if not isinstance(line, dict):
                continue
            if str(line.get("ProductCode") or "") != sku:
                continue
            qty = _qty(line.get("Quantity"))
            if qty <= 0:
                continue
            row = {
                "TaskID": task_id,
                "AssemblyNumber": task.get("AssemblyNumber"),
                "CompletionDate": completion,
                "Status": status,
                "ParentSKU": task.get("ProductCode"),
                "ParentName": task.get("ProductName"),
                "ParentQuantity": task.get("Quantity"),
                "ComponentSKU": line.get("ProductCode"),
                "ComponentName": line.get("Name"),
                "Quantity": qty,
                "Cost": line.get("Cost"),
                "Bin": line.get("Bin"),
            }
            total += qty
            rows.append(row)
        if i % 50 == 0:
            print(f"  processed {i}/{len(headers)} FG task details")
    return total, rows


def _by_parent(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = defaultdict(float)
    for row in rows:
        parent = str(row.get("ParentSKU") or row.get("SKU") or "(unknown)")
        out[parent] += _qty(row.get("Quantity"))
    return dict(sorted(out.items(), key=lambda item: item[1], reverse=True))


def live_product_movements(
    client: Cin7Client,
    sku: str,
    start: date,
    end: date,
) -> Tuple[float, List[Dict[str, Any]], Dict[str, float]]:
    print("\n[Live CIN7 product Movements] GET /product IncludeMovements=true")
    resp = client.get("product", params={
        "Sku": sku,
        "IncludeDeprecated": "true",
        "IncludeMovements": "true",
        "Limit": 20,
    })
    products = resp.get("Products") or [] if isinstance(resp, dict) else []
    exact = [
        p for p in products
        if isinstance(p, dict) and str(p.get("SKU") or "") == str(sku)
    ]
    product = exact[0] if exact else (products[0] if products else None)
    if not isinstance(product, dict):
        return 0.0, [], {}
    rows: List[Dict[str, Any]] = []
    by_type: Dict[str, float] = defaultdict(float)
    demand_qty = 0.0
    for mv in product.get("Movements") or []:
        if not isinstance(mv, dict):
            continue
        day = _parse_day(mv.get("Date"))
        if not _in_period(day, start, end):
            continue
        typ = str(mv.get("Type") or "")
        qty = _qty(mv.get("Quantity"))
        by_type[typ] += qty
        if typ.upper() in {"SALE", "ASSEMBLY"} and qty < 0:
            demand_qty += -qty
        rows.append({
            "Date": day.isoformat() if day else "",
            "Type": typ,
            "Number": mv.get("Number"),
            "FromTo": mv.get("FromTo"),
            "Quantity": qty,
            "Amount": mv.get("Amount"),
            "Location": mv.get("Location"),
        })
    rows = sorted(rows, key=lambda r: (r["Date"], str(r["Number"])),
                  reverse=True)
    return demand_qty, rows, dict(by_type)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only CIN7 demand audit for one SKU.")
    parser.add_argument("sku")
    parser.add_argument("--from", dest="start", default=None)
    parser.add_argument("--to", dest="end", default=None)
    parser.add_argument("--live-assemblies", action="store_true",
                        help="Pull live /finishedGoods records from CIN7.")
    parser.add_argument("--live-product-movements", action="store_true",
                        help="Pull live /product IncludeMovements ledger.")
    parser.add_argument("--live-sales", action="store_true",
                        help="Pull live sale details from CIN7. Slower.")
    parser.add_argument("--rate", type=float, default=float(
        os.environ.get("CIN7_RATE_SECONDS", "2.5") or "2.5"))
    parser.add_argument("--assembly-buffer-days", type=int, default=int(
        os.environ.get("CIN7_ASSEMBLY_LIST_BUFFER_DAYS", "180") or "180"))
    parser.add_argument("--max-live-sales", type=int, default=0)
    parser.add_argument("--max-live-assemblies", type=int, default=0)
    args = parser.parse_args()

    today = date.today()
    start = _parse_day(args.start) if args.start else today.replace(day=1)
    end = _parse_day(args.end) if args.end else today
    if start is None or end is None:
        print("ERROR: invalid --from/--to date. Use YYYY-MM-DD.")
        return 2
    if start > end:
        print("ERROR: --from must be on or before --to.")
        return 2

    sku = str(args.sku).strip()
    print("=" * 78)
    print(f"CIN7 DEMAND AUDIT: {sku}")
    print(f"Period: {start.isoformat()} to {end.isoformat()} inclusive")
    print(f"Output dir: {OUTPUT_DIR}")
    print("=" * 78)

    direct_qty, direct_rows, direct_path = local_direct_sales(sku, start, end)
    bom_qty, bom_rows, bom_path, bom_sl_path = local_bom_rollup_estimate(
        sku, start, end)
    asm_qty, asm_rows, asm_path = local_assembly_consumption(sku, start, end)

    print("\n[Local synced data]")
    print(f"  Direct invoice sales      : {direct_qty:g}")
    print(f"    sale-line file          : {direct_path or '(missing)'}")
    print(f"  BOM kit-sale estimate     : {bom_qty:g}")
    print(f"    BOM file                : {bom_path or '(missing)'}")
    print(f"    sale-line file          : {bom_sl_path or '(missing)'}")
    print(f"  FG assembly consumption   : {asm_qty:g}")
    print(f"    assembly file           : {asm_path or '(missing)'}")
    print(f"  Engine-correct local MTD  : {direct_qty + asm_qty:g}")
    print("    (Use direct + FG assembly. Do not add BOM estimate if FG rows exist.)")

    print("\n  Local direct sale rows:")
    _summarise(direct_rows, "InvoiceDate")
    print("\n  Local FG assembly rows:")
    _summarise(asm_rows, "CompletionDate")
    if asm_rows:
        print("  Local FG by parent SKU:")
        for parent, qty in _by_parent(asm_rows).items():
            print(f"    {parent}: {qty:g}")
    print("\n  Local BOM kit-sale estimate rows:")
    _summarise(bom_rows, "InvoiceDate")

    live_direct_qty: Optional[float] = None
    live_asm_qty: Optional[float] = None
    live_product_movement_qty: Optional[float] = None
    live_direct_rows: List[Dict[str, Any]] = []
    live_asm_rows: List[Dict[str, Any]] = []
    if args.live_sales or args.live_assemblies or args.live_product_movements:
        try:
            client = _cin7_client(args.rate)
        except RuntimeError as exc:
            print(f"\nERROR: {exc}")
            return 3
        if args.live_product_movements:
            live_product_movement_qty, live_product_rows, by_type = (
                live_product_movements(client, sku, start, end))
            print("\n[Live CIN7 product movement demand] total: "
                  f"{live_product_movement_qty:g}")
            print("  By type signed qty:")
            for typ, qty in sorted(by_type.items()):
                print(f"    {typ}: {qty:g}")
            _summarise([
                {
                    "CompletionDate": r.get("Date"),
                    "AssemblyNumber": r.get("Number"),
                    "ComponentSKU": sku,
                    "ParentSKU": r.get("Type"),
                    "Customer": r.get("FromTo"),
                    "Quantity": r.get("Quantity"),
                    "Status": "CIN7 movement",
                }
                for r in live_product_rows
            ], "CompletionDate")
        if args.live_sales:
            live_direct_qty, live_direct_rows = live_sales(
                client, sku, start, end, max_headers=args.max_live_sales)
            print(f"\n[Live CIN7 direct sales] total: {live_direct_qty:g}")
            _summarise(live_direct_rows, "InvoiceDate")
        if args.live_assemblies:
            live_asm_qty, live_asm_rows = live_assemblies(
                client,
                sku,
                start,
                end,
                buffer_days=args.assembly_buffer_days,
                max_tasks=args.max_live_assemblies,
            )
            print(f"\n[Live CIN7 FG assembly consumption] total: {live_asm_qty:g}")
            _summarise(live_asm_rows, "CompletionDate")
            if live_asm_rows:
                print("  Live FG by parent SKU:")
                for parent, qty in _by_parent(live_asm_rows).items():
                    print(f"    {parent}: {qty:g}")

    headline_direct = (
        live_direct_qty if live_direct_qty is not None else direct_qty)
    headline_asm = live_asm_qty if live_asm_qty is not None else asm_qty
    print("\n[Conclusion]")
    if live_product_movement_qty is not None:
        print("  Correct MTD demand        : "
              f"{live_product_movement_qty:g} "
              "(from live CIN7 product Movements)")
    else:
        print(f"  Direct sales used         : {headline_direct:g}")
        print(f"  FG assembly used          : {headline_asm:g}")
        print(f"  Correct MTD demand        : {headline_direct + headline_asm:g}")
    print(f"  Kit-sale BOM estimate     : {bom_qty:g} (audit only when FG exists)")
    if live_asm_qty is not None and abs(live_asm_qty - asm_qty) > 0.0001:
        print("  FINDING: live CIN7 FG assembly total differs from synced CSV.")
        print("           The app/AI will be wrong until the assembly sync catches up.")
    if live_direct_qty is not None and abs(live_direct_qty - direct_qty) > 0.0001:
        print("  FINDING: live CIN7 direct-sale total differs from synced CSV.")
        print("           The app/AI will be wrong until sale-line sync catches up.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
