"""fablab_stock_alert.py

Daily scan: which 865FabLab build-list SKUs have dropped below their
reorder level (same "Suggested batch" math the in-app planner uses),
and posts an alert to the 865FabLab Slack channel -- once per drop, not
once per day. James, 2026-09-01: "it can notify when items drop below
the level."

Reuses build_planner_table() from app_pages/fablab_work_orders.py
(pure function, no Streamlit dependency) so the alert threshold always
matches what the in-app planner shows -- no separate math to drift out
of sync.

Dedup: db.fablab_stock_alert_active(sku) / record_fablab_stock_alert()
/ clear_fablab_stock_alert() (see db.py, 2026-09-01) track one active
alert per SKU. A SKU stays silent while still below the reorder level
(no daily spam); once its suggested batch clears to <= 0 (restocked or
a batch was placed) the alert clears, so a future re-drop notifies
again.

Data loading follows slack_listener.py's _get_data_for_listener()
pattern -- prefers the dashboard's canonical engine_output.csv,
falls back to worker_engine.compute_engine_signals().

CLI:
  python fablab_stock_alert.py run
"""

from __future__ import annotations

import argparse
import glob
import logging
import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import db  # noqa: E402
import fablab_slack  # noqa: E402
from data_paths import OUTPUT_DIR  # noqa: E402
from app_pages.fablab_work_orders import (  # noqa: E402
    build_planner_table, FABLAB_FLAG_TYPE)

log = logging.getLogger("fablab_stock_alert")

DEFAULT_WEEKS_COVER = 6.0


def _build_bom_parents(boms_df: pd.DataFrame) -> dict:
    """Minimal re-implementation of app.py's _build_bom_indexes()
    parents_of half -- the only part build_planner_table() needs."""
    parents_of: dict = {}
    if boms_df is None or boms_df.empty:
        return parents_of
    for _, row in boms_df.iterrows():
        asm = row.get("AssemblySKU")
        comp = row.get("ComponentSKU")
        if not asm or not comp or str(asm) == str(comp):
            continue
        parents_of.setdefault(asm, []).append({
            "ComponentSKU": comp,
            "ComponentName": row.get("ComponentName"),
            "Quantity": row.get("Quantity"),
            "BOMType": row.get("BOMType"),
        })
    return parents_of


def _load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """Returns (products, stock, engine_df, bom_parents)."""
    prod_files = sorted(glob.glob(str(OUTPUT_DIR / "products_*.csv")))
    stk_files = sorted(glob.glob(str(OUTPUT_DIR / "stock_on_hand_*.csv")))
    boms_files = sorted(glob.glob(str(OUTPUT_DIR / "boms_*.csv")))
    if not prod_files or not stk_files:
        raise SystemExit("No products/stock CSV available on this worker.")

    products = pd.read_csv(prod_files[-1], low_memory=False)
    stock = pd.read_csv(stk_files[-1], low_memory=False)
    boms = (pd.read_csv(boms_files[-1], low_memory=False)
             if boms_files else pd.DataFrame())
    bom_parents = _build_bom_parents(boms)

    engine_files = sorted(glob.glob(str(OUTPUT_DIR / "engine_output.csv")))
    engine_df = pd.DataFrame()
    if engine_files:
        try:
            engine_df = pd.read_csv(engine_files[-1], low_memory=False)
            log.info("Loaded canonical engine from %s",
                      Path(engine_files[-1]).name)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not load engine_output.csv: %s", exc)

    if engine_df.empty:
        import worker_engine
        sl_files = glob.glob(
            str(OUTPUT_DIR / "sale_lines_last_*d_*.csv"))
        sale_lines = (
            pd.read_csv(sorted(sl_files)[-1], low_memory=False)
            if sl_files else pd.DataFrame())
        engine_df = worker_engine.compute_engine_signals(
            products, stock, sale_lines, boms=boms)
        log.info("Computed engine_df via worker_engine "
                  "(%d row(s))", len(engine_df))

    return products, stock, engine_df, bom_parents


def _format_alert(sku: str, name: str, suggested: float,
                    materials_status: str, rule: dict | None) -> str:
    lines = [
        f":rotating_light: *{sku}* dropped below reorder level -- "
        f"suggested batch *{suggested:g}* ({materials_status})",
        name,
    ]
    if rule:
        lines.append(f"\n*{rule['RuleCode']}: {rule['Name']}*")
        lines.extend(f"{i}. {step}"
                      for i, step in enumerate(rule["Instructions"], 1))
    return "\n".join(lines)


def run(apply: bool = True) -> dict:
    from engine.sku_rules import parse_corner_bom_rule

    products, stock, engine_df, bom_parents = _load_data()
    product_map = (
        products.drop_duplicates("SKU", keep="last")
                 .set_index("SKU").to_dict(orient="index")
        if not products.empty else {})

    flag_rows = [f for f in db.list_flags(active_only=True)
                  if f["flag_type"] == FABLAB_FLAG_TYPE]
    flagged_skus = sorted({r["sku"] for r in flag_rows})
    if not flagged_skus:
        log.info("No 865FabLab build-list SKUs flagged; nothing to check.")
        return {"alerted": [], "cleared": []}

    planner_df = build_planner_table(
        flagged_skus, products, stock, engine_df, bom_parents,
        weeks_cover=DEFAULT_WEEKS_COVER)
    if planner_df.empty:
        log.info("Planner table came back empty; nothing to check.")
        return {"alerted": [], "cleared": []}

    alerted, cleared = [], []
    for _, row in planner_df.iterrows():
        sku = row["SKU"]
        suggested = float(row.get("Suggested batch") or 0)
        already_active = db.fablab_stock_alert_active(sku)
        if suggested > 0 and not already_active:
            prod = product_map.get(sku, {})
            rule = parse_corner_bom_rule(
                prod.get("AdditionalAttribute1"),
                prod.get("AdditionalAttribute2"))
            text = _format_alert(
                sku, row.get("Name") or "", suggested,
                row.get("Materials status") or "", rule)
            posted_ts = None
            error_msg = None
            if apply:
                posted_ts, error_msg = fablab_slack.post(text)
            log.info("Alerting %s (suggested batch %.1f)%s",
                      sku, suggested,
                      f" -- POST FAILED: {error_msg}" if error_msg else "")
            if apply:
                db.record_fablab_stock_alert(
                    sku, suggested, fablab_slack.FABLAB_CHANNEL_ID,
                    posted_ts, error_msg=error_msg)
            alerted.append(sku)
        elif suggested <= 0 and already_active:
            if apply:
                db.clear_fablab_stock_alert(sku)
            log.info("Clearing alert for %s (suggested batch now 0)", sku)
            cleared.append(sku)

    return {"alerted": alerted, "cleared": cleared}


def _setup_log(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout, force=True)


def cmd_run(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    result = run(apply=not args.dry_run)
    log.info("Done. Alerted %d, cleared %d.",
              len(result["alerted"]), len(result["cleared"]))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="Scan and post stock-drop alerts")
    p_run.add_argument("--dry-run", action="store_true",
                        help="Log what would be posted without posting")
    p_run.set_defaults(func=cmd_run)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
