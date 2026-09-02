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
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import db  # noqa: E402
import fablab_slack  # noqa: E402
from data_paths import OUTPUT_DIR  # noqa: E402
from app_pages.fablab_work_orders import (  # noqa: E402
    build_planner_table, FABLAB_FLAG_TYPE, FABLAB_SUPPLIER)

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


# ---------------------------------------------------------------------------
# Slack reply -> real CIN7 Draft PO (2026-09-02)
#
# James: "we want things as automated as possible" -- a buyer replying
# "approve" in a stock-drop alert's thread creates a draft order for
# that SKU and pushes it to CIN7 as a real Draft Purchase (same
# cin7_post_po.push_po_draft() flow the app's "Push to CIN7" button
# uses -- stops at CIN7 status DRAFT, a human still authorises in CIN7
# before 865FabLab sees anything).
#
# Deliberately requires an EXPLICIT affirmative keyword -- unlike
# stock_issues_handler.py's "any reply = acknowledge" fallback, this
# triggers a real financial write, so an unrelated thread reply must
# never be mistaken for approval.
#
# Mirrors stock_issues_handler.py's check_open_issues_for_replies()
# almost exactly: same conversations.replies polling pattern, same
# bot/empty-text skip, same _resolve_user() call for the display name.
# ---------------------------------------------------------------------------

_APPROVAL_KEYWORDS = (
    "approve", "approved", "order it", "place order", "go ahead", "do it",
)


def _approve_alert(alert_row, reply_user: str) -> tuple[bool, dict]:
    """Create a draft order for this alert's SKU and push it to CIN7 as
    a real Draft PO. Returns (ok, info) where info has cin7_po_id/
    cin7_po_number on success or an 'error' key on failure."""
    import db
    from cin7_post_po import push_po_draft

    sku = alert_row["sku"]
    qty = float(alert_row["suggested_batch"] or 0)
    if qty <= 0:
        return False, {"error": "suggested_batch is 0 -- nothing to order"}

    actor = f"slack:{reply_user or 'unknown'}"
    draft_id = db.create_po_draft(
        supplier=FABLAB_SUPPLIER,
        name=f"Slack approval {date.today().isoformat()}", actor=actor,
        note=f"Auto-created from Slack approval reply on {sku}")
    db.upsert_po_draft_line(draft_id, sku, qty, actor)

    result = push_po_draft(draft_id, actor=actor, apply=True,
                            require_mov=False)
    if result.ok:
        return True, {"cin7_po_id": result.cin7_po_id,
                       "cin7_po_number": result.cin7_po_number}
    return False, {"error": "; ".join(result.errors) or "push failed"}


def check_replies(apply: bool = True) -> dict:
    import db

    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not token:
        log.info("SLACK_BOT_TOKEN not set; skipping reply check.")
        return {"checked": 0, "approved": 0}

    alerts = db.list_active_fablab_stock_alerts_for_approval()
    if not alerts:
        return {"checked": 0, "approved": 0}

    import slack_sync
    session = slack_sync._build_session(token)

    n_checked = 0
    n_approved = 0
    for alert in alerts:
        ch = alert["posted_channel"]
        ts = alert["posted_ts"]
        sku = alert["sku"]
        if not ch or not ts:
            continue
        n_checked += 1
        try:
            body = slack_sync._slack_get(
                session, "conversations.replies",
                {"channel": ch, "ts": ts, "limit": 50})
        except Exception as exc:  # noqa: BLE001
            log.warning("conversations.replies %s/%s failed: %s",
                        ch, ts, exc)
            continue
        msgs = body.get("messages") or []
        if len(msgs) < 2:
            continue  # parent only -- no replies yet

        for m in msgs[1:]:
            text = m.get("text") or ""
            is_bot = bool(m.get("bot_id")
                          or m.get("subtype") == "bot_message")
            if is_bot or not text.strip():
                continue
            if not any(k in text.lower() for k in _APPROVAL_KEYWORDS):
                continue

            user_id = m.get("user") or ""
            user_name = ""
            if user_id:
                try:
                    user_name = slack_sync._resolve_user(session, user_id)
                except Exception:  # noqa: BLE001
                    user_name = user_id
            log.info("Approval reply matched for %s from %s: %r",
                      sku, user_name or user_id, text[:120])

            if not apply:
                log.info("[DRY] would approve %s", sku)
                n_approved += 1
                break

            ok, info = _approve_alert(alert, user_name or user_id)
            if ok:
                reply_text = (
                    f":white_check_mark: Approved by "
                    f"{user_name or user_id} -- CIN7 PO "
                    f"*#{info['cin7_po_number']}* created (DRAFT, "
                    f"needs authorisation in CIN7).")
                db.record_fablab_stock_alert_approval(
                    sku, user_name or user_id,
                    cin7_po_id=info.get("cin7_po_id"),
                    cin7_po_number=info.get("cin7_po_number"))
                n_approved += 1
            else:
                reply_text = (
                    f":x: Approval for *{sku}* failed: "
                    f"{info.get('error')}")
                db.record_fablab_stock_alert_approval(
                    sku, user_name or user_id, error_msg=info.get("error"))
            fablab_slack.post(reply_text, channel_id=ch, thread_ts=ts)
            break  # one approval per alert is enough

    return {"checked": n_checked, "approved": n_approved}


def cmd_check_replies(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    result = check_replies(apply=not args.dry_run)
    log.info("Done. Checked %d alert(s), approved %d.",
              result["checked"], result["approved"])
    return 0


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

    p_replies = sub.add_parser(
        "check-replies",
        help="Poll active alerts for an approval reply and push CIN7 PO")
    p_replies.add_argument("--dry-run", action="store_true",
                            help="Log matches without pushing to CIN7")
    p_replies.set_defaults(func=cmd_check_replies)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
