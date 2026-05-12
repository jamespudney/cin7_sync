"""po_dispatch_reminder.py (v2.67.130)
========================================

When a PO transitions to RECEIVED (fully or partially), scan its
line comments for SO-numbers — these are backorders the buyer
flagged at order-entry time, meaning customers are waiting on this
exact stock to ship. Once stock arrives, the fulfillment team
needs to drop everything and pick those orders first.

The reminder lands in a configured Slack channel (typically
#fulfillment) with the PO number, supplier, ETA, and the SO list
plus per-line SKU/quantity context. Dedup is enforced via the
po_dispatch_reminders table — each PO triggers exactly one
reminder regardless of how many times this script runs or how
many partial deliveries arrive.

CLI:
  python po_dispatch_reminder.py daily   # the main loop pass
  python po_dispatch_reminder.py dryrun  # scan + print, no Slack
  python po_dispatch_reminder.py one --po PO-7130   # debug one PO

Env vars
--------
  SLACK_BOT_TOKEN              standard bot token
  SLACK_FULFILLMENT_CHANNEL_ID channel where reminders post
  PO_REMINDER_LOOKBACK_DAYS    days back to consider (default 7;
                                  bootstrap on first run respects this)
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402

# data_paths is the project's central path resolver.
# v2.67.132 — cin7_sync writes CSVs into OUTPUT_DIR (= DATA_DIR /
# "output"), NOT into DATA_DIR directly. Original v2.67.130 search
# pattern looked in DATA_DIR and silently found nothing on Render.
try:
    from data_paths import OUTPUT_DIR
except ImportError:
    # Fallback for environments where data_paths isn't on the
    # Python path — shouldn't happen in production.
    OUTPUT_DIR = SCRIPT_DIR / "output"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
log = logging.getLogger("po_dispatch_reminder")

# SO-number regex. Mirrors slack_listener.SO_RE — accepts SO-12345
# or SO12345 (both formats seen in buyer comments). The hyphen is
# made optional so we catch both.
_SO_RE = re.compile(r"\bSO[-]?(\d{4,})\b", re.IGNORECASE)

# Comment fields on purchase_lines that buyers use. We scan all of
# them and merge SO numbers — buyers are inconsistent about which
# field they pick.
_COMMENT_FIELDS = ("Comments", "ShippingNotes", "Memo", "Note",
                      "Reference")

# v2.67.134 — PO HEADER comment fields. The buyer's actual practice
# (confirmed via James's screenshot of PO-7130) is to write SO refs
# in the order-level Comments box, NOT per-line. CIN7's CSV export
# names this column differently across versions/templates; we scan
# all plausible names.
_HEADER_COMMENT_FIELDS = (
    "Comments", "Comment", "Note", "Notes",
    "OrderMemo", "Memo", "OrderNotes", "OrderComments",
    "InternalNote", "Reference",
)

# Statuses we treat as "stock has arrived." CIN7 uses
# CombinedReceivingStatus to summarise across all lines.
_RECEIVED_STATUSES = ("FULLY RECEIVED", "PARTIALLY RECEIVED")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def _find_latest_csv(pattern: str) -> Optional[Path]:
    """Locate the most-recent CSV matching the glob. cin7_sync
    writes timestamped files; we always pick the freshest."""
    matches = glob.glob(str(OUTPUT_DIR /pattern))
    if not matches:
        return None
    return Path(max(matches, key=os.path.getmtime))


def _load_purchases_and_lines() -> Tuple[Optional[pd.DataFrame],
                                                Optional[pd.DataFrame]]:
    """Load the latest purchases + purchase_lines CSVs into
    DataFrames. Returns (None, None) if either file is missing —
    cin7_sync may not have run yet on a fresh worker."""
    p_path = _find_latest_csv("purchases_last_*d_*.csv")
    if not p_path:
        log.error("No purchases_last_*d_*.csv found in %s", OUTPUT_DIR)
        return None, None
    l_path = _find_latest_csv("purchase_lines_last_*d_*.csv")
    if not l_path:
        log.error("No purchase_lines_last_*d_*.csv found in %s",
                    OUTPUT_DIR)
        return None, None
    log.info("Loading purchases from %s", p_path)
    log.info("Loading purchase lines from %s", l_path)
    try:
        purchases = pd.read_csv(p_path)
        lines = pd.read_csv(l_path)
    except Exception as exc:
        log.error("Failed to read PO CSVs: %s", exc)
        return None, None
    return purchases, lines


# ---------------------------------------------------------------------------
# SO extraction
# ---------------------------------------------------------------------------
def _extract_sos_from_text(text: str) -> List[str]:
    """Return a list of distinct SO numbers found in `text`,
    normalised as 'SO-XXXXX' regardless of how the buyer wrote
    them. Empty list if none."""
    if not text or pd.isna(text):
        return []
    found: List[str] = []
    seen: Set[str] = set()
    for m in _SO_RE.finditer(str(text)):
        normalised = f"SO-{m.group(1)}"
        if normalised not in seen:
            seen.add(normalised)
            found.append(normalised)
    return found


def _extract_sos_from_po_header(po_row,
                                       purchases_columns) -> List[str]:
    """v2.67.134 — Scan the PO header's comment-like fields for
    SO references. CIN7's order-level Comments box is where the
    buyer actually puts backorder annotations (per the buyer
    confirmation on PO-7130: 'SO-56024; SO-56098' in Comments).

    `po_row` is a pandas Series for the PO; `purchases_columns`
    is the list of column names so we can check field existence
    once rather than per-row.
    """
    all_sos: List[str] = []
    seen: Set[str] = set()
    for f in _HEADER_COMMENT_FIELDS:
        if f not in purchases_columns:
            continue
        val = po_row.get(f)
        for so in _extract_sos_from_text(val):
            if so not in seen:
                seen.add(so)
                all_sos.append(so)
    return all_sos


def _extract_sos_from_lines(po_lines: pd.DataFrame
                                ) -> Tuple[List[str], List[dict]]:
    """Scan all comment-style fields across all lines of a PO.
    Returns (sorted unique SO list, per-line context for the
    reminder message).

    Per-line context shape:
      {sku, name, quantity, source_field, source_text, sos:[...]}
    Only includes lines where at least one SO was found."""
    all_sos: Set[str] = set()
    line_ctx: List[dict] = []
    for _, row in po_lines.iterrows():
        line_sos: List[str] = []
        source_field = None
        source_text = None
        for f in _COMMENT_FIELDS:
            if f not in po_lines.columns:
                continue
            val = row.get(f)
            sos = _extract_sos_from_text(val)
            if sos:
                line_sos.extend(sos)
                # Capture the FIRST field that yielded SOs as the
                # context for the message. Buyers usually use one
                # field per line, so this is usually clean.
                if source_field is None:
                    source_field = f
                    source_text = str(val)[:200]
        if line_sos:
            all_sos.update(line_sos)
            line_ctx.append({
                "sku": row.get("SKU"),
                "name": row.get("Name"),
                "quantity": row.get("Quantity"),
                "source_field": source_field,
                "source_text": source_text,
                "sos": sorted(set(line_sos)),
            })
    return sorted(all_sos), line_ctx


# ---------------------------------------------------------------------------
# Reminder composition + posting
# ---------------------------------------------------------------------------
def _compose_reminder(po_number: str,
                          supplier: Optional[str],
                          received_status: str,
                          received_date: Optional[str],
                          all_sos: List[str],
                          line_ctx: List[dict]) -> str:
    """Build the Slack message body. Mrkdwn formatting — bold for
    the PO number, bullets for each backorder line."""
    header_bits = [f"📦 *{po_number} received*"]
    if supplier:
        header_bits.append(f"({supplier}")
        if received_date:
            header_bits[-1] += f" · {received_date})"
        else:
            header_bits[-1] += ")"
    elif received_date:
        header_bits.append(f"({received_date})")
    header = " ".join(header_bits)
    if "PARTIAL" in (received_status or "").upper():
        header += "  _(partial delivery)_"

    lines: List[str] = [header, "", "*Backorders to dispatch:*"]
    for ctx in line_ctx:
        sku = ctx.get("sku") or "?"
        qty = ctx.get("quantity")
        sos_str = " · ".join(ctx["sos"])
        qty_str = (f" × {int(qty)}" if qty is not None
                     and not pd.isna(qty) else "")
        lines.append(f"• {sos_str} — `{sku}`{qty_str}")
    lines.append("")
    lines.append("_Please pick these orders first when this PO "
                  "arrives in the warehouse._")
    return "\n".join(lines)


def _post_to_slack(channel_id: str, text: str
                      ) -> Tuple[Optional[str], Optional[str]]:
    """Post via slack_sync's helper. Returns (posted_ts, error)."""
    try:
        import slack_sync
    except ImportError as exc:
        return None, f"slack_sync import failed: {exc}"
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not token:
        return None, "SLACK_BOT_TOKEN not set"
    try:
        session = slack_sync._build_session(token)
        body = slack_sync._slack_post(session, "chat.postMessage", {
            "channel": channel_id,
            "text": text,
            "unfurl_links": False,
            "unfurl_media": False,
        })
        if not body.get("ok"):
            return None, f"slack returned ok=false: {body}"
        return body.get("ts"), None
    except Exception as exc:
        return None, f"post error: {exc}"


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------
def _newly_received_pos(purchases: pd.DataFrame,
                              lookback_days: int) -> pd.DataFrame:
    """Filter the purchases frame to RECEIVED POs we haven't
    already reminded about. lookback_days bounds how far back we
    look — prevents the first run from notifying about years of
    historical POs."""
    if "CombinedReceivingStatus" not in purchases.columns:
        log.error("Purchases CSV missing CombinedReceivingStatus "
                    "column; columns are: %s",
                    list(purchases.columns)[:20])
        return pd.DataFrame()
    status_u = (purchases["CombinedReceivingStatus"]
                 .fillna("").astype(str).str.upper().str.strip())
    received_mask = status_u.isin(_RECEIVED_STATUSES)

    # Date filter — prefer LastUpdatedDate if it exists, fall back
    # to OrderDate.
    cutoff = (datetime.now(timezone.utc)
                - timedelta(days=lookback_days))
    date_col = None
    for cand in ("LastUpdatedDate", "ReceivedDate", "OrderDate"):
        if cand in purchases.columns:
            date_col = cand
            break
    if date_col:
        dates = pd.to_datetime(
            purchases[date_col], errors="coerce", utc=True)
        date_mask = dates >= pd.Timestamp(cutoff)
    else:
        date_mask = pd.Series(True, index=purchases.index)

    result = purchases[received_mask & date_mask].copy()
    log.info("Found %d RECEIVED POs in the last %d days "
              "(date column: %s)", len(result), lookback_days,
              date_col or "(none)")
    return result


def scan_and_notify(dryrun: bool = False,
                       lookback_days: int = 7) -> dict:
    """Top-level scan pass. Loads CSVs, finds eligible POs, posts
    reminders for those with SO references that haven't been
    notified yet. Returns a dict summarising what happened."""
    channel = os.environ.get(
        "SLACK_FULFILLMENT_CHANNEL_ID", "").strip()
    if not dryrun and not channel:
        log.warning(
            "SLACK_FULFILLMENT_CHANNEL_ID not set — no reminders "
            "will post. (Tip: get the channel ID from Slack: "
            "right-click channel → View channel details → bottom.)")
        return {"posted": 0, "skipped_no_channel": True}

    purchases, lines = _load_purchases_and_lines()
    if purchases is None or lines is None:
        return {"posted": 0, "error": "csv_load_failed"}

    eligible = _newly_received_pos(purchases, lookback_days)
    if eligible.empty:
        return {"posted": 0, "eligible": 0}

    n_posted = 0
    n_no_sos = 0
    n_already_notified = 0
    n_errors = 0

    # Pre-group lines by OrderNumber for cheap lookup.
    if "OrderNumber" not in lines.columns:
        log.error("Purchase lines missing OrderNumber column")
        return {"posted": 0, "error": "lines_schema_unexpected"}
    lines_by_po: Dict[str, pd.DataFrame] = {
        po: g for po, g in lines.groupby("OrderNumber")}

    for _, po in eligible.iterrows():
        po_number = str(po.get("OrderNumber") or "").strip()
        if not po_number:
            continue
        if db.has_notified_po_dispatch(po_number):
            n_already_notified += 1
            continue
        po_lines = lines_by_po.get(po_number)
        if po_lines is None or po_lines.empty:
            continue
        # v2.67.134 — Scan BOTH the PO header (where buyers
        # actually put SO refs per the PO-7130 example) AND the
        # individual line comments (older / per-line annotations).
        # Merge results so we catch both conventions.
        header_sos = _extract_sos_from_po_header(
            po, list(purchases.columns))
        line_sos_list, line_ctx = _extract_sos_from_lines(po_lines)
        all_sos = sorted(set(header_sos) | set(line_sos_list))

        # If the SOs came from the header, line_ctx is empty —
        # build one bullet per SO with a generic SKU placeholder
        # so the reminder message still has content.
        if header_sos and not line_ctx:
            line_ctx = [{
                "sku": "(see PO)",
                "name": None,
                "quantity": None,
                "source_field": "PO header",
                "source_text": None,
                "sos": header_sos,
            }]

        if not all_sos:
            n_no_sos += 1
            # v2.67.133 — diagnostic: in dryrun/verbose mode,
            # dump a sample of every comment-like field on this
            # PO's lines so we can SEE what's actually there.
            # Tells us whether the buyer didn't write SOs vs.
            # the SOs are in a field we're not scanning.
            if dryrun or log.isEnabledFor(logging.DEBUG):
                log.info("PO %s: no SOs found", po_number)
                # PO header fields (v2.67.134 — primary location).
                log.info("  -- PO header --")
                header_scanned = list(_HEADER_COMMENT_FIELDS)
                header_other = [
                    c for c in purchases.columns
                    if c not in header_scanned
                    and ("comment" in c.lower()
                          or "note" in c.lower()
                          or "memo" in c.lower())]
                for f in header_scanned + header_other:
                    if f not in purchases.columns:
                        continue
                    val = po.get(f)
                    if pd.notna(val) and str(val).strip():
                        log.info(
                            "    %s%s: %s",
                            f,
                            (" (unscanned)" if f in header_other
                              else ""),
                            str(val).strip()[:200])
                # Line-level fields.
                log.info("  -- line items --")
                sample_fields = [c for c in _COMMENT_FIELDS
                                    if c in po_lines.columns]
                other_text_cols = [
                    c for c in po_lines.columns
                    if c not in sample_fields
                    and ("comment" in c.lower()
                          or "note" in c.lower()
                          or "memo" in c.lower()
                          or "reference" in c.lower()
                          or "description" in c.lower())]
                for f in sample_fields + other_text_cols:
                    vals = (po_lines[f].dropna()
                            .astype(str).str.strip())
                    non_empty = vals[vals != ""].unique()
                    if len(non_empty):
                        sample = " | ".join(
                            str(v)[:120] for v in non_empty[:3])
                        log.info("    %s%s: %s",
                                   f,
                                   (" (unscanned)"
                                    if f in other_text_cols
                                    else ""),
                                   sample)
            continue
        supplier = po.get("Supplier")
        status = po.get("CombinedReceivingStatus")
        received_date = None
        for cand in ("LastUpdatedDate", "ReceivedDate",
                       "OrderDate"):
            if cand in po.index and pd.notna(po.get(cand)):
                received_date = str(po.get(cand))[:10]
                break

        msg = _compose_reminder(po_number, supplier, status,
                                    received_date, all_sos, line_ctx)
        log.info("PO %s: %d SOs (%s) %s",
                  po_number, len(all_sos),
                  ", ".join(all_sos[:5])
                  + ("…" if len(all_sos) > 5 else ""),
                  "[DRYRUN]" if dryrun else "")

        if dryrun:
            print(f"\n--- PO {po_number} ---\n{msg}\n")
            continue

        posted_ts, error = _post_to_slack(channel, msg)
        if error:
            log.error("Failed to post for %s: %s", po_number, error)
            db.record_po_dispatch_reminder(
                po_number=po_number,
                supplier=supplier,
                received_status=status,
                so_numbers=all_sos,
                posted_channel=channel,
                posted_ts=None,
                error_msg=error,
            )
            n_errors += 1
            continue
        db.record_po_dispatch_reminder(
            po_number=po_number,
            supplier=supplier,
            received_status=status,
            so_numbers=all_sos,
            posted_channel=channel,
            posted_ts=posted_ts,
            error_msg=None,
        )
        n_posted += 1

    return {
        "eligible": len(eligible),
        "posted": n_posted,
        "skipped_no_sos": n_no_sos,
        "skipped_already_notified": n_already_notified,
        "errors": n_errors,
    }


# ---------------------------------------------------------------------------
# Escalation: 24h after first reminder, check ShipStation (v2.67.131)
# ---------------------------------------------------------------------------
def _normalise_order_id(s) -> str:
    """Strip 'SO-', 'SO', 'INV-', 'INV' prefixes and leave the
    numeric core. ShipStation stores INV-XXXXX but our buyer
    writes SO-XXXXX — CIN7 reuses the numeric ID between them.
    Mirrors the prefix-stripping in ai_tools.get_shipping_details."""
    if s is None or pd.isna(s):
        return ""
    raw = str(s).strip().upper()
    for prefix in ("SO-", "INV-", "SO", "INV"):
        if raw.startswith(prefix):
            return raw[len(prefix):]
    return raw


def _load_latest_shipments() -> Optional[pd.DataFrame]:
    """Load the freshest shipments CSV. cin7_sync writes a few
    rolling-window files; we want the most recent so NearSync's
    15-min cadence keeps us current. Falls back to the full
    historical dump if no rolling file exists."""
    candidates = []
    for pat in (
        "shipments_last_*d_*.csv",  # rolling-window NearSync / Daily
        "shipments_full.csv",        # historical dump (5-year)
    ):
        for path in glob.glob(str(OUTPUT_DIR /pat)):
            candidates.append(Path(path))
    if not candidates:
        log.warning(
            "No shipments CSVs found in %s — can't verify dispatch",
            OUTPUT_DIR)
        return None
    latest = max(candidates, key=os.path.getmtime)
    log.info("Loading shipments from %s", latest)
    try:
        return pd.read_csv(latest)
    except Exception as exc:
        log.error("Failed to read shipments CSV %s: %s", latest, exc)
        return None


def _build_shipped_index(
        shipments: pd.DataFrame) -> Set[str]:
    """Return a set of normalised order IDs that have a non-empty
    ShipDate and aren't voided. Used to answer 'has this SO
    shipped?' with a cheap O(1) lookup."""
    if shipments is None or shipments.empty:
        return set()
    df = shipments
    if "ShipDate" not in df.columns or "OrderNumber" not in df.columns:
        log.warning(
            "Shipments CSV missing ShipDate or OrderNumber columns "
            "(have: %s) — can't index", list(df.columns)[:15])
        return set()
    has_shipdate = df["ShipDate"].notna() & (
        df["ShipDate"].astype(str).str.strip() != "")
    not_voided = (~df["Voided"].fillna(False).astype(bool)
                    if "Voided" in df.columns
                    else pd.Series(True, index=df.index))
    eligible = df[has_shipdate & not_voided]
    return {_normalise_order_id(o)
              for o in eligible["OrderNumber"].dropna()}


def _is_so_shipped(so_number: str,
                       shipped_index: Set[str]) -> bool:
    """O(1) lookup — has the SO number's numeric core appeared as
    a shipped order in ShipStation?"""
    if not so_number:
        return False
    norm = _normalise_order_id(so_number)
    if not norm:
        return False
    return norm in shipped_index


def _compose_escalation(po_number: str,
                            supplier: Optional[str],
                            unshipped_lines: List[dict],
                            posted_age_hours: float) -> str:
    """Build the escalation Slack message. Tone is more urgent
    than the initial reminder — these orders should already have
    shipped by now."""
    header = (f"⚠️ *{po_number} arrived ~{int(posted_age_hours)}h "
                f"ago — these orders STILL haven't shipped:*")
    if supplier:
        header += f"  _({supplier})_"
    lines: List[str] = [header, ""]
    for ctx in unshipped_lines:
        sku = ctx.get("sku") or "?"
        qty = ctx.get("quantity")
        sos_str = " · ".join(ctx["sos"])
        qty_str = (f" × {int(qty)}" if qty is not None
                     and not pd.isna(qty) else "")
        lines.append(f"• {sos_str} — `{sku}`{qty_str}")
    lines.append("")
    lines.append("_Please prioritise picking these. Customers "
                  "are waiting._")
    return "\n".join(lines)


def check_and_escalate(dryrun: bool = False,
                            min_age_hours: int = 24,
                            max_age_hours: int = 168
                            ) -> dict:
    """For each reminder posted >= min_age_hours ago that hasn't
    been escalated yet: check ShipStation; if any SO is still
    unshipped, post an escalation message and stamp escalated_at."""
    channel = os.environ.get(
        "SLACK_FULFILLMENT_CHANNEL_ID", "").strip()
    if not dryrun and not channel:
        return {"escalated": 0, "skipped_no_channel": True}

    pending = db.list_po_reminders_pending_escalation(
        min_age_hours=min_age_hours,
        max_age_hours=max_age_hours)
    if not pending:
        return {"escalated": 0, "pending": 0}

    shipments = _load_latest_shipments()
    shipped_idx = _build_shipped_index(shipments) if shipments is not None else set()
    if not shipped_idx:
        log.warning(
            "Shipped index is empty — every SO will look "
            "unshipped. Refusing to spam escalations; check that "
            "shipstation_sync is running.")
        return {"escalated": 0, "no_shipped_index": True}

    # We also need the per-line context (SKU, qty) — pull it on
    # demand from the latest purchase_lines CSV.
    _, lines = _load_purchases_and_lines()
    lines_by_po: Dict[str, pd.DataFrame] = {}
    if lines is not None and "OrderNumber" in lines.columns:
        lines_by_po = {po: g for po, g in lines.groupby("OrderNumber")}

    n_escalated = 0
    n_all_shipped = 0
    n_errors = 0

    for rem in pending:
        po_number = rem["po_number"]
        sos_csv = rem.get("so_numbers") or ""
        sos = [s.strip() for s in sos_csv.split(",") if s.strip()]
        if not sos:
            # Shouldn't happen — we only insert reminders with
            # at least one SO — but defensive.
            continue
        unshipped_sos = [s for s in sos
                            if not _is_so_shipped(s, shipped_idx)]
        if not unshipped_sos:
            log.info("PO %s: all %d SOs shipped — skipping "
                      "escalation", po_number, len(sos))
            db.record_po_dispatch_escalation(
                po_number=po_number,
                posted_ts=None,
                reason=(f"all {len(sos)} SOs shipped per "
                          f"ShipStation; no escalation needed"),
            )
            n_all_shipped += 1
            continue

        # Build per-line context for the unshipped SOs only.
        po_lines = lines_by_po.get(po_number)
        unshipped_ctx: List[dict] = []
        if po_lines is not None and not po_lines.empty:
            for _, row in po_lines.iterrows():
                line_sos: List[str] = []
                for f in _COMMENT_FIELDS:
                    if f in po_lines.columns:
                        line_sos.extend(
                            _extract_sos_from_text(row.get(f)))
                line_unshipped = [s for s in line_sos
                                      if s in unshipped_sos]
                if line_unshipped:
                    unshipped_ctx.append({
                        "sku": row.get("SKU"),
                        "name": row.get("Name"),
                        "quantity": row.get("Quantity"),
                        "sos": sorted(set(line_unshipped)),
                    })
        if not unshipped_ctx:
            # Couldn't reconstruct per-line context (probably the
            # PO is now outside the purchase_lines window). Fall
            # back to a single bullet per SO.
            unshipped_ctx = [{
                "sku": "—",
                "quantity": None,
                "sos": [s],
            } for s in unshipped_sos]

        # Compute age for the message.
        try:
            posted_dt = pd.to_datetime(rem["posted_at"])
            age_hours = (pd.Timestamp.now()
                            - posted_dt).total_seconds() / 3600.0
        except Exception:
            age_hours = float(min_age_hours)

        msg = _compose_escalation(
            po_number, rem.get("supplier"),
            unshipped_ctx, age_hours)
        log.info("PO %s: %d unshipped SOs (%s) — escalating %s",
                  po_number, len(unshipped_sos),
                  ", ".join(unshipped_sos[:5]),
                  "[DRYRUN]" if dryrun else "")

        if dryrun:
            print(f"\n--- ESCALATION for {po_number} ---\n"
                    f"{msg}\n")
            continue

        posted_ts, error = _post_to_slack(channel, msg)
        if error:
            log.error(
                "Failed to post escalation for %s: %s",
                po_number, error)
            db.record_po_dispatch_escalation(
                po_number=po_number,
                posted_ts=None,
                reason=f"{len(unshipped_sos)} unshipped SOs",
                error_msg=error,
            )
            n_errors += 1
            continue
        db.record_po_dispatch_escalation(
            po_number=po_number,
            posted_ts=posted_ts,
            reason=f"{len(unshipped_sos)} unshipped SOs after "
                     f"{int(age_hours)}h",
        )
        n_escalated += 1

    return {
        "pending": len(pending),
        "escalated": n_escalated,
        "all_shipped_no_escalation_needed": n_all_shipped,
        "errors": n_errors,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _setup_log(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=LOG_FORMAT,
        stream=sys.stdout,
        force=True,
    )


def cmd_daily(args: argparse.Namespace) -> int:
    """Run BOTH passes in order: initial reminders for newly-
    received POs, then escalations for 24h-stale reminders whose
    SOs haven't shipped per ShipStation."""
    _setup_log(args.verbose)
    days = int(os.environ.get(
        "PO_REMINDER_LOOKBACK_DAYS", "7") or 7)
    min_age = int(os.environ.get(
        "PO_REMINDER_ESCALATION_MIN_HOURS", "24") or 24)
    initial = scan_and_notify(
        dryrun=False, lookback_days=days)
    log.info("INITIAL pass: %s", initial)
    esc = check_and_escalate(
        dryrun=False, min_age_hours=min_age)
    log.info("ESCALATION pass: %s", esc)
    return 0


def cmd_dryrun(args: argparse.Namespace) -> int:
    """Show what would happen on both passes without posting."""
    _setup_log(args.verbose)
    days = int(args.days or 7)
    min_age = int(args.min_age or 24)
    initial = scan_and_notify(dryrun=True, lookback_days=days)
    log.info("INITIAL pass [DRYRUN]: %s", initial)
    esc = check_and_escalate(
        dryrun=True, min_age_hours=min_age)
    log.info("ESCALATION pass [DRYRUN]: %s", esc)
    return 0


def cmd_escalate(args: argparse.Namespace) -> int:
    """Run ONLY the escalation pass (debug helper)."""
    _setup_log(args.verbose)
    min_age = int(args.min_age or 24)
    result = check_and_escalate(
        dryrun=bool(args.dryrun), min_age_hours=min_age)
    log.info("ESCALATION pass: %s", result)
    return 0


def cmd_one(args: argparse.Namespace) -> int:
    """Debug a single PO — show what we'd post without posting."""
    _setup_log(args.verbose)
    purchases, lines = _load_purchases_and_lines()
    if purchases is None or lines is None:
        return 1
    if "OrderNumber" not in purchases.columns:
        log.error("purchases CSV missing OrderNumber")
        return 1
    match = purchases[purchases["OrderNumber"].astype(str)
                          == args.po]
    if match.empty:
        log.error("PO %s not found in purchases CSV", args.po)
        return 1
    po = match.iloc[0]
    lines_for = lines[lines["OrderNumber"].astype(str) == args.po]
    log.info("PO %s: status=%s · %d lines",
              args.po, po.get("CombinedReceivingStatus"),
              len(lines_for))
    # v2.67.134 — scan header + lines (header is the real spot)
    header_sos = _extract_sos_from_po_header(
        po, list(purchases.columns))
    line_sos_list, line_ctx = _extract_sos_from_lines(lines_for)
    all_sos = sorted(set(header_sos) | set(line_sos_list))
    if header_sos:
        log.info("  PO-header SOs: %s", header_sos)
    if line_sos_list:
        log.info("  Line-level SOs: %s", line_sos_list)
    if not all_sos:
        log.info("No SO numbers found. Header fields scanned: %s. "
                  "Line fields scanned: %s",
                  _HEADER_COMMENT_FIELDS, _COMMENT_FIELDS)
        return 0
    if header_sos and not line_ctx:
        line_ctx = [{
            "sku": "(see PO)",
            "quantity": None,
            "sos": header_sos,
        }]
    msg = _compose_reminder(
        args.po,
        po.get("Supplier"),
        po.get("CombinedReceivingStatus") or "",
        None,
        all_sos,
        line_ctx,
    )
    print(msg)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Notify #fulfillment when a PO is received "
                      "with SO-backorder references in comments.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_d = sub.add_parser(
        "daily", help="Both passes: initial reminders + 24h "
                       "escalations (the slack_loop entry point).")
    p_d.add_argument("--verbose", action="store_true")
    p_d.set_defaults(func=cmd_daily)

    p_dr = sub.add_parser(
        "dryrun", help="Both passes, print only — no Slack post.")
    p_dr.add_argument("--days", type=int, default=7)
    p_dr.add_argument("--min-age", type=int, default=24,
                          help="Hours since initial reminder before "
                                "escalating (default 24).")
    p_dr.add_argument("--verbose", action="store_true")
    p_dr.set_defaults(func=cmd_dryrun)

    p_e = sub.add_parser(
        "escalate", help="ONLY run the escalation pass (debug).")
    p_e.add_argument("--min-age", type=int, default=24)
    p_e.add_argument("--dryrun", action="store_true")
    p_e.add_argument("--verbose", action="store_true")
    p_e.set_defaults(func=cmd_escalate)

    p_o = sub.add_parser(
        "one", help="Inspect one specific PO.")
    p_o.add_argument("--po", required=True,
                        help="PO number, e.g. PO-7130")
    p_o.add_argument("--verbose", action="store_true")
    p_o.set_defaults(func=cmd_one)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
