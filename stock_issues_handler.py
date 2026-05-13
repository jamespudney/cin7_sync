"""stock_issues_handler.py (v2.67.144)
=========================================

Stock-issues tracker for #stock-issues-queries.

Design philosophy (per James):
  - Bot is a CONTEXT PROVIDER, not an answerer.
  - When a stock query lands, the bot replies with structured
    intelligence (SKU + bin + qty + allocations + ETA) for the
    stock controller to verify/correct — NOT a dispatch decision.
  - Querier gets a brief acknowledgment with caveats (e.g. "PO ETA
    looks unconfirmed — verify with @AndrewTunley").
  - If no thread reply within 4h, bot DMs Jamie Webb with the
    intelligence so accountability sticks to a specific person.
  - Morning summary lists outstanding issues so the team sees the
    pile-up.

Issue classification:
  - supply_query  — pre-dispatch supply question ('can we supply
                    SO-NNNNN?', 'how many can we ship?', 'what
                    are we short?'). Resolution = SO ships.
  - count_wrong   — discrepancy claim ('should be N, found M').
                    Resolution = stock_adjustments entry. (v1
                    skips auto-resolution; relies on the stock
                    controller replying 'fixed'.)
  - mixed         — both signals in same message.

CLI:
  python stock_issues_handler.py escalate
  python stock_issues_handler.py morning-summary [--dryrun]
  python stock_issues_handler.py inspect --issue-id N

Env vars:
  SLACK_BOT_TOKEN
  SLACK_STOCK_ISSUES_CHANNEL_ID     where queries land (C08NEMCEHNF)
  SLACK_STOCKKEEPER_DM_CHANNEL_ID   D-channel for DM escalation
  SLACK_BUYER_DM_CHANNEL_ID         D-channel for buyer DM (PO ETA)
  STOCK_ISSUE_ESCALATION_HOURS      default 4
  STOCK_ISSUE_MORNING_HOUR_ET       default 8 (i.e. 8:30 — minutes
                                     fixed at 30)
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402

try:
    from data_paths import OUTPUT_DIR
except ImportError:
    OUTPUT_DIR = SCRIPT_DIR / "output"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
log = logging.getLogger("stock_issues_handler")


# ---------------------------------------------------------------------------
# Classification — when does a message in #stock-issues-queries
# count as a stock issue worth tracking?
# ---------------------------------------------------------------------------
_SO_RE = re.compile(r"\bSO[-]?(\d{4,})\b", re.IGNORECASE)
_INV_RE = re.compile(r"\bINV[-]?(\d{4,})\b", re.IGNORECASE)
_SKU_RE = re.compile(r"\b(LED(?:KIT)?-[A-Z0-9-]+)\b", re.IGNORECASE)

# Supply-query keywords: phrases asking 'can we ship?'
_SUPPLY_KEYWORDS = (
    "can we supply", "can we ship", "can we fulfil", "can we fulfill",
    "do we have", "are we short", "what are we short", "what we short",
    "how many can we", "how much can we", "please ship",
    "ship the", "supply the", "have stock", "in stock",
)

# Count-wrong keywords: discrepancy claims about CIN7 numbers.
_COUNT_WRONG_KEYWORDS = (
    "should be", "actual count", "actual qty", "actual quantity",
    "missing", "found extra", "found only", "i counted",
    "we counted", "stocktake", "stock take", "wrong qty",
    "wrong quantity", "wrong stock", "stock is wrong",
    "stock count", "system shows", "but system",
    "incorrect", "out by", "wrong by",
)


def classify_message(text: str) -> Optional[dict]:
    """Return a classification dict if `text` reads as a stock
    issue, None otherwise. Includes the detected pattern type +
    extracted entities (SO numbers, INV numbers, SKUs)."""
    if not text:
        return None
    lower = text.lower()
    sos = [f"SO-{m.group(1)}" for m in _SO_RE.finditer(text)]
    invs = [f"INV-{m.group(1)}" for m in _INV_RE.finditer(text)]
    skus = [m.group(1).upper() for m in _SKU_RE.finditer(text)]
    is_supply = any(k in lower for k in _SUPPLY_KEYWORDS)
    is_count = any(k in lower for k in _COUNT_WRONG_KEYWORDS)
    # Require AT LEAST one of:
    #   - SO number + supply keyword (Richard's pattern)
    #   - SKU + count-wrong keyword
    if sos and is_supply:
        issue_type = "supply_query"
    elif skus and is_count:
        issue_type = "count_wrong"
    elif sos and is_count:
        issue_type = "count_wrong"
    elif skus and is_supply:
        issue_type = "supply_query"
    else:
        return None
    return {
        "issue_type": issue_type,
        "so_numbers": sos,
        "inv_numbers": invs,
        "skus": skus,
    }


# ---------------------------------------------------------------------------
# Intelligence-block building — pulls live data for each item
# ---------------------------------------------------------------------------
def _load_sale_lines() -> Optional[pd.DataFrame]:
    """Cache-once helper; per-issue lookups against the same DF."""
    import glob
    matches = []
    for pat in ("sale_lines_last_1d_*.csv",
                 "sale_lines_last_7d_*.csv",
                 "sale_lines_last_*d_*.csv"):
        matches.extend(glob.glob(str(OUTPUT_DIR / pat)))
    if not matches:
        return None
    path = max(matches, key=os.path.getmtime)
    try:
        return pd.read_csv(path)
    except Exception as exc:
        log.warning("Failed to load sale_lines: %s", exc)
        return None


def _so_line_skus(sale_lines: pd.DataFrame,
                       so_number: str) -> List[dict]:
    """Return list of {sku, qty, customer} for one SO."""
    if sale_lines is None or sale_lines.empty:
        return []
    so_col = next(
        (c for c in ("OrderNumber", "SaleNumber", "InvoiceNumber")
          if c in sale_lines.columns), None)
    sku_col = next(
        (c for c in ("SKU", "ProductCode")
          if c in sale_lines.columns), None)
    qty_col = next(
        (c for c in ("Quantity", "Qty") if c in sale_lines.columns),
        None)
    cust_col = next(
        (c for c in ("Customer", "CustomerName", "BillingName")
          if c in sale_lines.columns), None)
    if not so_col or not sku_col:
        return []
    # Normalise SO number for matching (drop SO- prefix, match
    # numeric core like ai_tools.get_shipping_details does).
    norm = so_number.upper().replace("SO-", "").replace("SO", "")
    match = sale_lines[
        sale_lines[so_col].astype(str).str.upper()
        .str.replace("SO-", "", regex=False)
        .str.replace("INV-", "", regex=False) == norm]
    out = []
    for _, row in match.iterrows():
        out.append({
            "sku": str(row.get(sku_col) or "").strip().upper(),
            "qty": row.get(qty_col),
            "customer": (str(row.get(cust_col))
                          if cust_col else None),
        })
    return out


def _sku_intel(sku: str, sale_lines: pd.DataFrame) -> dict:
    """Aggregate everything we know about a SKU into the format
    the stock-controller intelligence block expects."""
    out = {
        "sku": sku,
        "name": None,
        "bin": None,
        "abc": None,
        "trend": None,
        "on_hand": None,
        "allocated": None,
        "on_order": None,
        "open_sos": [],
        "next_po": None,
    }
    # bot_engine_lookup for ABC / trend / OnHand / bin
    try:
        from bot_engine_lookup import lookup_sku_signals
        sig = lookup_sku_signals(sku)
        if sig:
            out["on_hand"] = sig.get("stock")
            out["bin"] = sig.get("bin")
            out["abc"] = sig.get("abc")
            out["trend"] = sig.get("trend_flag")
    except Exception:
        pass
    # Open SOs allocated against this SKU (count from sale_lines)
    if sale_lines is not None and not sale_lines.empty:
        sku_col = next(
            (c for c in ("SKU", "ProductCode")
              if c in sale_lines.columns), None)
        so_col = next(
            (c for c in ("OrderNumber", "SaleNumber")
              if c in sale_lines.columns), None)
        qty_col = next(
            (c for c in ("Quantity", "Qty")
              if c in sale_lines.columns), None)
        status_col = next(
            (c for c in ("Status", "InvoiceStatus")
              if c in sale_lines.columns), None)
        if sku_col:
            m = sale_lines[
                sale_lines[sku_col].astype(str).str.upper() == sku]
            # Filter out shipped/voided if status column present.
            if status_col is not None and not m.empty:
                status_u = (m[status_col].fillna("").astype(str)
                              .str.upper())
                m = m[~status_u.str.contains(
                    "RECEIVED|CANCELLED|VOIDED|CLOSED|COMPLETED",
                    regex=True, na=False)]
            if not m.empty and so_col:
                so_qty = {}
                for _, r in m.iterrows():
                    so = str(r.get(so_col) or "").strip()
                    q = r.get(qty_col) if qty_col else None
                    try:
                        q = float(q)
                    except (TypeError, ValueError):
                        q = 0.0
                    so_qty[so] = so_qty.get(so, 0.0) + q
                total_alloc = sum(so_qty.values())
                out["allocated"] = total_alloc
                out["open_sos"] = sorted(so_qty.items())[:5]
    # Next incoming PO via po_dispatch_reminder helpers.
    try:
        from po_dispatch_reminder import _load_purchases_and_lines
        purchases, lines = _load_purchases_and_lines()
        if lines is not None and not lines.empty:
            if "SKU" in lines.columns:
                m = lines[lines["SKU"].astype(str).str.upper() == sku]
                if "Status" in m.columns:
                    sc = (m["Status"].fillna("").astype(str)
                            .str.upper())
                    m = m[~sc.str.contains(
                        "RECEIVED|CLOSED|COMPLETED|CANCELLED|VOIDED|"
                        "DRAFT", regex=True, na=False)]
                if not m.empty:
                    date_col = next(
                        (c for c in ("RequiredBy", "ExpectedDate",
                                        "DeliveryDate")
                          if c in m.columns), None)
                    if date_col:
                        m = m.copy()
                        m["__d"] = pd.to_datetime(
                            m[date_col], errors="coerce")
                        m = m.sort_values(
                            "__d", na_position="last")
                    r = m.iloc[0]
                    out["next_po"] = {
                        "po_number": r.get("OrderNumber"),
                        "qty": r.get("Quantity"),
                        "supplier": r.get("Supplier"),
                        "eta": (str(r.get(date_col))[:10]
                                  if date_col
                                  and pd.notna(r.get(date_col))
                                  else None),
                    }
                    out["on_order"] = float(r.get("Quantity") or 0)
    except Exception as exc:
        log.warning("Next PO lookup for %s failed: %s", sku, exc)
    return out


def _compose_intelligence_block(items: List[dict],
                                       so_numbers: List[str],
                                       issue_type: str) -> str:
    """Build the stock-controller intelligence block. Per James's
    spec: factual data + 'please confirm tracking' ask. NO
    dispatch recommendation."""
    lines: List[str] = ["📋 *Stock-issue intelligence — please verify and confirm:*", ""]
    for item in items:
        sku = item.get("sku") or "?"
        name = item.get("name") or ""
        head = f"*`{sku}`*"
        if name:
            head += f" — _{name}_"
        lines.append(head)
        parts = []
        if item.get("bin"):
            parts.append(f"Bin {item['bin']}")
        if item.get("abc"):
            parts.append(f"{item['abc']}-class")
        if item.get("trend"):
            parts.append(item["trend"])
        if parts:
            lines.append(f"• {' · '.join(parts)}")
        on_hand_str = (f"{int(item['on_hand'])}"
                        if item.get("on_hand") is not None
                        else "?")
        alloc_str = (f"{int(item['allocated'])}"
                      if item.get("allocated") is not None
                      else "0")
        lines.append(
            f"• CIN7 OnHand: *{on_hand_str}* · "
            f"Allocated: *{alloc_str}*"
            + (f" (across {len(item['open_sos'])} open SOs)"
                if item.get("open_sos") else ""))
        if item.get("open_sos"):
            sos_str = ", ".join(
                f"{so}×{int(q)}" for so, q in item["open_sos"])
            lines.append(f"   _Open SOs:_ {sos_str}")
        po = item.get("next_po")
        if po:
            po_bits = []
            if po.get("po_number"):
                po_bits.append(f"PO-{po['po_number']}")
            if po.get("qty") is not None:
                try:
                    po_bits.append(f"{int(po['qty'])} units")
                except Exception:
                    pass
            if po.get("supplier"):
                po_bits.append(str(po["supplier"]))
            if po.get("eta"):
                po_bits.append(f"ETA {po['eta']}")
            lines.append(f"• Next PO: {' · '.join(po_bits)}")
        else:
            lines.append("• Next PO: _none on order_")
        lines.append("")
    lines.append(
        "_Reply 'fixed' / 'adjusted' / 'no change' to close this "
        "issue. I'll DM Jamie if no response within 4h._")
    return "\n".join(lines)


def _fulfillment_status(item: dict) -> str:
    """Return 'yes' / 'no' / 'unknown' for can-we-fulfill-this-line.
    'yes' = OnHand >= requested qty. 'no' = OnHand < requested qty.
    'unknown' = either value is missing.

    Note: we use OnHand directly rather than (OnHand - allocated)
    because the allocation count INCLUDES this line's qty. The
    buyer reading the message can subtract for themselves; the
    bot's job is to surface the raw signals."""
    on_hand = item.get("on_hand")
    req = item.get("requested_qty")
    if on_hand is None or req is None:
        return "unknown"
    try:
        return "yes" if float(on_hand) >= float(req) else "no"
    except (TypeError, ValueError):
        return "unknown"


def _needs_buyer_ping(items: List[dict]) -> bool:
    """v2.67.145 — Only ping the buyer when:
      (a) at least one item cannot be fulfilled from on-hand
          (status 'no' or 'unknown'), AND
      (b) that same item has an incoming PO with a known ETA
          that's worth confirming.
    If status is 'yes' for everything, the bot just answers
    without involving the buyer. If status is 'no' but no PO is
    on the way, the buyer can't confirm an ETA that doesn't
    exist — different escalation (reorder decision) which we
    leave to the stockkeeper."""
    for item in items:
        status = _fulfillment_status(item)
        if status == "yes":
            continue
        po = item.get("next_po") or {}
        if po.get("eta"):
            return True
    return False


def _needs_reorder_flag(items: List[dict]) -> bool:
    """True if at least one item cannot be fulfilled AND has NO
    incoming PO. Surface as a 'consider reorder' note — distinct
    from the buyer ETA-confirmation case."""
    for item in items:
        status = _fulfillment_status(item)
        if status == "yes":
            continue
        po = item.get("next_po") or {}
        if not po.get("eta"):
            return True
    return False


def _compose_querier_reply(items: List[dict],
                                so_numbers: List[str],
                                buyer_dm_channel: Optional[str]
                                ) -> str:
    """Brief reply to the querier — high-level snapshot + the
    SPECIFIC follow-up the bot is recommending.

    Per James (v2.67.145 refinement): only ask the querier to
    confirm with the buyer when (a) we can't fulfill from on-hand
    and (b) there's an incoming PO with an ETA to verify. Don't
    fire the buyer ping when stock is fine OR when no PO exists."""
    buyer_text = ("@AndrewTunley"
                    if buyer_dm_channel else "the buyer")

    summary_bits = []
    any_unknown_qty = False
    for item in items:
        sku = item.get("sku") or "?"
        on_hand = item.get("on_hand")
        alloc = item.get("allocated") or 0
        req = item.get("requested_qty")
        next_eta = (item.get("next_po") or {}).get("eta")
        status = _fulfillment_status(item)

        oh = (int(on_hand) if on_hand is not None else "?")
        bit_prefix = (
            "✅" if status == "yes"
            else "🟥" if status == "no"
            else "❔")
        bit = f"{bit_prefix} `{sku}` — OnHand {oh}"
        if req is not None:
            try:
                bit += f", needs {int(req)}"
            except (TypeError, ValueError):
                pass
        else:
            any_unknown_qty = True
        if alloc and not req:
            bit += f", {int(alloc)} allocated total"
        if status == "no" and next_eta:
            bit += f" · next PO ETA *{next_eta}*"
        elif status == "no":
            bit += f" · *no incoming PO*"
        summary_bits.append(bit)
    snapshot = "\n".join(summary_bits[:5])

    body = snapshot
    needs_buyer = _needs_buyer_ping(items)
    needs_reorder = _needs_reorder_flag(items)

    if needs_buyer:
        body += (f"\n\n⚠️ Stock is short on the items above with "
                  f"an incoming PO. Please confirm with "
                  f"{buyer_text} that the listed ETA is accurate.")
    if needs_reorder:
        body += (f"\n\n🔴 No incoming PO for items marked above. "
                  f"Stockkeeper / buyer to decide on reorder.")
    if not needs_buyer and not needs_reorder:
        all_yes = all(
            _fulfillment_status(it) == "yes" for it in items)
        if all_yes:
            body += "\n\n✅ All items appear to have stock on hand."
        elif any_unknown_qty:
            body += ("\n\n_Requested quantity wasn't extractable "
                      "from the message — verify the SO/SKU "
                      "details before quoting._")

    body += ("\n\n_Full detail for the stock controller posted "
              "in the next message._")
    return body


# ---------------------------------------------------------------------------
# Slack posting
# ---------------------------------------------------------------------------
def _post_to_slack(channel_id: str, text: str,
                       thread_ts: Optional[str] = None
                       ) -> Tuple[Optional[str], Optional[str]]:
    try:
        import slack_sync
    except ImportError as exc:
        return None, f"slack_sync import failed: {exc}"
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not token:
        return None, "SLACK_BOT_TOKEN not set"
    body = {
        "channel": channel_id,
        "text": text,
        "unfurl_links": False,
        "unfurl_media": False,
    }
    if thread_ts:
        body["thread_ts"] = thread_ts
    try:
        session = slack_sync._build_session(token)
        resp = slack_sync._slack_post(
            session, "chat.postMessage", body)
        if not resp.get("ok"):
            return None, f"slack returned ok=false: {resp}"
        return resp.get("ts"), None
    except Exception as exc:
        return None, f"post error: {exc}"


# ---------------------------------------------------------------------------
# Top-level handler (called from slack_listener.process_once)
# ---------------------------------------------------------------------------
def handle_stock_issue(msg: dict) -> Tuple[Optional[str], List[str]]:
    """Classify, persist, build intelligence, post the brief
    querier reply + stock-controller intelligence block. Returns
    (querier_reply_text, tools_used) so the listener can record
    its standard slack_bot_responses row for the QUERIER reply.
    The intelligence block + tracking row are persisted here."""
    text = msg.get("text") or ""
    cls = classify_message(text)
    if not cls:
        return None, []

    tools_used: List[str] = ["classify_message"]

    so_numbers = cls.get("so_numbers") or []
    inv_numbers = cls.get("inv_numbers") or []
    skus_from_text = cls.get("skus") or []

    # Build the list of items to surface intelligence for.
    sale_lines = _load_sale_lines()
    items: List[dict] = []
    seen_skus: set = set()
    families: List[str] = []

    # Expand SOs → SKUs from sale_lines. v2.67.145 — also capture
    # the line's requested quantity so the fulfillment-status
    # check (can we ship?) can reason about OnHand vs needs.
    for so in so_numbers + inv_numbers:
        for line in _so_line_skus(sale_lines, so):
            sku = line.get("sku") or ""
            if not sku or sku in seen_skus:
                continue
            seen_skus.add(sku)
            intel = _sku_intel(sku, sale_lines)
            try:
                intel["requested_qty"] = (
                    float(line.get("qty"))
                    if line.get("qty") is not None
                    else None)
            except (TypeError, ValueError):
                intel["requested_qty"] = None
            intel["from_so"] = so
            intel["customer"] = line.get("customer")
            items.append(intel)
        tools_used.append("so_line_skus")

    # Add any SKUs directly mentioned in the text.
    for sku in skus_from_text:
        if sku in seen_skus:
            continue
        seen_skus.add(sku)
        items.append(_sku_intel(sku, sale_lines))
        tools_used.append("sku_intel")

    if not items:
        # We classified it but couldn't expand to any SKU
        # intelligence — log + skip.
        return None, tools_used

    # Persist the issue (idempotent on channel/ts).
    issue_id = db.upsert_stock_issue(
        raise_channel=msg["channel_id"],
        raise_ts=msg["ts"],
        raise_thread_ts=msg.get("thread_ts") or msg["ts"],
        raised_by=msg.get("user_name"),
        raised_text=text,
        issue_type=cls["issue_type"],
        so_numbers=so_numbers,
        skus=list(seen_skus),
        families=families,
    )
    tools_used.append(f"upsert_stock_issue:{issue_id}")

    # Compose both reply pieces.
    buyer_dm = os.environ.get(
        "SLACK_BUYER_DM_CHANNEL_ID", "").strip() or None
    querier_reply = _compose_querier_reply(
        items, so_numbers, buyer_dm)
    intel_block = _compose_intelligence_block(
        items, so_numbers, cls["issue_type"])

    # Post the intelligence block as a separate thread reply.
    # The querier reply is returned to the caller so they post it
    # via the standard listener post pathway (and record in
    # slack_bot_responses), keeping the auditing consistent.
    thread_ts = msg.get("thread_ts") or msg["ts"]
    posted_ts, err = _post_to_slack(
        msg["channel_id"], intel_block, thread_ts=thread_ts)
    if posted_ts:
        db.update_stock_issue_bot_reply(issue_id, posted_ts)
        tools_used.append("intel_block_posted")
    elif err:
        log.error("Intel block post failed for issue %d: %s",
                    issue_id, err)

    return querier_reply, tools_used


# ---------------------------------------------------------------------------
# Resolution detection — pick up confirmations from staff replies
# ---------------------------------------------------------------------------
_RESOLUTION_KEYWORDS = (
    "fixed", "adjusted", "corrected", "sorted", "done",
    "no change", "no adjustment", "all good", "confirmed",
    "resolved",
)


def maybe_resolve_from_thread_reply(msg: dict) -> bool:
    """If `msg` is a human reply in a thread we're tracking, and
    its text contains a resolution keyword, mark the corresponding
    stock_issue resolved. Returns True if resolution was applied."""
    text = (msg.get("text") or "").lower().strip()
    if not text:
        return False
    if msg.get("is_our_bot") or msg.get("is_bot"):
        return False
    thread_ts = msg.get("thread_ts")
    if not thread_ts:
        return False
    issue = db.find_stock_issue_by_thread(
        msg["channel_id"], thread_ts)
    if not issue:
        return False
    if not any(k in text for k in _RESOLUTION_KEYWORDS):
        return False
    db.resolve_stock_issue(
        int(issue["id"]),
        resolved_by=msg.get("user_name") or "unknown",
        resolution_text=(msg.get("text") or "")[:300])
    log.info("Resolved stock_issue %s via thread reply from %s",
              issue["id"], msg.get("user_name"))
    return True


# ---------------------------------------------------------------------------
# Escalation cycle — DM stock controller after Nh of no reply
# ---------------------------------------------------------------------------
def run_escalation_cycle(dryrun: bool = False,
                              min_age_hours: int = 4) -> dict:
    """For each awaiting_response stock_issue older than
    min_age_hours that hasn't been DM'd yet, DM Jamie Webb (or
    whoever SLACK_STOCKKEEPER_DM_CHANNEL_ID points at) with the
    intelligence block + a direct ask to confirm/correct."""
    dm_channel = os.environ.get(
        "SLACK_STOCKKEEPER_DM_CHANNEL_ID", "").strip()
    if not dryrun and not dm_channel:
        return {"escalated": 0, "skipped_no_channel": True}

    pending = db.list_stock_issues_pending_escalation(
        min_age_hours=min_age_hours)
    if not pending:
        return {"pending": 0, "escalated": 0}

    sale_lines = _load_sale_lines()
    n_escalated = 0
    n_errors = 0
    for issue in pending:
        so_numbers = (issue.get("so_numbers") or "").split(",")
        skus = (issue.get("skus") or "").split(",")
        so_numbers = [s for s in so_numbers if s]
        skus = [s for s in skus if s]
        items = []
        seen = set()
        for so in so_numbers:
            for line in _so_line_skus(sale_lines, so):
                sku = line.get("sku") or ""
                if not sku or sku in seen:
                    continue
                seen.add(sku)
                items.append(_sku_intel(sku, sale_lines))
        for sku in skus:
            if sku in seen:
                continue
            seen.add(sku)
            items.append(_sku_intel(sku, sale_lines))
        if not items:
            continue
        intel_text = _compose_intelligence_block(
            items, so_numbers, issue.get("issue_type"))
        # Wrap with a 'no reply, please action' header.
        age_hours = 0
        try:
            ts = pd.to_datetime(issue["created_at"])
            age_hours = int(
                (pd.Timestamp.now() - ts).total_seconds() / 3600)
        except Exception:
            pass
        dm_text = (
            f"🚨 *Stock issue waiting on you* "
            f"(raised ~{age_hours}h ago by "
            f"{issue.get('raised_by') or 'someone'}):\n\n"
            f"_Original message:_\n"
            f"> {(issue.get('raised_text') or '')[:300]}\n\n"
            + intel_text)
        log.info("Escalating issue %s to stockkeeper %s",
                  issue["id"], "[DRYRUN]" if dryrun else "")
        if dryrun:
            print(f"\n--- ESCALATION DM for issue "
                    f"{issue['id']} ---\n{dm_text}\n")
            continue
        posted_ts, err = _post_to_slack(dm_channel, dm_text)
        if err:
            log.error("DM escalation failed for issue %s: %s",
                        issue["id"], err)
            n_errors += 1
            continue
        db.update_stock_issue_dm(
            int(issue["id"]),
            dm_channel=dm_channel,
            dm_posted_ts=posted_ts,
            awaiting_user="stockkeeper")
        n_escalated += 1
    return {
        "pending": len(pending),
        "escalated": n_escalated,
        "errors": n_errors,
    }


# ---------------------------------------------------------------------------
# Morning summary
# ---------------------------------------------------------------------------
def run_morning_summary(dryrun: bool = False) -> dict:
    """Post a daily summary of outstanding stock issues to the
    #stock-issues-queries channel."""
    channel = os.environ.get(
        "SLACK_STOCK_ISSUES_CHANNEL_ID", "").strip()
    if not dryrun and not channel:
        return {"posted": 0, "skipped_no_channel": True}
    issues = db.list_open_stock_issues(limit=50, max_age_days=30)
    if not issues:
        return {"posted": 0, "open_count": 0}
    lines = [f"📋 *Outstanding stock issues — "
              f"{len(issues)} open as of "
              f"{datetime.now().strftime('%-d %b %Y')}*", ""]
    for iss in issues:
        try:
            ts = pd.to_datetime(iss["created_at"])
            age_days = int(
                (pd.Timestamp.now() - ts).total_seconds() / 86400)
        except Exception:
            age_days = 0
        age_text = (f"{age_days} day{'s' if age_days != 1 else ''}"
                      if age_days > 0 else "<1 day")
        sos = iss.get("so_numbers") or ""
        skus = iss.get("skus") or ""
        primary = sos or skus or "(no identifier)"
        bullet = (f"• *{primary.split(',')[0]}* — raised "
                    f"{age_text} ago by "
                    f"{iss.get('raised_by') or '?'} "
                    f"· status _{iss.get('status')}_")
        lines.append(bullet)
    lines.append("")
    lines.append(
        "_Reply 'fixed' / 'adjusted' / 'no change' in each "
        "thread once handled._")
    text = "\n".join(lines)
    if dryrun:
        print(text)
        return {"posted": 0, "open_count": len(issues),
                  "dryrun": True}
    posted_ts, err = _post_to_slack(channel, text)
    if err:
        return {"posted": 0, "error": err,
                  "open_count": len(issues)}
    return {"posted": 1, "open_count": len(issues)}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _setup_log(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=LOG_FORMAT, stream=sys.stdout, force=True)


def cmd_escalate(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    hours = int(os.environ.get(
        "STOCK_ISSUE_ESCALATION_HOURS", "4") or 4)
    result = run_escalation_cycle(
        dryrun=bool(args.dryrun), min_age_hours=hours)
    log.info("DONE: %s", result)
    return 0


def cmd_morning(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    result = run_morning_summary(dryrun=bool(args.dryrun))
    log.info("DONE: %s", result)
    return 0


def cmd_inspect(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    with db.connect() as c:
        r = c.execute(
            "SELECT * FROM stock_issues WHERE id = ?",
            (args.issue_id,)).fetchone()
    if not r:
        log.error("No issue with id=%d", args.issue_id)
        return 1
    import json as _json
    print(_json.dumps(dict(r), indent=2, default=str))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stock-issues tracker for "
                      "#stock-issues-queries.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_e = sub.add_parser("escalate")
    p_e.add_argument("--dryrun", action="store_true")
    p_e.add_argument("--verbose", action="store_true")
    p_e.set_defaults(func=cmd_escalate)
    p_m = sub.add_parser("morning-summary")
    p_m.add_argument("--dryrun", action="store_true")
    p_m.add_argument("--verbose", action="store_true")
    p_m.set_defaults(func=cmd_morning)
    p_i = sub.add_parser("inspect")
    p_i.add_argument("--issue-id", type=int, required=True)
    p_i.add_argument("--verbose", action="store_true")
    p_i.set_defaults(func=cmd_inspect)
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
