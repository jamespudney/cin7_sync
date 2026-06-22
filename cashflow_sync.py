"""cashflow_sync.py (v2.67.220)
=================================

Glue between QuickBooks Online and the Cashflow Management page.

Two jobs:
  1. sync_bills_from_qbo() — pull supplier Bills from QBO into the
     cashflow_payables table (the supplier-invoice tracker). Run
     on demand from the dashboard's "Sync from QuickBooks" button.
     The sync also pulls QBO's full open-bills list and marks local
     QBO mirrors paid/closed when they are no longer open in QBO.
  2. post_approval_to_slack() — when James approves a payable for
     payment, post the go-ahead into a dedicated Slack channel so
     Cheran sees the amount cleared to pay.

Env vars
--------
  CASHFLOW_APPROVAL_CHANNEL_ID  Slack channel for payment go-aheads
  SLACK_BOT_TOKEN               standard bot token (already set)
"""

from __future__ import annotations

import datetime as _dt
import logging
import os
from typing import Optional, Tuple

log = logging.getLogger("cashflow_sync")


# ---------------------------------------------------------------------------
# QBO Bills → cashflow_payables
# ---------------------------------------------------------------------------
def _bill_description(bill: dict) -> str:
    """Best-effort human description for a QBO Bill. Prefers the
    memo (PrivateNote); falls back to the first line item's
    description."""
    note = (bill.get("PrivateNote") or "").strip()
    if note:
        return note[:300]
    for line in (bill.get("Line") or []):
        if not isinstance(line, dict):
            continue
        desc = (line.get("Description") or "").strip()
        if desc:
            return desc[:300]
    return ""


def sync_bills_from_qbo(months_back: int = 6) -> dict:
    """Pull supplier Bills from QuickBooks into cashflow_payables.

    We import recent bills for invoice detail, then separately pull
    QBO's full open-bills list. Any locally mirrored QBO bill that is
    not in that open list is marked paid/zero-balance so old paid
    invoices do not stay due forever in the forecast.

    Returns a summary dict. Raises on a hard QBO failure so the
    caller (the dashboard button) can surface the error."""
    import qbo_client
    import db

    since = (_dt.date.today()
             - _dt.timedelta(days=31 * months_back)).isoformat()
    recent_bills = qbo_client.get_bills(since_date=since)
    open_bills = qbo_client.get_bills(only_unpaid=True)
    bills_by_id = {}
    for bill in recent_bills:
        bill_id = str(bill.get("Id") or "").strip()
        if bill_id:
            bills_by_id[bill_id] = bill
    for bill in open_bills:
        bill_id = str(bill.get("Id") or "").strip()
        if bill_id:
            # Open bill data wins if the same bill is in both sets.
            bills_by_id[bill_id] = bill
    bills = list(bills_by_id.values())
    open_bill_ids = {
        str(bill.get("Id") or "").strip()
        for bill in open_bills
        if str(bill.get("Id") or "").strip()
    }

    n_upserted = 0
    n_skipped = 0
    for bill in bills:
        bill_id = str(bill.get("Id") or "").strip()
        if not bill_id:
            n_skipped += 1
            continue
        vendor = (bill.get("VendorRef") or {}).get("name")
        doc_number = bill.get("DocNumber")
        currency = ((bill.get("CurrencyRef") or {}).get("value")
                    or "USD")
        try:
            total = float(bill.get("TotalAmt") or 0)
        except (TypeError, ValueError):
            total = None
        try:
            balance = float(bill.get("Balance") or 0)
        except (TypeError, ValueError):
            balance = None
        db.upsert_qbo_payable(
            qbo_bill_id=bill_id,
            supplier=vendor,
            reference=doc_number,
            description=_bill_description(bill),
            amount=total,
            currency=currency,
            invoice_date=bill.get("TxnDate"),
            due_date=bill.get("DueDate"),
            qbo_balance=balance,
        )
        n_upserted += 1
    n_closed = db.mark_qbo_payables_closed_except(
        sorted(open_bill_ids))

    result = {
        "recent_bills_seen": len(recent_bills),
        "open_bills_seen": len(open_bills),
        "bills_seen": len(bills),
        "upserted": n_upserted,
        "closed": n_closed,
        "skipped": n_skipped,
        "since": since,
    }
    log.info("sync_bills_from_qbo: %s", result)
    return result


# ---------------------------------------------------------------------------
# Approval → Slack
# ---------------------------------------------------------------------------
def _fmt_money(amount: Optional[float], currency: str = "USD") -> str:
    if amount is None:
        return "—"
    sym = {"USD": "$", "EUR": "€", "GBP": "£"}.get(
        (currency or "USD").upper(), "")
    try:
        return f"{sym}{float(amount):,.2f}"
    except (TypeError, ValueError):
        return f"{sym}{amount}"


def build_approval_message(payable: dict, approved_by: str) -> str:
    """Compose the Slack go-ahead message for an approved
    payable. mrkdwn formatting."""
    supplier = payable.get("supplier") or "(supplier?)"
    reference = payable.get("reference") or ""
    desc = payable.get("description") or ""
    currency = payable.get("currency") or "USD"
    # Effective amount — James's override wins over the QBO mirror.
    amount = payable.get("amount_override")
    if amount is None:
        amount = payable.get("amount")
    due = (payable.get("due_date_override")
           or payable.get("due_date") or "—")

    lines = [
        f":white_check_mark: *Payment approved* — "
        f"{_fmt_money(amount, currency)}",
        f"*Supplier:* {supplier}",
    ]
    if reference:
        lines.append(f"*Reference:* {reference}")
    if desc:
        lines.append(f"*For:* {desc}")
    lines.append(f"*Due:* {due}")
    lines.append(f"_Approved by {approved_by} — cleared to pay._")
    return "\n".join(lines)


def post_approval_to_slack(payable: dict, approved_by: str
                           ) -> Tuple[Optional[str], Optional[str]]:
    """Post a payment go-ahead to the cashflow approval channel.
    Returns (slack_ts, error). error is None on success."""
    channel = os.environ.get(
        "CASHFLOW_APPROVAL_CHANNEL_ID", "").strip()
    if not channel:
        return None, ("CASHFLOW_APPROVAL_CHANNEL_ID env var not "
                      "set — approval recorded but not posted to "
                      "Slack.")
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not token:
        return None, "SLACK_BOT_TOKEN not set"
    text = build_approval_message(payable, approved_by)
    try:
        import slack_sync
        session = slack_sync._build_session(token)
        body = slack_sync._slack_post(session, "chat.postMessage", {
            "channel": channel,
            "text": text,
            "unfurl_links": False,
            "unfurl_media": False,
        })
        if not body.get("ok"):
            return None, f"slack returned ok=false: {body}"
        return body.get("ts"), None
    except Exception as exc:  # noqa: BLE001
        return None, f"post error: {exc}"
