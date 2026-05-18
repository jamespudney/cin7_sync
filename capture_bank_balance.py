"""capture_bank_balance.py (v2.67.229)
==========================================

Captures the QuickBooks Online bank-account total into the
current cashflow week's opening_balance cell — automating the
manual "enter the actual bank opening balance" step the team does
each Monday at ~08:00.

How it works
------------
- Reads every QBO Bank-type account and sums CurrentBalance.
  (QBO holds these live as long as the bank accounts are
  bank-fed into QuickBooks.)
- Writes the total to cashflow_forecast for the CURRENT cashflow
  week's Monday, row_key 'opening_balance', updated_by
  'auto:bank-capture'.
- Each run only touches the current week's cell, so it never
  disturbs past weeks or a manual correction to an earlier week.

Intended to run once on a Monday morning (after the overnight
bank feed has updated). Also runnable on demand:

    python capture_bank_balance.py            # capture now
    python capture_bank_balance.py --dry-run  # show, don't write
"""

from __future__ import annotations

import datetime
import logging
import sys

log = logging.getLogger("capture_bank_balance")


def capture_bank_balance(dry_run: bool = False) -> dict:
    """Sum QBO Bank-account balances and store as this week's
    opening balance. Returns a summary dict."""
    import db
    import qbo_client

    accts = qbo_client.get_bank_accounts()
    total = 0.0
    detail = []
    for a in accts:
        bal = a.get("CurrentBalance")
        try:
            bal_f = float(bal or 0)
        except (TypeError, ValueError):
            bal_f = 0.0
        total += bal_f
        detail.append((a.get("Name") or "(account)", bal_f))

    # Current cashflow week — Monday of the current week.
    today = datetime.date.today()
    monday = today - datetime.timedelta(days=today.weekday())
    week_key = monday.isoformat()

    result = {
        "week_start": week_key,
        "bank_total": total,
        "accounts": detail,
        "dry_run": dry_run,
    }
    if dry_run:
        log.info("[dry-run] would set opening_balance %s = %.2f",
                 week_key, total)
        return result

    db.set_forecast_cell(week_key, "opening_balance", total,
                         updated_by="auto:bank-capture")
    log.info("Captured bank opening balance: week %s = %.2f "
             "(%d accounts)", week_key, total, len(detail))
    return result


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout, force=True)
    dry = "--dry-run" in sys.argv
    try:
        res = capture_bank_balance(dry_run=dry)
    except Exception as exc:  # noqa: BLE001
        log.error("capture_bank_balance failed: %s", exc)
        return 1
    print(f"Week {res['week_start']} opening balance "
          f"{'(dry-run) ' if dry else ''}= "
          f"{res['bank_total']:,.2f}")
    for name, bal in res["accounts"]:
        print(f"  {name}: {bal:,.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
