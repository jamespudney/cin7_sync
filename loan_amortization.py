"""loan_amortization.py (v2.67.235)
======================================

Deterministic loan amortization for the Cashflow Management
loan tracker. Pure calculation — no DB, no I/O, standard library
only — so it is easy to test and audit.

Interest method: Actual/365 simple interest.
  interest = opening_balance * (APR/100) * days / 365
where `days` is the actual day count of the period (the gap
between consecutive payment dates; the first period runs from
the loan start date to the first payment date).

Payment application: interest first, the remainder to principal.
Final payment: MIN(standard payment, opening balance + interest)
— so the loan closes exactly at zero.
"""

from __future__ import annotations

import calendar
import datetime
from typing import Dict, List


def _add_month(d: datetime.date) -> datetime.date:
    """Advance a date by one calendar month, clamping the day to
    the last day of the target month (e.g. 31 Jan -> 28 Feb)."""
    month = d.month + 1
    year = d.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    last = calendar.monthrange(year, month)[1]
    return datetime.date(year, month, min(d.day, last))


def _parse(d) -> datetime.date:
    if isinstance(d, datetime.date):
        return d
    return datetime.date.fromisoformat(str(d)[:10])


def compute_schedule(principal: float,
                     apr: float,
                     start_date: str,
                     first_payment_date: str,
                     monthly_payment: float,
                     max_periods: int = 600) -> List[Dict]:
    """Build the amortization schedule.

    Args:
      principal           — original loan amount
      apr                 — annual rate as a percent (e.g. 6.5)
      start_date          — loan start 'YYYY-MM-DD' (interest
                            accrues from here)
      first_payment_date  — first payment 'YYYY-MM-DD'; later
                            payments fall monthly on that day
      monthly_payment     — standard payment amount
      max_periods         — safety cap

    Returns a list of period dicts:
      {date, opening, interest, principal, payment, closing}
    """
    opening = float(principal or 0)
    apr_f = float(apr or 0) / 100.0
    pay = float(monthly_payment or 0)
    try:
        pstart = _parse(start_date)
        pay_date = _parse(first_payment_date)
    except (ValueError, TypeError):
        return []

    rows: List[Dict] = []
    for _ in range(max_periods):
        if opening <= 0.005 or pay <= 0:
            break
        days = max(0, (pay_date - pstart).days)
        interest = opening * apr_f * days / 365.0
        if opening + interest <= pay:
            # Final payment — clears the loan exactly.
            this_pay = opening + interest
            prin = opening
            closing = 0.0
        else:
            this_pay = pay
            prin = pay - interest
            closing = opening - prin
        rows.append({
            "date": pay_date.isoformat(),
            "opening": round(opening, 2),
            "interest": round(interest, 2),
            "principal": round(prin, 2),
            "payment": round(this_pay, 2),
            "closing": round(max(closing, 0.0), 2),
        })
        if closing <= 0.005:
            break
        opening = closing
        pstart = pay_date
        pay_date = _add_month(pay_date)
    return rows


def schedule_summary(rows: List[Dict]) -> Dict:
    """Headline figures for a computed schedule."""
    if not rows:
        return {"payoff_date": None, "total_interest": 0.0,
                "total_paid": 0.0, "periods": 0}
    return {
        "payoff_date": rows[-1]["date"],
        "total_interest": round(
            sum(r["interest"] for r in rows), 2),
        "total_paid": round(
            sum(r["payment"] for r in rows), 2),
        "periods": len(rows),
    }
