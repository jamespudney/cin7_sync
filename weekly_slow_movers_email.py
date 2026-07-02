"""weekly_slow_movers_email.py
================================
Send a weekly digest email of slow-mover progress to the sales /
buyer team. Designed to run from cron / Task Scheduler on Friday
mornings — every recipient gets one email summarising:

  - Top 20 slow movers by stock-value tied up (the biggest bets to
    clear)
  - Newcomers — SKUs that became dormant in the past 7 days
  - Progress — SKUs that recovered (warning auto-lifted) in the
    past 7 days
  - Stock-value cleared this week (cost basis of slow-stock sales)

Configuration via environment variables:

    SLOW_MOVERS_EMAIL_TO           comma-separated recipient list
                                    (e.g. "sales@x.com, buyer@x.com")
    SMTP_HOST, SMTP_PORT,
    SMTP_USER, SMTP_PASS, SMTP_FROM
                                    standard SMTP creds. SMTP_FROM
                                    defaults to SMTP_USER.

If SLOW_MOVERS_EMAIL_TO is unset, the script logs and exits 0
(silent disable). If SMTP settings are partial, exits 1.

How it's wired:
  - Local Windows: weekly_slow_movers_email.bat + Task Scheduler
    (schedule_weekly_email.bat registers it on Fridays).
  - Render: invoked from the existing sync_loop.sh on Fridays
    (date check inside).
"""

from __future__ import annotations

import os
import smtplib
import sys
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path


def _emit(msg: str, level: str = "info") -> None:
    sys.stderr.write(f"[weekly_slow_movers_email] {level}: {msg}\n")
    sys.stderr.flush()


def _load_data():
    """Load freshest CSVs + dormancy log."""
    import pandas as pd

    # Use data_paths conventions
    _here = Path(__file__).resolve().parent
    if str(_here) not in sys.path:
        sys.path.insert(0, str(_here))
    from data_paths import OUTPUT_DIR
    import db
    from sales_exclusions import filter_excluded_sales_customers

    def _latest(pattern: str) -> Path | None:
        matches = sorted(
            OUTPUT_DIR.glob(pattern),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        return matches[0] if matches else None

    products_csv = _latest("products_*.csv")
    stock_csv = _latest("stock_on_hand_*.csv")
    sale_lines_csv = _latest("sale_lines_last_*d_*.csv")
    if any(p is None for p in
            (products_csv, stock_csv, sale_lines_csv)):
        raise FileNotFoundError(
            "Missing one of products / stock_on_hand / sale_lines "
            f"CSVs in {OUTPUT_DIR}. Has the sync run?")

    products = pd.read_csv(products_csv, low_memory=False)
    stock = pd.read_csv(stock_csv, low_memory=False)
    sale_lines = filter_excluded_sales_customers(
        pd.read_csv(sale_lines_csv, low_memory=False))

    warnings = db.get_dormancy_warnings()
    return {
        "products": products,
        "stock": stock,
        "sale_lines": sale_lines,
        "warnings": warnings,
        "db": db,
    }


def _format_money(n: float) -> str:
    if n is None:
        return "—"
    return f"${n:,.0f}"


def _build_body(data: dict, week_start, week_end) -> tuple[str, str]:
    """Returns (plain_text_body, html_body)."""
    import pandas as pd

    warnings = data["warnings"]
    products = data["products"]
    stock = data["stock"]
    sale_lines = data["sale_lines"]

    # Build a per-SKU value index from stock_on_hand (FIFO basis).
    if "StockOnHand" in stock.columns:
        sv = (stock.assign(SKU=stock["SKU"].astype(str))
                    .groupby("SKU")["StockOnHand"]
                    .sum().to_dict())
    else:
        sv = {}
    if "OnHand" in stock.columns:
        oh = (stock.assign(SKU=stock["SKU"].astype(str))
                    .groupby("SKU")["OnHand"]
                    .sum().to_dict())
    else:
        oh = {}
    name_map = (products.assign(SKU=products["SKU"].astype(str))
                .set_index("SKU")["Name"].to_dict()) \
        if not products.empty else {}

    rows = []
    for sku, info in warnings.items():
        rows.append({
            "SKU": sku,
            "Name": str(name_map.get(sku, "(unknown)"))[:80],
            "OnHand": float(oh.get(sku, 0) or 0),
            "StockValue": float(sv.get(sku, 0) or 0),
            "FirstSeenDormant": str(
                info.get("first_seen_dormant_at") or "")[:10],
            "RecoveredAt": str(info.get("recovered_at") or "")[:10],
        })
    df = pd.DataFrame(rows)
    if df.empty:
        plain = (
            "No slow-mover SKUs are currently flagged. The engine's "
            "dormancy log is empty (or all warnings have lifted).\n"
            "\nNothing to action this week."
        )
        return plain, "<p>" + plain.replace("\n", "<br>") + "</p>"

    # Top 20 by stock value tied up
    top20 = df.sort_values("StockValue", ascending=False).head(20)

    # Newcomers — first_seen_dormant_at within last 7 days
    newcomers = df[
        df["FirstSeenDormant"].apply(
            lambda d: bool(d) and d >= str(week_start.date()))
    ].sort_values("StockValue", ascending=False)

    # Stock cleared this week — sum cost basis on slow SKUs whose
    # InvoiceDate falls inside the window.
    cleared_value = 0.0
    cleared_units = 0.0
    cleared_skus = 0
    if (not sale_lines.empty
            and "InvoiceDate" in sale_lines.columns):
        sl = sale_lines.copy()
        sl["InvoiceDate"] = pd.to_datetime(
            sl["InvoiceDate"], errors="coerce")
        sl["SKU"] = sl["SKU"].astype(str)
        in_window = sl[
            (sl["SKU"].isin(set(warnings.keys())))
            & (sl["InvoiceDate"] >= week_start)
            & (sl["InvoiceDate"] <= week_end)
        ]
        if "Status" in in_window.columns:
            bad = ("VOIDED", "CREDITED", "CANCELLED", "CANCELED")
            in_window = in_window[
                ~in_window["Status"].astype(str).str.upper().isin(bad)]
        if not in_window.empty:
            qty = pd.to_numeric(
                in_window["Quantity"], errors="coerce").fillna(0)
            if "AverageCost" in in_window.columns:
                cost = pd.to_numeric(
                    in_window["AverageCost"], errors="coerce").fillna(0)
            else:
                cost = pd.Series(0.0, index=in_window.index)
            cleared_value = float((qty * cost).sum())
            cleared_units = float(qty.sum())
            cleared_skus = int(in_window["SKU"].nunique())

    # ---- Plain text body ----
    week_label = (
        f"{week_start.strftime('%Y-%m-%d')} → "
        f"{week_end.strftime('%Y-%m-%d')}")
    plain_lines = [
        f"Slow-mover weekly digest — {week_label}",
        "",
        f"Active slow-mover warnings: {len(df):,}",
        f"Total stock value tied up: {_format_money(df['StockValue'].sum())}",
        "",
        f"CLEARED THIS WEEK",
        f"  - Cost basis sold: {_format_money(cleared_value)}",
        f"  - Units sold: {cleared_units:,.0f}",
        f"  - Distinct SKUs sold: {cleared_skus:,}",
        "",
        f"NEWCOMERS THIS WEEK ({len(newcomers):,})",
    ]
    for _, r in newcomers.head(10).iterrows():
        plain_lines.append(
            f"  - {r['SKU']} — {r['Name']} — "
            f"{r['OnHand']:.1f} on hand, "
            f"{_format_money(r['StockValue'])} value")
    plain_lines += [
        "",
        f"TOP 20 SLOW MOVERS BY STOCK VALUE",
    ]
    for i, r in enumerate(top20.itertuples(index=False), 1):
        plain_lines.append(
            f"  {i:>2}. {r.SKU} — {r.Name} — "
            f"{r.OnHand:.1f} on hand, "
            f"{_format_money(r.StockValue)} — flagged "
            f"{r.FirstSeenDormant}")
    plain_lines += [
        "",
        "—",
        "Open the Slow Movers page in the dashboard for the full "
        "list, pie chart, and dismiss controls.",
    ]
    plain_body = "\n".join(plain_lines)

    # ---- HTML body ----
    html_rows_top20 = "\n".join(
        f"<tr><td>{i}</td><td><code>{r.SKU}</code></td>"
        f"<td>{r.Name}</td>"
        f"<td style='text-align:right'>{r.OnHand:.1f}</td>"
        f"<td style='text-align:right'>{_format_money(r.StockValue)}</td>"
        f"<td>{r.FirstSeenDormant}</td></tr>"
        for i, r in enumerate(top20.itertuples(index=False), 1)
    )
    html_rows_newcomers = "\n".join(
        f"<tr><td><code>{r['SKU']}</code></td><td>{r['Name']}</td>"
        f"<td style='text-align:right'>{r['OnHand']:.1f}</td>"
        f"<td style='text-align:right'>{_format_money(r['StockValue'])}</td>"
        f"</tr>"
        for _, r in newcomers.head(15).iterrows()
    ) or "<tr><td colspan='4'><em>No new slow movers this week.</em></td></tr>"

    html_body = f"""\
<html><body style="font-family:Arial, sans-serif; max-width:780px;">
<h2>🪫 Slow-mover weekly digest</h2>
<p style="color:#666">{week_label}</p>

<table cellpadding="6" style="border-collapse:collapse;">
<tr><td><b>Active warnings</b></td><td>{len(df):,}</td></tr>
<tr><td><b>Stock value tied up</b></td>
    <td>{_format_money(df["StockValue"].sum())}</td></tr>
<tr><td><b>Cleared this week (cost)</b></td>
    <td>{_format_money(cleared_value)}</td></tr>
<tr><td><b>Cleared units</b></td><td>{cleared_units:,.0f}</td></tr>
<tr><td><b>Cleared distinct SKUs</b></td><td>{cleared_skus:,}</td></tr>
</table>

<h3>Newcomers this week ({len(newcomers):,})</h3>
<table border="1" cellpadding="4" cellspacing="0" \
       style="border-collapse:collapse; font-size:13px;">
<thead><tr style="background:#f6f6f6"><th>SKU</th><th>Name</th>
<th>OnHand</th><th>Stock value</th></tr></thead>
<tbody>
{html_rows_newcomers}
</tbody></table>

<h3>Top 20 slow movers by stock value</h3>
<table border="1" cellpadding="4" cellspacing="0" \
       style="border-collapse:collapse; font-size:13px;">
<thead><tr style="background:#f6f6f6"><th>#</th><th>SKU</th>
<th>Name</th><th>OnHand</th><th>Stock value</th>
<th>First flagged</th></tr></thead>
<tbody>
{html_rows_top20}
</tbody></table>

<p style="margin-top:24px; color:#666;">
Open the <b>Slow Movers</b> page in the dashboard for the full list,
pie chart, and per-row dismiss controls. Auto-lifts after 90d
sustained recovery.
</p>
</body></html>
"""
    return plain_body, html_body


def _send_email(subject: str, plain_body: str, html_body: str,
                  recipients: list[str]) -> None:
    smtp_host = os.environ.get("SMTP_HOST", "").strip()
    smtp_port = int(os.environ.get("SMTP_PORT", "587") or 587)
    smtp_user = os.environ.get("SMTP_USER", "").strip()
    smtp_pass = os.environ.get("SMTP_PASS", "").strip()
    smtp_from = (os.environ.get("SMTP_FROM", "").strip() or smtp_user)
    if not (smtp_host and smtp_user and smtp_pass and smtp_from):
        raise RuntimeError(
            "SMTP_HOST/SMTP_USER/SMTP_PASS/SMTP_FROM not all set in env. "
            "Configure them on Render (or in the local environment) "
            "before running this script.")
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = ", ".join(recipients)
    msg.set_content(plain_body)
    msg.add_alternative(html_body, subtype="html")
    with smtplib.SMTP(smtp_host, smtp_port) as s:
        s.ehlo()
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)


def main() -> int:
    recipients_raw = os.environ.get("SLOW_MOVERS_EMAIL_TO", "").strip()
    if not recipients_raw:
        _emit(
            "SLOW_MOVERS_EMAIL_TO not set; nothing to do (silent "
            "disable — set the env var to enable).")
        return 0
    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]
    if not recipients:
        _emit("Recipient list parsed empty; aborting.")
        return 1

    try:
        data = _load_data()
    except Exception as exc:  # noqa: BLE001
        _emit(f"data load failed: {exc!r}", level="error")
        return 2

    week_end = datetime.now()
    week_start = week_end - timedelta(days=7)
    try:
        plain_body, html_body = _build_body(data, week_start, week_end)
    except Exception as exc:  # noqa: BLE001
        _emit(f"body build failed: {exc!r}", level="error")
        return 3

    n_warnings = len(data["warnings"])
    subject = (
        f"🪫 Slow-mover digest — {n_warnings} active warnings — "
        f"{week_end.strftime('%Y-%m-%d')}")
    try:
        _send_email(subject, plain_body, html_body, recipients)
    except Exception as exc:  # noqa: BLE001
        _emit(f"send failed: {exc!r}", level="error")
        return 4
    _emit(f"sent to {len(recipients)} recipient(s); "
          f"{n_warnings} warnings summarised.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
