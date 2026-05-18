"""qbo_client.py (v2.67.211)
================================

Thin API client for QuickBooks Online (QBO), sitting on top of the
OAuth layer in qbo_oauth.py.

Responsibilities
----------------
- Resolve the correct API host for the connected environment
  (sandbox vs production).
- Attach a valid bearer token to every request, transparently
  refreshing it via qbo_oauth.get_valid_access_token().
- On a 401 (token expired mid-flight despite the skew margin),
  force ONE refresh + retry before giving up.
- Expose the two QBO surfaces the Cashflow Management page needs:
  the SQL-ish `query` endpoint and the `reports` endpoint.

This module deliberately does NOT interpret the data — it returns
raw QBO JSON. The Cashflow page (built once James shares the
spreadsheet) will shape it.

Public API
----------
- `is_ready()`                  — connected + token obtainable
- `query(sql)`                  — run a QBO query, return entity rows
- `report(name, params)`        — fetch a QBO report
- `company_info()`              — CompanyInfo for the connected realm
- `cashflow_report(start, end)` — convenience: CashFlow report
- `profit_and_loss(start, end)` — convenience: ProfitAndLoss report

All methods raise `QBOError` on failure.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import requests

import qbo_oauth
import db

log = logging.getLogger("qbo_client")

# Data API hosts — distinct from the OAuth host in qbo_oauth.py.
_API_HOST = {
    "sandbox": "https://sandbox-quickbooks.api.intuit.com",
    "production": "https://quickbooks.api.intuit.com",
}

# QBO API minor version — pin it so response shapes are stable.
_MINOR_VERSION = "75"


class QBOError(RuntimeError):
    """Raised for any QBO API failure (not connected, HTTP error,
    fault payload)."""


# ---------------------------------------------------------------------------
# Internal request plumbing
# ---------------------------------------------------------------------------
def _connection_or_raise() -> dict:
    row = db.get_qbo_connection()
    if not row:
        raise QBOError(
            "QuickBooks Online is not connected. Connect it from "
            "the Cashflow Management page first.")
    return row


def _base_url(row: dict) -> str:
    """Build the /v3/company/{realmId} base URL for the connected
    realm + environment."""
    realm_id = row.get("realm_id") or ""
    if not realm_id:
        raise QBOError("QBO connection row has no realm_id.")
    env = (row.get("environment") or qbo_oauth.environment()).lower()
    host = _API_HOST.get(env, _API_HOST["sandbox"])
    return f"{host}/v3/company/{realm_id}"


def _request(method: str, path: str,
             params: Optional[dict] = None,
             json_body: Optional[Any] = None,
             data_body: Optional[str] = None,
             content_type: str = "application/json") -> dict:
    """Issue an authenticated request to the QBO API.

    `path` is appended to the /v3/company/{realmId} base. Handles
    one transparent token-refresh-and-retry on a 401. Returns the
    parsed JSON body; raises QBOError on any failure."""
    row = _connection_or_raise()
    base = _base_url(row)
    url = f"{base}{path}"

    merged_params = dict(params or {})
    merged_params.setdefault("minorversion", _MINOR_VERSION)

    last_error = ""
    for attempt in range(2):
        token = qbo_oauth.get_valid_access_token()
        if not token:
            raise QBOError(
                "Could not obtain a valid QBO access token "
                "(connection may have been revoked — try "
                "reconnecting).")
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        if json_body is not None or data_body is not None:
            headers["Content-Type"] = content_type
        try:
            r = requests.request(
                method,
                url,
                params=merged_params,
                json=json_body,
                data=data_body,
                headers=headers,
                timeout=30,
            )
        except requests.RequestException as exc:
            raise QBOError(
                f"QBO API network error ({method} {path}): "
                f"{exc}") from exc

        # v2.67.217 — capture Intuit's transaction id from the
        # response headers. Intuit support uses intuit_tid to
        # trace a specific API call; surfacing it in every error
        # and log line makes troubleshooting far faster.
        intuit_tid = (r.headers.get("intuit_tid")
                      or r.headers.get("Intuit-Tid") or "")

        if r.status_code == 401 and attempt == 0:
            # Token was rejected — force a refresh on the next loop
            # by clearing the cached access expiry implicitly: the
            # next get_valid_access_token() sees a stale row only
            # if expiry passed, so explicitly refresh here.
            log.info("QBO API 401 (intuit_tid=%s) — forcing token "
                     "refresh + retry.", intuit_tid)
            refreshed = qbo_oauth._refresh_access_token(
                db.get_qbo_connection() or {})
            if not refreshed:
                raise QBOError(
                    "QBO API returned 401 and the token refresh "
                    "failed — reconnect QuickBooks Online. "
                    f"(intuit_tid={intuit_tid})")
            continue

        if r.status_code >= 400:
            last_error = (f"HTTP {r.status_code} "
                          f"(intuit_tid={intuit_tid}): "
                          f"{r.text[:400]}")
            log.error("QBO API error %s %s — %s",
                      method, path, last_error)
            raise QBOError(
                f"QBO API error ({method} {path}): {last_error}")

        if intuit_tid:
            log.debug("QBO API ok %s %s (intuit_tid=%s)",
                      method, path, intuit_tid)
        try:
            return r.json()
        except ValueError as exc:
            raise QBOError(
                f"QBO API returned non-JSON ({method} {path}, "
                f"intuit_tid={intuit_tid}): {r.text[:300]}") from exc

    raise QBOError(
        f"QBO API call failed after retry ({method} {path}): "
        f"{last_error}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def is_ready() -> bool:
    """True if QBO is connected AND a valid access token can be
    obtained right now. Safe to call from UI render paths — never
    raises."""
    try:
        if not db.get_qbo_connection():
            return False
        return qbo_oauth.get_valid_access_token() is not None
    except Exception as exc:  # noqa: BLE001
        log.warning("qbo_client.is_ready check failed: %s", exc)
        return False


def query(sql: str) -> list[dict]:
    """Run a QBO query (the SQL-like language QBO exposes, e.g.
    'SELECT * FROM Invoice WHERE TxnDate > '2026-01-01'') and
    return the list of entity rows.

    QBO wraps results as {'QueryResponse': {'<Entity>': [...]}}.
    We unwrap to the inner list; an empty result yields []."""
    body = _request("GET", "/query", params={"query": sql})
    qr = body.get("QueryResponse") or {}
    for key, value in qr.items():
        # The entity list is the only list-valued key (others are
        # startPosition / maxResults / totalCount ints).
        if isinstance(value, list):
            return value
    return []


def report(name: str,
           params: Optional[dict] = None) -> dict:
    """Fetch a QBO report by name (e.g. 'CashFlow',
    'ProfitAndLoss', 'BalanceSheet'). Returns the raw report JSON
    — a nested Header/Columns/Rows structure."""
    return _request("GET", f"/reports/{name}", params=params or {})


def company_info() -> dict:
    """Return the CompanyInfo entity for the connected realm —
    useful to confirm the connection is live and show the company
    name in the UI."""
    row = _connection_or_raise()
    realm_id = row.get("realm_id") or ""
    body = _request("GET", f"/companyinfo/{realm_id}")
    return (body.get("CompanyInfo")
            or body.get("QueryResponse", {}).get("CompanyInfo")
            or {})


def cashflow_report(start_date: str, end_date: str) -> dict:
    """Convenience wrapper: the QBO Statement of Cash Flows for a
    date range. Dates are 'YYYY-MM-DD' strings."""
    return report("CashFlow", params={
        "start_date": start_date,
        "end_date": end_date,
    })


def profit_and_loss(start_date: str, end_date: str) -> dict:
    """Convenience wrapper: the QBO Profit & Loss report for a
    date range. Dates are 'YYYY-MM-DD' strings."""
    return report("ProfitAndLoss", params={
        "start_date": start_date,
        "end_date": end_date,
    })


# ---------------------------------------------------------------------------
# Paginated query + Cashflow-page convenience wrappers (v2.67.219)
# ---------------------------------------------------------------------------
# QBO's query endpoint returns at most 1000 rows per call; large
# result sets need STARTPOSITION paging. query() above is fine for
# small/bounded queries; query_all() pages through everything.
_QBO_PAGE_SIZE = 1000


def query_all(select_from_where: str) -> list[dict]:
    """Run a QBO query and page through ALL results.

    Pass the query WITHOUT a STARTPOSITION/MAXRESULTS clause —
    e.g. "SELECT * FROM Bill WHERE TxnDate >= '2026-01-01'".
    query_all appends paging itself. Returns the combined list."""
    out: list[dict] = []
    start = 1  # QBO STARTPOSITION is 1-based.
    while True:
        page_sql = (f"{select_from_where} STARTPOSITION {start} "
                    f"MAXRESULTS {_QBO_PAGE_SIZE}")
        rows = query(page_sql)
        out.extend(rows)
        if len(rows) < _QBO_PAGE_SIZE:
            break
        start += _QBO_PAGE_SIZE
        if start > 100_000:  # hard safety stop
            log.warning("query_all hit the 100k safety cap.")
            break
    return out


def _qbo_escape(value: str) -> str:
    """Escape a string literal for embedding in a QBO query
    (single quotes are doubled)."""
    return str(value).replace("'", "''")


def get_bills(since_date: Optional[str] = None,
              only_unpaid: bool = False) -> list[dict]:
    """Fetch supplier bills (accounts payable) from QBO.

    - since_date: 'YYYY-MM-DD' lower bound on TxnDate. Defaults to
      no bound (caller usually passes ~6 months back).
    - only_unpaid: if True, only bills with an outstanding balance.

    Each Bill dict includes Id, DocNumber, TxnDate, DueDate,
    TotalAmt, Balance, VendorRef {value,name}, CurrencyRef. A
    Balance of 0 means the bill is fully paid."""
    clauses: list[str] = []
    if only_unpaid:
        clauses.append("Balance > '0'")
    if since_date:
        clauses.append(f"TxnDate >= '{_qbo_escape(since_date)}'")
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return query_all(f"SELECT * FROM Bill{where} ORDERBY TxnDate")


def get_bank_accounts() -> list[dict]:
    """Fetch QBO Bank-type accounts with their current balances —
    powers the bank-balance rows on the Cashflow page. Each dict
    includes Id, Name, CurrentBalance, AccountSubType."""
    return query_all(
        "SELECT * FROM Account WHERE AccountType = 'Bank'")


def get_vendors() -> list[dict]:
    """Fetch QBO vendors (suppliers). Useful for mapping/display;
    each dict includes Id, DisplayName, Active."""
    return query_all("SELECT * FROM Vendor")


def get_credit_card_accounts() -> list[dict]:
    """Fetch QBO Credit-Card-type accounts with their balances —
    powers the credit-card payment rows on the Cashflow page.
    Each dict includes Id, Name, AcctNum (the chart-of-accounts
    number), CurrentBalance. For a credit card CurrentBalance is
    the amount owed."""
    return query_all(
        "SELECT * FROM Account "
        "WHERE AccountType = 'Credit Card'")


def _walk_report_rows(node, out: list) -> None:
    """Recursively collect (name, account_id, amount) tuples from
    the nested Header/Rows structure of a QBO report."""
    if isinstance(node, dict):
        coldata = node.get("ColData")
        if isinstance(coldata, list) and coldata:
            name = (coldata[0] or {}).get("value") or ""
            acct_id = (coldata[0] or {}).get("id") or ""
            amount = None
            # The balance is the last non-empty numeric cell.
            for cell in reversed(coldata):
                val = (cell or {}).get("value")
                if val in (None, ""):
                    continue
                try:
                    amount = float(str(val).replace(",", ""))
                    break
                except ValueError:
                    continue
            if name:
                out.append((name, str(acct_id), amount))
        for key in ("Rows", "Row"):
            sub = node.get(key)
            if isinstance(sub, (dict, list)):
                _walk_report_rows(sub, out)
    elif isinstance(node, list):
        for item in node:
            _walk_report_rows(item, out)


def account_balance_as_of(as_of_date: str,
                          account_id: Optional[str] = None,
                          account_name: Optional[str] = None
                          ) -> Optional[float]:
    """Return an account's balance as of `as_of_date`
    ('YYYY-MM-DD') by reading the QBO Balance Sheet report. Match
    by account_id (preferred) or account_name. Returns None if
    the account is not found in the report."""
    body = report("BalanceSheet", params={"end_date": as_of_date})
    rows: list = []
    _walk_report_rows(body.get("Rows") or {}, rows)
    for name, aid, amount in rows:
        if account_id and aid and aid == str(account_id):
            return amount
        if (account_name and name
                and name.strip().lower()
                == account_name.strip().lower()):
            return amount
    return None
