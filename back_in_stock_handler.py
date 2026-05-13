"""back_in_stock_handler.py (v2.67.136)
=========================================

Handle FlowBot back-in-stock subscription notifications.

When a customer requests notification for an out-of-stock product on
the storefront, Shopify's FlowBot integration posts a message to the
#back-in-stock Slack channel:

    Back-in-stock subscription
    Customer info@example.com subscribed to back-in-stock
    notifications for Recessed Wall Wash LED Lighting Channel ~
    Model Acapulco | White / 2m (78").

These are pure demand signals — explicit customer demand for a
specific product that's out of stock right now. Every one is worth
capturing into our `demand_signals` table AND replying with triage
context so staff can action them immediately:
  - Current stock status (OnHand / Available / Bin / ABC class)
  - Incoming PO ETA if any
  - Suggested action (notify customer when PO lands, or offer
    in-stock alternative)

This module is called by slack_listener when it classifies an
incoming message as `back_in_stock_subscription`.

Public API
----------
- is_flowbot_subscription(msg)  -> bool
- handle_subscription(session, msg) -> (reply_text, tools_used)
"""

from __future__ import annotations

import logging
import re
from typing import List, Optional, Tuple

import pandas as pd

import db

log = logging.getLogger("back_in_stock_handler")


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------
_SUBSCRIPTION_PHRASES = (
    "back-in-stock subscription",
    "back-in-stock notifications",
    "subscribed to back-in-stock",
    "back in stock notification",   # tolerate spelling variants
)


def is_flowbot_subscription(msg: dict) -> bool:
    """Return True if this looks like a back-in-stock subscription
    notification. The phrase pattern is specific enough that we
    don't need to gate on is_bot or user_name — humans testing
    the integration with a paste-test should also trigger the
    handler so we know the path works end-to-end.

    v2.67.137 — removed is_bot gate (was blocking manual tests).
    """
    text = (msg.get("text") or "").lower()
    if not text:
        return False
    return any(p in text for p in _SUBSCRIPTION_PHRASES)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
# Customer email — robust to Slack's auto-link wrapping
# (<mailto:x@y|x@y> or <x@y|x@y>). The capture is just the address.
_EMAIL_RE = re.compile(
    r"(?:Customer\s+)?"
    r"(?:<[^|>]*\|)?"               # optional <link prefix
    r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})"
    r"(?:\|[^>]*)?>?",
    re.IGNORECASE,
)

# Product link — Slack renders hyperlinks as <URL|display text>.
# When FlowBot posts the product title with a hyperlink, the message
# text contains a block like <https://wired4signsusa.com/products/
# acapulco|Recessed Wall Wash LED Channel ...>. We extract both
# pieces.
_PRODUCT_LINK_RE = re.compile(
    r"<(https?://[^|>]*?/products/[^|>]+)\|([^>]+)>",
    re.IGNORECASE,
)

# v2.67.139 — Plain URL fallback. Manual paste-tests and some
# notification sources omit the Slack hyperlink wrapper. Without
# this fallback, the handle never got extracted and SKU resolution
# silently produced 'unknown product'.
_PRODUCT_URL_PLAIN_RE = re.compile(
    r"https?://[^\s<>|]*?/products/[^\s<>|?#]+",
    re.IGNORECASE,
)

# Shopify handle from a product URL: ...wired4signsusa.com/products/
# slim-led-channel-slim8-ac2-z?...  -> 'slim-led-channel-slim8-ac2-z'
_HANDLE_FROM_URL_RE = re.compile(
    r"/products/([^/?#]+)", re.IGNORECASE)


def parse_subscription(text: str) -> dict:
    """Pull customer email + product URL + product title out of a
    FlowBot subscription message. Returns a dict with whichever
    fields are present.

    Sample input:
      "Back-in-stock subscription\\n"
      "Customer <mailto:info@x.com|info@x.com> subscribed to "
      "back-in-stock notifications for "
      "<https://wired4signsusa.com/products/acapulco|"
      "Recessed Wall Wash LED Lighting Channel ~ Model Acapulco "
      "| White / 2m (78\")>."
    """
    out: dict = {}
    if not text:
        return out
    # Email — pick the FIRST email after "Customer"
    em = _EMAIL_RE.search(text)
    if em:
        out["customer_email"] = em.group(1).strip()
    # Product URL + title from <URL|title> block. There may be
    # multiple if the message bundles several subscriptions —
    # pick all, return the first as primary.
    matches = _PRODUCT_LINK_RE.findall(text)
    if matches:
        out["product_url"] = matches[0][0].strip()
        out["product_title"] = (
            matches[0][1].replace(" ", " ").strip())
        if len(matches) > 1:
            out["extra_products"] = [
                {"url": m[0], "title": m[1].strip()}
                for m in matches[1:]
            ]
    else:
        # v2.67.139 — fallback to plain URL (no Slack hyperlink
        # wrapper). Manual paste-tests and some integrations
        # post raw URLs without the <URL|title> markup.
        plain = _PRODUCT_URL_PLAIN_RE.search(text)
        if plain:
            out["product_url"] = plain.group(0).strip()
    # Handle from URL.
    if out.get("product_url"):
        hm = _HANDLE_FROM_URL_RE.search(out["product_url"])
        if hm:
            out["shopify_handle"] = hm.group(1).strip()
    return out


# ---------------------------------------------------------------------------
# SKU resolution
# ---------------------------------------------------------------------------
def _resolve_sku_from_handle(handle: str) -> Optional[dict]:
    """Map a Shopify handle to {sku, family, title}. Tries two
    sources in order:

      1. product_dimensions table (CIN7 sync side; per-product
         metadata with shopify_handle column)
      2. v2.67.139 — Shopify .md product files (storefront source
         of truth; one .md per handle, with parsed title / family
         / variant SKUs)

    Returns None only if NEITHER source has a match — at that
    point the handle truly isn't in our catalog."""
    if not handle:
        return None
    handle_norm = handle.strip()

    # Source 1: product_dimensions
    try:
        rows = db.all_product_dimensions()
        for r in rows:
            if (r.get("shopify_handle") or "").strip() == handle_norm:
                return {
                    "sku": r.get("sku") or "",
                    "family": r.get("family") or "",
                    "title": r.get("title") or "",
                    "shopify_handle": handle_norm,
                }
    except Exception:
        pass

    # Source 2: Shopify .md (filename = handle)
    try:
        from product_search import (
            SHOPIFY_PRODUCTS_DIR, _parse_shopify_product_md)
        md_path = SHOPIFY_PRODUCTS_DIR / f"{handle_norm}.md"
        if md_path.exists():
            sp = _parse_shopify_product_md(md_path)
            if sp is not None:
                # ShopifyProduct.skus is a list (variants). For
                # demand-signal purposes the family + title is
                # what matters; we leave sku empty when there are
                # multiple variants since we don't know which the
                # customer chose.
                primary_sku = (sp.skus[0]
                                  if (sp.skus
                                        and len(sp.skus) == 1)
                                  else "")
                return {
                    "sku": primary_sku,
                    "family": sp.family or "",
                    "title": sp.title or "",
                    "shopify_handle": handle_norm,
                    "n_variants": len(sp.skus or []),
                }
    except Exception:
        pass
    return None


def _resolve_sku(parsed: dict) -> dict:
    """Layered SKU resolution:
      1. Shopify handle from URL -> product_dimensions
      2. Product title contains a known family name -> family match
      3. Fall back to title text only (signal still useful even
         without a precise SKU)
    """
    if not parsed:
        return {}
    # Path 1: handle
    info = _resolve_sku_from_handle(parsed.get("shopify_handle") or "")
    if info:
        return info
    # Path 2: family detection from title. Use the same FAMILY_NAME_RE
    # the slack listener uses so detection is consistent.
    title = (parsed.get("product_title") or "")
    try:
        from slack_listener import FAMILY_NAME_RE
        m = FAMILY_NAME_RE.search(title)
        if m:
            return {
                "sku": "",
                "family": m.group(1).strip(),
                "title": title,
                "shopify_handle": parsed.get("shopify_handle") or "",
            }
    except Exception:
        pass
    # Path 3: title only
    return {
        "sku": "",
        "family": "",
        "title": title,
        "shopify_handle": parsed.get("shopify_handle") or "",
    }


# ---------------------------------------------------------------------------
# Triage reply composition
# ---------------------------------------------------------------------------
def _stock_status_for_sku(sku: str) -> Optional[dict]:
    """Look up current stock signals (OnHand / Available / Bin /
    ABC / trend) for the SKU via bot_engine_lookup. Returns None
    if not resolvable."""
    if not sku:
        return None
    try:
        from bot_engine_lookup import lookup_sku_signals
        return lookup_sku_signals(sku)
    except Exception:
        return None


def _incoming_po_for_sku(sku: str) -> Optional[dict]:
    """Find the next expected PO line for a SKU. Reads the latest
    purchase_lines CSV and returns the soonest open delivery."""
    if not sku:
        return None
    try:
        # Reuse the helper from po_dispatch_reminder for consistency.
        from po_dispatch_reminder import _load_purchases_and_lines
        purchases, lines = _load_purchases_and_lines()
        if lines is None:
            return None
        if "SKU" not in lines.columns:
            return None
        match = lines[lines["SKU"].astype(str).str.upper()
                          == sku.upper()]
        if match.empty:
            return None
        # Pick a date column.
        date_col = None
        for cand in ("RequiredBy", "ExpectedDate",
                       "DeliveryDate", "RequiredDate"):
            if cand in match.columns:
                date_col = cand
                break
        # Filter to open lines only.
        if "Status" in match.columns:
            closed = ("RECEIVED", "CLOSED", "COMPLETED",
                        "CANCELLED", "VOIDED", "DRAFT")
            status_u = (match["Status"].fillna("").astype(str)
                          .str.upper())
            keep = ~status_u.apply(
                lambda s: any(k in s for k in closed))
            match = match[keep]
        if match.empty:
            return None
        if date_col:
            match = match.copy()
            match["__d"] = pd.to_datetime(
                match[date_col], errors="coerce")
            match = match.sort_values("__d", na_position="last")
        row = match.iloc[0]
        return {
            "po_number": row.get("OrderNumber"),
            "quantity": row.get("Quantity"),
            "supplier": row.get("Supplier"),
            "expected": (str(row.get(date_col))[:10]
                          if date_col and pd.notna(row.get(date_col))
                          else None),
        }
    except Exception as exc:
        log.warning("incoming PO lookup failed for %s: %s",
                      sku, exc)
        return None


def _high_intent_signals(customer_email: str) -> dict:
    """Has this customer subscribed for multiple products lately?
    Multiple subscriptions in a short window = high purchase intent."""
    if not customer_email:
        return {"n_30d": 0}
    try:
        with db.connect() as c:
            r = c.execute(
                "SELECT COUNT(*) AS n FROM demand_signals "
                "WHERE source = 'slack' "
                "  AND signal_type = 'notify_me' "
                "  AND customer_name = ? "
                "  AND created_at >= datetime('now', '-30 days')",
                (customer_email,)).fetchone()
        return {"n_30d": int(r["n"] or 0)}
    except Exception:
        return {"n_30d": 0}


def compose_triage_reply(parsed: dict, sku_info: dict) -> str:
    """Build a Slack mrkdwn-formatted threaded reply with stock
    status, incoming PO ETA, customer-intent signals, and
    suggested action. All sections are best-effort — if we can't
    resolve the SKU we still post a helpful 'logged for buyer'
    reply rather than failing silently."""
    email = parsed.get("customer_email") or ""
    title = parsed.get("product_title") or "(unknown product)"
    sku = (sku_info or {}).get("sku") or ""
    family = (sku_info or {}).get("family") or ""

    lines: List[str] = []
    # Header — what / who.
    header_bits = [f"📥 *Demand signal logged*"]
    if family:
        header_bits.append(f"· `{family}`")
    elif title:
        header_bits.append(f"· {title[:60]}")
    lines.append(" ".join(header_bits))
    lines.append("")
    lines.append(f"• Customer: `{email}`")

    # Stock status (best effort).
    sig = _stock_status_for_sku(sku) if sku else None
    if sig:
        parts = []
        if sig.get("stock") is not None:
            parts.append(f"OnHand {int(sig['stock'])}")
        if sig.get("bin"):
            parts.append(f"Bin {sig['bin']}")
        if sig.get("abc"):
            parts.append(f"{sig['abc']}-class")
        if sig.get("trend_flag"):
            parts.append(sig["trend_flag"])
        if sig.get("is_dormant"):
            parts.append("dormant")
        if parts:
            lines.append(f"• Stock: {' · '.join(parts)}")
    else:
        lines.append(
            "• Stock: _can't resolve to a specific SKU — buyer "
            "to look up manually_")

    # Incoming PO (best effort).
    po = _incoming_po_for_sku(sku) if sku else None
    if po:
        po_bits = []
        if po.get("po_number"):
            po_bits.append(f"PO-{po['po_number']}")
        if po.get("supplier"):
            po_bits.append(po["supplier"])
        if po.get("quantity") is not None:
            try:
                po_bits.append(
                    f"{int(po['quantity'])} units incoming")
            except Exception:
                pass
        if po.get("expected"):
            po_bits.append(f"ETA {po['expected']}")
        if po_bits:
            lines.append(f"• Incoming: {' · '.join(po_bits)}")

    # High-intent flag if this customer has multiple recent
    # subscriptions.
    hi = _high_intent_signals(email)
    if hi.get("n_30d", 0) >= 2:
        lines.append(
            f"• 🔥 *High-intent customer* — "
            f"{hi['n_30d']} back-in-stock subscriptions in "
            f"last 30 days")

    # Suggested action.
    lines.append("")
    if po and po.get("expected"):
        lines.append(
            f"_Action: notify {email} when PO lands "
            f"({po['expected']})._")
    elif sig and sig.get("stock") and float(sig["stock"]) > 0:
        lines.append(
            "_Action: stock IS on hand — confirm this notification "
            "didn't fire by mistake (variant mismatch?)._")
    else:
        lines.append(
            "_Action: no incoming PO visible. Buyer to assess "
            "whether to reorder or offer an alternative._")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Top-level handler (called from slack_listener.process_once)
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Arrival notifications (v2.67.140) — closing the loop
# ---------------------------------------------------------------------------
# When a PO is received in CIN7, scan demand_signals for pending
# 'notify_me' rows whose SKU/family matches any line of the
# received PO. Post a summary in #back-in-stock listing the
# waiting customers so staff can reach out.
def _format_age(created_at_iso: str) -> str:
    """Pretty-print 'subscribed 22 days ago' from an ISO ts."""
    if not created_at_iso:
        return ""
    try:
        import datetime as _dt
        ts = _dt.datetime.fromisoformat(
            created_at_iso.replace("Z", "+00:00"))
        # SQLite datetime() returns naive UTC; normalise.
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=_dt.timezone.utc)
        now = _dt.datetime.now(_dt.timezone.utc)
        days = (now - ts).days
        if days <= 0:
            hours = int((now - ts).total_seconds() / 3600)
            return f"{hours}h ago" if hours > 0 else "just now"
        return f"{days} day{'s' if days != 1 else ''} ago"
    except Exception:
        return ""


def _channel_from_source_ref(source_ref: str) -> Optional[str]:
    """Extract the Slack channel_id from a demand_signal's
    source_ref. We persisted these as 'channel:CXXX/ts:NNN.MMM' so
    the arrival notification can be posted back in the same
    channel that captured the subscription."""
    if not source_ref or not source_ref.startswith("channel:"):
        return None
    try:
        # Format: "channel:C09A29STYDU/ts:1234567890.123456"
        body = source_ref[len("channel:"):]
        return body.split("/", 1)[0] or None
    except Exception:
        return None


def _po_lines_received_recently(lookback_hours: int = 48
                                       ) -> list:
    """Return a list of {po_number, sku, family, supplier, qty}
    for lines on POs that transitioned to RECEIVED recently.
    Reuses po_dispatch_reminder's CSV loading + status filtering
    so behaviour stays consistent."""
    try:
        from po_dispatch_reminder import (
            _load_purchases_and_lines,
            _newly_received_pos)
    except Exception:
        return []
    purchases, lines = _load_purchases_and_lines()
    if purchases is None or lines is None:
        return []
    days = max(1, int(lookback_hours / 24) + 1)
    eligible = _newly_received_pos(purchases, lookback_days=days)
    if eligible.empty:
        return []
    if "OrderNumber" not in lines.columns:
        return []
    # Build family index from product_dimensions for SKU → family
    # lookup so we can match family-only demand signals.
    try:
        pd_rows = db.all_product_dimensions()
    except Exception:
        pd_rows = []
    family_by_sku: dict = {}
    for r in pd_rows:
        sku = (r.get("sku") or "").strip().upper()
        fam = (r.get("family") or "").strip()
        if sku and fam:
            family_by_sku[sku] = fam

    out: List[dict] = []
    lines_by_po = {po: g for po, g in lines.groupby("OrderNumber")}
    for _, po in eligible.iterrows():
        po_number = str(po.get("OrderNumber") or "").strip()
        if not po_number:
            continue
        supplier = po.get("Supplier")
        po_lines = lines_by_po.get(po_number)
        if po_lines is None or po_lines.empty:
            continue
        for _, line in po_lines.iterrows():
            sku = str(line.get("SKU") or "").strip().upper()
            if not sku:
                continue
            out.append({
                "po_number": po_number,
                "sku": sku,
                "family": family_by_sku.get(sku, ""),
                "supplier": supplier,
                "quantity": line.get("Quantity"),
            })
    return out


def _compose_arrival_message(po_number: str,
                                   family_or_sku: str,
                                   supplier: Optional[str],
                                   total_qty: Optional[float],
                                   customers: List[dict]) -> str:
    """Build the Slack message body. Customers list is dicts of
    {customer, age_text, signal_id}."""
    header = f"🟢 *Stock arrived for {family_or_sku}*"
    bits = [f"PO-{po_number}" if not po_number.startswith("PO-")
              else po_number]
    if supplier:
        bits.append(str(supplier))
    if total_qty is not None:
        try:
            bits.append(f"{int(total_qty)} units")
        except Exception:
            pass
    header += f"  _({' · '.join(bits)})_"
    lines: List[str] = [header, ""]
    lines.append(
        f"*{len(customers)} customer"
        f"{'s' if len(customers) != 1 else ''} "
        f"{'were' if len(customers) != 1 else 'was'} waiting:*")
    for c in customers:
        email = c.get("customer") or "(unknown)"
        age = c.get("age_text") or ""
        if age:
            lines.append(f"• `{email}` (subscribed {age})")
        else:
            lines.append(f"• `{email}`")
    lines.append("")
    lines.append(
        "_Please notify these customers — their back-in-stock "
        "subscriptions are still pending._")
    return "\n".join(lines)


def _post_to_slack(channel_id: str, text: str
                      ) -> Tuple[Optional[str], Optional[str]]:
    """Reuse the same posting helper used by po_dispatch_reminder
    (bot token, simple chat.postMessage)."""
    try:
        import slack_sync
    except ImportError as exc:
        return None, f"slack_sync import failed: {exc}"
    import os as _os
    token = _os.environ.get("SLACK_BOT_TOKEN", "").strip()
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


def check_arrivals(dryrun: bool = False,
                       lookback_hours: int = 48,
                       max_subscription_age_days: int = 365
                       ) -> dict:
    """For each PO that just transitioned to RECEIVED, find pending
    notify_me demand_signals whose SKU or family matches any line.
    Post a summary listing the waiting customers; idempotent per
    (PO, demand_signal_id) pair."""
    po_lines = _po_lines_received_recently(lookback_hours)
    if not po_lines:
        return {"posted": 0, "received_lines": 0}

    # Group lines by family AND collect all skus/families seen.
    # For each (po_number, family), we'll find one set of waiting
    # customers and post one message — not one per individual line.
    grouped: dict = {}  # key = (po_number, family_or_sku)
    for ln in po_lines:
        po = ln["po_number"]
        fam = (ln["family"] or "").strip()
        sku = (ln["sku"] or "").strip()
        # Group by family when known, fall back to SKU
        key = (po, fam if fam else sku)
        grp = grouped.setdefault(key, {
            "po_number": po,
            "family": fam,
            "skus": set(),
            "supplier": ln.get("supplier"),
            "total_qty": 0.0,
        })
        if sku:
            grp["skus"].add(sku)
        try:
            q = float(ln.get("quantity") or 0)
            grp["total_qty"] += q
        except (TypeError, ValueError):
            pass

    n_posted = 0
    n_customers = 0
    n_already = 0
    n_errors = 0

    for (po_number, family_or_sku), grp in grouped.items():
        skus_list = list(grp["skus"])
        family = grp["family"]
        signals = db.find_pending_back_in_stock_signals(
            skus=skus_list,
            families=[family] if family else None,
            days=max_subscription_age_days,
        )
        if not signals:
            continue
        # Filter out already-notified.
        fresh = [s for s in signals
                  if not db.has_back_in_stock_arrival_notification(
                       po_number, int(s["id"]))]
        if not fresh:
            n_already += len(signals)
            continue
        # Compose customer list.
        customers = []
        for s in fresh:
            customers.append({
                "customer": s.get("customer_name") or "",
                "age_text": _format_age(s.get("created_at") or ""),
                "signal_id": int(s["id"]),
                "source_ref": s.get("source_ref") or "",
            })
        # Decide channel — use the channel from the FIRST
        # customer's original subscription source_ref. Falls back
        # to SLACK_BACK_IN_STOCK_CHANNEL_ID env if not parseable.
        import os as _os
        channel = None
        for c in customers:
            ch = _channel_from_source_ref(c["source_ref"])
            if ch:
                channel = ch
                break
        if not channel:
            channel = _os.environ.get(
                "SLACK_BACK_IN_STOCK_CHANNEL_ID", "").strip() or None
        if not channel:
            log.warning(
                "No channel resolvable for arrival notification "
                "(po=%s family=%s, %d customers) — skipping.",
                po_number, family_or_sku, len(customers))
            continue

        msg = _compose_arrival_message(
            po_number=po_number,
            family_or_sku=family or family_or_sku,
            supplier=grp.get("supplier"),
            total_qty=grp.get("total_qty"),
            customers=customers,
        )
        log.info(
            "Arrival notification for %s/%s — %d customers %s",
            po_number, family_or_sku, len(customers),
            "[DRYRUN]" if dryrun else "")
        if dryrun:
            print(f"\n--- ARRIVAL: {po_number} / "
                    f"{family_or_sku} ---\n{msg}\n")
            continue
        posted_ts, error = _post_to_slack(channel, msg)
        if error:
            log.error(
                "Failed to post arrival for %s/%s: %s",
                po_number, family_or_sku, error)
            for c in customers:
                db.record_back_in_stock_arrival_notification(
                    po_number=po_number,
                    sku=(skus_list[0] if skus_list else None),
                    family=family,
                    demand_signal_id=c["signal_id"],
                    posted_channel=channel,
                    posted_ts=None,
                    error_msg=error,
                )
            n_errors += 1
            continue
        for c in customers:
            db.record_back_in_stock_arrival_notification(
                po_number=po_number,
                sku=(skus_list[0] if skus_list else None),
                family=family,
                demand_signal_id=c["signal_id"],
                posted_channel=channel,
                posted_ts=posted_ts,
            )
        n_posted += 1
        n_customers += len(customers)

    return {
        "received_lines": len(po_lines),
        "groups": len(grouped),
        "posted": n_posted,
        "customers_notified": n_customers,
        "skipped_already_notified": n_already,
        "errors": n_errors,
    }


def handle_subscription(msg: dict) -> Tuple[str, List[str]]:
    """Parse the FlowBot message, write a demand_signals row,
    and return (reply_text, tools_used) for the caller to post
    as a threaded reply.

    Returns ("", []) if parsing fails — caller should skip posting
    rather than reply with a confusing partial answer.
    """
    text = msg.get("text") or ""
    parsed = parse_subscription(text)
    if not parsed:
        return "", []
    sku_info = _resolve_sku(parsed)

    # Persist as demand signal. We always insert one row per
    # PRIMARY subscription (the first product in the message); if
    # the message bundles multiple variants (`extra_products`),
    # insert one signal per extra too so the buyer sees each
    # variant separately.
    primary_handle = parsed.get("shopify_handle")
    customer = parsed.get("customer_email") or ""
    tools_used: List[str] = ["parse_subscription"]
    try:
        db.insert_demand_signal(
            source="slack",
            source_ref=f"channel:{msg.get('channel_id')}/"
                          f"ts:{msg.get('ts')}",
            sku=sku_info.get("sku") or None,
            product_family=sku_info.get("family") or None,
            raw_text=text[:1000],
            signal_type="notify_me",
            quantity=1.0,
            customer_id=None,
            customer_name=customer,
            confidence=0.9 if sku_info.get("sku") else 0.5,
            needs_review=not bool(sku_info.get("sku")),
            note=(f"shopify_handle={primary_handle}; "
                    f"title={parsed.get('product_title')}"),
            created_by="back_in_stock_handler",
        )
        tools_used.append("insert_demand_signal")
    except Exception as exc:
        log.error(
            "Failed to insert demand_signal for "
            "back-in-stock subscription: %s", exc)

    # Extra products (when FlowBot bundles multiple variants).
    for extra in (parsed.get("extra_products") or []):
        e_parsed = {
            "customer_email": customer,
            "product_url": extra.get("url"),
            "product_title": extra.get("title"),
        }
        h = _HANDLE_FROM_URL_RE.search(extra.get("url") or "")
        if h:
            e_parsed["shopify_handle"] = h.group(1)
        e_info = _resolve_sku(e_parsed)
        try:
            db.insert_demand_signal(
                source="slack",
                source_ref=f"channel:{msg.get('channel_id')}/"
                              f"ts:{msg.get('ts')}",
                sku=e_info.get("sku") or None,
                product_family=e_info.get("family") or None,
                raw_text=extra.get("title", "")[:1000],
                signal_type="notify_me",
                quantity=1.0,
                customer_name=customer,
                confidence=0.9 if e_info.get("sku") else 0.5,
                needs_review=not bool(e_info.get("sku")),
                note=(f"shopify_handle={e_parsed.get('shopify_handle')}"
                        f"; title={extra.get('title')}"),
                created_by="back_in_stock_handler",
            )
        except Exception as exc:
            log.error(
                "Failed to insert extra demand_signal: %s", exc)

    reply = compose_triage_reply(parsed, sku_info)
    return reply, tools_used


# ---------------------------------------------------------------------------
# CLI — exposed so slack_loop can fire `check_arrivals` on its
# own cadence rather than embedding python -c calls.
# ---------------------------------------------------------------------------
def _cli_main() -> int:
    import argparse
    parser = argparse.ArgumentParser(
        description="Back-in-stock arrival notifications.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_a = sub.add_parser(
        "check-arrivals",
        help="Scan recent RECEIVED POs for matching pending "
              "demand signals; post arrival reminders to the "
              "channel that captured each subscription.")
    p_a.add_argument("--hours", type=int, default=48)
    p_a.add_argument("--max-age-days", type=int, default=365)
    p_a.add_argument("--dryrun", action="store_true")
    p_a.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        force=True)
    if args.cmd == "check-arrivals":
        result = check_arrivals(
            dryrun=args.dryrun,
            lookback_hours=args.hours,
            max_subscription_age_days=args.max_age_days,
        )
        log.info("DONE: %s", result)
        return 0
    return 1


if __name__ == "__main__":
    import sys
    sys.exit(_cli_main())
