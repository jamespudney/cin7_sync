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
    """Use product_dimensions to map a Shopify handle to a SKU /
    family. Returns {sku, family, title} or None if no match.

    product_dimensions is per-product (not per-variant), so the
    SKU we return is the product-level identifier — the buyer can
    drill down to a specific variant in the dashboard."""
    if not handle:
        return None
    try:
        rows = db.all_product_dimensions()
    except Exception:
        return None
    for r in rows:
        if (r.get("shopify_handle") or "").strip() == handle:
            return {
                "sku": r.get("sku") or "",
                "family": r.get("family") or "",
                "title": r.get("title") or "",
                "shopify_handle": handle,
            }
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
