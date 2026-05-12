"""viktor_bridge.py (v2.67.126)
==================================

Shared logic for forwarding marketing questions to Viktor
(getviktor.com) and composing our engine-signal overlay on top of
its answers. Used by:

  - slack_listener.py — when a marketing question is asked in
    Slack, our bot redirects the user to ask @Viktor manually
    (Slack apps filter bot-to-bot mentions). Once Viktor replies,
    our bot adds an overlay.

  - app.py (Streamlit AI Assistant) — when a marketing question
    is asked in the dashboard, IF the user has authorised the
    dashboard via Slack OAuth, we post to Slack on their behalf
    (so Viktor sees a real human), poll for Viktor's reply, and
    render Viktor's answer + our overlay inline in the dashboard.

Why a shared module
-------------------
The two surfaces (Slack bot vs. dashboard) had drifted: each had
its own copy of the marketing-keyword regex and its own overlay
formatter. v2.67.126 consolidates so a tweak (e.g. adding "DSP"
or "TikTok ads" to the keyword set) only happens in one place.

Public API
----------
- is_marketing_question(text) -> bool
- compose_overlay(reply_text) -> (text, tools_used)
- forward_via_dashboard(user_id, question, channel_id) -> session_id
- get_session_status(session_id) -> dict | None
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from typing import List, Optional, Tuple

import db

log = logging.getLogger("viktor_bridge")


# ---------------------------------------------------------------------------
# Marketing-question detection
# ---------------------------------------------------------------------------
_MARKETING_RE = re.compile(
    r"\b("
    r"roas|roi|cpc|cpm|ctr|cpa|"
    r"ad\s*spend|ad\s*spends|ad\s*budget|ad\s*revenue|"
    r"google\s*ads?|meta\s*ads?|facebook\s*ads?|"
    r"instagram\s*ads?|tiktok\s*ads?|youtube\s*ads?|"
    r"shopping\s*(?:ads?|campaign|feed)|"
    r"performance\s*max|pmax|p-max|"
    r"klaviyo|email\s*campaign|email\s*flow|"
    r"campaign\s*performance|"
    r"attribution|conversion\s*(?:value|rate)|"
    r"impressions|"
    r"merchant\s*center|free\s*listings|"
    r"organic\s*traffic|paid\s*traffic|"
    r"bidnamic|"
    r"semrush|seo\s*rank|keyword\s*rank|"
    r"reviews\.?io|review\s*rating"
    r")\b",
    re.IGNORECASE,
)

_STRONG_OPS_RE = re.compile(
    r"\b("
    r"PO-?\d+|SO-?\d+|INV-?\d+|"
    r"bin\s*location|stock\s*on\s*hand|onhand|"
    r"reorder|backorder|"
    r"supplier|vendor|"
    r"slow.?mover|dead\s*stock|excess|dormant|abc\s*class"
    r")\b",
    re.IGNORECASE,
)


def is_marketing_question(text: str) -> bool:
    """Return True if the text reads as a marketing/ads question
    that Viktor would handle better than us. Conservative — we
    only forward when confident."""
    if not text:
        return False
    if not _MARKETING_RE.search(text):
        return False
    if _STRONG_OPS_RE.search(text):
        return False
    return True


# ---------------------------------------------------------------------------
# Overlay composition (engine signals Viktor doesn't see)
# ---------------------------------------------------------------------------
_SKU_RE = re.compile(r"\b(LED(?:KIT)?-[A-Z0-9-]+)\b", re.IGNORECASE)
_FAMILY_NAME_RE = re.compile(
    r"\b("
    r"slim8|sierra38|sierra65|oslo|nicho|trimless|"
    r"white\s*iris|white\s*lily|elite\s*gold|honey\s*suckle|"
    r"cardinal\s*flower|liatris|baltic\s*ivy|sauna\s*pro|"
    r"glow67|king\s*protea|decor|hannover|enoled|"
    r"vario30|begtin12|topmet|"
    r"slim10|romano|kentucky|tokyo|new\s*york|monorail"
    r")\b",
    re.IGNORECASE,
)


def compose_overlay(reply_text: str) -> Tuple[str, List[str]]:
    """Build an ops-context overlay on top of Viktor's marketing
    answer. Extract SKUs/families mentioned in Viktor's reply and
    surface our engine signals.

    Returns (overlay_text, tools_used). overlay_text is empty
    string if we have nothing useful to add (Viktor mentioned no
    SKUs/families we recognise) — caller should skip rendering it
    in that case."""
    text = reply_text or ""
    skus = _SKU_RE.findall(text)
    families = _FAMILY_NAME_RE.findall(text)
    tools_used: List[str] = []

    if not (skus or families):
        return "", tools_used

    # Deduplicate, preserve order.
    seen_sku = set()
    sku_list = []
    for s in skus:
        u = s.upper()
        if u not in seen_sku:
            seen_sku.add(u)
            sku_list.append(u)
    seen_fam = set()
    fam_list = []
    for f in families:
        fl = re.sub(r"\s+", "", f.lower())
        if fl not in seen_fam:
            seen_fam.add(fl)
            fam_list.append(f.strip())

    try:
        from bot_engine_lookup import (
            lookup_sku_signals, lookup_family_signals)
    except ImportError:
        return (
            "_Ops overlay (engine signals not wired in this "
            "environment — check the SKUs Viktor mentioned in the "
            "Ordering page for ABC class + stock context.)_"
        ), tools_used

    lines: List[str] = []
    for sku in sku_list[:10]:
        try:
            sig = lookup_sku_signals(sku)
            tools_used.append("lookup_sku_signals")
        except Exception:
            continue
        if not sig:
            continue
        parts = []
        if sig.get("abc"):
            parts.append(f"{sig['abc']}-class")
        if sig.get("trend_flag"):
            parts.append(sig["trend_flag"])
        if sig.get("is_dormant"):
            parts.append("dormant")
        if sig.get("stock") is not None:
            parts.append(f"{int(sig['stock'])} on hand")
        if sig.get("bin"):
            parts.append(f"Bin {sig['bin']}")
        if parts:
            lines.append(f"• `{sku}` — {' · '.join(parts)}")

    for fam in fam_list[:5]:
        try:
            fsig = lookup_family_signals(fam)
            tools_used.append("lookup_family_signals")
        except Exception:
            continue
        if not fsig:
            continue
        summary_bits = []
        if fsig.get("n_a_class") is not None:
            summary_bits.append(
                f"{fsig['n_a_class']} A-class")
        if fsig.get("n_b_class") is not None:
            summary_bits.append(
                f"{fsig['n_b_class']} B-class")
        if fsig.get("n_dormant"):
            summary_bits.append(
                f"{fsig['n_dormant']} dormant")
        if summary_bits:
            lines.append(
                f"• Family `{fam}` — {' · '.join(summary_bits)}")

    if not lines:
        return "", tools_used
    return (
        "_Ops overlay (engine signals Viktor doesn't see):_\n"
        + "\n".join(lines)
    ), tools_used


# ---------------------------------------------------------------------------
# Forwarding from the dashboard (OAuth-impersonated post)
# ---------------------------------------------------------------------------
def _viktor_bridge_channel() -> Optional[str]:
    """Slack channel where the dashboard posts on the user's
    behalf. Defaults to the configured Viktor channel if set,
    otherwise the first channel from SLACK_AUTONOMOUS_CHANNELS."""
    explicit = os.environ.get(
        "VIKTOR_BRIDGE_CHANNEL_ID", "").strip()
    if explicit:
        return explicit
    autonomous_raw = os.environ.get(
        "SLACK_AUTONOMOUS_CHANNELS", "").strip()
    if autonomous_raw:
        first = autonomous_raw.split(",")[0].strip()
        if first:
            return first
    return None


def _viktor_slack_user_id() -> str:
    """The Viktor app's Slack user_id (U-prefix). Required for the
    overlay polling to know whose reply to wait for."""
    return os.environ.get("VIKTOR_SLACK_USER_ID", "").strip()


def forward_via_dashboard(user_id: int, question: str
                              ) -> Optional[str]:
    """Forward a marketing question to Viktor via the dashboard
    user's stored OAuth token. Returns a session_id the caller
    can pass to `poll_for_viktor_reply` while waiting.

    Returns None if forwarding can't be done (no token, no
    channel configured, etc.)."""
    channel = _viktor_bridge_channel()
    if not channel:
        log.warning(
            "VIKTOR_BRIDGE_CHANNEL_ID not set and no "
            "SLACK_AUTONOMOUS_CHANNELS fallback")
        return None
    try:
        import slack_oauth
    except ImportError:
        log.error("slack_oauth module not importable")
        return None
    posted_ts, thread_ts = slack_oauth.post_as_user(
        user_id, channel, question)
    if not posted_ts:
        return None
    session_id = uuid.uuid4().hex
    try:
        db.create_viktor_bridge_session(
            session_id=session_id,
            user_id=user_id,
            question=question,
            channel_id=channel,
        )
        db.update_viktor_bridge_post(
            session_id=session_id,
            posted_ts=posted_ts,
            thread_ts=thread_ts or posted_ts,
        )
    except Exception as exc:
        log.error("Failed to record viktor bridge session: %s", exc)
        return None
    return session_id


def poll_for_viktor_reply(session_id: str) -> Optional[dict]:
    """Check if Viktor has replied to this bridge session yet.
    Returns:
      - None while waiting
      - dict with keys reply_text, reply_ts, overlay_text once
        Viktor has replied AND we've composed the overlay

    Callers (e.g. the Streamlit AI Assistant) loop on this every
    1-2s with a timeout."""
    viktor_uid = _viktor_slack_user_id()
    if not viktor_uid:
        log.warning("VIKTOR_SLACK_USER_ID not set — can't poll")
        return None
    reply = db.poll_viktor_bridge_reply(session_id, viktor_uid)
    if not reply:
        return None
    overlay_text, tools_used = compose_overlay(reply["text"])
    try:
        db.complete_viktor_bridge_session(
            session_id=session_id,
            viktor_reply_ts=reply["ts"],
            viktor_reply_text=reply["text"],
            overlay_text=overlay_text or None,
        )
    except Exception as exc:
        log.error("Failed to complete bridge session: %s", exc)
    return {
        "reply_text": reply["text"],
        "reply_ts": reply["ts"],
        "overlay_text": overlay_text,
        "tools_used": tools_used,
    }
