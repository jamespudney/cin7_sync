"""Slack listener / responder (v2.67.57).

Reads `slack_messages` for unprocessed entries, classifies each one,
and (if appropriate) composes a response via the existing AI tool
chain and posts a threaded reply. Mirrors every post to the audit
channel so admins can review what the bot is saying.

Designed to run alongside slack_sync.py. Recommended orchestration:

    while true:
      python slack_sync.py poll       # ingest new messages → DB
      python slack_listener.py once   # classify + respond
      sleep 60

…wrapped in slack_loop.sh as a Render background worker.

Why two scripts: ingest must always succeed even if the responder
breaks. Response composition is the slow / risky / external-LLM
step; ingest is just SQL writes.

Classification rules (in priority order):
  1. bot_self → skip (never reply to ourselves)
  2. is_bot → skip (don't reply to other bots)
  3. too_old (older than 30 min) → skip (avoid re-spamming on
     redeploys; 30 min is enough lag for the listener to catch up
     after a downtime)
  4. mention (`@` of our bot) → respond as a question
  5. po_review (in #stock-issues-queries channel AND looks like a
     PO submission) → run get_purchase_order-style commentary
  6. question (ends in `?` OR starts with question word) → respond
     using full AI tool chain
  7. trigger (mentions a SO/PO/INV number, SKU pattern, or known
     customer) → offer relevant context
  8. else → chatter, skip

Posting semantics:
  - Always thread-reply (use thread_ts of parent if user is in a
    thread; otherwise thread_ts = message ts)
  - Truncate response to <=3500 chars (Slack hard limit ~4000)
  - Cite tool names / source IDs at end of message
  - Mirror to #ai-audit with full context
  - Self-suppress if we already responded in this thread within
    last 5 minutes (prevents bot loops)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402
import slack_sync  # noqa: E402

LOG_FORMAT = "%(asctime)s  %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("slack_listener")

# How long after a message arrives are we still willing to respond?
# 30 minutes is generous enough to catch up after a redeploy outage
# without flooding old threads.
MAX_AGE_MINUTES = 30

# Don't repost in a thread if we've already replied within this
# many seconds. Prevents loops if our own message somehow gets
# classified as a question.
SELF_SUPPRESS_SECONDS = 300

# Slack response hard cap — Slack accepts up to ~4000 chars.
# We truncate at 3500 to leave headroom for our citation footer.
MAX_RESPONSE_CHARS = 3500

# Question-word starters (lowercased, single token).
QUESTION_WORDS = {
    "what", "where", "when", "why", "how", "who", "which",
    "can", "could", "does", "do", "did", "is", "are", "was",
    "were", "has", "have", "had", "should", "would", "will",
    "may", "might",
    # v2.67.68 — imperative / request phrases sales staff use.
    # Caught two real misses: 'I'm looking for a channel like
    # slim8' and 'show me 2700K stock' — neither ended with '?'
    # and neither started with a traditional question word.
    "i", "i'm", "im", "looking", "need", "want", "show",
    "find", "tell", "give", "list", "search", "check",
    "any", "anyone", "anybody", "got", "gotta",
}

# SKU + transaction-number patterns we recognise as triggers.
SKU_RE = re.compile(r"\b(LED(?:KIT)?-[A-Z0-9-]+)\b", re.IGNORECASE)
SO_RE = re.compile(r"\bSO-\d+\b", re.IGNORECASE)
PO_RE = re.compile(r"\bPO-\d+\b", re.IGNORECASE)
# v2.67.222 — UUID embedded in a CIN7 PurchaseAdvanced URL, e.g.
# .../PurchaseAdvanced#15153eff-6c4a-412a-a74f-4a54d31830d4 (the
# fragment may carry extra ~uuid~tab parts; we want the first).
_PO_UUID_RE = re.compile(
    r"PurchaseAdvanced#([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})")


def _extract_po_uuid(text: str):
    """Return the CIN7 PO UUID from a PurchaseAdvanced URL in
    `text`, or None. Used to make draft-PO commentary deterministic
    — the UUID lookup retrieves DRAFT purchase orders that an
    OrderNumber search filters out."""
    if not text:
        return None
    m = _PO_UUID_RE.search(text)
    return m.group(1) if m else None
INV_RE = re.compile(r"\bINV-\d+\b", re.IGNORECASE)
TRACKING_RE = re.compile(r"\b1Z[A-Z0-9]{16}\b")  # UPS tracking format

# v2.67.68 — known product family / channel names. These don't
# match SKU_RE (no LED- prefix) but are clear product references
# that should fire the listener. Curated from the Wired4Signs
# catalog. Add new families here as they're released.
FAMILY_NAME_RE = re.compile(
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


# ---------------------------------------------------------------------------
# Viktor (getviktor.com) marketing-AI integration (v2.67.124-126)
# ---------------------------------------------------------------------------
# Viktor is a Slack-native marketing-intelligence AI that connects to
# Google Ads, GA4, Klaviyo, Shopify etc. directly. They don't expose
# a public REST API at our $50 tier — instead, Viktor IS a Slack app
# that lives in the same workspace. We integrate by using Slack
# itself as the bus:
#
#   1. User asks a marketing question in a channel both bots can see.
#   2. Our bot detects it's marketing-intent and posts a redirect
#      message telling the user to @-mention Viktor themselves
#      (Slack apps universally filter bot-to-bot mentions; see
#      v2.67.125 for why).
#   3. Viktor replies in the thread (after the user pastes).
#   4. Our slack_sync polls and ingests Viktor's reply.
#   5. Our bot sees the reply, composes an "ops overlay" with engine
#      signals (ABC class, trend_flag, stock, supplier) that Viktor
#      can't see — and posts it as a follow-up in the same thread.
#
# v2.67.126 — marketing-question detection + overlay composition
# moved to viktor_bridge.py so the dashboard (app.py) can share the
# same logic for its OAuth-impersonated forwarding path.
#
# The integration is inactive unless VIKTOR_SLACK_USER_ID is set in
# the worker's environment. Without it, we fall through to the
# existing behaviour (our bot answers itself).

# v2.67.126 — shared detection
from viktor_bridge import is_marketing_question as _is_marketing_question  # noqa: E402


def _viktor_user_id() -> str:
    """Slack user_id (U-prefix) of the Viktor app in our workspace.
    Empty string disables the integration."""
    return os.environ.get("VIKTOR_SLACK_USER_ID", "").strip()


def _viktor_forwarding_enabled(channel_id: str) -> bool:
    """v2.67.124 — gate Viktor forwarding by channel. By default we
    forward in any channel that's in SLACK_AUTONOMOUS_CHANNELS
    (where the bot already responds without @-mention). If
    VIKTOR_FORWARDING_CHANNELS is set, that overrides — only
    forward in the listed channels."""
    if not _viktor_user_id():
        return False
    explicit = os.environ.get(
        "VIKTOR_FORWARDING_CHANNELS", "").strip()
    if explicit:
        allow = {c.strip() for c in explicit.split(",") if c.strip()}
        return channel_id in allow
    # Fallback: autonomous channels.
    autonomous_raw = os.environ.get(
        "SLACK_AUTONOMOUS_CHANNELS", "").strip()
    autonomous = {c.strip() for c in autonomous_raw.split(",")
                    if c.strip()}
    return channel_id in autonomous


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def _is_thread_we_posted_in(channel_id: str,
                                thread_ts: str) -> bool:
    """v2.67.109 — returns True if our bot has previously posted
    in this thread. Used so the bot continues a conversation
    naturally without requiring a re-mention on each reply.

    v2.67.203 — also match the bot's STARTER message. When the
    bot posts a new top-level message (e.g. the PO-dispatch
    escalation, the back-in-stock arrival reminder), Slack sets
    its thread_ts only AFTER the first reply lands. slack_sync's
    initial poll captures the bot's row with thread_ts=NULL, so
    the simple thread_ts=? join missed it. Now we ALSO match on
    ts=thread_ts — that's the bot's own message ts when it
    started the thread. Trevor's reply (thread_ts=parent_ts)
    then matches the parent.ts of the bot's escalation."""
    if not channel_id or not thread_ts:
        return False
    try:
        with db.connect() as c:
            r = c.execute(
                "SELECT 1 FROM slack_messages "
                "WHERE channel_id = ? "
                "  AND (thread_ts = ? OR ts = ?) "
                "  AND is_our_bot = 1 LIMIT 1",
                (channel_id, thread_ts, thread_ts)
            ).fetchone()
        return r is not None
    except Exception:
        return False


def _classify(msg: Dict[str, Any], bot_self_id: str,
                channel_intent: str) -> str:
    """Return classification string. Cheap pattern-match — no LLM
    calls. The composer is what's expensive; this just gates it."""
    if msg["is_our_bot"]:
        return "bot_self"

    # v2.67.188 — PO commentary crosspost. When a PO draft is
    # pasted into the source channel (typically C08KKG0RQCA —
    # the buyer-review channel), analyse it with our engine
    # signals and post the commentary to a SEPARATE channel
    # (C0B42QDKX9Q — #po-commentary). Fires BEFORE the muted-
    # channels check below so we can keep the source channel
    # muted for everything ELSE while letting this specific
    # cross-channel analysis through.
    #
    # v2.67.193 — Trigger expanded from "≥2 SKUs in text" to
    # ALSO fire on:
    #   • PO-NNNN reference (e.g. "Bigdog PO-7159 send")
    #   • CIN7 PurchaseAdvanced URL (link shared from CIN7)
    # Both signal the buyer wants commentary on a specific
    # purchase order, even when no SKUs are pasted inline.
    # The AI's get_purchase_order tool fetches the lines from
    # the local CSV so the commentary still works.
    _po_src = os.environ.get(
        "SLACK_PO_COMMENTARY_SOURCE_CHANNEL_ID", "").strip()
    _po_tgt = os.environ.get(
        "SLACK_PO_COMMENTARY_TARGET_CHANNEL_ID", "").strip()
    if (_po_src and _po_tgt
            and msg.get("channel_id") == _po_src):
        _text = msg.get("text") or ""
        _skus_found = SKU_RE.findall(_text)
        _po_found = PO_RE.search(_text)
        # CIN7 PO URL: https://inventory.dearsystems.com/
        # PurchaseAdvanced#<uuid> (or the older /Purchase/
        # path). Slack may show it with a |display label.
        _cin7_po_url = (
            "dearsystems.com/PurchaseAdvanced" in _text
            or "dearsystems.com/Purchase" in _text)
        if (len(_skus_found) >= 2
                or _po_found is not None
                or _cin7_po_url):
            return "po_commentary_crosspost"

    # v2.67.187 — SLACK_MUTED_CHANNELS hard mute. Comma-separated
    # list of channel IDs where the bot should NEVER respond — not
    # to mentions, not to triggers, not to FlowBot patterns,
    # nothing. Useful when a channel has been re-purposed and we
    # want to silence the bot there without ripping out env vars
    # one handler at a time. Add a channel ID, redeploy, done.
    _muted_raw = os.environ.get(
        "SLACK_MUTED_CHANNELS", "").strip()
    if _muted_raw:
        muted = {c.strip() for c in _muted_raw.split(",")
                    if c.strip()}
        if msg.get("channel_id") in muted:
            return "chatter"  # silent — no handler fires

    # v2.67.137 — back-in-stock subscription detection runs BEFORE
    # the is_bot branch so the pattern fires whether the message
    # came from FlowBot OR a human paste-test in the same channel.
    # The phrase pattern is specific enough that false-positives
    # in normal conversation are vanishingly unlikely.
    try:
        from back_in_stock_handler import is_flowbot_subscription
        if is_flowbot_subscription(msg):
            return "back_in_stock_subscription"
    except Exception:
        pass

    # v2.67.153 — UPS shipment email forwarded into the dropship
    # tracking channel via Slack Email app. Scoped to the
    # configured channel only, fires before the is_bot bot_other
    # short-circuit (the email is posted by Slack's own bot).
    _ds_track_ch = os.environ.get(
        "SLACK_DROPSHIP_TRACKING_CHANNEL_ID", "").strip()
    if (_ds_track_ch
            and msg.get("channel_id") == _ds_track_ch):
        try:
            from dropship_tracking_handler import (
                is_ups_shipment_email)
            if is_ups_shipment_email(msg):
                return "dropship_ups_email"
        except Exception:
            pass

    # v2.67.144 — stock-issues channel queries. Scoped strictly
    # to the configured channel so we don't classify random ops
    # chatter elsewhere as a stock issue. Resolution detection
    # (human reply 'fixed' / 'adjusted') handled in process_once.
    # v2.67.146 — read channel_id directly from msg here; the
    # local variable `channel_id` isn't bound until later in
    # this function. Previous version crashed with
    # UnboundLocalError on every non-back-in-stock message,
    # leaving classification NULL forever.
    _stock_issues_channel = os.environ.get(
        "SLACK_STOCK_ISSUES_CHANNEL_ID", "").strip()
    _msg_channel_id = msg.get("channel_id", "")
    if (_stock_issues_channel
            and _msg_channel_id == _stock_issues_channel
            and not msg.get("is_bot")):
        try:
            from stock_issues_handler import (
                classify_message as _classify_stock_issue,
                maybe_resolve_from_thread_reply)
            # First — does this look like a resolution reply in
            # an open issue's thread? If yes, mark resolved and
            # return a special classification so process_once
            # doesn't double-handle.
            if maybe_resolve_from_thread_reply(msg):
                return "stock_issue_resolution"
            # Otherwise — is it a NEW stock issue raise?
            if _classify_stock_issue(msg.get("text") or ""):
                return "stock_issue_raise"
        except Exception:
            pass

    if msg["is_bot"]:
        return "bot_other"
    text = (msg["text"] or "").strip()
    if not text:
        return "empty"
    # Age gate.
    try:
        ts_float = float(msg["ts"])
        age_minutes = (time.time() - ts_float) / 60.0
        if age_minutes > MAX_AGE_MINUTES:
            return "too_old"
    except Exception:
        pass

    lower = text.lower()
    channel_id = msg.get("channel_id", "")

    # Direct @-mention of our bot is always an answer-required.
    if bot_self_id and f"<@{bot_self_id}>" in text:
        return "mention"

    # v2.67.124 — Viktor handoff. If this is a marketing-intent
    # question in a channel where forwarding is enabled, route to
    # Viktor instead of running our LLM. Cheaper, faster, and
    # benefits from Viktor's deeper marketing-attribution data.
    # Only fires for autonomous (non-@-mentioned) messages — if
    # the user explicitly @s our bot, they want OUR answer.
    if (_viktor_forwarding_enabled(channel_id)
            and _is_marketing_question(text)):
        return "viktor_handoff"

    # v2.67.124 — Viktor overlay. When a new message arrives FROM
    # Viktor's user_id in a thread we previously handed off to it,
    # add an engine-signal overlay (ABC class, stock, supplier)
    # that Viktor can't compute on its own.
    viktor_uid = _viktor_user_id()
    if (viktor_uid and msg.get("user_id") == viktor_uid):
        thread_ts = msg.get("thread_ts")
        if thread_ts and _did_we_forward_to_viktor(
                channel_id, thread_ts):
            return "viktor_overlay"

    # v2.67.109 — users complained the bot was responding too
    # eagerly to channel chatter. New default: respond ONLY when
    # the bot is directly addressed. Three ways to address it:
    #   1. @-mention (handled above)
    #   2. DM to the bot (Slack DM channel IDs start with 'D')
    #   3. Reply in a thread the bot has already posted in
    #
    # Channels where the bot SHOULD continue to autonomously answer
    # questions/triggers can be added to SLACK_AUTONOMOUS_CHANNELS
    # env var (comma-separated channel IDs). Default: empty
    # (= conservative everywhere).
    if channel_id.startswith("D"):
        return "mention"  # DMs always get answered

    thread_ts = msg.get("thread_ts")
    if thread_ts and _is_thread_we_posted_in(channel_id, thread_ts):
        return "mention"  # bot-started thread reply

    autonomous_raw = os.environ.get(
        "SLACK_AUTONOMOUS_CHANNELS", "").strip()
    autonomous = {c.strip() for c in autonomous_raw.split(",")
                    if c.strip()}
    if channel_id not in autonomous:
        # Not @-mentioned, not a DM, not a bot-thread reply, and
        # this channel isn't on the autonomous allowlist. Stay
        # silent.
        return "chatter"

    # PO-review channel-specific: in #stock-issues-queries, a message
    # that contains MULTIPLE SKUs is likely a PO Andrew submitted for
    # review. Trigger a commentary regardless of question form.
    if channel_intent == "po_review":
        skus = SKU_RE.findall(text)
        if len(skus) >= 2:
            return "po_review"

    # v2.67.59 — Returns channel: ANY message mentioning a SKU is a
    # signal worth processing. The whole point of ingesting #returns
    # is to warn the buyer if the returned SKU is on an open PO. So
    # a 1-SKU mention is enough — staff don't ask "questions" about
    # returns, they just log them, and the AI's job is the proactive
    # cross-link.
    if channel_intent == "returns":
        if SKU_RE.search(text) or SO_RE.search(text) or INV_RE.search(text):
            return "returns_warning"

    # v2.67.255 — Shipping channel: any SO/INV reference is a
    # request to investigate that shipment's margin. Brandon
    # flagged the bot was silent on "SO-56629 client paid $43,
    # we paid $151" — the channel was treated as generic chat.
    # Now it auto-fires like orders/returns: SO or INV mention
    # -> classify as shipping_review, prompt the AI to pull the
    # sale + ShipStation cost + compute the gap.
    if channel_intent == "shipping":
        if SO_RE.search(text) or INV_RE.search(text):
            return "shipping_review"

    # v2.67.62 — Orders channel: any SO/INV/customer reference is
    # actionable, especially mentions of cancellations. Like
    # #returns, staff don't ask questions here — they discuss
    # specific orders. Bot's job is to surface context + flag
    # downstream PO impact when something gets cancelled.
    if channel_intent == "orders":
        cancel_kw = any(k in lower for k in
                          ("cancel", "cancelled", "cancellation",
                            "void", "refund", "abort"))
        if SO_RE.search(text) or INV_RE.search(text):
            return ("orders_cancel" if cancel_kw
                     else "orders_summary")
        if cancel_kw:
            # Cancellation discussion without a specific number —
            # bot can prompt for which order, or comment generically.
            return "orders_cancel"

    # Question detection: ends in '?' OR starts with question word.
    stripped = text.rstrip("!.")
    if stripped.endswith("?"):
        return "question"
    first_word = lower.split()[0] if lower.split() else ""
    if first_word.rstrip(",.?:") in QUESTION_WORDS:
        return "question"

    # Trigger detection: contains a transaction number / SKU /
    # tracking number → AI may have useful context to add.
    if (SO_RE.search(text) or PO_RE.search(text)
            or INV_RE.search(text) or TRACKING_RE.search(text)):
        return "trigger"
    if SKU_RE.search(text):
        return "trigger_sku"
    # v2.67.68 — family-name detection (slim8 / sierra38 / etc.)
    # for sales channels. A bare family-name mention WITHOUT
    # explicit question form is still a reasonable trigger in
    # sales / website / stock contexts.
    if (channel_intent in ("sales", "website", "po_review")
            and FAMILY_NAME_RE.search(text)):
        return "trigger_family"

    return "chatter"


def _channel_intent(channel_name: str) -> str:
    """Map channel name to its dominant intent. Used to bias the
    response composer.

    Channel intent map (v2.67.57+, v2.67.59 added returns):
      - po_review     → PO submissions + backorder discussions.
                        AI runs per-SKU commentary using engine
                        signals + open-PO checks.
                        Hits: #purchase-backorders (Andrew's PO
                        submissions + sales-order backorder
                        review), #stock-issues-queries (general
                        stock / PO questions).
      - returns       → customer returns. v2.67.59 — when a SKU
                        appears here, AI proactively warns the
                        buyer if that SKU is on an open PO so he
                        doesn't double-order. The whole point of
                        ingesting this channel is the
                        return→purchase warning loop.
                        Hits: #returns.
      - shipping      → freight / fulfilment questions. AI calls
                        get_shipping_details, get_shipping_margin
                        (post-v2.67.55c).
                        Hits: #shipping-issues, #fulfilment.
      - sales         → sale / customer / invoice questions. AI
                        calls get_sale_order, proactively follows
                        up with get_shopify_order on Shopify-
                        channel sales.
                        Hits: #saleschat.
      - website       → product / catalog / accessory questions.
                        AI uses find_products + get_compatible_
                        accessories.
                        Hits: #shopify-website-improvement.
      - general       → fallback for anything else."""
    name = (channel_name or "").lower().strip("#")
    # Returns checked first — 'returns' could otherwise hit on
    # 'sales' if a channel was named 'sales-returns' (it isn't,
    # but defensive ordering matters).
    if "return" in name:
        return "returns"
    # PO review checked before 'orders' so 'purchase-backorders'
    # hits this branch (it contains 'order' as substring).
    if ("purchase" in name or "backorder" in name
            or "stock" in name or "po-" in name or "po_" in name):
        return "po_review"
    # v2.67.62 — orders intent. Channel for order management +
    # cancellations (#w4s-orders). Distinct from #saleschat (which
    # is sales-staff product queries) — this one is workflow:
    # who's cancelling what, what changed on which order, what
    # downstream POs need adjusting.
    if "order" in name:
        return "orders"
    if "shipping" in name or "fulfil" in name:
        return "shipping"
    if "sales" in name:
        return "sales"
    if "shopify" in name or "website" in name:
        return "website"
    return "general"


# ---------------------------------------------------------------------------
# Response composition (calls the AI tool chain)
# ---------------------------------------------------------------------------


def _compose_response(msg: Dict[str, Any],
                        classification: str,
                        channel_intent: str
                        ) -> Tuple[str, List[str]]:
    """Compose a response using Anthropic's Messages API + the
    existing AI tool chain. Returns (text, tool_names_used).

    Why call Anthropic here rather than computing answers in Python:
    the AI Assistant page already does sophisticated tool-orchestration
    (multi-turn tool use, intent routing, freeform text answers); we
    want the SAME quality of response in Slack. So the listener acts
    as a thin Slack-shaped front-end on the same tool chain.

    Returns "" for the text if we decide not to respond (e.g. the AI
    said 'I don't have info on this'). Caller suppresses the post in
    that case.
    """
    try:
        import anthropic
    except ImportError:
        return ("", ["ERROR: anthropic SDK not installed"])
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return ("", ["ERROR: ANTHROPIC_API_KEY not set"])

    # Lazy-load the data the AI tools need. We import app helpers
    # only when actually composing — keeps the listener startup
    # fast for messages that classify as chatter/skip.
    try:
        from data_paths import OUTPUT_DIR  # noqa: F401
        import pandas as pd
        import ai_tools

        # Load data the same way the Streamlit page does. This is
        # expensive (CSV parses) so we cache via a module-level
        # holder pattern.
        engine_df, sale_lines_df = _get_data_for_listener()
        if engine_df is None:
            # v2.67.58 — bootstrapping graceful degrade. Worker's
            # /data is empty on first boot; the wrapper script runs
            # an initial sync before the loop starts, but until that
            # finishes the listener has no CSVs to read. Instead of
            # silently failing, return a friendly message to the
            # user. We post this DIRECTLY (skip the LLM call) since
            # we can't do any AI tool calls without data anyway.
            bootstrap_msg = (
                "_:hourglass: I'm still loading the team's data — "
                "first-boot sync runs ~30 minutes after deploy. "
                "Please ask again in a few minutes._"
            )
            return (bootstrap_msg, ["BOOTSTRAP_IN_PROGRESS"])
    except Exception as exc:  # noqa: BLE001
        return ("", [f"ERROR: data load failed: {exc!r}"])

    # System prompt — Slack-flavoured: shorter, citations-required,
    # acknowledge when no data.
    system = _build_slack_system_prompt(channel_intent)

    # User prompt: prefix with channel context so AI knows it's
    # being asked in Slack (not in the Streamlit UI).
    user_block = (
        f"[Slack channel: {msg.get('channel_name') or '(unknown)'}, "
        f"user: {msg.get('user_name') or '(unknown)'}, "
        f"intent: {channel_intent}, "
        f"classification: {classification}]\n\n"
        f"{msg['text']}"
    )

    # Run the tool loop.
    client = anthropic.Anthropic(api_key=api_key)
    messages = [{"role": "user", "content": user_block}]
    tools_used: List[str] = []
    text_chunks: List[str] = []
    MAX_TURNS = 8  # tighter than the Streamlit page's 14 — keep
                    # Slack responses focused, not exploratory.

    for turn in range(MAX_TURNS):
        try:
            resp = client.messages.create(
                model=os.environ.get(
                    "ANTHROPIC_MODEL_SLACK",
                    "claude-sonnet-4-5"),
                max_tokens=1500,
                system=system,
                tools=ai_tools.TOOL_SCHEMAS,
                messages=messages,
            )
        except Exception as exc:  # noqa: BLE001
            return (f"_(AI error: {exc})_", tools_used + [f"ERR:{exc}"])

        if resp.stop_reason == "end_turn":
            for block in resp.content:
                if hasattr(block, "text"):
                    text_chunks.append(block.text)
            break

        if resp.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": resp.content})
            tool_results = []
            for block in resp.content:
                if hasattr(block, "text") and block.text.strip():
                    text_chunks.append(block.text)
                if getattr(block, "type", "") == "tool_use":
                    name = block.name
                    args = block.input or {}
                    tools_used.append(name)
                    try:
                        result = ai_tools.call_tool(
                            name, engine_df, sale_lines_df, args)
                    except Exception as exc:  # noqa: BLE001
                        result = json.dumps({"error": str(exc)})
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
            messages.append({"role": "user", "content": tool_results})
            continue

        # Unexpected stop reason — bail with whatever we have.
        for block in resp.content:
            if hasattr(block, "text"):
                text_chunks.append(block.text)
        break

    text = "\n\n".join(t.strip() for t in text_chunks if t and t.strip())
    return (text.strip(), tools_used)


def _get_lessons_learned_block() -> str:
    """v2.67.66 — fetch the most recent 'lessons learned' summary
    from bot_lessons_learned and format it for prepending to the
    system prompt. Returns an empty string when no recent summary
    exists (first deployment, or summarizer hasn't run yet).

    Cached for 10 min per process to avoid hitting the DB on every
    compose call. The summary changes at most once per day so this
    is fine."""
    import time as _time
    cache_key = "_lessons_cache"
    cache = globals().setdefault(cache_key, {"text": None, "loaded_at": 0})
    if (cache["text"] is not None
            and _time.time() - cache["loaded_at"] < 600):
        return cache["text"]
    try:
        import bot_self_improvement
        summary_row = bot_self_improvement.get_latest_summary()
        if summary_row and summary_row.get("summary_text"):
            cache["text"] = (
                "## TEAM FEEDBACK CONTEXT (auto-generated daily)\n\n"
                "These are the lessons the bot has learned from "
                "team reactions and corrections in #ai-audit + "
                "thread replies. Apply them to your responses. "
                "Updated daily based on a sliding window of "
                "feedback.\n\n"
                + summary_row["summary_text"]
                + f"\n\n_(summary date: "
                  f"{summary_row.get('summary_date')}, "
                  f"based on {summary_row.get('feedback_count')} "
                  f"feedback events)_\n\n"
                  "---\n\n"
            )
        else:
            cache["text"] = ""
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to load lessons-learned: %s", exc)
        cache["text"] = ""
    cache["loaded_at"] = _time.time()
    return cache["text"]


def _build_slack_system_prompt(channel_intent: str) -> str:
    """Slack-shaped system prompt. Different from the Streamlit
    one in three ways:
      1. Be MUCH shorter — Slack readers scan, don't read paragraphs.
      2. Cite source IDs (PO numbers, INV numbers, SKU codes) at end.
      3. If no data, say so — don't hallucinate."""
    # v2.67.66 — prepend the daily lessons-learned summary if we
    # have one. This is the auto-improvement loop: feedback →
    # daily summary → injected into prompt → future answers
    # apply the learned rules.
    lessons = _get_lessons_learned_block()

    # v2.67.70 — prepend the canonical engine rule book so the
    # bot reasons identically to the dashboard's AI Assistant.
    # Same constant rendered in the dashboard's "How to read this
    # page" expander. Bot can now explain WHY a SKU is dormant /
    # A-class / excess using the exact rules the engine uses.
    # User principle: "the slack bot must always match answers
    # that our ai assistant would give".
    try:
        from intelligence_glossary import GLOSSARY_MARKDOWN
        glossary_block = (
            "## INTELLIGENCE MODEL CONTEXT\n\n"
            "These are the canonical rules the dashboard's engine "
            "uses. When you answer questions about ABC class, "
            "is_dormant, excess_units, trend_flag, REMNANT, "
            "A-class grace, etc., reason from THESE rules. When "
            "asked WHY something is flagged, cite the rule. Your "
            "answers must match the dashboard's AI Assistant — "
            "same terminology, same logic, same surfacing patterns.\n\n"
            f"{GLOSSARY_MARKDOWN}\n\n"
            "---\n\n"
        )
    except Exception:  # noqa: BLE001
        glossary_block = ""

    base = (
        glossary_block +
        lessons +
        "You're a Slack assistant for the Wired4Signs ops team. "
        "Answer questions and offer relevant context using the "
        "tools provided. Your output goes directly into a Slack "
        "thread reply.\n\n"
        # v2.67.70 — strongest possible parity rule. User: 'the
        # slack bot must always match answers that our ai
        # assistant would give'. The intelligence model is
        # imported from intelligence_glossary.py (above); same
        # constant powers the dashboard's glossary expander.
        "**PARITY WITH DASHBOARD AI ASSISTANT (v2.67.70):** Your "
        "answers must match what the dashboard's AI Assistant "
        "would give for the same question. Same engine rules "
        "(see INTELLIGENCE MODEL CONTEXT above), same tool "
        "chain (search_products_by_text, get_sku_details, "
        "get_velocity, get_dead_stock, get_purchase_order, "
        "get_sale_order, get_shipping_details, get_shopify_order, "
        "get_shipping_margin, get_compatible_accessories), same "
        "terminology (ABC / is_dormant / excess_units / "
        "trend_flag / REMNANT / A-class grace / once-slow), "
        "same response shape (group by family, surface engine "
        "signals, cite source IDs). The bot's slim engine on "
        "the worker can drift slightly from the dashboard's "
        "full engine on edge cases (bulk-master rollup, A-class "
        "grace refinement, buyer manual corrections); when you "
        "notice numbers that look slightly off vs what staff "
        "expect from the dashboard, mention the drift "
        "explicitly: '_my engine is a slim mirror of the "
        "dashboard; for buyer-corrected dormancy or bulk-master "
        "rollup the dashboard is canonical_'. Don't pretend to "
        "be more authoritative than you are.\n\n"
        # v2.67.64 — proactive-tool-use principle. Added after the
        # bot kept asking users to paste data ("could you paste the "
        # "SKU list from PO-7124?") instead of trying alternative
        # tools / lookups. The user's principle: don't make the team
        # do work the bot could potentially do — just answer with
        # what we have, or clearly say what's missing and why.
        "**TOOL-USE PRINCIPLE — be proactive, don't shift work to "
        "the user:**\n"
        "• Try MULTIPLE tools before giving up. If one tool returns "
        "empty/error, try the closest related tool with adapted "
        "args. Examples:\n"
        "  - get_purchase_order returns matched=0 → try "
        "    search_products_by_text using the supplier name to "
        "    find recent SKUs from them, OR check "
        "    get_incoming_stock for any open POs.\n"
        "  - find_similar_products returns 0 → try "
        "    search_products_by_text with the dimensional / "
        "    family / colour-temp keywords from the original.\n"
        "  - get_sale_order returns 0 → try by customer name, "
        "    by date range, or by invoice number variant.\n"
        "• ONLY after exhausting reasonable tool options should "
        "you ask the user for input. And when you do, explain "
        "WHY the tools failed (data window, etc.) — not just "
        "'please paste'.\n"
        "• If a SKU/PO/SO/customer is mentioned but the data "
        "isn't loaded, prefer 'this is outside my sync window — "
        "check CIN7 directly at <appropriate URL>' over 'paste "
        "the details'. Sales staff are time-poor; pasting is "
        "friction.\n\n"
        "STYLE RULES:\n"
        "• Be concise — bullets > paragraphs. Aim for under 200 "
        "words when possible.\n"
        "• Lead with the answer, then supporting detail.\n"
        "• Use Slack mrkdwn formatting: *bold*, _italic_, "
        "`code`, > quotes, • bullets. NO markdown headings "
        "(#, ##) — Slack doesn't render them.\n"
        "• Cite source IDs (PO-XXXX, SO-XXXX, INV-XXXX, SKUs) so "
        "humans can verify.\n"
        "• If you have no useful data even after multiple tool "
        "tries, say what you tried and what you found — don't "
        "hallucinate. Format: '_Searched X, Y, Z — none "
        "matched. Try [specific next step in source system]._'\n"
        # v2.67.120 — three rules to fix the 'we don't carry that'
        # hallucination pattern. Real example: user asked for
        # bendable LED channels for an outdoor firepit; bot ran
        # 2-3 searches, came up empty, and concluded 'we don't
        # currently stock flexible or bendable LED channels' —
        # which was wrong (we stock Arc12, Milano Slim, Lille,
        # etc.). Search misses ≠ product absence; the bot must
        # default to ENGAGEMENT, not DENIAL.
        "• **Anti-denial rule:** Do NOT conclude 'we don't stock "
        "X' or 'we don't carry that category' unless you've run "
        "at least 3 distinct search phrasings AND tried family-"
        "name searches (e.g. specific product line names). Search "
        "misses ≠ catalog gaps — they often mean the user's "
        "wording doesn't match the product naming. When in doubt, "
        "list the closest-related products with their constraints "
        "and let the user judge fit.\n"
        "• **Constraint-surface rule:** When a product MIGHT or "
        "MIGHT NOT fit the user's need, list it WITH its limits "
        "— min bend radius, max strip width, IP rating, voltage, "
        "max operating temperature. Don't pre-reject on the "
        "user's behalf; surface options + constraints and let "
        "them choose. Example: '_Arc12 — bendable along length, "
        "min radius 500mm, IP65 with cover. Won't curve around a "
        "small circumference._'\n"
        "• **Clarify-before-concluding rule:** If the user's "
        "question is missing key dimensions or constraints "
        "(size, environment, voltage, strip width, IP rating), "
        "EITHER ask one clarifying question first, OR surface "
        "candidates and end your reply with the clarifying "
        "question. Never conclude absence based on an "
        "under-specified query.\n"
        # v2.67.121 — broader ambiguity handling. The
        # clarify-before-concluding rule (above) covers missing
        # PHYSICAL CONSTRAINTS for product questions; this rule
        # extends it to ANY question whose interpretation could
        # change the answer materially.
        "• **Ambiguity handling — ask one targeted question when "
        "the answer depends on interpretation:**\n"
        "  Recognise these patterns and ask ONE clarifying "
        "question before searching:\n"
        "  - *Multiple plausible interpretations* — 'what's the "
        "spend on Slim8?' could mean ad spend / COGS / inventory "
        "holding. 'Is this PO needed?' could mean overdue / SKU-"
        "still-demanded / supplier-reliability.\n"
        "  - *Missing physical constraints* on product fit (size, "
        "voltage, IP, strip width, environment).\n"
        "  - *Vague action verbs* — 'what should I do about X?', "
        "'help with Y', 'look at this PO'. Probe the intent: "
        "cancel / push / expedite / replace?\n"
        "  - *Comparison without baseline* — 'is this good?', "
        "'should we keep stocking?'. Ask: vs last month, vs same "
        "month last year, vs siblings in the family?\n"
        "  Format the clarifier as ONE question with the likely "
        "options listed. Don't stack multiple questions in a "
        "row. Don't ask when the question is clear, when it's a "
        "simple lookup ('show me LED-123'), or when channel "
        "context already implies the action.\n"
        "  This is INTENT clarification, NOT data-paste asking. "
        "We still avoid 'please paste the SKU list'-style "
        "requests for data we can look up; we DO ask 'are you "
        "asking about A or B?' when both are plausible answers.\n"
        "• Surface engine signals when relevant: ABC class, "
        "trend_flag, is_dormant, excess_units. These are facts, "
        "not opinions.\n"
        "• Round dollar amounts to whole dollars, units to whole "
        "numbers unless context demands precision.\n"
        # v2.67.123 — Marketing intelligence response template.
        # When a user asks ANY question that touches ad spend,
        # campaign performance, ROAS, attribution, or paid-
        # marketing decisions, you must produce a response with
        # the same depth as the Slim8 example below — not a
        # one-line answer. This is the Triple-Whale replacement
        # the team is relying on; thin answers are a regression.
        # The data is already in the tool results (get_ad_overview,
        # get_sku_ad_spend, get_campaign_performance,
        # compare_ad_periods, find_campaigns_to_cut,
        # find_campaigns_to_scale, attribution_sanity_check).
        # Your job is to compose it consistently.
        "\n**MARKETING-INTELLIGENCE RESPONSE TEMPLATE — apply to "
        "ANY question about ad spend, campaign performance, "
        "ROAS, attribution, paid-marketing decisions:**\n"
        "Your reply MUST include ALL of these sections when the "
        "underlying tool returned data (omit a section only if "
        "that specific data is genuinely missing from the tool "
        "result, NOT because you didn't bother to surface it):\n"
        "1. **Headline** — one line: total spend, period, count "
        "of SKUs/campaigns covered. Example: `*Slim8 Ad Spend — "
        "April 2026* · Total: $864.57 across 12 SKUs`.\n"
        "2. **Performance summary** — bulleted block with ALL "
        "available volume + value metrics:\n"
        "   • Impressions\n"
        "   • Clicks\n"
        "   • Conversions\n"
        "   • Conv. value\n"
        "   • ROAS (computed if not in tool output: "
        "conv_value / spend)\n"
        "   • CPC (computed: spend / clicks)\n"
        "   Show all five-six; don't pick a subset.\n"
        "3. **Spend-by-campaign breakdown** — code-block "
        "formatted, sorted high→low, dollar amounts right-"
        "aligned. Example:\n"
        "   ```\n"
        "   $625.84  Bidnamic Shopping - topmet HIGH\n"
        "   $126.32  W4s P-Max Channels PT\n"
        "   ```\n"
        "4. **Attribution insight (Note)** — actively look for "
        "and surface these patterns when present:\n"
        "   • Named-campaign vs real-spend split (e.g. campaigns "
        "with the SKU/family name in their title but $0 spend — "
        "actual traffic came through broader Topmet/P-Max "
        "campaigns)\n"
        "   • Outlier campaigns spending disproportionately\n"
        "   • High-spend / low-ROAS campaigns vs the family avg\n"
        "   • SKUs with traffic but no conversions (or vice "
        "versa)\n"
        "   Format: '_Note: <observation>._' at the bottom of "
        "the reply.\n"
        "5. **Engine-signal overlay (staff Slack/dashboard ONLY "
        "— never customer-facing tools):** When the spend covers "
        "multiple SKUs, flag the ABC class / trend_flag of the "
        "highest-spend SKUs. If money is flowing to a B-class "
        "declining SKU while an A-class trending sibling gets "
        "less, call that out — that's actionable allocation "
        "intelligence.\n"
        "If the tool returns a non-empty result but you produce "
        "a one-line reply ('Slim8 spent $865 in April'), you "
        "have failed the response standard. The reason Triple "
        "Whale was cancellable is BECAUSE this team can now get "
        "this depth from you — don't underdeliver.\n\n"
        # v2.67.65 — bin/location surfacing. User: 'on the "
        # stock-issues-queries chats and any other query on "
        # product it would be good to tell the user the bin or "
        # location of the stock'. The data IS in engine_df (Bin "
        # column merged from stock_on_hand) — just need the prompt "
        # to actually surface it in answers.
        "• **Always include Bin location for stock answers.** "
        "When listing SKUs with their stock counts, ALSO show "
        "the Bin location (warehouse shelf code) when known. "
        "Format: `<SKU> · <name> · OnHand <X> · Bin <bin>`. "
        "If Bin is unknown / null for a SKU, just omit it for "
        "that row (don't say 'Bin: unknown'). The warehouse "
        "team needs to know WHERE to pick from, not just "
        "whether stock exists.\n"
        "• When showing one SKU in detail (single-SKU lookup), "
        "call get_sku_details and surface the full set: "
        "OnHand, OnOrder, Available, Bin, Location, ABC, "
        "trend_flag, is_dormant if true.\n"
        # v2.67.67 — slow-mover responses MUST include unit count
        # AND dollar value of overstock. The whole point of slow-
        # mover surfacing is the stock-reduction flywheel: staff
        # need to see what's at risk in $ to decide what to push
        # to customers / discount.
        "• **Slow-mover / dead-stock / excess-stock answers MUST "
        "include both unit count and dollar value.** When "
        "responding to questions like 'what slow movers do we "
        "have', 'what's dormant', 'what's overstocked', '...for "
        "<colour temp / family>': for EACH flagged SKU, surface "
        "(a) excess_units (units over the engine's target), and "
        "(b) excess_value (dollar value of those units, "
        "computed from OnHandValue or unit cost). Format: "
        "`<SKU> · <name> · OnHand X · *excess Y units / "
        "$Z*`. If excess columns are missing on a row, fall "
        "back to OnHand × cost as the value figure but flag the "
        "estimate (`~$Z`). End with a one-line total: 'Combined "
        "overstock: N SKUs, M units, $Y total.' Whole-dollar "
        "rounding.\n\n"
    )
    if channel_intent == "po_review":
        base += (
            "PO REVIEW + BACKORDER MODE: this channel "
            "(#purchase-backorders or similar) handles FOUR "
            "intertwined things:\n"
            "  (a) Andrew submits POs for staff review — message "
            "      contains supplier name + multiple SKUs with "
            "      quantities.\n"
            "  (b) Sales-order backorders — staff flag a sale "
            "      whose stock is short.\n"
            "  (c) Stock orders / replenishment discussions.\n"
            "  (d) BARE PO reference (v2.67.193) — buyer pastes "
            "      a PO number (PO-NNNN) or a CIN7 "
            "      PurchaseAdvanced URL with no inline SKUs. "
            "      Same response as case (a) but you fetch the "
            "      line items yourself via get_purchase_order "
            "      first.\n\n"
            "Detect which case it is from the message shape, then "
            "run the appropriate commentary:\n\n"
            "**Case (a) — PO submission with inline SKUs:** "
            "for EACH SKU listed:\n"
            "1. Call get_sku_details / search_products_by_text "
            "for engine signals (ABC, OnHand, OnOrder, "
            "is_dormant, excess_units, trend_flag, 12mo demand).\n"
            "2. Call get_incoming_stock to check if we're already "
            "on order (avoid duplicate POs from same supplier).\n"
            "3. Flag with emoji: ✅ sensible / ⚠️ check / "
            "🪫 dormant / 📦 excess / 💼 A-class-grace.\n"
            "4. Don't approve / disapprove — provide data, staff "
            "decide.\n"
            "Output format per SKU: `<emoji> <SKU> (<qty>): "
            "<one-line analysis>`. End with a one-line summary: "
            "supplier total, last PO from that supplier, days "
            "since.\n\n"
            "**v2.67.361 — STORAGE DIMS (always surface, flag gaps).** "
            "When the PO was fetched via `get_purchase_live`, each line carries "
            "`storage_dim` (raw value of CIN7's `Storage L x W x H In` field — "
            "always show it even if partial, e.g. '___ x 2.756\" x 1.969\"'). "
            "`storage_dim_missing` (true = field is blank), and "
            "`storage_dim_incomplete` (true = has placeholders or fewer than 3 dims). "
            "For EVERY line: append `📐 <value>` if a value exists, "
            "or `📐 dims not set` if missing. "
            "After all per-line output, if any SKUs have missing OR incomplete dims, "
            "include a summary section:\n"
            "```\n"
            "📐 *Storage dims incomplete/missing — update in CIN7:*\n"
            "<SKU1> (___ x 2.756 x 1.969 — length missing), <SKU2> (not set)\n"
            "@warehouse please capture before shipment lands.\n"
            "```\n"
            "Only omit this section if ALL lines have complete dims "
            "(missing_count == 0 AND incomplete_count == 0).\n\n"
            "**Case (d) — bare PO reference (v2.67.193):** when "
            "the message mentions PO-NNNN or contains a CIN7 "
            "URL like dearsystems.com/PurchaseAdvanced#<uuid> "
            "but has no inline SKU list:\n"
            "1. **v2.67.197 — UUID-first when a URL is present.** "
            "If the message contains a CIN7 PurchaseAdvanced "
            "URL, extract the UUID from the fragment "
            "(everything after the `#` until the next `~` or "
            "end of URL — typical shape: "
            "`PurchaseAdvanced#<uuid>` or "
            "`PurchaseAdvanced#<uuid>~<uuid>~tab`). Call "
            "get_purchase_live with purchase_id=<uuid>. UUID "
            "lookup works for DRAFT POs too, where OrderNumber "
            "search may miss them.\n"
            "2. Otherwise (just a PO-NNNN reference): FIRST "
            "call get_purchase_order with po_number=PO-NNNN. "
            "This is fast (reads local CSV) and works for any "
            "PO synced in the last 30 days.\n"
            "3. **Fallback for fresh / draft POs:** if "
            "get_purchase_order returns matched=0, call "
            "get_purchase_live with po_number=PO-NNNN. This "
            "hits CIN7's API directly. PO might be in DRAFT "
            "status — that's fine, surface it as "
            "`📝 DRAFT — pending approval` and analyse each "
            "line normally. The whole point of the commentary "
            "channel is to help decide whether to approve the "
            "draft.\n"
            "4. THEN run the same per-SKU analysis as case (a) "
            "for each line item returned.\n"
            "5. Only if BOTH get_purchase_order AND "
            "get_purchase_live fail (CIN7 returned nothing, "
            "credentials issue, etc.) should you surface the "
            "gap to the user and ask them to paste lines "
            "inline.\n\n"
            "**Case (b) — backorder mention:** if a SO/INV "
            "number is referenced, call get_sale_order to surface "
            "what's on it, get_incoming_stock for any open POs "
            "that would clear the backorder, and surface ETA "
            "from the matching PO's Required-By or shipping_notes "
            "freight progress.\n\n"
            "**Case (c) — replenishment chat:** if the question "
            "is 'should we order more X?', surface OnHand, "
            "12mo demand, current OnOrder, days-of-stock, "
            "is_dormant flag. Don't recommend a quantity — that's "
            "Andrew's call."
        )
    elif channel_intent == "returns":
        base += (
            "RETURNS MODE (v2.67.59): this channel is "
            "#returns — sales/fulfilment staff post when a "
            "customer returns an item. The buyer (Andrew) is "
            "in this channel. Your PRIMARY job here is the "
            "**return→purchase warning loop**: when a SKU is "
            "mentioned in a return, immediately check if that "
            "SKU is on an open PO and warn the buyer so he "
            "doesn't double-order.\n\n"
            "Workflow when you see a SKU in #returns:\n"
            "1. Call `get_incoming_stock` with the SKU. If "
            "matched > 0, the buyer has an open PO for it.\n"
            "2. Call `get_sku_details` or "
            "`search_products_by_text` to surface engine "
            "signals (OnHand, OnOrder, 12mo demand, "
            "is_dormant, excess_units, trend_flag).\n"
            "3. Post a SHORT warning thread reply, format:\n"
            "   `⚠️ Heads-up @<buyer> — <SKU> just returned. "
            "Currently: OnHand <X> · OnOrder <Y> from "
            "<supplier> on <PO-ZZZZ>. With this return, "
            "incoming stock would total <Y+returned>. 12mo "
            "demand <Z>. Consider reducing PO or holding off.`\n"
            "4. If the SKU is dormant or excess-flagged, make "
            "that VERY clear — those are the highest-value "
            "warnings (don't order more of stock that isn't "
            "selling).\n"
            "5. If there's NO open PO for the returned SKU, "
            "still post a brief `📦 noted — <SKU> "
            "returned, no open POs, OnHand now <X>` so the "
            "fulfilment team has confirmation the return is "
            "registered with the system. Keep it ONE LINE.\n\n"
            "Don't pull the customer's history or comment on "
            "WHY they returned — that's not your call. Stay "
            "focused on the procurement-warning angle. If the "
            "message also references a SO/INV number, you can "
            "briefly call get_sale_order to confirm "
            "what was on the original sale, but lead with "
            "the procurement warning."
        )
    elif channel_intent == "orders":
        base += (
            "ORDERS MODE (v2.67.62): this channel is "
            "#w4s-orders — staff discuss order status, "
            "changes, and **cancellations**. Your job is to "
            "surface context and flag downstream procurement "
            "effects so the team has full visibility when "
            "decisions are made.\n\n"
            "When you see a SO/INV number:\n"
            "1. Call `get_sale_order` with that number.\n"
            "2. Post a one-line summary: `📋 SO-XXXXX · "
            "<customer> · <date> · $<total> · <status>. "
            "<N items: top SKUs in compact form>`.\n"
            "3. If status is 'CANCELLED' or 'VOIDED' OR the "
            "message text contains 'cancel' / 'cancelled' / "
            "'cancellation' alongside the order number, ALSO "
            "do the **downstream check**: for each line-item "
            "SKU, call `get_incoming_stock` to see if there's "
            "an open PO. If yes, flag it: `⚠️ <SKU> has open "
            "PO-ZZZZ for X units from <supplier>. With this "
            "cancellation, may want to reduce/hold.`\n"
            "4. If the sale is Shopify-channel "
            "(SourceChannel='Shopify'), proactively follow up "
            "with `get_shopify_order` for conversion-attribution "
            "context (referring_site, source_name, "
            "discount_codes) — useful for understanding cancel "
            "patterns.\n\n"
            "When a customer name is mentioned without a "
            "specific order number:\n"
            "1. Call `get_sale_order` with customer + "
            "date_from=<7 days ago> to surface their recent "
            "orders.\n"
            "2. Post a summary listing each one as a bullet.\n\n"
            "Style: ONE bullet per key piece of info, no "
            "preamble like 'Here's what I found'. Lead with "
            "facts. Use the 📋 emoji to mark a fresh order "
            "summary, ⚠️ for downstream-PO warnings, ❌ for "
            "cancellations."
        )
    elif channel_intent == "shipping":
        base += (
            "SHIPPING / FULFILMENT MODE: when invoice numbers "
            "(INV-XXXXX) or tracking numbers are mentioned, call "
            "get_shipping_details and surface ship date, carrier, "
            "tracking, address.\n\n"
            "**Margin investigation (v2.67.255):** when a SO or "
            "INV is referenced — especially with phrases like "
            "'client paid X', 'we paid Y', or any cost / charge "
            "numbers — do a full investigation. Steps in order:\n"
            "1. `get_sale_order` for the SO/INV — pull the "
            "customer name and find the shipping charge "
            "(AdditionalCharge line whose Description starts "
            "with 'Shipping ').\n"
            "2. `get_shipping_details` for the same order — pull "
            "carrier, service, ShipDate, tracking, and the actual "
            "shipmentCost ShipStation recorded.\n"
            "3. `get_shipping_margin` for the same order — uses "
            "both feeds and returns the customer_charge, "
            "actual_cost, and net margin pre-computed.\n"
            "4. Compose a reply with: customer · SO/INV · carrier · "
            "service · charge · cost · *margin (or loss)*. Flag "
            "explicitly if the loss exceeds $20 ('🔴 *${X} loss*') "
            "or the margin is over 50% ('💸 fat margin'). State "
            "concrete possible causes (under-quoted on dims/weight, "
            "expedited upgrade, dim-weight surcharge, residential "
            "fee, freight class, declared value).\n"
            "Do NOT answer with vague guesses — the data is "
            "available; investigate before replying."
        )
    elif channel_intent == "sales":
        base += (
            "SALES MODE: this channel is staff asking sales-related "
            "questions about products + customers + orders.\n\n"
            "**Transactional lookups:** when SO/INV numbers or "
            "customer names are mentioned, call get_sale_order. "
            "If the sale's SourceChannel is 'Shopify', proactively "
            "follow up with get_shopify_order for conversion-"
            "attribution data.\n\n"
            "**Product-similarity / alternatives questions** "
            "(v2.67.63 — added after the bot missed Oslo as a "
            "Sierra38 alternative): when the user asks for "
            "'alternatives to X', 'something similar to X', "
            "'replacement for X', 'comparable to X':\n"
            "1. FIRST call `find_similar_products` (it matches "
            "on structured family/diameter attributes — fast, "
            "high precision).\n"
            "2. **If that returns 0 or 1 results, ALSO call "
            "`search_products_by_text` with key dimensions / "
            "characteristics extracted from the original product** "
            "— e.g. for SIERRA38 (38mm / 1.5\" diameter), call "
            "search_products_by_text(query='38mm') AND query="
            "'1.5\"' to catch products named with imperial sizes. "
            "Many product families don't have a structured "
            "diameter attribute but DO mention the dimension in "
            "the title/description. The text search is the "
            "fallback that catches them.\n"
            "3. Combine + dedupe results from both tools. List "
            "every parent SKU that matches.\n"
            "4. If the original product is a tube / channel / "
            "profile / strip with a numeric size (e.g. "
            "SIERRA38, SLIM8, NICHO), the size is almost "
            "certainly the key matcher — extract it and "
            "search.\n\n"
            "**Stock / availability questions** ('do we have "
            "warm white', 'what 2700K is in stock'): call "
            "search_products_by_text with the colour/spec "
            "keyword + parents_only=true + in_stock_only=true."
        )
    elif channel_intent == "website":
        base += (
            "WEBSITE MODE: questions here are usually about "
            "product catalog, family relationships, accessories. "
            "Use find_products and get_compatible_accessories."
        )
    return base


# ---------------------------------------------------------------------------
# Data loading for the listener (separate from Streamlit page)
# ---------------------------------------------------------------------------


_LISTENER_DATA_CACHE: Dict[str, Any] = {
    "engine_df": None,
    "sale_lines_df": None,
    "loaded_at": 0,
}
DATA_CACHE_TTL_SECONDS = 600  # 10 min — listener can run staler than UI


def _get_data_for_listener() -> Tuple[Any, Any]:
    """Load (and cache) the data the AI tools need. Reads CSVs
    directly — bypasses Streamlit's @cache_resource since this is
    a separate process."""
    import time as _time
    if (_LISTENER_DATA_CACHE["engine_df"] is not None
            and _time.time() - _LISTENER_DATA_CACHE["loaded_at"]
                < DATA_CACHE_TTL_SECONDS):
        return (_LISTENER_DATA_CACHE["engine_df"],
                _LISTENER_DATA_CACHE["sale_lines_df"])

    try:
        import pandas as pd
        import glob as _glob
        from data_paths import OUTPUT_DIR
        # Just enough to power the AI tools. Real engine_df is
        # built by Streamlit; here we use a slim version (products +
        # stock + sales) which the AI's get_sku_details +
        # search_products_by_text can work against.
        prod_files = sorted(_glob.glob(str(OUTPUT_DIR / "products_*.csv")))
        stk_files = sorted(_glob.glob(str(OUTPUT_DIR / "stock_on_hand_*.csv")))
        sl_files = sorted(_glob.glob(str(OUTPUT_DIR / "sale_lines_last_*d_*.csv")))
        if not prod_files or not stk_files:
            log.warning("No products/stock CSV available for listener")
            return (None, None)
        products = pd.read_csv(prod_files[-1], low_memory=False)
        stock = pd.read_csv(stk_files[-1], low_memory=False)
        sale_lines = pd.read_csv(sl_files[-1], low_memory=False) if sl_files else pd.DataFrame()

        # v2.67.69 — full engine intelligence on the worker.
        # Earlier versions merged products+stock for a slim engine_df
        # with no ABC, no dormancy flag, no excess columns — bot's
        # slow-mover / overstock answers were inconsistent with the
        # dashboard. worker_engine.compute_engine_signals now adds:
        # ABC, effective_units_12mo, effective_units_90d, is_dormant,
        # excess_units, excess_value, OnHandValue, trend_flag,
        # is_non_master_tube. Faithful-but-simplified vs the web
        # service's _abc_engine; v2.67.70 (Postgres migration) will
        # eliminate any remaining drift.
        try:
            import worker_engine
            engine_df = worker_engine.compute_engine_signals(
                products, stock, sale_lines)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "worker_engine.compute_engine_signals failed: "
                "%s — falling back to slim products+stock merge",
                exc)
            stock_cols = ["SKU"]
            for opt in ("OnHand", "Bin", "Location",
                          "OnOrder", "Available", "StockOnHand"):
                if opt in stock.columns:
                    stock_cols.append(opt)
            engine_df = products.merge(
                stock[stock_cols], on="SKU", how="left")
            if "AdditionalAttribute1" in engine_df.columns:
                engine_df["Family"] = engine_df["AdditionalAttribute1"]

        # Wire up purchase-line / shipment / shopify holders for the
        # AI tools that need them.
        try:
            import ai_tools
            import gc
            pl_files = sorted(_glob.glob(str(OUTPUT_DIR / "purchase_lines_last_*d_*.csv")))
            if pl_files:
                ai_tools.set_purchase_lines(pd.read_csv(pl_files[-1], low_memory=False))
            # v2.67.61 — memory fix. The Render Starter worker plan
            # has only 512 MB RAM. shipments_full.csv (43k rows) +
            # shopify_orders_full.csv (24k rows) + everything else
            # the listener loads = OOM kill.
            #
            # PREFER the windowed CSVs (shipments_last_30d_*.csv ~5MB,
            # shopify_orders_last_30d_*.csv ~3MB). Fall back to *_full
            # only if no windowed file exists.
            #
            # Trade-off: bot can't answer questions about shipments /
            # orders older than the windowed file's coverage. Most
            # questions are about recent stuff anyway. For older
            # transactions the bot can suggest the user check the
            # source system directly.
            sh_recent = sorted(_glob.glob(str(OUTPUT_DIR / "shipments_last_*d_*.csv")))
            if sh_recent:
                ai_tools.set_shipments(pd.read_csv(sh_recent[-1], low_memory=False))
            else:
                # No windowed file → fall back to full but warn.
                sh_full = OUTPUT_DIR / "shipments_full.csv"
                if sh_full.exists():
                    log.warning(
                        "Loading shipments_full.csv (no windowed file). "
                        "Memory pressure risk on 512MB plans.")
                    ai_tools.set_shipments(
                        pd.read_csv(sh_full, low_memory=False))
            so_recent = sorted(_glob.glob(
                str(OUTPUT_DIR / "shopify_orders_last_*d_*.csv")))
            if so_recent:
                ai_tools.set_shopify_orders(
                    pd.read_csv(so_recent[-1], low_memory=False))
            else:
                so_full = OUTPUT_DIR / "shopify_orders_full.csv"
                if so_full.exists():
                    log.warning(
                        "Loading shopify_orders_full.csv (no windowed "
                        "file). Memory pressure risk on 512MB plans.")
                    ai_tools.set_shopify_orders(
                        pd.read_csv(so_full, low_memory=False))
            ai_tools.set_sale_lines_longest(sale_lines)
            gc.collect()  # free any transient pandas allocations
        except Exception as exc:  # noqa: BLE001
            log.warning("listener data wiring partial: %s", exc)

        _LISTENER_DATA_CACHE["engine_df"] = engine_df
        _LISTENER_DATA_CACHE["sale_lines_df"] = sale_lines
        _LISTENER_DATA_CACHE["loaded_at"] = _time.time()
        return (engine_df, sale_lines)
    except Exception as exc:  # noqa: BLE001
        log.error("listener data load failed: %s", exc)
        return (None, None)


# ---------------------------------------------------------------------------
# Posting
# ---------------------------------------------------------------------------


def _truncate_for_slack(text: str) -> str:
    if len(text) <= MAX_RESPONSE_CHARS:
        return text
    return text[:MAX_RESPONSE_CHARS - 30] + "\n…[truncated]"


def _already_replied_recently(channel_id: str, thread_ts: str) -> bool:
    """Bot-loop guard: have we posted in this thread within the
    SELF_SUPPRESS_SECONDS window?"""
    cutoff = time.time() - SELF_SUPPRESS_SECONDS
    with db.connect() as c:
        row = c.execute(
            "SELECT MAX(strftime('%s', posted_at)) AS last_ts "
            "FROM slack_bot_responses "
            "WHERE in_channel = ? AND in_thread_ts = ?",
            (channel_id, thread_ts)).fetchone()
    last = float(row["last_ts"]) if row and row["last_ts"] else 0
    return last > cutoff


def _did_we_forward_to_viktor(channel_id: str,
                                    thread_ts: str) -> bool:
    """v2.67.124 — Check if we previously posted a viktor_handoff
    in this thread. Used to decide whether a fresh message from
    Viktor's user_id deserves an ops-overlay reply."""
    try:
        with db.connect() as c:
            r = c.execute(
                "SELECT 1 FROM slack_bot_responses "
                "WHERE in_channel = ? "
                "  AND in_thread_ts = ? "
                "  AND classification = 'viktor_handoff' "
                "LIMIT 1",
                (channel_id, thread_ts)).fetchone()
        return r is not None
    except Exception:
        return False


def _compose_viktor_handoff(user_text: str, user_name: str) -> str:
    """v2.67.125 — Slack bots ignore @-mentions from other bots
    (standard abuse-prevention pattern; Viktor has it on). So
    instead of trying to @-mention Viktor directly (which Viktor
    silently drops), we post a 'smart redirect' message:

      - Tells the user this is a marketing question, best handled
        by Viktor
      - Pre-formats their question in a code block so they can
        tap-to-copy and paste it into a fresh @viktor message
      - Promises the engine-signal overlay once Viktor answers

    We still mark this thread as a viktor_handoff in
    slack_bot_responses, so when Viktor DOES reply (after the user
    forwards manually), the overlay logic in _classify fires."""
    viktor_uid = _viktor_user_id()
    viktor_tag = f"<@{viktor_uid}>" if viktor_uid else "@Viktor"
    cleaned = (user_text or "").strip()
    return (
        f"_Marketing question — best answered by {viktor_tag}._ "
        f"Slack apps can't ping each other, so could you "
        f"@-mention Viktor with this question in the same "
        f"thread?\n\n"
        f"```\n"
        f"@Viktor {cleaned}\n"
        f"```\n"
        f"_Once Viktor answers, I'll add ops context — ABC "
        f"class, stock, supplier — for any SKUs they mention._"
    )


def _compose_viktor_overlay(viktor_reply_text: str) -> tuple:
    """v2.67.126 — Thin wrapper around viktor_bridge.compose_overlay
    so both Slack and dashboard use the same overlay formatting.
    Kept here as a stable indirection in case slack_listener needs
    Slack-specific post-processing later."""
    from viktor_bridge import compose_overlay
    return compose_overlay(viktor_reply_text)


def _post_response(session, channel_id: str, thread_ts: str,
                     text: str) -> Optional[str]:
    """Post a threaded reply. Returns the new message's ts on
    success, None on failure."""
    body = slack_sync._slack_post(session, "chat.postMessage", {
        "channel": channel_id,
        "thread_ts": thread_ts,
        "text": _truncate_for_slack(text),
        "unfurl_links": False,
        "unfurl_media": False,
    })
    return body.get("ts")


def _mirror_to_audit(session, original_msg: Dict[str, Any],
                       response_text: str, tools_used: List[str],
                       classification: str, response_id: int
                       ) -> None:
    """Post the response into #ai-audit so admins can review what
    the bot says without scrolling 5 channels."""
    audit_channel = os.environ.get(
        "SLACK_AUDIT_CHANNEL", "").strip()
    if not audit_channel:
        return
    user_name = original_msg.get("user_name") or "(unknown)"
    ch_name = original_msg.get("channel_name") or original_msg["channel_id"]
    body_text = (
        f"*AI response in #{ch_name}* (id={response_id}, "
        f"classification={classification})\n"
        f"*From {user_name}:*\n"
        f"> {(original_msg['text'] or '')[:400]}\n\n"
        f"*Bot replied:*\n{_truncate_for_slack(response_text)[:1500]}\n\n"
        f"_Tools used: {', '.join(tools_used) or 'none'}_"
    )
    try:
        slack_sync._slack_post(session, "chat.postMessage", {
            "channel": audit_channel,
            "text": body_text,
            "unfurl_links": False,
        })
        with db.connect() as c:
            c.execute(
                "UPDATE slack_bot_responses SET audit_posted = 1 "
                "WHERE id = ?", (response_id,))
    except Exception as exc:  # noqa: BLE001
        log.warning("audit mirror failed for response %s: %s",
                      response_id, exc)


# ---------------------------------------------------------------------------
# Top-level processing
# ---------------------------------------------------------------------------


def process_once(max_messages: int = 25) -> int:
    """Process up to `max_messages` unclassified messages. Returns
    count of bot responses actually posted (not classified-and-
    skipped). Designed to be called every minute or so."""
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not token:
        return 0
    session = slack_sync._build_session(token)
    bot_self = slack_sync.get_bot_self_id(session)

    # Pull a batch of unclassified messages, oldest first so users
    # don't see out-of-order responses.
    with db.connect() as c:
        rows = c.execute(
            "SELECT channel_id, ts, user_id, user_name, text, "
            "       thread_ts, is_bot, is_our_bot, raw_event "
            "FROM slack_messages "
            "WHERE classification IS NULL "
            "ORDER BY ts ASC "
            "LIMIT ?", (max_messages,)).fetchall()

    if not rows:
        return 0

    # v2.67.98 — ingest-only channel set. Bot polls these channels
    # so the AI can REFERENCE the content via get_slack_messages,
    # but never responds to or interacts with them. Used for
    # external/sensitive channels like ext-growmymail-wired4signs
    # where the bot is an observer, not a participant.
    ingest_only = slack_sync._ingest_only_channels()

    posts_made = 0
    for r in rows:
        msg = dict(r)
        ch_name = slack_sync._resolve_channel(session,
                                                  msg["channel_id"])
        msg["channel_name"] = ch_name
        intent = _channel_intent(ch_name)

        # v2.67.98 — short-circuit ingest-only channels.
        if msg["channel_id"] in ingest_only:
            with db.connect() as c:
                c.execute(
                    "UPDATE slack_messages "
                    "SET classification = ?, classified_at = "
                    "    datetime('now') "
                    "WHERE channel_id = ? AND ts = ?",
                    ("ingest_only", msg["channel_id"], msg["ts"]))
            continue

        classification = _classify(msg, bot_self, intent)

        # Mark classified regardless of whether we respond. Avoids
        # re-evaluating every poll.
        with db.connect() as c:
            c.execute(
                "UPDATE slack_messages "
                "SET classification = ?, classified_at = "
                "    datetime('now') "
                "WHERE channel_id = ? AND ts = ?",
                (classification, msg["channel_id"], msg["ts"]))

        # v2.67.149 — SO cross-reference auto-reply. Fires
        # ALONGSIDE whatever classification was set, as a
        # supplementary one-liner with hyperlinks to both the
        # CIN7 sale and the Shopify order. Dedup per
        # (channel, thread_ts, set of SOs) so the same mapping
        # doesn't get reposted on every reference within the
        # thread. Skipped for bot-self / empty / too_old messages.
        if classification not in (
                "bot_self", "bot_other", "empty", "too_old"):
            try:
                from so_lookup import (
                    find_so_references, lookup_so,
                    compose_reply as _so_compose)
                _so_text = (msg["text"] or "")
                _sos_in_msg = find_so_references(_so_text)
                if _sos_in_msg:
                    _so_thread = msg["thread_ts"] or msg["ts"]
                    # Dedup: have we ALREADY posted a
                    # so_cross_reference reply in this thread for
                    # ANY of these SOs? If yes, skip — one
                    # cross-ref per thread is enough.
                    with db.connect() as c:
                        _existing = c.execute(
                            "SELECT 1 FROM slack_bot_responses "
                            "WHERE in_channel = ? "
                            "  AND in_thread_ts = ? "
                            "  AND classification = "
                            "      'so_cross_reference' "
                            "LIMIT 1",
                            (msg["channel_id"], _so_thread)
                        ).fetchone()
                    if not _existing:
                        _records = []
                        for _so in _sos_in_msg[:5]:
                            _r = lookup_so(_so)
                            if _r:
                                _records.append(_r)
                        if _records:
                            _xr_text = _so_compose(_records)
                            _xr_ts = _post_response(
                                session, msg["channel_id"],
                                _so_thread, _xr_text)
                            if _xr_ts:
                                with db.connect() as c:
                                    c.execute(
                                        "INSERT INTO "
                                        "slack_bot_responses "
                                        "(in_channel, in_ts, "
                                        " in_thread_ts, "
                                        " user_question, "
                                        " response_text, "
                                        " response_ts, "
                                        " tools_used, "
                                        " classification) "
                                        "VALUES (?, ?, ?, ?, ?, "
                                        "        ?, ?, ?)",
                                        (msg["channel_id"],
                                          msg["ts"], _so_thread,
                                          _so_text[:300],
                                          _xr_text, _xr_ts,
                                          "so_lookup",
                                          "so_cross_reference"))
                                posts_made += 1
                                log.info(
                                    "Posted SO cross-reference "
                                    "in %s/%s (%d SO refs)",
                                    ch_name, msg["ts"],
                                    len(_records))
            except Exception as _exc:
                log.warning("SO cross-reference skipped: %s",
                              _exc)

        # Skip non-respondable categories.
        if classification in ("bot_self", "bot_other", "empty",
                                "too_old", "chatter"):
            continue

        # v2.67.144 — Stock-issue raise: post brief querier reply
        # + structured intelligence block (the handler posts the
        # intel block directly, we post the querier reply via
        # _post_response below for slack_bot_responses auditing).
        if classification == "stock_issue_raise":
            thread_ts = msg["thread_ts"] or msg["ts"]
            try:
                from stock_issues_handler import (
                    handle_stock_issue as _handle_si)
                reply_text, si_tools = _handle_si(msg)
            except Exception as exc:
                log.error("stock_issue handler error: %s", exc)
                continue
            if not reply_text:
                continue
            posted_ts = _post_response(
                session, msg["channel_id"], thread_ts, reply_text)
            if posted_ts:
                with db.connect() as c:
                    c.execute(
                        "INSERT INTO slack_bot_responses "
                        "(in_channel, in_ts, in_thread_ts, "
                        " user_question, response_text, "
                        " response_ts, tools_used, classification) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (msg["channel_id"], msg["ts"], thread_ts,
                         (msg["text"] or "")[:500],
                         reply_text, posted_ts,
                         ",".join(si_tools),
                         "stock_issue_raise"))
                posts_made += 1
                log.info("Posted stock_issue querier reply in "
                          "%s/%s", ch_name, msg["ts"])
            continue

        # v2.67.144 — stock-issue resolution reply (human said
        # 'fixed'/'adjusted'/etc in a tracked thread). Just log
        # — the resolution write already happened in _classify.
        if classification == "stock_issue_resolution":
            log.info("Tracked stock_issue resolution in %s/%s",
                      ch_name, msg["ts"])
            continue

        # v2.67.153 — Dropship UPS email handler: parse the UPS
        # shipment notification, match to the CIN7 sale, post
        # confirmation + weight-mismatch alert if any.
        if classification == "dropship_ups_email":
            thread_ts = msg["thread_ts"] or msg["ts"]
            if _already_replied_recently(
                    msg["channel_id"], thread_ts):
                continue
            try:
                from dropship_tracking_handler import (
                    handle_ups_email as _handle_ups)
                reply_text, ups_tools = _handle_ups(msg)
            except Exception as exc:
                log.error("dropship UPS handler error: %s", exc)
                continue
            if not reply_text:
                continue
            posted_ts = _post_response(
                session, msg["channel_id"], thread_ts,
                reply_text)
            if posted_ts:
                with db.connect() as c:
                    c.execute(
                        "INSERT INTO slack_bot_responses "
                        "(in_channel, in_ts, in_thread_ts, "
                        " user_question, response_text, "
                        " response_ts, tools_used, classification) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (msg["channel_id"], msg["ts"], thread_ts,
                         (msg["text"] or "[email]")[:500],
                         reply_text, posted_ts,
                         ",".join(ups_tools),
                         "dropship_ups_email"))
                posts_made += 1
                log.info(
                    "Posted dropship-UPS confirmation in "
                    "%s/%s", ch_name, msg["ts"])
            continue

        # v2.67.136 — Back-in-stock subscription from FlowBot.
        # Parse, log to demand_signals, post threaded triage reply.
        if classification == "back_in_stock_subscription":
            thread_ts = msg["thread_ts"] or msg["ts"]
            if _already_replied_recently(
                    msg["channel_id"], thread_ts):
                continue
            try:
                from back_in_stock_handler import (
                    handle_subscription as _handle_bis)
                reply_text, bis_tools = _handle_bis(msg)
            except Exception as exc:
                log.error("back_in_stock handler error: %s", exc)
                continue
            if not reply_text:
                # Parsing failed — silently log the message but
                # don't reply with garbage. Mark classified so we
                # don't retry.
                continue
            posted_ts = _post_response(
                session, msg["channel_id"], thread_ts,
                reply_text)
            if posted_ts:
                with db.connect() as c:
                    c.execute(
                        "INSERT INTO slack_bot_responses "
                        "(in_channel, in_ts, in_thread_ts, "
                        " user_question, response_text, "
                        " response_ts, tools_used, classification) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (msg["channel_id"], msg["ts"], thread_ts,
                         (msg["text"] or "")[:500],
                         reply_text, posted_ts,
                         ",".join(bis_tools),
                         "back_in_stock_subscription"))
                posts_made += 1
                log.info(
                    "Posted back-in-stock triage in %s/%s",
                    ch_name, msg["ts"])
            continue

        # v2.67.124 — Viktor handoff: post the forwarding message
        # in the thread and record it as a viktor_handoff response.
        # We skip the expensive LLM compose; Viktor will answer.
        if classification == "viktor_handoff":
            thread_ts = msg["thread_ts"] or msg["ts"]
            if _already_replied_recently(
                    msg["channel_id"], thread_ts):
                continue
            handoff_text = _compose_viktor_handoff(
                msg["text"] or "", msg.get("user_name") or "user")
            posted_ts = _post_response(
                session, msg["channel_id"], thread_ts,
                handoff_text)
            if posted_ts:
                with db.connect() as c:
                    c.execute(
                        "INSERT INTO slack_bot_responses "
                        "(in_channel, in_ts, in_thread_ts, "
                        " user_question, response_text, "
                        " response_ts, tools_used, classification) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (msg["channel_id"], msg["ts"], thread_ts,
                         (msg["text"] or "")[:500],
                         handoff_text, posted_ts,
                         "viktor_handoff", "viktor_handoff"))
                posts_made += 1
                log.info(
                    "Forwarded marketing q to Viktor in %s/%s",
                    ch_name, msg["ts"])
            continue

        # v2.67.124 — Viktor overlay: Viktor has replied in a
        # thread we previously forwarded. Compose engine-signal
        # context and post it as a follow-up.
        if classification == "viktor_overlay":
            thread_ts = msg["thread_ts"] or msg["ts"]
            overlay_text, overlay_tools = _compose_viktor_overlay(
                msg["text"] or "")
            if not overlay_text:
                log.info(
                    "Viktor overlay skipped (no SKUs/families) "
                    "for %s/%s", ch_name, msg["ts"])
                continue
            posted_ts = _post_response(
                session, msg["channel_id"], thread_ts,
                overlay_text)
            if posted_ts:
                with db.connect() as c:
                    c.execute(
                        "INSERT INTO slack_bot_responses "
                        "(in_channel, in_ts, in_thread_ts, "
                        " user_question, response_text, "
                        " response_ts, tools_used, classification) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (msg["channel_id"], msg["ts"], thread_ts,
                         "(viktor reply)",
                         overlay_text, posted_ts,
                         ",".join(overlay_tools),
                         "viktor_overlay"))
                posts_made += 1
                log.info("Posted Viktor overlay in %s/%s",
                          ch_name, msg["ts"])
            continue

        # v2.67.188 — PO commentary crosspost. Source message lives
        # in the buyer-review channel (typically muted); we run the
        # po_review composer against it and post the commentary to
        # a SEPARATE channel (the dedicated #po-commentary). Posts
        # exactly once per source-message ts (idempotency via the
        # slack_bot_responses table — we look up by in_ts).
        if classification == "po_commentary_crosspost":
            tgt_channel = os.environ.get(
                "SLACK_PO_COMMENTARY_TARGET_CHANNEL_ID",
                "").strip()
            if not tgt_channel:
                log.warning(
                    "po_commentary_crosspost classified but "
                    "SLACK_PO_COMMENTARY_TARGET_CHANNEL_ID not "
                    "set — skipping post.")
                continue
            # Idempotency — have we already posted commentary for
            # this exact source message?
            with db.connect() as c:
                already = c.execute(
                    "SELECT 1 FROM slack_bot_responses "
                    "WHERE in_channel = ? AND in_ts = ? "
                    "  AND classification = "
                    "      'po_commentary_crosspost' LIMIT 1",
                    (msg["channel_id"], msg["ts"])).fetchone()
            if already:
                log.info(
                    "po_commentary_crosspost: already posted "
                    "for %s/%s — skipping.",
                    ch_name, msg["ts"])
                continue
            # Use the po_review intent to drive the composer's
            # prompt — that branch (line 932+) already has the
            # right system-prompt copy for analysing PO drafts.
            log.info(
                "Composing po_commentary_crosspost for %s/%s "
                "→ posting to %s",
                ch_name, msg["ts"], tgt_channel)
            # v2.67.222 — draft-PO commentary must be deterministic.
            # If the source message carries a CIN7 PurchaseAdvanced
            # URL, extract the UUID in CODE and hand it to the AI as
            # an explicit directive. Relying on the model to parse
            # the URL fragment itself was unreliable — PO-7164 (a
            # DRAFT) was reported "not found" because the AI fell
            # back to an OrderNumber search, which filters drafts.
            _msg_for_compose = msg
            _po_uuid = _extract_po_uuid(msg.get("text") or "")
            if _po_uuid:
                _msg_for_compose = dict(msg)
                _msg_for_compose["text"] = (
                    (msg.get("text") or "")
                    + f"\n\n[system note: this purchase order's "
                    f"CIN7 UUID is {_po_uuid}. You MUST call "
                    f"get_purchase_live with "
                    f"purchase_id=\"{_po_uuid}\" — the UUID lookup "
                    f"retrieves DRAFT purchase orders that an "
                    f"OrderNumber search misses. Do NOT report the "
                    f"PO as 'not found' until this UUID lookup has "
                    f"been tried.]")
            try:
                text, tools = _compose_response(
                    _msg_for_compose, "po_review", "po_review")
            except Exception as exc:
                log.error("po_commentary compose failed: %s", exc)
                continue
            if not text or not text.strip():
                log.info(
                    "po_commentary_crosspost: AI returned empty "
                    "for %s/%s", ch_name, msg["ts"])
                continue
            # Permalink to the source message so reviewers can
            # jump back. Slack permalink format:
            #   https://<workspace>.slack.com/archives/<CH>/p<ts*1e6>
            _src_ts_id = "p" + str(msg["ts"]).replace(".", "")
            workspace = os.environ.get(
                "SLACK_WORKSPACE_SUBDOMAIN",
                "wired4signs-usa").strip()
            permalink = (
                f"https://{workspace}.slack.com/archives/"
                f"{msg['channel_id']}/{_src_ts_id}")
            header = (
                f":bar_chart: *PO commentary* "
                f"— source: <{permalink}|message in #"
                f"{ch_name or msg['channel_id']}>\n\n")
            body = header + text
            try:
                posted_ts = _post_response(
                    session, tgt_channel, None, body)
            except Exception as exc:
                log.error(
                    "po_commentary_crosspost post failed: %s",
                    exc)
                continue
            if posted_ts:
                with db.connect() as c:
                    c.execute(
                        "INSERT INTO slack_bot_responses "
                        "(in_channel, in_ts, in_thread_ts, "
                        " user_question, response_text, "
                        " response_ts, tools_used, "
                        " classification) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (msg["channel_id"], msg["ts"],
                          msg["thread_ts"] or msg["ts"],
                          (msg["text"] or "")[:500],
                          body, posted_ts,
                          ",".join(tools),
                          "po_commentary_crosspost"))
                posts_made += 1
                log.info(
                    "Posted po_commentary_crosspost to %s for "
                    "source %s/%s",
                    tgt_channel, ch_name, msg["ts"])
            continue

        # Self-suppress check.
        # v2.67.128 — bypass suppression for direct @-mentions.
        # The 5-minute SELF_SUPPRESS_SECONDS window is there to
        # prevent loops on passive classifications (question /
        # trigger / *_summary), but a direct @-mention is the
        # user EXPLICITLY asking for another reply (e.g. 'you
        # should look up the parent') — silencing the bot in
        # that case is a bug, not a feature. Real case: James
        # @-mentioned 60s after the bot's initial reply with
        # corrective feedback; bot silently dropped it because of
        # the 5-min window.
        thread_ts = msg["thread_ts"] or msg["ts"]
        if (classification != "mention"
                and _already_replied_recently(
                    msg["channel_id"], thread_ts)):
            log.info("Skipping %s/%s — recent reply in thread "
                       "(cls=%s)", ch_name, msg["ts"], classification)
            continue

        # Compose. This is the expensive call.
        log.info("Composing for %s/%s (cls=%s, intent=%s, user=%s)",
                   ch_name, msg["ts"], classification, intent,
                   msg.get("user_name"))
        text, tools = _compose_response(msg, classification, intent)

        if not text or not text.strip():
            log.info("  AI declined to respond (no useful data)")
            # Still log the attempt with an empty response_text for
            # audit visibility.
            with db.connect() as c:
                c.execute(
                    "INSERT INTO slack_bot_responses "
                    "(in_channel, in_ts, in_thread_ts, "
                    " user_question, response_text, response_ts, "
                    " tools_used, classification, audit_posted) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)",
                    (msg["channel_id"], msg["ts"], thread_ts,
                     msg["text"], "", None,
                     ",".join(tools), classification))
            continue

        # Post the threaded reply.
        try:
            posted_ts = _post_response(session, msg["channel_id"],
                                          thread_ts, text)
        except Exception as exc:  # noqa: BLE001
            log.error("post failed for %s/%s: %s",
                        ch_name, msg["ts"], exc)
            continue

        # Save to slack_bot_responses.
        with db.connect() as c:
            cur = c.execute(
                "INSERT INTO slack_bot_responses "
                "(in_channel, in_ts, in_thread_ts, "
                " user_question, response_text, response_ts, "
                " tools_used, classification) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (msg["channel_id"], msg["ts"], thread_ts,
                 msg["text"], text, posted_ts,
                 ",".join(tools), classification))
            response_id = cur.lastrowid
            c.execute(
                "UPDATE slack_messages SET response_id = ? "
                "WHERE channel_id = ? AND ts = ?",
                (response_id, msg["channel_id"], msg["ts"]))

        # Mirror to audit channel.
        _mirror_to_audit(session, msg, text, tools, classification,
                          response_id)
        posts_made += 1

    return posts_made


def loop_forever(interval_seconds: int = 60) -> None:
    """Listener loop. Runs alongside slack_sync.loop_forever()."""
    log.info("Starting Slack listener loop (interval=%ds)",
              interval_seconds)
    while True:
        try:
            n = process_once()
            if n:
                log.info("Posted %d responses", n)
        except Exception as exc:  # noqa: BLE001
            log.error("Listener iteration failed: %s", exc)
        time.sleep(max(15, interval_seconds))


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv(SCRIPT_DIR / ".env")
    p = argparse.ArgumentParser(description="Slack listener.")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("once", help="Process one batch and exit.")
    lp = sub.add_parser("loop", help="Loop forever.")
    lp.add_argument("--interval", type=int, default=60)
    args = p.parse_args(argv)
    if args.cmd == "once":
        n = process_once()
        log.info("Posted %d responses", n)
    elif args.cmd == "loop":
        loop_forever(args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
