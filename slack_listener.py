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
}

# SKU + transaction-number patterns we recognise as triggers.
SKU_RE = re.compile(r"\b(LED(?:KIT)?-[A-Z0-9-]+)\b", re.IGNORECASE)
SO_RE = re.compile(r"\bSO-\d+\b", re.IGNORECASE)
PO_RE = re.compile(r"\bPO-\d+\b", re.IGNORECASE)
INV_RE = re.compile(r"\bINV-\d+\b", re.IGNORECASE)
TRACKING_RE = re.compile(r"\b1Z[A-Z0-9]{16}\b")  # UPS tracking format


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def _classify(msg: Dict[str, Any], bot_self_id: str,
                channel_intent: str) -> str:
    """Return classification string. Cheap pattern-match — no LLM
    calls. The composer is what's expensive; this just gates it."""
    if msg["is_our_bot"]:
        return "bot_self"
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
    # Direct @-mention of our bot is always an answer-required.
    if bot_self_id and f"<@{bot_self_id}>" in text:
        return "mention"

    # PO-review channel-specific: in #stock-issues-queries, a message
    # that contains MULTIPLE SKUs is likely a PO Andrew submitted for
    # review. Trigger a commentary regardless of question form.
    if channel_intent == "po_review":
        skus = SKU_RE.findall(text)
        if len(skus) >= 2:
            return "po_review"

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

    return "chatter"


def _channel_intent(channel_name: str) -> str:
    """Map channel name to its dominant intent. Used to bias the
    response composer.

    Channel intent map (v2.67.57):
      - po_review     → PO submissions + backorder discussions.
                        AI runs per-SKU commentary using engine
                        signals + open-PO checks.
                        Hits: #purchase-backorders (Andrew's PO
                        submissions + sales-order backorder
                        review), #stock-issues-queries (general
                        stock / PO questions).
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
    if ("purchase" in name or "backorder" in name
            or "stock" in name or "po-" in name or "po_" in name):
        return "po_review"
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
            return ("", ["ERROR: data not loadable"])
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


def _build_slack_system_prompt(channel_intent: str) -> str:
    """Slack-shaped system prompt. Different from the Streamlit
    one in three ways:
      1. Be MUCH shorter — Slack readers scan, don't read paragraphs.
      2. Cite source IDs (PO numbers, INV numbers, SKU codes) at end.
      3. If no data, say so — don't hallucinate."""
    base = (
        "You're a Slack assistant for the Wired4Signs ops team. "
        "Answer questions and offer relevant context using the "
        "tools provided. Your output goes directly into a Slack "
        "thread reply.\n\n"
        "STYLE RULES:\n"
        "• Be concise — bullets > paragraphs. Aim for under 200 "
        "words when possible.\n"
        "• Lead with the answer, then supporting detail.\n"
        "• Use Slack mrkdwn formatting: *bold*, _italic_, "
        "`code`, > quotes, • bullets. NO markdown headings "
        "(#, ##) — Slack doesn't render them.\n"
        "• Cite source IDs (PO-XXXX, SO-XXXX, INV-XXXX, SKUs) so "
        "humans can verify.\n"
        "• If you have no useful data, say 'I don't have data on "
        "that — try the dashboard' rather than guessing.\n"
        "• Surface engine signals when relevant: ABC class, "
        "trend_flag, is_dormant, excess_units. These are facts, "
        "not opinions.\n"
        "• Round dollar amounts to whole dollars, units to whole "
        "numbers unless context demands precision.\n\n"
    )
    if channel_intent == "po_review":
        base += (
            "PO REVIEW + BACKORDER MODE: this channel "
            "(#purchase-backorders or similar) handles three "
            "intertwined things:\n"
            "  (a) Andrew submits POs for staff review — message "
            "      contains supplier name + multiple SKUs with "
            "      quantities.\n"
            "  (b) Sales-order backorders — staff flag a sale "
            "      whose stock is short.\n"
            "  (c) Stock orders / replenishment discussions.\n\n"
            "Detect which case it is from the message shape, then "
            "run the appropriate commentary:\n\n"
            "**Case (a) — PO submission:** for EACH SKU listed:\n"
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
    elif channel_intent == "shipping":
        base += (
            "SHIPPING / FULFILMENT MODE: when invoice numbers "
            "(INV-XXXXX) or tracking numbers are mentioned, call "
            "get_shipping_details and surface ship date, carrier, "
            "tracking, address. If a shipping-cost question, "
            "compute margin = customer_charge - actual_cost from "
            "the data."
        )
    elif channel_intent == "sales":
        base += (
            "SALES MODE: when SO/INV numbers or customer names "
            "are mentioned, call get_sale_order. If the sale's "
            "SourceChannel is 'Shopify', proactively follow up "
            "with get_shopify_order for conversion-attribution "
            "data."
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

        # Minimal engine_df: products joined with stock OnHand. The
        # Streamlit version has way more derived columns (ABC,
        # trend_flag, is_dormant) but the listener can still answer
        # most questions with this slim shape. Tools will gracefully
        # fallback if columns are missing.
        engine_df = products.merge(
            stock[["SKU", "OnHand"]] if "OnHand" in stock.columns else stock[["SKU"]],
            on="SKU", how="left")
        if "AdditionalAttribute1" in engine_df.columns:
            engine_df["Family"] = engine_df["AdditionalAttribute1"]

        # Wire up purchase-line / shipment / shopify holders for the
        # AI tools that need them.
        try:
            import ai_tools
            pl_files = sorted(_glob.glob(str(OUTPUT_DIR / "purchase_lines_last_*d_*.csv")))
            if pl_files:
                ai_tools.set_purchase_lines(pd.read_csv(pl_files[-1], low_memory=False))
            sh_full = OUTPUT_DIR / "shipments_full.csv"
            sh_recent = sorted(_glob.glob(str(OUTPUT_DIR / "shipments_last_*d_*.csv")))
            if sh_full.exists() and sh_recent:
                ships = pd.concat([
                    pd.read_csv(sh_full, low_memory=False),
                    pd.read_csv(sh_recent[-1], low_memory=False),
                ], ignore_index=True).drop_duplicates(subset=["ShipmentID"], keep="last")
                ai_tools.set_shipments(ships)
            elif sh_recent:
                ai_tools.set_shipments(pd.read_csv(sh_recent[-1], low_memory=False))
            so_full = OUTPUT_DIR / "shopify_orders_full.csv"
            if so_full.exists():
                ai_tools.set_shopify_orders(pd.read_csv(so_full, low_memory=False))
            ai_tools.set_sale_lines_longest(sale_lines)
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
            "       thread_ts, is_bot, is_our_bot "
            "FROM slack_messages "
            "WHERE classification IS NULL "
            "ORDER BY ts ASC "
            "LIMIT ?", (max_messages,)).fetchall()

    if not rows:
        return 0

    posts_made = 0
    for r in rows:
        msg = dict(r)
        ch_name = slack_sync._resolve_channel(session,
                                                  msg["channel_id"])
        msg["channel_name"] = ch_name
        intent = _channel_intent(ch_name)
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

        # Skip non-respondable categories.
        if classification in ("bot_self", "bot_other", "empty",
                                "too_old", "chatter"):
            continue

        # Self-suppress check.
        thread_ts = msg["thread_ts"] or msg["ts"]
        if _already_replied_recently(msg["channel_id"], thread_ts):
            log.info("Skipping %s/%s — recent reply in thread",
                       ch_name, msg["ts"])
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
