"""bot_self_improvement.py (v2.67.66)
=========================================

Daily 'lessons learned' summarizer for the Slack bot.

The flow:
  1. slack_sync.py captures feedback events into slack_audit_feedback:
     - Emoji reactions on bot posts (👍 / 👎 / 🛑 / etc.)
     - Human thread replies in threads where the bot also replied
  2. THIS script runs once per day. Reads the last N days of feedback,
     pairs each event with the bot's original response and the user's
     question, and asks Anthropic Sonnet to summarize the recurring
     patterns of correction.
  3. The summary is stored in bot_lessons_learned and ALSO written as
     a markdown file to /data/output/bot_lessons_learned.md for easy
     reading.
  4. slack_listener.py reads the latest summary at compose time and
     prepends it to the system prompt as 'TEAM FEEDBACK CONTEXT'.

Feedback loop closes itself overnight. No manual prompt edits needed.

CLI:
  python bot_self_improvement.py daily         # run today's summary
  python bot_self_improvement.py show          # print latest summary
  python bot_self_improvement.py dump --days 7 # print recent feedback

Env vars:
  ANTHROPIC_API_KEY   required for summarization
  ANTHROPIC_MODEL_SUMMARY  override model (default sonnet)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402
from data_paths import OUTPUT_DIR  # noqa: E402

LOG_FORMAT = "%(asctime)s  %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("bot_self_improvement")

DEFAULT_WINDOW_DAYS = 7
DEFAULT_MODEL = "claude-sonnet-4-5"
SUMMARY_MD_PATH = OUTPUT_DIR / "bot_lessons_learned.md"


# ---------------------------------------------------------------------------
# Feedback retrieval
# ---------------------------------------------------------------------------


def fetch_recent_feedback(window_days: int = DEFAULT_WINDOW_DAYS
                            ) -> List[Dict[str, Any]]:
    """Pull every audit-feedback event from the last N days, joined
    with the bot's original response + the user's question. Returns
    list of dicts ordered newest first.

    Each entry has enough context for the summarizer to identify
    patterns:
      - The user's question (what triggered the bot)
      - The bot's response
      - The tools the bot used
      - The feedback (reaction emoji or thread-reply text)
      - Polarity hint (+1 / 0 / -1)
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)
              ).strftime("%Y-%m-%d %H:%M:%S")
    sql = (
        "SELECT "
        "  f.id AS feedback_id, "
        "  f.feedback_type, "
        "  f.user_name AS feedback_user, "
        "  f.content AS feedback_content, "
        "  f.is_positive, "
        "  f.captured_at AS feedback_at, "
        "  r.id AS response_id, "
        "  r.user_question, "
        "  r.response_text, "
        "  r.tools_used, "
        "  r.classification, "
        "  r.in_channel "
        "FROM slack_audit_feedback f "
        "LEFT JOIN slack_bot_responses r ON f.response_id = r.id "
        "WHERE f.captured_at >= ? "
        "ORDER BY f.captured_at DESC"
    )
    out = []
    try:
        with db.connect() as c:
            for row in c.execute(sql, (cutoff,)).fetchall():
                out.append(dict(row))
    except Exception as exc:
        log.error("Failed to fetch feedback: %s", exc)
    return out


# ---------------------------------------------------------------------------
# Summarization
# ---------------------------------------------------------------------------


SUMMARY_SYSTEM_PROMPT = """\
You are an internal coach for an AI assistant that runs in a small
ops-team Slack workspace. Your job: read the team's feedback on the
bot's recent replies and produce a concise 'lessons learned' note
that will be prepended to the bot's system prompt to guide its
future answers.

OUTPUT FORMAT (mrkdwn-friendly, max ~400 words):

A flat list of bullet points, each:
- Phrased as a CONCRETE behavioural rule the bot should follow,
  not a generic 'be better' statement.
- Cites the symptom (what feedback indicated the issue) briefly,
  no need for full quotes.
- If the feedback contradicts itself (some users 👍, others 👎 the
  same response style), flag the disagreement and note the
  preference of the majority OR ask the bot to be conservative.

Group bullets by theme: 'Stock answers', 'PO commentary',
'Shipping lookups', 'Returns warnings', 'General tone', etc.
Use *bold theme labels* in Slack mrkdwn.

DO NOT:
- Suggest the bot apologize or be more cautious in tone — only
  improve factual accuracy / response shape.
- Make up rules not supported by the feedback. If feedback is too
  thin or noisy, output: "_Insufficient feedback signal in this
  window — no new rules to apply._"
- Reference specific user names. Generalise.

PRIORITISE CORRECTIONS where:
- Multiple users gave the same negative signal
- A thread-reply explicitly said the bot was wrong
- A 👎 / 🛑 / ❌ followed a specific response pattern
"""


def summarize_feedback(feedback: List[Dict[str, Any]],
                         model: Optional[str] = None) -> str:
    """Call Anthropic with the feedback context, return the summary
    as a markdown string. Returns a graceful fallback message on
    error or empty feedback."""
    if not feedback:
        return ("_No feedback events in this window — "
                "no new rules to apply._")

    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return ("_ANTHROPIC_API_KEY not set — "
                "cannot generate summary._")

    try:
        import anthropic
    except ImportError:
        return ("_anthropic SDK not installed._")

    # Build the input block. Format compactly so the model sees the
    # important context without burning excess tokens.
    lines = []
    pos_count = 0
    neg_count = 0
    neutral_count = 0
    for f in feedback:
        polarity = f.get("is_positive") or 0
        if polarity > 0:
            pos_count += 1
        elif polarity < 0:
            neg_count += 1
        else:
            neutral_count += 1
        polarity_label = (
            "POSITIVE" if polarity > 0
            else "NEGATIVE" if polarity < 0
            else "neutral")
        question = (f.get("user_question") or "")[:200]
        bot_reply = (f.get("response_text") or "")[:400]
        tools = f.get("tools_used") or ""
        feedback_text = (f.get("feedback_content") or "")[:300]
        feedback_type = f.get("feedback_type")
        classification = f.get("classification") or ""
        lines.append(
            f"--- feedback_id={f.get('feedback_id')} "
            f"({feedback_type}, {polarity_label}, "
            f"intent={classification}) ---\n"
            f"USER ASKED: {question}\n"
            f"BOT REPLIED: {bot_reply}\n"
            f"TOOLS: {tools}\n"
            f"FEEDBACK: {feedback_text}\n")

    user_block = (
        f"Window: {len(feedback)} feedback events "
        f"({pos_count} positive, {neg_count} negative, "
        f"{neutral_count} neutral).\n\n"
        f"FEEDBACK EVENTS:\n\n" + "\n".join(lines))

    log.info("Calling Anthropic with %d feedback events "
              "(%d pos, %d neg, %d neutral)",
              len(feedback), pos_count, neg_count, neutral_count)

    client = anthropic.Anthropic(api_key=api_key)
    try:
        resp = client.messages.create(
            model=model or os.environ.get(
                "ANTHROPIC_MODEL_SUMMARY", DEFAULT_MODEL),
            max_tokens=1500,
            system=SUMMARY_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_block}])
    except Exception as exc:
        log.error("Anthropic summary call failed: %s", exc)
        return f"_Summary generation failed: {exc}_"

    text_chunks = []
    for block in resp.content:
        if hasattr(block, "text"):
            text_chunks.append(block.text)
    return "\n\n".join(t.strip() for t in text_chunks if t.strip())


def write_summary(window_days: int,
                    feedback: List[Dict[str, Any]],
                    summary_text: str) -> int:
    """Persist the summary to the DB AND to a markdown file.
    Returns the new row id."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw_json = json.dumps([
        {k: v for k, v in f.items()
         if k in ("feedback_id", "feedback_type", "is_positive",
                  "user_question", "response_text", "tools_used",
                  "classification", "feedback_content")}
        for f in feedback
    ], default=str)[:50000]  # cap to ~50KB
    with db.connect() as c:
        c.execute(
            "INSERT INTO bot_lessons_learned "
            "(summary_date, feedback_window_days, feedback_count, "
            " summary_text, raw_feedback_json) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(summary_date) DO UPDATE SET "
            "  feedback_window_days = excluded.feedback_window_days, "
            "  feedback_count = excluded.feedback_count, "
            "  summary_text = excluded.summary_text, "
            "  raw_feedback_json = excluded.raw_feedback_json, "
            "  generated_at = datetime('now')",
            (today, window_days, len(feedback),
             summary_text, raw_json))
        row = c.execute(
            "SELECT id FROM bot_lessons_learned "
            "WHERE summary_date = ?", (today,)).fetchone()

    # Also write a friendly markdown file for human review.
    SUMMARY_MD_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_MD_PATH.write_text(
        f"# Bot Lessons Learned — {today}\n\n"
        f"Generated from {len(feedback)} feedback events over the "
        f"last {window_days} days.\n\n"
        f"---\n\n"
        f"{summary_text}\n",
        encoding="utf-8")

    return int(row["id"]) if row else 0


def get_latest_summary() -> Optional[Dict[str, Any]]:
    """Return the most recent lessons-learned summary, or None if
    none exist or the latest is older than 14 days (stale)."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)
              ).strftime("%Y-%m-%d")
    try:
        with db.connect() as c:
            row = c.execute(
                "SELECT id, summary_date, feedback_window_days, "
                "       feedback_count, summary_text, generated_at "
                "FROM bot_lessons_learned "
                "WHERE summary_date >= ? "
                "ORDER BY summary_date DESC LIMIT 1",
                (cutoff,)).fetchone()
        return dict(row) if row else None
    except Exception as exc:
        log.warning("Failed to fetch latest summary: %s", exc)
        return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def cmd_daily(window_days: int) -> int:
    """Run today's summary — fetch feedback, ask Anthropic, persist."""
    feedback = fetch_recent_feedback(window_days)
    log.info("Fetched %d feedback events from last %d days",
              len(feedback), window_days)
    summary = summarize_feedback(feedback)
    new_id = write_summary(window_days, feedback, summary)
    log.info("Wrote bot_lessons_learned id=%d (%d events)",
              new_id, len(feedback))
    log.info("Markdown at: %s", SUMMARY_MD_PATH)
    print("\n=== SUMMARY ===\n")
    print(summary)
    return 0


def cmd_show() -> int:
    """Print the latest summary from DB."""
    s = get_latest_summary()
    if not s:
        print("No summary exists yet. Run `daily` first.")
        return 1
    print(f"# Bot Lessons Learned — {s['summary_date']}")
    print(f"_{s['feedback_count']} events over "
          f"{s['feedback_window_days']} days; "
          f"generated {s['generated_at']}_\n")
    print(s["summary_text"])
    return 0


def cmd_dump(window_days: int) -> int:
    """Print the raw feedback events from the window."""
    feedback = fetch_recent_feedback(window_days)
    print(f"=== {len(feedback)} feedback events in last "
          f"{window_days} days ===\n")
    for f in feedback:
        polarity = (f.get("is_positive") or 0)
        sym = "+" if polarity > 0 else "-" if polarity < 0 else "·"
        print(f"[{sym}] {f.get('feedback_type')} by "
              f"{f.get('feedback_user')} on "
              f"response_id={f.get('response_id')} ({f.get('classification')})")
        print(f"   Q: {(f.get('user_question') or '')[:150]}")
        print(f"   FB: {(f.get('feedback_content') or '')[:200]}")
        print()
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv(SCRIPT_DIR / ".env")
    p = argparse.ArgumentParser(
        description="Bot self-improvement summarizer.")
    sub = p.add_subparsers(dest="cmd", required=True)
    dp = sub.add_parser("daily", help="Run today's summary.")
    dp.add_argument("--days", type=int, default=DEFAULT_WINDOW_DAYS)
    sub.add_parser("show", help="Show latest summary.")
    dump_p = sub.add_parser("dump", help="Dump raw feedback events.")
    dump_p.add_argument("--days", type=int, default=DEFAULT_WINDOW_DAYS)
    args = p.parse_args(argv)

    if args.cmd == "daily":
        return cmd_daily(args.days)
    if args.cmd == "show":
        return cmd_show()
    if args.cmd == "dump":
        return cmd_dump(args.days)
    return 0


if __name__ == "__main__":
    sys.exit(main())
