"""
demand_scoring.py
=================
Pure scoring logic for the demand_signals table. No DB, no Streamlit,
no I/O — takes a list of signal dicts in, returns a score dict out.
This makes the formula testable, swappable, and reusable from the
buyer dashboard, the AI Warning column, and any future caller.

Spec lives in docs/demand-scoring.md. If you change a weight or a
formula here, update the doc too so the AI Assistant's KB stays in
sync with reality.

Public API:
    SOURCE_WEIGHTS, TYPE_WEIGHTS    — the tunable weights
    score_signals(signals, ...)     — main scoring function
    score_warning_level(score, ...) — convert score → warning level
    explain_score(score_dict)       — human-readable "why this score"
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Iterable


# ---------------------------------------------------------------------------
# Tunable weights — change these in one place; the doc gets updated separately.
# Source: docs/demand-scoring.md.
# ---------------------------------------------------------------------------

# Per-signal-type weights. Higher = stronger demand indicator.
# Negative values aren't used here; cancellations/returns flow through
# `quality_penalty` instead so they explicitly subtract from the score.
TYPE_WEIGHTS: dict[str, float] = {
    "quote":              1.5,
    "notify_me":          1.4,
    "inquiry":            1.0,
    "sold":               1.2,   # boost — converted demand counts
    "lost":               0.6,   # something went wrong but interest was real
    "substitute_offered": 0.8,
    "abandoned_cart":     0.7,
    "search_query":       0.4,
    "seo_rank":           0.3,
    "complaint":          0.0,   # surfaced via quality_penalty separately
    "cancelled":          0.0,   # via quality_penalty
    "returned":           0.0,   # via quality_penalty
}

# Per-source credibility. Higher = we trust the signal more.
SOURCE_WEIGHTS: dict[str, float] = {
    "manual":              1.0,   # curated by sales — high signal
    "slack":               1.0,
    "gorgias":             0.9,   # customer-direct
    "phone":               0.9,
    "web_form":            0.9,
    "seo":                 0.7,   # leading-indirect
    "shopify_search":      0.6,   # volume-heavy, low individual signal
    "shopify_abandoned":   0.5,
}
DEFAULT_SOURCE_WEIGHT = 0.6  # unknown sources get a middling weight

# Quality penalty per cancelled / returned / complaint event.
# Capped so a single bad streak doesn't bottom out the score forever.
QUALITY_PENALTY_PER_EVENT = 5
QUALITY_PENALTY_CAP = 30

# Default windows
DEFAULT_WINDOW_DAYS = 30
DEFAULT_CONVERSION_WINDOW_DAYS = 90


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_dt(value) -> datetime | None:
    """Normalise the created_at field. SQLite stores it as 'YYYY-MM-DD
    HH:MM:SS' or ISO. Handles both. Returns naive UTC datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    s = str(value).strip()
    if not s:
        return None
    # Try common formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.split("+")[0].rstrip("Z"), fmt)
        except ValueError:
            continue
    # Fallback: pandas-style parse via fromisoformat
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(
            tzinfo=None)
    except (ValueError, AttributeError):
        return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


# ---------------------------------------------------------------------------
# Score computation
# ---------------------------------------------------------------------------

def score_signals(signals: Iterable[dict],
                   *,
                   window_days: int = DEFAULT_WINDOW_DAYS,
                   conversion_signals: Iterable[dict] | None = None,
                   conversion_window_days: int = (
                       DEFAULT_CONVERSION_WINDOW_DAYS),
                   now: datetime | None = None) -> dict:
    """Compute a 0-100 demand score from a list of signal dicts.

    `signals` should be the rows from demand_signals for ONE sku that
    fall within the past `window_days`. Each dict needs at least:
        signal_type, source, created_at, customer_name (optional),
        customer_id (optional)

    `conversion_signals` is the same list but covering the wider
    conversion window (default 90d). Used to compute conversion rate
    (sold / total_inquiry-class) — this rewards SKUs whose signals
    actually turn into sales.

    Returns a dict with score, confidence, components, breakdown, why.
    """
    now = now or datetime.utcnow()
    signals = list(signals)
    conversion_signals = list(conversion_signals or signals)

    if not signals:
        return {
            "score": 0,
            "confidence": 0.0,
            "components": {},
            "breakdown": {},
            "distinct_sources": 0,
            "distinct_customers": 0,
            "window_days": window_days,
            "why": "No signals in window.",
        }

    # ---- Volume (log-scaled count)
    n = len(signals)
    base_volume = min(100.0, 20 * math.log2(1 + n))

    # ---- Signal-quality weighted average
    quality_total = 0.0
    type_breakdown: dict[str, int] = {}
    quality_event_count = 0
    for s in signals:
        t = (s.get("signal_type") or "").lower()
        type_breakdown[t] = type_breakdown.get(t, 0) + 1
        w = TYPE_WEIGHTS.get(t, 0.5)
        quality_total += w
        if t in ("cancelled", "returned", "complaint"):
            quality_event_count += 1
    signal_quality_weight = quality_total / max(n, 1)

    # ---- Source-credibility weighted average
    source_total = 0.0
    source_breakdown: dict[str, int] = {}
    for s in signals:
        src = (s.get("source") or "").lower()
        source_breakdown[src] = source_breakdown.get(src, 0) + 1
        source_total += SOURCE_WEIGHTS.get(src, DEFAULT_SOURCE_WEIGHT)
    source_credibility_weight = source_total / max(n, 1)

    # ---- Recency (linear decay over window)
    recency_total = 0.0
    for s in signals:
        dt = _parse_dt(s.get("created_at"))
        if dt is None:
            recency_total += 0.5  # unknown date — middling weight
            continue
        age_days = max(0, (now - dt).total_seconds() / 86400.0)
        weight = max(0.1, 1.0 - (age_days / window_days))
        recency_total += weight
    recency_weight = recency_total / max(n, 1)

    # ---- Conversion factor over wider window.
    #
    # Numerator counts "this demand turned into a sale". Two ways a row
    # can qualify:
    #   (a) signal_type == 'sold' (the original closed-sale event), OR
    #   (b) outcome == 'converted' (an inquiry/quote/etc. that the buyer
    #       later marked as won via the Demand Signals review page).
    # Dedup by row id so a single row counted under both still counts
    # once. Rows without an id (synthetic / test data) fall through to
    # a positional count which is fine because there's nothing to dedup
    # against.
    #
    # Denominator counts inquiry-class rows regardless of outcome, so a
    # converted inquiry is correctly scored as 1/1 not 0/1.
    n_conv_total = 0
    converted_ids: set = set()
    n_conv_sold_no_id = 0
    inquiry_class = ("inquiry", "quote", "abandoned_cart",
                      "notify_me", "search_query")
    for s in conversion_signals:
        t = (s.get("signal_type") or "").lower()
        o = (s.get("outcome") or "").lower()
        rid = s.get("id")
        if t in inquiry_class:
            n_conv_total += 1
        if t == "sold" or o == "converted":
            if rid is not None:
                converted_ids.add(rid)
            else:
                n_conv_sold_no_id += 1
    n_conv_sold = len(converted_ids) + n_conv_sold_no_id
    if n_conv_total > 0:
        conversion_rate = n_conv_sold / n_conv_total
    else:
        conversion_rate = 0.0
    conversion_factor = 1.0 + min(0.5, conversion_rate)

    # ---- Quality penalty
    quality_penalty = min(
        QUALITY_PENALTY_CAP,
        QUALITY_PENALTY_PER_EVENT * quality_event_count)

    # ---- Combine
    raw_score = (
        base_volume
        * signal_quality_weight
        * source_credibility_weight
        * recency_weight
        * conversion_factor
        - quality_penalty
    )
    score = round(_clamp(raw_score, 0, 100), 1)

    # ---- Confidence
    distinct_sources = len({(s.get("source") or "").lower()
                              for s in signals})
    distinct_customers = len({
        s.get("customer_id") or s.get("customer_name") or ""
        for s in signals
        if s.get("customer_id") or s.get("customer_name")
    })
    confidence = _clamp(
        0.3
        + 0.1 * min(7, n)
        + 0.05 * distinct_sources
        + 0.1 * distinct_customers,
        0.0, 1.0)

    return {
        "score": score,
        "confidence": round(confidence, 2),
        "components": {
            "base_volume": round(base_volume, 1),
            "signal_quality_weight": round(signal_quality_weight, 2),
            "source_credibility_weight": round(
                source_credibility_weight, 2),
            "recency_weight": round(recency_weight, 2),
            "conversion_factor": round(conversion_factor, 2),
            "conversion_rate": round(conversion_rate, 2),
            "quality_penalty": quality_penalty,
            "raw_score_before_clamp": round(raw_score, 1),
        },
        "breakdown": {
            "by_type": type_breakdown,
            "by_source": source_breakdown,
        },
        "distinct_sources": distinct_sources,
        "distinct_customers": distinct_customers,
        "n_signals": n,
        "n_conversion_total": n_conv_total,
        "n_conversion_sold": n_conv_sold,
        "window_days": window_days,
        "conversion_window_days": conversion_window_days,
        "quality_events": quality_event_count,
    }


# ---------------------------------------------------------------------------
# Score → warning level (replaces rule-based logic in the Ordering page)
# ---------------------------------------------------------------------------

def score_warning_level(score_dict: dict, *,
                          classification: str = "",
                          reorder_suggested: float = 0.0) -> tuple:
    """Map a score dict + context → (level, short_text). Level is one
    of 'high', 'medium', 'watch', or None.

    classification: SKU's current status ('active', 'slow', 'dead',
                    'watchlist').
    reorder_suggested: how many units the engine wants to reorder.
                       Used to escalate warnings only when the buyer
                       is actually about to order.
    """
    if not score_dict or score_dict.get("score") is None:
        return (None, "")
    score = float(score_dict.get("score") or 0)
    confidence = float(score_dict.get("confidence") or 0)
    quality_events = int(score_dict.get("quality_events") or 0)
    cls = (classification or "").lower()
    n_signals = int(score_dict.get("n_signals") or 0)

    # --- HIGH levels (stop and verify) ---
    # Reorder suggested + classified slow/dead = engine and demand
    # disagree. Verify before committing inventory dollars.
    if reorder_suggested > 0 and cls in ("dead", "slow") and score >= 40:
        return ("high",
                f"⛔ Score {score:.0f}/100 ({cls.upper()}) — engine "
                "wants reorder but classification says dormant. Verify "
                "demand source.")

    # Quality concerns (cancellations/returns/complaints)
    if quality_events >= 3:
        return ("high",
                f"⛔ Score {score:.0f}/100 — {quality_events} "
                "cancellation/return/complaint events recently. "
                "Review reason before reordering.")

    # --- MEDIUM levels (caution) ---
    if cls in ("dead", "slow") and n_signals >= 2 and reorder_suggested > 0:
        return ("medium",
                f"⚠️ Score {score:.0f}/100 — was {cls.upper()}, "
                f"{n_signals} recent signals. Promotion or one-off?")

    if quality_events >= 1 and reorder_suggested > 0:
        return ("medium",
                f"⚠️ Score {score:.0f}/100 — {quality_events} recent "
                "cancellation/return event(s). Reorder cautiously.")

    if score >= 70 and confidence < 0.5:
        return ("medium",
                f"⚠️ Score {score:.0f}/100 but confidence "
                f"{confidence:.0%}. Strong recent interest but few "
                "sources/customers — verify before scaling reorder.")

    # --- WATCH levels (informational) ---
    if score >= 60:
        return ("watch",
                f"👀 Score {score:.0f}/100 (confidence "
                f"{confidence:.0%}). Strong demand signals — track.")

    if score >= 40 and reorder_suggested > 0:
        return ("watch",
                f"👀 Score {score:.0f}/100 — moderate demand. "
                "Suggested reorder qty looks aligned.")

    # No warning
    return (None, "")


# ---------------------------------------------------------------------------
# Human-readable explanation
# ---------------------------------------------------------------------------

def explain_score(score_dict: dict) -> str:
    """Return a 1-paragraph human explanation of the score breakdown."""
    if not score_dict or not score_dict.get("n_signals"):
        return "No signals in window — score is 0."
    s = score_dict.get("score", 0)
    c = score_dict.get("confidence", 0)
    n = score_dict.get("n_signals", 0)
    src_n = score_dict.get("distinct_sources", 0)
    cust_n = score_dict.get("distinct_customers", 0)
    by_type = score_dict.get("breakdown", {}).get("by_type", {}) or {}
    by_source = score_dict.get("breakdown", {}).get("by_source",
                                                       {}) or {}
    type_summary = ", ".join(
        f"{k}={v}" for k, v in sorted(
            by_type.items(), key=lambda x: -x[1])[:5])
    source_summary = ", ".join(
        f"{k}={v}" for k, v in sorted(
            by_source.items(), key=lambda x: -x[1])[:5])
    qe = score_dict.get("quality_events", 0)
    conv = score_dict.get("components", {}).get("conversion_rate", 0)
    parts = [
        f"Score {s:.0f}/100 (confidence {c:.0%}).",
        f"{n} signal{'s' if n != 1 else ''} from "
        f"{src_n} source{'s' if src_n != 1 else ''}, "
        f"{cust_n} distinct customer{'s' if cust_n != 1 else ''}.",
    ]
    if type_summary:
        parts.append(f"Types: {type_summary}.")
    if source_summary and src_n > 1:
        parts.append(f"Sources: {source_summary}.")
    if conv > 0:
        parts.append(f"Conversion rate: {conv:.0%}.")
    if qe > 0:
        parts.append(
            f"⚠ {qe} quality event{'s' if qe != 1 else ''} "
            f"(cancellation/return/complaint).")
    return " ".join(parts)
