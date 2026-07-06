"""Shared dashboard configuration.

Keep page metadata out of ``app.py`` so navigation, permissions, and
profile defaults all read from one small source of truth.
"""

from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
import os
from pathlib import Path
import subprocess


_STATIC_APP_VERSION = "v2.67.360"
_STATIC_APP_DEPLOYED = "2026-06-03"


def _git_output(*args: str) -> str:
    try:
        return subprocess.check_output(
            ["git", *args],
            cwd=Path(__file__).resolve().parent,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2,
        ).strip()
    except Exception:
        return ""


def _short_commit(value: str) -> str:
    text = (value or "").strip()
    return text[:7] if text else ""


def _normalise_date(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    if text.isdigit():
        try:
            return datetime.fromtimestamp(int(text)).strftime("%Y-%m-%d")
        except Exception:
            return ""
    return text[:10]


def _build_commit() -> str:
    for key in (
        "APP_BUILD_COMMIT",
        "RENDER_GIT_COMMIT",
        "COMMIT_SHA",
        "GIT_COMMIT",
        "SOURCE_VERSION",
    ):
        commit = _short_commit(os.environ.get(key, ""))
        if commit:
            return commit
    return _short_commit(_git_output("rev-parse", "--short=7", "HEAD"))


def _build_date() -> str:
    for key in (
        "APP_BUILD_DATE",
        "RENDER_DEPLOYED_AT",
        "RENDER_DEPLOY_CREATED_AT",
        "BUILD_DATE",
        "SOURCE_DATE_EPOCH",
    ):
        deployed = _normalise_date(os.environ.get(key, ""))
        if deployed:
            return deployed
    deployed = _normalise_date(
        _git_output("show", "-s", "--format=%cs", "HEAD"))
    if deployed:
        return deployed
    try:
        return datetime.fromtimestamp(
            Path(__file__).stat().st_mtime).strftime("%Y-%m-%d")
    except Exception:
        return _STATIC_APP_DEPLOYED


def _app_version_label() -> str:
    explicit = os.environ.get("APP_VERSION", "").strip()
    if explicit:
        return explicit
    commit = _build_commit()
    return f"build {commit}" if commit else _STATIC_APP_VERSION


APP_VERSION = _app_version_label()
APP_DEPLOYED = os.environ.get("APP_DEPLOYED", "").strip() or _build_date()


PAGE_GROUPS = OrderedDict({
    "Command Center": [
        "Overview",
        "AI Assistant",
        "Data Health",
    ],
    "Buying": [
        "Ordering",
        "Anodizing & Powder coating",
        "Slow Movers",
        "Supplier Pricing",
        "FixedCost Audit",
        "Migrations",
    ],
    "Product Intelligence": [
        "Product Detail",
        "Stock Explorer",
        "Product Master",
        "Kits & Fixtures",
        "LED Tubes",
        "Demand Signals",
    ],
    "Sales & Marketing": [
        "Sales Recent",
        "Monthly Metrics",
        "Ad-Umpire",
    ],
    "Finance": [
        "Cashflow",
        "Purchase Analysis",
    ],
    "Admin": [
        "My Profile",
        "AI Feedback",
        "User Permissions",
    ],
})


PAGE_DESCRIPTIONS = {
    "Overview": "High-level KPIs: stock value, sales, slow movers, today vs YoY.",
    "AI Assistant": "Natural-language Q&A grounded in live CIN7 + Shopify data.",
    "Data Health": "Sync freshness, CSV row counts, data integrity flags.",
    "Ordering": "ABC-driven reorder workbench with PO drafts.",
    "Anodizing & Powder coating": (
        "BOM-driven coating/anodizing work queue for finished SKU replenishment."
    ),
    "Slow Movers": "Stock-reduction workspace: dormant SKUs, value tied up, dismiss/flag.",
    "Supplier Pricing": "Per-SKU supplier costs, freight modes, lead times.",
    "FixedCost Audit": "Review fixed-cost overrides applied to PO calculations.",
    "Migrations": "Retiring -> successor SKU mappings + impact.",
    "Product Detail": "Single-SKU drill-down: stock, sales, BOM, pricing, history.",
    "Stock Explorer": "Searchable stock view with FIFO values + flags.",
    "Product Master": "Browse / search products with categories + statuses.",
    "Kits & Fixtures": "Pre-built kits, components, build candidates.",
    "LED Tubes": "Tube families, MP variants, migration forecast.",
    "Demand Signals": "Track customer interest before it shows up in sales.",
    "Sales Recent": "Recent sales feed + filters.",
    "Monthly Metrics": "Month-over-month KPI report — commission reference.",
    "Ad-Umpire": "Paid-ads dashboard: Google Ads + GA4 attribution + ROAS.",
    "Cashflow": "Cash position + forecast, backed by QuickBooks Online.",
    "Purchase Analysis": "PO-side analytics: spend by supplier, lead-time variance.",
    "My Profile": "Edit your profile; admins manage all users.",
    "AI Feedback": "Review past AI answers and team-logged corrections.",
    "User Permissions": "Admin: assign which pages each user can access.",
}


PAGE_OPTIONS = [
    page
    for pages in PAGE_GROUPS.values()
    for page in pages
]

PAGE_CAPTIONS = [PAGE_DESCRIPTIONS[page] for page in PAGE_OPTIONS]

PAGE_GROUP_BY_NAME = {
    page: group
    for group, pages in PAGE_GROUPS.items()
    for page in pages
}
