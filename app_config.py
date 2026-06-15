"""Shared dashboard configuration.

Keep page metadata out of ``app.py`` so navigation, permissions, and
profile defaults all read from one small source of truth.
"""

from __future__ import annotations

from collections import OrderedDict


APP_VERSION = "v2.67.360"
APP_DEPLOYED = "2026-06-03"


PAGE_GROUPS = OrderedDict({
    "Command Center": [
        "Overview",
        "AI Assistant",
        "Data Health",
    ],
    "Buying": [
        "Ordering",
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

