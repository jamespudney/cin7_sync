"""Business sales exclusions shared by reporting and demand tools."""

from __future__ import annotations

import re
import unicodedata
from typing import Iterable

import pandas as pd


EXCLUDED_SALES_CUSTOMERS = ("Altar'd State",)
_EXCLUDED_CUSTOMER_KEYS = {"ALTARDSTATE"}


def _normalise_customer_name(value: object) -> str:
    """Normalise customer names so apostrophe variants match reliably."""
    if value is None or pd.isna(value):
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = text.replace("’", "'").replace("`", "'")
    text = text.upper()
    return re.sub(r"[^A-Z0-9]+", "", text)


def excluded_sales_customer_mask(
    df: pd.DataFrame | None,
    *,
    customer_columns: Iterable[str] = ("Customer",),
) -> pd.Series:
    """Return True for sales rows excluded from company analytics."""
    if df is None:
        return pd.Series(dtype=bool)
    if df.empty:
        return pd.Series(False, index=df.index)

    mask = pd.Series(False, index=df.index)
    for col in customer_columns:
        if col not in df.columns:
            continue
        keys = df[col].map(_normalise_customer_name)
        mask = mask | keys.isin(_EXCLUDED_CUSTOMER_KEYS)
    return mask


def filter_excluded_sales_customers(
    df: pd.DataFrame | None,
    *,
    customer_columns: Iterable[str] = ("Customer",),
) -> pd.DataFrame:
    """Remove sales rows for customers excluded from W4S analytics."""
    if df is None:
        return pd.DataFrame()
    if df.empty:
        return df
    mask = excluded_sales_customer_mask(df, customer_columns=customer_columns)
    if not bool(mask.any()):
        return df
    return df.loc[~mask].copy()
