"""Storage-dimension field handling for CIN7 product data."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any, Iterable


STORAGE_DIM_FIELD = "Storage L x W x H In"

_TARGET_LABEL = "storage l x w x h in"
_TARGET_COMPACT = re.sub(r"[^a-z0-9]+", "", _TARGET_LABEL)
_DIM_UNIT = r"(?:\"|'|in(?:ches?)?)?"
_DIM_VALUE_RE = re.compile(
    rf"(?:\d+(?:\.\d+)?|_+)\s*{_DIM_UNIT}\s*[xX×]\s*"
    rf"(?:\d+(?:\.\d+)?|_+)"
)
_EMPTY_VALUES = {"", "<na>", "na", "nan", "none", "null", "nat"}


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if value != value:
            return ""
    except Exception:  # noqa: BLE001
        pass
    text = str(value).strip()
    if text.lower() in _EMPTY_VALUES:
        return ""
    return text


def _normalise_label(value: Any) -> str:
    text = _clean_text(value).lower().replace("_", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _compact_label(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _normalise_label(value))


def _is_storage_dim_key(key: Any) -> bool:
    label = _normalise_label(key)
    if label in {"storage dim", "storage dimension", _TARGET_LABEL}:
        return True
    compact = _compact_label(key)
    return bool(compact and compact.endswith(_TARGET_COMPACT))


def _is_additional_attribute_key(key: Any) -> bool:
    text = str(key or "")
    return bool(re.search(r"(?:^|[._\s])AdditionalAttribute\d+$", text, re.I))


def _json_if_possible(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text or text[0] not in "[{":
        return value
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _extract_from_positionals(mapping: Mapping[str, Any]) -> str:
    for i in range(1, 31):
        value = _clean_text(mapping.get(f"AdditionalAttribute{i}"))
        if value and _DIM_VALUE_RE.search(value):
            return value
    return ""


def _extract_from_attribute_blob(value: Any) -> str:
    attrs = _json_if_possible(value)
    if isinstance(attrs, list):
        for attr in attrs:
            if not isinstance(attr, Mapping):
                continue
            for key, val in attr.items():
                if _is_storage_dim_key(key):
                    cleaned = _clean_text(val)
                    if cleaned:
                        return cleaned
            name = next(
                (
                    _clean_text(attr.get(key))
                    for key in (
                        "Name",
                        "AttributeName",
                        "Attribute",
                        "Label",
                        "Key",
                    )
                    if _clean_text(attr.get(key))
                ),
                "",
            )
            if _is_storage_dim_key(name):
                for value_key in (
                    "Value",
                    "AttributeValue",
                    "Text",
                    "Content",
                ):
                    cleaned = _clean_text(attr.get(value_key))
                    if cleaned:
                        return cleaned
    if isinstance(attrs, Mapping):
        for key, val in attrs.items():
            if _is_storage_dim_key(key):
                cleaned = _clean_text(val)
                if cleaned:
                    return cleaned
        return _extract_from_positionals(attrs)
    return ""


def extract_storage_dim(record: Mapping[str, Any] | Any) -> str:
    """Return CIN7's Storage L x W x H In value from a product row."""
    if not isinstance(record, Mapping):
        return ""

    for key, value in record.items():
        if _is_storage_dim_key(key):
            cleaned = _clean_text(value)
            if cleaned:
                return cleaned

    for key, value in record.items():
        cleaned = _clean_text(value)
        if (
            _is_additional_attribute_key(key)
            and cleaned
            and _DIM_VALUE_RE.search(cleaned)
        ):
            return cleaned

    for key in (
        "AdditionalAttributes",
        "Additional Attributes",
        "Attributes",
        "ProductAdditionalAttributes",
    ):
        if key in record:
            dim = _extract_from_attribute_blob(record.get(key))
            if dim:
                return dim

    return _extract_from_positionals(record)


def storage_dim_source_columns(columns: Iterable[Any]) -> list[Any]:
    """Return columns that may contain CIN7's storage dimension value."""
    out: list[Any] = []
    for col in columns:
        label = _normalise_label(col)
        if (
            _is_storage_dim_key(col)
            or label in {
                "additionalattributes",
                "additional attributes",
                "attributes",
                "productadditionalattributes",
                "product additional attributes",
            }
            or _is_additional_attribute_key(col)
        ):
            out.append(col)
    return out


def ensure_storage_dim_column(df: Any) -> Any:
    """Populate df.storage_dim from CIN7's raw storage-dimension field."""
    if df is None:
        return df
    if "storage_dim" not in df.columns:
        df["storage_dim"] = ""
    else:
        df["storage_dim"] = df["storage_dim"].fillna("")

    if getattr(df, "empty", False):
        return df

    def _missing(value: Any) -> bool:
        return not bool(_clean_text(value))

    storage_dim_col = df.columns.get_loc("storage_dim")
    for row_pos, (_, row) in enumerate(df.iterrows()):
        if not _missing(row.get("storage_dim")):
            continue
        dim = extract_storage_dim(row.to_dict())
        if dim:
            df.iat[row_pos, storage_dim_col] = dim
    return df
