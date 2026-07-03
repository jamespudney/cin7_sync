"""SKU parsing and sourcing-rule helpers.

These helpers are used by the dashboard engine, LED tube pages, and
ordering workflow. Keeping them Streamlit-free makes them testable and
reusable from the Slack bot or background workers later.
"""

from __future__ import annotations

import re
from typing import Optional


def parse_sourcing_rule(attr1: Optional[str]) -> dict:
    """Parse CIN7 AdditionalAttribute1 sourcing-rule text."""
    out = {
        "RuleCode": None,
        "Logic": None,
        "IsMaster": False,
        "SourceFraction": None,
        "SourceLengthMM": None,
        "HasPlate": False,
        "AutoAssembly": None,
        "Note": None,
    }
    if not attr1 or not isinstance(attr1, str):
        return out
    source = attr1.strip()
    if not source:
        return out
    for segment in source.split("|"):
        seg = segment.strip()
        if not seg:
            continue
        lower = seg.lower()
        if lower.startswith("rule:"):
            out["RuleCode"] = seg.split(":", 1)[1].strip()
        elif lower.startswith("logic:"):
            logic = seg.split(":", 1)[1].strip()
            out["Logic"] = logic
            low = logic.lower()
            if "purchas" in low:
                out["IsMaster"] = True
            else:
                match = re.search(
                    r"([\d.]+)\s*x\s*([\d.]+)\s*(mm|m|ft)?",
                    logic,
                    flags=re.IGNORECASE,
                )
                if match:
                    try:
                        frac = float(match.group(1))
                    except ValueError:
                        frac = None
                    try:
                        length_value = float(match.group(2))
                    except ValueError:
                        length_value = None
                    unit = (match.group(3) or "").lower()
                    out["SourceFraction"] = frac
                    if length_value is not None:
                        if unit == "m":
                            out["SourceLengthMM"] = int(round(length_value * 1000))
                        elif unit == "ft":
                            out["SourceLengthMM"] = int(round(length_value * 304.8))
                        else:
                            out["SourceLengthMM"] = (
                                int(round(length_value * 1000))
                                if length_value < 20 else int(round(length_value))
                            )
            if "plate" in low:
                out["HasPlate"] = True
        elif lower.startswith("auto-assembly:"):
            out["AutoAssembly"] = seg.split(":", 1)[1].strip()
        elif lower.startswith("note:"):
            out["Note"] = seg.split(":", 1)[1].strip()
    return out


def _parse_length(value) -> Optional[int]:
    """Return length in mm. ``1`` -> 1000, ``0609`` -> 609."""
    if value is None:
        return None
    try:
        number = float(str(value).strip())
    except (ValueError, TypeError):
        return None
    if number <= 0:
        return None
    return int(round(number * 1000)) if number < 20 else int(round(number))


TUBE_FAMILY_NAME_KEYWORDS = [
    ("OSLO MINI", "OSLOMINI"),
    ("OSLO DOBLE", "OSLODOBLE"),
    ("OSLO DOUBLE", "OSLODOBLE"),
    ("OSLOMINI", "OSLOMINI"),
    ("OSLODOBLE", "OSLODOBLE"),
]

NON_TUBE_NAME_PATTERNS = (
    "END CAP",
    "ENDCAP",
    "HEATSINK",
    "HEAT PLATE",
    "MOUNTING PLATE FOR",
    "MOUNT PLATE FOR",
    "BASE FOR",
    "JOINER",
    "ADAPTOR",
    "ADAPTER",
    "CLIP FOR",
    "SWIVEL",
    "SLIDE ",
    "BRACKET",
)


def _parse_tube_sku(sku: str, name: str = "") -> Optional[dict]:
    """Identify tube metadata from SKU/name."""
    if not sku or not isinstance(sku, str):
        return None
    sku_upper = sku.upper()
    name_upper = (name or "").upper()
    if any(tok in name_upper for tok in NON_TUBE_NAME_PATTERNS):
        return None

    if sku_upper.startswith("LED-"):
        parts = sku_upper.split("-")
        if len(parts) >= 4:
            family = parts[1]
            length_mm = _parse_length(parts[-1])
            skipped_tokens = {
                "EC",
                "TJ",
                "CLIP",
                "SLIDE",
                "SWIVEL",
                "3D",
                "VEND",
                "ACCESSORY",
                "ACC",
                "ANODIZED",
                "HEATSINK",
            }
            middle = parts[2:-1]
            has_skipped = any(token in skipped_tokens for token in middle)
            if length_mm is not None and not has_skipped and middle:
                color = middle[0]
                has_mp = "MP" in middle
                if color in ("W", "B", "R", "C", "A", "G", "S", "BULK"):
                    return {
                        "SKU": sku,
                        "Family": family,
                        "Color": color,
                        "HasMP": has_mp,
                        "LengthMM": length_mm,
                    }

    family_from_name = None
    for keyword, family in TUBE_FAMILY_NAME_KEYWORDS:
        if keyword in name_upper:
            family_from_name = family
            break
    if not family_from_name:
        return None

    length_mm = None
    for part in reversed(sku_upper.split("-")):
        parsed = _parse_length(part)
        if parsed is not None and 50 <= parsed <= 5000:
            length_mm = parsed
            break
    if length_mm is None:
        match = re.search(r"(\d+(?:\.\d+)?)\s*(mm|m)\b", name_upper)
        if match:
            try:
                raw = float(match.group(1))
                unit = match.group(2).lower()
                length_mm = int(round(raw * 1000)) if unit == "m" else int(round(raw))
            except ValueError:
                pass
    if length_mm is None:
        return None

    color = "W"
    if "BLACK" in name_upper:
        color = "B"
    elif "CLEAR" in name_upper:
        color = "C"
    return {
        "SKU": sku,
        "Family": family_from_name,
        "Color": color,
        "HasMP": False,
        "LengthMM": length_mm,
    }


STRIP_FAMILY_PREFIXES = (
    "LEDIRIS",
    "LEDUL",
    "LED-UL",
    "LEDHR",
    "LEDAW",
    "LEDRGB",
    "LED-STRIP",
    "LED-TSB",
)

STRIP_NAME_KEYWORDS = ("STRIP", "LED TAPE", "FLEX LED")


def _is_strip_sku(sku: str, name: str) -> bool:
    """Heuristic: is this an LED-strip SKU?"""
    if not sku:
        return False
    sku_upper = str(sku).upper()
    name_upper = (name or "").upper()
    prefix_match = any(sku_upper.startswith(p) for p in STRIP_FAMILY_PREFIXES)
    name_match = any(k in name_upper for k in STRIP_NAME_KEYWORDS)
    return prefix_match or name_match


def _parse_strip_length_suffix(part: str) -> Optional[float]:
    """Turn a strip SKU suffix into length in metres."""
    if not part:
        return None
    suffix = str(part).strip().upper()
    if not suffix or suffix in ("12V", "24V"):
        return None
    if suffix.startswith("0") and suffix.isdigit() and len(suffix) >= 3:
        return int(suffix) / 1000.0
    core = suffix.rstrip("Mm")
    if core.replace(".", "", 1).isdigit():
        try:
            number = float(core)
        except ValueError:
            return None
        if len(core) >= 4 and "." not in core and not suffix.endswith(("m", "M")):
            return number / 1000.0
        return number
    return None


def _parse_strip_base(sku: str) -> Optional[tuple]:
    """Return ``(base_family, length_m)`` for strip SKUs."""
    if not sku:
        return None
    parts = str(sku).upper().split("-")
    if len(parts) < 2:
        return None

    voltage = None
    if parts[-1] in ("12V", "24V"):
        voltage = parts[-1]
        length_part = parts[-2]
        body = parts[:-2]
    else:
        length_part = parts[-1]
        body = parts[:-1]

    length_m = _parse_strip_length_suffix(length_part)
    if length_m is None:
        return None

    base = "-".join(body)
    if voltage:
        base = f"{base}-{voltage}"
    return (base, length_m)


BULK_STRIP_ROLL_MIN_LENGTH_M = 25.0


def is_bulk_strip_roll_length(length_m: float) -> bool:
    """True when a parsed strip length is a real buying-master roll.

    CIN7 BOMs remain the source of truth for kit/cut relationships. This
    guard is only for the naming fallback used when BOMs are absent: short
    finished lengths such as 1m/2m/2.35m must not be crowned as family
    masters just because they are the longest SKU in a naming family.
    """
    try:
        return float(length_m or 0) >= BULK_STRIP_ROLL_MIN_LENGTH_M
    except (TypeError, ValueError):
        return False


def parse_pack_purchase_sku(sku: str) -> Optional[tuple[str, int]]:
    """Return ``(base_sku, pack_size)`` for purchase-pack SKUs.

    Example: ``SNFX-L-CR-SCKT-X100`` is the 100-pack buying SKU for
    ``SNFX-L-CR-SCKT``. This is deliberately strict: it only matches a final
    ``-X<number>`` suffix so unrelated SKUs containing ``X100`` elsewhere do
    not get rolled together.
    """
    sku_s = str(sku or "").strip()
    if not sku_s:
        return None
    match = re.match(r"^(.+)-X(\d+)$", sku_s, flags=re.IGNORECASE)
    if not match:
        return None
    base = match.group(1).strip()
    try:
        pack_size = int(match.group(2))
    except ValueError:
        return None
    if not base or pack_size <= 1:
        return None
    return base, pack_size
