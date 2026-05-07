"""dimension_describer.py (v2.67.72)
=========================================

Generates structured 'Parameter: Value' dimension blocks for every
active SKU, ready for the Shopify content team to paste into
product descriptions.

User context (from Shopify Agentic Commerce webinar takeaway):
  "Structured data (not hidden, simply clearly ordered Parameter:
  Value bullet points) is easily digested by AI and humans. So we
  are on the right track. If we want AI to be able to digest
  dimensions, then we need to add those to our descriptions
  explicitly. Rather than in diagrams and cut sheets only."

This tool reads CIN7's product master, pulls the existing
Length / Width / Height / Weight fields where present, and
generates a draft markdown block per SKU. For complex shapes
(channels with wings, mud-in profiles, kits), the draft is
flagged with NeedsManualElaboration=TRUE so a human reviews
the wing/flange/mounting specifics before publishing.

CLI:
  python dimension_describer.py
    --output dimension_descriptions.csv  (default)

Output columns:
  SKU
  Name
  Family
  Type
  CurrentLength_mm
  CurrentWidth_mm
  CurrentHeight_mm
  CurrentWeight_g
  StructuredDimensions   (markdown, ready to paste)
  NeedsManualElaboration (TRUE / FALSE)
  ElaborationReason      (what to hand-fill, e.g. 'wings/flanges')
  ShopifyHandle          (if known)

Workflow for the content team:
1. Run the tool. Output CSV in /data/output/dimension_descriptions.csv.
2. Open in Excel. Filter to a category (e.g. all LED-V* channels).
3. For each row marked NeedsManualElaboration=TRUE, hand-fill
   the missing wing/mounting/depth specifics in the
   StructuredDimensions cell.
4. Copy the final block into the Shopify product description
   (preserve any existing copy; append the structured block at
   the bottom or after current dimension info).
5. Save Shopify product. AI can now answer dimensional questions
   like "will this fit in a 30mm gap?" from the description text.

Future: a Shopify-import CSV format that updates descriptions in
bulk, scoped to v2.68+.
"""

from __future__ import annotations

import argparse
import csv
import glob
import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from data_paths import OUTPUT_DIR  # noqa: E402

LOG_FORMAT = "%(asctime)s  %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("dimension_describer")


# ---------------------------------------------------------------------------
# Heuristics for "needs manual elaboration"
# ---------------------------------------------------------------------------
# Families / categories where the auto-extracted W×H from CIN7 misses
# important shape information that the AI needs to answer real
# questions. The team should hand-fill these before publishing.

# Channel families known to have mud-in flanges or wings.
_MUD_IN_FAMILIES = {
    "V3140020",       # Plaster-in Drywall LED Channel
    "V3060001",       # Surface mount but with various flange options
    "TL-539",         # Trimless Mud-in
    "TL-",            # other Trimless variants
    "PLASTER",        # any explicit plaster-in
    "DRYWALL",        # any drywall
    "MUDIN",
    "RECESSED",
    "LED-AB",         # mud-in adjustable beam
    "LED-AL",         # asymmetric flange
}

# Patterns indicating a multi-component kit — needs per-component
# breakdown, not just outer-box dimensions.
_KIT_PATTERNS = ("LEDKIT-", "-KIT-", "SET", "BUNDLE")

# Family prefixes for endcaps — dimensions are specific to the
# channel they cap, so just W/H from CIN7 is misleading.
_ENDCAP_PATTERNS = ("END", "ENDCAP", "CAP-", "-CAP")

# Diffuser / cover families — need diffusion type, light loss %,
# clip-in vs slide-in profile.
_DIFFUSER_FAMILIES = {
    "V3000938S",      # LED Channel Cover
    "DIFFUSER",
    "COVER-",
    "OPAL",
    "FROSTED",
    "CLEAR-",
}

# Strip / spool products — need IP rating, watts/m, voltage, LEDs/m
# beyond just length.
_STRIP_PATTERNS = ("LEDIRIS", "LEDWLWW", "LED-WL", "LED-EG",
                    "LED-HS", "LED-DECOR", "LED-LIATRIS",
                    "LED-CARDINAL", "LED-BALTIC", "LED-SAUNA")


def _classify_elaboration(sku: str, family: str, name: str) -> tuple:
    """Decide if a SKU needs human review for additional dimensions.
    Returns (needs_manual: bool, reason: str)."""
    s = (sku or "").upper()
    f = (family or "").upper()
    n = (name or "").upper()

    # Kits — need component breakdown.
    for pat in _KIT_PATTERNS:
        if pat in s:
            return (True,
                    "Kit — list each component's dimensions + "
                    "compatibility")
    # Mud-in / plaster-in / trimless — wings/flanges.
    for fam in _MUD_IN_FAMILIES:
        if fam in s or fam in f or fam in n:
            return (True,
                    "Mud-in / plaster-in / trimless — add wing or "
                    "flange extension dimensions, mounting depth, "
                    "cover compatibility")
    # Endcaps — channel-specific.
    for pat in _ENDCAP_PATTERNS:
        if pat in s:
            return (True,
                    "Endcap — specify the channel SKU it fits, "
                    "and confirm hole / no-hole variant")
    # Diffusers — diffusion type + LL%.
    for pat in _DIFFUSER_FAMILIES:
        if pat in s or pat in f:
            return (True,
                    "Diffuser/cover — specify diffusion type "
                    "(opal / frosted / clear), light loss %, "
                    "and clip vs slide profile")
    # LED strips — IP rating + voltage + LEDs/m.
    for pat in _STRIP_PATTERNS:
        if pat in s:
            return (True,
                    "LED strip — confirm IP rating, voltage, W/m, "
                    "LEDs/m, kelvin, cuttable interval")

    # Otherwise auto block is probably sufficient.
    return (False, "")


# ---------------------------------------------------------------------------
# Structured-block generator
# ---------------------------------------------------------------------------


def _to_mm(val, units: str) -> Optional[float]:
    """Convert a length value to mm, given CIN7's unit field
    (sometimes 'mm', sometimes 'cm', sometimes blank)."""
    if val in (None, "", "0", 0):
        return None
    try:
        v = float(val)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    u = (units or "").lower().strip()
    if u in ("cm",):
        return round(v * 10.0, 1)
    if u in ("m",):
        return round(v * 1000.0, 1)
    if u in ("in", "inch", "inches"):
        return round(v * 25.4, 1)
    if u in ("ft", "feet"):
        return round(v * 304.8, 1)
    # Default: assume mm (CIN7's most common shape).
    return round(v, 1)


def _to_grams(val, units: str) -> Optional[float]:
    if val in (None, "", "0", 0):
        return None
    try:
        v = float(val)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    u = (units or "").lower().strip()
    if u in ("kg",):
        return round(v * 1000.0, 1)
    if u in ("oz",):
        return round(v * 28.3495, 1)
    if u in ("lb", "lbs", "pound", "pounds"):
        return round(v * 453.592, 1)
    return round(v, 1)


def _build_structured_block(name: str, family: str,
                                length_mm: Optional[float],
                                width_mm: Optional[float],
                                height_mm: Optional[float],
                                weight_g: Optional[float],
                                needs_manual: bool,
                                manual_reason: str
                                ) -> str:
    """Render the markdown 'Parameter: Value' block."""
    lines = ["**Dimensions**"]
    if length_mm is not None:
        lines.append(f"- Length: {length_mm:.0f} mm")
    if width_mm is not None:
        lines.append(f"- Width: {width_mm:.0f} mm")
    if height_mm is not None:
        lines.append(f"- Height: {height_mm:.0f} mm")
    if weight_g is not None:
        # Render as kg if > 1000 g.
        if weight_g >= 1000:
            lines.append(f"- Weight: {weight_g/1000:.2f} kg")
        else:
            lines.append(f"- Weight: {weight_g:.0f} g")
    if family and family.strip() and family.lower() != "nan":
        lines.append(f"- Family: {family}")
    if needs_manual:
        lines.append("")
        lines.append(f"**[TODO]** {manual_reason}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def find_latest_products_csv() -> Optional[Path]:
    pattern = str(OUTPUT_DIR / "products_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        return None
    return Path(files[-1])


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Generate structured dimension blocks per SKU.")
    p.add_argument("--input", default=None,
                     help="Path to products CSV (auto-detects latest).")
    p.add_argument("--output", default=None,
                     help="Output CSV path "
                          "(default: OUTPUT_DIR/dimension_descriptions.csv).")
    p.add_argument("--active-only", action="store_true", default=True,
                     help="Only include active products (default).")
    p.add_argument("--limit", type=int, default=0,
                     help="Limit row count (0 = all). Useful for testing.")
    args = p.parse_args(argv)

    in_path = (Path(args.input) if args.input
                 else find_latest_products_csv())
    if not in_path or not in_path.exists():
        log.error("No products CSV found at %s — run cin7_sync first",
                    in_path)
        return 1

    log.info("Reading %s", in_path)
    df = pd.read_csv(in_path, low_memory=False)
    log.info("Loaded %d products", len(df))

    if args.active_only and "Status" in df.columns:
        df = df[df["Status"].astype(str).str.upper() == "ACTIVE"]
        log.info("Active filter: %d remain", len(df))

    # v2.67.72 — filter to actual LED products. Skip:
    # - Service / accounting / billable expense entries
    # - Deleted / API-test placeholder rows
    # - SKUs that don't start with LED- (or LEDKIT-)
    if "SKU" in df.columns:
        sku_upper = df["SKU"].astype(str).str.upper()
        df = df[sku_upper.str.startswith(("LED-", "LEDKIT-"))]
        log.info("LED-product filter: %d remain", len(df))
    if "Name" in df.columns:
        name_upper = df["Name"].astype(str).str.upper()
        skip_terms = ("DELETED", "API TEST", "TEST PRODUCT",
                        "BILLABLE EXPENSE", "(DELETED)")
        skip_mask = name_upper.apply(
            lambda n: any(t in n for t in skip_terms))
        df = df[~skip_mask]
        log.info("Skip-deleted/test filter: %d remain", len(df))
    if "Type" in df.columns:
        df = df[df["Type"].astype(str).str.upper() != "SERVICE"]
        log.info("Skip-service filter: %d remain", len(df))

    if args.limit > 0:
        df = df.head(args.limit)

    out_path = (Path(args.output) if args.output
                  else OUTPUT_DIR / "dimension_descriptions.csv")

    rows_written = 0
    needs_review = 0
    auto_only = 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "SKU", "Name", "Family", "Type",
            "CurrentLength_mm", "CurrentWidth_mm",
            "CurrentHeight_mm", "CurrentWeight_g",
            "StructuredDimensions",
            "NeedsManualElaboration", "ElaborationReason",
            "ShopifyHandle",
        ])

        for _, r in df.iterrows():
            sku = str(r.get("SKU", ""))
            if not sku:
                continue
            name = str(r.get("Name", ""))
            family_raw = r.get("AdditionalAttribute1")
            family = (str(family_raw) if family_raw is not None
                        and str(family_raw).strip() != "nan"
                        else "")
            ptype = str(r.get("Type", ""))
            l_mm = _to_mm(
                r.get("Length"),
                str(r.get("DimensionsUnits") or "mm"))
            w_mm = _to_mm(
                r.get("Width"),
                str(r.get("DimensionsUnits") or "mm"))
            h_mm = _to_mm(
                r.get("Height"),
                str(r.get("DimensionsUnits") or "mm"))
            wg_g = _to_grams(
                r.get("Weight"),
                str(r.get("WeightUnits") or "g"))

            needs_manual, reason = _classify_elaboration(
                sku, family, name)
            block = _build_structured_block(
                name, family, l_mm, w_mm, h_mm, wg_g,
                needs_manual, reason)

            writer.writerow([
                sku, name, family, ptype,
                f"{l_mm:.1f}" if l_mm is not None else "",
                f"{w_mm:.1f}" if w_mm is not None else "",
                f"{h_mm:.1f}" if h_mm is not None else "",
                f"{wg_g:.1f}" if wg_g is not None else "",
                block,
                "TRUE" if needs_manual else "FALSE",
                reason,
                "",  # ShopifyHandle — populated when we have a
                       # cross-ref against shopify products
            ])
            rows_written += 1
            if needs_manual:
                needs_review += 1
            else:
                auto_only += 1

    log.info("Wrote %d rows to %s", rows_written, out_path)
    log.info("  needs manual review: %d (%.1f%%)",
              needs_review,
              100.0 * needs_review / max(1, rows_written))
    log.info("  auto-block sufficient: %d (%.1f%%)",
              auto_only,
              100.0 * auto_only / max(1, rows_written))
    return 0


if __name__ == "__main__":
    sys.exit(main())
