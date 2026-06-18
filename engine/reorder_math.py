"""Small reorder-math helpers shared by the dashboard engine.

The dashboard keeps most ordering logic in app.py today. These helpers are
kept Streamlit-free so the subtle quantity rules can be unit-tested without
importing the full app and loading CSV snapshots.
"""

from __future__ import annotations


BULK_RESIDUE_FLOOR_METRES = 5.0
UNIT_EPSILON = 1e-6
NEONICA_FRACTIONAL_ROLL_METRES = 100.0


def bulk_residue_floor_units(is_bulk_master: bool,
                             bulk_length_m: float,
                             *,
                             floor_metres: float = (
                                 BULK_RESIDUE_FLOOR_METRES)) -> float:
    """Return the bulk-roll quantity below which stock is just residue.

    Example: for a 100m master roll, 5m = 0.05 rolls. Anything below that
    is too small to buy/sell/plan around and should not create an overstock
    signal.
    """
    try:
        length = float(bulk_length_m or 0)
    except (TypeError, ValueError):
        length = 0.0
    if not is_bulk_master or length <= 0:
        return UNIT_EPSILON
    return max(UNIT_EPSILON, float(floor_metres) / length)


def normalise_planning_quantity(quantity,
                                *,
                                is_bulk_master: bool = False,
                                bulk_length_m: float = 0.0) -> float:
    """Treat tiny quantities as zero for planning/status purposes."""
    try:
        qty = float(quantity or 0)
    except (TypeError, ValueError):
        return 0.0
    floor = bulk_residue_floor_units(is_bulk_master, bulk_length_m)
    if abs(qty) < floor:
        return 0.0
    return qty


def excess_units_over_target(onhand,
                             target,
                             *,
                             is_bulk_master: bool = False,
                             bulk_length_m: float = 0.0) -> float:
    """Excess stock after ignoring non-actionable bulk-roll residue."""
    try:
        excess = max(0.0, float(onhand or 0) - float(target or 0))
    except (TypeError, ValueError):
        return 0.0
    return normalise_planning_quantity(
        excess,
        is_bulk_master=is_bulk_master,
        bulk_length_m=bulk_length_m,
    )


def fractional_bulk_order_allowed(supplier_name,
                                  is_bulk_master: bool,
                                  bulk_length_m,
                                  supplier_config: dict | None = None) -> bool:
    """Return whether a bulk roll can be ordered as a decimal quantity.

    Neonica sells 100m rolls by partial-roll quantity in CIN7 PO terms:
    40m required is ordered as 0.40 of a 100m roll, not rounded to 1.00.
    Keep that rule explicit so supplier config changes cannot accidentally
    turn Neonica 100m rolls into full-roll-only buys.
    """
    try:
        length_m = float(bulk_length_m or 0)
    except (TypeError, ValueError):
        length_m = 0.0
    if not is_bulk_master or length_m <= 0:
        return False

    supplier_key = " ".join(str(supplier_name or "").lower().split())
    if ("neonica" in supplier_key
            and abs(length_m - NEONICA_FRACTIONAL_ROLL_METRES) < 0.001):
        return True

    cfg = supplier_config or {}
    return bool(cfg.get("allow_fractional_qty", True))
