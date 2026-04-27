"""Mimic the running engine's dormancy logic step-by-step on the target
SKU. Every intermediate value is printed so we can see WHERE is_dormant
goes wrong vs my expectation."""
import pandas as pd
import glob
from datetime import datetime

target_master = "LEDIRIS2200-180-5m"
target_child = "LEDIRIS2200-180-0305"

today = pd.Timestamp(datetime.now().date())
cutoff_365 = today - pd.Timedelta(days=365)
cutoff_90 = today - pd.Timedelta(days=90)

# 1. Load sale_lines (same as engine)
sl_files = sorted(glob.glob("output/sale_lines_*.csv"))
sl = pd.concat([pd.read_csv(f, low_memory=False) for f in sl_files],
                ignore_index=True)
if "LineID" in sl.columns:
    sl = sl.drop_duplicates(subset=["LineID"], keep="last")
sl["InvoiceDate"] = pd.to_datetime(sl["InvoiceDate"], errors="coerce")
sl = sl.dropna(subset=["InvoiceDate"])
# Apply same Status filter as engine
if "Status" in sl.columns:
    excluded = ("CREDITED", "VOIDED", "CANCELLED")
    sl = sl[~sl["Status"].astype(str).str.upper().isin(excluded)]
sl["Quantity"] = pd.to_numeric(sl["Quantity"], errors="coerce").fillna(0)

# 2. Filter to 365-day window (engine's primary window)
sl_window = sl[sl["InvoiceDate"] >= cutoff_365]

# 3. Compute units_12mo and units_90d (per SKU, in same way as engine)
u12 = sl_window.groupby("SKU")["Quantity"].sum()
u90 = sl_window[sl_window["InvoiceDate"] >= cutoff_90].groupby(
    "SKU")["Quantity"].sum()

# 4. Load BOM
bom_files = sorted(glob.glob("output/boms_*.csv"))
bom = pd.read_csv(bom_files[-1])
print(f"BOM file: {bom_files[-1]}")

bom_components_by_asm = {}
for _, b in bom.iterrows():
    asm = b.get("AssemblySKU")
    comp = b.get("ComponentSKU")
    qty = b.get("Quantity")
    if asm and comp and pd.notna(qty):
        bom_components_by_asm.setdefault(asm, []).append((comp, float(qty)))

# 5. For target_master, compute the rollup
print(f"\n=== Investigating {target_master} ===")

# Direct sales of master
master_u12 = float(u12.get(target_master, 0))
master_u90 = float(u90.get(target_master, 0))
print(f"  Direct units_12mo: {master_u12}")
print(f"  Direct units_90d:  {master_u90}")

# Now find every assembly whose BOM points to target_master
# (i.e. which children roll up into target_master?)
children_rolling_up = []
for asm_sku, components in bom_components_by_asm.items():
    for comp_sku, qty_per in components:
        if comp_sku == target_master:
            children_rolling_up.append((asm_sku, qty_per))

print(f"\n  Children whose BOM points to {target_master}:")
for child_sku, qty_per in children_rolling_up:
    child_u12 = float(u12.get(child_sku, 0))
    child_u90 = float(u90.get(child_sku, 0))
    print(f"    {child_sku} (qty {qty_per:g}): "
          f"u12={child_u12:.0f}, u90={child_u90:.0f}, "
          f"contribs 12mo={child_u12 * qty_per:.2f}, "
          f"90d={child_u90 * qty_per:.2f}")

# Total rollup contribution
rollup_12 = sum(float(u12.get(child, 0)) * qty
                for child, qty in children_rolling_up)
rollup_90 = sum(float(u90.get(child, 0)) * qty
                for child, qty in children_rolling_up)

print(f"\n  Total rollup_in (BOM-based):")
print(f"    12mo: {rollup_12:.2f}")
print(f"    90d:  {rollup_90:.2f}")

# 6. Strip-rollup math (length-based)
# For LEDIRIS2200-180-5m, length is 5m. -0305 is 0.305m. Bulk = 5m.
# consumption_m = own_units * 0.305
# consumption_in_master_units = consumption_m / 5
strip_rollup_12 = (float(u12.get(target_child, 0)) * 0.305) / 5
strip_rollup_90 = (float(u90.get(target_child, 0)) * 0.305) / 5
print(f"\n  Strip rollup (length-based, -0305 → -5m, 0.305/5):")
print(f"    12mo: {strip_rollup_12:.2f}")
print(f"    90d:  {strip_rollup_90:.2f}")

# 7. Combined: BOTH paths fire (potential double-counting)
total_rollup_12 = rollup_12 + strip_rollup_12
total_rollup_90 = rollup_90 + strip_rollup_90
print(f"\n  TOTAL rollup (BOM + strip combined — engine adds both):")
print(f"    12mo: {total_rollup_12:.2f}")
print(f"    90d:  {total_rollup_90:.2f}")

# 8. Effective demand
eff_12mo = master_u12 + total_rollup_12  # Skipping migration for simplicity
eff_90d = master_u90 + total_rollup_90

print(f"\n  Effective demand:")
print(f"    eff_12mo: {eff_12mo:.2f}")
print(f"    eff_90d:  {eff_90d:.2f}")

# 9. Dormancy check
rate_12 = eff_12mo / 365.0
rate_90 = eff_90d / 90.0
ratio = (rate_90 / rate_12) if rate_12 > 0 else 0
is_dormant = (eff_12mo > 0 and rate_12 >= 0.05
              and rate_90 < 0.20 * rate_12)

print(f"\n  Dormancy check:")
print(f"    rate_12mo: {rate_12:.4f} (threshold ≥ 0.05)")
print(f"    rate_90d:  {rate_90:.4f}")
print(f"    ratio:     {ratio*100:.1f}% (must be < 20% for dormant)")
print(f"    is_dormant: {is_dormant}")

# Also check what the engine's avg_daily would be (without dormancy)
avg_daily_old = eff_12mo / 365.0
print(f"\n  WITHOUT dormancy: avg_daily = {avg_daily_old:.3f}")
print(f"  WITH dormancy:    avg_daily = {eff_90d / 90.0:.3f}")
