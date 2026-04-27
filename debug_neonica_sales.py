import pandas as pd, glob

skus = ["LEDIRIS2200-180-0305", "LEDIRIS2200-180-5m"]
files = sorted(glob.glob("output/sale_lines_*.csv"))
print(f"Reading {len(files)} sale_lines files...\n")

sl = pd.concat([pd.read_csv(f, low_memory=False) for f in files], ignore_index=True)
if "LineID" in sl.columns:
    sl = sl.drop_duplicates(subset=["LineID"], keep="last")

sku_col = "SKU" if "SKU" in sl.columns else "Code"
qty_col = "Quantity" if "Quantity" in sl.columns else "Qty"
date_candidates = [c for c in ["OrderDate","Date","SaleDate","CreatedDate"] if c in sl.columns]
date_col = date_candidates[0] if date_candidates else None

for target in skus:
    rows = sl[sl[sku_col].astype(str).str.lower() == target.lower()]
    print(f"=== {target} ===")
    print(f"Total sales rows ever: {len(rows)}")
    if date_col and len(rows) > 0:
        rows = rows.copy()
        rows[date_col] = pd.to_datetime(rows[date_col], errors="coerce")
        valid = rows.dropna(subset=[date_col])
        if len(valid):
            print(f"  First sale: {valid[date_col].min().strftime('%Y-%m-%d')}")
            print(f"  Last sale:  {valid[date_col].max().strftime('%Y-%m-%d')}")
        for days in [30, 90, 180, 365, 730, 1825]:
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
            w = valid[valid[date_col] >= cutoff]
            qty = w[qty_col].sum() if qty_col in w.columns else "n/a"
            print(f"  Last {days:>4} days: {len(w):>4} rows, qty: {qty}")
        print("  Sales by year:")
        valid["year"] = valid[date_col].dt.year
        for year, group in valid.groupby("year"):
            qty = group[qty_col].sum() if qty_col in group.columns else "n/a"
            print(f"    {year}: {len(group):>4} rows, qty {qty}")
    print()
