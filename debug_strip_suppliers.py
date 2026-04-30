"""See which suppliers are assigned to LED strip SKUs in the products data."""
import pandas as pd
import glob

prod_files = sorted(glob.glob("output/products_*.csv"))
if not prod_files:
    print("No products CSV files found. Run `python cin7_sync.py products`.")
    exit(0)

df = pd.read_csv(prod_files[-1], low_memory=False)
print(f"Reading: {prod_files[-1]}")
print(f"Total products: {len(df):,}\n")

# Filter to LED Strip products
strip = df[df["Name"].astype(str).str.contains(
    r"LED Strip|COB.*Strip|Bend.*Strip|Continuous COB|Bendable LED",
    case=False, regex=True, na=False)]
print(f"LED Strip-related SKUs: {len(strip):,}\n")

if "Suppliers" in strip.columns:
    print("=== Top suppliers for LED Strip SKUs ===")
    counts = strip["Suppliers"].astype(str).value_counts().head(15)
    for sup, n in counts.items():
        # Truncate long supplier names for readability
        sup_display = sup if len(sup) < 50 else sup[:47] + "..."
        print(f"  {n:>4}  {sup_display}")

    # Count how many have NO supplier
    no_sup = strip[strip["Suppliers"].astype(str).isin(
        ["", "nan", "None"])]
    print(f"\n  ⚠ LED Strip SKUs with no Supplier set: {len(no_sup)}")

    # List a sample of no-supplier strip SKUs
    if len(no_sup):
        print(f"\n  Examples (up to 10):")
        for s in no_sup["SKU"].head(10).tolist():
            print(f"    {s}")
else:
    print("'Suppliers' column not found in products. Check sync output.")
