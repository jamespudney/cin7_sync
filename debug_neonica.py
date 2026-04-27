import pandas as pd, glob

# Load latest BOM and product files
bom_file = sorted(glob.glob("output/boms_*.csv"))[-1]
prod_file = sorted(glob.glob("output/products_*.json"))[-1] if glob.glob("output/products_*.json") else None
bom = pd.read_csv(bom_file)

target = "LEDIRIS2200-180-5m"
print(f"=== Investigating {target} ===")
print(f"BOM file: {bom_file}\n")

# Is this SKU a component in any assembly? (i.e., is it a cut source?)
as_component = bom[bom["ComponentSKU"].str.lower() == target.lower()]
print(f"Used as component in {len(as_component)} BOM(s):")
for _, row in as_component.iterrows():
    print(f"  {row['AssemblySKU']}  (qty {row.get('Quantity', '?')})")

# Does this SKU have its own BOM? (i.e., is it itself assembled from something?)
as_assembly = bom[bom["AssemblySKU"].str.lower() == target.lower()]
print(f"\nHas {len(as_assembly)} BOM component(s) of its own:")
for _, row in as_assembly.iterrows():
    print(f"  {row['ComponentSKU']}  (qty {row.get('Quantity', '?')})")

# What other SKUs share the LEDIRIS2200-180 family prefix?
prefix = "LEDIRIS2200-180"
family = bom[
    (bom["AssemblySKU"].str.startswith(prefix, na=False)) |
    (bom["ComponentSKU"].str.startswith(prefix, na=False))
]
print(f"\n=== Family {prefix}* in BOM data ===")
print(family[["AssemblySKU","ComponentSKU","Quantity"]].to_string(index=False))
