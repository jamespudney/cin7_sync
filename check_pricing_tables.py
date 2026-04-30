"""Quick check: what's in the family pricing tables right now."""
import db

with db.connect() as c:
    print("=== family_color_pricing ===")
    rows = c.execute(
        "SELECT family, color, supplier, tier_qty, unit_price "
        "FROM family_color_pricing ORDER BY family, color, tier_qty"
    ).fetchall()
    if not rows:
        print("  (empty — Quick Seed has not been run)")
    else:
        print(f"  {len(rows)} rows. Distinct suppliers:")
        suppliers = set(r["supplier"] for r in rows)
        for s in suppliers:
            n = sum(1 for r in rows if r["supplier"] == s)
            print(f"    '{s}'  ({n} rows)")

    print("\n=== family_setup_fees ===")
    rows = c.execute(
        "SELECT family, supplier, fee_type, fee_amount "
        "FROM family_setup_fees ORDER BY family, supplier"
    ).fetchall()
    if not rows:
        print("  (empty)")
    else:
        for r in rows:
            print(f"  {r['family']} / {r['supplier']} / "
                   f"{r['fee_type']} = ${r['fee_amount']:.2f}")

    print("\n=== family_pricing_rules ===")
    rows = c.execute(
        "SELECT family, supplier, rule, nag_threshold_savings "
        "FROM family_pricing_rules ORDER BY family"
    ).fetchall()
    if not rows:
        print("  (empty)")
    else:
        for r in rows:
            print(f"  {r['family']} / {r['supplier']}: rule={r['rule']}  "
                   f"nag>${r['nag_threshold_savings']:.0f}")
