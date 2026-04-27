"""
configure_air_suppliers.py — one-shot script to mark the four
air-freight-default suppliers in team_actions.db.

Run once from the project folder:
    .venv\\Scripts\\python configure_air_suppliers.py

Safe to re-run. Each config UPSERTs — existing values are overwritten,
not duplicated. Tweak the numbers below if your actual production or
shipping times change.
"""
from __future__ import annotations

import db

ACTOR = "james"   # who set these — shows up in the audit log

# Production + EU→US air lead time. "Production" is the supplier's build
# time before they can even ship; add ~7 days for air-freight transit.
# EnoLED is US-local, so no air/sea distinction — they just ship ground
# in a week.
CONFIGS = [
    {
        "supplier_name": "Neonica Polska Sp. z o.o.",
        "lead_time_air_days": 28,   # 21d production + 7d air
        "lead_time_sea_days": 45,   # 21d production + 24d sea
        "air_eligible_default": 1,
        "preferred_freight": "air",
        "note": "Custom neon-flex made to order. 3 weeks production "
                "+ air shipping from Poland. Sea fallback is slow.",
    },
    {
        "supplier_name": "ARDITI GmbH (EUR)",
        "lead_time_air_days": 14,   # 7d production + 7d air
        "lead_time_sea_days": 30,   # 7d production + 23d sea
        "air_eligible_default": 1,
        "preferred_freight": "air",
        "note": "Small German electronics supplier. 1 week production "
                "+ air from Germany. Items are small/light, always air.",
    },
    {
        "supplier_name": "Blebox sp. z.o.o.",
        "lead_time_air_days": 14,
        "lead_time_sea_days": 30,
        "air_eligible_default": 1,
        "preferred_freight": "air",
        "mov_amount": 250.0,
        "mov_currency": "USD",
        "note": "Polish home-automation controllers. 1 week production "
                "+ air from Poland. Small boxes, always air-freight. "
                "MOV target $250 per shipment — the MOV warning in the "
                "PO editor will flag drafts below this.",
    },
    {
        "supplier_name": "DIGIMAX SRL (Formerly DALCNET) (EUR)",
        "lead_time_air_days": 14,
        "lead_time_sea_days": 30,
        "air_eligible_default": 1,
        "preferred_freight": "air",
        "note": "Italian LED controller supplier (formerly Dalcnet). "
                "1 week production + air from Italy. Electronics "
                "- always air.",
    },
    {
        "supplier_name": "EnoLED",
        "lead_time_air_days": 7,    # 1-week total — local US supplier
        "lead_time_sea_days": 7,    # same (no sea path, keep equal)
        "air_eligible_default": 1,
        "preferred_freight": "air",
        "note": "US-local supplier. 1-week total lead time (ground/air "
                "doesn't matter — they just ship). Some items we stock, "
                "others we hold on dropship basis until volume justifies "
                "stocking — use the Dropship flag on those lines.",
    },
    {
        "supplier_name": "Gyford Décor, LLC",
        "lead_time_air_days": 7,      # US-local, 1 week
        "lead_time_sea_days": 7,
        "air_eligible_default": 1,
        "preferred_freight": "air",
        "dropship_default": 1,        # 100%-dropship supplier
        "note": "US-local supplier. We dropship EVERYTHING from Gyford — "
                "we don't stock any of their items. Supplier-level dropship "
                "flag auto-applies to every Gyford SKU so the reorder "
                "engine zeros out targets.",
    },
]


def main() -> None:
    for cfg in CONFIGS:
        name = cfg["supplier_name"]
        print(f"Setting {name}...")
        db.set_supplier_config(
            supplier_name=name,
            lead_time_sea_days=cfg["lead_time_sea_days"],
            lead_time_air_days=cfg["lead_time_air_days"],
            air_eligible_default=cfg["air_eligible_default"],
            preferred_freight=cfg["preferred_freight"],
            dropship_default=cfg.get("dropship_default", 0),
            mov_amount=cfg.get("mov_amount"),
            mov_currency=cfg.get("mov_currency"),
            actor=ACTOR,
            note=cfg["note"],
        )
    print()
    print(f"Done — {len(CONFIGS)} suppliers configured.")
    print("All items from these suppliers will default to AIR freight "
          "in the Ordering page reorder calculations, with the lead "
          "times above. You can override per-row in the editor.")


if __name__ == "__main__":
    main()
