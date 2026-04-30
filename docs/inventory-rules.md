# Inventory classification rules

Each SKU in the engine gets a **classification** that drives how it's
shown in the Ordering page and how the reorder engine treats it.

## The four classifications

- **Active** — moving normally; reorder engine treats it as in-scope.
- **Slow-moving** — sells but at a low rate vs. its on-hand cover.
  Buyer should reorder cautiously and look for ways to clear excess.
- **Dead stock** — has not moved within the dormancy window AND has
  positive on-hand. Buyer should NOT reorder; sales should consider
  pushing it.
- **Watchlist** — borderline cases. Could go either way. Buyer
  reviews case-by-case.

## How a SKU becomes "slow-moving"

A SKU is classified slow-moving when ALL of the following hold:

- It has on-hand stock greater than zero.
- Its 90-day demand is less than 25% of its on-hand quantity. In
  other words, on current trajectory the existing stock would last
  more than ~12 months.
- It has had at least some demand in the last 12 months. (No demand
  at all → "dead stock", not "slow-moving".)
- It is not excluded by another rule (see exclusions below).

## How a SKU becomes "dead stock"

A SKU is classified dead stock when:

- It has on-hand stock greater than zero, AND
- It has had zero sales in the dormancy window (default 365 days),
  AND
- It is not excluded by another rule.

## Exclusions — items the engine does NOT classify

Some SKUs are excluded from slow/dead stock classification regardless
of movement:

- **Service / non-inventory items** — labour, soldering, shipping
  fees, etc. They have no physical stock to be "dead".
- **Active BOM components** — if a SKU is a child of a BOM that has
  any movement (i.e., the parent assembly is selling), the component
  is excluded. Otherwise we'd flag every screw and connector as dead.
- **Items with active purchase orders** — if there's an open PO for
  the SKU, classification waits until the receipt clears.
- **Migration successors** — if SKU A migrated to SKU B, A's old
  movement counts as B's "effective demand". B is judged on the
  rolled-up history, not just its own ramp-up.
- **Family bulk variants** — see the LED strip / LED channel rules
  for how variant lengths roll up.

## LED strip family rule

LED strips often come in bulk-roll lengths (100m, 50m, 25m) and
short cut variants (5m, 1ft, 0305mm).

**The rule:** if any related variant in the family has movement,
the bulk variants are NOT classified as slow/dead. The bulk roll is
treated as raw material for the cut variants.

**Example:** LED-XYZ-100M (bulk) is dead stock by raw movement, but
LED-XYZ-5M (cut) is selling well. The 100M roll should NOT be flagged
as dead — the 5M cuts are made from it. Only when the entire family
shows zero movement do we mark the bulk as dead.

## LED channel / profile / diffuser family rule

Same pattern as LED strips. Profiles come in long lengths (3m, 2.5m,
2m) and short cut lengths (1m, 609mm, 2390mm). Long lengths are raw
stock for the short cuts; do not classify the long length as
slow/dead unless the whole family shows no movement.

## How to override a classification

If the buyer disagrees with a classification, they can:

- Add a note on the SKU (Ordering page → notes column) — visible to
  the team but doesn't change the classification.
- Mark a SKU as "do not reorder" — engine excludes from suggestions
  but classification still computes.
- Set a SKU-specific override in `db.sku_policy_overrides` — manual
  intervention, requires writing the row directly. Reserved for edge
  cases.
