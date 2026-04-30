# Purchase Order workflow

The app has a multi-draft Purchase Order system on the **Ordering**
page. It supports multiple drafts per supplier, pessimistic locking
(only one user edits at a time), and a real CIN7 push that creates a
DRAFT Advanced Purchase in CIN7 for the buyer to review and
authorise.

## Lifecycle of a PO draft

A draft moves through these statuses (`po_drafts.status`):

1. **editing** — the buyer is composing it. They can change qty,
   add/remove lines, etc. Other users can see the draft but can't
   edit unless they take the lock.
2. **submitted** — pushed to CIN7. CIN7 PO ID + number are recorded.
   Local edits frozen; further changes happen in CIN7.
3. **finalized** — CIN7 has authorised the PO and moved it to the
   ORDERED status. Local draft is archived. Future task: auto-detect
   this via daily sync.
4. **cancelled** — buyer voided the local draft. If it had been
   submitted, the CIN7 PO must be voided separately in CIN7.

## Pessimistic locking

Only the locker can edit lines. Lock is held for 30 minutes; if no
activity in that window, anyone else can take the lock. The lock
record is in `po_drafts.locked_by` + `po_drafts.locked_at`.

## CIN7 push — what actually happens

When a buyer clicks "Create draft PO in CIN7" with all lines saved:

1. **Pre-flight validation** in `cin7_post_po.validate_draft()`:
   - Status must be `editing`.
   - `cin7_po_id` must NOT already be set (idempotency guard).
   - Every line must have `qty > 0` and a unique SKU.
   - Supplier name must EXACTLY match a CIN7 supplier (case-
     insensitive). If not, the push is refused and the user is shown
     the candidate names. This avoids the wrong-vendor scenario
     that hit PO-7076.
   - Every SKU must resolve to a CIN7 ProductID.
   - MOV must be met (or explicitly waived).
2. **Master POST** to `/advanced-purchase` — creates the master PO
   with supplier, location, approach (INVOICE/STOCK), terms, note.
   CIN7 returns the new PO's ID + OrderNumber.
3. **Local DB updated** with the CIN7 PO ID immediately, so a
   subsequent failure leaves a recoverable trail.
4. **Lines POST** to `/purchase/order` with the master TaskID.
   Each line carries ProductID, SKU, Quantity, Price (sourced from
   the per-supplier `Cost` field, NOT AverageCost), and Total
   (= Quantity × Price, which CIN7 validates server-side).
5. **Auto-rollback on failure** — if the lines POST fails, the
   script DELETEs the empty master so no orphan PO is left in
   CIN7. Local cin7_po_id is cleared so the user can retry.

## Hard rules — never relax these

- **Never modify a SKU.** SKUs are the join key between CIN7 and
  Shopify. The push code has whitelist guards that refuse to send
  any field that could rename a product.
- **Never auto-AUTHORISE.** The PO is always created in DRAFT
  status. A human reviews and authorises in CIN7 before the
  supplier sees it.
- **Strict supplier name match.** No fallback to "first hit" — if
  the local draft says "Reeves" but CIN7 has "Reeves Extruded
  Products, Inc", the push is refused until the buyer renames the
  supplier locally to match exactly.

## Recovery from partial failure

If a master is created but lines fail (e.g., a SKU's Total doesn't
match what CIN7 expects), the user has three options in the UI:

1. **Retry lines** — re-POST lines against the same master ID.
   Useful when you fix the underlying issue (e.g., sync the per-
   supplier Cost) without wanting to lose the master.
2. **Clear CIN7 link & start fresh** — wipe the local cin7_po_id so
   the next push creates a brand-new master. Buyer should void the
   old master in CIN7 manually.
3. **Cancel** — close the panel; deal with it later.

## Supplier name discipline

The script `sync_supplier_names.py` runs nightly to detect drift
between local supplier names and CIN7's canonical names. If it finds
a mismatch (e.g., local "Reeves" vs. CIN7 "Reeves Extruded Products,
Inc"), it auto-renames in all 9 local tables that reference
suppliers. This keeps local data in sync with CIN7 — the source of
truth.

When a supplier is added in CIN7 and the team starts using it locally,
spelling matters: the AI push will refuse to route a PO to "Reeves"
if CIN7 only has "Reeves Extruded Products, Inc - LLC".
