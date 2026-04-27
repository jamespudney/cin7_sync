"""
restore_layout.py — restore a previously-saved PO-editor column layout
======================================================================
Every time you click Save in the Column Layout expander, the complete
column list gets written to the audit_log table. This script lists the
last N saves for your user and lets you restore any of them.

Run:
    .venv\\Scripts\\python restore_layout.py
"""
from __future__ import annotations

import sys
from typing import List

import db

VIEW = "ordering_po_editor"


def _list_saves(user: str, limit: int = 15) -> List[dict]:
    """Return [{'idx': 1, 'at': '...', 'columns': [...]}] for this user,
    most recent first."""
    out = []
    with db.connect() as c:
        rows = c.execute(
            "SELECT at, detail FROM audit_log "
            "WHERE event = 'ui_prefs.save' AND actor = ? AND target = ? "
            "ORDER BY at DESC LIMIT ?",
            (user.lower(), VIEW, limit),
        ).fetchall()
    for i, r in enumerate(rows, start=1):
        cols = [c.strip() for c in (r["detail"] or "").split(",") if c.strip()]
        out.append({"idx": i, "at": r["at"], "columns": cols})
    return out


def _print_save(s: dict) -> None:
    cols = s["columns"]
    preview = ", ".join(cols[:6])
    if len(cols) > 6:
        preview += f", … (+{len(cols) - 6} more)"
    print(f"  [{s['idx']:>2}]  {s['at']}  ({len(cols)} cols)")
    print(f"        {preview}")


def main() -> None:
    user_input = input(
        "Your name (as entered in the app's sidebar) [james]: "
    ).strip() or "james"
    user = user_input.lower()

    saves = _list_saves(user)
    if not saves:
        print(f"No saved layouts found for user '{user}'.")
        print("Try running debug_layouts.py to see which user keys exist.")
        sys.exit(1)

    print(f"\nRecent layout saves for '{user}':\n")
    for s in saves:
        _print_save(s)
    print()
    pick = input(
        f"Which one to restore? Enter 1-{len(saves)} "
        "(or 'q' to quit): "
    ).strip().lower()
    if pick == "q":
        return
    try:
        idx = int(pick)
        chosen = next(s for s in saves if s["idx"] == idx)
    except (ValueError, StopIteration):
        print("Invalid choice.")
        sys.exit(1)

    # Current layout for comparison
    current = db.get_column_layout(user, VIEW) or []
    print(f"\nCurrent live layout ({len(current)} cols): "
          f"{', '.join(current[:6])}"
          + ("…" if len(current) > 6 else ""))
    print(f"Restoring save from {chosen['at']} ({len(chosen['columns'])} "
          f"cols)")
    confirm = input("Confirm? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        return

    db.save_column_layout(user, VIEW, chosen["columns"])
    print(f"✓ Restored. Refresh the app (🔄 Refresh data now in sidebar) "
          "and the PO editor will use this layout.")


if __name__ == "__main__":
    main()
