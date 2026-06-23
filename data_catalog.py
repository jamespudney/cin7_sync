"""Dataset catalog and freshness helpers.

This module is intentionally Streamlit-free so the dashboard, bot, and
future tests can share the same understanding of which CSV snapshots
exist, how fresh they should be, and where the latest file lives.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from data_paths import OUTPUT_DIR


@dataclass(frozen=True)
class DatasetSpec:
    label: str
    prefix: str
    group: str
    expected_cadence_hours: Optional[float] = None
    command: str = ""


DATASETS: tuple[DatasetSpec, ...] = (
    DatasetSpec("Products", "products", "CIN7 master", 24, "python cin7_sync.py products"),
    DatasetSpec("Stock on hand", "stock_on_hand", "CIN7 near-sync", 0.5, "python cin7_sync.py stock"),
    DatasetSpec("Customers", "customers", "CIN7 master", 24, "python cin7_sync.py customers"),
    DatasetSpec("Suppliers", "suppliers", "CIN7 master", 24, "python cin7_sync.py suppliers"),
    DatasetSpec("Sales headers (30d)", "sales_last_30d", "CIN7 sales", 24, "python cin7_sync.py sales --days 30"),
    DatasetSpec("Purchase headers (30d)", "purchases_last_30d", "CIN7 purchasing", 24, "python cin7_sync.py purchases --days 30"),
    DatasetSpec("Sale lines (rolling)", "sale_lines_last", "CIN7 sales", 24, "python cin7_sync.py salelines --days 30"),
    DatasetSpec("Purchase lines (rolling)", "purchase_lines_last", "CIN7 purchasing", 24, "python cin7_sync.py purchaselines --days 30"),
    DatasetSpec("BOMs", "boms", "CIN7 production", 168, "python cin7_sync.py boms"),
    DatasetSpec("Assemblies (30d)", "assemblies_last_30d", "CIN7 production", 30, "python cin7_sync.py assemblies --days 30"),
    DatasetSpec("Stock adjustments (30d)", "stock_adjustments_last_30d", "CIN7 movements", 24, "python cin7_sync.py stockadjustments --days 30"),
    DatasetSpec("Stock transfers (30d)", "stock_transfers_last_30d", "CIN7 movements", 24, "python cin7_sync.py stocktransfers --days 30"),
    DatasetSpec("Shipments", "shipments_last_30d", "ShipStation", 24, "python shipstation_sync.py daily --days 30"),
    DatasetSpec("Shopify orders", "shopify_orders", "Shopify", 24, "python shopify_sync.py --orders-recent 7"),
    DatasetSpec("Inventory Planner notes", "ip_notes", "Inventory Planner", 24, "python ip_pull_alternates.py"),
    DatasetSpec("Engine output", "engine_output", "Derived", 1, "python warm_engine.py"),
)


def latest_file(prefix: str, output_dir: Path = OUTPUT_DIR) -> Optional[Path]:
    """Return the most recent CSV whose name starts with ``prefix``.

    Most sync files are timestamped as ``<prefix>_<stamp>.csv``. A few
    derived files use stable names, so the fallback also checks
    ``<prefix>.csv``.
    """
    files = sorted(output_dir.glob(f"{prefix}_*.csv"))
    stable = output_dir / f"{prefix}.csv"
    if stable.exists():
        files.append(stable)
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def file_mtime(prefix: str, output_dir: Path = OUTPUT_DIR) -> Optional[datetime]:
    path = latest_file(prefix, output_dir)
    return datetime.fromtimestamp(path.stat().st_mtime) if path else None


def dataset_status(
    spec: DatasetSpec,
    *,
    row_count: object = None,
    now: Optional[datetime] = None,
    output_dir: Path = OUTPUT_DIR,
) -> dict:
    now = now or datetime.now()
    path = latest_file(spec.prefix, output_dir)
    if path is None:
        return {
            "Dataset": spec.label,
            "Group": spec.group,
            "File prefix": spec.prefix,
            "Latest file": "missing",
            "Rows": _format_rows(row_count),
            "Last sync": "never",
            "Age (hours)": "—",
            "Expected cadence": _format_cadence(spec.expected_cadence_hours),
            "Status": "missing",
            "Command": spec.command,
        }

    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    age_hours = (now - mtime).total_seconds() / 3600.0
    status = "fresh"
    if spec.expected_cadence_hours is not None:
        if age_hours > spec.expected_cadence_hours * 2:
            status = "stale"
        elif age_hours > spec.expected_cadence_hours:
            status = "aging"

    return {
        "Dataset": spec.label,
        "Group": spec.group,
        "File prefix": spec.prefix,
        "Latest file": path.name,
        "Rows": _format_rows(row_count),
        "Last sync": mtime.strftime("%Y-%m-%d %H:%M"),
        "Age (hours)": f"{age_hours:.1f}",
        "Expected cadence": _format_cadence(spec.expected_cadence_hours),
        "Status": status,
        "Command": spec.command,
    }


def catalog_rows(
    *,
    row_counts: Optional[dict[str, object]] = None,
    specs: Iterable[DatasetSpec] = DATASETS,
    now: Optional[datetime] = None,
    output_dir: Path = OUTPUT_DIR,
) -> list[dict]:
    row_counts = row_counts or {}
    return [
        dataset_status(
            spec,
            row_count=row_counts.get(spec.prefix),
            now=now,
            output_dir=output_dir,
        )
        for spec in specs
    ]


def _format_cadence(hours: Optional[float]) -> str:
    if hours is None:
        return "as needed"
    if hours < 1:
        return f"{int(hours * 60)} min"
    if hours == 1:
        return "1 hour"
    if hours % 24 == 0:
        days = int(hours / 24)
        return f"{days} day" if days == 1 else f"{days} days"
    return f"{hours:g} hours"


def _format_rows(row_count: object) -> str:
    if row_count is None:
        return "—"
    if isinstance(row_count, int):
        return f"{row_count:,}"
    return str(row_count)
