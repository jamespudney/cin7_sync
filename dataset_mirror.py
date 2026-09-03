"""dataset_mirror.py — one copy of the raw data, in the shared DB.

Why
---
Both Render services keep their own persistent disk, and until now each
ran its OWN CIN7 / ShipStation / Shopify sync into it. The dashboard and
the Slack bot therefore worked from two independently-pulled copies of
the same data (different sync times, different windows), so on-hand,
"last sold" and demand could disagree by up to the sync gap — and CIN7
was being hit twice for the same rows.

James, 2026-09-03: "we made a central database in render for this so
that the systems always match in everything they say".

How
---
The dashboard service (the only one that syncs from source) PUBLISHES
every data CSV it writes to `dataset_files` in Postgres (gzip'd bytea,
one row per logical file). The worker PULLS them onto its own disk with
the same filenames and mtimes, so every existing glob-based loader
(`products_*.csv`, `sale_lines_last_*d_*.csv`, ...) keeps working
unchanged — it just reads bytes that came from the dashboard's disk.

Logical file key = filename with the sync timestamp stripped, e.g.
    products_2026-09-03_020112.csv      -> products.csv
    sale_lines_last_730d_2026-08-01_... -> sale_lines_last_730d.csv
    shipments_full.csv                  -> shipments_full.csv
so each rolling window keeps its own slot and the dashboard's union
semantics (widest window + newer narrower windows) survive the trip.

CLI
---
    python dataset_mirror.py publish   # dashboard side, after each sync
    python dataset_mirror.py pull      # worker side, replaces its syncs
    python dataset_mirror.py status    # exit 0 if the DB has datasets
"""
from __future__ import annotations

import gzip
import hashlib
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from data_paths import OUTPUT_DIR

log = logging.getLogger("dataset_mirror")

# CSVs that any service reads. Only CSV — the *.json twins cin7_sync
# writes are raw dumps nobody loads.
MIRROR_PATTERNS: tuple[str, ...] = (
    "products_*.csv",
    "stock_on_hand_*.csv",
    "customers_*.csv",
    "suppliers_*.csv",
    "sales_last_*d_*.csv",
    "sale_lines_last_*d_*.csv",
    "purchases_last_*d_*.csv",
    "purchase_lines_last_*d_*.csv",
    "boms_*.csv",
    "assemblies_last_*d_*.csv",
    "stock_adjustments_last_*d_*.csv",
    "stock_transfers_last_*d_*.csv",
    "shipments_last_*d_*.csv",
    "shipments_full.csv",
    "shopify_orders_last_*d_*.csv",
    "shopify_orders_full.csv",
    "ip_notes_*.csv",
    "engine_output.csv",
)

# Files larger than this are skipped with a warning rather than pushed
# into a 1 GB Postgres plan. Raise deliberately if a wide backfill needs
# to travel.
MAX_RAW_BYTES = int(os.environ.get("DATASET_MIRROR_MAX_MB", "400")) * 1024 * 1024

_STAMP_RE = re.compile(r"_\d{4}-\d{2}-\d{2}_\d{6}(?=\.csv$)")


def logical_key(filename: str) -> str:
    """Strip the sync timestamp so each rolling window has one slot."""
    return _STAMP_RE.sub("", Path(filename).name)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _gzip_file(path: Path) -> bytes:
    """gzip a file in 1 MB chunks (never holds the raw file in memory)."""
    import io
    buf = io.BytesIO()
    with path.open("rb") as src, gzip.GzipFile(fileobj=buf, mode="wb",
                                                compresslevel=6) as gz:
        for chunk in iter(lambda: src.read(1 << 20), b""):
            gz.write(chunk)
    return buf.getvalue()


def local_candidates(output_dir: Path = OUTPUT_DIR,
                     patterns: Iterable[str] = MIRROR_PATTERNS) -> dict[str, Path]:
    """Newest local file per logical key."""
    best: dict[str, Path] = {}
    for pattern in patterns:
        for p in output_dir.glob(pattern):
            if not p.is_file() or p.name.endswith(".tmp.csv"):
                continue
            key = logical_key(p.name)
            cur = best.get(key)
            if cur is None or p.stat().st_mtime > cur.stat().st_mtime:
                best[key] = p
    return best


# ---------------------------------------------------------------------------
# Publish (dashboard side)
# ---------------------------------------------------------------------------
def publish(output_dir: Path = OUTPUT_DIR, *,
            publisher: str = "dashboard") -> dict:
    import db

    existing = {r["key"]: r for r in db.list_dataset_files()}
    pushed, skipped, too_big = [], [], []
    for key, path in sorted(local_candidates(output_dir).items()):
        st = path.stat()
        if st.st_size > MAX_RAW_BYTES:
            too_big.append((key, st.st_size))
            log.warning("dataset_mirror: %s is %.0f MB > limit, not published",
                        path.name, st.st_size / 1e6)
            continue
        prev = existing.get(key)
        if (prev and prev.get("filename") == path.name
                and abs(float(prev.get("mtime") or 0) - st.st_mtime) < 1
                and int(prev.get("size_bytes") or -1) == st.st_size):
            skipped.append(key)
            continue
        digest = _sha256(path)
        if prev and prev.get("sha256") == digest:
            # Same bytes under a new stamp — just refresh the name/mtime.
            db.touch_dataset_file(key, filename=path.name,
                                  mtime=st.st_mtime, publisher=publisher)
            skipped.append(key)
            continue
        payload = _gzip_file(path)
        db.put_dataset_file(
            key, filename=path.name, mtime=st.st_mtime,
            size_bytes=st.st_size, sha256=digest, payload=payload,
            publisher=publisher)
        pushed.append((key, st.st_size, len(payload)))
        log.info("dataset_mirror: published %s (%.1f MB -> %.1f MB gz)",
                 path.name, st.st_size / 1e6, len(payload) / 1e6)
    return {"pushed": pushed, "unchanged": skipped, "too_big": too_big}


# ---------------------------------------------------------------------------
# Pull (worker side)
# ---------------------------------------------------------------------------
def pull(output_dir: Path = OUTPUT_DIR) -> dict:
    import db

    output_dir.mkdir(parents=True, exist_ok=True)
    rows = db.list_dataset_files()
    if not rows:
        return {"available": False, "written": [], "removed": []}
    written, removed, unchanged = [], [], []
    for meta in rows:
        key, filename = meta["key"], meta["filename"]
        target = output_dir / filename
        # Already have these bytes locally (same name, or an older-stamped
        # sibling with identical content)? Then just make the name/mtime
        # match — no download.
        have = None
        for cand in _local_siblings(output_dir, key):
            if (cand.stat().st_size == int(meta["size_bytes"] or -1)
                    and _sha256(cand) == meta["sha256"]):
                have = cand
                break
        if have is not None:
            if have != target:
                have.replace(target)
            mtime = float(meta.get("mtime") or target.stat().st_mtime)
            if abs(target.stat().st_mtime - mtime) >= 1:
                os.utime(target, (mtime, mtime))
            unchanged.append(key)
            _prune_siblings(output_dir, key, keep=filename, removed=removed)
            continue
        payload = db.get_dataset_file_payload(key)
        if payload is None:
            continue
        tmp = output_dir / f".{filename}.mirror.tmp"
        with gzip.open(_BytesReader(payload), "rb") as src, tmp.open("wb") as dst:
            for chunk in iter(lambda: src.read(1 << 20), b""):
                dst.write(chunk)
        if _sha256(tmp) != meta["sha256"]:
            tmp.unlink(missing_ok=True)
            log.error("dataset_mirror: checksum mismatch for %s, skipped", filename)
            continue
        mtime = float(meta.get("mtime") or datetime.now(timezone.utc).timestamp())
        os.utime(tmp, (mtime, mtime))
        tmp.replace(target)
        written.append(filename)
        _prune_siblings(output_dir, key, keep=filename, removed=removed)
    _write_pull_marker(output_dir)
    log.info("dataset_mirror: pulled %d files (%d unchanged, %d stale removed)",
             len(written), len(unchanged), len(removed))
    return {"available": True, "written": written, "unchanged": unchanged,
            "removed": removed}


class _BytesReader:
    """Minimal file-like over bytes/memoryview for gzip.open."""

    def __init__(self, data):
        self._mv = memoryview(data)
        self._pos = 0

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            n = len(self._mv) - self._pos
        chunk = self._mv[self._pos:self._pos + n]
        self._pos += len(chunk)
        return chunk.tobytes()

    def readable(self) -> bool:  # pragma: no cover - gzip probes this
        return True


def _local_siblings(output_dir: Path, key: str) -> list[Path]:
    """Local files that map to this logical key (any sync stamp)."""
    stem = Path(key).stem
    out = [p for p in output_dir.glob(f"{stem}_*.csv")
           if p.is_file() and logical_key(p.name) == key]
    stable = output_dir / key
    if stable.is_file():
        out.append(stable)
    return sorted(out, key=lambda p: p.stat().st_mtime, reverse=True)


def _prune_siblings(output_dir: Path, key: str, *, keep: str,
                    removed: list) -> None:
    """Delete older-stamped local files that share this logical key, so
    the worker's own historic syncs can never out-glob the mirrored copy."""
    for p in _local_siblings(output_dir, key):
        if p.name != keep:
            try:
                p.unlink()
                removed.append(p.name)
            except OSError:
                pass


_PULL_MARKER = ".dataset_mirror_last_pull"


def _write_pull_marker(output_dir: Path) -> None:
    try:
        (output_dir / _PULL_MARKER).write_text(
            datetime.now(timezone.utc).isoformat(), encoding="utf-8")
    except OSError:
        pass


def status() -> dict:
    import db
    rows = db.list_dataset_files()
    return {
        "available": bool(rows),
        "files": len(rows),
        "newest_published_at": max((str(r.get("published_at") or "") for r in rows),
                                   default=None),
        "total_gz_mb": round(sum(int(r.get("gz_bytes") or 0) for r in rows) / 1e6, 1),
    }


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    args = list(sys.argv[1:] if argv is None else argv)
    cmd = args[0] if args else "status"
    if cmd == "publish":
        info = publish(publisher=os.environ.get("RENDER_SERVICE_NAME", "dashboard"))
        print({"pushed": [k for k, *_ in info["pushed"]],
               "unchanged": len(info["unchanged"]), "too_big": info["too_big"]})
        return 0
    if cmd == "pull":
        info = pull()
        print({k: (v if k == "available" else len(v)) for k, v in info.items()})
        return 0 if info["available"] else 2
    if cmd == "status":
        info = status()
        print(info)
        return 0 if info["available"] else 2
    print(__doc__)
    return 1


if __name__ == "__main__":
    sys.exit(main())
