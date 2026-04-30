"""
ai_kb.py
========
Knowledge-base layer for the AI Assistant. Indexes all .md files in
the docs/ folder (plus the handful of top-level docs like DEPLOY.md
and SAAS_NOTES.md) and provides a paragraph-level keyword search.

Phase 0 design choices:
  - Markdown files on disk (no DB, no separate KB store) — easy for
    you to edit, easy to version in git, easy to grep manually.
  - Keyword/term-frequency scoring (no embeddings yet). Works well
    when the doc corpus is small and queries are mostly fact lookups.
    Will upgrade to embeddings in a later phase if recall suffers.
  - Paragraph granularity (split on blank lines). Each result is a
    short, citable passage with file path + line range.
  - Cached in memory after first load. Re-reads files when their
    mtimes change so editing a doc takes effect on next call.

To add a new doc: drop a .md file into docs/ (or any subdir of it).
The next call to search_knowledge_base() picks it up automatically.

Public API:
  index_knowledge_base() -> list[Paragraph]   # for debugging
  search_knowledge_base(query, max_results)   # what the tool calls
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from data_paths import DATA_DIR

# Where docs live. We look in:
#   1. <repo>/docs/ — the canonical knowledge base
#   2. <repo>/ — top-level .md files (README, DEPLOY, SAAS_NOTES, RULES)
# We deliberately do NOT search /data/ output CSVs or random places.
APP_DIR = Path(__file__).resolve().parent
DOCS_DIRS: list[Path] = [APP_DIR / "docs", APP_DIR]
ALLOWED_TOP_LEVEL = {
    "README.md", "DEPLOY.md", "SAAS_NOTES.md", "RULES.md",
    "NEXT_STEPS.md",
}

# Common English stopwords — we drop these when scoring so a query
# like "what does the engine do" doesn't match every paragraph that
# happens to contain "the".
_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "do", "does",
    "for", "from", "have", "how", "i", "if", "in", "is", "it", "its",
    "me", "of", "on", "or", "our", "the", "this", "to", "was", "we",
    "what", "when", "where", "which", "who", "why", "with", "you",
    "your", "any", "can", "could", "should", "will", "would", "should",
    "show", "tell", "explain", "list",
}


@dataclass
class Paragraph:
    """One indexed chunk of a doc. Cited back to the user as
    `<file>:<start_line>-<end_line>`."""
    source: str            # e.g. "docs/inventory-rules.md"
    title: str             # nearest heading above this paragraph
    text: str
    start_line: int
    end_line: int
    score: float = 0.0     # populated by search()


# In-process cache. Keyed by (path, mtime) so an edit invalidates.
_CACHE: dict[tuple, list[Paragraph]] = {}


def _tokenize(text: str) -> list[str]:
    """Simple lowercase + alphanumeric word split, stopwords removed."""
    out = []
    for tok in re.findall(r"[a-zA-Z0-9_-]+", text.lower()):
        if tok in _STOPWORDS:
            continue
        out.append(tok)
    return out


def _split_paragraphs(text: str) -> list[tuple[str, int, int]]:
    """Split a markdown doc into paragraphs (blank-line separated).
    Returns list of (paragraph_text, start_line, end_line)."""
    out = []
    lines = text.splitlines()
    buf: list[str] = []
    buf_start: Optional[int] = None
    for i, line in enumerate(lines, start=1):
        if line.strip() == "":
            if buf:
                out.append(("\n".join(buf), buf_start or i, i - 1))
                buf = []
                buf_start = None
        else:
            if buf_start is None:
                buf_start = i
            buf.append(line)
    if buf:
        out.append(("\n".join(buf), buf_start or 1, len(lines)))
    return out


def _nearest_heading(lines: list[str], paragraph_start_line: int) -> str:
    """Walk backwards from a paragraph's start line to find the most
    recent markdown heading (#, ##, ### etc). Falls back to the first
    line of the file."""
    for i in range(paragraph_start_line - 1, 0, -1):
        line = lines[i - 1].strip()
        m = re.match(r"^#+\s+(.+)", line)
        if m:
            return m.group(1).strip()
    # No heading found — return the file name without extension as fallback
    return ""


def _index_one_file(path: Path) -> list[Paragraph]:
    """Read a markdown file and return paragraph entries."""
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return []
    lines = raw.splitlines()
    out: list[Paragraph] = []
    rel = str(path.resolve().relative_to(APP_DIR))
    for text, start_line, end_line in _split_paragraphs(raw):
        # Skip pure code-fence blocks and pure heading-only paragraphs
        # (they're noise without context).
        stripped = text.strip()
        if not stripped:
            continue
        if stripped.startswith("```") and stripped.endswith("```"):
            # Pure code block — index but with low signal weight via
            # short text. Keep for ability to search verbatim code.
            pass
        title = _nearest_heading(lines, start_line)
        out.append(Paragraph(
            source=rel,
            title=title,
            text=text,
            start_line=start_line,
            end_line=end_line,
        ))
    return out


def index_knowledge_base() -> list[Paragraph]:
    """Build (or reuse cached) paragraph index for all KB files."""
    paths_to_index: list[Path] = []
    for d in DOCS_DIRS:
        if not d.exists():
            continue
        if d == APP_DIR:
            # Top level — only allow specific .md files, don't index
            # every random markdown that lands here.
            for p in d.glob("*.md"):
                if p.name in ALLOWED_TOP_LEVEL:
                    paths_to_index.append(p)
        else:
            # docs/ subtree — index everything.
            for p in d.rglob("*.md"):
                paths_to_index.append(p)

    fingerprint = tuple(
        sorted((str(p), p.stat().st_mtime) for p in paths_to_index
               if p.exists()))
    if fingerprint in _CACHE:
        return _CACHE[fingerprint]

    out: list[Paragraph] = []
    for p in paths_to_index:
        out.extend(_index_one_file(p))
    _CACHE.clear()
    _CACHE[fingerprint] = out
    return out


def search_knowledge_base(query: str,
                           max_results: int = 5) -> list[Paragraph]:
    """Return top paragraphs matching the query. Score is normalized
    term-frequency × inverse-document-frequency style — paragraphs
    containing rarer query terms score higher than those with common
    ones. Title hits get a 2x boost."""
    query_terms = _tokenize(query)
    if not query_terms:
        return []
    paragraphs = index_knowledge_base()
    if not paragraphs:
        return []

    # Compute document frequency (how many paragraphs contain each
    # term) so we can weight rare terms higher.
    df: dict[str, int] = {}
    para_token_sets: list[set] = []
    for p in paragraphs:
        toks = set(_tokenize(p.text))
        para_token_sets.append(toks)
        for t in toks:
            df[t] = df.get(t, 0) + 1

    n_paras = len(paragraphs)

    def _idf(term: str) -> float:
        d = df.get(term, 0)
        if d == 0:
            return 0.0
        # Smooth IDF
        import math
        return math.log((n_paras + 1) / (d + 1)) + 1.0

    scored: list[Paragraph] = []
    for p, toks in zip(paragraphs, para_token_sets):
        score = 0.0
        for term in query_terms:
            if term in toks:
                score += _idf(term)
        # Title boost
        title_toks = set(_tokenize(p.title))
        for term in query_terms:
            if term in title_toks:
                score += _idf(term) * 1.5
        if score > 0:
            new_p = Paragraph(
                source=p.source, title=p.title, text=p.text,
                start_line=p.start_line, end_line=p.end_line,
                score=round(score, 3))
            scored.append(new_p)
    scored.sort(key=lambda x: x.score, reverse=True)
    return scored[:max_results]


def kb_stats() -> dict:
    """For diagnostics / Data Health page — how many docs, paragraphs,
    bytes are indexed."""
    paragraphs = index_knowledge_base()
    sources = sorted({p.source for p in paragraphs})
    total_bytes = sum(len(p.text) for p in paragraphs)
    return {
        "n_documents": len(sources),
        "n_paragraphs": len(paragraphs),
        "total_bytes": total_bytes,
        "documents": sources,
    }
