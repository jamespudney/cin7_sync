# Wired4Signs Analytics — Documentation

This folder is the canonical knowledge base for the AI Assistant.
Anything in here is searchable from the **AI Assistant** page in the
app. Add a new `.md` file or edit an existing one and the AI picks
it up on the next question.

## What goes in here

- Business rules (inventory classification, reorder logic, dead-stock
  rules, supplier rules)
- App mechanics (how each page works, what each calculation means)
- SOPs and workflows (PO drafting, supplier-name discipline, sync
  cadences)
- Glossary of terms (engine, family, MOV, MOQ, etc.)
- Decision logs (why we set things up this way)

## What does NOT go here

- Source code (lives in the project root)
- API responses or sync output (`/data/output/`)
- User secrets (`.env`, env vars)
- Personally identifying customer data

## How the AI uses this

When you ask a "how" or "why" question on the AI Assistant page,
Claude calls the `search_knowledge_base` tool, which scans these
markdown files for paragraphs matching your query. The top 5 hits
are passed back to Claude, which composes an answer and cites the
source file + line range.

If Claude says "the documentation doesn't explain this — please
ask an admin to add it", that's a signal to write a new doc here.
The doc doesn't have to be long — 3-5 paragraphs is usually plenty.

## Format conventions

- One topic per file. Long files are fine; multiple short files are
  better than one giant one.
- Use markdown headings (`#`, `##`) liberally — they're indexed and
  boost search relevance.
- Keep paragraphs separated by blank lines (the indexer splits on
  blank lines).
- Plain English where possible. The AI is good at translating
  precise technical text into casual answers.
