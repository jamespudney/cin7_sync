# CIN7 Core → Cowork bridge

A local script that pulls data out of CIN7 Core and drops it into a folder on
your PC as CSV and JSON. Claude (in Cowork) and the companion Streamlit app
then read those files to build the ABC analysis, reorder governance, and
team review workflows.

## Files

- `cin7_sync.py` — the sync script
- `.env.example` — credential template (rename to `.env` and fill in)
- `requirements.txt` — Python dependencies
- `run_sync.bat` — Windows double-click launcher
- `output/` — generated CSV + JSON + log files (plus `.checkpoint_*.json` for
  resume support on long-running line-item pulls)

## One-time setup

1. Install **Python 3.10+** from https://www.python.org/downloads/ (tick "Add
   Python to PATH" during install).
2. Open PowerShell in this folder (Shift + right-click → "Open PowerShell here").
3. Create the venv and install dependencies:
   ```
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```
4. In CIN7 Core, **Integrations & API → API v2 → + Application**, create
   an app named "Cowork Bridge". Copy the Account ID and Application Key.
5. Rename `.env.example` to `.env` and paste the two values in.

## Commands

### Master data
```
python cin7_sync.py test             # verify credentials
python cin7_sync.py products         # full product master
python cin7_sync.py stock            # current stock on hand (availability)
python cin7_sync.py customers
python cin7_sync.py suppliers
```

### Transaction headers (fast — one call per page of 1000)
```
python cin7_sync.py sales     --days 365
python cin7_sync.py purchases --days 365
```

### Line items (slow — one detail call per order, ~1.1 sec each)
```
python cin7_sync.py salelines     --days 365   # sale invoice lines
python cin7_sync.py purchaselines --days 365   # PO order + receipt lines
```

### Stock movements (needed for demand definition in the ABC policy)
```
python cin7_sync.py stockadjustments --days 365
python cin7_sync.py stocktransfers   --days 365
python cin7_sync.py movements        --days 365   # both of the above
```

### Composite
```
python cin7_sync.py quick --days 30    # daily refresh: masters + headers
python cin7_sync.py full  --days 365   # everything including line items
```

## Expected timing (based on your current volumes)

| Command                  | First run (12 months) | Daily refresh (7 days) |
| ------------------------ | --------------------- | ---------------------- |
| `quick`                  | 2–4 min               | 2–4 min                |
| `salelines`              | 6–8 hours             | 30–60 min              |
| `purchaselines`          | 20–40 min             | 5–10 min               |
| `movements`              | 15–30 min             | 5–10 min               |
| `full`                   | ~8 hours              | ~1 hour                |

Long-running commands **checkpoint every 25 records** to `output/.checkpoint_*.json`.
If a run is interrupted (Ctrl-C, network blip, PC sleep), just run the same
command again — it resumes where it left off.

## Letting Claude see the data

In Cowork, select this folder (or `output/` inside it) as your working
directory and Claude will have read access to every CSV and JSON file.

## Scheduling

### Windows Task Scheduler — daily quick refresh at 6:30am
1. Open **Task Scheduler → Create Basic Task**
2. Trigger: Daily, 06:30
3. Action: **Start a program**
   - Program: `powershell.exe`
   - Arguments: `-Command "cd 'C:\Tools\cin7_sync'; .\.venv\Scripts\Activate.ps1; python cin7_sync.py quick --days 7"`

### Recommended cadence
- **Quick sync**: every morning (refreshes stock + headers)
- **Salelines**: weekly (or after a busy sales weekend)
- **Movements**: weekly
- **Full sync**: once a month or when something looks off

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `Missing credentials` | Check `.env` exists and both values are filled in. |
| `401` / `403` | Key revoked or typo. Regenerate in CIN7 and update `.env`. |
| `429 rate limit` | Script auto-backs off. Just wait. |
| `Too many detail fetch errors` | An endpoint may be off in your account plan. Paste the log at me. |
| Long pull got interrupted | Just re-run the same command. It resumes from the checkpoint. |

## Security note

Your `.env` contains API keys. Don't commit it to git, email it, or paste it
into chat. If it does leak, rotate the key in CIN7 immediately.
