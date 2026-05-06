@echo off
REM ============================================================
REM  CIN7 near-real-time sync
REM  Called by Task Scheduler every 5-15 minutes.
REM  Pulls stock snapshot + last 24h of movements.
REM  Skips masters (products/customers/suppliers) — those come
REM  from the daily "quick" sync.
REM ============================================================

cd /d "%~dp0"
call .venv\Scripts\activate.bat
python cin7_sync.py nearsync --days 1 >> output\nearsync.log 2>&1

REM v2.67.54 — ShipStation 1-day NearSync. Picks up shipments
REM created since the last run so the AI can answer "where's my
REM shipment" questions within 15 minutes of the carrier label
REM being created. The script no-ops if SHIPSTATION env vars
REM aren't set — safe to leave enabled before keys are configured.
python shipstation_sync.py recent --days 1 >> output\nearsync.log 2>&1

REM v2.67.36 — warm the engine cache after each near-sync. The
REM 15-min cadence means the cache stays fresh throughout the
REM workday, so any user opening the dashboard gets an instant
REM page load. Best-effort; failures are logged silently.
python warm_engine.py >> output\nearsync.log 2>&1

exit /b %ERRORLEVEL%
