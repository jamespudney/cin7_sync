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
exit /b %ERRORLEVEL%
