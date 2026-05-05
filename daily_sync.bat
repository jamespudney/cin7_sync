@echo off
REM ============================================================
REM  CIN7 daily incremental sync
REM  Called by Windows Task Scheduler every morning.
REM  Pulls masters + 3 days of sales/purchase headers.
REM ============================================================

cd /d "%~dp0"

REM Activate venv (silent)
call .venv\Scripts\activate.bat

REM Quick sync: masters + 3-day headers
python cin7_sync.py quick --days 3 >> output\daily_sync.log 2>&1

REM If you also want line-level refresh, uncomment:
REM python cin7_sync.py salelines --days 3 >> output\daily_sync.log 2>&1

REM v2.67.36 — warm the ABC engine cache after the sync so the next
REM user that opens the dashboard gets an instant page load instead
REM of waiting 30-60s for the engine to recompute. Best-effort; a
REM failure here is logged but doesn't break the sync exit code.
python warm_engine.py >> output\daily_sync.log 2>&1

exit /b %ERRORLEVEL%
