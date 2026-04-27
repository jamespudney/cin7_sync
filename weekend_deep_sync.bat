@echo off
REM ============================================================
REM  CIN7 WEEKEND DEEP SYNC  (Fri 6pm -> Mon 8am window)
REM
REM  Uses the full ~62-hour weekend. Goes deeper than the
REM  Friday-only plan: 5-year line-item history, 3-year
REM  movements, then builds a DuckDB warehouse for fast queries.
REM
REM  PHASES (each has a checkpoint file and can resume):
REM    1) Backup current output folder          ~2 min
REM    2) Masters: products/customers/suppliers/BOMs   ~15 min
REM    3) 3-year stock movements (adj/trans/events)    ~10-14 hrs
REM    4) 5-year sale lines backfill                   ~14-18 hrs
REM    5) 5-year purchase lines backfill               ~3-4 hrs
REM    6) Build DuckDB warehouse (warehouse.duckdb)    ~30-60 min
REM    7) Summary report                               instant
REM
REM  Total: 28-38 hrs. Starts Fri evening, finishes Sat night
REM  / Sun morning. Monday AM the team walks in to a warm
REM  warehouse with 5 years of history, all indexed.
REM
REM  The 15-minute nearsync keeps running in parallel. Safe -
REM  each endpoint has its own checkpoint file.
REM ============================================================

cd /d "%~dp0"
call .venv\Scripts\activate.bat

REM Overnight rate: 1.5s/call = 40/min, well below CIN7's
REM 60/min account cap.
set CIN7_RATE_SECONDS=1.5

set LOG=output\weekend_deep.log

echo ============================================================  >> %LOG%
echo  WEEKEND DEEP SYNC started %DATE% %TIME%                        >> %LOG%
echo ============================================================  >> %LOG%
echo ============================================================
echo  WEEKEND DEEP SYNC started %DATE% %TIME%
echo  Rate: 1.5s/call ^(40 calls/min^)
echo  Log:  %LOG%
echo  Expected completion: Sun morning
echo ============================================================

REM ---- Phase 1: backup ----
echo.
echo [1/7] Backing up current output folder...
set BKDIR=backups\output_%DATE:/=-%_%TIME:~0,2%%TIME:~3,2%
set BKDIR=%BKDIR: =0%
if not exist backups mkdir backups
xcopy /E /I /Y /Q output "%BKDIR%" >> %LOG% 2>&1
echo   Backup -> %BKDIR%

REM ---- Phase 2: masters + 5-year sales/purchase headers ----
REM (Sales headers at 5yr are needed for accurate monthly shipping charges;
REM  CIN7's list endpoint omits shipping from per-line data, so we derive
REM  shipping = InvoiceAmount - sum(line totals) - tax from headers.)
echo.
echo [2/7] Masters + 5-year sales/purchase headers...
python cin7_sync.py products         >> %LOG% 2>&1
python cin7_sync.py customers        >> %LOG% 2>&1
python cin7_sync.py suppliers        >> %LOG% 2>&1
python cin7_sync.py boms             >> %LOG% 2>&1
python cin7_sync.py sales     --days 1825  >> %LOG% 2>&1
python cin7_sync.py purchases --days 1825  >> %LOG% 2>&1
echo   Phase 2 done at %TIME%

REM ---- Phase 3: 3-year stock movements ----
echo.
echo [3/7] 3-year stock movements (adjustments + transfers + events)...
python cin7_sync.py stockadjustments --days 1095   >> %LOG% 2>&1
python cin7_sync.py stocktransfers   --days 1095   >> %LOG% 2>&1
python cin7_sync.py movements        --days 1095   >> %LOG% 2>&1
echo   Phase 3 done at %TIME%

REM ---- Phase 4: 5-year sale lines ----
echo.
echo [4/7] 5-year sale lines backfill (~14-18 hrs)...
python cin7_sync.py salelines --days 1825  >> %LOG% 2>&1
echo   Phase 4 done at %TIME%

REM ---- Phase 5: 5-year purchase lines ----
echo.
echo [5/7] 5-year purchase lines backfill (~3-4 hrs)...
python cin7_sync.py purchaselines --days 1825  >> %LOG% 2>&1
echo   Phase 5 done at %TIME%

REM ---- Phase 6: DuckDB warehouse build ----
echo.
echo [6/7] Building DuckDB warehouse (warehouse.duckdb)...
python load_warehouse.py  >> %LOG% 2>&1
echo   Phase 6 done at %TIME%

REM ---- Phase 7: summary ----
echo.
echo [7/7] Summary
echo ============================================================ >> %LOG%
echo  WEEKEND DEEP SYNC complete %DATE% %TIME%                     >> %LOG%
echo ============================================================ >> %LOG%
dir /B /O-D output\*.csv | findstr /C:"_last_1825d" /C:"_last_1095d" >> %LOG%

echo.
echo ============================================================
echo  WEEKEND DEEP SYNC complete at %DATE% %TIME%
echo  Backup: %BKDIR%
echo  Warehouse: warehouse.duckdb
echo  Tail log:  type %LOG%
echo ============================================================

pause
