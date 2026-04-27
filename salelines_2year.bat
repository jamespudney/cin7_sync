@echo off
REM ============================================================
REM  CIN7 2-year sale-lines pull
REM  Runs salelines --days 730 at 1.5s rate (overnight speed).
REM  Expect ~14 hours at current volumes.
REM  Checkpoint resumes if interrupted - rerun the same file.
REM ============================================================

cd /d "%~dp0"
call .venv\Scripts\activate.bat

REM Temporary rate override for THIS run only. Your .env default stays as is.
set CIN7_RATE_SECONDS=1.5

echo ====================================================
echo Starting 2-year sale-lines pull at %DATE% %TIME%
echo Rate: 1.5s/call (40 calls/min)
echo ====================================================

python cin7_sync.py salelines --days 730 >> output\salelines_2year.log 2>&1

echo ====================================================
echo Finished at %DATE% %TIME%
echo Output: sale_lines_last_730d_*.csv
echo ====================================================
