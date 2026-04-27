@echo off
REM ============================================================
REM  CIN7 Core sync - Windows launcher
REM  Double-click to run the "all" sync (last 30 days).
REM  Edit the last line to change what it pulls.
REM ============================================================

cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
    echo First-time setup: creating virtual environment...
    python -m venv .venv
    call .venv\Scripts\activate.bat
    pip install -r requirements.txt
) else (
    call .venv\Scripts\activate.bat
)

python cin7_sync.py all --days 30

echo.
echo Finished. Files are in the output\ folder.
pause
