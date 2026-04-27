@echo off
setlocal EnableDelayedExpansion
REM ============================================================
REM  CIN7 Analytics - launch the Streamlit app
REM
REM  Double-click this file. Your browser opens at the URL
REM  shown below. Close the black window (or Ctrl-C) to stop.
REM
REM  Teammates on your LAN can hit the Network URL too - make
REM  sure you click "Allow" if Windows Firewall prompts once.
REM ============================================================

cd /d "%~dp0"
call .venv\Scripts\activate.bat

echo.
echo [1/3] Checking dependencies...
pip install -q -r requirements.txt

REM ---- Pick first free port in range 8501..8520 ----
REM This avoids "Port 8501 is not available" when an earlier
REM Streamlit session didn't exit cleanly. Auto-bumps to the
REM next free port silently.
echo.
echo [2/3] Finding a free port...
set PORT=
for /L %%P in (8501,1,8520) do (
    if not defined PORT (
        netstat -ano | findstr "LISTENING" | findstr ":%%P " >nul 2>&1
        if errorlevel 1 set PORT=%%P
    )
)
if not defined PORT (
    echo.
    echo ERROR: ports 8501-8520 are all busy. Something else is
    echo hogging them. Close other dev tools and try again, or
    echo reboot.
    pause
    exit /b 1
)
echo Using port !PORT!

echo.
echo [3/3] Starting app...
echo ====================================================
echo  Local:   http://localhost:!PORT!
echo  Network: http://%COMPUTERNAME%:!PORT!
echo ====================================================
echo.
echo Press Ctrl-C (in this window) to stop the app.
echo.

streamlit run app.py --server.address 0.0.0.0 --server.port !PORT!

pause
