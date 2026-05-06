@echo off
REM ============================================================
REM  CIN7 Weekly Slow-Mover Email
REM  Called by Windows Task Scheduler every Friday morning.
REM  Sends a digest of slow-mover progress to the sales/buyer team.
REM
REM  Configuration: env vars must be set in the user's environment
REM  for this script (NOT in the system environment, since Task
REM  Scheduler runs as the same user). Required vars:
REM
REM    SLOW_MOVERS_EMAIL_TO   comma-separated recipients
REM    SMTP_HOST              e.g. smtp.gmail.com
REM    SMTP_PORT              e.g. 587
REM    SMTP_USER              SMTP login
REM    SMTP_PASS              SMTP password / app password
REM    SMTP_FROM              optional sender display
REM
REM  If SLOW_MOVERS_EMAIL_TO is unset, the script is a silent
REM  no-op — the schedule still fires but nothing is sent.
REM ============================================================

cd /d "%~dp0"
call .venv\Scripts\activate.bat
python weekly_slow_movers_email.py >> output\weekly_email.log 2>&1
exit /b %ERRORLEVEL%
