@echo off
REM ============================================================
REM  ONE-TIME SETUP: register the Weekly Slow-Mover Email with
REM  Windows Task Scheduler so it fires automatically every
REM  Friday at 8:00 AM without you clicking anything.
REM
REM  Double-click this file ONCE. Re-run anytime to refresh
REM  settings.
REM
REM  Pre-requisite: SLOW_MOVERS_EMAIL_TO + SMTP_* env vars must
REM  be set in the user environment that runs this task. See
REM  weekly_slow_movers_email.bat for the list.
REM ============================================================

cd /d "%~dp0"

echo Registering weekly task "CIN7 Weekly Slow-Mover Email"...
echo   When:   Every Friday at 8:00 AM
echo   Runs:   %~dp0weekly_slow_movers_email.bat
echo   Wakes:  the computer if asleep
echo.

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$action = New-ScheduledTaskAction -Execute '%~dp0weekly_slow_movers_email.bat' -WorkingDirectory '%~dp0';" ^
  "$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Friday -At '8:00AM';" ^
  "$settings = New-ScheduledTaskSettingsSet -WakeToRun -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 15) -MultipleInstances IgnoreNew;" ^
  "Register-ScheduledTask -TaskName 'CIN7 Weekly Slow-Mover Email' -Action $action -Trigger $trigger -Settings $settings -Description 'Sends weekly slow-mover digest email every Friday 8am. Recipients + SMTP creds via env vars.' -Force"

if errorlevel 1 (
    echo.
    echo FAILED to register the task. Common causes:
    echo  - PowerShell blocked by policy: open an elevated PS and run:
    echo    Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
    echo  - Task Scheduler service disabled - unlikely but check services.msc
    pause
    exit /b 1
)

echo.
echo ============================================================
echo  DONE - task registered.
echo.
echo  Next run:  this Friday at 8:00 AM
echo  Log file:  output\weekly_email.log
echo.
echo  To verify or edit:
echo    1. Press Win+R, type  taskschd.msc  and press Enter
echo    2. Task Scheduler Library - find "CIN7 Weekly Slow-Mover Email"
echo    3. Right-click - Properties to see/change settings
echo.
echo  IMPORTANT: ensure these env vars are set in your user
echo  environment (NOT system) so Task Scheduler can read them:
echo    SLOW_MOVERS_EMAIL_TO   comma-separated recipients
echo    SMTP_HOST              e.g. smtp.gmail.com
echo    SMTP_PORT              e.g. 587
echo    SMTP_USER              SMTP login
echo    SMTP_PASS              SMTP password / app password
echo    SMTP_FROM              optional display sender
echo ============================================================
pause
