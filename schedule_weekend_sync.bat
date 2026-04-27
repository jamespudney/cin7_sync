@echo off
REM ============================================================
REM  ONE-TIME SETUP: register the Weekend Deep Sync with
REM  Windows Task Scheduler so it fires automatically every
REM  Friday evening at 6:00 PM without you clicking anything.
REM
REM  Double-click this file ONCE. After that you can check or
REM  edit the task in Task Scheduler (taskschd.msc) under the
REM  Task Scheduler Library, look for "CIN7 Weekend Deep Sync".
REM
REM  Re-run this file anytime to refresh the task's settings.
REM ============================================================

cd /d "%~dp0"

echo Registering weekly task "CIN7 Weekend Deep Sync"...
echo   When:   Every Friday at 6:00 PM
echo   Runs:   %~dp0weekend_deep_sync.bat
echo   Wakes:  the computer if asleep (Windows will cancel any
echo           pending sleep when it's time to run)
echo.

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$action = New-ScheduledTaskAction -Execute '%~dp0weekend_deep_sync.bat' -WorkingDirectory '%~dp0';" ^
  "$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Friday -At '6:00PM';" ^
  "$settings = New-ScheduledTaskSettingsSet -WakeToRun -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 48) -MultipleInstances IgnoreNew;" ^
  "Register-ScheduledTask -TaskName 'CIN7 Weekend Deep Sync' -Action $action -Trigger $trigger -Settings $settings -Description 'Runs weekend_deep_sync.bat every Friday 6pm. 5yr salelines + 3yr movements + DuckDB warehouse build. Auto-retries if PC was off.' -Force"

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
echo  Next run:  this Friday at 6:00 PM
echo  Log file:  output\weekend_deep.log
echo.
echo  To verify or edit:
echo    1. Press Win+R, type  taskschd.msc  and press Enter
echo    2. Task Scheduler Library - find "CIN7 Weekend Deep Sync"
echo    3. Right-click - Properties to see/change settings
echo.
echo  IMPORTANT before Friday:
echo    - Leave the laptop PLUGGED IN and online from Fri eve
echo    - Power and Sleep settings: set "Put the PC to sleep" to
echo      "Never" while plugged in (Settings ^> System ^> Power)
echo    - Windows Update can still reboot - pause updates Friday
echo      evening if you want a clean uninterrupted run
echo ============================================================
pause
