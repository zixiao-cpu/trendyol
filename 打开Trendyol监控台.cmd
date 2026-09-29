@echo off
cd /d "%~dp0"
where pwsh.exe >nul 2>&1
if errorlevel 1 goto windows_powershell
pwsh.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0monitor_dashboard.ps1"
goto finished

:windows_powershell
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0monitor_dashboard.ps1"

:finished
if errorlevel 1 pause
