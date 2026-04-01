@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PS_SCRIPT=%SCRIPT_DIR%amx_soak_test.ps1"

if not exist "%PS_SCRIPT%" (
  echo ERROR: Cannot find script:
  echo   %PS_SCRIPT%
  pause
  exit /b 1
)

echo ==========================================
echo AMX Soak Test Launcher ^(30 minutes^)
echo ==========================================
echo Folder : %SCRIPT_DIR%
echo.
echo Enter decoder IPs separated by commas.
echo Example: 192.168.1.50,192.168.1.51
echo.
set /p DECODER_IPS=Decoder IPs: 

if "%DECODER_IPS%"=="" (
  echo ERROR: No decoder IPs entered.
  pause
  exit /b 1
)

echo.
echo Command to send every cycle ^(script appends CR automatically^).
echo Examples: ?, getStatus
echo.
set /p SOAK_CMD=Command [default ?]: 
if "%SOAK_CMD%"=="" set "SOAK_CMD=?"

echo.
set /p POLL_SECONDS=Poll interval seconds [default 10]: 
if "%POLL_SECONDS%"=="" set "POLL_SECONDS=10"

set "DECODER_IPS_PS='%DECODER_IPS: '='',''%'"

echo Running AMX persistent soak test...
echo Duration : 30 minutes
echo IPs      : %DECODER_IPS%
echo Command  : %SOAK_CMD%
echo Poll     : %POLL_SECONDS%s
echo.

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "& '%PS_SCRIPT%' -DecoderIps @(%DECODER_IPS_PS%) -DurationMinutes 30 -PollSeconds %POLL_SECONDS% -Command '%SOAK_CMD%'"

echo.
echo Finished. Check output JSON/CSV in:
echo   %SCRIPT_DIR%amx-captures
echo.
pause
exit /b %ERRORLEVEL%

