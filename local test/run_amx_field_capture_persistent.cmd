@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PS_SCRIPT=%SCRIPT_DIR%amx_field_capture.ps1"

if not exist "%PS_SCRIPT%" (
  echo ERROR: Cannot find script:
  echo   %PS_SCRIPT%
  pause
  exit /b 1
)

echo ==========================================
echo AMX Field Capture Launcher ^(persistent only^)
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
echo Enter commands separated by commas.
echo Use AMX command text only (script appends CR automatically).
echo.
echo Examples:
echo   ?,getStatus
echo   ?,set:1,?,hdmiOff,?,hdmiOn,?
echo   getNetStatus
echo.
set /p COMMANDS_CSV=Commands CSV: 

if "%COMMANDS_CSV%"=="" (
  echo ERROR: No commands entered.
  pause
  exit /b 1
)

set "DECODER_IPS_PS='%DECODER_IPS: '='',''%'"
set "COMMANDS_PS='%COMMANDS_CSV:,=','%'"

echo Running AMX capture...
echo Mode  : persistent + single-connection probe
echo Script: %PS_SCRIPT%
echo IPs   : %DECODER_IPS%
echo Cmds  : %COMMANDS_CSV%
echo.

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "& '%PS_SCRIPT%' -DecoderIps @(%DECODER_IPS_PS%) -Commands @(%COMMANDS_PS%) -RunPersistent -ProbeSingleConnection"

echo.
echo Finished. Check output JSON/CSV in:
echo   %SCRIPT_DIR%amx-captures
echo.
pause
exit /b %ERRORLEVEL%

