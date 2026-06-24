@echo off
cd /d "%~dp0"
py -m drivertranslator --config config.json --listen 0.0.0.0 --port 2323 --log-level INFO
if errorlevel 1 pause
