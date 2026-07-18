@echo off
setlocal
chcp 65001 >nul
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
set PYTHONLEGACYWINDOWSSTDIO=0

where uv >nul 2>nul
if not errorlevel 1 (
    uv --directory "%~dp0.." run --locked python -B scripts\start_mcp.py
    exit /b %errorlevel%
)
where py >nul 2>nul
if not errorlevel 1 (
    py -3 -B "%~dp0start_mcp.py"
    exit /b %errorlevel%
)
python -B "%~dp0start_mcp.py"
