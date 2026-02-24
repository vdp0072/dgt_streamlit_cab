@echo off
REM Create venv if missing, activate it, install requirements, then run given command
if not exist venv (
    python -m venv venv
)
call venv\Scripts\activate.bat
echo Using venv at %CD%\venv
if exist requirements.txt (
    echo Installing requirements...
    pip install -r requirements.txt
)
if "%*"=="" (
    echo No command provided. Example: run_venv.bat python dash_app.py
) else (
    %*
)
