@echo off
REM ============================================
REM Setup Windows Task Scheduler for PFT Agent
REM Run this as Administrator
REM ============================================

echo Setting up PFT Internet Ticket Agent - Daily Schedule (10:15 AM IST)...

REM Delete existing task if any
schtasks /delete /tn "PFT_Internet_Ticket_Agent" /f >nul 2>&1

REM Create scheduled task - runs daily at 10:15 AM
schtasks /create ^
  /tn "PFT_Internet_Ticket_Agent" ^
  /tr "python \"C:\Users\avaka\OneDrive\Desktop\PFT_AGENT_TICKET_UPLOAD\run_daily_agent.py\"" ^
  /sc daily ^
  /st 10:15 ^
  /rl HIGHEST ^
  /f

if %ERRORLEVEL% EQU 0 (
    echo.
    echo SUCCESS! Task scheduled.
    echo   Task Name: PFT_Internet_Ticket_Agent
    echo   Schedule:  Daily at 10:15 AM
    echo   Script:    run_daily_agent.py
    echo.
    echo IMPORTANT: Set your Gmail App Password first:
    echo   setx GMAIL_APP_PASSWORD "your_16_char_app_password"
    echo.
    echo To test manually:
    echo   python run_daily_agent.py
    echo.
    echo To check task status:
    echo   schtasks /query /tn "PFT_Internet_Ticket_Agent" /v
) else (
    echo FAILED to create scheduled task.
    echo Try running this script as Administrator.
)

pause
