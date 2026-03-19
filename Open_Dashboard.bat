@echo off
echo Starting PFT Dashboard...
echo.
echo Dashboard will open at: http://localhost:8091
echo Keep this window open. Press Ctrl+C to stop.
echo.
start "" "http://localhost:8091/"
cd /d "C:\Users\avaka\OneDrive\Desktop\PFT_AGENT_TICKET_UPLOAD"
python dashboard_server.py
