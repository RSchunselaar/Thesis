@echo off
REM Orchestrates some Windows steps
call prep.cmd
powershell -File .\ps\stage.ps1