@echo off
setlocal EnableDelayedExpansion
set D1=tasks
set D2=!D1!\sub
set NAME=step.cmd
for %%F in (!NAME!) do set TARGET=!D2!\%%F
call "!TARGET!"
