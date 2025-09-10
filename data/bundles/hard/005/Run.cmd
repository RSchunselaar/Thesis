@echo off
setlocal EnableDelayedExpansion
set D1=bin
set D2=!D1!\core
set NAME=step.cmd
for %%F in (!NAME!) do set TARGET=!D2!\%%F
call "!TARGET!"
