@echo off
setlocal EnableDelayedExpansion
set D=bin
for %%F in (step.cmd) do set T=!D!\%%F
call "!T!"
