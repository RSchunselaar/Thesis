@echo off
setlocal EnableDelayedExpansion
set D=tasks
for %%F in (step.cmd) do set T=!D!\%%F
call "!T!"
