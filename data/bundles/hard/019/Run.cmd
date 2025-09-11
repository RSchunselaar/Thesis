@echo off
setlocal EnableDelayedExpansion
set D=steps
for %%F in (step.cmd) do set T=!D!\%%F
call "!T!"
