@echo off
setlocal EnableDelayedExpansion
set BASE=steps
set NAME=stage.cmd
set TARGET=!BASE!\!NAME!
call "!TARGET!"
