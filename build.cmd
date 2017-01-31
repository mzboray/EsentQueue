@echo off
if '%1'=='/?' goto help
if '%1'=='-help' goto help
if '%1'=='-h' goto help

powershell -NoProfile -ExecutionPolicy Bypass -Command "& '%~dp0\scripts\build\build.ps1' %*"
goto :eof

:help
powershell -NoProfile -ExecutionPolicy Bypass -Command "& '%~dp0\scripts\build\build.ps1' Help"