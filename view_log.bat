@echo off
title Python API Service Log Monitor
echo.
echo [python-api] 실시간 로그 모니터링 중입니다. (CMD 창을 닫으면 모니터링이 종료됩니다.)
echo --------------------------------------------------
echo.

REM NSSM이 저장하는 로그 파일의 경로를 실시간으로 읽어옵니다.
powershell.exe -Command "Get-Content 'D:\python-api\error.log' -Wait"