@echo off
echo ============================================================
echo   Khoi dong cum Hadoop va Spark (Citi Bike Big Data)
echo ============================================================

:: Thiet lap PATH cho Docker
set "PATH=%PATH%;C:\Program Files\Docker\Docker\resources\bin"

:: Chuyen ve thu muc chua file docker-compose.yml
cd /d "%~dp0"

echo [1/2] Kiem tra Docker Daemon...
docker --version
if %errorlevel% neq 0 (
    echo [LOI] Docker chua duoc bat hoac chua chay! Hay mo Docker Desktop truoc.
    pause
    exit /b 1
)

echo [2/2] Dang khoi dong cac container...
docker compose up -d

echo.
echo ============================================================
echo   Cum Hadoop & Spark da duoc bat thanh cong!
echo ============================================================
echo   - NameNode UI:       http://localhost:9870
echo   - Spark Master UI:   http://localhost:8080
echo   - DataNode 1 UI:     http://localhost:9864
echo   - DataNode 2 UI:     http://localhost:9865
echo ============================================================
pause
