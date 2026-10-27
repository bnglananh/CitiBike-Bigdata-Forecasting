@echo off
setlocal

echo ============================================================
echo   Khoi dong cum Hadoop va Spark (Citi Bike Big Data)
echo ============================================================

:: 1. Kiem tra va thiet lap duong dan Docker vao PATH (ho tro ca o C va D)
where docker >nul 2>&1
if %errorlevel% equ 0 goto :docker_found

if exist "D:\Program Files\Docker\Docker\resources\bin" (
    set "PATH=%PATH%;D:\Program Files\Docker\Docker\resources\bin"
) else if exist "D:\Docker\Docker\resources\bin" (
    set "PATH=%PATH%;D:\Docker\Docker\resources\bin"
) else if exist "D:\Docker\resources\bin" (
    set "PATH=%PATH%;D:\Docker\resources\bin"
) else if exist "C:\Program Files\Docker\Docker\resources\bin" (
    set "PATH=%PATH%;C:\Program Files\Docker\Docker\resources\bin"
) else if exist "C:\ProgramData\DockerDesktop\version-bin" (
    set "PATH=%PATH%;C:\ProgramData\DockerDesktop\version-bin"
)

:docker_found
:: 2. Chuyen ve thu muc chua file docker-compose.yml
cd /d "%~dp0"

:: 3. Kiem tra Docker CLI
echo [1/3] Kiem tra Docker CLI...
where docker >nul 2>&1
if %errorlevel% neq 0 (
    echo [LOI] Khong tim thay lenh 'docker'!
    echo Vui long kiem tra lai duong dan cai dat Docker hoac them vao bien moi truong PATH.
    pause
    exit /b 1
)
docker --version

:: 4. Kiem tra Docker Daemon (Engine) dang chay
echo [2/3] Kiem tra Docker Daemon (Docker Desktop)...
docker info >nul 2>&1
if %errorlevel% neq 0 goto :daemon_error

echo [OK] Docker Daemon dang hoat dong binh thuong.
echo.

:: 5. Khoi dong cac container
echo [3/3] Dang khoi dong cac container (HDFS ^& Spark)...
docker compose up -d
if %errorlevel% neq 0 goto :compose_error

echo.
echo ============================================================
echo   Cum Hadoop ^& Spark da duoc bat thanh cong!
echo ============================================================
echo   - NameNode UI:       http://localhost:9870
echo   - Spark Master UI:   http://localhost:8080
echo   - DataNode 1 UI:     http://localhost:9864
echo   - DataNode 2 UI:     http://localhost:9865
echo ============================================================
pause
exit /b 0

:daemon_error
echo.
echo ============================================================
echo [CANH BAO] Docker CLI ton tai nhung Docker Daemon chua chay!
echo ============================================================
echo Nguyen nhan thuong gap:
echo   1. Docker Desktop chua duoc bat.
echo   2. Docker Desktop dang trong qua trinh khoi dong (Starting...).
echo   3. WSL 2 backend chua san sang (neu da chuyen du lieu sang o D).
echo.
echo   * Vui long MO DOCKER DESKTOP, doi bieu tuong Docker chuyen sang
echo     mau xanh (Engine running) roi chay lai file start.bat nay!
echo ============================================================
pause
exit /b 1

:compose_error
echo.
echo ============================================================
echo [LOI] Khong the khoi dong cum container!
echo Vui long kiem tra lai thong bao loi cua Docker o tren.
echo ============================================================
pause
exit /b %errorlevel%


