# Arranque local del pipeline (Spark local + Kafka del host en localhost:9092).
# Uso:  powershell -ExecutionPolicy Bypass -File run_local.ps1  [port]
param(
    [int]$Port = 3011
)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (-not (Test-Path ".venv")) {
    Write-Host "[run_local] creando entorno virtual .venv ..." -ForegroundColor Cyan
    python -m venv .venv
}

Write-Host "[run_local] instalando dependencias ..." -ForegroundColor Cyan
& .\.venv\Scripts\python.exe -m pip install -q --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -q -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw "pip install fallo" }

Write-Host "[run_local] arrancando la app en :$Port ..." -ForegroundColor Green
$env:PORT = "$Port"
& .\.venv\Scripts\python.exe app.py