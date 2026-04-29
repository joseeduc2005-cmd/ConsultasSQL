# Script para iniciar Frontend y Backend en modo desarrollo
# Uso: .\start-dev.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "🚀 INICIANDO PROYECTO PRACTICASPREPROFESIONALES" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Verificar que estamos en la raíz del proyecto
$currentPath = Get-Location
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

if ($currentPath -ne $projectRoot) {
    Set-Location $projectRoot
}

Write-Host "📁 Ubicación: $(Get-Location)" -ForegroundColor Yellow
Write-Host ""

# Verificar que existen las carpetas
if (!(Test-Path "frontend")) {
    Write-Host "❌ ERROR: Carpeta 'frontend' no encontrada" -ForegroundColor Red
    exit 1
}

if (!(Test-Path "backend")) {
    Write-Host "❌ ERROR: Carpeta 'backend' no encontrada" -ForegroundColor Red
    exit 1
}

Write-Host "✅ Estructura verificada" -ForegroundColor Green
Write-Host ""

# Iniciar Frontend
Write-Host "🎨 Iniciando FRONTEND (Next.js)..." -ForegroundColor Magenta
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd frontend; npm run dev" -WindowStyle Normal

# Esperar un poco para que inicie el frontend
Start-Sleep -Seconds 3

# Iniciar Backend
Write-Host "🔌 Iniciando BACKEND (Node.js)..." -ForegroundColor Magenta
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd backend; npm run dev" -WindowStyle Normal

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "✅ APLICACIÓN INICIADA" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "📍 Frontend:  http://localhost:3000" -ForegroundColor Yellow
Write-Host "📍 Backend:   http://localhost:3001 (o puerto configurado)" -ForegroundColor Yellow
Write-Host ""
Write-Host "Se abrirán dos ventanas de PowerShell:" -ForegroundColor Cyan
Write-Host "  • Una para el Frontend" -ForegroundColor Cyan
Write-Host "  • Una para el Backend" -ForegroundColor Cyan
Write-Host ""
Write-Host "Presiona Ctrl+C en cualquier ventana para detener el servicio" -ForegroundColor Yellow
