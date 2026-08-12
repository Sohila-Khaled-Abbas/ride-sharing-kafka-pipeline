# ==============================================================================
# run_pipeline.ps1 — Automated Pipeline Runner (Windows PowerShell)
# ==============================================================================
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  🚖 Automated Ride-Sharing Kafka Pipeline (PowerShell Runner)" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

Write-Host "Step 1: Installing dependencies..." -ForegroundColor Yellow
python -m pip install -q -r requirements.txt

Write-Host "Step 2: Executing automated test suite..." -ForegroundColor Yellow
python -m unittest discover tests -v

Write-Host "Step 3: Launching automated pipeline..." -ForegroundColor Yellow
python -m src.pipeline_runner --num-events 15

Write-Host "✅ Automation completed successfully!" -ForegroundColor Green
