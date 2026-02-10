# DataBridge AI V2 - Validation Script
# Usage: .\validate-v2.ps1 [-Token "your-jwt-token"]

param(
    [string]$Token = "",
    [string]$ApiUrl = "http://localhost:3002/api",
    [string]$FrontendUrl = "http://localhost:5174"
)

$ErrorActionPreference = "SilentlyContinue"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  DataBridge AI V2 - Validation Tool   " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 1. Backend Health Check
Write-Host "[1/5] Backend Health Check..." -ForegroundColor Yellow
try {
    $health = Invoke-RestMethod -Uri "$ApiUrl/health" -Method Get -TimeoutSec 5
    $dbStatus = $health.data.database.status
    $uptime = [math]::Round($health.data.uptime / 60, 2)
    Write-Host "      Status: OK" -ForegroundColor Green
    Write-Host "      Database: $dbStatus" -ForegroundColor Gray
    Write-Host "      Uptime: $uptime minutes" -ForegroundColor Gray
} catch {
    Write-Host "      Status: FAILED - Backend not responding" -ForegroundColor Red
    Write-Host "      Run: cd v2/backend && npm run start:dev" -ForegroundColor Gray
}

# 2. Frontend Check
Write-Host ""
Write-Host "[2/5] Frontend Check..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri $FrontendUrl -UseBasicParsing -TimeoutSec 5
    if ($response.StatusCode -eq 200) {
        Write-Host "      Status: OK (HTTP 200)" -ForegroundColor Green
    }
} catch {
    Write-Host "      Status: FAILED - Frontend not responding" -ForegroundColor Red
    Write-Host "      Run: cd v2/frontend && npm run dev" -ForegroundColor Gray
}

# 3. Database Check
Write-Host ""
Write-Host "[3/5] MySQL Database Check (port 3308)..." -ForegroundColor Yellow
$mysqlTest = mysql -h localhost -P 3308 -u root -proot -e "SELECT 1 as test" 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "      Status: OK" -ForegroundColor Green
} else {
    Write-Host "      Status: Cannot verify (mysql client needed)" -ForegroundColor Yellow
    Write-Host "      Tip: Check if MySQL container is running" -ForegroundColor Gray
}

# 4. API Endpoints (if token provided)
Write-Host ""
Write-Host "[4/5] API Endpoints Check..." -ForegroundColor Yellow
if ($Token -eq "") {
    Write-Host "      Skipped (no token provided)" -ForegroundColor Yellow
    Write-Host "      Tip: Run with -Token 'your-jwt-token'" -ForegroundColor Gray
} else {
    $headers = @{
        "Authorization" = "Bearer $Token"
        "Content-Type" = "application/json"
    }

    # Test projects endpoint
    try {
        $projects = Invoke-RestMethod -Uri "$ApiUrl/hierarchy-projects" -Headers $headers -Method Get
        $projectCount = ($projects | Measure-Object).Count
        Write-Host "      Projects API: OK ($projectCount projects)" -ForegroundColor Green
    } catch {
        Write-Host "      Projects API: FAILED" -ForegroundColor Red
    }

    # Test reference tables endpoint
    try {
        $tables = Invoke-RestMethod -Uri "$ApiUrl/smart-hierarchy/reference-tables" -Headers $headers -Method Get
        $tableCount = ($tables | Measure-Object).Count
        Write-Host "      Reference Tables API: OK ($tableCount tables)" -ForegroundColor Green
    } catch {
        if ($_.Exception.Response.StatusCode -eq 401) {
            Write-Host "      Reference Tables API: Auth Failed (invalid token)" -ForegroundColor Red
        } else {
            Write-Host "      Reference Tables API: FAILED" -ForegroundColor Red
        }
    }
}

# 5. File System Check
Write-Host ""
Write-Host "[5/5] File System Check..." -ForegroundColor Yellow
$uploadPath = "v2/UploadFiles"
if (Test-Path $uploadPath) {
    $csvFiles = Get-ChildItem -Path $uploadPath -Filter "*.csv" | Measure-Object
    Write-Host "      Upload folder: OK ($($csvFiles.Count) CSV files)" -ForegroundColor Green

    # List DIM files
    $dimFiles = Get-ChildItem -Path $uploadPath -Filter "dim*.csv"
    if ($dimFiles.Count -gt 0) {
        Write-Host "      DIM files found:" -ForegroundColor Gray
        foreach ($file in $dimFiles) {
            Write-Host "        - $($file.Name)" -ForegroundColor Gray
        }
    }
} else {
    Write-Host "      Upload folder: Not found" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Validation Complete                  " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Quick Commands:" -ForegroundColor Yellow
Write-Host "  Start Backend:  cd v2/backend && npm run start:dev" -ForegroundColor Gray
Write-Host "  Start Frontend: cd v2/frontend && npm run dev" -ForegroundColor Gray
Write-Host "  Open App:       start http://localhost:5174" -ForegroundColor Gray
Write-Host ""
