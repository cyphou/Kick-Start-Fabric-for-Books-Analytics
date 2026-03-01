<#
.SYNOPSIS
    Uploads the 17 SampleData CSV files to BronzeLH Files/ via OneLake DFS API.
.DESCRIPTION
    Standalone script extracted from Deploy-Full.ps1 Step 2.
    Uses the existing BronzeLH lakehouse and workspace IDs.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

# ── IDs ──────────────────────────────────────────────────────────────
$WorkspaceId     = "91b2dca3-5729-4e7d-a473-bfeb85c16aa1"
$BronzeLakehouseId = "899cf688-adfe-47fd-9b59-3d047478d6f0"
$OneLakeBase     = "https://onelake.dfs.fabric.microsoft.com"

$projectRoot = Split-Path $PSScriptRoot -Parent
$dataFolder  = Join-Path $projectRoot "SampleData"

# ── CSV Manifest ─────────────────────────────────────────────────────
$LakehouseFiles = @(
    @{ Folder = "Finance";    Files = @("DimAccounts.csv", "DimCostCenters.csv", "FactFinancialTransactions.csv", "FactBudget.csv") },
    @{ Folder = "HR";         Files = @("DimEmployees.csv", "DimDepartments.csv", "FactPayroll.csv", "FactPerformanceReviews.csv", "FactRecruitment.csv") },
    @{ Folder = "Operations"; Files = @("DimBooks.csv", "DimAuthors.csv", "DimCustomers.csv", "DimGeography.csv", "DimWarehouses.csv", "FactOrders.csv", "FactInventory.csv", "FactReturns.csv") }
)

# ── Helpers ──────────────────────────────────────────────────────────
function Get-StorageToken {
    (Get-AzAccessToken -ResourceUrl "https://storage.azure.com").Token
}

function Upload-FileToOneLake {
    param(
        [string]$LocalFilePath,
        [string]$OneLakePath,
        [string]$Token
    )
    $fileBytes = [System.IO.File]::ReadAllBytes($LocalFilePath)
    $fileName  = [System.IO.Path]::GetFileName($LocalFilePath)

    # Step 1 – Create file
    Invoke-RestMethod -Method Put `
        -Uri "${OneLakePath}/${fileName}?resource=file" `
        -Headers @{ "Authorization" = "Bearer $Token"; "Content-Length" = "0" } | Out-Null

    # Step 2 – Append data
    Invoke-RestMethod -Method Patch `
        -Uri "${OneLakePath}/${fileName}?action=append&position=0" `
        -Headers @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/octet-stream"; "Content-Length" = $fileBytes.Length.ToString() } `
        -Body $fileBytes | Out-Null

    # Step 3 – Flush
    Invoke-RestMethod -Method Patch `
        -Uri "${OneLakePath}/${fileName}?action=flush&position=$($fileBytes.Length)" `
        -Headers @{ "Authorization" = "Bearer $Token"; "Content-Length" = "0" } | Out-Null
}

# ── Main ─────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=== Upload SampleData CSVs to BronzeLH Files/ ===" -ForegroundColor Cyan
Write-Host "Workspace : $WorkspaceId"
Write-Host "BronzeLH  : $BronzeLakehouseId"
Write-Host "Data root : $dataFolder"
Write-Host ""

# Authenticate
$account = Get-AzContext
if (-not $account) {
    Write-Host "No active Azure session - launching interactive login..." -ForegroundColor Yellow
    Connect-AzAccount | Out-Null
}
else {
    Write-Host "Session: $($account.Account.Id)" -ForegroundColor Green
}

$storageToken = Get-StorageToken
$oneLakeFiles = "$OneLakeBase/$WorkspaceId/$BronzeLakehouseId/Files"

# Create Files directory (idempotent)
Write-Host "Creating Files/ directory..." -ForegroundColor Gray
try {
    Invoke-RestMethod -Method Put -Uri "${oneLakeFiles}?resource=directory" `
        -Headers @{ "Authorization" = "Bearer $storageToken"; "Content-Length" = "0" } | Out-Null
}
catch {
    Write-Host "  (directory may already exist - continuing)" -ForegroundColor DarkGray
}

# Upload each CSV
$uploaded = 0; $total = 0; $errors = @()
foreach ($group in $LakehouseFiles) {
    Write-Host ""
    Write-Host "── $($group.Folder) ──" -ForegroundColor Yellow
    foreach ($fileName in $group.Files) {
        $total++
        $localPath = Join-Path (Join-Path $dataFolder $group.Folder) $fileName
        if (-not (Test-Path $localPath)) {
            Write-Host "  SKIP (not found): $($group.Folder)/$fileName" -ForegroundColor Red
            $errors += "$($group.Folder)/$fileName"
            continue
        }
        $sizeKB = [math]::Round((Get-Item $localPath).Length / 1KB, 1)
        Write-Host "  Uploading $fileName ($($sizeKB) KB)..." -ForegroundColor White -NoNewline
        try {
            $storageToken = Get-StorageToken   # refresh for each file
            Upload-FileToOneLake -LocalFilePath $localPath -OneLakePath $oneLakeFiles -Token $storageToken
            $uploaded++
            Write-Host " OK" -ForegroundColor Green
        }
        catch {
            Write-Host " FAILED: $($_.Exception.Message)" -ForegroundColor Red
            $errors += "$($group.Folder)/$fileName"
        }
    }
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Uploaded: $uploaded / $total CSV files" -ForegroundColor $(if ($uploaded -eq $total) { "Green" } else { "Yellow" })
if ($errors.Count -gt 0) {
    Write-Host "Failures:" -ForegroundColor Red
    $errors | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
}
Write-Host "========================================" -ForegroundColor Cyan
