<#
.SYNOPSIS
    Validates a Horizon Books Fabric deployment (3-lakehouse medallion architecture).

.DESCRIPTION
    Checks that all expected items (BronzeLH, SilverLH, GoldLH, Notebooks, Dataflows,
    Pipeline, Semantic Model, Report, Data Agent) exist in the specified workspace
    and enumerates Gold Lakehouse tables.

.PARAMETER WorkspaceId
    The GUID of the Fabric workspace to validate.

.PARAMETER SemanticModelName
    Expected semantic model name. Defaults to HorizonBooksModel.

.EXAMPLE
    .\Validate-Deployment.ps1 -WorkspaceId "your-workspace-guid"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,

    [Parameter(Mandatory = $false)]
    [string]$SemanticModelName = "HorizonBooksModel"
)

$ErrorActionPreference = "Stop"
Import-Module (Join-Path $PSScriptRoot 'HorizonBooks.psm1') -Force
$FabricApiBase = $script:FabricApiBase

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  Horizon Books — Deployment Validation" -ForegroundColor Cyan
Write-Host "  (3-Lakehouse Medallion Architecture)" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Workspace: $WorkspaceId"
Write-Host ""

$fabricToken = Get-FabricToken
$headers = @{ "Authorization" = "Bearer $fabricToken" }

# Get all items in the workspace
$allItems = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" -Headers $headers).value

$passed = 0
$failed = 0

# ── Expected Fabric Items ────────────────────────────────────────────
$checks = @(
    # 3 Lakehouses (medallion)
    @{ Name = "BronzeLH";  Type = "Lakehouse"; Required = $true },
    @{ Name = "SilverLH";  Type = "Lakehouse"; Required = $true },
    @{ Name = "GoldLH";    Type = "Lakehouse"; Required = $true },
    # 5 Notebooks
    @{ Name = "HorizonBooks_01_BronzeToSilver"; Type = "Notebook"; Required = $true },
    @{ Name = "HorizonBooks_02_WebEnrichment";  Type = "Notebook"; Required = $true },
    @{ Name = "HorizonBooks_03_SilverToGold";   Type = "Notebook"; Required = $true },
    @{ Name = "HorizonBooks_04_Forecasting";    Type = "Notebook"; Required = $true },
    @{ Name = "HorizonBooks_05_DiagnosticCheck"; Type = "Notebook"; Required = $false },
    # Spark Environment
    @{ Name = "HorizonBooks_SparkEnv";          Type = "Environment"; Required = $false },
    # 3 Dataflows
    @{ Name = "HorizonBooks_DF_Finance";    Type = "Dataflow"; Required = $true },
    @{ Name = "HorizonBooks_DF_HR";         Type = "Dataflow"; Required = $true },
    @{ Name = "HorizonBooks_DF_Operations"; Type = "Dataflow"; Required = $true },
    # Pipeline
    @{ Name = "PL_HorizonBooks_Orchestration"; Type = "DataPipeline"; Required = $true },
    # Semantic Model & Report
    @{ Name = $SemanticModelName;              Type = "SemanticModel"; Required = $true },
    @{ Name = "HorizonBooksAnalytics";         Type = "Report";        Required = $false },
    # Data Agent
    @{ Name = "HorizonBooks DataAgent";        Type = "DataAgent";     Required = $false }
)

Write-Host "  Checking workspace items..." -ForegroundColor Cyan
Write-Host ""

foreach ($check in $checks) {
    $item = $allItems | Where-Object { $_.displayName -eq $check.Name -and $_.type -eq $check.Type } | Select-Object -First 1
    $status = if ($item) { "FOUND" } else { "MISSING" }
    $color  = if ($item) { "Green" } elseif ($check.Required) { "Red" } else { "Yellow" }
    $icon   = if ($item) { "[OK]" } elseif ($check.Required) { "[FAIL]" } else { "[SKIP]" }

    Write-Host "  $icon $($check.Type.PadRight(15)) : $($check.Name) — $status" -ForegroundColor $color

    if ($item) {
        Write-Host "       ID: $($item.id)" -ForegroundColor Gray
        $passed++
    }
    else {
        if ($check.Required) { $failed++ } else { $passed++ }
    }
}

# ── Check Dataflow connections ────────────────────────────────────────
Write-Host ""
Write-Host "  Checking Dataflow connections..." -ForegroundColor Cyan
$dfNames = @("HorizonBooks_DF_Finance", "HorizonBooks_DF_HR", "HorizonBooks_DF_Operations")
foreach ($dfName in $dfNames) {
    $dfItem = $allItems | Where-Object { $_.displayName -eq $dfName -and $_.type -eq "Dataflow" } | Select-Object -First 1
    if (-not $dfItem) { continue }
    try {
        $conns = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$($dfItem.id)/connections" -Headers $headers).value
        $bound = $conns | Where-Object { $_.connectivityType -ne "None" }
        if ($bound -and $bound.Count -gt 0) {
            Write-Host "  [OK]   $dfName — connection bound" -ForegroundColor Green
            $passed++
        }
        else {
            Write-Host "  [WARN] $dfName — connection NOT bound (configure in portal)" -ForegroundColor Yellow
            Write-Host "         https://app.fabric.microsoft.com/groups/$WorkspaceId/dataflows/$($dfItem.id)" -ForegroundColor Cyan
        }
    }
    catch {
        Write-Host "  [WARN] $dfName — could not check connections: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

# ── Check GoldLH tables ──────────────────────────────────────────────
Write-Host ""
Write-Host "  Checking GoldLH tables..." -ForegroundColor Cyan
$goldLH = $allItems | Where-Object { $_.displayName -eq "GoldLH" -and $_.type -eq "Lakehouse" } | Select-Object -First 1
if ($goldLH) {
    try {
        $lhDetail = Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses/$($goldLH.id)/tables" -Headers $headers
        $tableCount = $lhDetail.data.Count
        $expectedTables = @(
            # Dimensions
            "DimDate", "DimAccounts", "DimCostCenters", "DimBooks", "DimAuthors",
            "DimGeography", "DimCustomers", "DimEmployees", "DimDepartments", "DimWarehouses",
            # Facts
            "FactFinancialTransactions", "FactBudget", "FactOrders",
            "FactInventory", "FactReturns", "FactPayroll",
            "FactPerformanceReviews", "FactRecruitment",
            # Analytics
            "GoldCohortAnalysis", "GoldRevenueAnomalies", "GoldBookCoPurchase", "GoldRevenueForecast",
            # Forecasts
            "ForecastSalesRevenue", "ForecastGenreDemand", "ForecastFinancial",
            "ForecastInventoryDemand", "ForecastWorkforce"
        )
        $existingTableNames = $lhDetail.data | ForEach-Object { $_.name }

        foreach ($et in $expectedTables) {
            $found = $existingTableNames -contains $et
            $icon  = if ($found) { "[OK]" } else { "[MISS]" }
            $color = if ($found) { "Green" } else { "Yellow" }
            Write-Host "    $icon Table: $et" -ForegroundColor $color
        }
        Write-Host "  Total tables in GoldLH: $tableCount" -ForegroundColor Gray
    }
    catch {
        Write-Host "  [WARN] Could not enumerate GoldLH tables: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}
else {
    Write-Host "  [SKIP] GoldLH not found — skipping table check" -ForegroundColor Yellow
}

# ── Summary ──────────────────────────────────────────────────────────
Write-Host ""
Write-Host "======================================================" -ForegroundColor $(if ($failed -eq 0) { "Green" } else { "Red" })
Write-Host "  VALIDATION: $passed passed, $failed failed" -ForegroundColor $(if ($failed -eq 0) { "Green" } else { "Red" })
Write-Host "======================================================" -ForegroundColor $(if ($failed -eq 0) { "Green" } else { "Red" })
Write-Host ""

if ($failed -gt 0) {
    Write-Host "  Run Deploy-Full.ps1 to create missing items." -ForegroundColor Yellow
    exit 1
}
