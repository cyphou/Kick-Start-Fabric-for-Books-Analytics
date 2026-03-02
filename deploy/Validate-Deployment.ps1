<#
.SYNOPSIS
    Validates a Horizon Books Fabric deployment.

.DESCRIPTION
    Checks that all expected items (Lakehouse, Notebooks, Semantic Model, Data Agent)
    exist in the specified workspace and reports their status.

.PARAMETER WorkspaceId
    The GUID of the Fabric workspace to validate.

.PARAMETER LakehouseName
    Expected lakehouse name. Defaults to HorizonBooksLH.

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
    [string]$LakehouseName = "HorizonBooksLH",

    [Parameter(Mandatory = $false)]
    [string]$SemanticModelName = "HorizonBooksModel"
)

$ErrorActionPreference = "Stop"
$FabricApiBase = "https://api.fabric.microsoft.com/v1"

function Get-FabricToken {
    try {
        $token = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"
        return $token.Token
    }
    catch {
        Write-Error "Failed to get Fabric API token. Run 'Connect-AzAccount' first."
        throw
    }
}

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  Horizon Books - Deployment Validation" -ForegroundColor Cyan
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

# Expected items
$checks = @(
    @{ Name = $LakehouseName; Type = "Lakehouse"; Required = $true },
    @{ Name = "HorizonBooks_01_BronzeToSilver"; Type = "Notebook"; Required = $true },
    @{ Name = "HorizonBooks_02_WebEnrichment"; Type = "Notebook"; Required = $true },
    @{ Name = "HorizonBooks_03_SilverToGold"; Type = "Notebook"; Required = $true },
    @{ Name = "HorizonBooks_DF_Finance"; Type = "Dataflow"; Required = $true },
    @{ Name = "HorizonBooks_DF_HR"; Type = "Dataflow"; Required = $true },
    @{ Name = "HorizonBooks_DF_Operations"; Type = "Dataflow"; Required = $true },
    @{ Name = "PL_HorizonBooks_Orchestration"; Type = "DataPipeline"; Required = $true },
    @{ Name = $SemanticModelName; Type = "SemanticModel"; Required = $true },
    @{ Name = "HorizonBooks DataAgent"; Type = "DataAgent"; Required = $false }
)

foreach ($check in $checks) {
    $item = $allItems | Where-Object { $_.displayName -eq $check.Name -and $_.type -eq $check.Type } | Select-Object -First 1
    $status = if ($item) { "FOUND" } else { "MISSING" }
    $color = if ($item) { "Green" } elseif ($check.Required) { "Red" } else { "Yellow" }
    $icon = if ($item) { "[OK]" } elseif ($check.Required) { "[FAIL]" } else { "[SKIP]" }

    Write-Host "  $icon $($check.Type.PadRight(15)) : $($check.Name) - $status" -ForegroundColor $color

    if ($item) {
        Write-Host "       ID: $($item.id)" -ForegroundColor Gray
        $passed++
    }
    else {
        if ($check.Required) { $failed++ } else { $passed++ }
    }
}

# Check Dataflow connections
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
            Write-Host "  [OK]   $dfName - connection bound" -ForegroundColor Green
            $passed++
        }
        else {
            Write-Host "  [WARN] $dfName - connection NOT bound (configure in portal)" -ForegroundColor Yellow
            Write-Host "         https://app.fabric.microsoft.com/groups/$WorkspaceId/dataflows/$($dfItem.id)" -ForegroundColor Cyan
        }
    }
    catch {
        Write-Host "  [WARN] $dfName - could not check connections: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

# Check lakehouse tables
Write-Host ""
Write-Host "  Checking Lakehouse tables..." -ForegroundColor Cyan
$lh = $allItems | Where-Object { $_.displayName -eq $LakehouseName -and $_.type -eq "Lakehouse" } | Select-Object -First 1
if ($lh) {
    try {
        $lhDetail = Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses/$($lh.id)/tables" -Headers $headers
        $tableCount = $lhDetail.data.Count
        $expectedTables = @("DimDate", "DimAccounts", "DimCostCenters", "DimBooks", "DimAuthors",
            "DimGeography", "DimCustomers", "DimEmployees", "DimDepartments", "DimWarehouses",
            "FactFinancialTransactions", "FactBudget", "FactOrders",
            "FactInventory", "FactReturns", "FactPayroll",
            "FactPerformanceReviews", "FactRecruitment")
        $existingTableNames = $lhDetail.data | ForEach-Object { $_.name }

        foreach ($et in $expectedTables) {
            $found = $existingTableNames -contains $et
            $icon = if ($found) { "[OK]" } else { "[MISS]" }
            $color = if ($found) { "Green" } else { "Yellow" }
            Write-Host "    $icon Table: $et" -ForegroundColor $color
        }
        Write-Host "  Total tables in lakehouse: $tableCount" -ForegroundColor Gray
    }
    catch {
        Write-Host "  [WARN] Could not enumerate lakehouse tables: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

# Summary
Write-Host ""
Write-Host "======================================================" -ForegroundColor $(if ($failed -eq 0) { "Green" } else { "Red" })
Write-Host "  VALIDATION: $passed passed, $failed failed" -ForegroundColor $(if ($failed -eq 0) { "Green" } else { "Red" })
Write-Host "======================================================" -ForegroundColor $(if ($failed -eq 0) { "Green" } else { "Red" })
Write-Host ""

if ($failed -gt 0) {
    Write-Host "  Run Deploy-HorizonBooks.ps1 to create missing items." -ForegroundColor Yellow
    exit 1
}
