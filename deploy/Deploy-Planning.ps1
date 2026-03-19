<#
.SYNOPSIS
    Deploys Planning in Fabric IQ tables and populates them with scenario data.

.DESCRIPTION
    Extends the Horizon Books demo with Planning in Fabric IQ capabilities:

    Step 1: Resolve GoldLH from the workspace
    Step 2: Populate planning tables via PySpark notebook (drops any pre-existing
            SQL-DDL shells, creates proper Delta tables with scenario data):
            - PlanRevenueTargets      - Channel revenue targets (3 scenarios)
            - PlanFinancialTargets    - P&L account targets from budget baseline
            - PlanWorkforceTargets    - Headcount and payroll targets by department
            - PlanVarianceAnalysis    - Consolidated plan-vs-actual variance
            - PlanScenarioSummary     - Executive scenario comparison
    Step 3: Execute SQL (schema + views only) after Delta tables exist
    Step 4: Create "07 - Planning" folder and organize planning items
    Step 5: Clean up temporary diagnostic artifacts

    Prerequisites: GoldLH must exist with populated fact tables (run Deploy-Full.ps1 first).

.PARAMETER WorkspaceId
    The GUID of the Fabric workspace containing the Horizon Books deployment.

.PARAMETER GoldLakehouseName
    Name of the Gold Lakehouse. Defaults to GoldLH.

.PARAMETER SkipPopulate
    If set, creates schema and views but does not populate tables with data.

.EXAMPLE
    .\Deploy-Planning.ps1 -WorkspaceId "your-workspace-guid"

.EXAMPLE
    .\Deploy-Planning.ps1 -WorkspaceId "guid" -SkipPopulate
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,

    [Parameter(Mandatory = $false)]
    [string]$GoldLakehouseName = "GoldLH",

    [Parameter(Mandatory = $false)]
    [switch]$SkipPopulate
)

$ErrorActionPreference = "Stop"
$scriptDir   = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$projectRoot = Split-Path -Parent $scriptDir

Import-Module (Join-Path $PSScriptRoot 'HorizonBooks.psm1') -Force
$FabricApiBase = $script:FabricApiBase
if (-not $FabricApiBase) {
    $FabricApiBase = "https://api.fabric.microsoft.com/v1"
}

function Get-Token { return (Get-FabricToken) }

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  Horizon Books - Deploy Planning in Fabric IQ" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Workspace    : $WorkspaceId" -ForegroundColor White
Write-Host "  Gold LH      : $GoldLakehouseName" -ForegroundColor White
Write-Host "  Populate     : $(if ($SkipPopulate) { 'Skipped' } else { 'Yes' })" -ForegroundColor White
Write-Host ""

# == Step 1: Resolve GoldLH ==================================================
Write-Host "  [1/5] Resolving Lakehouse IDs..." -ForegroundColor Cyan

$token = Get-Token
$items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
    -Headers @{ Authorization = "Bearer $token" }).value

$goldLH = $items | Where-Object { $_.displayName -eq $GoldLakehouseName -and $_.type -eq "Lakehouse" } | Select-Object -First 1
if (-not $goldLH) {
    Write-Host "  [FAIL] GoldLH '$GoldLakehouseName' not found. Run Deploy-Full.ps1 first." -ForegroundColor Red
    exit 1
}
$goldId = $goldLH.id
Write-Host "  [OK]   GoldLH: $goldId" -ForegroundColor Green

# Build lakehouse metadata block (shared by both notebooks)
$NL = [char]10
$metaLines = @(
    ('# META   "dependencies": {'),
    ('# META     "lakehouse": {'),
    ('# META       "default_lakehouse": "' + $goldId + '",'),
    ('# META       "default_lakehouse_name": "' + $GoldLakehouseName + '",'),
    ('# META       "default_lakehouse_workspace_id": "' + $WorkspaceId + '",'),
    ('# META       "known_lakehouses": ['),
    ('# META         {'),
    ('# META           "id": "' + $goldId + '"'),
    ('# META         }'),
    ('# META       ]'),
    ('# META     }'),
    ('# META   }')
)
$lhMeta = $metaLines -join $NL

# == Step 2: Populate Planning Tables (PySpark) ===============================
Write-Host ""
Write-Host "  [2/5] Populating planning tables with scenario data..." -ForegroundColor Cyan

$popNbId = $null

if ($SkipPopulate) {
    Write-Host "  [SKIP] Table population skipped (-SkipPopulate)" -ForegroundColor Yellow
}
else {
    $popNbPyPath = Join-Path (Join-Path $projectRoot "notebooks") "Planning_Populate.py"
    if (-not (Test-Path $popNbPyPath)) {
        Write-Host "  [FAIL] Notebook file not found: $popNbPyPath" -ForegroundColor Red
        exit 1
    }
    $populateContent = Get-Content -Path $popNbPyPath -Raw -Encoding UTF8

    # Inject lakehouse metadata
    $populateContent = $populateContent -replace '# META\s+"dependencies":\s*\{\}', $lhMeta
    $populateContent = $populateContent -replace "`r`n", "`n"
    $popBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($populateContent))

    $popNbName = "HorizonBooks_Planning_Populate"
    $token = Get-Token
    $popNbId = New-OrGetFabricItem -DisplayName $popNbName -Type "Notebook" `
        -Description "Planning in Fabric IQ data population - scenario targets and variance analysis" `
        -WsId $WorkspaceId -Token $token

    if (-not $popNbId) {
        Write-Host "  [FAIL] Could not create population notebook" -ForegroundColor Red
        exit 1
    }

    $defJson = '{"definition":{"parts":[{"path":"notebook-content.py","payload":"' + $popBase64 + '","payloadType":"InlineBase64"}]}}'
    $token = Get-Token
    $defOk = Update-FabricItemDefinition -ItemId $popNbId -WsId $WorkspaceId -DefinitionJson $defJson -Token $token

    if (-not $defOk) {
        Write-Host "  [WARN] Population notebook definition upload may have failed" -ForegroundColor Yellow
    }

    Start-Sleep -Seconds 5
    $token = Get-Token
    $popOk = Run-FabricNotebook -NotebookId $popNbId -NotebookName $popNbName `
        -WsId $WorkspaceId -Token $token -TimeoutMinutes 15

    if ($popOk) {
        Write-Host "  [OK]   5 planning tables populated" -ForegroundColor Green
    }
    else {
        Write-Host "  [WARN] Population may not have completed. Run '$popNbName' manually." -ForegroundColor Yellow
    }
}

# == Step 3: Execute SQL (schema + views) =====================================
Write-Host ""
Write-Host "  [3/5] Executing CreatePlanningTables.sql (schema + views)..." -ForegroundColor Cyan

$sqlFilePath = Join-Path (Join-Path $projectRoot "Lakehouse") "CreatePlanningTables.sql"
if (-not (Test-Path $sqlFilePath)) {
    Write-Host "  [FAIL] SQL file not found: $sqlFilePath" -ForegroundColor Red
    exit 1
}

$sqlBytes = [IO.File]::ReadAllBytes($sqlFilePath)
$sqlB64   = [Convert]::ToBase64String($sqlBytes)

$sqlNbPyPath = Join-Path (Join-Path $projectRoot "notebooks") "Planning_SQLSetup.py"
if (-not (Test-Path $sqlNbPyPath)) {
    Write-Host "  [FAIL] Notebook file not found: $sqlNbPyPath" -ForegroundColor Red
    exit 1
}
$pyContent = Get-Content -Path $sqlNbPyPath -Raw -Encoding UTF8

# Inject the base64-encoded SQL and lakehouse metadata
$pyContent = $pyContent.Replace('__SQL_B64__', $sqlB64)
$pyContent = $pyContent -replace '# META\s+"dependencies":\s*\{\}', $lhMeta
$pyContent = $pyContent -replace "`r`n", "`n"
$nbBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($pyContent))

$sqlNbName = "HorizonBooks_Planning_SQLSetup"
$token = Get-Token

$sqlNbId = New-OrGetFabricItem -DisplayName $sqlNbName -Type "Notebook" `
    -Description "Planning schema and view setup (auto-generated by Deploy-Planning.ps1)" `
    -WsId $WorkspaceId -Token $token

if (-not $sqlNbId) {
    Write-Host "  [FAIL] Could not create SQL setup notebook" -ForegroundColor Red
    exit 1
}

$defJson = '{"definition":{"parts":[{"path":"notebook-content.py","payload":"' + $nbBase64 + '","payloadType":"InlineBase64"}]}}'
$token = Get-Token
$defOk = Update-FabricItemDefinition -ItemId $sqlNbId -WsId $WorkspaceId -DefinitionJson $defJson -Token $token

if (-not $defOk) {
    Write-Host "  [WARN] SQL notebook definition upload may have failed" -ForegroundColor Yellow
}

Start-Sleep -Seconds 5
$token = Get-Token
$sqlOk = Run-FabricNotebook -NotebookId $sqlNbId -NotebookName $sqlNbName `
    -WsId $WorkspaceId -Token $token -TimeoutMinutes 10

if ($sqlOk) {
    Write-Host "  [OK]   Planning schema + views created" -ForegroundColor Green
}
else {
    Write-Host "  [WARN] SQL execution may not have completed. Run '$sqlNbName' manually." -ForegroundColor Yellow
}

# == Step 4: Create "07 - Planning" folder and organize items =================
Write-Host ""
Write-Host "  [4/5] Creating '07 - Planning' folder and organizing items..." -ForegroundColor Cyan

try {
    $token = Get-Token
    $hdrs = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }

    # Create or get the Planning folder
    $planFolderName = "07 - Planning"
    $planFolderId = $null
    try {
        $resp = Invoke-WebRequest -Method Post `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" `
            -Headers $hdrs `
            -Body (@{ displayName = $planFolderName } | ConvertTo-Json -Depth 3) -UseBasicParsing
        if ($resp.StatusCode -in @(200, 201)) {
            $planFolderId = ($resp.Content | ConvertFrom-Json).id
            Write-Host "  [OK]   Created folder '$planFolderName': $planFolderId" -ForegroundColor Green
        }
    }
    catch {
        $errBody = ""
        try {
            if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $errBody = $_.ErrorDetails.Message }
        } catch {}
        if ($errBody -like "*already*" -or $errBody -like "*AlreadyExists*" -or $errBody -like "*DisplayName*") {
            $token = Get-Token
            $folders = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" `
                -Headers @{ Authorization = "Bearer $token" }).value
            $f = $folders | Where-Object { $_.displayName -eq $planFolderName } | Select-Object -First 1
            if ($f) {
                $planFolderId = $f.id
                Write-Host "  [OK]   Existing folder '$planFolderName': $planFolderId" -ForegroundColor Green
            }
        }
        else {
            Write-Host "  [WARN] Could not create folder: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }

    # Move planning notebooks to the folder
    if ($planFolderId) {
        $planNbs = @($sqlNbId, $popNbId) | Where-Object { $_ }
        $token = Get-Token
        $hdrs = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }

        foreach ($nbIdToMove in $planNbs) {
            try {
                Invoke-RestMethod -Method Post `
                    -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$nbIdToMove/move" `
                    -Headers $hdrs `
                    -Body (@{ workspaceFolderId = $planFolderId } | ConvertTo-Json -Depth 3) | Out-Null
                Start-Sleep -Seconds 2
            }
            catch {
                # Not critical - folder organization is best-effort
            }
        }
        Write-Host "  [OK]   Planning notebooks moved to '$planFolderName'" -ForegroundColor Green
    }
    else {
        Write-Host "  [SKIP] Could not resolve planning folder ID" -ForegroundColor Yellow
    }
}
catch {
    Write-Host "  [SKIP] Folder organization: $($_.Exception.Message)" -ForegroundColor Yellow
}

# == Step 5: Clean up temporary artifacts =====================================
Write-Host ""
Write-Host "  [5/5] Cleaning up temporary artifacts..." -ForegroundColor Cyan

try {
    $token = Get-Token
    $allItems = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
        -Headers @{ Authorization = "Bearer $token" }).value
    $checkNb = $allItems | Where-Object { $_.displayName -eq "HorizonBooks_Planning_Check" -and $_.type -eq "Notebook" }
    if ($checkNb) {
        Invoke-RestMethod -Method Delete `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$($checkNb.id)" `
            -Headers @{ Authorization = "Bearer $token" } | Out-Null
        Write-Host "  [OK]   Removed temporary diagnostic notebook" -ForegroundColor Green
    }
    else {
        Write-Host "  [OK]   No temporary artifacts to clean" -ForegroundColor Green
    }
}
catch {
    Write-Host "  [SKIP] Cleanup: $($_.Exception.Message)" -ForegroundColor Yellow
}

# == Summary ==================================================================
Write-Host ""
Write-Host "======================================================" -ForegroundColor Green
Write-Host "  Planning in Fabric IQ - Deployment Complete" -ForegroundColor Green
Write-Host "======================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Planning Tables (GoldLH.planning.*):" -ForegroundColor White
Write-Host "    - PlanRevenueTargets     (channel revenue targets)" -ForegroundColor White
Write-Host "    - PlanFinancialTargets   (P&L account targets)" -ForegroundColor White
Write-Host "    - PlanWorkforceTargets   (headcount and payroll)" -ForegroundColor White
Write-Host "    - PlanVarianceAnalysis   (plan vs actual variance)" -ForegroundColor White
Write-Host "    - PlanScenarioSummary    (executive comparison)" -ForegroundColor White
Write-Host ""
Write-Host "  Planning Views:" -ForegroundColor White
Write-Host "    - vw_RevenuePlanVsForecast" -ForegroundColor White
Write-Host "    - vw_FinancialPlanVsBudget" -ForegroundColor White
Write-Host "    - vw_WorkforcePlanSummary" -ForegroundColor White
Write-Host ""
Write-Host "  Workspace Folder: 07 - Planning" -ForegroundColor White
Write-Host ""
Write-Host "  Fabric Portal: https://app.fabric.microsoft.com/groups/$WorkspaceId" -ForegroundColor Cyan
Write-Host ""
