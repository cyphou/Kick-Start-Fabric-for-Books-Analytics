<#
.SYNOPSIS
    Deploys the Power BI Semantic Model and Reports for Horizon Books to Microsoft Fabric.

.DESCRIPTION
    Standalone deployment script for the Power BI layer:

    Step 1: Deploy Semantic Model (TMDL Direct Lake on GoldLH)
            - 23 tables across dim/fact/analytics schemas
            - 27 relationships, 96 DAX measures
            - Auto-binds to GoldLH SQL endpoint

    Step 2: Deploy Power BI Analytics Report (PBIR)
            - 10-page interactive report
            - Bound to the Semantic Model deployed in Step 1

    Step 3: Deploy Power BI Forecasting Report (PBIR)
            - 5-page forecasting report (Sales, Genre, Financial, Inventory, Workforce)
            - Bound to the same Semantic Model

    This script can be run independently after the data pipeline has
    populated the Gold Lakehouse, or as part of Deploy-Full.ps1.

.PARAMETER WorkspaceId
    The GUID of the Fabric workspace.

.PARAMETER GoldLakehouseId
    The GUID of the Gold Lakehouse. If not provided, the script will
    look up a Lakehouse named per -GoldLakehouseName.

.PARAMETER GoldLakehouseName
    Name of the Gold Lakehouse. Defaults to GoldLH.

.PARAMETER SemanticModelName
    Name for the Semantic Model. Defaults to HorizonBooksModel.

.PARAMETER ReportName
    Name for the Analytics PBIR Report. Defaults to HorizonBooksAnalytics.

.PARAMETER ForecastReportName
    Name for the Forecasting PBIR Report. Defaults to HorizonBooksForecasting.

.PARAMETER SkipReport
    If set, deploys only the Semantic Model (no reports).

.EXAMPLE
    # Deploy Semantic Model + both reports
    .\Deploy-PowerBI.ps1 -WorkspaceId "your-workspace-guid"

.EXAMPLE
    # Deploy with explicit Gold Lakehouse ID
    .\Deploy-PowerBI.ps1 -WorkspaceId "ws-guid" -GoldLakehouseId "gold-lh-guid"

.EXAMPLE
    # Deploy only the Semantic Model
    .\Deploy-PowerBI.ps1 -WorkspaceId "ws-guid" -SkipReport
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,

    [Parameter(Mandatory = $false)]
    [string]$GoldLakehouseId,

    [Parameter(Mandatory = $false)]
    [string]$GoldLakehouseName = "GoldLH",

    [Parameter(Mandatory = $false)]
    [string]$SemanticModelName = "HorizonBooksModel",

    [Parameter(Mandatory = $false)]
    [string]$ReportName = "HorizonBooksAnalytics",

    [Parameter(Mandatory = $false)]
    [string]$ForecastReportName = "HorizonBooksForecasting",

    [Parameter(Mandatory = $false)]
    [switch]$SkipReport,

    [Parameter(Mandatory = $false)]
    [string]$SemanticModelFolderName = "Semantic Model",

    [Parameter(Mandatory = $false)]
    [string]$ReportFolderName = "Report",

    [Parameter(Mandatory = $false)]
    [string]$ParentFolderName = "05 - Analytics"
)

$ErrorActionPreference = "Stop"
Import-Module (Join-Path $PSScriptRoot 'HorizonBooks.psm1') -Force

# ── Imported from HorizonBooks.psm1 ──────────────────────────────────────
#   Write-Banner, Write-Step, Write-Info, Write-Success, Write-Warn
#   Get-FabricToken, Invoke-FabricApi, New-OrGetFabricItem
#   Update-FabricItemDefinition
#   $FabricApiBase

# Resolve paths
$scriptDir   = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$projectRoot = Split-Path -Parent $scriptDir

$FabricApiBase = $script:FabricApiBase

# ============================================================================
#                           MAIN DEPLOYMENT
# ============================================================================

Write-Banner "Horizon Books - Power BI Deployment"
Write-Host ""
Write-Host "  Workspace      : $WorkspaceId" -ForegroundColor White
Write-Host "  Gold Lakehouse : $GoldLakehouseName" -ForegroundColor White
Write-Host "  Semantic Model : $SemanticModelName (folder: $ParentFolderName/$SemanticModelFolderName)" -ForegroundColor White
Write-Host "  Report         : $(if ($SkipReport) { 'Skipped' } else { "$ReportName + $ForecastReportName (folder: $ParentFolderName/$ReportFolderName)" })" -ForegroundColor White
Write-Host ""

# ------------------------------------------------------------------
# Step 0: Authenticate & Resolve Gold Lakehouse
# ------------------------------------------------------------------
Write-Step "0/2" "Authenticating and resolving Gold Lakehouse"

$account = Get-AzContext
if (-not $account) {
    Write-Info "No active session - launching interactive login..."
    Connect-AzAccount | Out-Null
}
else {
    Write-Info "Session: $($account.Account.Id)"
}

$fabricToken = Get-FabricToken

# Resolve Gold Lakehouse ID if not provided
if (-not $GoldLakehouseId) {
    Write-Info "Looking up Lakehouse '$GoldLakehouseName'..."
    $lhItems = (Invoke-RestMethod `
        -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses" `
        -Headers @{ Authorization = "Bearer $fabricToken" }).value
    $goldLh = $lhItems | Where-Object { $_.displayName -eq $GoldLakehouseName } | Select-Object -First 1

    if (-not $goldLh) {
        Write-Error "Gold Lakehouse '$GoldLakehouseName' not found. Deploy lakehouses first."
        exit 1
    }
    $GoldLakehouseId = $goldLh.id
    Write-Success "Found $GoldLakehouseName : $GoldLakehouseId"
}

# Get SQL endpoint from Gold Lakehouse
$sqlEndpoint = ""
Write-Info "Retrieving GoldLH SQL endpoint..."
$maxWait = 120; $waited = 0
while ($waited -lt $maxWait) {
    try {
        $fabricToken = Get-FabricToken
        $lhProps = Invoke-FabricApi -Method Get `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses/$GoldLakehouseId" `
            -Token $fabricToken
        if ($lhProps.properties.sqlEndpointProperties.connectionString) {
            $sqlEndpoint = $lhProps.properties.sqlEndpointProperties.connectionString
            Write-Success "SQL endpoint: $sqlEndpoint"
            break
        }
        Write-Info "  SQL endpoint not ready ($($waited)s)..."
    }
    catch { Write-Info "  Waiting ($($waited)s)..." }
    Start-Sleep -Seconds 15; $waited += 15
}
if (-not $sqlEndpoint) {
    Write-Warn "SQL endpoint not available - semantic model expressions.tmdl may need manual configuration."
}

Write-Success "Authenticated and resolved Gold Lakehouse"

# ------------------------------------------------------------------
# Resolve / Create target folders
# ------------------------------------------------------------------
Write-Info "Resolving workspace folders..."
$fabricToken = Get-FabricToken
$allFolders = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" `
    -Headers @{ Authorization = "Bearer $fabricToken"; "Content-Type" = "application/json" }).value

# Find or create parent folder
$parentFolder = $allFolders | Where-Object { $_.displayName -eq $ParentFolderName } | Select-Object -First 1
if (-not $parentFolder) {
    Write-Info "Creating parent folder '$ParentFolderName'..."
    $body = @{ displayName = $ParentFolderName } | ConvertTo-Json
    $parentFolder = Invoke-RestMethod -Method Post -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" `
        -Headers @{ Authorization = "Bearer $fabricToken"; "Content-Type" = "application/json" } -Body $body
    Write-Success "Created folder '$ParentFolderName': $($parentFolder.id)"
} else {
    Write-Info "Found parent folder '$ParentFolderName': $($parentFolder.id)"
}
$parentFolderId = $parentFolder.id

# Refresh folder list after potential creation
$allFolders = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" `
    -Headers @{ Authorization = "Bearer $fabricToken"; "Content-Type" = "application/json" }).value

# Find or create SM subfolder
$smFolder = $allFolders | Where-Object { $_.displayName -eq $SemanticModelFolderName -and $_.parentFolderId -eq $parentFolderId } | Select-Object -First 1
if (-not $smFolder) {
    Write-Info "Creating subfolder '$SemanticModelFolderName' under '$ParentFolderName'..."
    $body = @{ displayName = $SemanticModelFolderName; parentFolderId = $parentFolderId } | ConvertTo-Json
    $smFolder = Invoke-RestMethod -Method Post -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" `
        -Headers @{ Authorization = "Bearer $fabricToken"; "Content-Type" = "application/json" } -Body $body
    Write-Success "Created folder '$SemanticModelFolderName': $($smFolder.id)"
} else {
    Write-Info "Found subfolder '$SemanticModelFolderName': $($smFolder.id)"
}
$smFolderId = $smFolder.id

# Find or create Report subfolder
if (-not $SkipReport) {
    $rptFolder = $allFolders | Where-Object { $_.displayName -eq $ReportFolderName -and $_.parentFolderId -eq $parentFolderId } | Select-Object -First 1
    if (-not $rptFolder) {
        Write-Info "Creating subfolder '$ReportFolderName' under '$ParentFolderName'..."
        $body = @{ displayName = $ReportFolderName; parentFolderId = $parentFolderId } | ConvertTo-Json
        $rptFolder = Invoke-RestMethod -Method Post -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" `
            -Headers @{ Authorization = "Bearer $fabricToken"; "Content-Type" = "application/json" } -Body $body
        Write-Success "Created folder '$ReportFolderName': $($rptFolder.id)"
    } else {
        Write-Info "Found subfolder '$ReportFolderName': $($rptFolder.id)"
    }
    $rptFolderId = $rptFolder.id
}
Write-Success "Folder structure ready"

# ------------------------------------------------------------------
# Step 1: Deploy Semantic Model (TMDL)
# ------------------------------------------------------------------
Write-Step "1/2" "Deploying Semantic Model '$SemanticModelName' (TMDL Direct Lake)"

$tmdlRoot      = Join-Path $projectRoot "HorizonBooksAnalytics\HorizonBooksAnalytics.SemanticModel"
$tmdlDefDir    = Join-Path $tmdlRoot "definition"
$tmdlTablesDir = Join-Path $tmdlDefDir "tables"

if (-not (Test-Path $tmdlRoot)) {
    Write-Error "TMDL folder not found at $tmdlRoot"
    exit 1
}

$smParts = @()

# definition.pbism
$pbismPath = Join-Path $tmdlRoot "definition.pbism"
if (Test-Path $pbismPath) {
    $bytes = [IO.File]::ReadAllBytes($pbismPath)
    $b64 = [Convert]::ToBase64String($bytes)
    $smParts += '{"path":"definition.pbism","payload":"' + $b64 + '","payloadType":"InlineBase64"}'
    Write-Info "Loaded definition.pbism"
}

# definition/*.tmdl - read as raw bytes to avoid BOM issues
foreach ($f in (Get-ChildItem -Path $tmdlDefDir -Filter "*.tmdl" -File | Sort-Object Name)) {
    $bytes = [IO.File]::ReadAllBytes($f.FullName)
    # Strip UTF-8 BOM if present
    if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
        $bytes = $bytes[3..($bytes.Length - 1)]
    }
    if ($f.Name -eq "expressions.tmdl") {
        $text = [Text.Encoding]::UTF8.GetString($bytes)
        $text = $text -replace '\{\{SQL_ENDPOINT\}\}', $sqlEndpoint
        $text = $text -replace '\{\{LAKEHOUSE_NAME\}\}', $GoldLakehouseName
        $bytes = [Text.Encoding]::UTF8.GetBytes($text)
    }
    $b64 = [Convert]::ToBase64String($bytes)
    $smParts += '{"path":"definition/' + $f.Name + '","payload":"' + $b64 + '","payloadType":"InlineBase64"}'
}

# definition/tables/*.tmdl - read as raw bytes
if (Test-Path $tmdlTablesDir) {
    foreach ($f in (Get-ChildItem -Path $tmdlTablesDir -Filter "*.tmdl" -File | Sort-Object Name)) {
        $bytes = [IO.File]::ReadAllBytes($f.FullName)
        # Strip UTF-8 BOM if present
        if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
            $bytes = $bytes[3..($bytes.Length - 1)]
        }
        $b64 = [Convert]::ToBase64String($bytes)
        $smParts += '{"path":"definition/tables/' + $f.Name + '","payload":"' + $b64 + '","payloadType":"InlineBase64"}'
    }
}

Write-Info "Total TMDL parts: $($smParts.Count)"

$smDesc    = "Direct Lake semantic model on GoldLH - 23 tables (dim/fact/analytics schemas), 27 relationships, 96 DAX measures, 5 forecast tables"
$partsJson = $smParts -join ","
$createSmJson = '{"displayName":"' + $SemanticModelName + '","type":"SemanticModel","description":"' + $smDesc + '","folderId":"' + $smFolderId + '","definition":{"parts":[' + $partsJson + ']}}'

$semanticModelId = $null
$smLroFailed = $false

try {
    $fabricToken = Get-FabricToken
    $smHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }

    $smResponse = Invoke-WebRequest -Method Post `
        -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
        -Headers $smHeaders -Body $createSmJson -UseBasicParsing

    if ($smResponse.StatusCode -eq 202) {
        Write-Info "Semantic model creation accepted (202). Polling..."
        $smOpUrl = $null
        try { $smOpUrl = $smResponse.Headers["Location"] } catch {}
        if (-not $smOpUrl) {
            try {
                $opId = $smResponse.Headers["x-ms-operation-id"]
                if ($opId) { $smOpUrl = "$FabricApiBase/operations/$opId" }
            } catch {}
        }
        if ($smOpUrl) {
            $smMaxPoll = 120; $smPolled = 0
            while ($smPolled -lt $smMaxPoll) {
                Start-Sleep -Seconds 10; $smPolled += 10
                try {
                    $fabricToken = Get-FabricToken
                    $pollData = Invoke-RestMethod -Method Get -Uri $smOpUrl `
                        -Headers @{ "Authorization" = "Bearer $fabricToken" }
                    Write-Info "  Operation: $($pollData.status) ($($smPolled)s)"
                    if ($pollData.status -eq "Succeeded") { break }
                    if ($pollData.status -eq "Failed") {
                        $errDetail = $pollData | ConvertTo-Json -Depth 10 -Compress
                        Write-Warn "SM creation LRO failed: $errDetail"
                        try {
                            $resultUrl = "$smOpUrl/result"
                            $resultData = Invoke-RestMethod -Uri $resultUrl -Headers @{ "Authorization" = "Bearer $fabricToken" }
                            Write-Warn "SM LRO Result: $($resultData | ConvertTo-Json -Depth 10 -Compress)"
                        } catch { Write-Warn "Could not retrieve SM LRO result: $($_.Exception.Message)" }
                        $smLroFailed = $true
                        break
                    }
                }
                catch { Write-Warn "SM poll error: $($_.Exception.Message)"; $smLroFailed = $true; break }
            }
        }
        else { Start-Sleep -Seconds 15 }

        # Look up created/existing model
        $fabricToken = Get-FabricToken
        $smItems = Invoke-FabricApi -Method Get `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=SemanticModel" -Token $fabricToken
        $sm = $smItems.value | Where-Object { $_.displayName -eq $SemanticModelName } | Select-Object -First 1
        $semanticModelId = $sm.id

        # Always update definition when item already existed (creation 202 may not update existing definition)
        if ($semanticModelId) {
            Write-Info "Updating semantic model definition to ensure latest TMDL is applied..."
            $updateJson = '{"definition":{"parts":[' + $partsJson + ']}}'
            $fabricToken = Get-FabricToken
            $updated = Update-FabricItemDefinition -ItemId $semanticModelId `
                -WsId $WorkspaceId -DefinitionJson $updateJson -Token $fabricToken
            if ($updated) {
                Write-Success "Semantic model definition updated: $semanticModelId"
            }
            else {
                Write-Warn "Semantic model definition update may have failed"
            }
        }
    }
    else {
        $sm = $smResponse.Content | ConvertFrom-Json
        $semanticModelId = $sm.id
    }
    Write-Success "Semantic model deployed: $semanticModelId"
}
catch {
    $errMsg = $_.Exception.Message
    if ($errMsg -like "*ItemDisplayNameAlreadyInUse*") {
        Write-Info "Semantic model '$SemanticModelName' already exists."
        $fabricToken = Get-FabricToken
        $smItems = Invoke-FabricApi -Method Get `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=SemanticModel" -Token $fabricToken
        $sm = $smItems.value | Where-Object { $_.displayName -eq $SemanticModelName } | Select-Object -First 1
        $semanticModelId = $sm.id

        if ($semanticModelId) {
            Write-Info "Updating existing semantic model definition..."
            $updateJson = '{"definition":{"parts":[' + $partsJson + ']}}'
            $fabricToken = Get-FabricToken
            $updated = Update-FabricItemDefinition -ItemId $semanticModelId `
                -WsId $WorkspaceId -DefinitionJson $updateJson -Token $fabricToken
            if ($updated) {
                Write-Success "Semantic model definition updated: $semanticModelId"
            }
            else {
                Write-Warn "Semantic model definition update may have failed"
            }
        }
    }
    else { Write-Warn "Semantic model deployment issue: $errMsg" }
}

# ------------------------------------------------------------------
# Helper: Deploy a PBIR Report
# ------------------------------------------------------------------
function Deploy-PbirReport {
    param(
        [string]$RptName,
        [string]$RptRoot,
        [string]$Description,
        [string]$FolderId,
        [string]$SmId
    )

    $rptDefDir = Join-Path $RptRoot "definition"
    if (-not (Test-Path $RptRoot)) {
        Write-Warn "Report folder not found at $RptRoot - skipping"
        return $null
    }

    $parts = @()

    # --- definition.pbir: rewrite byPath -> byConnection with SM ID ---
    $pbirPath = Join-Path $RptRoot "definition.pbir"
    if (Test-Path $pbirPath) {
        if ($SmId) {
            $pbirObj = @{
                version = "4.0"
                datasetReference = @{
                    byPath = $null
                    byConnection = @{
                        connectionString          = $null
                        pbiServiceModelId         = $null
                        pbiModelVirtualServerName = "sobe_wowvirtualserver"
                        pbiModelDatabaseName      = $SmId
                        name                      = "EntityDataSource"
                        connectionType            = "pbiServiceXmlaStyleLive"
                    }
                }
            }
            $pbirJson  = $pbirObj | ConvertTo-Json -Depth 5 -Compress
            $pbirBytes = [Text.Encoding]::UTF8.GetBytes($pbirJson)
        }
        else {
            Write-Warn "Semantic model ID not available - using byPath reference"
            $pbirBytes = [IO.File]::ReadAllBytes($pbirPath)
        }
        $b64 = [Convert]::ToBase64String($pbirBytes)
        $parts += '{"path":"definition.pbir","payload":"' + $b64 + '","payloadType":"InlineBase64"}'
        Write-Info "Loaded definition.pbir (bound to SM $SmId)"
    }

    # --- Recursively collect all files under definition/ ---
    if (Test-Path $rptDefDir) {
        $defFiles = Get-ChildItem -Path $rptDefDir -Recurse -File | Sort-Object FullName
        foreach ($f in $defFiles) {
            $relPath = $f.FullName.Substring($RptRoot.Length + 1).Replace('\', '/')
            if ($relPath -like "definition/RegisteredResources/*") {
                $relPath = $relPath -replace '^definition/RegisteredResources/', 'StaticResources/RegisteredResources/'
            }
            $bytes = [IO.File]::ReadAllBytes($f.FullName)
            $b64   = [Convert]::ToBase64String($bytes)
            $parts += '{"path":"' + $relPath + '","payload":"' + $b64 + '","payloadType":"InlineBase64"}'
        }
        Write-Info "Loaded $($defFiles.Count) report definition files"
    }

    Write-Info "Total report parts: $($parts.Count)"

    $partsJson = $parts -join ","
    $createJson = '{"displayName":"' + $RptName + '","type":"Report","description":"' + $Description + '","folderId":"' + $FolderId + '","definition":{"parts":[' + $partsJson + ']}}'

    $rptId = $null

    try {
        $fabricToken = Get-FabricToken
        $rptHeaders  = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }

        $rptResponse = Invoke-WebRequest -Method Post `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
            -Headers $rptHeaders -Body $createJson -UseBasicParsing

        if ($rptResponse.StatusCode -eq 202) {
            Write-Info "Report creation accepted (202). Polling..."
            $rptOpUrl = $null
            try { $rptOpUrl = $rptResponse.Headers["Location"] } catch {}
            if (-not $rptOpUrl) {
                try {
                    $opId = $rptResponse.Headers["x-ms-operation-id"]
                    if ($opId) { $rptOpUrl = "$FabricApiBase/operations/$opId" }
                } catch {}
            }
            if ($rptOpUrl) {
                $rptMaxPoll = 120; $rptPolled = 0
                while ($rptPolled -lt $rptMaxPoll) {
                    Start-Sleep -Seconds 10; $rptPolled += 10
                    try {
                        $fabricToken = Get-FabricToken
                        $pollData = Invoke-RestMethod -Method Get -Uri $rptOpUrl `
                            -Headers @{ "Authorization" = "Bearer $fabricToken" }
                        Write-Info "  Operation: $($pollData.status) ($($rptPolled)s)"
                        if ($pollData.status -eq "Succeeded") { break }
                        if ($pollData.status -eq "Failed") {
                            Write-Warn "Report creation LRO failed - will try update"
                            break
                        }
                    }
                    catch { Write-Warn "Report poll error: $($_.Exception.Message)"; break }
                }
            }
            else { Start-Sleep -Seconds 15 }

            # Look up created/existing report
            $fabricToken = Get-FabricToken
            $rptItems = Invoke-FabricApi -Method Get `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Report" -Token $fabricToken
            $rpt = $rptItems.value | Where-Object { $_.displayName -eq $RptName } | Select-Object -First 1
            $rptId = $rpt.id

            if ($rptId) {
                Write-Info "Updating report definition to ensure latest PBIR is applied..."
                $updateJson = '{"definition":{"parts":[' + $partsJson + ']}}'
                $fabricToken = Get-FabricToken
                $updated = Update-FabricItemDefinition -ItemId $rptId `
                    -WsId $WorkspaceId -DefinitionJson $updateJson -Token $fabricToken
                if ($updated) { Write-Success "Report definition updated: $rptId" }
                else          { Write-Warn "Report definition update may have failed" }
            }
        }
        else {
            $rpt = $rptResponse.Content | ConvertFrom-Json
            $rptId = $rpt.id
        }
        Write-Success "Report deployed: $RptName ($rptId)"
    }
    catch {
        $errMsg = $_.Exception.Message
        if ($errMsg -like "*ItemDisplayNameAlreadyInUse*") {
            Write-Info "Report '$RptName' already exists - updating definition."
            $fabricToken = Get-FabricToken
            $rptItems = Invoke-FabricApi -Method Get `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Report" -Token $fabricToken
            $rpt = $rptItems.value | Where-Object { $_.displayName -eq $RptName } | Select-Object -First 1
            $rptId = $rpt.id

            if ($rptId) {
                $updateJson = '{"definition":{"parts":[' + $partsJson + ']}}'
                $fabricToken = Get-FabricToken
                $updated = Update-FabricItemDefinition -ItemId $rptId `
                    -WsId $WorkspaceId -DefinitionJson $updateJson -Token $fabricToken
                if ($updated) { Write-Success "Report definition updated: $rptId" }
                else          { Write-Warn "Report definition update may have failed" }
            }
        }
        else { Write-Warn "Report deployment issue: $errMsg" }
    }

    return $rptId
}

# ------------------------------------------------------------------
# Step 2: Deploy Power BI Analytics Report (PBIR)
# ------------------------------------------------------------------
$reportId = $null
$forecastReportId = $null

if (-not $SkipReport) {
    Write-Step "2/3" "Deploying Analytics Report '$ReportName' (PBIR)"

    $analyticsRoot = Join-Path $projectRoot "HorizonBooksAnalytics\HorizonBooksAnalytics.Report"
    $reportId = Deploy-PbirReport -RptName $ReportName `
        -RptRoot $analyticsRoot `
        -Description "Horizon Books Analytics - 10-page Power BI report (PBIR) bound to $SemanticModelName" `
        -FolderId $rptFolderId -SmId $semanticModelId

    # ------------------------------------------------------------------
    # Step 3: Deploy Power BI Forecasting Report (PBIR)
    # ------------------------------------------------------------------
    Write-Step "3/3" "Deploying Forecasting Report '$ForecastReportName' (PBIR)"

    $forecastRoot = Join-Path $projectRoot "HorizonBooksForecasting\HorizonBooksForecasting.Report"
    $forecastReportId = Deploy-PbirReport -RptName $ForecastReportName `
        -RptRoot $forecastRoot `
        -Description "Horizon Books Forecasting - 5-page forecast report (PBIR) bound to $SemanticModelName" `
        -FolderId $rptFolderId -SmId $semanticModelId
}

# ============================================================================
# SUMMARY
# ============================================================================

Write-Host ""
Write-Banner "POWER BI DEPLOYMENT COMPLETE" "Green"
Write-Host ""
Write-Host "  Workspace      : $WorkspaceId" -ForegroundColor White
Write-Host "  Gold Lakehouse : $GoldLakehouseName ($GoldLakehouseId)" -ForegroundColor White
if ($sqlEndpoint) {
    Write-Host "  SQL Endpoint   : $sqlEndpoint" -ForegroundColor White
}
if ($semanticModelId) {
    Write-Host "  Semantic Model : $SemanticModelName ($semanticModelId)" -ForegroundColor White
}
if ($reportId) {
    Write-Host "  Analytics Rpt  : $ReportName ($reportId)" -ForegroundColor White
}
if ($forecastReportId) {
    Write-Host "  Forecast Rpt   : $ForecastReportName ($forecastReportId)" -ForegroundColor White
}
Write-Host ""
Write-Host "  Fabric Portal  : https://app.fabric.microsoft.com/groups/$WorkspaceId" -ForegroundColor Cyan
Write-Host ""
