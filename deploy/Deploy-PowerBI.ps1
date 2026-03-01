<#
.SYNOPSIS
    Deploys the Power BI Semantic Model and Report for Horizon Books to Microsoft Fabric.

.DESCRIPTION
    Standalone deployment script for the Power BI layer:

    Step 1: Deploy Semantic Model (TMDL Direct Lake on GoldLH)
            - 23 tables across dim/fact/analytics schemas
            - 27 relationships, 96 DAX measures
            - Auto-binds to GoldLH SQL endpoint

    Step 2: Deploy Power BI Report (PBIR)
            - 10-page interactive report
            - Bound to the Semantic Model deployed in Step 1
            - Includes geographic map visuals, KPIs, drill-through

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
    Name for the PBIR Report. Defaults to HorizonBooksAnalytics.

.PARAMETER SkipReport
    If set, deploys only the Semantic Model (no report).

.EXAMPLE
    # Deploy both Semantic Model and Report
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
    [switch]$SkipReport
)

$ErrorActionPreference = "Stop"

# Resolve paths
$scriptDir   = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$projectRoot = Split-Path -Parent $scriptDir

# API endpoints
$FabricApiBase = "https://api.fabric.microsoft.com/v1"

# ============================================================================
# DISPLAY HELPERS
# ============================================================================

function Write-Step {
    param([string]$StepNum, [string]$Message)
    Write-Host ""
    Write-Host "  [$StepNum] $Message" -ForegroundColor Cyan
    Write-Host ("  " + "-" * 60) -ForegroundColor DarkGray
}

function Write-Info    { param([string]$M) Write-Host "      [INFO] $M" -ForegroundColor Gray }
function Write-Success { param([string]$M) Write-Host "      [ OK ] $M" -ForegroundColor Green }
function Write-Warn    { param([string]$M) Write-Host "      [WARN] $M" -ForegroundColor Yellow }

function Write-Banner {
    param([string]$Title, [ConsoleColor]$Color = "Yellow")
    Write-Host ""
    Write-Host ("=" * 70) -ForegroundColor $Color
    Write-Host "  $Title" -ForegroundColor $Color
    Write-Host ("=" * 70) -ForegroundColor $Color
}

# ============================================================================
# TOKEN HELPERS
# ============================================================================

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

# ============================================================================
# FABRIC API HELPERS
# ============================================================================

function Invoke-FabricApi {
    param(
        [string]$Method,
        [string]$Uri,
        [object]$Body = $null,
        [string]$BodyJson = $null,
        [string]$Token,
        [int]$MaxRetries = 10
    )

    $headers = @{
        "Authorization" = "Bearer $Token"
        "Content-Type"  = "application/json"
    }

    if (-not $BodyJson -and $Body) {
        $BodyJson = $Body | ConvertTo-Json -Depth 10
    }

    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            $params = @{
                Method          = $Method
                Uri             = $Uri
                Headers         = $headers
                UseBasicParsing = $true
            }
            if ($BodyJson) { $params["Body"] = $BodyJson }

            $webResponse = Invoke-WebRequest @params
            $statusCode  = $webResponse.StatusCode

            if ($statusCode -eq 202) {
                $locationHeader = $webResponse.Headers["Location"]
                if ($locationHeader) {
                    Write-Info "Waiting for long-running operation..."
                    $maxPoll = 180; $polled = 0
                    while ($polled -lt $maxPoll) {
                        Start-Sleep -Seconds 10; $polled += 10
                        try {
                            $pollData = Invoke-RestMethod -Method Get -Uri $locationHeader `
                                -Headers @{ "Authorization" = "Bearer $Token" }
                            Write-Info "  Operation: $($pollData.status) ($($polled)s)"
                            if ($pollData.status -eq "Succeeded") { return $pollData }
                            if ($pollData.status -eq "Failed") { Write-Warn "Operation failed"; return $null }
                        }
                        catch { Write-Warn "Poll error: $($_.Exception.Message)"; break }
                    }
                }
                return $null
            }

            if ($webResponse.Content) {
                try   { return $webResponse.Content | ConvertFrom-Json }
                catch { return $webResponse.Content }
            }
            return $null
        }
        catch {
            $ex = $_.Exception
            $statusCode = $null
            $errorBody  = ""
            if ($ex -and $ex.Response) {
                $statusCode = [int]$ex.Response.StatusCode
                try {
                    $sr = New-Object System.IO.StreamReader($ex.Response.GetResponseStream())
                    $errorBody = $sr.ReadToEnd(); $sr.Close()
                } catch {}
            }

            if ($statusCode -eq 429) {
                $retryAfter = 30
                Write-Warn "Rate limited (429) - retrying in $($retryAfter)s (attempt $attempt/$MaxRetries)"
                Start-Sleep -Seconds $retryAfter
            }
            else {
                if ($errorBody) { throw "Fabric API error (HTTP $statusCode): $errorBody" }
                throw
            }
        }
    }
    throw "Max retries exceeded for $Uri"
}

function New-OrGetFabricItem {
    param(
        [string]$DisplayName,
        [string]$Type,
        [string]$Description,
        [string]$WsId,
        [string]$Token
    )

    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
    $body = @{ displayName = $DisplayName; type = $Type; description = $Description } | ConvertTo-Json -Depth 5

    try {
        $resp = Invoke-WebRequest -Method Post -Uri "$FabricApiBase/workspaces/$WsId/items" `
            -Headers $headers -Body $body -UseBasicParsing

        if ($resp.StatusCode -eq 201) {
            return ($resp.Content | ConvertFrom-Json).id
        }
        elseif ($resp.StatusCode -eq 202) {
            $opUrl = $resp.Headers["Location"]
            if ($opUrl) {
                for ($p = 1; $p -le 24; $p++) {
                    Start-Sleep -Seconds 5
                    $poll = Invoke-RestMethod -Uri $opUrl -Headers @{ Authorization = "Bearer $Token" }
                    Write-Info "  LRO: $($poll.status) ($($p*5)s)"
                    if ($poll.status -eq "Succeeded") { break }
                    if ($poll.status -eq "Failed") { Write-Warn "LRO failed"; break }
                }
            }
            Start-Sleep -Seconds 3
            $items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WsId/items?type=$Type" `
                -Headers @{ Authorization = "Bearer $Token" }).value
            $found = $items | Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1
            if ($found) { return $found.id }
        }
    }
    catch {
        $errBody = ""
        try {
            $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $errBody = $sr.ReadToEnd(); $sr.Close()
        } catch {}
        $errMsg = "$($_.Exception.Message) $errBody"

        if ($errMsg -like "*ItemDisplayNameAlreadyInUse*" -or $errMsg -like "*already in use*") {
            Write-Info "'$DisplayName' already exists - reusing"
            $items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WsId/items?type=$Type" `
                -Headers @{ Authorization = "Bearer $Token" }).value
            $found = $items | Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1
            if ($found) { return $found.id }
        }
        else { throw "Failed to create $Type '${DisplayName}': $errMsg" }
    }
    return $null
}

function Update-FabricItemDefinition {
    param(
        [string]$ItemId,
        [string]$WsId,
        [string]$DefinitionJson,
        [string]$Token
    )

    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }

    for ($attempt = 1; $attempt -le 3; $attempt++) {
        if ($attempt -gt 1) {
            Write-Info "Definition update retry $attempt/3 - waiting 10s..."
            Start-Sleep -Seconds 10
            $Token = Get-FabricToken
            $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
        }
        try {
            $resp = Invoke-WebRequest -Method Post `
                -Uri "$FabricApiBase/workspaces/$WsId/items/$ItemId/updateDefinition" `
                -Headers $headers -Body $DefinitionJson -UseBasicParsing

            if ($resp.StatusCode -eq 200) { return $true }
            if ($resp.StatusCode -eq 202) {
                $opUrl = $resp.Headers["Location"]
                if ($opUrl) {
                    for ($p = 1; $p -le 24; $p++) {
                        Start-Sleep -Seconds 5
                        $poll = Invoke-RestMethod -Uri $opUrl -Headers @{ Authorization = "Bearer $Token" }
                        Write-Info "  Definition LRO: $($poll.status) ($($p*5)s)"
                        if ($poll.status -eq "Succeeded") { return $true }
                        if ($poll.status -eq "Failed") { Write-Warn "Definition LRO failed"; return $false }
                    }
                }
            }
        }
        catch {
            Write-Warn "Definition update error (attempt $attempt): $($_.Exception.Message)"
        }
    }
    return $false
}

# ============================================================================
#                           MAIN DEPLOYMENT
# ============================================================================

Write-Banner "Horizon Books - Power BI Deployment"
Write-Host ""
Write-Host "  Workspace      : $WorkspaceId" -ForegroundColor White
Write-Host "  Gold Lakehouse : $GoldLakehouseName" -ForegroundColor White
Write-Host "  Semantic Model : $SemanticModelName" -ForegroundColor White
Write-Host "  Report         : $(if ($SkipReport) { 'Skipped' } else { $ReportName })" -ForegroundColor White
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
        $b64 = [Convert]::ToBase64String($bytes)
        $smParts += '{"path":"definition/tables/' + $f.Name + '","payload":"' + $b64 + '","payloadType":"InlineBase64"}'
    }
}

Write-Info "Total TMDL parts: $($smParts.Count)"

$smDesc    = "Direct Lake semantic model on GoldLH - 23 tables (dim/fact/analytics schemas), 27 relationships, 96 DAX measures, 5 forecast tables"
$partsJson = $smParts -join ","
$createSmJson = '{"displayName":"' + $SemanticModelName + '","type":"SemanticModel","description":"' + $smDesc + '","definition":{"parts":[' + $partsJson + ']}}'

$semanticModelId = $null

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
                        Write-Warn "SM operation failed"
                        break
                    }
                }
                catch { Write-Warn "SM poll error: $($_.Exception.Message)"; break }
            }
        }
        else { Start-Sleep -Seconds 15 }

        # Look up created model
        $fabricToken = Get-FabricToken
        $smItems = Invoke-FabricApi -Method Get `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=SemanticModel" -Token $fabricToken
        $sm = $smItems.value | Where-Object { $_.displayName -eq $SemanticModelName } | Select-Object -First 1
        $semanticModelId = $sm.id
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
# Step 2: Deploy Power BI Report (PBIR)
# ------------------------------------------------------------------
if (-not $SkipReport) {
    Write-Step "2/2" "Deploying Power BI Report '$ReportName' (PBIR)"

    $reportRoot   = Join-Path $projectRoot "HorizonBooksAnalytics\HorizonBooksAnalytics.Report"
    $reportDefDir = Join-Path $reportRoot "definition"

    if (-not (Test-Path $reportRoot)) {
        Write-Error "Report folder not found at $reportRoot"
        exit 1
    }

    $reportParts = @()

    # --- definition.pbir: rewrite byPath -> byConnection with SM ID ---
    $pbirPath = Join-Path $reportRoot "definition.pbir"
    if (Test-Path $pbirPath) {
        if ($semanticModelId) {
            # Build byConnection JSON referencing deployed semantic model
            $pbirObj = @{
                version = "4.0"
                datasetReference = @{
                    byPath = $null
                    byConnection = @{
                        connectionString          = $null
                        pbiServiceModelId         = $null
                        pbiModelVirtualServerName = "sobe_wowvirtualserver"
                        pbiModelDatabaseName      = $semanticModelId
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
        $reportParts += '{"path":"definition.pbir","payload":"' + $b64 + '","payloadType":"InlineBase64"}'
        Write-Info "Loaded definition.pbir (bound to SM $semanticModelId)"
    }

    # --- Recursively collect all files under definition/ ---
    if (Test-Path $reportDefDir) {
        $defFiles = Get-ChildItem -Path $reportDefDir -Recurse -File | Sort-Object FullName
        foreach ($f in $defFiles) {
            $relPath = $f.FullName.Substring($reportRoot.Length + 1).Replace('\', '/')
            $bytes   = [IO.File]::ReadAllBytes($f.FullName)
            $b64     = [Convert]::ToBase64String($bytes)
            $reportParts += '{"path":"' + $relPath + '","payload":"' + $b64 + '","payloadType":"InlineBase64"}'
        }
        Write-Info "Loaded $($defFiles.Count) report definition files"
    }

    Write-Info "Total report parts: $($reportParts.Count)"

    $reportDesc = "Horizon Books Analytics - 10-page Power BI report (PBIR) bound to $SemanticModelName"
    $partsJson  = $reportParts -join ","
    $createReportJson = '{"displayName":"' + $ReportName + '","type":"Report","description":"' + $reportDesc + '","definition":{"parts":[' + $partsJson + ']}}'

    $reportId = $null

    try {
        $fabricToken = Get-FabricToken
        $rptHeaders  = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }

        $rptResponse = Invoke-WebRequest -Method Post `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
            -Headers $rptHeaders -Body $createReportJson -UseBasicParsing

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
                            Write-Warn "Report operation failed"
                            break
                        }
                    }
                    catch { Write-Warn "Report poll error: $($_.Exception.Message)"; break }
                }
            }
            else { Start-Sleep -Seconds 15 }

            # Look up created report
            $fabricToken = Get-FabricToken
            $rptItems = Invoke-FabricApi -Method Get `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Report" -Token $fabricToken
            $rpt = $rptItems.value | Where-Object { $_.displayName -eq $ReportName } | Select-Object -First 1
            $reportId = $rpt.id
        }
        else {
            $rpt = $rptResponse.Content | ConvertFrom-Json
            $reportId = $rpt.id
        }
        Write-Success "Report deployed: $reportId"
    }
    catch {
        $errMsg = $_.Exception.Message
        if ($errMsg -like "*ItemDisplayNameAlreadyInUse*") {
            Write-Info "Report '$ReportName' already exists - updating definition."
            $fabricToken = Get-FabricToken
            $rptItems = Invoke-FabricApi -Method Get `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Report" -Token $fabricToken
            $rpt = $rptItems.value | Where-Object { $_.displayName -eq $ReportName } | Select-Object -First 1
            $reportId = $rpt.id

            if ($reportId) {
                $updateJson = '{"definition":{"parts":[' + $partsJson + ']}}'
                $fabricToken = Get-FabricToken
                $updated = Update-FabricItemDefinition -ItemId $reportId `
                    -WsId $WorkspaceId -DefinitionJson $updateJson -Token $fabricToken
                if ($updated) {
                    Write-Success "Report definition updated: $reportId"
                }
                else {
                    Write-Warn "Report definition update may have failed - check portal"
                }
            }
        }
        else { Write-Warn "Report deployment issue: $errMsg" }
    }
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
    Write-Host "  Report         : $ReportName ($reportId)" -ForegroundColor White
}
Write-Host ""
Write-Host "  Fabric Portal  : https://app.fabric.microsoft.com/groups/$WorkspaceId" -ForegroundColor Cyan
Write-Host ""
