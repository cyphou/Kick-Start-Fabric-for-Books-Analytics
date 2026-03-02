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
    [switch]$SkipReport,

    [Parameter(Mandatory = $false)]
    [string]$SemanticModelFolderName = "Semantic Model",

    [Parameter(Mandatory = $false)]
    [string]$ReportFolderName = "Report",

    [Parameter(Mandatory = $false)]
    [string]$ParentFolderName = "05 - Analytics"
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
                        if ($poll.status -eq "Failed") {
                            $errDetail = $poll | ConvertTo-Json -Depth 10 -Compress
                            Write-Warn "Definition LRO failed: $errDetail"
                            # Also try to get the result endpoint
                            try {
                                $resultUrl = "$opUrl/result"
                                $resultData = Invoke-RestMethod -Uri $resultUrl -Headers @{ Authorization = "Bearer $Token" }
                                Write-Warn "LRO Result: $($resultData | ConvertTo-Json -Depth 10 -Compress)"
                            } catch {
                                Write-Warn "Could not retrieve LRO result: $($_.Exception.Message)"
                            }
                            return $false
                        }
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
Write-Host "  Semantic Model : $SemanticModelName (folder: $ParentFolderName/$SemanticModelFolderName)" -ForegroundColor White
Write-Host "  Report         : $(if ($SkipReport) { 'Skipped' } else { "$ReportName (folder: $ParentFolderName/$ReportFolderName)" })" -ForegroundColor White
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
        $reportParts += '{"path":"definition.pbir","payload":"' + $b64 + '","payloadType":"InlineBase64"}'
        Write-Info "Loaded definition.pbir (bound to SM $semanticModelId)"
    }

    # --- Recursively collect all files under definition/ ---
    if (Test-Path $reportDefDir) {
        $defFiles = Get-ChildItem -Path $reportDefDir -Recurse -File | Sort-Object FullName
        foreach ($f in $defFiles) {
            $relPath = $f.FullName.Substring($reportRoot.Length + 1).Replace('\', '/')
            # PBIR format: RegisteredResources must be under StaticResources/
            if ($relPath -like "definition/RegisteredResources/*") {
                $relPath = $relPath -replace '^definition/RegisteredResources/', 'StaticResources/RegisteredResources/'
            }
            $bytes   = [IO.File]::ReadAllBytes($f.FullName)
            $b64     = [Convert]::ToBase64String($bytes)
            $reportParts += '{"path":"' + $relPath + '","payload":"' + $b64 + '","payloadType":"InlineBase64"}'
        }
        Write-Info "Loaded $($defFiles.Count) report definition files"
    }

    Write-Info "Total report parts: $($reportParts.Count)"

    $reportDesc = "Horizon Books Analytics - 10-page Power BI report (PBIR) bound to $SemanticModelName"
    $partsJson  = $reportParts -join ","
    $createReportJson = '{"displayName":"' + $ReportName + '","type":"Report","description":"' + $reportDesc + '","folderId":"' + $rptFolderId + '","definition":{"parts":[' + $partsJson + ']}}'

    $reportId = $null
    $rptLroFailed = $false

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
                            $errDetail = $pollData | ConvertTo-Json -Depth 10 -Compress
                            Write-Warn "Report creation LRO failed: $errDetail"
                            Write-Warn "Will try updating existing item"
                            $rptLroFailed = $true
                            break
                        }
                    }
                    catch { Write-Warn "Report poll error: $($_.Exception.Message)"; $rptLroFailed = $true; break }
                }
            }
            else { Start-Sleep -Seconds 15 }

            # Look up created/existing report
            $fabricToken = Get-FabricToken
            $rptItems = Invoke-FabricApi -Method Get `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Report" -Token $fabricToken
            $rpt = $rptItems.value | Where-Object { $_.displayName -eq $ReportName } | Select-Object -First 1
            $reportId = $rpt.id

            # Always update definition when item already existed
            if ($reportId) {
                Write-Info "Updating report definition to ensure latest PBIR is applied..."
                $updateJson = '{"definition":{"parts":[' + $partsJson + ']}}'
                $fabricToken = Get-FabricToken
                $updated = Update-FabricItemDefinition -ItemId $reportId `
                    -WsId $WorkspaceId -DefinitionJson $updateJson -Token $fabricToken
                if ($updated) {
                    Write-Success "Report definition updated: $reportId"
                }
                else {
                    Write-Warn "Report definition update may have failed"
                }
            }
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
