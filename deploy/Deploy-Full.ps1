<#
.SYNOPSIS
    Fully automated end-to-end deployment of the Horizon Books demo to Microsoft Fabric.

.DESCRIPTION
    One-command deployment that creates, configures, and executes everything
    using a 3-Lakehouse Medallion architecture with Lakehouse schemas:

    Phase 1 - Provision:
      Step 1: Create 3 schema-enabled Lakehouses (BronzeLH, SilverLH, GoldLH)
              and wait for GoldLH SQL endpoint (needed for Direct Lake)
      Step 2: Upload 17 CSV files to BronzeLH Files/ via OneLake DFS API
      Step 3: Deploy 4 Spark Notebooks (each bound to its default Lakehouse)

    Phase 2 - Execute:
      Step 4: Run Notebook 01 (Bronze-to-Silver): schema, quality, dedup, transforms
      Step 5: Deploy 3 Dataflows Gen2 + 1 Data Pipeline
      Step 6: Run the orchestration pipeline:
              DF_Finance + DF_HR + DF_Operations (parallel) then NB01 BronzeToSilver
              then NB02 WebEnrichment then NB03 SilverToGold then NB04 Forecasting
      Step 7: Execute Lakehouse SQL scripts (CreateTables.sql + GenerateDateDimension.sql)

    Phase 3 - Model, Report and AI:
      Step 8: Deploy Semantic Model (Direct Lake on GoldLH, 27 relationships, 79 measures)
      Step 9: Deploy Power BI Report (PBIR, 10 pages bound to Semantic Model)
      Step 10: Deploy Data Agent (requires F64+ capacity, skipped on trial)

    Phase 4 - Validate:
      Step 11: Run deployment validation checks

    Medallion Lakehouse layout:
      BronzeLH - Raw CSV files in Files/ (ingested by Dataflows Gen2)
      SilverLH - Schemas: finance, hr, operations, web (cleaned Delta tables)
      GoldLH   - Schemas: dim, fact, analytics (star schema + analytics)

    The script is fully idempotent - re-running it will reuse existing items.

.PARAMETER WorkspaceId
    The GUID of an existing Fabric workspace to deploy into.

.PARAMETER BronzeLakehouseName
    Name for the Bronze-layer Lakehouse. Defaults to BronzeLH.

.PARAMETER SilverLakehouseName
    Name for the Silver-layer Lakehouse. Defaults to SilverLH.

.PARAMETER GoldLakehouseName
    Name for the Gold-layer Lakehouse. Defaults to GoldLH.

.PARAMETER SemanticModelName
    Name for the Semantic Model. Defaults to HorizonBooksModel.

.PARAMETER SkipPipelineRun
    If set, creates the pipeline but does not trigger it (deploy-only mode).

.PARAMETER SkipDataAgent
    If set, skips Data Agent deployment (useful on Trial capacity).

.PARAMETER SkipValidation
    If set, skips the post-deployment validation step.

.EXAMPLE
    # Full deployment to an existing workspace
    .\deploy\Deploy-Full.ps1 -WorkspaceId "your-workspace-guid"

.EXAMPLE
    # Deploy without running the pipeline (manual trigger later)
    .\deploy\Deploy-Full.ps1 -WorkspaceId "guid" -SkipPipelineRun

.EXAMPLE
    # Create workspace first, then deploy
    $ws = .\deploy\New-HorizonBooksWorkspace.ps1 -CapacityId "cap-guid"
    .\deploy\Deploy-Full.ps1 -WorkspaceId $ws.WorkspaceId
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,

    [Parameter(Mandatory = $false)]
    [string]$BronzeLakehouseName = "BronzeLH",

    [Parameter(Mandatory = $false)]
    [string]$SilverLakehouseName = "SilverLH",

    [Parameter(Mandatory = $false)]
    [string]$GoldLakehouseName = "GoldLH",

    [Parameter(Mandatory = $false)]
    [string]$SemanticModelName = "HorizonBooksModel",

    [Parameter(Mandatory = $false)]
    [string]$ReportName = "HorizonBooksAnalytics",

    [Parameter(Mandatory = $false)]
    [switch]$SkipPipelineRun,

    [Parameter(Mandatory = $false)]
    [switch]$SkipReport,

    [Parameter(Mandatory = $false)]
    [switch]$SkipDataAgent,

    [Parameter(Mandatory = $false)]
    [switch]$SkipValidation
)

$ErrorActionPreference = "Stop"
$deployStart = Get-Date

# Resolve paths
$scriptDir   = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$projectRoot = Split-Path -Parent $scriptDir
$dataFolder  = Join-Path $projectRoot "SampleData"

# API endpoints
$FabricApiBase = "https://api.fabric.microsoft.com/v1"
$OneLakeBase   = "https://onelake.dfs.fabric.microsoft.com"

# Step timing tracker
$stepTimings = [System.Collections.ArrayList]::new()

# ============================================================================
# DISPLAY HELPERS
# ============================================================================

function Write-Banner {
    param([string]$Title, [ConsoleColor]$Color = "Yellow")
    Write-Host ""
    Write-Host ("=" * 70) -ForegroundColor $Color
    Write-Host "  $Title" -ForegroundColor $Color
    Write-Host ("=" * 70) -ForegroundColor $Color
}

function Write-Step {
    param([string]$StepNum, [string]$Message)
    Write-Host ""
    Write-Host "  [$StepNum] $Message" -ForegroundColor Cyan
    Write-Host ("  " + "-" * 60) -ForegroundColor DarkGray
}

function Write-Info    { param([string]$M) Write-Host "      [INFO] $M" -ForegroundColor Gray }
function Write-Success { param([string]$M) Write-Host "      [ OK ] $M" -ForegroundColor Green }
function Write-Warn    { param([string]$M) Write-Host "      [WARN] $M" -ForegroundColor Yellow }
function Write-Err     { param([string]$M) Write-Host "      [FAIL] $M" -ForegroundColor Red }

function Measure-Step {
    param([string]$Name, [scriptblock]$Block)
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        & $Block
        $sw.Stop()
        $null = $stepTimings.Add([PSCustomObject]@{
            Step     = $Name
            Duration = $sw.Elapsed
            Status   = "OK"
        })
    }
    catch {
        $sw.Stop()
        $null = $stepTimings.Add([PSCustomObject]@{
            Step     = $Name
            Duration = $sw.Elapsed
            Status   = "FAILED"
        })
        throw
    }
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

function Get-StorageToken {
    try {
        $token = Get-AzAccessToken -ResourceTypeName Storage
        return $token.Token
    }
    catch {
        Write-Error "Failed to get Storage token. Run 'Connect-AzAccount' first."
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
            $statusCode = $webResponse.StatusCode

            # 202 Accepted - Long Running Operation
            if ($statusCode -eq 202) {
                $locationHeader = $webResponse.Headers["Location"]
                $opIdHeader     = $webResponse.Headers["x-ms-operation-id"]
                $operationUrl   = $null
                if ($locationHeader)  { $operationUrl = $locationHeader }
                elseif ($opIdHeader)  { $operationUrl = "$FabricApiBase/operations/$opIdHeader" }

                if ($operationUrl) {
                    Write-Info "Waiting for long-running operation..."
                    return Wait-FabricOperation -OperationUrl $operationUrl -Token $Token
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

            $isRetriable = $errorBody -like "*isRetriable*true*" -or $errorBody -like "*NotAvailableYet*"

            if ($statusCode -eq 429 -or $isRetriable) {
                $retryAfter = if ($isRetriable) { 15 } else { 30 }
                try {
                    $ra = $ex.Response.Headers | Where-Object { $_.Key -eq "Retry-After" } |
                        Select-Object -ExpandProperty Value -First 1
                    if ($ra) { $retryAfter = [int]$ra }
                } catch {}
                $reason = if ($isRetriable) { "Retriable error" } else { "Rate limited (429)" }
                Write-Warn "$reason - retrying in $($retryAfter)s (attempt $attempt/$MaxRetries)"
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

function Wait-FabricOperation {
    param(
        [string]$OperationUrl,
        [string]$Token,
        [int]$TimeoutSeconds = 600,
        [int]$PollIntervalSeconds = 10
    )

    $headers = @{ "Authorization" = "Bearer $Token" }
    $elapsed = 0

    while ($elapsed -lt $TimeoutSeconds) {
        Start-Sleep -Seconds $PollIntervalSeconds
        $elapsed += $PollIntervalSeconds
        try {
            $status = Invoke-RestMethod -Method Get -Uri $OperationUrl -Headers $headers
            $state  = $status.status
            Write-Info "  Operation: $state ($($elapsed)s)"

            if ($state -eq "Succeeded") { return $status }
            if ($state -eq "Failed") {
                Write-Err "Operation failed: $($status | ConvertTo-Json -Depth 5)"
                throw "Fabric operation failed"
            }
        }
        catch {
            if ($_.Exception.Response -and $_.Exception.Response.StatusCode -eq 429) {
                Write-Warn "Rate limited while polling - waiting 30s..."
                Start-Sleep -Seconds 30
            }
            else { throw }
        }
    }
    throw "Operation timed out after $($TimeoutSeconds)s"
}

function Upload-FileToOneLake {
    param(
        [string]$LocalFilePath,
        [string]$OneLakePath,
        [string]$Token
    )

    $fileBytes = [System.IO.File]::ReadAllBytes($LocalFilePath)
    $fileName  = [System.IO.Path]::GetFileName($LocalFilePath)

    # Create file
    Invoke-RestMethod -Method Put `
        -Uri "${OneLakePath}/${fileName}?resource=file" `
        -Headers @{ "Authorization" = "Bearer $Token"; "Content-Length" = "0" } | Out-Null

    # Append data
    Invoke-RestMethod -Method Patch `
        -Uri "${OneLakePath}/${fileName}?action=append&position=0" `
        -Headers @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/octet-stream"; "Content-Length" = $fileBytes.Length.ToString() } `
        -Body $fileBytes | Out-Null

    # Flush
    Invoke-RestMethod -Method Patch `
        -Uri "${OneLakePath}/${fileName}?action=flush&position=$($fileBytes.Length)" `
        -Headers @{ "Authorization" = "Bearer $Token"; "Content-Length" = "0" } | Out-Null
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

function Run-FabricNotebook {
    <#
    .SYNOPSIS
        Triggers a notebook job and waits for completion.
    #>
    param(
        [string]$NotebookId,
        [string]$NotebookName,
        [string]$WsId,
        [string]$Token,
        [int]$TimeoutMinutes = 15
    )

    Write-Info "Starting $NotebookName (Spark session may take a few minutes)..."

    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
    $jobLoc  = $null

    for ($runAttempt = 1; $runAttempt -le 3; $runAttempt++) {
        if ($runAttempt -gt 1) {
            Write-Info "Run retry $runAttempt/3 - waiting 30s..."
            Start-Sleep -Seconds 30
            $Token   = Get-FabricToken
            $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
        }
        try {
            $runResp = Invoke-WebRequest -Method Post `
                -Uri "$FabricApiBase/workspaces/$WsId/items/$NotebookId/jobs/instances?jobType=RunNotebook" `
                -Headers $headers -UseBasicParsing

            if ($runResp.StatusCode -eq 202) {
                $jobLoc = $runResp.Headers["Location"]
                break
            }
        }
        catch {
            Write-Warn "Run error (attempt $runAttempt): $($_.Exception.Message)"
        }
    }

    if (-not $jobLoc) {
        Write-Warn "${NotebookName}: Could not start notebook job"
        return $false
    }

    # Poll for completion
    $maxSeconds = $TimeoutMinutes * 60
    $waited     = 0
    while ($waited -lt $maxSeconds) {
        Start-Sleep -Seconds 15
        $waited += 15
        try {
            $jobStat = Invoke-RestMethod -Uri $jobLoc -Headers @{ Authorization = "Bearer $Token" }
            Write-Info "  $NotebookName status: $($jobStat.status) ($($waited)s)"
            if ($jobStat.status -eq "Completed") {
                Write-Success "$NotebookName completed"
                return $true
            }
            if ($jobStat.status -eq "Failed" -or $jobStat.status -eq "Cancelled") {
                $reason = ""
                if ($jobStat.failureReason) { $reason = $jobStat.failureReason.message }
                Write-Err "$NotebookName $($jobStat.status): $reason"
                return $false
            }
        }
        catch {
            if ($_.Exception.Response -and [int]$_.Exception.Response.StatusCode -eq 404) {
                Write-Info "  Job not ready yet ($($waited)s)"
            }
            else { Write-Warn "  Poll error: $($_.Exception.Message)" }
        }
    }

    Write-Warn "${NotebookName}: timed out after $TimeoutMinutes minutes"
    return $false
}

function Run-FabricPipeline {
    <#
    .SYNOPSIS
        Triggers a Data Pipeline run and waits for completion.
    #>
    param(
        [string]$PipelineId,
        [string]$PipelineName,
        [string]$WsId,
        [string]$Token,
        [int]$TimeoutMinutes = 30
    )

    Write-Info "Starting pipeline $PipelineName ..."

    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
    $jobLoc  = $null

    for ($runAttempt = 1; $runAttempt -le 3; $runAttempt++) {
        if ($runAttempt -gt 1) {
            Write-Info "Pipeline run retry $runAttempt/3 - waiting 30s..."
            Start-Sleep -Seconds 30
            $Token   = Get-FabricToken
            $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
        }
        try {
            $runResp = Invoke-WebRequest -Method Post `
                -Uri "$FabricApiBase/workspaces/$WsId/items/$PipelineId/jobs/instances?jobType=Pipeline" `
                -Headers $headers -UseBasicParsing

            if ($runResp.StatusCode -eq 202) {
                $jobLoc = $runResp.Headers["Location"]
                break
            }
        }
        catch {
            Write-Warn "Pipeline run error (attempt $runAttempt): $($_.Exception.Message)"
        }
    }

    if (-not $jobLoc) {
        Write-Warn "Could not start pipeline run"
        return $false
    }

    # Poll for completion
    $maxSeconds = $TimeoutMinutes * 60
    $waited     = 0
    while ($waited -lt $maxSeconds) {
        Start-Sleep -Seconds 20
        $waited += 20
        try {
            $jobStat = Invoke-RestMethod -Uri $jobLoc -Headers @{ Authorization = "Bearer $Token" }
            Write-Info "  Pipeline status: $($jobStat.status) ($($waited)s)"
            if ($jobStat.status -eq "Completed") {
                Write-Success "Pipeline $PipelineName completed"
                return $true
            }
            if ($jobStat.status -eq "Failed" -or $jobStat.status -eq "Cancelled") {
                $reason = ""
                if ($jobStat.failureReason) { $reason = $jobStat.failureReason.message }
                Write-Err "Pipeline $($jobStat.status): $reason"
                return $false
            }
        }
        catch {
            if ($_.Exception.Response -and [int]$_.Exception.Response.StatusCode -eq 404) {
                Write-Info "  Pipeline job not ready yet ($($waited)s)"
            }
            else { Write-Warn "  Poll error: $($_.Exception.Message)" }
        }
    }

    Write-Warn "Pipeline timed out after $TimeoutMinutes minutes"
    return $false
}

# ============================================================================
# CSV FILE MANIFEST
# ============================================================================
$LakehouseFiles = @(
    @{ Folder = "Finance";    Files = @("DimAccounts.csv", "DimCostCenters.csv", "FactFinancialTransactions.csv", "FactBudget.csv") },
    @{ Folder = "HR";         Files = @("DimEmployees.csv", "DimDepartments.csv", "FactPayroll.csv", "FactPerformanceReviews.csv", "FactRecruitment.csv") },
    @{ Folder = "Operations"; Files = @("DimBooks.csv", "DimAuthors.csv", "DimCustomers.csv", "DimGeography.csv", "DimWarehouses.csv", "FactOrders.csv", "FactInventory.csv", "FactReturns.csv") }
)

# ============================================================================
# NOTEBOOK MANIFEST
# ============================================================================
$Notebooks = @(
    @{
        Name        = "HorizonBooks_01_BronzeToSilver"
        FileName    = "01_BronzeToSilver.py"
        Description = "Ingests CSV files with schema enforcement, data quality checks, deduplication, and dimension/fact-specific transformations (Bronze to Silver)"
    },
    @{
        Name        = "HorizonBooks_02_WebEnrichment"
        FileName    = "02_WebEnrichment.py"
        Description = "Fetches data from public web APIs (exchange rates, holidays, country indicators, book metadata) and enriches Silver tables"
    },
    @{
        Name        = "HorizonBooks_03_SilverToGold"
        FileName    = "03_SilverToGold.py"
        Description = "Generates DimDate, RFM segmentation, cohort analysis, anomaly detection, co-purchasing patterns, and revenue forecasting (Silver to Gold)"
    },
    @{
        Name        = "HorizonBooks_04_Forecasting"
        FileName    = "04_Forecasting.py"
        Description = "Builds Holt-Winters time-series forecasts on Gold data: sales revenue, genre demand, financial P&L, inventory demand, and workforce planning"
    }
)

# ============================================================================
#                           MAIN DEPLOYMENT
# ============================================================================

Write-Banner "Horizon Books - Fully Automated Fabric Deployment"
Write-Host ""
Write-Host "  Workspace       : $WorkspaceId" -ForegroundColor White
Write-Host "  Bronze Lakehouse: $BronzeLakehouseName" -ForegroundColor White
Write-Host "  Silver Lakehouse: $SilverLakehouseName" -ForegroundColor White
Write-Host "  Gold Lakehouse  : $GoldLakehouseName" -ForegroundColor White
Write-Host "  Semantic Model  : $SemanticModelName" -ForegroundColor White
Write-Host "  Data Folder     : $dataFolder" -ForegroundColor White
Write-Host "  Pipeline Run    : $(if ($SkipPipelineRun) { 'Skipped' } else { 'Yes (auto)' })" -ForegroundColor White
Write-Host "  Data Agent      : $(if ($SkipDataAgent) { 'Skipped' } else { 'Yes' })" -ForegroundColor White
Write-Host "  Validation      : $(if ($SkipValidation) { 'Skipped' } else { 'Yes' })" -ForegroundColor White
Write-Host ""

# Validate prerequisites
if (-not (Test-Path $dataFolder)) {
    Write-Error "Data folder not found: $dataFolder"
    exit 1
}

# ------------------------------------------------------------------
# Step 0: Authenticate
# ------------------------------------------------------------------
Write-Step "0/12" "Authenticating to Azure / Fabric"

$account = Get-AzContext
if (-not $account) {
    Write-Info "No active session - launching interactive login..."
    Connect-AzAccount | Out-Null
}
else {
    Write-Info "Session: $($account.Account.Id)"
}

$fabricToken  = Get-FabricToken
$storageToken = Get-StorageToken
Write-Success "Authenticated"

# ------------------------------------------------------------------
# Step 1: Create 3 Lakehouses (Bronze, Silver, Gold)
# ------------------------------------------------------------------
Measure-Step "1. Create Lakehouses" {
    Write-Step "1/12" "Creating 3 Medallion Lakehouses (schema-enabled)"

    $script:bronzeLakehouseId = $null
    $script:silverLakehouseId = $null
    $script:goldLakehouseId   = $null

    # Helper to create a schema-enabled lakehouse
    function New-SchemaLakehouse {
        param([string]$Name, [string]$Desc, [string]$Tkn)

        # REST API body with enableSchemas
        $lhBodyJson = '{"displayName":"' + $Name + '","type":"Lakehouse","description":"' + $Desc + '","creationPayload":{"enableSchemas":true}}'

        try {
            $resp = Invoke-WebRequest -Method Post `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
                -Headers @{ "Authorization" = "Bearer $Tkn"; "Content-Type" = "application/json" } `
                -Body $lhBodyJson -UseBasicParsing

            if ($resp.StatusCode -eq 201) {
                $lhObj = $resp.Content | ConvertFrom-Json
                return $lhObj.id
            }
            elseif ($resp.StatusCode -eq 202) {
                $opUrl = $resp.Headers["Location"]
                if ($opUrl) {
                    for ($p = 1; $p -le 24; $p++) {
                        Start-Sleep -Seconds 5
                        $poll = Invoke-RestMethod -Uri $opUrl -Headers @{ Authorization = "Bearer $Tkn" }
                        Write-Info "  LRO: $($poll.status) ($($p*5)s)"
                        if ($poll.status -eq "Succeeded") { break }
                        if ($poll.status -eq "Failed") { Write-Warn "LRO failed"; break }
                    }
                }
                Start-Sleep -Seconds 3
                $items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Lakehouse" `
                    -Headers @{ Authorization = "Bearer $Tkn" }).value
                $found = $items | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
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
                Write-Info "'$Name' already exists - reusing"
                $items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Lakehouse" `
                    -Headers @{ Authorization = "Bearer $Tkn" }).value
                $found = $items | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
                if ($found) { return $found.id }
            }
            else { throw "Failed to create Lakehouse '${Name}': $errMsg" }
        }
        return $null
    }

    # Create BronzeLH
    Write-Info "Creating BronzeLH (raw CSV files)..."
    $fabricToken = Get-FabricToken
    $script:bronzeLakehouseId = New-SchemaLakehouse `
        -Name $BronzeLakehouseName `
        -Desc "Horizon Books Bronze layer - raw CSV files for Finance, HR, Operations" `
        -Tkn $fabricToken
    Write-Success "BronzeLH: $($script:bronzeLakehouseId)"

    Start-Sleep -Seconds 5

    # Create SilverLH
    Write-Info "Creating SilverLH (cleaned Delta tables)..."
    $fabricToken = Get-FabricToken
    $script:silverLakehouseId = New-SchemaLakehouse `
        -Name $SilverLakehouseName `
        -Desc "Horizon Books Silver layer - schemas: finance, hr, operations, web" `
        -Tkn $fabricToken
    Write-Success "SilverLH: $($script:silverLakehouseId)"

    Start-Sleep -Seconds 5

    # Create GoldLH
    Write-Info "Creating GoldLH (star schema + analytics)..."
    $fabricToken = Get-FabricToken
    $script:goldLakehouseId = New-SchemaLakehouse `
        -Name $GoldLakehouseName `
        -Desc "Horizon Books Gold layer - schemas: dim, fact, analytics" `
        -Tkn $fabricToken
    Write-Success "GoldLH: $($script:goldLakehouseId)"

    # Wait for GoldLH SQL endpoint (needed for Direct Lake semantic model)
    $script:sqlEndpoint = ""
    $maxWait = 180; $waited = 0
    Write-Info "Waiting for GoldLH SQL endpoint..."
    while ($waited -lt $maxWait) {
        Start-Sleep -Seconds 15; $waited += 15
        try {
            $fabricToken = Get-FabricToken
            $lhProps = Invoke-FabricApi -Method Get `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses/$($script:goldLakehouseId)" `
                -Token $fabricToken
            if ($lhProps.properties.sqlEndpointProperties.connectionString) {
                $script:sqlEndpoint = $lhProps.properties.sqlEndpointProperties.connectionString
                Write-Success "GoldLH SQL endpoint ready: $($script:sqlEndpoint) ($($waited)s)"
                break
            }
            Write-Info "  SQL endpoint not ready ($($waited)s)..."
        }
        catch { Write-Info "  Waiting ($($waited)s)..." }
    }
    if (-not $script:sqlEndpoint) {
        Write-Warn "GoldLH SQL endpoint not available after $($maxWait)s - semantic model may need manual config."
    }
}

# ------------------------------------------------------------------
# Step 2: Upload CSV Files
# ------------------------------------------------------------------
Measure-Step "2. Upload CSVs" {
    Write-Step "2/12" "Uploading 17 CSV files to BronzeLH Files/"

    $oneLakeFiles = "$OneLakeBase/$WorkspaceId/$($script:bronzeLakehouseId)/Files"
    $storageToken = Get-StorageToken

    # Create Files directory
    try {
        Invoke-RestMethod -Method Put -Uri "${oneLakeFiles}?resource=directory" `
            -Headers @{ "Authorization" = "Bearer $storageToken"; "Content-Length" = "0" } | Out-Null
    } catch {}

    $uploaded = 0; $total = 0
    foreach ($group in $LakehouseFiles) {
        foreach ($fileName in $group.Files) {
            $total++
            $localPath = Join-Path (Join-Path $dataFolder $group.Folder) $fileName
            if (-not (Test-Path $localPath)) {
                Write-Warn "File not found: $($group.Folder)/$fileName"
                continue
            }
            Write-Info "Uploading $($group.Folder)/$fileName ..."
            $storageToken = Get-StorageToken
            Upload-FileToOneLake -LocalFilePath $localPath -OneLakePath $oneLakeFiles -Token $storageToken
            $uploaded++
        }
    }
    Write-Success "Uploaded $uploaded / $total CSV files"
}

# ------------------------------------------------------------------
# Step 3: Deploy Notebooks (create items + upload definitions)
# ------------------------------------------------------------------
Measure-Step "3. Deploy Notebooks" {
    Write-Step "3/12" "Deploying 4 Spark Notebooks (per-Lakehouse binding)"

    $notebooksDir = Join-Path $projectRoot "notebooks"

    # Map each notebook to its default lakehouse
    $notebookLhMap = @{
        "HorizonBooks_01_BronzeToSilver" = @{ Id = $script:bronzeLakehouseId; Name = $BronzeLakehouseName }
        "HorizonBooks_02_WebEnrichment"  = @{ Id = $script:silverLakehouseId; Name = $SilverLakehouseName }
        "HorizonBooks_03_SilverToGold"   = @{ Id = $script:goldLakehouseId;   Name = $GoldLakehouseName }
        "HorizonBooks_04_Forecasting"    = @{ Id = $script:goldLakehouseId;   Name = $GoldLakehouseName }
    }

    $script:notebookIds = @{}

    foreach ($nb in $Notebooks) {
        $nbFilePath = Join-Path $notebooksDir $nb.FileName
        if (-not (Test-Path $nbFilePath)) {
            Write-Warn "Notebook file not found: $nbFilePath"
            continue
        }

        Write-Info "Deploying $($nb.Name)..."

        # Read and inject lakehouse binding (per-notebook default LH + all 3 as known)
        $rawContent = Get-Content -Path $nbFilePath -Raw -Encoding UTF8
        $nbLh   = $notebookLhMap[$nb.Name]
        $defLhId   = $nbLh.Id
        $defLhName = $nbLh.Name
        $lakehouseMeta = @"
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "$defLhId",
# META       "default_lakehouse_name": "$defLhName",
# META       "default_lakehouse_workspace_id": "$WorkspaceId",
# META       "known_lakehouses": [
# META         {
# META           "id": "$($script:bronzeLakehouseId)"
# META         },
# META         {
# META           "id": "$($script:silverLakehouseId)"
# META         },
# META         {
# META           "id": "$($script:goldLakehouseId)"
# META         }
# META       ]
# META     }
# META   }
"@
        $depPattern = '# META\s+"dependencies":\s*\{[ \t]*\}'
        $rawContent = $rawContent -replace $depPattern, $lakehouseMeta.Trim()
        $rawContent = $rawContent -replace "`r`n", "`n"
        $nbBase64   = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($rawContent))

        # Create notebook item
        $fabricToken = Get-FabricToken
        $nbId = New-OrGetFabricItem `
            -DisplayName $nb.Name -Type "Notebook" -Description $nb.Description `
            -WsId $WorkspaceId -Token $fabricToken

        if (-not $nbId) {
            Write-Warn "Failed to create notebook '$($nb.Name)'"
            continue
        }

        $script:notebookIds[$nb.Name] = $nbId
        Write-Info "  Item created: $nbId"

        # Upload definition
        $defJson = '{"definition":{"parts":[{"path":"notebook-content.py","payload":"' + $nbBase64 + '","payloadType":"InlineBase64"}]}}'
        $fabricToken = Get-FabricToken
        $defOk = Update-FabricItemDefinition -ItemId $nbId -WsId $WorkspaceId -DefinitionJson $defJson -Token $fabricToken
        if ($defOk) {
            Write-Success "$($nb.Name) deployed"
        }
        else {
            Write-Warn "Definition upload failed for '$($nb.Name)'"
        }
    }

    Write-Success "$($script:notebookIds.Count) / $($Notebooks.Count) notebooks deployed"
}

# ------------------------------------------------------------------
# Step 4: Run Bronze-to-Silver Notebook
# ------------------------------------------------------------------
Measure-Step "4. Run BronzeToSilver" {
    Write-Step "4/12" "Running Notebook 01: Bronze to Silver"

    $nb01Id = $script:notebookIds["HorizonBooks_01_BronzeToSilver"]
    if (-not $nb01Id) {
        Write-Warn "Notebook 01 not available - skipping"
        return
    }

    Start-Sleep -Seconds 10  # brief pause after definition upload
    $fabricToken = Get-FabricToken
    $ok = Run-FabricNotebook -NotebookId $nb01Id -NotebookName "01_BronzeToSilver" `
        -WsId $WorkspaceId -Token $fabricToken -TimeoutMinutes 15

    if (-not $ok) {
        Write-Warn "Bronze-to-Silver notebook did not complete. Run manually from the Fabric portal."
    }
}

# ------------------------------------------------------------------
# Step 5: Deploy Dataflows + Pipeline
# ------------------------------------------------------------------
Measure-Step "5. Deploy Dataflows + Pipeline" {
    Write-Step "5/12" "Creating Dataflows Gen2 and Orchestration Pipeline"

    $pipelineScript = Join-Path $scriptDir "Deploy-Pipeline.ps1"
    if (-not (Test-Path $pipelineScript)) {
        Write-Warn "Deploy-Pipeline.ps1 not found - skipping"
        return
    }

    & $pipelineScript -WorkspaceId $WorkspaceId `
        -LakehouseId $script:bronzeLakehouseId `
        -LakehouseName $BronzeLakehouseName

    Write-Success "Dataflows + Pipeline deployed"
}

# ------------------------------------------------------------------
# Step 6: Run the Orchestration Pipeline
# ------------------------------------------------------------------
Measure-Step "6. Run Pipeline" {
    Write-Step "6/12" "Running Orchestration Pipeline (Dataflows -> WebEnrich -> Gold)"

    if ($SkipPipelineRun) {
        Write-Info "Pipeline run skipped (-SkipPipelineRun). Trigger manually from the Fabric portal."
        return
    }

    # Look up pipeline ID
    $fabricToken = Get-FabricToken
    $pipelineItems = (Invoke-RestMethod `
        -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=DataPipeline" `
        -Headers @{ Authorization = "Bearer $fabricToken" }).value

    $pipeline = $pipelineItems | Where-Object { $_.displayName -eq "PL_HorizonBooks_Orchestration" } | Select-Object -First 1

    if (-not $pipeline) {
        Write-Warn "Pipeline 'PL_HorizonBooks_Orchestration' not found - skipping run"
        return
    }

    $fabricToken = Get-FabricToken
    $ok = Run-FabricPipeline -PipelineId $pipeline.id `
        -PipelineName "PL_HorizonBooks_Orchestration" `
        -WsId $WorkspaceId -Token $fabricToken -TimeoutMinutes 30

    if (-not $ok) {
        Write-Warn "Pipeline did not complete. Check status in the Fabric portal."
    }
}

# ------------------------------------------------------------------
# Step 7: Execute Lakehouse SQL Scripts (Tables, Views & Date Dimension)
# ------------------------------------------------------------------
Measure-Step "7. Execute SQL Scripts" {
    Write-Step "7/12" "Executing Lakehouse SQL Scripts (Tables, Views & Date Dimension)"

    $sqlDir = Join-Path $projectRoot "Lakehouse"
    $createTablesSql = Join-Path $sqlDir "CreateTables.sql"
    $generateDateSql = Join-Path $sqlDir "GenerateDateDimension.sql"

    if (-not (Test-Path $sqlDir)) {
        Write-Warn "Lakehouse SQL folder not found at $sqlDir - skipping"
        return
    }

    # Collect SQL files to execute
    $sqlFiles = @()
    if (Test-Path $createTablesSql) { $sqlFiles += $createTablesSql }
    if (Test-Path $generateDateSql) { $sqlFiles += $generateDateSql }

    if ($sqlFiles.Count -eq 0) {
        Write-Warn "No SQL files found in $sqlDir - skipping"
        return
    }

    Write-Info "Found $($sqlFiles.Count) SQL file(s) to execute"

    # Base64-encode each SQL file for safe embedding in the notebook
    $sqlDictEntries = @()
    foreach ($sqlFile in $sqlFiles) {
        $fileName = [IO.Path]::GetFileName($sqlFile)
        $sqlBytes = [IO.File]::ReadAllBytes($sqlFile)
        $sqlB64   = [Convert]::ToBase64String($sqlBytes)
        $sqlDictEntries += "    `"$fileName`": `"$sqlB64`""
        Write-Info "  Loaded $fileName ($($sqlBytes.Length) bytes)"
    }
    $sqlDictBody = $sqlDictEntries -join ",`n"

    # Build Python notebook that decodes and executes each SQL file via Spark
    $pyContent = @"
# Fabric notebook source
# Horizon Books - Lakehouse Table & View Definitions
# Auto-generated by Deploy-Full.ps1

import base64
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

sql_files = {
$sqlDictBody
}

total_executed = 0
total_skipped = 0

for file_name, b64_content in sql_files.items():
    print(f"\n{'=' * 60}")
    print(f"Executing {file_name}...")
    print('=' * 60)
    sql_content = base64.b64decode(b64_content).decode('utf-8')
    statements = sql_content.split(';')
    file_executed = 0
    for i, stmt in enumerate(statements, 1):
        stmt = stmt.strip()
        # Skip empty or comment-only blocks
        lines = [l for l in stmt.split('\n') if l.strip() and not l.strip().startswith('--')]
        if not lines:
            continue
        try:
            spark.sql(stmt)
            file_executed += 1
            total_executed += 1
            print(f'  OK  Statement {i} executed')
        except Exception as e:
            total_skipped += 1
            print(f'  WARN Statement {i} skipped: {e}')
    print(f"  {file_name}: {file_executed} statements executed")

print(f"\nSQL execution complete: {total_executed} succeeded, {total_skipped} skipped")
"@

    # Inject lakehouse metadata binding (target: GoldLH)
    $lakehouseMeta = @"

# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "$($script:goldLakehouseId)",
# META       "default_lakehouse_name": "$GoldLakehouseName",
# META       "default_lakehouse_workspace_id": "$WorkspaceId",
# META       "known_lakehouses": [
# META         {
# META           "id": "$($script:goldLakehouseId)"
# META         }
# META       ]
# META     }
# META   }
"@
    $pyContent = $pyContent + $lakehouseMeta
    $pyContent = $pyContent -replace "`r`n", "`n"
    $nbBase64  = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($pyContent))

    # Deploy the notebook
    $tempNbName = "HorizonBooks_SQL_Setup"
    Write-Info "Creating notebook '$tempNbName'..."

    $fabricToken = Get-FabricToken
    $tempNbId = New-OrGetFabricItem `
        -DisplayName $tempNbName -Type "Notebook" `
        -Description "Lakehouse table/view definitions and date dimension generation (from Lakehouse/*.sql)" `
        -WsId $WorkspaceId -Token $fabricToken

    if (-not $tempNbId) {
        Write-Warn "Could not create SQL setup notebook - skipping"
        return
    }

    # Upload definition
    $defJson = '{"definition":{"parts":[{"path":"notebook-content.py","payload":"' + $nbBase64 + '","payloadType":"InlineBase64"}]}}'
    $fabricToken = Get-FabricToken
    $defOk = Update-FabricItemDefinition -ItemId $tempNbId -WsId $WorkspaceId `
        -DefinitionJson $defJson -Token $fabricToken

    if (-not $defOk) {
        Write-Warn "Failed to upload SQL notebook definition"
        return
    }
    Write-Success "SQL notebook deployed: $tempNbId"

    # Execute the notebook
    Start-Sleep -Seconds 5
    $fabricToken = Get-FabricToken
    $ok = Run-FabricNotebook -NotebookId $tempNbId -NotebookName $tempNbName `
        -WsId $WorkspaceId -Token $fabricToken -TimeoutMinutes 10

    if ($ok) {
        Write-Success "Lakehouse SQL scripts executed successfully"
    }
    else {
        Write-Warn "SQL script execution did not complete. Run '$tempNbName' manually from the Fabric portal."
    }
}

# ------------------------------------------------------------------
# Step 8: Deploy Semantic Model (TMDL)
# ------------------------------------------------------------------
Measure-Step "8. Deploy Semantic Model" {
    Write-Step "8/12" "Deploying Semantic Model '$SemanticModelName' (TMDL Direct Lake)"

    $tmdlRoot      = Join-Path $projectRoot "HorizonBooksAnalytics\HorizonBooksAnalytics.SemanticModel"
    $tmdlDefDir    = Join-Path $tmdlRoot "definition"
    $tmdlTablesDir = Join-Path $tmdlDefDir "tables"

    if (-not (Test-Path $tmdlRoot)) {
        Write-Warn "TMDL folder not found at $tmdlRoot - skipping"
        return
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
            $text = $text -replace '\{\{SQL_ENDPOINT\}\}', $script:sqlEndpoint
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

    $smDesc    = "Direct Lake semantic model on GoldLH - 18 tables (dim/fact schemas), 27 relationships, 79 DAX measures"
    $partsJson = $smParts -join ","
    $createSmJson = '{"displayName":"' + $SemanticModelName + '","type":"SemanticModel","description":"' + $smDesc + '","definition":{"parts":[' + $partsJson + ']}}'

    $script:semanticModelId = $null

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
                        $fabricToken  = Get-FabricToken
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
            $script:semanticModelId = $sm.id
        }
        else {
            $sm = $smResponse.Content | ConvertFrom-Json
            $script:semanticModelId = $sm.id
        }
        Write-Success "Semantic model deployed: $($script:semanticModelId)"
    }
    catch {
        $errMsg = $_.Exception.Message
        if ($errMsg -like "*ItemDisplayNameAlreadyInUse*") {
            Write-Info "Semantic model '$SemanticModelName' already exists."
            $fabricToken = Get-FabricToken
            $smItems = Invoke-FabricApi -Method Get `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=SemanticModel" -Token $fabricToken
            $sm = $smItems.value | Where-Object { $_.displayName -eq $SemanticModelName } | Select-Object -First 1
            $script:semanticModelId = $sm.id
            Write-Info "Using existing: $($script:semanticModelId)"
        }
        else { Write-Warn "Semantic model deployment issue: $errMsg" }
    }
}

# ------------------------------------------------------------------
# Step 9: Deploy Power BI Report (PBIR)
# ------------------------------------------------------------------
Measure-Step "9. Deploy Report" {
    Write-Step "9/12" "Deploying Power BI Report '$ReportName' (PBIR)"

    if ($SkipReport) {
        Write-Info "Report deployment skipped (-SkipReport)"
        return
    }

    $reportRoot   = Join-Path $projectRoot "HorizonBooksAnalytics\HorizonBooksAnalytics.Report"
    $reportDefDir = Join-Path $reportRoot "definition"

    if (-not (Test-Path $reportRoot)) {
        Write-Warn "Report folder not found at $reportRoot - skipping"
        return
    }

    $reportParts = @()

    # --- definition.pbir: rewrite byPath -> byConnection with SM ID ---
    $pbirPath = Join-Path $reportRoot "definition.pbir"
    if (Test-Path $pbirPath) {
        if ($script:semanticModelId) {
            # Build byConnection JSON referencing deployed semantic model (v2 format for Fabric REST API)
            $pbirObj = @{
                version = "4.0"
                datasetReference = @{
                    byPath = $null
                    byConnection = @{
                        connectionString          = "semanticmodelid=$($script:semanticModelId)"
                        pbiServiceModelId         = $null
                        pbiModelVirtualServerName = "sobe_wowvirtualserver"
                        pbiModelDatabaseName      = $script:semanticModelId
                        connectionType            = "pbiServiceXmlaStyleLive"
                        name                      = "EntityDataSource"
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
        Write-Info "Loaded definition.pbir (bound to SM $($script:semanticModelId))"
    }

    # --- Recursively collect all files under definition/ ---
    if (Test-Path $reportDefDir) {
        $defFiles = Get-ChildItem -Path $reportDefDir -Recurse -File | Sort-Object FullName
        foreach ($f in $defFiles) {
            $relPath = $f.FullName.Substring($reportRoot.Length + 1).Replace('\', '/')
            # Remap definition/RegisteredResources/ -> StaticResources/RegisteredResources/ for Fabric API
            if ($relPath -match '^definition/RegisteredResources/') {
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
    $createReportJson = '{"displayName":"' + $ReportName + '","type":"Report","description":"' + $reportDesc + '","definition":{"parts":[' + $partsJson + ']}}'

    $script:reportId = $null

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
            $lroSucceeded = $false
            if ($rptOpUrl) {
                $rptMaxPoll = 180; $rptPolled = 0
                while ($rptPolled -lt $rptMaxPoll) {
                    Start-Sleep -Seconds 10; $rptPolled += 10
                    try {
                        $fabricToken = Get-FabricToken
                        $pollData = Invoke-RestMethod -Method Get -Uri $rptOpUrl `
                            -Headers @{ "Authorization" = "Bearer $fabricToken" }
                        Write-Info "  Operation: $($pollData.status) ($($rptPolled)s)"
                        if ($pollData.status -eq "Succeeded") { $lroSucceeded = $true; break }
                        if ($pollData.status -eq "Failed") {
                            $errDetail = "no detail"
                            try { $errDetail = $pollData.error | ConvertTo-Json -Depth 5 -Compress } catch {}
                            Write-Warn "Report LRO failed: $errDetail"
                            break
                        }
                    }
                    catch { Write-Warn "Report poll error: $($_.Exception.Message)"; break }
                }
            }
            else { Start-Sleep -Seconds 20 }

            # Look up created report regardless of LRO outcome
            Start-Sleep -Seconds 5
            $fabricToken = Get-FabricToken
            $rptItems = Invoke-FabricApi -Method Get `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Report" -Token $fabricToken
            $rpt = $rptItems.value | Where-Object { $_.displayName -eq $ReportName } | Select-Object -First 1
            $script:reportId = $rpt.id

            if ($script:reportId -and -not $lroSucceeded) {
                # Report item exists but LRO failed – try updating its definition
                Write-Info "Report item exists ($($script:reportId)) despite LRO failure - updating definition..."
                $updateJson = '{"definition":{"parts":[' + $partsJson + ']}}'
                $fabricToken = Get-FabricToken
                $updated = Update-FabricItemDefinition -ItemId $script:reportId `
                    -WsId $WorkspaceId -DefinitionJson $updateJson -Token $fabricToken
                if ($updated) { Write-Success "Report definition updated after retry: $($script:reportId)" }
                else          { Write-Warn "Report definition update may have failed - check portal" }
            }
        }
        else {
            $rpt = $rptResponse.Content | ConvertFrom-Json
            $script:reportId = $rpt.id
        }
        if ($script:reportId) {
            Write-Success "Report deployed: $($script:reportId)"
        } else {
            Write-Warn "Report creation failed - no report ID captured. Check Fabric portal."
        }
    }
    catch {
        $errMsg = $_.Exception.Message
        if ($errMsg -like "*ItemDisplayNameAlreadyInUse*") {
            Write-Info "Report '$ReportName' already exists - updating definition."
            $fabricToken = Get-FabricToken
            $rptItems = Invoke-FabricApi -Method Get `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Report" -Token $fabricToken
            $rpt = $rptItems.value | Where-Object { $_.displayName -eq $ReportName } | Select-Object -First 1
            $script:reportId = $rpt.id

            if ($script:reportId) {
                $updateJson = '{"definition":{"parts":[' + $partsJson + ']}}'
                $updated = Update-FabricItemDefinition -ItemId $script:reportId `
                    -WsId $WorkspaceId -DefinitionJson $updateJson -Token $fabricToken
                if ($updated) {
                    Write-Success "Report definition updated: $($script:reportId)"
                }
                else {
                    Write-Warn "Report definition update may have failed - check portal"
                }
            }
        }
        else { Write-Warn "Report deployment issue: $errMsg" }
    }
}

# ------------------------------------------------------------------
# Step 10: Organize Workspace Folders
# ------------------------------------------------------------------
Measure-Step "10. Workspace Folders" {
    Write-Step "10/12" "Organizing Items into Workspace Folders"

    # Helper: create or get a workspace folder
    function New-OrGetWorkspaceFolder {
        param([string]$FolderName, [string]$WsId, [string]$Token)
        try {
            $resp = Invoke-WebRequest -Method Post `
                -Uri "$FabricApiBase/workspaces/$WsId/folders" `
                -Headers @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" } `
                -Body (@{ displayName = $FolderName } | ConvertTo-Json -Depth 3) -UseBasicParsing
            if ($resp.StatusCode -in @(200, 201)) {
                return ($resp.Content | ConvertFrom-Json).id
            }
        }
        catch {
            $errBody = ""
            try {
                if ($_.Exception.Response) {
                    $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                    $errBody = $sr.ReadToEnd(); $sr.Close()
                }
            } catch {}
            if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $errBody = $_.ErrorDetails.Message }

            if ($errBody -like "*already*" -or $errBody -like "*AlreadyExists*" -or $errBody -like "*DisplayName*") {
                try {
                    $folders = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WsId/folders" `
                        -Headers @{ Authorization = "Bearer $Token" }).value
                    $f = $folders | Where-Object { $_.displayName -eq $FolderName } | Select-Object -First 1
                    if ($f) { return $f.id }
                } catch {}
            }
            else { Write-Warn "Folder '$FolderName' error: $($_.Exception.Message)" }
        }
        return $null
    }

    # Helper: move item to folder via POST /items/{id}/move with targetFolderId
    function Move-ItemToFolder {
        param([string]$ItemId, [string]$ItemName, [string]$FolderId, [string]$FolderName, [string]$WsId, [string]$Token)
        try {
            Invoke-RestMethod -Method Post `
                -Uri "$FabricApiBase/workspaces/$WsId/items/$ItemId/move" `
                -Headers @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" } `
                -Body (@{ targetFolderId = $FolderId } | ConvertTo-Json -Depth 3) | Out-Null
            Write-Info "  Moved $ItemName -> $FolderName/"
        }
        catch { Write-Warn "  Could not move $ItemName to $FolderName/ : $($_.Exception.Message)" }
    }

    $fabricToken = Get-FabricToken

    # --- 01 - Data Storage folder: Lakehouses ---
    $dataFolderName = "01 - Data Storage"
    $dataFolderId = New-OrGetWorkspaceFolder -FolderName $dataFolderName -WsId $WorkspaceId -Token $fabricToken
    if ($dataFolderId) {
        Write-Success "Folder '$dataFolderName' ready ($dataFolderId)"
        foreach ($lhEntry in @(
            @{ Id = $script:bronzeLakehouseId; Name = $BronzeLakehouseName },
            @{ Id = $script:silverLakehouseId; Name = $SilverLakehouseName },
            @{ Id = $script:goldLakehouseId;   Name = $GoldLakehouseName }
        )) {
            if ($lhEntry.Id) {
                Move-ItemToFolder -ItemId $lhEntry.Id -ItemName $lhEntry.Name `
                    -FolderId $dataFolderId -FolderName $dataFolderName -WsId $WorkspaceId -Token $fabricToken
            }
        }
    }

    # --- 03 - Data Transformation folder: Notebooks ---
    $nbFolderName = "03 - Data Transformation"
    $nbFolderId = New-OrGetWorkspaceFolder -FolderName $nbFolderName -WsId $WorkspaceId -Token $fabricToken
    if ($nbFolderId) {
        Write-Success "Folder '$nbFolderName' ready ($nbFolderId)"
        $fabricToken = Get-FabricToken
        $allNbs = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Notebook" `
            -Headers @{ Authorization = "Bearer $fabricToken" }).value
        $hbNbs = $allNbs | Where-Object { $_.displayName -like "HorizonBooks_*" }
        foreach ($nb in $hbNbs) {
            Move-ItemToFolder -ItemId $nb.id -ItemName $nb.displayName `
                -FolderId $nbFolderId -FolderName $nbFolderName -WsId $WorkspaceId -Token $fabricToken
        }
    }

    # --- 05 - Analytics folder: Semantic Model + Report ---
    $analyticsFolderName = "05 - Analytics"
    $analyticsFolderId = New-OrGetWorkspaceFolder -FolderName $analyticsFolderName -WsId $WorkspaceId -Token $fabricToken
    if ($analyticsFolderId) {
        Write-Success "Folder '$analyticsFolderName' ready ($analyticsFolderId)"
        if ($script:semanticModelId) {
            Move-ItemToFolder -ItemId $script:semanticModelId -ItemName $SemanticModelName `
                -FolderId $analyticsFolderId -FolderName $analyticsFolderName -WsId $WorkspaceId -Token $fabricToken
        }
        if ($script:reportId) {
            Move-ItemToFolder -ItemId $script:reportId -ItemName $ReportName `
                -FolderId $analyticsFolderId -FolderName $analyticsFolderName -WsId $WorkspaceId -Token $fabricToken
        }
    }

    # --- 04 - Orchestration folder: Pipeline ---
    $orchFolderName = "04 - Orchestration"
    $orchFolderId = New-OrGetWorkspaceFolder -FolderName $orchFolderName -WsId $WorkspaceId -Token $fabricToken
    if ($orchFolderId) {
        Write-Success "Folder '$orchFolderName' ready ($orchFolderId)"
        $fabricToken = Get-FabricToken
        $pipelines = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=DataPipeline" `
            -Headers @{ Authorization = "Bearer $fabricToken" }).value
        $orchPl = $pipelines | Where-Object { $_.displayName -like "*HorizonBooks*" } | Select-Object -First 1
        if ($orchPl) {
            Move-ItemToFolder -ItemId $orchPl.id -ItemName $orchPl.displayName `
                -FolderId $orchFolderId -FolderName $orchFolderName -WsId $WorkspaceId -Token $fabricToken
        }
    }

    # --- 02 - Data Ingestion folder: Dataflows ---
    $ingestionFolderName = "02 - Data Ingestion"
    $ingestionFolderId = New-OrGetWorkspaceFolder -FolderName $ingestionFolderName -WsId $WorkspaceId -Token $fabricToken
    if ($ingestionFolderId) {
        Write-Success "Folder '$ingestionFolderName' ready ($ingestionFolderId)"
        $fabricToken = Get-FabricToken
        $dfItems = @()
        try {
            $dfItems = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Dataflow" `
                -Headers @{ Authorization = "Bearer $fabricToken" }).value
        } catch {
            # Fallback: list all items and filter for Dataflow types
            try {
                $allItems = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
                    -Headers @{ Authorization = "Bearer $fabricToken" }).value
                $dfItems = $allItems | Where-Object { $_.type -like "*Dataflow*" }
            } catch { Write-Warn "Could not list dataflows: $($_.Exception.Message)" }
        }
        $hbDfs = $dfItems | Where-Object { $_.displayName -like "*DF_*" -or $_.displayName -like "*HorizonBooks*" }
        foreach ($df in $hbDfs) {
            Move-ItemToFolder -ItemId $df.id -ItemName $df.displayName `
                -FolderId $ingestionFolderId -FolderName $ingestionFolderName -WsId $WorkspaceId -Token $fabricToken
        }
    }

    Write-Success "Workspace folders organized"
}

# ------------------------------------------------------------------
# Step 11: Deploy Data Agent
# ------------------------------------------------------------------
Measure-Step "11. Deploy Data Agent" {
    Write-Step "11/12" "Deploying Data Agent"

    if ($SkipDataAgent) {
        Write-Info "Data Agent deployment skipped (-SkipDataAgent)"
        return
    }

    $agentScript = Join-Path $scriptDir "Deploy-DataAgent.ps1"
    if (-not (Test-Path $agentScript)) {
        Write-Warn "Deploy-DataAgent.ps1 not found - skipping"
        return
    }

    try {
        & $agentScript -WorkspaceId $WorkspaceId
        Write-Success "Data Agent deployed"
    }
    catch {
        Write-Warn "Data Agent deployment issue: $_"
        Write-Info "Data Agents require F64+ capacity (not supported on Trial)."
    }
}

# ------------------------------------------------------------------
# Step 12: Validate Deployment
# ------------------------------------------------------------------
Measure-Step "12. Validate" {
    Write-Step "12/12" "Validating Deployment"

    if ($SkipValidation) {
        Write-Info "Validation skipped (-SkipValidation)"
        return
    }

    $validateScript = Join-Path $scriptDir "Validate-Deployment.ps1"
    if (-not (Test-Path $validateScript)) {
        Write-Warn "Validate-Deployment.ps1 not found - skipping"
        return
    }

    try {
        & $validateScript -WorkspaceId $WorkspaceId `
            -LakehouseName $GoldLakehouseName `
            -SemanticModelName $SemanticModelName
    }
    catch {
        Write-Warn "Validation encountered issues: $_"
    }
}

# ============================================================================
# FINAL SUMMARY
# ============================================================================

$deployEnd    = Get-Date
$totalElapsed = $deployEnd - $deployStart

Write-Host ""
Write-Banner "DEPLOYMENT COMPLETE" "Green"
Write-Host ""

# Step timings table
Write-Host "  Step Timings:" -ForegroundColor White
Write-Host "  $("-" * 55)" -ForegroundColor DarkGray
foreach ($t in $stepTimings) {
    $dur    = $t.Duration.ToString("mm\:ss")
    $color  = if ($t.Status -eq "OK") { "Green" } else { "Red" }
    $icon   = if ($t.Status -eq "OK") { "[OK]  " } else { "[FAIL]" }
    Write-Host "  $icon $($t.Step.PadRight(35)) $dur" -ForegroundColor $color
}
Write-Host "  $("-" * 55)" -ForegroundColor DarkGray
Write-Host "  Total elapsed: $($totalElapsed.ToString('mm\:ss'))" -ForegroundColor Cyan
Write-Host ""

# Resources
Write-Host "  Deployed Resources:" -ForegroundColor White
Write-Host "    Workspace      : $WorkspaceId" -ForegroundColor White
Write-Host "    BronzeLH       : $BronzeLakehouseName ($($script:bronzeLakehouseId))" -ForegroundColor White
Write-Host "    SilverLH       : $SilverLakehouseName ($($script:silverLakehouseId))" -ForegroundColor White
Write-Host "    GoldLH         : $GoldLakehouseName ($($script:goldLakehouseId))" -ForegroundColor White
if ($script:sqlEndpoint) {
    Write-Host "    SQL Endpoint   : $($script:sqlEndpoint) (GoldLH)" -ForegroundColor White
}
if ($script:semanticModelId) {
    Write-Host "    Semantic Model : $SemanticModelName ($($script:semanticModelId))" -ForegroundColor White
}
if ($script:reportId) {
    Write-Host "    Report         : $ReportName ($($script:reportId))" -ForegroundColor White
}
Write-Host ""
Write-Host "  Fabric Portal: https://app.fabric.microsoft.com/groups/$WorkspaceId" -ForegroundColor Cyan
Write-Host ""

$failedSteps = $stepTimings | Where-Object { $_.Status -eq "FAILED" }
if ($failedSteps.Count -gt 0) {
    Write-Host "  WARNING: $($failedSteps.Count) step(s) failed. Review output above." -ForegroundColor Yellow
    Write-Host ""
    exit 1
}
