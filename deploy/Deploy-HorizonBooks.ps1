<#
.SYNOPSIS
    Deploys the Horizon Books Publishing & Distribution demo to Microsoft Fabric.

.DESCRIPTION
    This script automates the deployment of:
    1. A Lakehouse with all CSV data files uploaded via OneLake
    2. Three Spark Notebooks (Medallion Architecture: Bronze→Silver, Web Enrichment, Silver→Gold)
    3. Three Dataflow Gen2 items + a Data Pipeline for orchestration
    4. A Semantic Model (Direct Lake TMDL) with 27 relationships and 96 DAX measures
    5. A Data Agent for natural-language data exploration (requires F64+ capacity)

.PARAMETER WorkspaceId
    The GUID of the Fabric workspace to deploy to.

.PARAMETER DataFolder
    Path to the local folder containing CSV data files. Defaults to ./SampleData

.PARAMETER LakehouseName
    Name for the lakehouse. Defaults to HorizonBooksLH

.PARAMETER SemanticModelName
    Name for the semantic model. Defaults to HorizonBooksModel

.EXAMPLE
    .\Deploy-HorizonBooks.ps1 -WorkspaceId "your-workspace-guid"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,

    [Parameter(Mandatory = $false)]
    [string]$DataFolder,

    [Parameter(Mandatory = $false)]
    [string]$LakehouseName = "HorizonBooksLH",

    [Parameter(Mandatory = $false)]
    [string]$SemanticModelName = "HorizonBooksModel"
)

# Resolve script root for both dot-sourced and powershell -File invocations
$scriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$projectRoot = Split-Path -Parent $scriptDir
if (-not $DataFolder) { $DataFolder = Join-Path $projectRoot "SampleData" }

$ErrorActionPreference = "Stop"

# ============================================================================
# CONFIGURATION
# ============================================================================
$FabricApiBase = "https://api.fabric.microsoft.com/v1"
$OneLakeBase = "https://onelake.dfs.fabric.microsoft.com"

# CSV files to upload to lakehouse (organized by domain folder)
$LakehouseFiles = @(
    @{ Folder = "Finance"; Files = @("DimAccounts.csv", "DimCostCenters.csv", "FactFinancialTransactions.csv", "FactBudget.csv") },
    @{ Folder = "HR"; Files = @("DimEmployees.csv", "DimDepartments.csv", "FactPayroll.csv", "FactPerformanceReviews.csv", "FactRecruitment.csv") },
    @{ Folder = "Operations"; Files = @("DimBooks.csv", "DimAuthors.csv", "DimCustomers.csv", "DimGeography.csv", "DimWarehouses.csv", "FactOrders.csv", "FactInventory.csv", "FactReturns.csv") }
)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "=====================================================================" -ForegroundColor Cyan
    Write-Host " $Message" -ForegroundColor Cyan
    Write-Host "=====================================================================" -ForegroundColor Cyan
}

function Write-Info {
    param([string]$Message)
    Write-Host "  [INFO] $Message" -ForegroundColor Gray
}

function Write-Success {
    param([string]$Message)
    Write-Host "  [OK]   $Message" -ForegroundColor Green
}

function Write-Warn {
    param([string]$Message)
    Write-Host "  [WARN] $Message" -ForegroundColor Yellow
}

function Get-FabricToken {
    <#
    .SYNOPSIS
        Retrieves a bearer token for Fabric REST API.
    #>
    try {
        $token = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"
        return $token.Token
    }
    catch {
        Write-Error "Failed to get Fabric API token. Run 'Connect-AzAccount' first. Error: $_"
        throw
    }
}

function Get-StorageToken {
    <#
    .SYNOPSIS
        Retrieves a bearer token for OneLake (Storage) API.
    #>
    try {
        $token = Get-AzAccessToken -ResourceTypeName Storage
        return $token.Token
    }
    catch {
        Write-Error "Failed to get Storage token. Run 'Connect-AzAccount' first. Error: $_"
        throw
    }
}

function Invoke-FabricApi {
    <#
    .SYNOPSIS
        Calls the Fabric REST API with retry logic for 429/retriable responses.
        Compatible with PowerShell 5.1+.
    #>
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

    # Use pre-built JSON if provided (avoids PS 5.1 ConvertTo-Json crash with large payloads)
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
            if ($BodyJson) {
                $params["Body"] = $BodyJson
            }

            $webResponse = Invoke-WebRequest @params
            $statusCode = $webResponse.StatusCode

            # Handle 202 Accepted (Long Running Operation)
            if ($statusCode -eq 202) {
                $locationHeader = $webResponse.Headers["Location"]
                $opIdHeader = $webResponse.Headers["x-ms-operation-id"]
                if ($locationHeader) {
                    $operationUrl = $locationHeader
                }
                elseif ($opIdHeader) {
                    $operationUrl = "$FabricApiBase/operations/$opIdHeader"
                }
                else {
                    Write-Warn "202 response but no Location or operation-id header found."
                    return $null
                }
                Write-Info "Waiting for long-running operation to complete..."
                return Wait-FabricOperation -OperationUrl $operationUrl -Token $Token
            }

            # Parse JSON response body
            if ($webResponse.Content) {
                try { return $webResponse.Content | ConvertFrom-Json }
                catch { return $webResponse.Content }
            }
            return $null
        }
        catch {
            $ex = $_.Exception
            $statusCode = $null
            $errorBody = ""
            if ($ex -and $ex.Response) {
                $statusCode = [int]$ex.Response.StatusCode
                try {
                    $sr = New-Object System.IO.StreamReader($ex.Response.GetResponseStream())
                    $errorBody = $sr.ReadToEnd()
                    $sr.Close()
                } catch {}
            }

            $isRetriable = $false
            if ($errorBody -like "*isRetriable*true*" -or $errorBody -like "*NotAvailableYet*") {
                $isRetriable = $true
            }

            if ($statusCode -eq 429 -or $isRetriable) {
                $retryAfter = if ($isRetriable) { 15 } else { 30 }
                try {
                    $ra = $ex.Response.Headers | Where-Object { $_.Key -eq "Retry-After" } | Select-Object -ExpandProperty Value -First 1
                    if ($ra) { $retryAfter = [int]$ra }
                } catch {}
                $reason = if ($isRetriable) { "Retriable error" } else { "Rate limited (429)" }
                Write-Warn "$reason. Retrying after $retryAfter seconds (attempt $attempt/$MaxRetries)..."
                Start-Sleep -Seconds $retryAfter
            }
            else {
                if ($errorBody) {
                    throw "Fabric API error (HTTP $statusCode): $errorBody"
                }
                throw
            }
        }
    }
    throw "Max retries exceeded for $Uri"
}

function Wait-FabricOperation {
    <#
    .SYNOPSIS
        Polls a Fabric long-running operation until it completes.
    #>
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
            $state = $status.status
            Write-Info "  Operation status: $state ($elapsed`s elapsed)"

            if ($state -eq "Succeeded") {
                return $status
            }
            elseif ($state -eq "Failed") {
                Write-Error "Operation failed: $($status | ConvertTo-Json -Depth 5)"
                throw "Fabric operation failed"
            }
        }
        catch {
            if ($_.Exception.Response -and $_.Exception.Response.StatusCode -eq 429) {
                Write-Warn "Rate limited while polling. Waiting..."
                Start-Sleep -Seconds 30
            }
            else {
                throw
            }
        }
    }
    throw "Operation timed out after $TimeoutSeconds seconds"
}

function Upload-FileToOneLake {
    <#
    .SYNOPSIS
        Uploads a local file to OneLake via DFS API.
    #>
    param(
        [string]$LocalFilePath,
        [string]$OneLakePath,
        [string]$Token
    )

    $fileBytes = [System.IO.File]::ReadAllBytes($LocalFilePath)
    $fileName = [System.IO.Path]::GetFileName($LocalFilePath)

    # Step 1: Create the file (PUT with resource=file)
    $createUri = "${OneLakePath}/${fileName}?resource=file"
    $headers = @{
        "Authorization" = "Bearer $Token"
        "Content-Length" = "0"
    }
    Invoke-RestMethod -Method Put -Uri $createUri -Headers $headers | Out-Null

    # Step 2: Append data (PATCH with action=append)
    $appendUri = "${OneLakePath}/${fileName}?action=append&position=0"
    $headers = @{
        "Authorization" = "Bearer $Token"
        "Content-Type"  = "application/octet-stream"
        "Content-Length" = $fileBytes.Length.ToString()
    }
    Invoke-RestMethod -Method Patch -Uri $appendUri -Headers $headers -Body $fileBytes | Out-Null

    # Step 3: Flush (PATCH with action=flush)
    $flushUri = "${OneLakePath}/${fileName}?action=flush&position=$($fileBytes.Length)"
    $headers = @{
        "Authorization" = "Bearer $Token"
        "Content-Length" = "0"
    }
    Invoke-RestMethod -Method Patch -Uri $flushUri -Headers $headers | Out-Null
}

# ============================================================================
# MAIN DEPLOYMENT SEQUENCE
# ============================================================================

Write-Host ""
Write-Host "======================================================" -ForegroundColor Yellow
Write-Host "  Horizon Books Publishing - Fabric Deployment" -ForegroundColor Yellow
Write-Host "======================================================" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Workspace ID  : $WorkspaceId"
Write-Host "  Data Folder   : $DataFolder"
Write-Host "  Lakehouse     : $LakehouseName"
Write-Host "  Semantic Model: $SemanticModelName"
Write-Host ""

# Validate data folder
if (-not (Test-Path $DataFolder)) {
    Write-Error "Data folder not found: $DataFolder"
    exit 1
}

# ------------------------------------------------------------------
# Step 0: Authenticate
# ------------------------------------------------------------------
Write-Step "Step 0: Authenticating to Azure / Fabric"

$account = Get-AzContext
if (-not $account) {
    Write-Info "No active Azure session. Launching interactive login..."
    Connect-AzAccount
}
else {
    Write-Info "Using existing Azure session: $($account.Account.Id)"
}

$fabricToken = Get-FabricToken
$storageToken = Get-StorageToken
Write-Success "Authenticated successfully"

# ------------------------------------------------------------------
# Step 1: Create Lakehouse
# ------------------------------------------------------------------
Write-Step "Step 1: Creating Lakehouse '$LakehouseName'"

$lakehouseBody = @{
    displayName = $LakehouseName
    type        = "Lakehouse"
    description = "Horizon Books Publishing & Distribution data lakehouse - Finance, HR, Operations"
}

try {
    $lakehouse = Invoke-FabricApi -Method Post `
        -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
        -Body $lakehouseBody `
        -Token $fabricToken

    $lakehouseId = $lakehouse.id
    Write-Success "Lakehouse created: $lakehouseId"
}
catch {
    if ($_.Exception.Message -like "*ItemDisplayNameAlreadyInUse*" -or $_.Exception.Message -like "*already in use*") {
        Write-Warn "Lakehouse '$LakehouseName' already exists. Looking up ID..."
        $items = Invoke-FabricApi -Method Get `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Lakehouse" `
            -Token $fabricToken
        $lakehouse = $items.value | Where-Object { $_.displayName -eq $LakehouseName } | Select-Object -First 1
        $lakehouseId = $lakehouse.id
        Write-Info "Using existing lakehouse: $lakehouseId"
    }
    else { throw }
}

# Wait for lakehouse SQL endpoint to become available (required for Direct Lake semantic model)
$sqlEndpointConnStr = ""
$sqlEndpointMaxWait = 180
$sqlEndpointWaited = 0
Write-Info "Waiting for SQL endpoint to provision (may take up to 3 minutes)..."
while ($sqlEndpointWaited -lt $sqlEndpointMaxWait) {
    Start-Sleep -Seconds 15
    $sqlEndpointWaited += 15
    try {
        $fabricToken = Get-FabricToken
        $lhProps = Invoke-FabricApi -Method Get `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses/$lakehouseId" `
            -Token $fabricToken
        if ($lhProps.properties -and $lhProps.properties.sqlEndpointProperties -and $lhProps.properties.sqlEndpointProperties.connectionString) {
            $sqlEndpointConnStr = $lhProps.properties.sqlEndpointProperties.connectionString
            Write-Success "SQL endpoint ready: $sqlEndpointConnStr ($sqlEndpointWaited`s)"
            break
        }
        Write-Info "  SQL endpoint not ready yet ($sqlEndpointWaited`s)..."
    } catch {
        Write-Info "  Waiting for SQL endpoint ($sqlEndpointWaited`s)..."
    }
}
if (-not $sqlEndpointConnStr) {
    Write-Warn "SQL endpoint not available after $sqlEndpointMaxWait`s. Semantic model may need manual configuration."
}

# ------------------------------------------------------------------
# Step 2: Upload CSV Files to Lakehouse Files/
# ------------------------------------------------------------------
Write-Step "Step 2: Uploading CSV files to Lakehouse Files/"

$oneLakeFilesPath = "$OneLakeBase/$WorkspaceId/$lakehouseId/Files"

# Create the Files directory
try {
    $headers = @{
        "Authorization" = "Bearer $storageToken"
        "Content-Length" = "0"
    }
    Invoke-RestMethod -Method Put -Uri "${oneLakeFilesPath}?resource=directory" -Headers $headers | Out-Null
}
catch {
    # Directory might already exist
}

$uploadedCount = 0
$totalFiles = 0
foreach ($group in $LakehouseFiles) {
    foreach ($fileName in $group.Files) {
        $totalFiles++
        $localPath = Join-Path (Join-Path $DataFolder $group.Folder) $fileName
        if (-not (Test-Path $localPath)) {
            Write-Warn "File not found, skipping: $($group.Folder)/$fileName"
            continue
        }
        Write-Info "Uploading $($group.Folder)/$fileName..."
        $storageToken = Get-StorageToken
        Upload-FileToOneLake -LocalFilePath $localPath -OneLakePath $oneLakeFilesPath -Token $storageToken
        $uploadedCount++
        Write-Success "Uploaded $fileName"
    }
}
Write-Success "Uploaded $uploadedCount / $totalFiles files to lakehouse"

# ------------------------------------------------------------------
# Step 3: Deploy Transformation Notebooks (create + run Bronze→Silver)
# ------------------------------------------------------------------
Write-Step "Step 3: Deploying Spark Notebooks (Medallion Architecture)"
Write-Info "Notebook 01 runs now; Notebooks 02-03 will be orchestrated by the pipeline in Step 4."

$notebooksDir = Join-Path $projectRoot "notebooks"

# Notebook definitions: order matters (Bronze→Silver first, then Web Enrichment, then Silver→Gold)
$notebooks = @(
    @{
        Name        = "HorizonBooks_01_BronzeToSilver"
        FileName    = "01_BronzeToSilver.py"
        Description = "Ingests CSV files with schema enforcement, data quality checks, deduplication, and dimension/fact-specific transformations (Bronze→Silver)"
    },
    @{
        Name        = "HorizonBooks_02_WebEnrichment"
        FileName    = "02_WebEnrichment.py"
        Description = "Fetches data from public web APIs (exchange rates, holidays, country indicators, book metadata) and enriches Silver tables with web data"
    },
    @{
        Name        = "HorizonBooks_03_SilverToGold"
        FileName    = "03_SilverToGold.py"
        Description = "Applies business logic, generates DimDate, enriches dimensions with RFM segmentation, cohort analysis, anomaly detection, co-purchasing patterns, and revenue forecasting (Silver→Gold)"
    }
)

# ── Helper: Deploy a single notebook (create/update + run) ──
function Deploy-FabricNotebook {
    param(
        [string]$NotebookName,
        [string]$NotebookFilePath,
        [string]$Description,
        [string]$WsId,
        [string]$LhId,
        [string]$LhName,
        [string]$ApiBase,
        [switch]$SkipRun
    )

    Write-Info "── Deploying notebook: $NotebookName ──"

    # Read and inject lakehouse binding into notebook source
    $rawContent = Get-Content -Path $NotebookFilePath -Raw -Encoding UTF8

    # Inject lakehouse dependency if not already present
    $lakehouseMeta = @"
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "$LhId",
# META       "default_lakehouse_name": "$LhName",
# META       "default_lakehouse_workspace_id": "$WsId",
# META       "known_lakehouses": [
# META         {
# META           "id": "$LhId"
# META         }
# META       ]
# META     }
# META   }
"@

    # Replace the empty dependencies block with lakehouse binding
    $rawContent = $rawContent -replace '# META\s+"dependencies":\s*\{\}', $lakehouseMeta.Trim()

    # Normalize line endings for Fabric
    $rawContent = $rawContent -replace "`r`n", "`n"
    $notebookBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($rawContent))
    Write-Info "Encoded $NotebookName (Base64 length: $($notebookBase64.Length))"

    # Create notebook item
    $fabricToken = Get-FabricToken
    $nbHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
    $createBody = @{
        displayName = $NotebookName
        type        = "Notebook"
        description = $Description
    } | ConvertTo-Json -Depth 5

    $notebookId = $null
    for ($createAttempt = 1; $createAttempt -le 3; $createAttempt++) {
        if ($createAttempt -gt 1) {
            Write-Info "Notebook creation retry $createAttempt/3 - waiting 15s..."
            Start-Sleep -Seconds 15
            $fabricToken = Get-FabricToken
            $nbHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
        }
        try {
            Write-Info "Creating notebook item (attempt $createAttempt)..."
            $nbCreateResp = Invoke-WebRequest -Method Post `
                -Uri "$ApiBase/workspaces/$WsId/items" `
                -Headers $nbHeaders -Body $createBody -UseBasicParsing

            if ($nbCreateResp.StatusCode -eq 201) {
                $nbObj = $nbCreateResp.Content | ConvertFrom-Json
                $notebookId = $nbObj.id
            }
            elseif ($nbCreateResp.StatusCode -eq 202) {
                $nbOpUrl = $nbCreateResp.Headers["Location"]
                if ($nbOpUrl) {
                    for ($p = 1; $p -le 12; $p++) {
                        Start-Sleep -Seconds 5
                        $nbPoll = Invoke-RestMethod -Uri $nbOpUrl -Headers @{Authorization = "Bearer $fabricToken"}
                        Write-Info "  Creation LRO: $($nbPoll.status) ($($p*5)s)"
                        if ($nbPoll.status -eq "Succeeded") { break }
                        if ($nbPoll.status -eq "Failed") { Write-Warn "Creation LRO failed"; break }
                    }
                }
                Start-Sleep -Seconds 3
                $nbItems = (Invoke-RestMethod -Uri "$ApiBase/workspaces/$WsId/items?type=Notebook" `
                    -Headers @{Authorization = "Bearer $fabricToken"}).value
                $nbFound = $nbItems | Where-Object { $_.displayName -eq $NotebookName } | Select-Object -First 1
                if ($nbFound) { $notebookId = $nbFound.id }
            }

            if ($notebookId) {
                Write-Success "Notebook item created: $notebookId"
                break
            }
            Write-Warn "Notebook not found after creation attempt $createAttempt"
        }
        catch {
            $nbErrBody = ""
            try {
                $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $nbErrBody = $sr.ReadToEnd(); $sr.Close()
            } catch {}
            $errMsg = "$($_.Exception.Message) $nbErrBody"
            if ($errMsg -like "*ItemDisplayNameAlreadyInUse*" -or $errMsg -like "*already in use*") {
                Write-Warn "Notebook '$NotebookName' already exists - looking up..."
                $nbItems = (Invoke-RestMethod -Uri "$ApiBase/workspaces/$WsId/items?type=Notebook" `
                    -Headers @{Authorization = "Bearer $fabricToken"}).value
                $nbFound = $nbItems | Where-Object { $_.displayName -eq $NotebookName } | Select-Object -First 1
                if ($nbFound) { $notebookId = $nbFound.id; Write-Info "Using existing notebook: $notebookId" }
                break
            }
            Write-Warn "Creation error (attempt $createAttempt): $errMsg"
        }
    }

    # Update definition with lakehouse-bound content
    $definitionApplied = $false
    if ($notebookId) {
        $updateDefJson = '{"definition":{"parts":[{"path":"notebook-content.py","payload":"' + $notebookBase64 + '","payloadType":"InlineBase64"}]}}'
        for ($defAttempt = 1; $defAttempt -le 3; $defAttempt++) {
            if ($defAttempt -gt 1) {
                Write-Info "Definition update retry $defAttempt/3 - waiting 10s..."
                Start-Sleep -Seconds 10
                $fabricToken = Get-FabricToken
            }
            $nbHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
            try {
                Write-Info "Updating notebook definition (attempt $defAttempt)..."
                $udResp = Invoke-WebRequest -Method Post `
                    -Uri "$ApiBase/workspaces/$WsId/items/$notebookId/updateDefinition" `
                    -Headers $nbHeaders -Body $updateDefJson -UseBasicParsing

                if ($udResp.StatusCode -eq 200) {
                    $definitionApplied = $true
                }
                elseif ($udResp.StatusCode -eq 202) {
                    $udOpUrl = $udResp.Headers["Location"]
                    if ($udOpUrl) {
                        for ($p = 1; $p -le 12; $p++) {
                            Start-Sleep -Seconds 5
                            $udPoll = Invoke-RestMethod -Uri $udOpUrl -Headers @{Authorization = "Bearer $fabricToken"}
                            Write-Info "  Definition LRO: $($udPoll.status) ($($p*5)s)"
                            if ($udPoll.status -eq "Succeeded") { $definitionApplied = $true; break }
                            if ($udPoll.status -eq "Failed") { Write-Warn "Definition LRO failed"; break }
                        }
                    }
                }

                if ($definitionApplied) {
                    Write-Success "Notebook definition updated with lakehouse binding"
                    break
                }
            }
            catch {
                Write-Warn "Definition update error (attempt $defAttempt): $($_.Exception.Message)"
            }
        }
        if (-not $definitionApplied) {
            Write-Warn "Failed to update notebook definition for '$NotebookName'. Update manually."
        }
    }

    # Run the notebook (unless SkipRun is set — pipeline will handle execution)
    $notebookSuccess = $false
    if ($SkipRun -and $notebookId -and $definitionApplied) {
        Write-Info "Skipping run for '$NotebookName' — pipeline will orchestrate execution."
        $notebookSuccess = $true
    }
    elseif ($notebookId -and $definitionApplied) {
        Write-Info "Running $NotebookName (Spark session may take a few minutes to start)..."
        Start-Sleep -Seconds 15

        for ($runAttempt = 1; $runAttempt -le 3; $runAttempt++) {
            if ($runAttempt -gt 1) {
                Write-Info "Run retry $runAttempt/3 - waiting 30s..."
                Start-Sleep -Seconds 30
                $fabricToken = Get-FabricToken
            }
            $nbHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
            try {
                Write-Info "Starting notebook run (attempt $runAttempt)..."
                $runResp = Invoke-WebRequest -Method Post `
                    -Uri "$ApiBase/workspaces/$WsId/items/$notebookId/jobs/instances?jobType=RunNotebook" `
                    -Headers $nbHeaders -UseBasicParsing

                if ($runResp.StatusCode -eq 202) {
                    $jobLoc = $runResp.Headers["Location"]
                    if ($jobLoc) {
                        $maxWait = 900; $waited = 0
                        while ($waited -lt $maxWait) {
                            Start-Sleep -Seconds 15; $waited += 15
                            try {
                                $jobStat = Invoke-RestMethod -Uri $jobLoc -Headers @{Authorization = "Bearer $fabricToken"}
                                Write-Info "  Notebook job: $($jobStat.status) ($waited`s)"
                                if ($jobStat.status -eq "Completed") {
                                    Write-Success "$NotebookName completed successfully"
                                    $notebookSuccess = $true; break
                                }
                                if ($jobStat.status -eq "Failed" -or $jobStat.status -eq "Cancelled") {
                                    $reason = ""
                                    if ($jobStat.failureReason) { $reason = $jobStat.failureReason.message }
                                    Write-Warn "Notebook job $($jobStat.status): $reason"
                                    break
                                }
                            }
                            catch {
                                if ($_.Exception.Response -and [int]$_.Exception.Response.StatusCode -eq 404) {
                                    Write-Info "  Job not ready yet ($waited`s)"
                                } else { Write-Warn "  Poll error: $($_.Exception.Message)" }
                            }
                        }
                    }
                }
                if ($notebookSuccess) { break }
            }
            catch {
                Write-Warn "Notebook run error (attempt $runAttempt): $($_.Exception.Message)"
            }
        }

        if (-not $notebookSuccess) {
            Write-Warn "$NotebookName did not complete. Run it manually from the Fabric portal."
        }
    }
    elseif ($notebookId -and -not $definitionApplied) {
        Write-Warn "Skipping run for '$NotebookName' - definition not applied. Update and run manually."
    }
    else {
        Write-Warn "Failed to create '$NotebookName'. Create and run it manually."
    }

    return @{ Id = $notebookId; Success = $notebookSuccess; Name = $NotebookName }
}

# ── Deploy each notebook sequentially ──
# Notebook 01 (BronzeToSilver) runs immediately; 02 & 03 are deployed but not run
# (the pipeline in Step 4 will orchestrate Dataflows → NB02 → NB03)
$allNotebooksOk = $true
foreach ($nb in $notebooks) {
    $nbFilePath = Join-Path $notebooksDir $nb.FileName
    if (-not (Test-Path $nbFilePath)) {
        Write-Warn "Notebook file not found: $nbFilePath"
        $allNotebooksOk = $false
        continue
    }

    # Only run NB01 directly; NB02 and NB03 will be executed by the pipeline
    $skipRun = $nb.FileName -ne "01_BronzeToSilver.py"

    $deployParams = @{
        NotebookName     = $nb.Name
        NotebookFilePath = $nbFilePath
        Description      = $nb.Description
        WsId             = $WorkspaceId
        LhId             = $lakehouseId
        LhName           = $LakehouseName
        ApiBase          = $FabricApiBase
    }
    if ($skipRun) { $deployParams["SkipRun"] = $true }

    $result = Deploy-FabricNotebook @deployParams

    if (-not $result.Success) {
        $allNotebooksOk = $false
        Write-Warn "Notebook '$($nb.Name)' did not complete. Subsequent notebooks may fail."
        # Continue deploying remaining notebooks even if one fails
    }
}

if ($allNotebooksOk) {
    Write-Success "All transformation notebooks deployed successfully"
} else {
    Write-Warn "Some notebooks did not deploy. Check the Fabric portal and fix manually if needed."
}

# ------------------------------------------------------------------
# Step 4: Deploy Dataflows + Orchestration Pipeline
# ------------------------------------------------------------------
Write-Step "Step 4: Creating Dataflows and Orchestration Pipeline"

$pipelineScript = Join-Path $scriptDir "Deploy-Pipeline.ps1"
if (Test-Path $pipelineScript) {
    try {
        Write-Info "Running Deploy-Pipeline.ps1 to create Dataflows and Pipeline..."
        & $pipelineScript -WorkspaceId $WorkspaceId -LakehouseId $lakehouseId -LakehouseName $LakehouseName
        Write-Success "Dataflows and Pipeline deployed successfully"
        Write-Info "Pipeline 'PL_HorizonBooks_Orchestration' can be run from the Fabric portal"
        Write-Info "It orchestrates: Dataflows (CSV→Bronze) → WebEnrichment → SilverToGold"
    }
    catch {
        Write-Warn "Pipeline deployment encountered an issue: $_"
        Write-Info "You can re-run: deploy\Deploy-Pipeline.ps1 -WorkspaceId $WorkspaceId -LakehouseId $lakehouseId"
    }
}
else {
    Write-Warn "Pipeline script not found at: $pipelineScript"
    Write-Info "Create the pipeline manually in the Fabric portal."
}

# ------------------------------------------------------------------
# Step 5: Create Semantic Model (TMDL format)
# ------------------------------------------------------------------
Write-Step "Step 5: Creating Semantic Model '$SemanticModelName' (TMDL Direct Lake)"

# TMDL files are stored in the PBIP project structure
$tmdlRoot = Join-Path $projectRoot "HorizonBooksAnalytics\HorizonBooksAnalytics.SemanticModel"
$tmdlDefDir = Join-Path $tmdlRoot "definition"
$tmdlTablesDir = Join-Path $tmdlDefDir "tables"

if (Test-Path $tmdlRoot) {
    $smParts = @()

    # 1. definition.pbism
    $pbismPath = Join-Path $tmdlRoot "definition.pbism"
    if (Test-Path $pbismPath) {
        $pbismContent = Get-Content -Path $pbismPath -Raw -Encoding UTF8
        $pbismBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($pbismContent))
        $smParts += '{"path":"definition.pbism","payload":"' + $pbismBase64 + '","payloadType":"InlineBase64"}'
        Write-Info "  Loaded definition.pbism"
    }

    # 2. definition/*.tmdl files
    $defFiles = Get-ChildItem -Path $tmdlDefDir -Filter "*.tmdl" -File | Sort-Object Name
    foreach ($f in $defFiles) {
        $fileContent = Get-Content -Path $f.FullName -Raw -Encoding UTF8

        # Replace placeholders in expressions.tmdl with actual lakehouse connection info
        if ($f.Name -eq "expressions.tmdl") {
            $fileContent = $fileContent -replace '\{\{SQL_ENDPOINT\}\}', $sqlEndpointConnStr
            $fileContent = $fileContent -replace '\{\{LAKEHOUSE_NAME\}\}', $LakehouseName
        }

        $fileBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($fileContent))
        $partPath = "definition/" + $f.Name
        $smParts += '{"path":"' + $partPath + '","payload":"' + $fileBase64 + '","payloadType":"InlineBase64"}'
        Write-Info "  Loaded $partPath"
    }

    # 3. definition/tables/*.tmdl files
    if (Test-Path $tmdlTablesDir) {
        $tableFiles = Get-ChildItem -Path $tmdlTablesDir -Filter "*.tmdl" -File | Sort-Object Name
        foreach ($f in $tableFiles) {
            $fileContent = Get-Content -Path $f.FullName -Raw -Encoding UTF8
            $fileBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($fileContent))
            $partPath = "definition/tables/" + $f.Name
            $smParts += '{"path":"' + $partPath + '","payload":"' + $fileBase64 + '","payloadType":"InlineBase64"}'
            Write-Info "  Loaded $partPath"
        }
    }

    Write-Info "Total TMDL parts: $($smParts.Count)"

    # Build JSON manually (PS 5.1 ConvertTo-Json crashes with large payloads)
    $smDescription = "Direct Lake semantic model for Horizon Books Publishing - 23 tables, 27 relationships, 96 DAX measures across Finance, HR, Operations, and Forecasting"
    $partsJson = $smParts -join ","
    $createSmJson = '{"displayName":"' + $SemanticModelName + '","type":"SemanticModel","description":"' + $smDescription + '","definition":{"parts":[' + $partsJson + ']}}'
    Write-Info "SM payload size: $($createSmJson.Length) chars"

    try {
        Write-Info "Sending semantic model creation request..."
        $smHeaders = @{
            "Authorization" = "Bearer $fabricToken"
            "Content-Type"  = "application/json"
        }
        $smResponse = Invoke-WebRequest -Method Post `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
            -Headers $smHeaders `
            -Body $createSmJson `
            -UseBasicParsing

        if ($smResponse.StatusCode -eq 202) {
            Write-Info "Semantic model creation accepted (202). Waiting for provisioning..."
            $smOpUrl = $null
            try { $smOpUrl = $smResponse.Headers["Location"] } catch {}
            if (-not $smOpUrl) {
                try {
                    $smOpId = $smResponse.Headers["x-ms-operation-id"]
                    if ($smOpId) { $smOpUrl = "$FabricApiBase/operations/$smOpId" }
                } catch {}
            }
            if ($smOpUrl) {
                $smPollHeaders = @{ "Authorization" = "Bearer $fabricToken" }
                $smMaxPoll = 120; $smPolled = 0
                while ($smPolled -lt $smMaxPoll) {
                    Start-Sleep -Seconds 10
                    $smPolled += 10
                    try {
                        $fabricToken = Get-FabricToken
                        $smPollHeaders = @{ "Authorization" = "Bearer $fabricToken" }
                        $smPollResp = Invoke-WebRequest -Method Get -Uri $smOpUrl -Headers $smPollHeaders -UseBasicParsing
                        $smPollData = $smPollResp.Content | ConvertFrom-Json
                        Write-Info "  Operation: $($smPollData.status) ($smPolled`s)"
                        if ($smPollData.status -eq "Succeeded") { break }
                        if ($smPollData.status -eq "Failed") {
                            Write-Warn "SM operation failed: $($smPollResp.Content)"
                            break
                        }
                    } catch {
                        Write-Warn "SM poll error: $($_.Exception.Message)"
                        break
                    }
                }
            }
            else {
                Start-Sleep -Seconds 15
            }
            Write-Info "Looking up semantic model ID..."
            $smItems = Invoke-FabricApi -Method Get `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=SemanticModel" `
                -Token $fabricToken
            $sm = $smItems.value | Where-Object { $_.displayName -eq $SemanticModelName } | Select-Object -First 1
            $semanticModelId = $sm.id
        }
        else {
            $semanticModel = $smResponse.Content | ConvertFrom-Json
            $semanticModelId = $semanticModel.id
        }
        Write-Success "Semantic model created: $semanticModelId"
    }
    catch {
        $smErrBody = ""
        if ($_.Exception.Response) {
            try {
                $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $smErrBody = $sr.ReadToEnd()
                $sr.Close()
            } catch {}
        }
        $errMsg = "$($_.Exception.Message) $smErrBody"
        if ($errMsg -like "*ItemDisplayNameAlreadyInUse*") {
            Write-Warn "Semantic model '$SemanticModelName' already exists."
        }
        else {
            Write-Warn "Semantic model creation encountered an issue: $errMsg"
        }
    }
}
else {
    Write-Warn "TMDL folder not found at: $tmdlRoot"
    Write-Info "Please create the semantic model manually from the lakehouse."
}

# ------------------------------------------------------------------
# Step 6: Deploy Data Agent
# ------------------------------------------------------------------
Write-Step "Step 6: Deploying Data Agent"

$dataAgentScript = Join-Path $scriptDir "Deploy-DataAgent.ps1"
if (Test-Path $dataAgentScript) {
    try {
        & $dataAgentScript -WorkspaceId $WorkspaceId
        Write-Success "Data Agent deployment script executed."
    }
    catch {
        Write-Warn "Data Agent deployment encountered an issue: $_"
        Write-Info "Data Agents require Fabric capacity F64+. Trial capacity is not supported."
        Write-Info "You can re-run: deploy\Deploy-DataAgent.ps1 -WorkspaceId $WorkspaceId"
    }
}
else {
    Write-Warn "Data Agent script not found at: $dataAgentScript"
    Write-Info "Create the Data Agent manually. See DataAgent/DataAgentConfiguration.md"
}

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
Write-Host ""
Write-Host "======================================================" -ForegroundColor Green
Write-Host "  DEPLOYMENT SUMMARY" -ForegroundColor Green
Write-Host "======================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Workspace     : $WorkspaceId" -ForegroundColor White
Write-Host "  Lakehouse     : $LakehouseName ($lakehouseId)" -ForegroundColor White
if ($semanticModelId) {
    Write-Host "  Semantic Model: $SemanticModelName ($semanticModelId)" -ForegroundColor White
}
Write-Host ""
Write-Host "  PIPELINE ORCHESTRATION:" -ForegroundColor Cyan
Write-Host "  -------------------------------------------------" -ForegroundColor Cyan
Write-Host "  Pipeline 'PL_HorizonBooks_Orchestration' is ready to run." -ForegroundColor Cyan
Write-Host "  Flow: Dataflows (CSV→Bronze) → WebEnrichment → SilverToGold" -ForegroundColor Cyan
Write-Host ""
Write-Host "  REMAINING MANUAL STEPS:" -ForegroundColor Yellow
Write-Host "  -------------------------------------------------" -ForegroundColor Yellow
Write-Host "  1. Run the pipeline from the Fabric portal (or schedule it)" -ForegroundColor Yellow
Write-Host "  2. Open HorizonBooksAnalytics.pbip in Power BI Desktop" -ForegroundColor Yellow
Write-Host "     to explore the pre-built 10-page report" -ForegroundColor Yellow
Write-Host "  3. Data Agent: Requires F64+ capacity" -ForegroundColor Yellow
Write-Host "     (see DataAgent/DataAgentConfiguration.md)" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Fabric Portal: https://app.fabric.microsoft.com/" -ForegroundColor Cyan
Write-Host ""
