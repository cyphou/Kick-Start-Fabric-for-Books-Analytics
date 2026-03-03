<#
.SYNOPSIS
    Shared helper functions for Horizon Books Fabric deployment scripts.

.DESCRIPTION
    This module centralises common utilities (authentication, Fabric API,
    OneLake, logging) used across all deploy/*.ps1 scripts.

    Usage:
        Import-Module (Join-Path $PSScriptRoot 'HorizonBooks.psm1') -Force
#>

# ── Module-scoped defaults ──────────────────────────────────────────────
$script:FabricApiBase = "https://api.fabric.microsoft.com/v1"
$script:OneLakeBase   = "https://onelake.dfs.fabric.microsoft.com"

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
    <#
    .SYNOPSIS
        Times a script block and records the result in a caller-provided list.
    .PARAMETER Name
        A label for the step.
    .PARAMETER Block
        The script block to execute and time.
    .PARAMETER Timings
        A [System.Collections.Generic.List[PSCustomObject]] to append the result to.
    #>
    param(
        [string]$Name,
        [scriptblock]$Block,
        [System.Collections.Generic.List[PSCustomObject]]$Timings
    )
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        & $Block
        $sw.Stop()
        $null = $Timings.Add([PSCustomObject]@{
            Step     = $Name
            Duration = $sw.Elapsed
            Status   = "OK"
        })
    }
    catch {
        $sw.Stop()
        $null = $Timings.Add([PSCustomObject]@{
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
    <#
    .SYNOPSIS
        Returns a bearer token for the Fabric REST API.
    #>
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
    <#
    .SYNOPSIS
        Returns a bearer token for the OneLake / Azure Storage DFS API.
    #>
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
    <#
    .SYNOPSIS
        Calls the Fabric REST API with automatic retry, 429 handling,
        and long-running-operation polling.
    #>
    param(
        [string]$Method,
        [string]$Uri,
        [object]$Body        = $null,
        [string]$BodyJson    = $null,
        [string]$Token,
        [int]$MaxRetries     = 10
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

            # 202 Accepted - Long Running Operation
            if ($statusCode -eq 202) {
                $locationHeader = $webResponse.Headers["Location"]
                $opIdHeader     = $webResponse.Headers["x-ms-operation-id"]
                $operationUrl   = $null
                if ($locationHeader) { $operationUrl = $locationHeader }
                elseif ($opIdHeader) { $operationUrl = "$($script:FabricApiBase)/operations/$opIdHeader" }

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
                Write-Warn ("$reason - retrying in {0}s (attempt {1}/{2})" -f $retryAfter, $attempt, $MaxRetries)
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
    <#
    .SYNOPSIS
        Polls a Fabric long-running operation until it succeeds,
        fails, or times out.
    #>
    param(
        [string]$OperationUrl,
        [string]$Token,
        [int]$TimeoutSeconds     = 600,
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
            Write-Info ("  Operation: {0} ({1}s)" -f $state, $elapsed)

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
    throw ("Operation timed out after {0}s" -f $TimeoutSeconds)
}

function New-OrGetFabricItem {
    <#
    .SYNOPSIS
        Returns the ID of an existing Fabric item with the given display-name
        and type, or creates a new one if none exists.  Checks for existing
        items FIRST to avoid creating duplicates across re-runs.
    #>
    param(
        [string]$DisplayName,
        [string]$Type,
        [string]$Description,
        [string]$WsId,
        [string]$Token
    )

    # ── Look for an existing item first ──────────────────────────────
    try {
        $existing = (Invoke-RestMethod `
            -Uri "$($script:FabricApiBase)/workspaces/$WsId/items?type=$Type" `
            -Headers @{ Authorization = "Bearer $Token" }).value
        $found = $existing | Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1
        if ($found) {
            Write-Info "'$DisplayName' ($Type) already exists - reusing $($found.id)"
            return $found.id
        }
    }
    catch { Write-Warn "Could not list existing ${Type} items: $($_.Exception.Message)" }

    # ── No existing item found - create a new one ────────────────────
    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
    $body = @{ displayName = $DisplayName; type = $Type; description = $Description } | ConvertTo-Json -Depth 5

    try {
        $resp = Invoke-WebRequest -Method Post -Uri "$($script:FabricApiBase)/workspaces/$WsId/items" `
            -Headers $headers -Body $body -UseBasicParsing

        if ($resp.StatusCode -eq 201) {
            $newId = ($resp.Content | ConvertFrom-Json).id
            Write-Info "Created $Type '$DisplayName': $newId"
            return $newId
        }
        elseif ($resp.StatusCode -eq 202) {
            $opUrl = $resp.Headers["Location"]
            if ($opUrl) {
                for ($p = 1; $p -le 24; $p++) {
                    Start-Sleep -Seconds 5
                    $poll = Invoke-RestMethod -Uri $opUrl -Headers @{ Authorization = "Bearer $Token" }
                    Write-Info ("  LRO: {0} ({1}s)" -f $poll.status, ($p*5))
                    if ($poll.status -eq "Succeeded") { break }
                    if ($poll.status -eq "Failed") { Write-Warn "LRO failed"; break }
                }
            }
            Start-Sleep -Seconds 3
            $items = (Invoke-RestMethod -Uri "$($script:FabricApiBase)/workspaces/$WsId/items?type=$Type" `
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
            $items = (Invoke-RestMethod -Uri "$($script:FabricApiBase)/workspaces/$WsId/items?type=$Type" `
                -Headers @{ Authorization = "Bearer $Token" }).value
            $found = $items | Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1
            if ($found) { return $found.id }
        }
        else { throw "Failed to create $Type '${DisplayName}': $errMsg" }
    }
    return $null
}

# ============================================================================
# ONELAKE HELPERS
# ============================================================================

function Upload-FileToOneLake {
    <#
    .SYNOPSIS
        Uploads a local file to OneLake via the DFS API (create → append → flush).
    #>
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

# ============================================================================
# DEFINITION HELPERS
# ============================================================================

function Update-FabricItemDefinition {
    <#
    .SYNOPSIS
        Updates the definition of a Fabric item (notebook, semantic model, report, etc.)
        with retry logic and LRO handling.
    #>
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
            $Token   = Get-FabricToken
            $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
        }
        try {
            $resp = Invoke-WebRequest -Method Post `
                -Uri "$($script:FabricApiBase)/workspaces/$WsId/items/$ItemId/updateDefinition" `
                -Headers $headers -Body $DefinitionJson -UseBasicParsing

            if ($resp.StatusCode -eq 200) { return $true }
            if ($resp.StatusCode -eq 202) {
                $opUrl = $resp.Headers["Location"]
                if ($opUrl) {
                    for ($p = 1; $p -le 24; $p++) {
                        Start-Sleep -Seconds 5
                        $poll = Invoke-RestMethod -Uri $opUrl -Headers @{ Authorization = "Bearer $Token" }
                        Write-Info ("  Definition LRO: {0} ({1}s)" -f $poll.status, ($p*5))
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
        Triggers a Fabric Spark notebook job and waits for completion.
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
                -Uri "$($script:FabricApiBase)/workspaces/$WsId/items/$NotebookId/jobs/instances?jobType=RunNotebook" `
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
            Write-Info ("  {0} status: {1} ({2}s)" -f $NotebookName, $jobStat.status, $waited)
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
                Write-Info ("  Job not ready yet ({0}s)" -f $waited)
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
        Triggers a Fabric Data Pipeline run and waits for completion.
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
                -Uri "$($script:FabricApiBase)/workspaces/$WsId/items/$PipelineId/jobs/instances?jobType=Pipeline" `
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
            Write-Info ("  Pipeline status: {0} ({1}s)" -f $jobStat.status, $waited)
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
                Write-Info ("  Pipeline job not ready yet ({0}s)" -f $waited)
            }
            else { Write-Warn "  Poll error: $($_.Exception.Message)" }
        }
    }

    Write-Warn "Pipeline timed out after $TimeoutMinutes minutes"
    return $false
}

# ============================================================================
# EXPORTS
# ============================================================================

Export-ModuleMember -Function @(
    # Display
    'Write-Banner', 'Write-Step', 'Write-Info', 'Write-Success',
    'Write-Warn', 'Write-Err', 'Measure-Step',
    # Tokens
    'Get-FabricToken', 'Get-StorageToken',
    # Fabric API
    'Invoke-FabricApi', 'Wait-FabricOperation', 'New-OrGetFabricItem',
    # OneLake
    'Upload-FileToOneLake',
    # Definitions & Jobs
    'Update-FabricItemDefinition', 'Run-FabricNotebook', 'Run-FabricPipeline'
) -Variable @('FabricApiBase', 'OneLakeBase')
