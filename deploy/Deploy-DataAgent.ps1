<#
.SYNOPSIS
    Deploys a Data Agent for Horizon Books Publishing natural-language exploration.

.DESCRIPTION
    Creates a Data Agent item in Microsoft Fabric that connects to the Horizon Books
    semantic model. Requires F64+ capacity (not supported on trial).

.PARAMETER WorkspaceId
    The GUID of the Fabric workspace.

.PARAMETER AgentName
    Name for the Data Agent. Defaults to HorizonBooks DataAgent.

.EXAMPLE
    .\Deploy-DataAgent.ps1 -WorkspaceId "your-workspace-guid"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,

    [Parameter(Mandatory = $false)]
    [string]$AgentName = "HorizonBooks DataAgent",

    [Parameter(Mandatory = $false)]
    [string]$ParentFolderName = "05 - Analytics"
)

$ErrorActionPreference = "Stop"
Import-Module (Join-Path $PSScriptRoot 'HorizonBooks.psm1') -Force

# ── Imported from HorizonBooks.psm1 ──────────────────────────────────────
#   Get-FabricToken, Write-Info, Write-Success, Write-Warn
#   $FabricApiBase

$FabricApiBase = $script:FabricApiBase

Write-Host ""
Write-Host "  Deploying Data Agent: $AgentName" -ForegroundColor Cyan
Write-Host ""

$fabricToken = Get-FabricToken

# ── Resolve parent folder ──
$folderId = $null
if ($ParentFolderName) {
    Write-Info "Resolving folder '$ParentFolderName'..."
    $allItems = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
        -Headers @{Authorization = "Bearer $fabricToken"}).value
    $folder = $allItems | Where-Object { $_.type -eq "Folder" -and $_.displayName -eq $ParentFolderName } | Select-Object -First 1
    if ($folder) {
        $folderId = $folder.id
        Write-Success "Folder found: $folderId"
    } else {
        Write-Warn "Folder '$ParentFolderName' not found – deploying to workspace root."
    }
}

# Create Data Agent item
$agentBody = @{
    displayName = $AgentName
    type        = "DataAgent"
    description = "Natural-language data exploration agent for Horizon Books Publishing & Distribution. Covers Finance (P&L, Budget), HR (Workforce, Payroll, Recruitment), and Operations (Orders, Inventory, Returns) across 15+ international markets."
}
if ($folderId) { $agentBody["folderId"] = $folderId }
$agentBody = $agentBody | ConvertTo-Json -Depth 5

$agentId = $null
try {
    Write-Info "Creating Data Agent item..."
    $headers = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
    $resp = Invoke-WebRequest -Method Post `
        -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
        -Headers $headers -Body $agentBody -UseBasicParsing

    if ($resp.StatusCode -eq 201) {
        $agent = $resp.Content | ConvertFrom-Json
        $agentId = $agent.id
        Write-Success "Data Agent created: $agentId"
    }
    elseif ($resp.StatusCode -eq 202) {
        $opUrl = $resp.Headers["Location"]
        if ($opUrl) {
            for ($p = 1; $p -le 12; $p++) {
                Start-Sleep -Seconds 5
                $poll = Invoke-RestMethod -Uri $opUrl -Headers @{Authorization = "Bearer $fabricToken"}
                Write-Info "  LRO: $($poll.status) ($($p*5)s)"
                if ($poll.status -eq "Succeeded") { break }
                if ($poll.status -eq "Failed") { Write-Warn "LRO failed"; break }
            }
        }
        Start-Sleep -Seconds 3
        $items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=DataAgent" `
            -Headers @{Authorization = "Bearer $fabricToken"}).value
        $found = $items | Where-Object { $_.displayName -eq $AgentName } | Select-Object -First 1
        if ($found) { $agentId = $found.id; Write-Success "Data Agent created: $agentId" }
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
        Write-Warn "Data Agent '$AgentName' already exists."
        $items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=DataAgent" `
            -Headers @{Authorization = "Bearer $fabricToken"}).value
        $found = $items | Where-Object { $_.displayName -eq $AgentName } | Select-Object -First 1
        if ($found) { $agentId = $found.id; Write-Info "Using existing: $agentId" }
    }
    else {
        Write-Warn "Data Agent creation failed: $errMsg"
        Write-Warn "Data Agents require Fabric capacity F64+."
    }
}

if ($agentId) {
    # Move to folder if needed
    if ($ParentFolderName -and -not $folderId) {
        # Try the known folder IDs
        $knownFolders = @{
            "05 - Analytics" = "ad3ca0c1-06db-4653-a3e6-05c20dcc835c"
        }
        if ($knownFolders.ContainsKey($ParentFolderName)) {
            $targetFolder = $knownFolders[$ParentFolderName]
            Write-Info "Moving Data Agent to folder '$ParentFolderName'..."
            try {
                $moveBody = @{ folderId = $targetFolder } | ConvertTo-Json
                Invoke-WebRequest -Method Post `
                    -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$agentId/move" `
                    -Headers @{Authorization = "Bearer $fabricToken"; "Content-Type" = "application/json"} `
                    -Body $moveBody -UseBasicParsing -ErrorAction Stop | Out-Null
                Write-Success "Moved to folder '$ParentFolderName'"
            } catch {
                Write-Warn "Could not move to folder: $($_.Exception.Message)"
            }
        }
    }

    Write-Host ""
    Write-Host "  Data Agent deployed successfully!" -ForegroundColor Green
    Write-Host "  Next steps:" -ForegroundColor Yellow
    Write-Host "    1. Open the Data Agent in Fabric portal" -ForegroundColor Yellow
    Write-Host "    2. Add 'HorizonBooksModel' as the data source" -ForegroundColor Yellow
    Write-Host "    3. Configure AI instructions from DataAgent/DataAgentConfiguration.md" -ForegroundColor Yellow
    Write-Host "    4. Add starter questions for each domain" -ForegroundColor Yellow
    Write-Host ""
}
