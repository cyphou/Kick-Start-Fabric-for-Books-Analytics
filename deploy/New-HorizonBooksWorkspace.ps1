<#
.SYNOPSIS
    Creates (or reuses) a Fabric workspace for the Horizon Books demo
    and uploads a custom workspace logo.

.DESCRIPTION
    This script:
    1. Creates a new workspace via the Fabric REST API (idempotent).
    2. Assigns the workspace to a Fabric capacity (F64/Trial).
    3. Uploads a branded logo image to the workspace via the Power BI REST API.
    4. Returns the workspace ID for downstream deployment scripts.

.PARAMETER WorkspaceName
    Display name for the workspace. Defaults to "Horizon Books Analytics".

.PARAMETER CapacityId
    The Fabric capacity (F-SKU) GUID to assign the workspace to.
    If omitted, the workspace is created without capacity assignment
    (assign manually from portal or pass -CapacityId).

.PARAMETER LogoPath
    Path to a PNG image (recommended 200×200 px) for the workspace icon.
    Defaults to assets/workspace-logo.png relative to project root.

.PARAMETER SkipLogo
    If set, skips the logo upload step.

.OUTPUTS
    [PSCustomObject] with WorkspaceId and WorkspaceName properties.

.EXAMPLE
    # Create workspace on a specific capacity
    $ws = .\New-HorizonBooksWorkspace.ps1 -CapacityId "your-capacity-guid"
    .\Deploy-HorizonBooks.ps1 -WorkspaceId $ws.WorkspaceId

.EXAMPLE
    # Reuse existing workspace (idempotent)
    .\New-HorizonBooksWorkspace.ps1 -WorkspaceName "Horizon Books Analytics"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$WorkspaceName = "Horizon Books Analytics",

    [Parameter(Mandatory = $false)]
    [string]$CapacityId,

    [Parameter(Mandatory = $false)]
    [string]$LogoPath,

    [Parameter(Mandatory = $false)]
    [switch]$SkipLogo
)

$ErrorActionPreference = "Stop"

$scriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$projectRoot = Split-Path -Parent $scriptDir

if (-not $LogoPath) {
    $LogoPath = Join-Path $projectRoot "assets\workspace-logo.png"
}

$FabricApiBase = "https://api.fabric.microsoft.com/v1"
$PowerBIApiBase = "https://api.powerbi.com/v1.0/myorg"

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
    try {
        $token = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"
        return $token.Token
    }
    catch {
        Write-Error "Failed to get Fabric API token. Run 'Connect-AzAccount' first."
        throw
    }
}

function Get-PowerBIToken {
    try {
        $token = Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api"
        return $token.Token
    }
    catch {
        Write-Error "Failed to get Power BI token. Run 'Connect-AzAccount' first."
        throw
    }
}

# ============================================================================
# MAIN FLOW
# ============================================================================

Write-Host ""
Write-Host "======================================================" -ForegroundColor Yellow
Write-Host "  Horizon Books - Workspace Setup" -ForegroundColor Yellow
Write-Host "======================================================" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Workspace  : $WorkspaceName"
Write-Host "  Capacity   : $(if ($CapacityId) { $CapacityId } else { '(none - assign manually)' })"
Write-Host "  Logo       : $(if ($SkipLogo) { '(skipped)' } else { $LogoPath })"
Write-Host ""

# ------------------------------------------------------------------
# Step 0: Authenticate
# ------------------------------------------------------------------
Write-Step "Step 0: Authenticating"

$account = Get-AzContext
if (-not $account) {
    Write-Info "No active Azure session. Launching interactive login..."
    Connect-AzAccount | Out-Null
}
else {
    Write-Info "Using Azure session: $($account.Account.Id)"
}

$fabricToken = Get-FabricToken
Write-Success "Authenticated"

# ------------------------------------------------------------------
# Step 1: Create Workspace
# ------------------------------------------------------------------
Write-Step "Step 1: Creating workspace '$WorkspaceName'"

$workspaceId = $null
$wsDescription = "Horizon Books Publishing & Distribution - Microsoft Fabric End-to-End Demo. " +
    "Covers Finance (P&L, Budget), HR (Workforce, Compensation, Recruitment), and " +
    "Operations (Books, Orders, Inventory, Returns). " +
    "18 tables, 25 relationships, 74 DAX measures, 10-page Power BI report."

$createBody = @{
    displayName = $WorkspaceName
    description = $wsDescription
}

# Include capacityId in creation if provided
if ($CapacityId) {
    $createBody["capacityId"] = $CapacityId
}

$createJson = $createBody | ConvertTo-Json -Depth 5
$headers = @{
    "Authorization" = "Bearer $fabricToken"
    "Content-Type"  = "application/json"
}

try {
    $wsResponse = Invoke-WebRequest -Method Post `
        -Uri "$FabricApiBase/workspaces" `
        -Headers $headers `
        -Body $createJson `
        -UseBasicParsing

    $wsObj = $wsResponse.Content | ConvertFrom-Json
    $workspaceId = $wsObj.id
    Write-Success "Workspace created: $workspaceId"
}
catch {
    $errBody = ""
    if ($_.Exception.Response) {
        try {
            $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $errBody = $sr.ReadToEnd(); $sr.Close()
        } catch {}
    }

    if ($errBody -like "*WorkspaceNameAlreadyExists*" -or
        $errBody -like "*already exists*" -or
        $errBody -like "*already in use*") {

        Write-Warn "Workspace '$WorkspaceName' already exists. Looking up ID..."

        # List workspaces and find by name
        $fabricToken = Get-FabricToken
        $wsHeaders = @{ "Authorization" = "Bearer $fabricToken" }
        $allWs = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces" -Headers $wsHeaders).value
        $existingWs = $allWs | Where-Object { $_.displayName -eq $WorkspaceName } | Select-Object -First 1

        if ($existingWs) {
            $workspaceId = $existingWs.id
            Write-Info "Using existing workspace: $workspaceId"
        }
        else {
            Write-Error "Workspace name conflict but could not find it. Check permissions."
            throw
        }
    }
    else {
        Write-Error "Failed to create workspace: $errBody"
        throw
    }
}

# ------------------------------------------------------------------
# Step 2: Assign to Capacity (if not done in creation and CapacityId provided)
# ------------------------------------------------------------------
if ($CapacityId) {
    Write-Step "Step 2: Assigning workspace to capacity"
    Write-Info "Capacity ID: $CapacityId"

    $fabricToken = Get-FabricToken
    $capHeaders = @{
        "Authorization" = "Bearer $fabricToken"
        "Content-Type"  = "application/json"
    }
    $capBody = @{ capacityId = $CapacityId } | ConvertTo-Json

    try {
        $capResp = Invoke-WebRequest -Method Post `
            -Uri "$FabricApiBase/workspaces/$workspaceId/assignToCapacity" `
            -Headers $capHeaders `
            -Body $capBody `
            -UseBasicParsing

        if ($capResp.StatusCode -eq 202) {
            Write-Info "Capacity assignment in progress (202 Accepted)..."
            # Wait briefly for assignment
            Start-Sleep -Seconds 10
        }
        Write-Success "Workspace assigned to capacity $CapacityId"
    }
    catch {
        $capErr = ""
        try {
            $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $capErr = $sr.ReadToEnd(); $sr.Close()
        } catch {}

        if ($capErr -like "*already assigned*") {
            Write-Info "Workspace already assigned to this capacity"
        }
        else {
            Write-Warn "Capacity assignment issue: $capErr"
            Write-Info "You can assign capacity manually from the Fabric portal."
        }
    }
}
else {
    Write-Step "Step 2: Capacity assignment (skipped - no CapacityId provided)"
    Write-Warn "Workspace created without capacity. Assign one from the Fabric portal or re-run with -CapacityId."
}

# ------------------------------------------------------------------
# Step 3: Upload Workspace Logo
# ------------------------------------------------------------------
if (-not $SkipLogo) {
    Write-Step "Step 3: Uploading workspace logo"

    if (-not (Test-Path $LogoPath)) {
        Write-Warn "Logo file not found: $LogoPath"
        Write-Info "Skipping logo upload. Place a PNG at assets/workspace-logo.png and re-run."
    }
    else {
        Write-Info "Logo file: $LogoPath ($(((Get-Item $LogoPath).Length / 1KB).ToString('F1')) KB)"

        # Power BI REST API: Upload custom group image
        # POST https://api.powerbi.com/v1.0/myorg/groups/{groupId}/uploadCustomGroupImage
        # Content-Type: multipart/form-data  (or image/png)
        $pbiToken = Get-PowerBIToken
        $logoBytes = [System.IO.File]::ReadAllBytes($LogoPath)

        $uploadHeaders = @{
            "Authorization" = "Bearer $pbiToken"
            "Content-Type"  = "image/png"
        }

        try {
            Invoke-RestMethod -Method Post `
                -Uri "$PowerBIApiBase/groups/$workspaceId/uploadCustomGroupImage" `
                -Headers $uploadHeaders `
                -Body $logoBytes | Out-Null

            Write-Success "Workspace logo uploaded successfully"
        }
        catch {
            $logoErr = ""
            if ($_.Exception.Response) {
                try {
                    $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                    $logoErr = $sr.ReadToEnd(); $sr.Close()
                } catch {}
            }
            Write-Warn "Logo upload issue: $($_.Exception.Message) $logoErr"
            Write-Info "Logo upload requires workspace Admin role. You can set the image manually from the portal."
        }
    }
}
else {
    Write-Step "Step 3: Logo upload (skipped via -SkipLogo)"
}

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
Write-Host ""
Write-Host "======================================================" -ForegroundColor Green
Write-Host "  WORKSPACE READY" -ForegroundColor Green
Write-Host "======================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Workspace ID   : $workspaceId" -ForegroundColor White
Write-Host "  Workspace Name : $WorkspaceName" -ForegroundColor White
Write-Host "  Portal URL     : https://app.fabric.microsoft.com/groups/$workspaceId" -ForegroundColor Cyan
Write-Host ""
Write-Host "  NEXT STEP:" -ForegroundColor Yellow
Write-Host "  .\Deploy-HorizonBooks.ps1 -WorkspaceId `"$workspaceId`"" -ForegroundColor Yellow
Write-Host ""

# Return useful object for pipeline chaining
[PSCustomObject]@{
    WorkspaceId   = $workspaceId
    WorkspaceName = $WorkspaceName
    CapacityId    = $CapacityId
    PortalUrl     = "https://app.fabric.microsoft.com/groups/$workspaceId"
}
