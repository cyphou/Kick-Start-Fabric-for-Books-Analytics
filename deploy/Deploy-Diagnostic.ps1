param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId
)

$ErrorActionPreference = "Stop"
Import-Module (Join-Path $PSScriptRoot 'HorizonBooks.psm1') -Force
$FabricApiBase = $script:FabricApiBase
$scriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$projectRoot = Split-Path -Parent $scriptDir

function Get-Token { return (Get-FabricToken) }

Write-Host ""
Write-Host "=== Deploy Diagnostic Notebook ===" -ForegroundColor Cyan

$token = Get-Token
$items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" -Headers @{ Authorization = "Bearer $token" }).value

$goldId   = ($items | Where-Object { $_.displayName -eq "GoldLH"   -and $_.type -eq "Lakehouse" }).id
$bronzeId = ($items | Where-Object { $_.displayName -eq "BronzeLH" -and $_.type -eq "Lakehouse" }).id
$silverId = ($items | Where-Object { $_.displayName -eq "SilverLH" -and $_.type -eq "Lakehouse" }).id

Write-Host "  GoldLH   : $goldId"
Write-Host "  BronzeLH : $bronzeId"
Write-Host "  SilverLH : $silverId"

$filePath = Join-Path (Join-Path $projectRoot "notebooks") "05_DiagnosticCheck.py"
$raw = Get-Content -Path $filePath -Raw -Encoding UTF8

# Build lakehouse metadata
$NL = [char]10
$metaLines = @(
    ('# META   "dependencies": {'),
    ('# META     "lakehouse": {'),
    ('# META       "default_lakehouse": "' + $goldId + '",'),
    ('# META       "default_lakehouse_name": "GoldLH",'),
    ('# META       "default_lakehouse_workspace_id": "' + $WorkspaceId + '",'),
    ('# META       "known_lakehouses": ['),
    ('# META         {'),
    ('# META           "id": "' + $bronzeId + '"'),
    ('# META         },'),
    ('# META         {'),
    ('# META           "id": "' + $silverId + '"'),
    ('# META         },'),
    ('# META         {'),
    ('# META           "id": "' + $goldId + '"'),
    ('# META         }'),
    ('# META       ]'),
    ('# META     }'),
    ('# META   }')
)
$lhMeta = $metaLines -join $NL

$raw = $raw -replace '# META\s+"dependencies":\s*\{[ \t]*\}', $lhMeta
$raw = $raw -replace "\r\n", "`n"
$b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($raw))

$nbName = "HorizonBooks_05_DiagnosticCheck"
$hdrs = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

$nbId = ($items | Where-Object { $_.displayName -eq $nbName -and $_.type -eq "Notebook" } | Select-Object -First 1).id

if (-not $nbId) {
    Write-Host "  Creating notebook..."
    $bodyObj = @{ displayName = $nbName; type = "Notebook"; description = "Diagnostic check" }
    $bodyJson = $bodyObj | ConvertTo-Json -Depth 5
    $resp = Invoke-WebRequest -Method Post -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" -Headers $hdrs -Body $bodyJson -UseBasicParsing
    $nbId = ($resp.Content | ConvertFrom-Json).id
    Write-Host "  Created: $nbId" -ForegroundColor Green
}
else {
    Write-Host "  Existing: $nbId"
}

# Upload definition
$defJson = '{"definition":{"parts":[{"path":"notebook-content.py","payload":"' + $b64 + '","payloadType":"InlineBase64"}]}}'
$token = Get-Token
$hdrs = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

$resp = Invoke-WebRequest -Method Post -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$nbId/updateDefinition" -Headers $hdrs -Body $defJson -UseBasicParsing

if ($resp.StatusCode -eq 200) {
    Write-Host "  OK - definition updated" -ForegroundColor Green
}
elseif ($resp.StatusCode -eq 202) {
    $opUrl = $resp.Headers["Location"]
    for ($p = 1; $p -le 20; $p++) {
        Start-Sleep -Seconds 5
        $poll = Invoke-RestMethod -Uri $opUrl -Headers @{ Authorization = "Bearer $token" }
        $secs = $p * 5
        if ($poll.status -eq "Succeeded") {
            Write-Host "  OK - deployed (LRO ${secs}s)" -ForegroundColor Green
            break
        }
        if ($poll.status -eq "Failed") {
            Write-Host "  FAILED" -ForegroundColor Red
            break
        }
    }
}

Write-Host ""
Write-Host "=== Diagnostic notebook deployed ===" -ForegroundColor Green
Write-Host "  Run 'HorizonBooks_05_DiagnosticCheck' in Fabric to check Gold tables."
