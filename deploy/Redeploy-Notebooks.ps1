param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId
)

$ErrorActionPreference = "Stop"
Import-Module (Join-Path $PSScriptRoot 'HorizonBooks.psm1') -Force
$FabricApiBase = $script:FabricApiBase
$scriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$projectRoot = Split-Path -Parent $scriptDir

# Get-Token kept as a thin alias for backward compatibility
function Get-Token { return (Get-FabricToken) }

Write-Host ""
Write-Host "=== Redeploy Notebooks Only ===" -ForegroundColor Cyan

$token = Get-Token
$items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" -Headers @{ Authorization = "Bearer $token" }).value

$bronzeId = ($items | Where-Object { $_.displayName -eq "BronzeLH" -and $_.type -eq "Lakehouse" }).id
$silverId = ($items | Where-Object { $_.displayName -eq "SilverLH" -and $_.type -eq "Lakehouse" }).id
$goldId   = ($items | Where-Object { $_.displayName -eq "GoldLH"   -and $_.type -eq "Lakehouse" }).id

Write-Host "  BronzeLH : $bronzeId"
Write-Host "  SilverLH : $silverId"
Write-Host "  GoldLH   : $goldId"

if (-not $bronzeId -or -not $silverId -or -not $goldId) {
    throw "Could not find all 3 lakehouses in workspace."
}

$notebooks = @(
    @{ Name = "HorizonBooks_01_BronzeToSilver"; File = "01_BronzeToSilver.py"; LhId = $bronzeId; LhName = "BronzeLH" }
    @{ Name = "HorizonBooks_02_WebEnrichment";  File = "02_WebEnrichment.py";  LhId = $silverId; LhName = "SilverLH" }
    @{ Name = "HorizonBooks_03_SilverToGold";   File = "03_SilverToGold.py";   LhId = $goldId;   LhName = "GoldLH" }
    @{ Name = "HorizonBooks_04_Forecasting";    File = "04_Forecasting.py";    LhId = $goldId;   LhName = "GoldLH" }
)

$depPattern = '# META\s+"dependencies":\s*\{[ \t]*\}'
$deployed = 0

function Build-LakehouseMeta {
    param([string]$DefLhId, [string]$DefLhName, [string]$WsId, [string]$BId, [string]$SId, [string]$GId)
    $NL = [char]10
    $lines = @(
        ('# META   "dependencies": {'),
        ('# META     "lakehouse": {'),
        ('# META       "default_lakehouse": "' + $DefLhId + '",'),
        ('# META       "default_lakehouse_name": "' + $DefLhName + '",'),
        ('# META       "default_lakehouse_workspace_id": "' + $WsId + '",'),
        ('# META       "known_lakehouses": ['),
        ('# META         {'),
        ('# META           "id": "' + $BId + '"'),
        ('# META         },'),
        ('# META         {'),
        ('# META           "id": "' + $SId + '"'),
        ('# META         },'),
        ('# META         {'),
        ('# META           "id": "' + $GId + '"'),
        ('# META         }'),
        ('# META       ]'),
        ('# META     }'),
        ('# META   }')
    )
    return $lines -join $NL
}

foreach ($nb in $notebooks) {
    Write-Host ""
    Write-Host "--- $($nb.Name) ---" -ForegroundColor Cyan

    $filePath = Join-Path (Join-Path $projectRoot "notebooks") $nb.File
    if (-not (Test-Path $filePath)) {
        Write-Host "  SKIP: file not found" -ForegroundColor Yellow
        continue
    }

    $raw = Get-Content -Path $filePath -Raw -Encoding UTF8
    $lhMeta = Build-LakehouseMeta -DefLhId $nb.LhId -DefLhName $nb.LhName -WsId $WorkspaceId -BId $bronzeId -SId $silverId -GId $goldId

    if ($raw -match $depPattern) {
        $raw = $raw -replace $depPattern, $lhMeta
    }
    else {
        $srcLines = $raw.Split([char]10)
        $outLines = [System.Collections.Generic.List[string]]::new()
        $inDeps = $false
        $depBrace = 0
        foreach ($srcLine in $srcLines) {
            if ((-not $inDeps) -and ($srcLine -match '# META.*dependencies')) {
                $inDeps = $true
                $depBrace = 0
                foreach ($ch in $srcLine.ToCharArray()) {
                    if ($ch -eq '{') { $depBrace++ }
                    if ($ch -eq '}') { $depBrace-- }
                }
                if ($depBrace -le 0) {
                    $outLines.Add($srcLine)
                    $inDeps = $false
                }
                else {
                    $outLines.Add('# META   "dependencies": {}')
                }
                continue
            }
            if ($inDeps) {
                foreach ($ch in $srcLine.ToCharArray()) {
                    if ($ch -eq '{') { $depBrace++ }
                    if ($ch -eq '}') { $depBrace-- }
                }
                if ($depBrace -le 0) { $inDeps = $false }
                continue
            }
            $outLines.Add($srcLine)
        }
        $raw = $outLines -join ([char]10)
        $raw = $raw -replace $depPattern, $lhMeta
    }

    $raw = $raw -replace "\r\n", "`n"
    $b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($raw))

    # Find existing notebook
    $token = Get-Token
    $hdrs = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

    $allNbs = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Notebook" -Headers @{ Authorization = "Bearer $token" }).value
    $nbId = ($allNbs | Where-Object { $_.displayName -eq $nb.Name } | Select-Object -First 1).id

    if (-not $nbId) {
        Write-Host "  Creating notebook item..."
        $bodyObj = @{ displayName = $nb.Name; type = "Notebook"; description = "Horizon Books" }
        $bodyJson = $bodyObj | ConvertTo-Json -Depth 5
        try {
            $resp = Invoke-WebRequest -Method Post -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" -Headers $hdrs -Body $bodyJson -UseBasicParsing
            $nbId = ($resp.Content | ConvertFrom-Json).id
        }
        catch {
            Write-Host ("  FAILED to create: " + $_.Exception.Message) -ForegroundColor Red
            continue
        }
    }
    else {
        Write-Host "  Existing item: $nbId"
    }

    # Upload definition
    $defJson = '{"definition":{"parts":[{"path":"notebook-content.py","payload":"' + $b64 + '","payloadType":"InlineBase64"}]}}'
    $token = Get-Token
    $hdrs = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

    try {
        $resp = Invoke-WebRequest -Method Post -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$nbId/updateDefinition" -Headers $hdrs -Body $defJson -UseBasicParsing

        if ($resp.StatusCode -eq 200) {
            Write-Host "  OK - definition updated" -ForegroundColor Green
            $deployed++
        }
        elseif ($resp.StatusCode -eq 202) {
            $opUrl = $resp.Headers["Location"]
            if ($opUrl) {
                for ($p = 1; $p -le 30; $p++) {
                    Start-Sleep -Seconds 5
                    $poll = Invoke-RestMethod -Uri $opUrl -Headers @{ Authorization = "Bearer $token" }
                    $secs = $p * 5
                    if ($poll.status -eq "Succeeded") {
                        Write-Host "  OK - updated (LRO ${secs}s)" -ForegroundColor Green
                        $deployed++
                        break
                    }
                    if ($poll.status -eq "Failed") {
                        $detail = $poll | ConvertTo-Json -Depth 3 -Compress
                        Write-Host "  FAILED (LRO): $detail" -ForegroundColor Red
                        break
                    }
                    Write-Host "  Waiting... $($poll.status) (${secs}s)" -ForegroundColor Gray
                }
            }
        }
    }
    catch {
        Write-Host ("  FAILED: " + $_.Exception.Message) -ForegroundColor Red
    }

    Start-Sleep -Seconds 2
}

Write-Host ""
Write-Host "=== $deployed / $($notebooks.Count) notebooks redeployed ===" -ForegroundColor Green
