$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$ws = "91b2dca3-5729-4e7d-a473-bfeb85c16aa1"
$smId = "b712621e-924f-4b8a-8de5-b6144646e464"
$folderId = "3ca9bfaf-63a2-44dd-b218-88b541aab37e"
$url = "https://api.fabric.microsoft.com/v1/workspaces/$ws/explorations"

function TryPost($label, $bodyStr) {
    Write-Host "--- $label ---"
    Write-Host "Body: $bodyStr"
    try {
        $r = Invoke-WebRequest -Uri $url -Headers @{Authorization="Bearer $token"; "Content-Type"="application/json"} -Method Post -Body $bodyStr -UseBasicParsing -ErrorAction Stop
        Write-Host "OK: $($r.StatusCode) $($r.Content)"
        return $true
    } catch {
        $status = $_.Exception.Response.StatusCode.value__
        $errBody = ""
        try { $stream = $_.Exception.Response.GetResponseStream(); $reader = New-Object System.IO.StreamReader($stream); $reader.BaseStream.Position = 0; $errBody = $reader.ReadToEnd() } catch {}
        Write-Host "HTTP $status : $errBody`n"
        return $false
    }
}

# Try different body formats
$attempts = @(
    @{ label = "1: displayName only"; body = '{"displayName":"HorizonBooks Exploration"}' },
    @{ label = "2: displayName + description"; body = '{"displayName":"HorizonBooks Exploration","description":"Exploration"}' },
    @{ label = "3: displayName + artifactObjectId (SM)"; body = '{"displayName":"HorizonBooks Exploration","artifactObjectId":"' + $smId + '"}' },
    @{ label = "4: displayName + semanticModelId"; body = '{"displayName":"HorizonBooks Exploration","semanticModelId":"' + $smId + '"}' },
    @{ label = "5: displayName + modelId"; body = '{"displayName":"HorizonBooks Exploration","modelId":"' + $smId + '"}' },
    @{ label = "6: displayName + datasetId"; body = '{"displayName":"HorizonBooks Exploration","datasetId":"' + $smId + '"}' }
)

foreach ($a in $attempts) {
    $ok = TryPost $a.label $a.body
    if ($ok) { break }
}
