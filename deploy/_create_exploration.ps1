$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$wsId = "91b2dca3-5729-4e7d-a473-bfeb85c16aa1"
$hdrs = @{Authorization="Bearer $token";"Content-Type"="application/json"}
$smId = "b712621e-924f-4b8a-8de5-b6144646e464"
$folderId = "3ca9bfaf-63a2-44dd-b218-88b541aab37e"

$body = @{
    displayName = "HorizonBooks Exploration"
    type = "Exploration"
    folderId = $folderId
    description = "Self-service exploration based on HorizonBooksModel"
    definition = @{
        parts = @(
            @{
                path = "exploration.json"
                payload = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes((@{
                    version = "1.0"
                    datasetReference = @{
                        byConnection = @{
                            pbiModelDatabaseName = $smId
                            pbiModelVirtualServerName = "sobe_wowvirtualserver"
                            connectionType = "pbiServiceXmlaStyleLive"
                            name = "EntityDataSource"
                        }
                    }
                } | ConvertTo-Json -Depth 5)))
                payloadType = "InlineBase64"
            }
        )
    }
} | ConvertTo-Json -Depth 10

Write-Host "Sending body:"
Write-Host $body
Write-Host ""

try {
    $r = Invoke-WebRequest -UseBasicParsing -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items" -Headers $hdrs -Body $body
    Write-Host "Status: $($r.StatusCode)"
    Write-Host $r.Content
} catch {
    $ex = $_.Exception
    Write-Host "Error: $($ex.Message)"
    if ($ex.Response) {
        $sr = [IO.StreamReader]::new($ex.Response.GetResponseStream())
        Write-Host "Response: $($sr.ReadToEnd())"
        $sr.Close()
    }
}
