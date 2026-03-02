# Update-DataflowDestinations.ps1
# Updates all 3 Horizon Books dataflows with Lakehouse data destinations
# Uses global TargetWorkspaceId/TargetLakehouseId parameters and _Target navigation queries

param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId
)

$ErrorActionPreference = "Stop"
Import-Module (Join-Path $PSScriptRoot 'HorizonBooks.psm1') -Force

# Get IDs
$fabricToken = Get-FabricToken
$apiBase = $script:FabricApiBase
$headers = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }

# Find Lakehouse and Dataflow IDs
$items = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers @{"Authorization"="Bearer $fabricToken"}).value
$bronzeLh = $items | Where-Object { $_.displayName -eq "BronzeLH" -and $_.type -eq "Lakehouse" }
$bronzeLhId = $bronzeLh.id
Write-Host "BronzeLH: $bronzeLhId"

$dataflows = @{
    "HorizonBooks_DF_Finance" = $null
    "HorizonBooks_DF_HR" = $null
    "HorizonBooks_DF_Operations" = $null
}

foreach ($df in ($items | Where-Object { $_.type -eq "Dataflow" })) {
    if ($dataflows.ContainsKey($df.displayName)) {
        $dataflows[$df.displayName] = $df.id
        Write-Host "$($df.displayName): $($df.id)"
    }
}

# ── Helper: Generate global target parameter block ──
function Get-TargetParamBlock {
    param([string]$WsId, [string]$LhId)
    return @"
// -- Global Target Parameters --
shared TargetWorkspaceId = "$WsId" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];
shared TargetLakehouseId = "$LhId" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];
"@
}

# ── Helper: Generate destination + source query pair ──
function Get-QueryPair {
    param([string]$TableName, [string]$ColumnTypeDefs)

    $dest = @"
shared ${TableName}_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "$TableName", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "${TableName}_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared $TableName = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="${TableName}.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
$ColumnTypeDefs
    }),
    RemovedDuplicates = Table.Distinct(ChangedType),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;
"@
    return $dest
}

# ── Finance tables ──
$financeQueries = @()
$financeQueries += Get-QueryPair -TableName "DimAccounts" -ColumnTypeDefs @'
        {"AccountID", type text},
        {"AccountName", type text},
        {"AccountType", type text},
        {"AccountCategory", type text},
        {"ParentAccountID", type text},
        {"IsActive", type logical}
'@
$financeQueries += Get-QueryPair -TableName "DimCostCenters" -ColumnTypeDefs @'
        {"CostCenterID", type text},
        {"CostCenterName", type text},
        {"Department", type text},
        {"DivisionHead", type text}
'@
$financeQueries += Get-QueryPair -TableName "FactFinancialTransactions" -ColumnTypeDefs @'
        {"TransactionID", type text},
        {"TransactionDate", type date},
        {"AccountID", type text},
        {"BookID", type text},
        {"Amount", type number},
        {"Currency", type text},
        {"TransactionType", type text},
        {"FiscalYear", type text},
        {"FiscalQuarter", type text},
        {"FiscalMonth", type text},
        {"CostCenterID", type text},
        {"Description", type text}
'@
$financeQueries += Get-QueryPair -TableName "FactBudget" -ColumnTypeDefs @'
        {"BudgetID", type text},
        {"FiscalYear", type text},
        {"FiscalQuarter", type text},
        {"FiscalMonth", type text},
        {"AccountID", type text},
        {"CostCenterID", type text},
        {"BudgetAmount", type number},
        {"ActualAmount", type number},
        {"Variance", type number},
        {"VariancePct", type number}
'@
$paramBlock = Get-TargetParamBlock -WsId $WorkspaceId -LhId $bronzeLhId
$financeMashup = "section Section1;`n`n" + $paramBlock + "`n`n" + ($financeQueries -join "`n")
$financeTables = @("DimAccounts", "DimCostCenters", "FactFinancialTransactions", "FactBudget")

# ── HR tables ──
$hrQueries = @()
$hrQueries += Get-QueryPair -TableName "DimEmployees" -ColumnTypeDefs @'
        {"EmployeeID", type text},
        {"FirstName", type text},
        {"LastName", type text},
        {"Email", type text},
        {"HireDate", type date},
        {"DepartmentID", type text},
        {"JobTitle", type text},
        {"ManagerID", type text},
        {"EmploymentType", type text},
        {"Location", type text},
        {"GeoID", type text},
        {"IsActive", type logical}
'@
$hrQueries += Get-QueryPair -TableName "DimDepartments" -ColumnTypeDefs @'
        {"DepartmentID", type text},
        {"DepartmentName", type text},
        {"DepartmentHead", type text},
        {"HeadCount", Int64.Type},
        {"AnnualBudget", type number},
        {"Location", type text}
'@
$hrQueries += Get-QueryPair -TableName "FactPayroll" -ColumnTypeDefs @'
        {"PayrollID", type text},
        {"EmployeeID", type text},
        {"PayPeriodStart", type date},
        {"PayPeriodEnd", type date},
        {"BaseSalary", type number},
        {"Bonus", type number},
        {"Overtime", type number},
        {"Deductions", type number},
        {"NetPay", type number},
        {"PayDate", type date}
'@
$hrQueries += Get-QueryPair -TableName "FactPerformanceReviews" -ColumnTypeDefs @'
        {"ReviewID", type text},
        {"EmployeeID", type text},
        {"ReviewDate", type date},
        {"ReviewerID", type text},
        {"PerformanceRating", type text},
        {"GoalsMet", type text},
        {"Strengths", type text},
        {"AreasForImprovement", type text},
        {"OverallScore", type number}
'@
$hrQueries += Get-QueryPair -TableName "FactRecruitment" -ColumnTypeDefs @'
        {"RequisitionID", type text},
        {"DepartmentID", type text},
        {"JobTitle", type text},
        {"OpenDate", type date},
        {"CloseDate", type date},
        {"Status", type text},
        {"ApplicationsReceived", Int64.Type},
        {"Interviewed", Int64.Type},
        {"OffersExtended", Int64.Type},
        {"OfferAccepted", Int64.Type},
        {"HiringManagerID", type text},
        {"SalaryRangeMin", type number},
        {"SalaryRangeMax", type number},
        {"TimeToFillDays", type text}
'@
$hrMashup = "section Section1;`n`n" + $paramBlock + "`n`n" + ($hrQueries -join "`n")
$hrTables = @("DimEmployees", "DimDepartments", "FactPayroll", "FactPerformanceReviews", "FactRecruitment")

# ── Operations tables ──
$opsQueries = @()
$opsQueries += Get-QueryPair -TableName "DimBooks" -ColumnTypeDefs @'
        {"BookID", type text},
        {"Title", type text},
        {"AuthorID", type text},
        {"Genre", type text},
        {"SubGenre", type text},
        {"ISBN", type text},
        {"PublishDate", type date},
        {"ListPrice", type number},
        {"Format", type text},
        {"PageCount", Int64.Type},
        {"PrintRunSize", Int64.Type},
        {"ImprintName", type text},
        {"Status", type text}
'@
$opsQueries += Get-QueryPair -TableName "DimAuthors" -ColumnTypeDefs @'
        {"AuthorID", type text},
        {"FirstName", type text},
        {"LastName", type text},
        {"PenName", type text},
        {"AgentName", type text},
        {"AgentCompany", type text},
        {"ContractStartDate", type date},
        {"ContractEndDate", type date},
        {"RoyaltyRate", type number},
        {"AdvanceAmount", type number},
        {"Genre", type text},
        {"Nationality", type text},
        {"BookCount", Int64.Type}
'@
$opsQueries += Get-QueryPair -TableName "DimCustomers" -ColumnTypeDefs @'
        {"CustomerID", type text},
        {"CustomerName", type text},
        {"CustomerType", type text},
        {"ContactEmail", type text},
        {"City", type text},
        {"State", type text},
        {"Country", type text},
        {"Region", type text},
        {"GeoID", type text},
        {"CreditLimit", type number},
        {"PaymentTerms", type text},
        {"IsActive", type logical},
        {"AccountOpenDate", type date}
'@
$opsQueries += Get-QueryPair -TableName "DimGeography" -ColumnTypeDefs @'
        {"GeoID", type text},
        {"City", type text},
        {"StateProvince", type text},
        {"Country", type text},
        {"Continent", type text},
        {"Region", type text},
        {"SubRegion", type text},
        {"Latitude", type number},
        {"Longitude", type number},
        {"TimeZone", type text},
        {"Currency", type text},
        {"Population", Int64.Type},
        {"IsCapital", type logical}
'@
$opsQueries += Get-QueryPair -TableName "DimWarehouses" -ColumnTypeDefs @'
        {"WarehouseID", type text},
        {"WarehouseName", type text},
        {"Address", type text},
        {"City", type text},
        {"State", type text},
        {"Country", type text},
        {"SquareFootage", Int64.Type},
        {"MaxCapacityUnits", Int64.Type},
        {"CurrentUtilization", type number},
        {"ManagerID", type text},
        {"MonthlyRent", type number},
        {"IsActive", type logical}
'@
$opsQueries += Get-QueryPair -TableName "FactOrders" -ColumnTypeDefs @'
        {"OrderID", type text},
        {"OrderDate", type date},
        {"CustomerID", type text},
        {"BookID", type text},
        {"Quantity", Int64.Type},
        {"UnitPrice", type number},
        {"Discount", type number},
        {"TotalAmount", type number},
        {"OrderStatus", type text},
        {"ShipDate", type date},
        {"DeliveryDate", type date},
        {"WarehouseID", type text},
        {"SalesRepID", type text},
        {"Channel", type text}
'@
$opsQueries += Get-QueryPair -TableName "FactInventory" -ColumnTypeDefs @'
        {"InventoryID", type text},
        {"BookID", type text},
        {"WarehouseID", type text},
        {"SnapshotDate", type date},
        {"QuantityOnHand", Int64.Type},
        {"QuantityReserved", Int64.Type},
        {"QuantityAvailable", Int64.Type},
        {"ReorderPoint", Int64.Type},
        {"ReorderQuantity", Int64.Type},
        {"UnitCost", type number},
        {"TotalInventoryValue", type number},
        {"DaysOfSupply", Int64.Type},
        {"Status", type text}
'@
$opsQueries += Get-QueryPair -TableName "FactReturns" -ColumnTypeDefs @'
        {"ReturnID", type text},
        {"OrderID", type text},
        {"BookID", type text},
        {"CustomerID", type text},
        {"ReturnDate", type date},
        {"Quantity", Int64.Type},
        {"Reason", type text},
        {"ReturnStatus", type text},
        {"RefundAmount", type number},
        {"Condition", type text},
        {"RestockStatus", type text}
'@
$opsMashup = "section Section1;`n`n" + $paramBlock + "`n`n" + ($opsQueries -join "`n")
$opsTables = @("DimBooks", "DimAuthors", "DimCustomers", "DimGeography", "DimWarehouses", "FactOrders", "FactInventory", "FactReturns")

# ── Deploy function ──
function Update-DataflowWithDestinations {
    param(
        [string]$Name,
        [string]$DfId,
        [string]$Mashup,
        [string[]]$Tables
    )

    $fabricToken = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
    $mashupBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Mashup))

    $queriesMetadata = @{}

    # Global target parameter query metadata (hidden)
    $queriesMetadata["TargetWorkspaceId"] = @{
        queryId      = [guid]::NewGuid().ToString()
        queryName    = "TargetWorkspaceId"
        queryGroupId = $null
        isHidden     = $true
        loadEnabled  = $false
    }
    $queriesMetadata["TargetLakehouseId"] = @{
        queryId      = [guid]::NewGuid().ToString()
        queryName    = "TargetLakehouseId"
        queryGroupId = $null
        isHidden     = $true
        loadEnabled  = $false
    }

    foreach ($t in $Tables) {
        $queriesMetadata[$t] = @{
            queryId = [guid]::NewGuid().ToString()
            queryName = $t
            queryGroupId = $null
            isHidden = $false
            loadEnabled = $true
            destinationSettings = @{ enableStaging = $false }
        }
        $destName = "${t}_Target"
        $queriesMetadata[$destName] = @{
            queryId = [guid]::NewGuid().ToString()
            queryName = $destName
            queryGroupId = $null
            isHidden = $true
            loadEnabled = $false
        }
    }

    $queryMeta = @{
        formatVersion = "202502"
        computeEngineSettings = @{ allowFastCopy = $true; maxConcurrency = 1 }
        name = $Name
        queryGroups = @()
        documentLocale = "en-US"
        queriesMetadata = $queriesMetadata
        fastCombine = $false
        allowNativeQueries = $true
        skipAutomaticTypeAndHeaderDetection = $false
    } | ConvertTo-Json -Depth 10
    $queryMetaBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($queryMeta))

    $defJson = '{"definition":{"parts":[{"path":"queryMetadata.json","payload":"' + $queryMetaBase64 + '","payloadType":"InlineBase64"},{"path":"mashup.pq","payload":"' + $mashupBase64 + '","payloadType":"InlineBase64"}]}}'

    Write-Host "Updating $Name ($DfId) with $($Tables.Count) tables + destinations..."

    try {
        $r = Invoke-WebRequest -Method Post `
            -Uri "$apiBase/workspaces/$WorkspaceId/dataflows/$DfId/updateDefinition" `
            -Headers @{"Authorization"="Bearer $fabricToken";"Content-Type"="application/json"} `
            -Body $defJson -UseBasicParsing

        if ($r.StatusCode -eq 200) {
            Write-Host "  OK: $Name updated (HTTP 200)"
            return $true
        }
        if ($r.StatusCode -eq 202) {
            $opUrl = $r.Headers["Location"]
            for ($i = 1; $i -le 24; $i++) {
                Start-Sleep -Seconds 5
                $poll = Invoke-RestMethod -Uri $opUrl -Headers @{Authorization = "Bearer $fabricToken"}
                if ($poll.status -eq "Succeeded") { Write-Host "  OK: $Name updated (LRO)"; return $true }
                if ($poll.status -eq "Failed") { Write-Host "  FAILED: $Name LRO failed"; return $false }
            }
        }
    } catch {
        $sc = $_.Exception.Response.StatusCode.Value__
        Write-Host "  ERROR: $Name - HTTP $sc"
        try {
            $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $sr.BaseStream.Position = 0
            Write-Host "  Body: $($sr.ReadToEnd())"
        } catch {}
        return $false
    }
    return $false
}

# ── Execute updates ──
$results = @{}

$results["Finance"] = Update-DataflowWithDestinations `
    -Name "HorizonBooks_DF_Finance" `
    -DfId $dataflows["HorizonBooks_DF_Finance"] `
    -Mashup $financeMashup `
    -Tables $financeTables

$results["HR"] = Update-DataflowWithDestinations `
    -Name "HorizonBooks_DF_HR" `
    -DfId $dataflows["HorizonBooks_DF_HR"] `
    -Mashup $hrMashup `
    -Tables $hrTables

$results["Operations"] = Update-DataflowWithDestinations `
    -Name "HorizonBooks_DF_Operations" `
    -DfId $dataflows["HorizonBooks_DF_Operations"] `
    -Mashup $opsMashup `
    -Tables $opsTables

Write-Host ""
Write-Host "=== Results ==="
$results.GetEnumerator() | ForEach-Object { Write-Host "  $($_.Key): $(if ($_.Value) { 'SUCCESS' } else { 'FAILED' })" }
