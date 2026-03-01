<#
.SYNOPSIS
    Creates Dataflow Gen2 items and a Data Pipeline for Horizon Books orchestration.

.DESCRIPTION
    Deploys the orchestration layer for the Horizon Books 3-Lakehouse Medallion demo:
    1. Creates 3 Dataflow Gen2 items (DF_Finance, DF_HR, DF_Operations) to load
       CSV files from BronzeLH Files/ into Bronze tables
    2. Creates 1 Data Pipeline (PL_HorizonBooks_Orchestration) that orchestrates:
       - Phase 1: Run 3 Dataflows in parallel (CSV → Bronze tables)
       - Phase 2: Run NB01 BronzeToSilver (Bronze tables → SilverLH with transforms)
       - Phase 3: Run NB02 WebEnrichment (external API data → SilverLH)
       - Phase 4: Run NB03 SilverToGold (Silver → Gold star schema)
       - Phase 5: Run NB04 Forecasting (Holt-Winters forecasts on Gold data)

    The pipeline provides a visual, schedulable orchestration of the full ETL process.
    Dataflows handle the initial CSV ingestion using Power Query (M language),
    NB01 reads Bronze tables (loaded by DFs) and writes to SilverLH,
    and downstream Notebooks handle the complex PySpark transformations.

.PARAMETER WorkspaceId
    The GUID of the Fabric workspace.

.PARAMETER LakehouseId
    The GUID of the BronzeLH Lakehouse (source of CSV files and target for raw tables).

.PARAMETER LakehouseName
    Name of the Bronze Lakehouse. Defaults to BronzeLH.

.EXAMPLE
    .\Deploy-Pipeline.ps1 -WorkspaceId "ws-guid" -LakehouseId "bronze-lh-guid"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,

    [Parameter(Mandatory = $true)]
    [string]$LakehouseId,

    [Parameter(Mandatory = $false)]
    [string]$LakehouseName = "BronzeLH"
)

$ErrorActionPreference = "Stop"
$FabricApiBase = "https://api.fabric.microsoft.com/v1"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

function Write-Step  { param([string]$M) Write-Host "`n$('='*70)" -ForegroundColor Cyan; Write-Host " $M" -ForegroundColor Cyan; Write-Host "$('='*70)" -ForegroundColor Cyan }
function Write-Info  { param([string]$M) Write-Host "  [INFO] $M" -ForegroundColor Gray }
function Write-OK    { param([string]$M) Write-Host "  [OK]   $M" -ForegroundColor Green }
function Write-Warn  { param([string]$M) Write-Host "  [WARN] $M" -ForegroundColor Yellow }

function Get-FabricToken {
    try {
        $tok = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"
        return $tok.Token
    }
    catch {
        Write-Error "Failed to get Fabric API token. Run 'Connect-AzAccount' first."
        throw
    }
}

function New-OrGetFabricItem {
    <#
    .SYNOPSIS
        Creates a Fabric item or retrieves it if it already exists.
        Returns the item ID.
    .NOTES
        Fabric REST API type mapping:
          - Dataflow Gen2: create with type "Dataflow"
          - DataPipeline:  same name for create and list
    #>
    param(
        [string]$DisplayName,
        [string]$Type,
        [string]$Description,
        [string]$WsId,
        [string]$Token
    )

    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }

    # Map the create-API type to the list-API type (they can differ)
    $listType = $Type

    # --- Step 1: Check if item already exists (GET before POST) ---
    try {
        $existingItems = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WsId/items?type=$listType" `
            -Headers @{Authorization = "Bearer $Token"}).value
        $existing = $existingItems | Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1
        if ($existing) {
            Write-Info "'$DisplayName' already exists - reusing ($($existing.id))"
            return $existing.id
        }
    }
    catch {
        Write-Info "Could not list existing items, will attempt create..."
    }

    # --- Step 2: Create new item ---
    $body = @{
        displayName = $DisplayName
        type        = $Type
        description = $Description
    } | ConvertTo-Json -Depth 5

    $itemId = $null
    try {
        $resp = Invoke-WebRequest -Method Post -Uri "$FabricApiBase/workspaces/$WsId/items" `
            -Headers $headers -Body $body -UseBasicParsing

        if ($resp.StatusCode -eq 201) {
            $obj = $resp.Content | ConvertFrom-Json
            $itemId = $obj.id
        }
        elseif ($resp.StatusCode -eq 202) {
            # LRO - poll for completion
            $opUrl = $resp.Headers["Location"]
            if ($opUrl) {
                for ($p = 1; $p -le 24; $p++) {
                    Start-Sleep -Seconds 5
                    $poll = Invoke-RestMethod -Uri $opUrl -Headers @{Authorization = "Bearer $Token"}
                    Write-Info "  LRO: $($poll.status) ($($p*5)s)"
                    if ($poll.status -eq "Succeeded") { break }
                    if ($poll.status -eq "Failed") { Write-Warn "LRO failed"; break }
                }
            }
            Start-Sleep -Seconds 3
            # Look up the newly created item
            $items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WsId/items?type=$listType" `
                -Headers @{Authorization = "Bearer $Token"}).value
            $found = $items | Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1
            if ($found) { $itemId = $found.id }
        }
    }
    catch {
        # Try to read the error body from the response stream
        $errBody = ""
        try {
            if ($_.Exception.Response) {
                $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $errBody = $sr.ReadToEnd(); $sr.Close()
            }
        } catch {}

        # Also check ErrorDetails (PS 5.1 sometimes populates this)
        if (-not $errBody -and $_.ErrorDetails -and $_.ErrorDetails.Message) {
            $errBody = $_.ErrorDetails.Message
        }

        $errMsg = "$($_.Exception.Message) $errBody"

        if ($errMsg -like "*ItemDisplayNameAlreadyInUse*" -or $errMsg -like "*already in use*") {
            Write-Info "'$DisplayName' already exists - looking up..."
            try {
                $items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WsId/items?type=$listType" `
                    -Headers @{Authorization = "Bearer $Token"}).value
                $found = $items | Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1
                if ($found) { $itemId = $found.id }
            } catch {}
        }
        else {
            Write-Warn "Create $Type '$DisplayName' error: $errMsg"
        }
    }

    return $itemId
}

function Update-FabricItemDefinition {
    <#
    .SYNOPSIS
        Updates the definition of a Fabric item using the updateDefinition API.
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
                        $poll = Invoke-RestMethod -Uri $opUrl -Headers @{Authorization = "Bearer $Token"}
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

function Update-PipelineDefinition {
    <#
    .SYNOPSIS
        Updates a Data Pipeline definition using the dedicated /dataPipelines/ endpoint.
        This endpoint is used by SSISToFabric and is the canonical way to update pipelines.
    #>
    param(
        [string]$PipelineId,
        [string]$WsId,
        [string]$DefinitionJson,
        [string]$Token
    )

    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }

    for ($attempt = 1; $attempt -le 3; $attempt++) {
        if ($attempt -gt 1) {
            Write-Info "Pipeline definition update retry $attempt/3 - waiting 10s..."
            Start-Sleep -Seconds 10
            $Token = Get-FabricToken
            $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
        }
        try {
            $resp = Invoke-WebRequest -Method Post `
                -Uri "$FabricApiBase/workspaces/$WsId/dataPipelines/$PipelineId/updateDefinition" `
                -Headers $headers -Body $DefinitionJson -UseBasicParsing

            if ($resp.StatusCode -eq 200) { return $true }
            if ($resp.StatusCode -eq 202) {
                $opUrl = $resp.Headers["Location"]
                if ($opUrl) {
                    for ($p = 1; $p -le 24; $p++) {
                        Start-Sleep -Seconds 5
                        $poll = Invoke-RestMethod -Uri $opUrl -Headers @{Authorization = "Bearer $Token"}
                        Write-Info "  Pipeline definition LRO: $($poll.status) ($($p*5)s)"
                        if ($poll.status -eq "Succeeded") { return $true }
                        if ($poll.status -eq "Failed") { Write-Warn "Pipeline definition LRO failed"; return $false }
                    }
                }
            }
        }
        catch {
            Write-Warn "Pipeline definition update error (attempt $attempt): $($_.Exception.Message)"
        }
    }
    return $false
}

function Update-DataflowDefinition {
    <#
    .SYNOPSIS
        Updates a Dataflow definition using the dedicated /dataflows/ endpoint.
        This endpoint correctly persists queriesMetadata unlike the generic /items/ endpoint.
    #>
    param(
        [string]$DataflowId,
        [string]$WsId,
        [string]$DefinitionJson,
        [string]$Token
    )

    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }

    for ($attempt = 1; $attempt -le 3; $attempt++) {
        if ($attempt -gt 1) {
            Write-Info "Dataflow definition update retry $attempt/3 - waiting 10s..."
            Start-Sleep -Seconds 10
            $Token = Get-FabricToken
            $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
        }
        try {
            $resp = Invoke-WebRequest -Method Post `
                -Uri "$FabricApiBase/workspaces/$WsId/dataflows/$DataflowId/updateDefinition" `
                -Headers $headers -Body $DefinitionJson -UseBasicParsing

            if ($resp.StatusCode -eq 200) { return $true }
            if ($resp.StatusCode -eq 202) {
                $opUrl = $resp.Headers["Location"]
                if ($opUrl) {
                    for ($p = 1; $p -le 24; $p++) {
                        Start-Sleep -Seconds 5
                        $poll = Invoke-RestMethod -Uri $opUrl -Headers @{Authorization = "Bearer $Token"}
                        Write-Info "  Dataflow definition LRO: $($poll.status) ($($p*5)s)"
                        if ($poll.status -eq "Succeeded") { return $true }
                        if ($poll.status -eq "Failed") { Write-Warn "Dataflow definition LRO failed"; return $false }
                    }
                }
            }
        }
        catch {
            Write-Warn "Dataflow definition update error (attempt $attempt): $($_.Exception.Message)"
        }
    }
    return $false
}

# ============================================================================
# DATAFLOW GEN2 DEFINITIONS (Power Query M Language)
# ============================================================================
# Each dataflow reads CSV files from Lakehouse Files and loads into tables.
# The M queries use the Lakehouse connector with workspace/lakehouse IDs.
#
# NOTE: Static copies of all definitions are in definitions/ for CI/CD reference.

function Get-DataflowMashup {
    <#
    .SYNOPSIS
        Generates a Power Query (M Language) mashup document for a set of CSV tables.
        Each query reads a CSV file from the Lakehouse Files folder and types the columns.
        Includes global TargetWorkspaceId/TargetLakehouseId parameters and _Target
        navigation queries so the dataflow automatically writes output to BronzeLH tables.
    #>
    param(
        [string]$Domain,       # Finance, HR, or Operations
        [array]$TableDefs      # Array of @{ Name; Folder; Columns = @(@{Name;Type}) }
    )

    # Global target parameters — centralized connection settings
    $paramBlock = @"
// -- Global Target Parameters --
// These are replaced at deployment time by Deploy-Pipeline.ps1 or Update-DataflowDestinations.ps1
shared TargetWorkspaceId = "$WorkspaceId" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];
shared TargetLakehouseId = "$LakehouseId" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];
"@

    $queries = @()
    foreach ($td in $TableDefs) {
        $tableName = $td.Name
        $folder = $td.Folder
        $fileName = "$tableName.csv"

        # Build column type transformations
        $typeEntries = @()
        foreach ($c in $td.Columns) {
            $mType = switch ($c.Type) {
                "Int64"    { "Int64.Type" }
                "Double"   { "type number" }
                "Date"     { "type date" }
                "DateTime" { "type datetime" }
                "Boolean"  { "type logical" }
                default    { "type text" }
            }
            $typeEntries += "        {`"$($c.Name)`", $mType}"
        }
        $typeList = $typeEntries -join ",`n"

        # Target navigation query — points to the Lakehouse table destination
        $destQuery = @"
shared ${tableName}_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "$tableName", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;
"@

        # Source query with DataDestinations attribute referencing _Target query
        $query = @"
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "${tableName}_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared $tableName = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="$fileName"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
$typeList
    }),
    RemovedDuplicates = Table.Distinct(ChangedType),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;
"@
        $queries += $destQuery
        $queries += $query
    }

    $mashup = "section Section1;`n`n" + $paramBlock + "`n`n" + ($queries -join "`n")
    return $mashup
}

# ── Finance Domain Tables ──
$FinanceTables = @(
    @{
        Name = "DimAccounts"; Folder = "Finance"
        Columns = @(
            @{ Name = "AccountID"; Type = "Text" },
            @{ Name = "AccountName"; Type = "Text" },
            @{ Name = "AccountType"; Type = "Text" },
            @{ Name = "AccountCategory"; Type = "Text" },
            @{ Name = "ParentAccountID"; Type = "Text" },
            @{ Name = "IsActive"; Type = "Boolean" }
        )
    },
    @{
        Name = "DimCostCenters"; Folder = "Finance"
        Columns = @(
            @{ Name = "CostCenterID"; Type = "Text" },
            @{ Name = "CostCenterName"; Type = "Text" },
            @{ Name = "Department"; Type = "Text" },
            @{ Name = "DivisionHead"; Type = "Text" }
        )
    },
    @{
        Name = "FactFinancialTransactions"; Folder = "Finance"
        Columns = @(
            @{ Name = "TransactionID"; Type = "Text" },
            @{ Name = "TransactionDate"; Type = "Date" },
            @{ Name = "AccountID"; Type = "Text" },
            @{ Name = "BookID"; Type = "Text" },
            @{ Name = "Amount"; Type = "Double" },
            @{ Name = "Currency"; Type = "Text" },
            @{ Name = "TransactionType"; Type = "Text" },
            @{ Name = "FiscalYear"; Type = "Text" },
            @{ Name = "FiscalQuarter"; Type = "Text" },
            @{ Name = "FiscalMonth"; Type = "Text" },
            @{ Name = "CostCenterID"; Type = "Text" },
            @{ Name = "Description"; Type = "Text" }
        )
    },
    @{
        Name = "FactBudget"; Folder = "Finance"
        Columns = @(
            @{ Name = "BudgetID"; Type = "Text" },
            @{ Name = "FiscalYear"; Type = "Text" },
            @{ Name = "FiscalQuarter"; Type = "Text" },
            @{ Name = "FiscalMonth"; Type = "Text" },
            @{ Name = "AccountID"; Type = "Text" },
            @{ Name = "CostCenterID"; Type = "Text" },
            @{ Name = "BudgetAmount"; Type = "Double" },
            @{ Name = "ActualAmount"; Type = "Double" },
            @{ Name = "Variance"; Type = "Double" },
            @{ Name = "VariancePct"; Type = "Double" }
        )
    }
)

# ── HR Domain Tables ──
$HRTables = @(
    @{
        Name = "DimEmployees"; Folder = "HR"
        Columns = @(
            @{ Name = "EmployeeID"; Type = "Text" },
            @{ Name = "FirstName"; Type = "Text" },
            @{ Name = "LastName"; Type = "Text" },
            @{ Name = "Email"; Type = "Text" },
            @{ Name = "HireDate"; Type = "Date" },
            @{ Name = "DepartmentID"; Type = "Text" },
            @{ Name = "JobTitle"; Type = "Text" },
            @{ Name = "ManagerID"; Type = "Text" },
            @{ Name = "EmploymentType"; Type = "Text" },
            @{ Name = "Location"; Type = "Text" },
            @{ Name = "GeoID"; Type = "Text" },
            @{ Name = "IsActive"; Type = "Boolean" }
        )
    },
    @{
        Name = "DimDepartments"; Folder = "HR"
        Columns = @(
            @{ Name = "DepartmentID"; Type = "Text" },
            @{ Name = "DepartmentName"; Type = "Text" },
            @{ Name = "DepartmentHead"; Type = "Text" },
            @{ Name = "HeadCount"; Type = "Int64" },
            @{ Name = "AnnualBudget"; Type = "Double" },
            @{ Name = "Location"; Type = "Text" }
        )
    },
    @{
        Name = "FactPayroll"; Folder = "HR"
        Columns = @(
            @{ Name = "PayrollID"; Type = "Text" },
            @{ Name = "EmployeeID"; Type = "Text" },
            @{ Name = "PayPeriodStart"; Type = "Date" },
            @{ Name = "PayPeriodEnd"; Type = "Date" },
            @{ Name = "BaseSalary"; Type = "Double" },
            @{ Name = "Bonus"; Type = "Double" },
            @{ Name = "Overtime"; Type = "Double" },
            @{ Name = "Deductions"; Type = "Double" },
            @{ Name = "NetPay"; Type = "Double" },
            @{ Name = "PayDate"; Type = "Date" }
        )
    },
    @{
        Name = "FactPerformanceReviews"; Folder = "HR"
        Columns = @(
            @{ Name = "ReviewID"; Type = "Text" },
            @{ Name = "EmployeeID"; Type = "Text" },
            @{ Name = "ReviewDate"; Type = "Date" },
            @{ Name = "ReviewerID"; Type = "Text" },
            @{ Name = "PerformanceRating"; Type = "Text" },
            @{ Name = "GoalsMet"; Type = "Text" },
            @{ Name = "Strengths"; Type = "Text" },
            @{ Name = "AreasForImprovement"; Type = "Text" },
            @{ Name = "OverallScore"; Type = "Double" }
        )
    },
    @{
        Name = "FactRecruitment"; Folder = "HR"
        Columns = @(
            @{ Name = "RequisitionID"; Type = "Text" },
            @{ Name = "DepartmentID"; Type = "Text" },
            @{ Name = "JobTitle"; Type = "Text" },
            @{ Name = "OpenDate"; Type = "Date" },
            @{ Name = "CloseDate"; Type = "Date" },
            @{ Name = "Status"; Type = "Text" },
            @{ Name = "ApplicationsReceived"; Type = "Int64" },
            @{ Name = "Interviewed"; Type = "Int64" },
            @{ Name = "OffersExtended"; Type = "Int64" },
            @{ Name = "OfferAccepted"; Type = "Int64" },
            @{ Name = "HiringManagerID"; Type = "Text" },
            @{ Name = "SalaryRangeMin"; Type = "Double" },
            @{ Name = "SalaryRangeMax"; Type = "Double" },
            @{ Name = "TimeToFillDays"; Type = "Text" }
        )
    }
)

# ── Operations Domain Tables ──
$OperationsTables = @(
    @{
        Name = "DimBooks"; Folder = "Operations"
        Columns = @(
            @{ Name = "BookID"; Type = "Text" },
            @{ Name = "Title"; Type = "Text" },
            @{ Name = "AuthorID"; Type = "Text" },
            @{ Name = "Genre"; Type = "Text" },
            @{ Name = "SubGenre"; Type = "Text" },
            @{ Name = "ISBN"; Type = "Text" },
            @{ Name = "PublishDate"; Type = "Date" },
            @{ Name = "ListPrice"; Type = "Double" },
            @{ Name = "Format"; Type = "Text" },
            @{ Name = "PageCount"; Type = "Int64" },
            @{ Name = "PrintRunSize"; Type = "Int64" },
            @{ Name = "ImprintName"; Type = "Text" },
            @{ Name = "Status"; Type = "Text" }
        )
    },
    @{
        Name = "DimAuthors"; Folder = "Operations"
        Columns = @(
            @{ Name = "AuthorID"; Type = "Text" },
            @{ Name = "FirstName"; Type = "Text" },
            @{ Name = "LastName"; Type = "Text" },
            @{ Name = "PenName"; Type = "Text" },
            @{ Name = "AgentName"; Type = "Text" },
            @{ Name = "AgentCompany"; Type = "Text" },
            @{ Name = "ContractStartDate"; Type = "Date" },
            @{ Name = "ContractEndDate"; Type = "Date" },
            @{ Name = "RoyaltyRate"; Type = "Double" },
            @{ Name = "AdvanceAmount"; Type = "Double" },
            @{ Name = "Genre"; Type = "Text" },
            @{ Name = "Nationality"; Type = "Text" },
            @{ Name = "BookCount"; Type = "Int64" }
        )
    },
    @{
        Name = "DimCustomers"; Folder = "Operations"
        Columns = @(
            @{ Name = "CustomerID"; Type = "Text" },
            @{ Name = "CustomerName"; Type = "Text" },
            @{ Name = "CustomerType"; Type = "Text" },
            @{ Name = "ContactEmail"; Type = "Text" },
            @{ Name = "City"; Type = "Text" },
            @{ Name = "State"; Type = "Text" },
            @{ Name = "Country"; Type = "Text" },
            @{ Name = "Region"; Type = "Text" },
            @{ Name = "GeoID"; Type = "Text" },
            @{ Name = "CreditLimit"; Type = "Double" },
            @{ Name = "PaymentTerms"; Type = "Text" },
            @{ Name = "IsActive"; Type = "Boolean" },
            @{ Name = "AccountOpenDate"; Type = "Date" }
        )
    },
    @{
        Name = "DimGeography"; Folder = "Operations"
        Columns = @(
            @{ Name = "GeoID"; Type = "Text" },
            @{ Name = "City"; Type = "Text" },
            @{ Name = "StateProvince"; Type = "Text" },
            @{ Name = "Country"; Type = "Text" },
            @{ Name = "Continent"; Type = "Text" },
            @{ Name = "Region"; Type = "Text" },
            @{ Name = "SubRegion"; Type = "Text" },
            @{ Name = "Latitude"; Type = "Double" },
            @{ Name = "Longitude"; Type = "Double" },
            @{ Name = "TimeZone"; Type = "Text" },
            @{ Name = "Currency"; Type = "Text" },
            @{ Name = "Population"; Type = "Int64" },
            @{ Name = "IsCapital"; Type = "Boolean" }
        )
    },
    @{
        Name = "DimWarehouses"; Folder = "Operations"
        Columns = @(
            @{ Name = "WarehouseID"; Type = "Text" },
            @{ Name = "WarehouseName"; Type = "Text" },
            @{ Name = "Address"; Type = "Text" },
            @{ Name = "City"; Type = "Text" },
            @{ Name = "State"; Type = "Text" },
            @{ Name = "Country"; Type = "Text" },
            @{ Name = "SquareFootage"; Type = "Int64" },
            @{ Name = "MaxCapacityUnits"; Type = "Int64" },
            @{ Name = "CurrentUtilization"; Type = "Double" },
            @{ Name = "ManagerID"; Type = "Text" },
            @{ Name = "MonthlyRent"; Type = "Double" },
            @{ Name = "IsActive"; Type = "Boolean" }
        )
    },
    @{
        Name = "FactOrders"; Folder = "Operations"
        Columns = @(
            @{ Name = "OrderID"; Type = "Text" },
            @{ Name = "OrderDate"; Type = "Date" },
            @{ Name = "CustomerID"; Type = "Text" },
            @{ Name = "BookID"; Type = "Text" },
            @{ Name = "Quantity"; Type = "Int64" },
            @{ Name = "UnitPrice"; Type = "Double" },
            @{ Name = "Discount"; Type = "Double" },
            @{ Name = "TotalAmount"; Type = "Double" },
            @{ Name = "OrderStatus"; Type = "Text" },
            @{ Name = "ShipDate"; Type = "Date" },
            @{ Name = "DeliveryDate"; Type = "Date" },
            @{ Name = "WarehouseID"; Type = "Text" },
            @{ Name = "SalesRepID"; Type = "Text" },
            @{ Name = "Channel"; Type = "Text" }
        )
    },
    @{
        Name = "FactInventory"; Folder = "Operations"
        Columns = @(
            @{ Name = "InventoryID"; Type = "Text" },
            @{ Name = "BookID"; Type = "Text" },
            @{ Name = "WarehouseID"; Type = "Text" },
            @{ Name = "SnapshotDate"; Type = "Date" },
            @{ Name = "QuantityOnHand"; Type = "Int64" },
            @{ Name = "QuantityReserved"; Type = "Int64" },
            @{ Name = "QuantityAvailable"; Type = "Int64" },
            @{ Name = "ReorderPoint"; Type = "Int64" },
            @{ Name = "ReorderQuantity"; Type = "Int64" },
            @{ Name = "UnitCost"; Type = "Double" },
            @{ Name = "TotalInventoryValue"; Type = "Double" },
            @{ Name = "DaysOfSupply"; Type = "Int64" },
            @{ Name = "Status"; Type = "Text" }
        )
    },
    @{
        Name = "FactReturns"; Folder = "Operations"
        Columns = @(
            @{ Name = "ReturnID"; Type = "Text" },
            @{ Name = "OrderID"; Type = "Text" },
            @{ Name = "BookID"; Type = "Text" },
            @{ Name = "CustomerID"; Type = "Text" },
            @{ Name = "ReturnDate"; Type = "Date" },
            @{ Name = "Quantity"; Type = "Int64" },
            @{ Name = "Reason"; Type = "Text" },
            @{ Name = "ReturnStatus"; Type = "Text" },
            @{ Name = "RefundAmount"; Type = "Double" },
            @{ Name = "Condition"; Type = "Text" },
            @{ Name = "RestockStatus"; Type = "Text" }
        )
    }
)

# ============================================================================
# EXECUTION
# ============================================================================

$fabricToken = Get-FabricToken

# ── Step 1: Create Dataflow Gen2 Items ──
Write-Step "Step 1: Creating Dataflow Gen2 Items (CSV -> Bronze Tables)"

$dataflows = @(
    @{
        Name        = "HorizonBooks_DF_Finance"
        Description = "Loads Finance CSV files (DimAccounts, DimCostCenters, FactFinancialTransactions, FactBudget) from Lakehouse Files into tables"
        Domain      = "Finance"
        Tables      = $FinanceTables
    },
    @{
        Name        = "HorizonBooks_DF_HR"
        Description = "Loads HR CSV files (DimEmployees, DimDepartments, FactPayroll, FactPerformanceReviews, FactRecruitment) from Lakehouse Files into tables"
        Domain      = "HR"
        Tables      = $HRTables
    },
    @{
        Name        = "HorizonBooks_DF_Operations"
        Description = "Loads Operations CSV files (DimBooks, DimAuthors, DimCustomers, DimGeography, DimWarehouses, FactOrders, FactInventory, FactReturns) from Lakehouse Files into tables"
        Domain      = "Operations"
        Tables      = $OperationsTables
    }
)

$dataflowIds = @{}

foreach ($df in $dataflows) {
    Write-Info "Creating Dataflow Gen2: $($df.Name) ($($df.Tables.Count) tables)..."

    $dfId = New-OrGetFabricItem `
        -DisplayName $df.Name `
        -Type "Dataflow" `
        -Description $df.Description `
        -WsId $WorkspaceId `
        -Token $fabricToken

    if (-not $dfId) {
        Write-Warn "Failed to create Dataflow '$($df.Name)'"
        continue
    }

    Write-OK "Dataflow created: $($df.Name) ($dfId)"
    $dataflowIds[$df.Name] = $dfId

    # Generate and apply M query mashup definition
    $mashup = Get-DataflowMashup -Domain $df.Domain -TableDefs $df.Tables
    $mashupBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($mashup))

    # Build queriesMetadata object (keyed by query name) for Fabric Dataflow Gen2.
    # This format uses "queriesMetadata" (an object), NOT "queries" (an array).
    # Includes hidden _Target entries for each query's Lakehouse destination,
    # plus hidden TargetWorkspaceId/TargetLakehouseId parameter queries.
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

    foreach ($td in $df.Tables) {
        # Source query metadata
        $queriesMetadata[$td.Name] = @{
            queryId             = [guid]::NewGuid().ToString()
            queryName           = $td.Name
            queryGroupId        = $null
            isHidden            = $false
            loadEnabled         = $true
            destinationSettings = @{ enableStaging = $false }
        }
        # Target navigation query metadata (hidden)
        $destName = "$($td.Name)_Target"
        $queriesMetadata[$destName] = @{
            queryId             = [guid]::NewGuid().ToString()
            queryName           = $destName
            queryGroupId        = $null
            isHidden            = $true
            loadEnabled         = $false
        }
    }

    $queryMeta = @{
        formatVersion                       = "202502"
        computeEngineSettings               = @{ allowFastCopy = $true; maxConcurrency = 1 }
        name                                = $df.Name
        queryGroups                         = @()
        documentLocale                      = "en-US"
        queriesMetadata                     = $queriesMetadata
        fastCombine                         = $false
        allowNativeQueries                  = $true
        skipAutomaticTypeAndHeaderDetection = $false
    } | ConvertTo-Json -Depth 10
    $queryMetaBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($queryMeta))

    # Dataflow definition uses "mashup.pq" (M queries) and "queryMetadata.json" (compute/query settings).
    # Destinations are embedded in mashup.pq via _DataDestination queries and DataDestinations attributes.
    $defJson = '{"definition":{"parts":[' +
        '{"path":"queryMetadata.json","payload":"' + $queryMetaBase64 + '","payloadType":"InlineBase64"},' +
        '{"path":"mashup.pq","payload":"' + $mashupBase64 + '","payloadType":"InlineBase64"}' +
        ']}}'

    # Use dedicated /dataflows/ endpoint for definition updates
    $defOk = Update-DataflowDefinition -DataflowId $dfId -WsId $WorkspaceId -DefinitionJson $defJson -Token $fabricToken
    if ($defOk) {
        Write-OK "Dataflow definition applied with Lakehouse destination: $($df.Name)"
    } else {
        Write-Warn "Failed to apply definition for '$($df.Name)'. Configure manually in the Fabric portal."
    }
}

Write-OK "Created $($dataflowIds.Count) Dataflow Gen2 items with BronzeLH destinations"

# ── Step 1b: Organize Dataflows into Workspace Folder ──
Write-Step "Step 1b: Organizing Dataflows into Workspace Folder"

$dataflowFolderName = "02 - Data Ingestion"
$dataflowFolderId   = $null

# Create folder
try {
    $fabricToken = Get-FabricToken
    $folderHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
    $folderBody = @{ displayName = $dataflowFolderName } | ConvertTo-Json -Depth 3

    $folderResp = Invoke-WebRequest -Method Post `
        -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" `
        -Headers $folderHeaders -Body $folderBody -UseBasicParsing

    if ($folderResp.StatusCode -in @(200, 201)) {
        $dataflowFolderId = ($folderResp.Content | ConvertFrom-Json).id
        Write-OK "Created workspace folder '$dataflowFolderName' ($dataflowFolderId)"
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

    if ($errBody -like "*already*" -or $errBody -like "*AlreadyExists*" -or $errBody -like "*DisplayNameAlreadyInUse*") {
        Write-Info "Folder '$dataflowFolderName' already exists - looking up..."
        try {
            $fabricToken = Get-FabricToken
            $folders = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" `
                -Headers @{ Authorization = "Bearer $fabricToken" }).value
            $existing = $folders | Where-Object { $_.displayName -eq $dataflowFolderName } | Select-Object -First 1
            if ($existing) { $dataflowFolderId = $existing.id; Write-OK "Found existing folder ($dataflowFolderId)" }
        } catch { Write-Warn "Could not look up folder: $($_.Exception.Message)" }
    }
    else {
        Write-Warn "Could not create folder '$dataflowFolderName': $($_.Exception.Message) $errBody"
    }
}

# Move dataflows into the folder
if ($dataflowFolderId) {
    foreach ($df in $dataflows) {
        $dfId = $dataflowIds[$df.Name]
        if (-not $dfId) { continue }

        try {
            $fabricToken = Get-FabricToken
            $moveBody = @{ targetFolderId = $dataflowFolderId } | ConvertTo-Json -Depth 3
            Invoke-RestMethod -Method Post `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$dfId/move" `
                -Headers @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" } `
                -Body $moveBody | Out-Null
            Write-OK "Moved $($df.Name) -> $dataflowFolderName/"
        }
        catch {
            Write-Warn "Could not move $($df.Name) to folder: $($_.Exception.Message)"
        }
    }
}

# ── Step 2: Look up Notebook IDs ──
Write-Step "Step 2: Resolving Notebook Item IDs"

$fabricToken = Get-FabricToken
$nbHeaders = @{ "Authorization" = "Bearer $fabricToken" }

$allNotebooks = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Notebook" `
    -Headers $nbHeaders).value

$notebookNames = @(
    "HorizonBooks_01_BronzeToSilver",
    "HorizonBooks_02_WebEnrichment",
    "HorizonBooks_03_SilverToGold",
    "HorizonBooks_04_Forecasting"
)
$notebookIds = @{}

foreach ($nbName in $notebookNames) {
    $nbItem = $allNotebooks | Where-Object { $_.displayName -eq $nbName } | Select-Object -First 1
    if ($nbItem) {
        $notebookIds[$nbName] = $nbItem.id
        Write-OK "Found notebook: $nbName ($($nbItem.id))"
    } else {
        Write-Warn "Notebook '$nbName' not found. Deploy notebooks first (Deploy-HorizonBooks.ps1 Step 3)."
    }
}

# ── Step 3: Create Data Pipeline ──
Write-Step "Step 3: Creating Data Pipeline (PL_HorizonBooks_Orchestration)"

$pipelineName = "PL_HorizonBooks_Orchestration"
$pipelineDesc = "Orchestrates the full Horizon Books Medallion ETL: Dataflows (CSV to BronzeLH) + NB01 (Bronze to SilverLH) -> NB02 (Web Enrichment) -> NB03 (Silver to GoldLH) -> NB04 (Forecasting)"

$pipelineId = New-OrGetFabricItem `
    -DisplayName $pipelineName `
    -Type "DataPipeline" `
    -Description $pipelineDesc `
    -WsId $WorkspaceId `
    -Token $fabricToken

if (-not $pipelineId) {
    Write-Error "Failed to create Data Pipeline."
    exit 1
}

Write-OK "Pipeline created: $pipelineName ($pipelineId)"

# Build pipeline activities JSON
# Phase 1: 3 Dataflows in parallel (CSV -> BronzeLH tables)
# Phase 2: NB01 BronzeToSilver (depends on all 3 DFs)
# Phase 3: NB02 WebEnrichment (depends on NB01)
# Phase 4: NB03 SilverToGold (depends on NB02)

$activities = @()

# ── Phase 1a: Dataflow activities (parallel, no dependencies) ──
foreach ($df in $dataflows) {
    $dfId = $dataflowIds[$df.Name]
    if (-not $dfId) { continue }

    $activities += @{
        name             = $df.Name
        type             = "RefreshDataflow"
        dependsOn        = @()
        policy           = @{
            timeout                = "0.12:00:00"
            retry                  = 0
            retryIntervalInSeconds = 30
            secureOutput           = $false
            secureInput            = $false
        }
        typeProperties   = @{
            dataflowId     = $dfId
            workspaceId    = $WorkspaceId
            notifyOption   = "NoNotification"
            dataflowType   = "DataflowFabric"
        }
    }
}

# ── Phase 2: NB01 BronzeToSilver (depends on all 3 Dataflows) ──
$bronzeToSilverId = $notebookIds["HorizonBooks_01_BronzeToSilver"]
if ($bronzeToSilverId) {
    # NB01 depends on all 3 Dataflows completing first (Bronze tables must be loaded)
    $nb01DependsOn = @()
    foreach ($df in $dataflows) {
        $dfId = $dataflowIds[$df.Name]
        if ($dfId) {
            $nb01DependsOn += @{
                activity             = $df.Name
                dependencyConditions = @("Succeeded")
            }
        }
    }

    $activities += @{
        name             = "HorizonBooks_01_BronzeToSilver"
        type             = "TridentNotebook"
        dependsOn        = $nb01DependsOn
        policy           = @{
            timeout                = "0.01:00:00"
            retry                  = 1
            retryIntervalInSeconds = 60
        }
        typeProperties   = @{
            notebookId  = $bronzeToSilverId
            workspaceId = $WorkspaceId
        }
    }
}

# ── Phase 3: Web Enrichment Notebook (depends on NB01) ──
$webEnrichmentId = $notebookIds["HorizonBooks_02_WebEnrichment"]
if ($webEnrichmentId) {
    $nb02DependsOn = @()
    if ($bronzeToSilverId) {
        $nb02DependsOn = @(@{
            activity             = "HorizonBooks_01_BronzeToSilver"
            dependencyConditions = @("Succeeded")
        })
    }

    $activities += @{
        name             = "HorizonBooks_02_WebEnrichment"
        type             = "TridentNotebook"
        dependsOn        = $nb02DependsOn
        policy           = @{
            timeout                = "0.01:00:00"
            retry                  = 1
            retryIntervalInSeconds = 60
        }
        typeProperties   = @{
            notebookId  = $webEnrichmentId
            workspaceId = $WorkspaceId
        }
    }
}

# ── Phase 4: Silver->Gold Notebook (depends on WebEnrichment) ──
$silverToGoldId = $notebookIds["HorizonBooks_03_SilverToGold"]
if ($silverToGoldId) {
    $goldDependsOn = @()
    if ($webEnrichmentId) {
        $goldDependsOn = @(@{
            activity             = "HorizonBooks_02_WebEnrichment"
            dependencyConditions = @("Succeeded")
        })
    }
    else {
        # Fall back to depending on dataflows directly
        foreach ($df in $dataflows) {
            $goldDependsOn += @{
                activity             = $df.Name
                dependencyConditions = @("Succeeded")
            }
        }
    }

    $activities += @{
        name             = "HorizonBooks_03_SilverToGold"
        type             = "TridentNotebook"
        dependsOn        = $goldDependsOn
        policy           = @{
            timeout                = "0.01:00:00"
            retry                  = 1
            retryIntervalInSeconds = 60
        }
        typeProperties   = @{
            notebookId  = $silverToGoldId
            workspaceId = $WorkspaceId
        }
    }
}

# ── Phase 5: Forecasting Notebook (depends on SilverToGold) ──
$forecastingId = $notebookIds["HorizonBooks_04_Forecasting"]
if ($forecastingId) {
    $forecastDependsOn = @()
    if ($silverToGoldId) {
        $forecastDependsOn = @(@{
            activity             = "HorizonBooks_03_SilverToGold"
            dependencyConditions = @("Succeeded")
        })
    }

    $activities += @{
        name             = "HorizonBooks_04_Forecasting"
        type             = "TridentNotebook"
        dependsOn        = $forecastDependsOn
        policy           = @{
            timeout                = "0.01:00:00"
            retry                  = 1
            retryIntervalInSeconds = 60
        }
        typeProperties   = @{
            notebookId  = $forecastingId
            workspaceId = $WorkspaceId
        }
    }
}

# Build pipeline definition JSON
$pipelineContent = @{
    properties = @{
        activities = $activities
    }
} | ConvertTo-Json -Depth 15

$pipelineContentBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($pipelineContent))

$pipelineDefJson = '{"definition":{"parts":[{"path":"pipeline-content.json","payload":"' + $pipelineContentBase64 + '","payloadType":"InlineBase64"}]}}'

# Use dedicated /dataPipelines/ endpoint (same as SSISToFabric fabric_deployer.py)
$defOk = Update-PipelineDefinition -PipelineId $pipelineId -WsId $WorkspaceId -DefinitionJson $pipelineDefJson -Token $fabricToken
if ($defOk) {
    Write-OK "Pipeline definition applied with $($activities.Count) activities"
} else {
    Write-Warn "Failed to apply pipeline definition. Configure manually in the Fabric portal."
}

# ── Summary ──
Write-Host ""
Write-Host "======================================================" -ForegroundColor Green
Write-Host "  PIPELINE DEPLOYMENT SUMMARY" -ForegroundColor Green
Write-Host "======================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Pipeline        : $pipelineName ($pipelineId)" -ForegroundColor White
Write-Host "  Dataflows       :" -ForegroundColor White
foreach ($df in $dataflows) {
    $dfId = $dataflowIds[$df.Name]
    Write-Host "    - $($df.Name) ($dfId)" -ForegroundColor White
}
Write-Host "  Notebooks       :" -ForegroundColor White
foreach ($nbName in $notebookNames) {
    $nbId = $notebookIds[$nbName]
    Write-Host "    - $nbName ($nbId)" -ForegroundColor White
}
Write-Host ""
Write-Host "  ORCHESTRATION FLOW:" -ForegroundColor Cyan
Write-Host "  DF_Finance   ──┐" -ForegroundColor Cyan
Write-Host "  DF_HR        ──┼──> NB01_BronzeToSilver ──> NB02_WebEnrichment ──> NB03_SilverToGold ──> NB04_Forecasting" -ForegroundColor Cyan
Write-Host "  DF_Operations──┘" -ForegroundColor Cyan
Write-Host ""
Write-Host "  To run the pipeline:" -ForegroundColor Yellow
Write-Host "  * Fabric Portal: Open the pipeline -> Click 'Run'" -ForegroundColor Yellow
Write-Host "  * Or schedule it: Pipeline -> Schedule -> Set recurrence" -ForegroundColor Yellow
Write-Host ""
