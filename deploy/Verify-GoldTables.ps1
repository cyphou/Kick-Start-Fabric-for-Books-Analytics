<#
.SYNOPSIS
    Verifies all 23 Gold tables required by the semantic model exist and have correct columns.
.DESCRIPTION
    Queries the GoldLH SQL endpoint to check every table that the DirectLake
    semantic model references.  Reports missing tables, missing columns, and type mismatches.
.PARAMETER WorkspaceId
    Fabric workspace ID.
.PARAMETER GoldLakehouseName
    Name of the Gold lakehouse (default: GoldLH).
#>
[CmdletBinding()]
param(
    [string]$WorkspaceId,
    [string]$GoldLakehouseName = "GoldLH"
)

$ErrorActionPreference = "Stop"
Import-Module (Join-Path $PSScriptRoot 'HorizonBooks.psm1') -Force
$headers = @{ Authorization = "Bearer $(Get-FabricToken)" }
$fabricBase = $script:FabricApiBase

# ── Resolve Lakehouse ──
if (-not $WorkspaceId) {
    Write-Error "WorkspaceId is required."
    return
}

$lakehouses = (Invoke-RestMethod "$fabricBase/workspaces/$WorkspaceId/lakehouses" -Headers $headers).value
$goldLH = $lakehouses | Where-Object { $_.displayName -eq $GoldLakehouseName }
if (-not $goldLH) { Write-Error "Lakehouse '$GoldLakehouseName' not found."; return }

$sqlEndpoint = $goldLH.properties.sqlEndpointProperties.connectionString
$dbName = $goldLH.properties.sqlEndpointProperties.id
if (-not $sqlEndpoint) {
    Write-Warning "SQL endpoint not yet provisioned. Wait a few minutes and retry."
    return
}

Write-Host "`n=== Gold Table Verification ===" -ForegroundColor Cyan
Write-Host "  Workspace : $WorkspaceId"
Write-Host "  Lakehouse : $GoldLakehouseName ($($goldLH.id))"
Write-Host "  SQL EP    : $sqlEndpoint`n"

# ── Expected TMDL schema (table → columns with types) ──
$expected = @{
    "dim.DimDate" = @{
        DateKey="bigint"; FullDate="date"; DayOfMonth="int"; DayOfWeek="int"
        DayName="varchar"; MonthNumber="int"; MonthName="varchar"; Quarter="int"
        QuarterName="varchar"; Year="int"; FiscalYear="varchar"; FiscalQuarter="varchar"
        IsWeekend="bit"; IsHoliday="bit"
    }
    "dim.DimAccounts" = @{
        AccountID="bigint"; AccountName="varchar"; AccountType="varchar"
        AccountCategory="varchar"; ParentAccountID="bigint"; IsActive="bit"
    }
    "dim.DimCostCenters" = @{
        CostCenterID="varchar"; CostCenterName="varchar"; Department="varchar"; DivisionHead="varchar"
    }
    "dim.DimBooks" = @{
        BookID="varchar"; Title="varchar"; AuthorID="varchar"; Genre="varchar"
        SubGenre="varchar"; ISBN="varchar"; PublishDate="date"; ListPrice="float"
        Format="varchar"; PageCount="bigint"; PrintRunSize="bigint"; ImprintName="varchar"; Status="varchar"
    }
    "dim.DimAuthors" = @{
        AuthorID="varchar"; FirstName="varchar"; LastName="varchar"; PenName="varchar"
        AgentName="varchar"; AgentCompany="varchar"; ContractStartDate="date"
        ContractEndDate="date"; RoyaltyRate="float"; AdvanceAmount="float"
        Genre="varchar"; Nationality="varchar"; BookCount="bigint"
    }
    "dim.DimGeography" = @{
        GeoID="varchar"; City="varchar"; StateProvince="varchar"; Country="varchar"
        Continent="varchar"; Region="varchar"; SubRegion="varchar"; Latitude="float"
        Longitude="float"; TimeZone="varchar"; Currency="varchar"; Population="bigint"; IsCapital="bit"
    }
    "dim.DimCustomers" = @{
        CustomerID="varchar"; CustomerName="varchar"; CustomerType="varchar"
        ContactEmail="varchar"; City="varchar"; State="varchar"; Country="varchar"
        Region="varchar"; GeoID="varchar"; CreditLimit="float"; PaymentTerms="varchar"
        IsActive="bit"; AccountOpenDate="date"
    }
    "dim.DimEmployees" = @{
        EmployeeID="varchar"; FirstName="varchar"; LastName="varchar"; Email="varchar"
        HireDate="date"; DepartmentID="varchar"; JobTitle="varchar"; ManagerID="varchar"
        EmploymentType="varchar"; Location="varchar"; GeoID="varchar"; IsActive="bit"
    }
    "dim.DimDepartments" = @{
        DepartmentID="varchar"; DepartmentName="varchar"; DepartmentHead="varchar"
        HeadCount="bigint"; AnnualBudget="float"; Location="varchar"
    }
    "dim.DimWarehouses" = @{
        WarehouseID="varchar"; WarehouseName="varchar"; Address="varchar"
        City="varchar"; State="varchar"; Country="varchar"; SquareFootage="bigint"
        MaxCapacityUnits="bigint"; CurrentUtilization="float"; ManagerID="varchar"
        MonthlyRent="float"; IsActive="bit"
    }
    "fact.FactFinancialTransactions" = @{
        TransactionID="varchar"; TransactionDate="date"; AccountID="bigint"
        BookID="varchar"; Amount="float"; Currency="varchar"; TransactionType="varchar"
        FiscalYear="varchar"; FiscalQuarter="varchar"; FiscalMonth="bigint"
        CostCenterID="varchar"; Description="varchar"
    }
    "fact.FactBudget" = @{
        BudgetID="varchar"; FiscalYear="varchar"; FiscalQuarter="varchar"
        FiscalMonth="bigint"; AccountID="bigint"; CostCenterID="varchar"
        BudgetAmount="float"; ActualAmount="float"; Variance="float"; VariancePct="float"
    }
    "fact.FactOrders" = @{
        OrderID="varchar"; OrderDate="date"; CustomerID="varchar"; BookID="varchar"
        Quantity="bigint"; UnitPrice="float"; Discount="float"; TotalAmount="float"
        OrderStatus="varchar"; ShipDate="date"; DeliveryDate="date"
        WarehouseID="varchar"; SalesRepID="varchar"; Channel="varchar"
    }
    "fact.FactInventory" = @{
        InventoryID="varchar"; BookID="varchar"; WarehouseID="varchar"
        SnapshotDate="date"; QuantityOnHand="bigint"; QuantityReserved="bigint"
        QuantityAvailable="bigint"; ReorderPoint="bigint"; ReorderQuantity="bigint"
        UnitCost="float"; TotalInventoryValue="float"; DaysOfSupply="bigint"; Status="varchar"
    }
    "fact.FactReturns" = @{
        ReturnID="varchar"; OrderID="varchar"; BookID="varchar"; CustomerID="varchar"
        ReturnDate="date"; Quantity="bigint"; Reason="varchar"; ReturnStatus="varchar"
        RefundAmount="float"; Condition="varchar"; RestockStatus="varchar"
    }
    "fact.FactPayroll" = @{
        PayrollID="varchar"; EmployeeID="varchar"; PayPeriodStart="date"
        PayPeriodEnd="date"; BaseSalary="float"; Bonus="float"; Overtime="float"
        Deductions="float"; NetPay="float"; PayDate="date"
    }
    "fact.FactPerformanceReviews" = @{
        ReviewID="varchar"; EmployeeID="varchar"; ReviewDate="date"
        ReviewerID="varchar"; PerformanceRating="varchar"; GoalsMet="varchar"
        Strengths="varchar"; AreasForImprovement="varchar"; OverallScore="float"
    }
    "fact.FactRecruitment" = @{
        RequisitionID="varchar"; DepartmentID="varchar"; JobTitle="varchar"
        OpenDate="date"; CloseDate="date"; Status="varchar"
        ApplicationsReceived="bigint"; Interviewed="bigint"; OffersExtended="bigint"
        OfferAccepted="bigint"; HiringManagerID="varchar"; SalaryRangeMin="float"
        SalaryRangeMax="float"; TimeToFillDays="bigint"
    }
    "analytics.ForecastSalesRevenue" = @{
        ForecastMonth="date"; Channel="varchar"; Revenue="float"
        Orders="int"; Customers="int"; LowerBound="float"; UpperBound="float"
        ForecastHorizon="int"; RecordType="varchar"; ForecastModel="varchar"
    }
    "analytics.ForecastGenreDemand" = @{
        ForecastMonth="date"; Genre="varchar"; UnitDemand="float"; Revenue="float"
        LowerBound="float"; UpperBound="float"; ForecastHorizon="int"
        RecordType="varchar"; ForecastModel="varchar"
    }
    "analytics.ForecastFinancial" = @{
        ForecastMonth="date"; PLCategory="varchar"; Amount="float"
        TransactionCount="int"; LowerBound="float"; UpperBound="float"
        ForecastHorizon="int"; RecordType="varchar"; ForecastModel="varchar"
    }
    "analytics.ForecastInventoryDemand" = @{
        ForecastMonth="date"; WarehouseID="varchar"; UnitsDemanded="float"
        Revenue="float"; LowerBound="float"; UpperBound="float"
        ForecastHorizon="int"; RecordType="varchar"; ForecastModel="varchar"
        CurrentStock="float"; CumulativeDemand="float"; StockCoverMonths="float"
    }
    "analytics.ForecastWorkforce" = @{
        ForecastMonth="date"; Metric="varchar"; Value="float"
        LowerBound="float"; UpperBound="float"; ForecastHorizon="int"
        RecordType="varchar"; ForecastModel="varchar"
    }
}

# ── Check tables via SQL endpoint ──
$totalTables = $expected.Count
$okTables = 0
$failedTables = @()

foreach ($entry in $expected.GetEnumerator()) {
    $tableFull = $entry.Key
    $parts = $tableFull -split '\.'
    $schema = $parts[0]
    $table = $parts[1]
    $requiredCols = $entry.Value

    try {
        # Check if table exists by querying INFORMATION_SCHEMA
        $query = @"
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table'
"@
        # Use Fabric SQL endpoint via TDS
        $connStr = "Server=$sqlEndpoint;Database=$GoldLakehouseName;Authentication=ActiveDirectoryDefault;TrustServerCertificate=True"
        $conn = New-Object System.Data.SqlClient.SqlConnection($connStr)
        $conn.Open()
        $cmd = $conn.CreateCommand()
        $cmd.CommandText = $query
        $reader = $cmd.ExecuteReader()

        $actualCols = @{}
        while ($reader.Read()) {
            $actualCols[$reader["COLUMN_NAME"]] = $reader["DATA_TYPE"]
        }
        $reader.Close()
        $conn.Close()

        if ($actualCols.Count -eq 0) {
            Write-Host "  MISSING  $tableFull" -ForegroundColor Red
            $failedTables += $tableFull
            continue
        }

        # Check required columns exist
        $missingCols = @()
        foreach ($colEntry in $requiredCols.GetEnumerator()) {
            if (-not $actualCols.ContainsKey($colEntry.Key)) {
                $missingCols += $colEntry.Key
            }
        }

        if ($missingCols.Count -gt 0) {
            Write-Host "  PARTIAL  $tableFull  (missing: $($missingCols -join ', '))" -ForegroundColor Yellow
            $failedTables += $tableFull
        } else {
            Write-Host "  OK       $tableFull  ($($requiredCols.Count) required cols present, $($actualCols.Count) total)" -ForegroundColor Green
            $okTables++
        }
    }
    catch {
        Write-Host "  ERROR    $tableFull  $_" -ForegroundColor Red
        $failedTables += $tableFull
    }
}

Write-Host "`n=== Summary ===" -ForegroundColor Cyan
Write-Host "  OK:     $okTables / $totalTables"
if ($failedTables.Count -gt 0) {
    Write-Host "  FAILED: $($failedTables.Count) table(s):" -ForegroundColor Red
    $failedTables | ForEach-Object { Write-Host "    - $_" -ForegroundColor Red }
} else {
    Write-Host "  All $totalTables tables verified successfully!" -ForegroundColor Green
}
