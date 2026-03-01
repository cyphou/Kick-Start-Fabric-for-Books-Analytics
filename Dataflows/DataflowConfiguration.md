# Horizon Books — Dataflows Gen2 Configuration Guide
## Microsoft Fabric Dataflows for CSV-to-Lakehouse Ingestion

## Overview

Three **Dataflows Gen2** ingest CSV files from the Lakehouse `Files/` folder into
Bronze Lakehouse Delta tables. Each dataflow handles one business domain and applies
data-quality transformations before writing.

### Connection Architecture

Each `mashup.pq` file uses **centralised target parameters** instead of inline IDs:

| Parameter | Purpose | Placeholder |
|---|---|---|
| `TargetWorkspaceId` | Fabric Workspace GUID | `{{WORKSPACE_ID}}` |
| `TargetLakehouseId` | BronzeLH Lakehouse GUID | `{{BRONZE_LH_ID}}` |

These parameters are declared once at the top of every mashup file as
`shared … meta [IsParameterQuery=true]` queries. Deploy scripts
(`Deploy-Pipeline.ps1`, `Update-DataflowDestinations.ps1`) substitute the
placeholders with actual GUIDs at deployment time.

### Query Naming Convention

| Query | Visibility | Role |
|---|---|---|
| `<Table>` | Visible | Reads CSV, transforms, outputs rows |
| `<Table>_Target` | Hidden | Lakehouse navigation query used as `[DataDestinations]` reference |
| `TargetWorkspaceId` | Hidden | Parameter — workspace GUID |
| `TargetLakehouseId` | Hidden | Parameter — lakehouse GUID |

### Common Transformations (all queries)

Every source query applies at minimum:

1. **Type enforcement** — `Table.TransformColumnTypes` with explicit M types
2. **Duplicate removal** — `Table.Distinct`
3. **Null-row filtering** — `Table.SelectRows` excluding fully-null records

---

## Dataflow 1: DF_Finance

**Source:** `Files/DimAccounts.csv`, `DimCostCenters.csv`, `FactFinancialTransactions.csv`, `FactBudget.csv`
**Destination:** BronzeLH tables

### Query Details

#### DimAccounts

| Column | M Type | Transformation |
|---|---|---|
| AccountID | `Int64.Type` | — |
| AccountName | `type text` | `Text.Trim` |
| AccountType | `type text` | `Text.Proper(Text.Trim(_))` |
| AccountCategory | `type text` | Null → `"Uncategorized"`, else `Text.Proper(Text.Trim(_))` |
| ParentAccountID | `Int64.Type` | — |
| IsActive | `type logical` | — |

#### DimCostCenters

| Column | M Type | Transformation |
|---|---|---|
| CostCenterID | `type text` | — |
| CostCenterName | `type text` | `Text.Trim` |
| Department | `type text` | `Text.Proper(Text.Trim(_))` |
| DivisionHead | `type text` | `Text.Proper(Text.Trim(_))` |

#### FactFinancialTransactions

| Column | M Type | Transformation |
|---|---|---|
| TransactionID | `type text` | — |
| TransactionDate | `type date` | — |
| AccountID | `Int64.Type` | — |
| BookID | `type text` | — |
| Amount | `type number` | Null → `0` |
| Currency | `type text` | Null → `"USD"`, else `Text.Upper(Text.Trim(_))` |
| TransactionType | `type text` | `Text.Proper(Text.Trim(_))` |
| FiscalYear | `type text` | `Text.Trim` |
| FiscalQuarter | `type text` | `Text.Trim` |
| FiscalMonth | `Int64.Type` | — |
| CostCenterID | `type text` | — |
| Description | `type text` | Null → `""`, else `Text.Trim` |

#### FactBudget

| Column | M Type | Transformation |
|---|---|---|
| BudgetID | `type text` | — |
| FiscalYear | `type text` | `Text.Trim` |
| FiscalQuarter | `type text` | `Text.Trim` |
| FiscalMonth | `Int64.Type` | — |
| AccountID | `Int64.Type` | — |
| CostCenterID | `type text` | — |
| BudgetAmount | `type number` | Null → `0` |
| ActualAmount | `type number` | Null → `0` |
| Variance | `type number` | Null → `0` |
| VariancePct | `type number` | Null → `0` |

---

## Dataflow 2: DF_HR

**Source:** `Files/DimEmployees.csv`, `DimDepartments.csv`, `FactPayroll.csv`, `FactPerformanceReviews.csv`, `FactRecruitment.csv`
**Destination:** BronzeLH tables

### Query Details

#### DimEmployees

| Column | M Type | Transformation |
|---|---|---|
| EmployeeID | `type text` | — |
| FirstName | `type text` | `Text.Proper(Text.Trim(_))` |
| LastName | `type text` | `Text.Proper(Text.Trim(_))` |
| Email | `type text` | `Text.Lower(Text.Trim(_))` — normalised to lowercase |
| HireDate | `type date` | — |
| DepartmentID | `type text` | — |
| JobTitle | `type text` | `Text.Proper(Text.Trim(_))` |
| ManagerID | `type text` | — (nullable for VPs) |
| EmploymentType | `type text` | `Text.Proper(Text.Trim(_))` |
| Location | `type text` | `Text.Proper(Text.Trim(_))` |
| GeoID | `type text` | — |
| IsActive | `type logical` | — |

#### DimDepartments

| Column | M Type | Transformation |
|---|---|---|
| DepartmentID | `type text` | — |
| DepartmentName | `type text` | `Text.Proper(Text.Trim(_))` |
| DepartmentHead | `type text` | `Text.Proper(Text.Trim(_))` |
| HeadCount | `Int64.Type` | Null or negative → `0` |
| AnnualBudget | `type number` | Null or negative → `0` |
| Location | `type text` | `Text.Proper(Text.Trim(_))` |

#### FactPayroll

| Column | M Type | Transformation |
|---|---|---|
| PayrollID | `type text` | — |
| EmployeeID | `type text` | — |
| PayPeriodStart | `type date` | — |
| PayPeriodEnd | `type date` | — |
| BaseSalary | `type number` | Null → `0`, else `Number.Abs` |
| Bonus | `type number` | Null → `0`, else `Number.Abs` |
| Overtime | `type number` | Null → `0`, else `Number.Abs` |
| Deductions | `type number` | Null → `0`, else `Number.Abs` |
| NetPay | `type number` | Null → `0` |
| PayDate | `type date` | — |

#### FactPerformanceReviews

| Column | M Type | Transformation |
|---|---|---|
| ReviewID | `type text` | — |
| EmployeeID | `type text` | — |
| ReviewDate | `type date` | — |
| ReviewerID | `type text` | — |
| PerformanceRating | `type text` | `Text.Proper(Text.Trim(_))` |
| GoalsMet | `type text` | `Text.Trim` |
| Strengths | `type text` | Null → `""`, else `Text.Trim` |
| AreasForImprovement | `type text` | Null → `""`, else `Text.Trim` |
| OverallScore | `type number` | Clamped to **0–100** range |

#### FactRecruitment

| Column | M Type | Transformation |
|---|---|---|
| RequisitionID | `type text` | — |
| DepartmentID | `type text` | — |
| JobTitle | `type text` | `Text.Proper(Text.Trim(_))` |
| OpenDate | `type date` | — |
| CloseDate | `type date` | — (nullable for open positions) |
| Status | `type text` | `Text.Proper(Text.Trim(_))` |
| ApplicationsReceived | `Int64.Type` | Null or negative → `0` |
| Interviewed | `Int64.Type` | Null or negative → `0` |
| OffersExtended | `Int64.Type` | Null or negative → `0` |
| OfferAccepted | `Int64.Type` | Null or negative → `0` |
| HiringManagerID | `type text` | — |
| SalaryRangeMin | `type number` | Null or negative → `0` |
| SalaryRangeMax | `type number` | Null or negative → `0` |
| TimeToFillDays | `Int64.Type` | — (nullable for open positions) |

---

## Dataflow 3: DF_Operations

**Source:** `Files/DimBooks.csv`, `DimAuthors.csv`, `DimCustomers.csv`, `DimGeography.csv`, `DimWarehouses.csv`, `FactOrders.csv`, `FactInventory.csv`, `FactReturns.csv`
**Destination:** BronzeLH tables

### Query Details

#### DimBooks

| Column | M Type | Transformation |
|---|---|---|
| BookID | `type text` | — |
| Title | `type text` | `Text.Trim` |
| AuthorID | `type text` | — |
| Genre | `type text` | `Text.Proper(Text.Trim(_))` |
| SubGenre | `type text` | `Text.Proper(Text.Trim(_))` (nullable) |
| ISBN | `type text` | `Text.Trim` |
| PublishDate | `type date` | — |
| ListPrice | `type number` | Null or negative → `0` |
| Format | `type text` | `Text.Proper(Text.Trim(_))` |
| PageCount | `Int64.Type` | Null or ≤ 0 → `null` |
| PrintRunSize | `Int64.Type` | Null or ≤ 0 → `null` |
| ImprintName | `type text` | `Text.Trim` (nullable) |
| Status | `type text` | `Text.Proper(Text.Trim(_))` |

#### DimAuthors

| Column | M Type | Transformation |
|---|---|---|
| AuthorID | `type text` | — |
| FirstName | `type text` | `Text.Proper(Text.Trim(_))` |
| LastName | `type text` | `Text.Proper(Text.Trim(_))` |
| PenName | `type text` | `Text.Trim` (nullable) |
| AgentName | `type text` | `Text.Proper(Text.Trim(_))` (nullable) |
| AgentCompany | `type text` | `Text.Trim` (nullable) |
| ContractStartDate | `type date` | — |
| ContractEndDate | `type date` | — |
| RoyaltyRate | `type number` | Normalised to 0–1 (values > 1 divided by 100) |
| AdvanceAmount | `type number` | Null or negative → `0` |
| Genre | `type text` | `Text.Proper(Text.Trim(_))` |
| Nationality | `type text` | `Text.Proper(Text.Trim(_))` |
| BookCount | `Int64.Type` | — |

#### DimCustomers

| Column | M Type | Transformation |
|---|---|---|
| CustomerID | `type text` | — |
| CustomerName | `type text` | `Text.Trim` |
| CustomerType | `type text` | `Text.Proper(Text.Trim(_))` |
| ContactEmail | `type text` | `Text.Lower(Text.Trim(_))` — normalised to lowercase |
| City | `type text` | `Text.Proper(Text.Trim(_))` |
| State | `type text` | `Text.Proper(Text.Trim(_))` |
| Country | `type text` | `Text.Proper(Text.Trim(_))` |
| Region | `type text` | `Text.Proper(Text.Trim(_))` |
| GeoID | `type text` | — |
| CreditLimit | `type number` | Null or negative → `0` |
| PaymentTerms | `type text` | `Text.Trim` (nullable) |
| IsActive | `type logical` | — |
| AccountOpenDate | `type date` | — |

#### DimGeography

| Column | M Type | Transformation |
|---|---|---|
| GeoID | `type text` | — |
| City | `type text` | `Text.Proper(Text.Trim(_))` |
| StateProvince | `type text` | `Text.Proper(Text.Trim(_))` (nullable) |
| Country | `type text` | `Text.Proper(Text.Trim(_))` |
| Continent | `type text` | `Text.Proper(Text.Trim(_))` |
| Region | `type text` | `Text.Proper(Text.Trim(_))` |
| SubRegion | `type text` | `Text.Proper(Text.Trim(_))` (nullable) |
| Latitude | `type number` | Out of [-90, 90] → `null` |
| Longitude | `type number` | Out of [-180, 180] → `null` |
| TimeZone | `type text` | `Text.Trim` |
| Currency | `type text` | `Text.Upper(Text.Trim(_))` |
| Population | `Int64.Type` | — |
| IsCapital | `type logical` | — |

#### DimWarehouses

| Column | M Type | Transformation |
|---|---|---|
| WarehouseID | `type text` | — |
| WarehouseName | `type text` | `Text.Trim` |
| Address | `type text` | `Text.Trim` (nullable) |
| City | `type text` | `Text.Proper(Text.Trim(_))` |
| State | `type text` | `Text.Proper(Text.Trim(_))` |
| Country | `type text` | `Text.Proper(Text.Trim(_))` |
| SquareFootage | `Int64.Type` | Null or negative → `0` |
| MaxCapacityUnits | `Int64.Type` | Null or negative → `0` |
| CurrentUtilization | `type number` | Normalised to 0–1 (values > 1 divided by 100, clamped) |
| ManagerID | `type text` | — |
| MonthlyRent | `type number` | Null or negative → `0` |
| IsActive | `type logical` | — |

#### FactOrders

| Column | M Type | Transformation |
|---|---|---|
| OrderID | `type text` | — |
| OrderDate | `type date` | — |
| CustomerID | `type text` | — |
| BookID | `type text` | — |
| Quantity | `Int64.Type` | Null or negative → `0` |
| UnitPrice | `type number` | Null or negative → `0` |
| Discount | `type number` | Normalised to 0–1 (values > 1 divided by 100) |
| TotalAmount | `type number` | Null or negative → `0` |
| OrderStatus | `type text` | `Text.Proper(Text.Trim(_))` |
| ShipDate | `type date` | — |
| DeliveryDate | `type date` | — |
| WarehouseID | `type text` | — (nullable for digital orders) |
| SalesRepID | `type text` | — |
| Channel | `type text` | `Text.Proper(Text.Trim(_))` |

#### FactInventory

| Column | M Type | Transformation |
|---|---|---|
| InventoryID | `type text` | — |
| BookID | `type text` | — |
| WarehouseID | `type text` | — |
| SnapshotDate | `type date` | — |
| QuantityOnHand | `Int64.Type` | Null or negative → `0` |
| QuantityReserved | `Int64.Type` | Null or negative → `0` |
| QuantityAvailable | `Int64.Type` | Null or negative → `0` |
| ReorderPoint | `Int64.Type` | Null or negative → `0` |
| ReorderQuantity | `Int64.Type` | Null or negative → `0` |
| UnitCost | `type number` | Null or negative → `0` |
| TotalInventoryValue | `type number` | Null or negative → `0` |
| DaysOfSupply | `Int64.Type` | Null or negative → `0` |
| Status | `type text` | `Text.Proper(Text.Trim(_))` |

#### FactReturns

| Column | M Type | Transformation |
|---|---|---|
| ReturnID | `type text` | — |
| OrderID | `type text` | — |
| BookID | `type text` | — |
| CustomerID | `type text` | — |
| ReturnDate | `type date` | — |
| Quantity | `Int64.Type` | Null or negative → `0` |
| Reason | `type text` | `Text.Proper(Text.Trim(_))` |
| ReturnStatus | `type text` | `Text.Proper(Text.Trim(_))` |
| RefundAmount | `type number` | Null → `0`, else `Number.Abs` |
| Condition | `type text` | `Text.Proper(Text.Trim(_))` |
| RestockStatus | `type text` | `Text.Proper(Text.Trim(_))` |

---

## Step-by-Step Instructions

### Automated Deployment (recommended)

Run `Deploy-Pipeline.ps1` or `Deploy-Full.ps1` — these scripts create dataflows,
substitute `{{WORKSPACE_ID}}` / `{{BRONZE_LH_ID}}` placeholders with real GUIDs,
and set Lakehouse destinations automatically.

### Manual Creation in Portal

1. Go to your **Fabric Workspace**
2. Click **+ New** → **Dataflow Gen2**
3. Rename to `DF_Finance` / `DF_HR` / `DF_Operations`
4. For each query:
   a. Click **Get Data** → **Text/CSV** or use the Lakehouse connector
   b. Browse to the CSV file in the Lakehouse `Files/` folder
   c. Apply the type changes and transformations listed above
   d. Set **Data Destination** → your Lakehouse → select the target table
   e. Map columns (should auto-map if names match)
5. Click **Publish** to save and run

### Upload CSV Files First

Upload all CSV files to the Lakehouse `Files/` folder (flat structure — all CSVs
in the root of `Files/`):

```
BronzeLH/
  Files/
    DimAccounts.csv
    DimCostCenters.csv
    FactFinancialTransactions.csv
    FactBudget.csv
    DimEmployees.csv
    DimDepartments.csv
    FactPayroll.csv
    FactPerformanceReviews.csv
    FactRecruitment.csv
    DimBooks.csv
    DimAuthors.csv
    DimCustomers.csv
    DimWarehouses.csv
    DimGeography.csv
    FactOrders.csv
    FactInventory.csv
    FactReturns.csv
```

---

## Refresh Schedule

After initial load, set up scheduled refreshes:
- **DF_Finance**: Daily at 6:00 AM UTC
- **DF_HR**: Weekly on Monday at 7:00 AM UTC
- **DF_Operations**: Daily at 5:30 AM UTC (before Finance)

## Data Quality Checks

After each dataflow run, verify:
- [ ] Row counts match source CSVs
- [ ] No null values in required ID/key fields
- [ ] Date formats parsed correctly
- [ ] Decimal precision maintained
- [ ] Foreign key relationships are valid
- [ ] Text casing is consistent (Proper case for names, Upper for currencies)
- [ ] Numeric ranges are valid (0–100 for scores, 0–1 for rates/utilization)
