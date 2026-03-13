<p align="center">
  <img src="../assets/workspace-logo.png" alt="Horizon Books" width="80"/>
</p>

<h1 align="center">Dataflow Gen2 Configuration</h1>

<p align="center">
  <strong>3 Dataflows for CSV-to-Lakehouse ingestion with data quality transformations</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Dataflow%20Gen2-E8A838?style=flat-square&logo=microsoft&logoColor=white" alt="Dataflow Gen2"/>
  <img src="https://img.shields.io/badge/dataflows-3-blue?style=flat-square" alt="Dataflows"/>
  <img src="https://img.shields.io/badge/tables-17-orange?style=flat-square" alt="Tables"/>
  <img src="https://img.shields.io/badge/destination-BronzeLH-CD7F32?style=flat-square" alt="Bronze"/>
</p>

<p align="center">
  <a href="#-connection-architecture">Architecture</a> •
  <a href="#-df_finance">DF_Finance</a> •
  <a href="#-df_hr">DF_HR</a> •
  <a href="#-df_operations">DF_Operations</a> •
  <a href="#-deployment">Deployment</a>
</p>

---

## 🔗 Connection Architecture

Each `mashup.pq` file uses **centralised target parameters** instead of inline IDs:

| Parameter | Purpose | Placeholder |
|-----------|---------|-------------|
| `TargetWorkspaceId` | Fabric Workspace GUID | `{{WORKSPACE_ID}}` |
| `TargetLakehouseId` | BronzeLH Lakehouse GUID | `{{BRONZE_LH_ID}}` |

Deploy scripts substitute placeholders with actual GUIDs at deployment time.

### Query Naming Convention

| Query | Visibility | Role |
|-------|------------|------|
| `<Table>` | Visible | Reads CSV, transforms, outputs rows |
| `<Table>_Target` | Hidden | Lakehouse navigation — `[DataDestinations]` reference |
| `TargetWorkspaceId` | Hidden | Parameter — workspace GUID |
| `TargetLakehouseId` | Hidden | Parameter — lakehouse GUID |

### Common Transformations (all queries)

1. **Type enforcement** — `Table.TransformColumnTypes` with explicit M types
2. **Duplicate removal** — `Table.Distinct`
3. **Null-row filtering** — `Table.SelectRows` excluding fully-null records

---

## 💰 DF_Finance

**Source:** `Files/DimAccounts.csv`, `DimCostCenters.csv`, `FactFinancialTransactions.csv`, `FactBudget.csv`

<details>
<summary><b>DimAccounts</b> (6 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
| AccountID | `Int64.Type` | — |
| AccountName | `type text` | `Text.Trim` |
| AccountType | `type text` | `Text.Proper(Text.Trim(_))` |
| AccountCategory | `type text` | Null → `"Uncategorized"`, else `Text.Proper(Text.Trim(_))` |
| ParentAccountID | `Int64.Type` | — |
| IsActive | `type logical` | — |

</details>

<details>
<summary><b>DimCostCenters</b> (4 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
| CostCenterID | `type text` | — |
| CostCenterName | `type text` | `Text.Trim` |
| Department | `type text` | `Text.Proper(Text.Trim(_))` |
| DivisionHead | `type text` | `Text.Proper(Text.Trim(_))` |

</details>

<details>
<summary><b>FactFinancialTransactions</b> (12 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
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

</details>

<details>
<summary><b>FactBudget</b> (10 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
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

</details>

---

## 👥 DF_HR

**Source:** `Files/DimEmployees.csv`, `DimDepartments.csv`, `FactPayroll.csv`, `FactPerformanceReviews.csv`, `FactRecruitment.csv`

<details>
<summary><b>DimEmployees</b> (12 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
| EmployeeID | `type text` | — |
| FirstName | `type text` | `Text.Proper(Text.Trim(_))` |
| LastName | `type text` | `Text.Proper(Text.Trim(_))` |
| Email | `type text` | `Text.Lower(Text.Trim(_))` |
| HireDate | `type date` | — |
| DepartmentID | `type text` | — |
| JobTitle | `type text` | `Text.Proper(Text.Trim(_))` |
| ManagerID | `type text` | — (nullable for VPs) |
| EmploymentType | `type text` | `Text.Proper(Text.Trim(_))` |
| Location | `type text` | `Text.Proper(Text.Trim(_))` |
| GeoID | `type text` | — |
| IsActive | `type logical` | — |

</details>

<details>
<summary><b>DimDepartments</b> (6 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
| DepartmentID | `type text` | — |
| DepartmentName | `type text` | `Text.Proper(Text.Trim(_))` |
| DepartmentHead | `type text` | `Text.Proper(Text.Trim(_))` |
| HeadCount | `Int64.Type` | Null or negative → `0` |
| AnnualBudget | `type number` | Null or negative → `0` |
| Location | `type text` | `Text.Proper(Text.Trim(_))` |

</details>

<details>
<summary><b>FactPayroll</b> (10 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
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

</details>

<details>
<summary><b>FactPerformanceReviews</b> (9 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
| ReviewID | `type text` | — |
| EmployeeID | `type text` | — |
| ReviewDate | `type date` | — |
| ReviewerID | `type text` | — |
| PerformanceRating | `type text` | `Text.Proper(Text.Trim(_))` |
| GoalsMet | `type text` | `Text.Trim` |
| Strengths | `type text` | Null → `""`, else `Text.Trim` |
| AreasForImprovement | `type text` | Null → `""`, else `Text.Trim` |
| OverallScore | `type number` | Clamped to **0–100** |

</details>

<details>
<summary><b>FactRecruitment</b> (14 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
| RequisitionID | `type text` | — |
| DepartmentID | `type text` | — |
| JobTitle | `type text` | `Text.Proper(Text.Trim(_))` |
| OpenDate | `type date` | — |
| CloseDate | `type date` | — (nullable) |
| Status | `type text` | `Text.Proper(Text.Trim(_))` |
| ApplicationsReceived | `Int64.Type` | Null or negative → `0` |
| Interviewed | `Int64.Type` | Null or negative → `0` |
| OffersExtended | `Int64.Type` | Null or negative → `0` |
| OfferAccepted | `Int64.Type` | Null or negative → `0` |
| HiringManagerID | `type text` | — |
| SalaryRangeMin | `type number` | Null or negative → `0` |
| SalaryRangeMax | `type number` | Null or negative → `0` |
| TimeToFillDays | `Int64.Type` | — (nullable) |

</details>

---

## 📦 DF_Operations

**Source:** `Files/DimBooks.csv`, `DimAuthors.csv`, `DimCustomers.csv`, `DimGeography.csv`, `DimWarehouses.csv`, `FactOrders.csv`, `FactInventory.csv`, `FactReturns.csv`

<details>
<summary><b>DimBooks</b> (13 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
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

</details>

<details>
<summary><b>DimAuthors</b> (13 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
| AuthorID | `type text` | — |
| FirstName | `type text` | `Text.Proper(Text.Trim(_))` |
| LastName | `type text` | `Text.Proper(Text.Trim(_))` |
| PenName | `type text` | `Text.Trim` (nullable) |
| AgentName | `type text` | `Text.Proper(Text.Trim(_))` (nullable) |
| AgentCompany | `type text` | `Text.Trim` (nullable) |
| ContractStartDate | `type date` | — |
| ContractEndDate | `type date` | — |
| RoyaltyRate | `type number` | Normalised to 0–1 (> 1 ÷ 100) |
| AdvanceAmount | `type number` | Null or negative → `0` |
| Genre | `type text` | `Text.Proper(Text.Trim(_))` |
| Nationality | `type text` | `Text.Proper(Text.Trim(_))` |
| BookCount | `Int64.Type` | — |

</details>

<details>
<summary><b>DimCustomers</b> (13 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
| CustomerID | `type text` | — |
| CustomerName | `type text` | `Text.Trim` |
| CustomerType | `type text` | `Text.Proper(Text.Trim(_))` |
| ContactEmail | `type text` | `Text.Lower(Text.Trim(_))` |
| City | `type text` | `Text.Proper(Text.Trim(_))` |
| State | `type text` | `Text.Proper(Text.Trim(_))` |
| Country | `type text` | `Text.Proper(Text.Trim(_))` |
| Region | `type text` | `Text.Proper(Text.Trim(_))` |
| GeoID | `type text` | — |
| CreditLimit | `type number` | Null or negative → `0` |
| PaymentTerms | `type text` | `Text.Trim` (nullable) |
| IsActive | `type logical` | — |
| AccountOpenDate | `type date` | — |

</details>

<details>
<summary><b>DimGeography</b> (13 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
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

</details>

<details>
<summary><b>DimWarehouses</b> (12 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
| WarehouseID | `type text` | — |
| WarehouseName | `type text` | `Text.Trim` |
| Address | `type text` | `Text.Trim` (nullable) |
| City | `type text` | `Text.Proper(Text.Trim(_))` |
| State | `type text` | `Text.Proper(Text.Trim(_))` |
| Country | `type text` | `Text.Proper(Text.Trim(_))` |
| SquareFootage | `Int64.Type` | Null or negative → `0` |
| MaxCapacityUnits | `Int64.Type` | Null or negative → `0` |
| CurrentUtilization | `type number` | Normalised to 0–1 (> 1 ÷ 100, clamped) |
| ManagerID | `type text` | — |
| MonthlyRent | `type number` | Null or negative → `0` |
| IsActive | `type logical` | — |

</details>

<details>
<summary><b>FactOrders</b> (14 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
| OrderID | `type text` | — |
| OrderDate | `type date` | — |
| CustomerID | `type text` | — |
| BookID | `type text` | — |
| Quantity | `Int64.Type` | Null or negative → `0` |
| UnitPrice | `type number` | Null or negative → `0` |
| Discount | `type number` | Normalised to 0–1 (> 1 ÷ 100) |
| TotalAmount | `type number` | Null or negative → `0` |
| OrderStatus | `type text` | `Text.Proper(Text.Trim(_))` |
| ShipDate | `type date` | — |
| DeliveryDate | `type date` | — |
| WarehouseID | `type text` | — (nullable for digital) |
| SalesRepID | `type text` | — |
| Channel | `type text` | `Text.Proper(Text.Trim(_))` |

</details>

<details>
<summary><b>FactInventory</b> (13 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
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

</details>

<details>
<summary><b>FactReturns</b> (11 columns)</summary>

| Column | M Type | Transformation |
|--------|--------|----------------|
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

</details>

---

## 🚀 Deployment

### Automated (recommended)

```powershell
# Full deployment (creates dataflows + sets destinations automatically)
.\deploy\Deploy-Pipeline.ps1 -WorkspaceId "<your-workspace-guid>"

# Re-apply destinations after editing dataflows in the portal
.\deploy\Update-DataflowDestinations.ps1 -WorkspaceId "<your-workspace-guid>"
```

### Manual (Portal)

1. **+ New** → **Dataflow Gen2** → Name: `DF_Finance` / `DF_HR` / `DF_Operations`
2. **Get Data** → **Lakehouse** → select `BronzeLH` → `Files/`
3. Add each CSV, apply transformations as listed above
4. **Data Destination** → Lakehouse `BronzeLH` → target table
5. **Publish** and wait for refresh

---

## 📅 Refresh Schedule

| Dataflow | Schedule | Time |
|----------|----------|------|
| DF_Operations | Daily | 5:30 AM UTC |
| DF_Finance | Daily | 6:00 AM UTC |
| DF_HR | Weekly | Mon 7:00 AM UTC |

---

## ✅ Data Quality Checks

After each run, verify:
- [ ] Row counts match source CSVs
- [ ] No nulls in ID/key fields
- [ ] Dates parsed correctly
- [ ] Decimal precision maintained
- [ ] Text casing consistent (Proper for names, Upper for currencies)
- [ ] Numeric ranges valid (0–100 scores, 0–1 rates)
- [ ] Foreign key relationships valid

---

<p align="center">
  <sub>Dataflows use Power Query M with centralised target parameters — see <code>definitions/dataflows/</code></sub>
</p>
