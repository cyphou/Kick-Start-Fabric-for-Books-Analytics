<p align="center">
  <img src="../assets/workspace-logo.png" alt="Horizon Books" width="80"/>
</p>

<h1 align="center">Semantic Model Definition</h1>

<p align="center">
  <strong>Power BI Direct Lake Semantic Model — 27 relationships, 96 DAX measures, 23 tables</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/model-Direct%20Lake-0078D4?style=flat-square&logo=microsoft&logoColor=white" alt="Direct Lake"/>
  <img src="https://img.shields.io/badge/relationships-27-blue?style=flat-square" alt="Relationships"/>
  <img src="https://img.shields.io/badge/DAX%20measures-96-purple?style=flat-square" alt="Measures"/>
  <img src="https://img.shields.io/badge/tables-23-orange?style=flat-square" alt="Tables"/>
</p>

<p align="center">
  <a href="#-star-schema-relationships">Relationships</a> •
  <a href="#-dax-measures">DAX Measures</a> •
  <a href="#-display-folders">Folders</a> •
  <a href="#-forecast-tables">Forecasts</a> •
  <a href="#-row-level-security">RLS</a>
</p>

---

## ⭐ Star Schema Relationships

| # | From Table | From Column | To Table | To Column | Cardinality | Notes |
|---|-----------|-------------|----------|-----------|-------------|-------|
| 1 | FactFinancialTransactions | TransactionDate | DimDate | FullDate | Many→One | |
| 2 | FactFinancialTransactions | AccountID | DimAccounts | AccountID | Many→One | |
| 3 | FactFinancialTransactions | CostCenterID | DimCostCenters | CostCenterID | Many→One | |
| 4 | FactFinancialTransactions | BookID | DimBooks | BookID | Many→One | |
| 5 | FactBudget | AccountID | DimAccounts | AccountID | Many→One | |
| 6 | FactBudget | CostCenterID | DimCostCenters | CostCenterID | Many→One | |
| 7 | FactOrders | OrderDate | DimDate | FullDate | Many→One | |
| 8 | FactOrders | BookID | DimBooks | BookID | Many→One | |
| 9 | FactOrders | CustomerID | DimCustomers | CustomerID | Many→One | |
| 10 | FactOrders | SalesRepID | DimEmployees | EmployeeID | Many→One | |
| 11 | FactOrders | WarehouseID | DimWarehouses | WarehouseID | Many→One | |
| 12 | FactInventory | BookID | DimBooks | BookID | Many→One | |
| 13 | FactInventory | WarehouseID | DimWarehouses | WarehouseID | Many→One | |
| 14 | FactInventory | SnapshotDate | DimDate | FullDate | Many→One | |
| 15 | FactReturns | BookID | DimBooks | BookID | Many→One | |
| 16 | FactReturns | CustomerID | DimCustomers | CustomerID | Many→One | |
| 17 | FactReturns | ReturnDate | DimDate | FullDate | Many→One | |
| 18 | FactPayroll | EmployeeID | DimEmployees | EmployeeID | Many→One | |
| 19 | FactPayroll | PayDate | DimDate | FullDate | Many→One | |
| 20 | FactPerformanceReviews | EmployeeID | DimEmployees | EmployeeID | Many→One | |
| 21 | FactPerformanceReviews | ReviewDate | DimDate | FullDate | Many→One | |
| 22 | FactRecruitment | DepartmentID | DimDepartments | DepartmentID | Many→One | |
| 23 | FactRecruitment | OpenDate | DimDate | FullDate | Many→One | |
| 24 | DimEmployees | DepartmentID | DimDepartments | DepartmentID | Many→One | |
| 25 | DimBooks | AuthorID | DimAuthors | AuthorID | Many→One | |
| 26 | DimCustomers | GeoID | DimGeography | GeoID | Many→One | |
| 27 | DimEmployees | GeoID | DimGeography | GeoID | Many→One | **Inactive** (role-playing) |

---

## 📊 DAX Measures

### 💰 Finance Measures

<details>
<summary><b>Revenue Measures</b> (click to expand)</summary>

```dax
Total Revenue = 
CALCULATE(
    SUM(FactFinancialTransactions[Amount]),
    DimAccounts[AccountType] = "Revenue"
)

Book Sales Revenue = 
CALCULATE(SUM(FactFinancialTransactions[Amount]), DimAccounts[AccountID] = 1001)

E-Book Sales Revenue = 
CALCULATE(SUM(FactFinancialTransactions[Amount]), DimAccounts[AccountID] = 1002)

Audiobook Revenue = 
CALCULATE(SUM(FactFinancialTransactions[Amount]), DimAccounts[AccountID] = 1003)

Rights & Licensing Revenue = 
CALCULATE(SUM(FactFinancialTransactions[Amount]), DimAccounts[AccountCategory] = "Licensing")

Digital Revenue Share = 
DIVIDE([E-Book Sales Revenue] + [Audiobook Revenue], [Total Revenue], 0)
```

</details>

<details>
<summary><b>Cost Measures</b></summary>

```dax
Total COGS = 
CALCULATE(ABS(SUM(FactFinancialTransactions[Amount])), DimAccounts[AccountCategory] = "Cost of Goods Sold")

Total Royalties = 
CALCULATE(ABS(SUM(FactFinancialTransactions[Amount])), DimAccounts[AccountCategory] = "Royalties")

Total Marketing Spend = 
CALCULATE(ABS(SUM(FactFinancialTransactions[Amount])), DimAccounts[AccountCategory] = "Marketing")

Total Distribution Cost = 
CALCULATE(ABS(SUM(FactFinancialTransactions[Amount])), DimAccounts[AccountCategory] = "Distribution")

Total Operating Expenses = 
CALCULATE(ABS(SUM(FactFinancialTransactions[Amount])), DimAccounts[AccountType] = "Expense")
```

</details>

<details>
<summary><b>Profitability Measures</b></summary>

```dax
Gross Profit = [Total Revenue] - [Total COGS]
Gross Margin % = DIVIDE([Gross Profit], [Total Revenue], 0)
Operating Profit = [Total Revenue] - [Total Operating Expenses]
Operating Margin % = DIVIDE([Operating Profit], [Total Revenue], 0)
```

</details>

<details>
<summary><b>Budget Measures</b></summary>

```dax
Budget Amount = SUM(FactBudget[BudgetAmount])
Actual Amount = SUM(FactBudget[ActualAmount])
Budget Variance = SUM(FactBudget[Variance])
Budget Variance % = DIVIDE([Budget Variance], ABS([Budget Amount]), 0)
Budget Attainment = DIVIDE([Actual Amount], [Budget Amount], 0)
```

</details>

<details>
<summary><b>Growth Measures (YoY / QoQ)</b></summary>

```dax
Revenue PY = 
CALCULATE([Total Revenue], DATEADD(DimDate[FullDate], -1, YEAR))

Revenue YoY Growth = 
DIVIDE([Total Revenue] - [Revenue PY], ABS([Revenue PY]), 0)

Revenue QoQ = 
VAR CurrentQ = [Total Revenue]
VAR PriorQ = CALCULATE([Total Revenue], DATEADD(DimDate[FullDate], -1, QUARTER))
RETURN DIVIDE(CurrentQ - PriorQ, ABS(PriorQ), 0)
```

</details>

---

### 📦 Operations / Sales Measures

<details>
<summary><b>Order Measures</b></summary>

```dax
Total Orders = DISTINCTCOUNT(FactOrders[OrderID])
Total Units Sold = SUM(FactOrders[Quantity])
Order Revenue = SUM(FactOrders[TotalAmount])
Average Order Value = DIVIDE([Order Revenue], [Total Orders], 0)
Average Discount = AVERAGE(FactOrders[Discount])
Average Selling Price = DIVIDE([Order Revenue], [Total Units Sold], 0)
```

</details>

<details>
<summary><b>Channel Measures</b></summary>

```dax
Online Revenue = CALCULATE([Order Revenue], FactOrders[Channel] = "Online")
Retail Revenue = CALCULATE([Order Revenue], FactOrders[Channel] = "Retail")
Digital Revenue = CALCULATE([Order Revenue], FactOrders[Channel] = "Digital")
International Revenue = CALCULATE([Order Revenue], FactOrders[Channel] = "International")
Online Revenue Share = DIVIDE([Online Revenue], [Order Revenue], 0)
```

</details>

<details>
<summary><b>Customer Measures</b></summary>

```dax
Active Customers = DISTINCTCOUNT(FactOrders[CustomerID])

New Customers = 
CALCULATE(
    DISTINCTCOUNT(FactOrders[CustomerID]),
    FILTER(VALUES(FactOrders[CustomerID]),
        CALCULATE(MIN(FactOrders[OrderDate])) >= MIN(DimDate[FullDate]))
)

Revenue per Customer = DIVIDE([Order Revenue], [Active Customers], 0)
```

</details>

<details>
<summary><b>Inventory Measures</b></summary>

```dax
Current Inventory Value = 
CALCULATE(SUM(FactInventory[TotalInventoryValue]), LASTDATE(FactInventory[SnapshotDate]))

Current Units on Hand = 
CALCULATE(SUM(FactInventory[QuantityOnHand]), LASTDATE(FactInventory[SnapshotDate]))

Avg Days of Supply = 
CALCULATE(AVERAGE(FactInventory[DaysOfSupply]), LASTDATE(FactInventory[SnapshotDate]))

Low Stock Items = 
CALCULATE(DISTINCTCOUNT(FactInventory[BookID]),
    FactInventory[Status] IN {"Low Stock", "Critical"},
    LASTDATE(FactInventory[SnapshotDate]))

Inventory Turnover = DIVIDE([Total COGS], [Current Inventory Value], 0)
```

</details>

<details>
<summary><b>Returns & Fulfillment Measures</b></summary>

```dax
Total Returns = SUM(FactReturns[Quantity])
Total Refunds = SUM(FactReturns[RefundAmount])
Return Rate = DIVIDE([Total Returns], [Total Units Sold], 0)
Damaged Returns = CALCULATE(SUM(FactReturns[Quantity]), FactReturns[Condition] = "Damaged")
Damage Rate = DIVIDE([Damaged Returns], [Total Returns], 0)

Avg Ship Days = AVERAGE(DATEDIFF(FactOrders[OrderDate], FactOrders[ShipDate], DAY))
Avg Delivery Days = AVERAGE(DATEDIFF(FactOrders[OrderDate], FactOrders[DeliveryDate], DAY))
On Time Delivery Rate = 
DIVIDE(
    CALCULATE(COUNTROWS(FactOrders), DATEDIFF(FactOrders[OrderDate], FactOrders[DeliveryDate], DAY) <= 7),
    COUNTROWS(FactOrders), 0)
```

</details>

---

### 👥 HR Measures

<details>
<summary><b>Workforce & Compensation</b></summary>

```dax
Total Headcount = CALCULATE(COUNTROWS(DimEmployees), DimEmployees[IsActive] = TRUE())
Full Time Employees = CALCULATE(COUNTROWS(DimEmployees), DimEmployees[IsActive] = TRUE(), DimEmployees[EmploymentType] = "Full-Time")
Part Time & Contract = CALCULATE(COUNTROWS(DimEmployees), DimEmployees[IsActive] = TRUE(), DimEmployees[EmploymentType] IN {"Part-Time", "Contract"})
Avg Tenure Years = AVERAGEX(FILTER(DimEmployees, DimEmployees[IsActive] = TRUE()), DATEDIFF(DimEmployees[HireDate], TODAY(), YEAR))

Total Payroll Cost = SUM(FactPayroll[NetPay])
Total Base Salary = SUM(FactPayroll[BaseSalary])
Total Bonus Paid = SUM(FactPayroll[Bonus])
Total Overtime = SUM(FactPayroll[Overtime])
Avg Salary per Employee = DIVIDE([Total Base Salary], DISTINCTCOUNT(FactPayroll[EmployeeID]), 0)
Bonus as % of Base = DIVIDE([Total Bonus Paid], [Total Base Salary], 0)
Payroll Cost per Revenue Dollar = DIVIDE([Total Payroll Cost], [Total Revenue], 0)
```

</details>

<details>
<summary><b>Performance & Recruitment</b></summary>

```dax
Avg Performance Score = AVERAGE(FactPerformanceReviews[OverallScore])
Top Performers = CALCULATE(COUNTROWS(FactPerformanceReviews), FactPerformanceReviews[PerformanceRating] IN {"Exceeds Expectations", "Outstanding"})
Top Performer Rate = DIVIDE([Top Performers], COUNTROWS(FactPerformanceReviews), 0)

Open Positions = CALCULATE(COUNTROWS(FactRecruitment), FactRecruitment[Status] = "Open")
Avg Time to Fill = CALCULATE(AVERAGE(FactRecruitment[TimeToFillDays]), FactRecruitment[Status] = "Filled")
Offer Acceptance Rate = DIVIDE(SUM(FactRecruitment[OfferAccepted]), SUM(FactRecruitment[OffersExtended]), 0)
Application to Interview Rate = DIVIDE(SUM(FactRecruitment[Interviewed]), SUM(FactRecruitment[ApplicationsReceived]), 0)
Cost per Hire = DIVIDE(CALCULATE(ABS(SUM(FactFinancialTransactions[Amount])), DimAccounts[AccountID] = 1050), CALCULATE(COUNTROWS(FactRecruitment), FactRecruitment[Status] = "Filled"), 0)
Revenue per Employee = DIVIDE([Total Revenue], [Total Headcount], 0)
```

</details>

---

### 🌍 Geographic Measures

<details>
<summary><b>Geographic Sales</b></summary>

```dax
International Customer Count = CALCULATE(DISTINCTCOUNT(DimCustomers[CustomerID]), DimGeography[Country] <> "United States")
Domestic Customer Count = CALCULATE(DISTINCTCOUNT(DimCustomers[CustomerID]), DimGeography[Country] = "United States")
International Revenue = CALCULATE(SUM(FactOrders[TotalAmount]), DimGeography[Country] <> "United States")
Domestic Revenue = CALCULATE(SUM(FactOrders[TotalAmount]), DimGeography[Country] = "United States")
International Revenue % = DIVIDE([International Revenue], [Order Revenue], 0)
Countries Served = DISTINCTCOUNT(DimGeography[Country])
Continents Served = DISTINCTCOUNT(DimGeography[Continent])
Revenue per Country = DIVIDE([Order Revenue], [Countries Served], 0)
Employee Locations = CALCULATE(DISTINCTCOUNT(DimGeography[City]), RELATEDTABLE(DimEmployees))
```

</details>

---

## 📁 Display Folders

| Folder | Measures |
|--------|----------|
| **Finance \ Revenue** | Total Revenue, Book Sales Revenue, E-Book Sales Revenue, Audiobook Revenue, Rights & Licensing Revenue, Digital Revenue Share |
| **Finance \ Costs** | Total COGS, Total Royalties, Total Marketing Spend, Total Distribution Cost, Total Operating Expenses |
| **Finance \ Profitability** | Gross Profit, Gross Margin %, Operating Profit, Operating Margin % |
| **Finance \ Budget** | Budget Amount, Actual Amount, Budget Variance, Budget Variance %, Budget Attainment |
| **Finance \ Growth** | Revenue PY, Revenue YoY Growth, Revenue QoQ |
| **Operations \ Orders** | Total Orders, Total Units Sold, Order Revenue, Average Order Value, Average Discount, Average Selling Price |
| **Operations \ Channels** | Online Revenue, Retail Revenue, Digital Revenue, International Revenue, Online Revenue Share |
| **Operations \ Customers** | Active Customers, New Customers, Revenue per Customer |
| **Operations \ Inventory** | Current Inventory Value, Current Units on Hand, Avg Days of Supply, Low Stock Items, Inventory Turnover |
| **Operations \ Returns** | Total Returns, Total Refunds, Return Rate, Damaged Returns, Damage Rate |
| **Operations \ Fulfillment** | Avg Ship Days, Avg Delivery Days, On Time Delivery Rate |
| **HR \ Workforce** | Total Headcount, Full Time Employees, Part Time & Contract, Avg Tenure Years |
| **HR \ Compensation** | Total Payroll Cost, Total Base Salary, Total Bonus Paid, Total Overtime, Avg Salary per Employee, Bonus as % of Base, Payroll Cost per Revenue Dollar |
| **HR \ Performance** | Avg Performance Score, Top Performers, Top Performer Rate |
| **HR \ Recruitment** | Open Positions, Avg Time to Fill, Offer Acceptance Rate, Application to Interview Rate, Cost per Hire, Revenue per Employee |
| **Geographic** | International/Domestic Customer Count, International/Domestic Revenue, International Revenue %, Countries/Continents Served, Revenue per Country, Employee Locations |
| **Forecasting \ Sales** | Forecast Revenue, Revenue Lower/Upper Bound, Forecast vs Actual Revenue |
| **Forecasting \ Genre** | Forecast Unit Demand, Forecast Genre Revenue, Demand Confidence Range |
| **Forecasting \ Financial** | Forecast P&L Amount, Forecast P&L Lower, Forecast P&L Upper |
| **Forecasting \ Inventory** | Forecast Demand Units, Stock Coverage Months, Cumulative Forecast Demand |
| **Forecasting \ Workforce** | Forecast Workforce Value, Forecast Payroll, Forecast Headcount, Forecast Openings |

---

## 🔮 Forecast Tables

Generated by `04_Forecasting.py` (Holt-Winters, 6-month horizon, 95% confidence intervals):

| Table | Key Columns | Measures |
|-------|-------------|----------|
| **ForecastSalesRevenue** | Channel, Revenue, Orders, Customers | Forecast Revenue, Revenue Bounds, Forecast vs Actual |
| **ForecastGenreDemand** | Genre, UnitDemand, Revenue | Forecast Unit Demand, Genre Revenue, Confidence Range |
| **ForecastFinancial** | PLCategory, Amount, TransactionCount | Forecast P&L Amount, P&L Bounds |
| **ForecastInventoryDemand** | WarehouseID, UnitsDemanded, StockCoverMonths | Forecast Demand Units, Stock Coverage |
| **ForecastWorkforce** | Metric, Value (Headcount/Payroll/Openings) | Forecast Payroll, Headcount, Openings |

All tables include: `ForecastMonth`, `LowerBound`, `UpperBound`, `ForecastHorizon`, `RecordType`, `ForecastModel`, `_generated_at`

---

## 🔒 Row-Level Security

<details>
<summary><b>Optional RLS roles</b></summary>

### Role: Department Managers
```dax
[DepartmentID] = LOOKUPVALUE(
    DimEmployees[DepartmentID],
    DimEmployees[Email],
    USERPRINCIPALNAME()
)
```

### Role: Regional Sales
```dax
[Region] = "Northeast"  // Or dynamic based on user mapping
```

</details>

---

<p align="center">
  <sub>Model Name: <code>HorizonBooksModel</code> — Compatibility Level 1604 — Direct Lake on GoldLH</sub>
</p>
