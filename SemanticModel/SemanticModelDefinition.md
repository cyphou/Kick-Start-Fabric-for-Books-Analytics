# ============================================================================
# Horizon Books - Semantic Model Definition
# Power BI Dataset (Semantic Model) for Fabric
# ============================================================================

## Model Name: HorizonBooks_SemanticModel

## Star Schema Relationships

### Relationship Definitions

| From Table | From Column | To Table | To Column | Cardinality | Cross-filter |
|---|---|---|---|---|---|
| FactFinancialTransactions | TransactionDate | DimDate | FullDate | Many-to-One | Single |
| FactFinancialTransactions | AccountID | DimAccounts | AccountID | Many-to-One | Single |
| FactFinancialTransactions | CostCenterID | DimCostCenters | CostCenterID | Many-to-One | Single |
| FactFinancialTransactions | BookID | DimBooks | BookID | Many-to-One | Single |
| FactBudget | AccountID | DimAccounts | AccountID | Many-to-One | Single |
| FactOrders | OrderDate | DimDate | FullDate | Many-to-One | Single |
| FactOrders | BookID | DimBooks | BookID | Many-to-One | Single |
| FactOrders | CustomerID | DimCustomers | CustomerID | Many-to-One | Single |
| FactOrders | SalesRepID | DimEmployees | EmployeeID | Many-to-One | Single |
| FactOrders | WarehouseID | DimWarehouses | WarehouseID | Many-to-One | Single |
| FactInventory | BookID | DimBooks | BookID | Many-to-One | Single |
| FactInventory | WarehouseID | DimWarehouses | WarehouseID | Many-to-One | Single |
| FactInventory | SnapshotDate | DimDate | FullDate | Many-to-One | Single |
| FactReturns | BookID | DimBooks | BookID | Many-to-One | Single |
| FactReturns | CustomerID | DimCustomers | CustomerID | Many-to-One | Single |
| FactReturns | ReturnDate | DimDate | FullDate | Many-to-One | Single |
| FactPayroll | EmployeeID | DimEmployees | EmployeeID | Many-to-One | Single |
| FactPayroll | PayDate | DimDate | FullDate | Many-to-One | Single |
| FactPerformanceReviews | EmployeeID | DimEmployees | EmployeeID | Many-to-One | Single |
| FactRecruitment | DepartmentID | DimDepartments | DepartmentID | Many-to-One | Single |
| DimEmployees | DepartmentID | DimDepartments | DepartmentID | Many-to-One | Single |
| DimBooks | AuthorID | DimAuthors | AuthorID | Many-to-One | Single |
| DimCustomers | GeoID | DimGeography | GeoID | Many-to-One | Single |
| DimEmployees | GeoID | DimGeography | GeoID | Many-to-One | Single |
| FactBudget | CostCenterID | DimCostCenters | CostCenterID | Many-to-One | Single |

---

## DAX Measures

### ═══════════════════════════════════════
### FINANCE MEASURES
### ═══════════════════════════════════════

```dax
// --- Revenue Measures ---

Total Revenue = 
CALCULATE(
    SUM(FactFinancialTransactions[Amount]),
    DimAccounts[AccountType] = "Revenue"
)

Book Sales Revenue = 
CALCULATE(
    SUM(FactFinancialTransactions[Amount]),
    DimAccounts[AccountID] = 1001
)

E-Book Sales Revenue = 
CALCULATE(
    SUM(FactFinancialTransactions[Amount]),
    DimAccounts[AccountID] = 1002
)

Audiobook Revenue = 
CALCULATE(
    SUM(FactFinancialTransactions[Amount]),
    DimAccounts[AccountID] = 1003
)

Rights & Licensing Revenue = 
CALCULATE(
    SUM(FactFinancialTransactions[Amount]),
    DimAccounts[AccountCategory] = "Licensing"
)

Digital Revenue Share = 
DIVIDE(
    [E-Book Sales Revenue] + [Audiobook Revenue],
    [Total Revenue],
    0
)

// --- Cost Measures ---

Total COGS = 
CALCULATE(
    ABS(SUM(FactFinancialTransactions[Amount])),
    DimAccounts[AccountCategory] = "Cost of Goods Sold"
)

Total Royalties = 
CALCULATE(
    ABS(SUM(FactFinancialTransactions[Amount])),
    DimAccounts[AccountCategory] = "Royalties"
)

Total Marketing Spend = 
CALCULATE(
    ABS(SUM(FactFinancialTransactions[Amount])),
    DimAccounts[AccountCategory] = "Marketing"
)

Total Distribution Cost = 
CALCULATE(
    ABS(SUM(FactFinancialTransactions[Amount])),
    DimAccounts[AccountCategory] = "Distribution"
)

Total Operating Expenses = 
CALCULATE(
    ABS(SUM(FactFinancialTransactions[Amount])),
    DimAccounts[AccountType] = "Expense"
)

// --- Profitability Measures ---

Gross Profit = [Total Revenue] - [Total COGS]

Gross Margin % = DIVIDE([Gross Profit], [Total Revenue], 0)

Operating Profit = [Total Revenue] - [Total Operating Expenses]

Operating Margin % = DIVIDE([Operating Profit], [Total Revenue], 0)

// --- Budget Measures ---

Budget Amount = SUM(FactBudget[BudgetAmount])

Actual Amount = SUM(FactBudget[ActualAmount])

Budget Variance = SUM(FactBudget[Variance])

Budget Variance % = 
DIVIDE(
    [Budget Variance],
    ABS([Budget Amount]),
    0
)

Budget Attainment = 
DIVIDE(
    [Actual Amount],
    [Budget Amount],
    0
)

// --- YoY / Growth Measures ---

Revenue PY = 
CALCULATE(
    [Total Revenue],
    DATEADD(DimDate[FullDate], -1, YEAR)
)

Revenue YoY Growth = 
DIVIDE(
    [Total Revenue] - [Revenue PY],
    ABS([Revenue PY]),
    0
)

Revenue QoQ = 
VAR CurrentQ = [Total Revenue]
VAR PriorQ = CALCULATE([Total Revenue], DATEADD(DimDate[FullDate], -1, QUARTER))
RETURN DIVIDE(CurrentQ - PriorQ, ABS(PriorQ), 0)
```

### ═══════════════════════════════════════
### OPERATIONS / SALES MEASURES
### ═══════════════════════════════════════

```dax
// --- Order Measures ---

Total Orders = DISTINCTCOUNT(FactOrders[OrderID])

Total Units Sold = SUM(FactOrders[Quantity])

Order Revenue = SUM(FactOrders[TotalAmount])

Average Order Value = DIVIDE([Order Revenue], [Total Orders], 0)

Average Discount = AVERAGE(FactOrders[Discount])

Average Selling Price = DIVIDE([Order Revenue], [Total Units Sold], 0)

// --- Channel Measures ---

Online Revenue = 
CALCULATE([Order Revenue], FactOrders[Channel] = "Online")

Retail Revenue = 
CALCULATE([Order Revenue], FactOrders[Channel] = "Retail")

Digital Revenue = 
CALCULATE([Order Revenue], FactOrders[Channel] = "Digital")

International Revenue = 
CALCULATE([Order Revenue], FactOrders[Channel] = "International")

Online Revenue Share = DIVIDE([Online Revenue], [Order Revenue], 0)

// --- Customer Measures ---

Active Customers = DISTINCTCOUNT(FactOrders[CustomerID])

New Customers = 
CALCULATE(
    DISTINCTCOUNT(FactOrders[CustomerID]),
    FILTER(
        VALUES(FactOrders[CustomerID]),
        CALCULATE(MIN(FactOrders[OrderDate])) >= MIN(DimDate[FullDate])
    )
)

Revenue per Customer = DIVIDE([Order Revenue], [Active Customers], 0)

// --- Inventory Measures ---

Current Inventory Value = 
CALCULATE(
    SUM(FactInventory[TotalInventoryValue]),
    LASTDATE(FactInventory[SnapshotDate])
)

Current Units on Hand = 
CALCULATE(
    SUM(FactInventory[QuantityOnHand]),
    LASTDATE(FactInventory[SnapshotDate])
)

Avg Days of Supply = 
CALCULATE(
    AVERAGE(FactInventory[DaysOfSupply]),
    LASTDATE(FactInventory[SnapshotDate])
)

Low Stock Items = 
CALCULATE(
    DISTINCTCOUNT(FactInventory[BookID]),
    FactInventory[Status] IN {"Low Stock", "Critical"},
    LASTDATE(FactInventory[SnapshotDate])
)

Inventory Turnover = 
DIVIDE(
    [Total COGS],
    [Current Inventory Value],
    0
)

// --- Returns Measures ---

Total Returns = SUM(FactReturns[Quantity])

Total Refunds = SUM(FactReturns[RefundAmount])

Return Rate = DIVIDE([Total Returns], [Total Units Sold], 0)

Damaged Returns = 
CALCULATE(
    SUM(FactReturns[Quantity]),
    FactReturns[Condition] = "Damaged"
)

Damage Rate = DIVIDE([Damaged Returns], [Total Returns], 0)

// --- Fulfillment Measures ---

Avg Ship Days = 
AVERAGE(
    DATEDIFF(FactOrders[OrderDate], FactOrders[ShipDate], DAY)
)

Avg Delivery Days = 
AVERAGE(
    DATEDIFF(FactOrders[OrderDate], FactOrders[DeliveryDate], DAY)
)

On Time Delivery Rate = 
DIVIDE(
    CALCULATE(
        COUNTROWS(FactOrders),
        DATEDIFF(FactOrders[OrderDate], FactOrders[DeliveryDate], DAY) <= 7
    ),
    COUNTROWS(FactOrders),
    0
)
```

### ═══════════════════════════════════════
### HR MEASURES
### ═══════════════════════════════════════

```dax
// --- Headcount and Workforce ---

Total Headcount = 
CALCULATE(
    COUNTROWS(DimEmployees),
    DimEmployees[IsActive] = TRUE()
)

Full Time Employees = 
CALCULATE(
    COUNTROWS(DimEmployees),
    DimEmployees[IsActive] = TRUE(),
    DimEmployees[EmploymentType] = "Full-Time"
)

Part Time & Contract = 
CALCULATE(
    COUNTROWS(DimEmployees),
    DimEmployees[IsActive] = TRUE(),
    DimEmployees[EmploymentType] IN {"Part-Time", "Contract"}
)

Avg Tenure Years = 
AVERAGEX(
    FILTER(DimEmployees, DimEmployees[IsActive] = TRUE()),
    DATEDIFF(DimEmployees[HireDate], TODAY(), YEAR)
)

// --- Compensation Measures ---

Total Payroll Cost = SUM(FactPayroll[NetPay])

Total Base Salary = SUM(FactPayroll[BaseSalary])

Total Bonus Paid = SUM(FactPayroll[Bonus])

Total Overtime = SUM(FactPayroll[Overtime])

Avg Salary per Employee = 
DIVIDE(
    [Total Base Salary],
    DISTINCTCOUNT(FactPayroll[EmployeeID]),
    0
)

Bonus as % of Base = 
DIVIDE([Total Bonus Paid], [Total Base Salary], 0)

Payroll Cost per Revenue Dollar = 
DIVIDE(
    [Total Payroll Cost],
    [Total Revenue],
    0
)

// --- Performance Measures ---

Avg Performance Score = AVERAGE(FactPerformanceReviews[OverallScore])

Top Performers = 
CALCULATE(
    COUNTROWS(FactPerformanceReviews),
    FactPerformanceReviews[PerformanceRating] IN {"Exceeds Expectations", "Outstanding"}
)

Top Performer Rate = 
DIVIDE(
    [Top Performers],
    COUNTROWS(FactPerformanceReviews),
    0
)

// --- Recruitment Measures ---

Open Positions = 
CALCULATE(
    COUNTROWS(FactRecruitment),
    FactRecruitment[Status] = "Open"
)

Avg Time to Fill = 
CALCULATE(
    AVERAGE(FactRecruitment[TimeToFillDays]),
    FactRecruitment[Status] = "Filled"
)

Offer Acceptance Rate = 
DIVIDE(
    SUM(FactRecruitment[OfferAccepted]),
    SUM(FactRecruitment[OffersExtended]),
    0
)

Application to Interview Rate = 
DIVIDE(
    SUM(FactRecruitment[Interviewed]),
    SUM(FactRecruitment[ApplicationsReceived]),
    0
)

Cost per Hire = 
DIVIDE(
    CALCULATE(ABS(SUM(FactFinancialTransactions[Amount])), DimAccounts[AccountID] = 1050),
    CALCULATE(COUNTROWS(FactRecruitment), FactRecruitment[Status] = "Filled"),
    0
)

Revenue per Employee = 
DIVIDE([Total Revenue], [Total Headcount], 0)
```

### ═══════════════════════════════════════
### GEOGRAPHIC MEASURES
### ═══════════════════════════════════════

```dax
// --- Geographic Sales ---

Revenue by Country = 
SUMMARIZE(
    FactOrders,
    DimGeography[Country],
    "Revenue", SUM(FactOrders[TotalAmount])
)

International Customer Count = 
CALCULATE(
    DISTINCTCOUNT(DimCustomers[CustomerID]),
    DimGeography[Country] <> "United States"
)

Domestic Customer Count = 
CALCULATE(
    DISTINCTCOUNT(DimCustomers[CustomerID]),
    DimGeography[Country] = "United States"
)

International Revenue = 
CALCULATE(
    SUM(FactOrders[TotalAmount]),
    DimGeography[Country] <> "United States"
)

Domestic Revenue = 
CALCULATE(
    SUM(FactOrders[TotalAmount]),
    DimGeography[Country] = "United States"
)

International Revenue % = 
DIVIDE([International Revenue], [Order Revenue], 0)

Countries Served = 
DISTINCTCOUNT(DimGeography[Country])

Continents Served = 
DISTINCTCOUNT(DimGeography[Continent])

Revenue per Country = 
DIVIDE(
    [Order Revenue],
    [Countries Served],
    0
)

Employee Locations = 
CALCULATE(
    DISTINCTCOUNT(DimGeography[City]),
    RELATEDTABLE(DimEmployees)
)
```

---

## Display Folders (Organize in Semantic Model)

| Folder | Measures |
|---|---|
| Finance \ Revenue | Total Revenue, Book Sales Revenue, E-Book Sales Revenue, Audiobook Revenue, Rights & Licensing Revenue, Digital Revenue Share |
| Finance \ Costs | Total COGS, Total Royalties, Total Marketing Spend, Total Distribution Cost, Total Operating Expenses |
| Finance \ Profitability | Gross Profit, Gross Margin %, Operating Profit, Operating Margin % |
| Finance \ Budget | Budget Amount, Actual Amount, Budget Variance, Budget Variance %, Budget Attainment |
| Finance \ Growth | Revenue PY, Revenue YoY Growth, Revenue QoQ |
| Operations \ Orders | Total Orders, Total Units Sold, Order Revenue, Average Order Value, Average Discount, Average Selling Price |
| Operations \ Channels | Online Revenue, Retail Revenue, Digital Revenue, International Revenue, Online Revenue Share |
| Operations \ Customers | Active Customers, New Customers, Revenue per Customer |
| Operations \ Inventory | Current Inventory Value, Current Units on Hand, Avg Days of Supply, Low Stock Items, Inventory Turnover |
| Operations \ Returns | Total Returns, Total Refunds, Return Rate, Damaged Returns, Damage Rate |
| Operations \ Fulfillment | Avg Ship Days, Avg Delivery Days, On Time Delivery Rate |
| HR \ Workforce | Total Headcount, Full Time Employees, Part Time & Contract, Avg Tenure Years |
| HR \ Compensation | Total Payroll Cost, Total Base Salary, Total Bonus Paid, Total Overtime, Avg Salary per Employee, Bonus as % of Base, Payroll Cost per Revenue Dollar |
| HR \ Performance | Avg Performance Score, Top Performers, Top Performer Rate |
| HR \ Recruitment | Open Positions, Avg Time to Fill, Offer Acceptance Rate, Application to Interview Rate, Cost per Hire, Revenue per Employee |
| Geographic | International Customer Count, Domestic Customer Count, International Revenue, Domestic Revenue, International Revenue %, Countries Served, Continents Served, Revenue per Country, Employee Locations |

---

## Row-Level Security (Optional)

### Role: Department Managers
```dax
// Filter DimEmployees to show only their department
[DepartmentID] = LOOKUPVALUE(
    DimEmployees[DepartmentID],
    DimEmployees[Email],
    USERPRINCIPALNAME()
)
```

### Role: Regional Sales
```dax
// Filter customers by region
[Region] = "Northeast"  // or dynamic based on user mapping
```
