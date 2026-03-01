# ============================================================================
# Horizon Books - Power BI Report Specifications
# Report Pages, Visuals, and Layout Guide
# ============================================================================

## Report Name: Horizon Books Publishing Analytics

### Theme & Branding
- **Primary Color:** #1B3A5C (Dark Navy Blue)
- **Secondary Color:** #E8A838 (Golden Amber)
- **Accent 1:** #3A8FBF (Sky Blue)
- **Accent 2:** #6B4C9A (Royal Purple)
- **Accent 3:** #2ECC71 (Emerald Green)
- **Background:** #F5F6FA (Light Gray)
- **Font:** Segoe UI
- **Company Logo:** Horizon Books Publishing (top-left corner)

---

## Page 1: Executive Dashboard (Landing Page)

### Layout: 4 KPI cards on top + 2 charts middle + 1 table bottom

| Visual | Type | Measures/Fields | Size |
|---|---|---|---|
| Total Revenue Card | Card | [Total Revenue] formatted $X.XM | 1/4 width top |
| Gross Margin Card | Card | [Gross Margin %] formatted XX.X% | 1/4 width top |
| Total Orders Card | Card | [Total Orders] formatted #,### | 1/4 width top |
| Active Headcount Card | Card | [Total Headcount] | 1/4 width top |
| Revenue by Quarter | Clustered Column | X: FiscalQuarter, Y: [Total Revenue], Legend: FiscalYear | Half width middle-left |
| Revenue by Channel | Donut Chart | Values: [Order Revenue], Legend: Channel | Half width middle-right |
| Top 10 Books by Revenue | Table | Title, AuthorName, [Order Revenue], [Total Units Sold], [Return Rate] | Full width bottom |

### Slicers (top ribbon):
- Fiscal Year (dropdown)
- Genre (dropdown)
- Imprint (dropdown)

---

## Page 2: Finance - P&L Analysis

### Layout: KPI row + waterfall + matrix

| Visual | Type | Measures/Fields | Size |
|---|---|---|---|
| Revenue Card | Card | [Total Revenue] | 1/5 width |
| COGS Card | Card | [Total COGS] | 1/5 width |
| Gross Profit Card | Card | [Gross Profit] | 1/5 width |
| OpEx Card | Card | [Total Operating Expenses] | 1/5 width |
| Operating Margin Card | Card | [Operating Margin %] | 1/5 width |
| P&L Waterfall | Waterfall Chart | Categories: Revenue items → COGS → Gross Profit → OpEx → Operating Profit | Full width middle |
| Monthly Trend | Line Chart | X: Month, Y: [Total Revenue] & [Total Operating Expenses] | Half width |
| Account Breakdown | Matrix | Rows: AccountCategory > AccountName, Values: [Total Amount] by Quarter | Half width |

### Slicers:
- Fiscal Year, Fiscal Quarter

---

## Page 3: Finance - Budget vs Actual

### Layout: KPI row + variance analysis

| Visual | Type | Measures/Fields | Size |
|---|---|---|---|
| Budget Attainment Gauge | Gauge | [Budget Attainment] target 100% | 1/3 width |
| Total Variance Card | Card | [Budget Variance] with conditional color | 1/3 width |
| Variance % Card | Card | [Budget Variance %] | 1/3 width |
| Budget vs Actual by Account | Grouped Bar | X: AccountName, Y: [Budget Amount] & [Actual Amount] | Full width middle |
| Variance by Quarter | Clustered Column | X: FiscalQuarter, Y: [Budget Variance], Color: conditional (green positive, red negative) | Half width |
| Detailed Variance Table | Table | AccountCategory, AccountName, Budget, Actual, Variance, Variance% | Half width |

---

## Page 4: Operations - Book Performance

### Layout: Top performers + trend analysis

| Visual | Type | Measures/Fields | Size |
|---|---|---|---|
| Best Seller Highlight | Card | Top book by revenue (dynamic) | 1/3 width |
| Total Titles Active | Card | Count of DimBooks | 1/3 width |
| Avg Selling Price | Card | [Average Selling Price] | 1/3 width |
| Sales by Genre | Treemap | Group: Genre, Values: [Order Revenue] | Half width |
| Top 10 Books Bar | Horizontal Bar | Y: Title, X: [Order Revenue], Sorted desc | Half width |
| Monthly Book Sales Trend | Area Chart | X: Month, Y: [Order Revenue], Legend: Top 5 Books | Full width middle |
| Book Detail Table | Table | Title, Author, Genre, Imprint, ListPrice, UnitsSold, Revenue, ReturnRate, Margin | Full width bottom |

### Slicers:
- Genre, Imprint, Publish Date Range

---

## Page 5: Operations - Customer & Distribution

### Layout: Customer analysis + geographic

| Visual | Type | Measures/Fields | Size |
|---|---|---|---|
| Active Customers | Card | [Active Customers] | 1/4 width |
| Revenue per Customer | Card | [Revenue per Customer] | 1/4 width |
| Avg Order Value | Card | [Average Order Value] | 1/4 width |
| On-Time Delivery | Card | [On Time Delivery Rate] formatted % | 1/4 width |
| Revenue by Customer Type | Stacked Bar | Y: CustomerType, X: [Order Revenue] | Half width |
| Top Customers Table | Table | CustomerName, Type, Region, Orders, Revenue, AvgDiscount | Half width |
| Revenue by Region | Pie Chart or Map | Values: [Order Revenue], Legend: Region | Half width |
| Order Volume Trend | Line + Column | X: Month, Column: [Total Orders], Line: [Average Order Value] | Half width |

### Drill-through:
- Click any customer → Customer Detail Page (shows all orders for that customer)

---

## Page 6: Geographic Analysis (NEW)

### Layout: Map-centric global sales view

| Visual | Type | Measures/Fields | Size |
|---|---|---|---|
| Countries Served | Card | [Countries Served] | 1/5 width |
| International Revenue | Card | [International Revenue] formatted $X.XM | 1/5 width |
| International Revenue % | Card | [International Revenue %] formatted XX.X% | 1/5 width |
| International Customers | Card | [International Customer Count] | 1/5 width |
| Employee Locations | Card | [Employee Locations] | 1/5 width |
| Global Sales Map | Azure Map / Filled Map | Location: DimGeography[Latitude], DimGeography[Longitude], Size: [Order Revenue], Color: [Order Revenue] gradient, Tooltip: Country, City, CustomerName, Revenue | Full width large |
| Revenue by Continent | Stacked Column | X: Continent, Y: [Order Revenue], Legend: CustomerType | Half width |
| Revenue by Country Table | Table | Country, Continent, Region, CustomerCount, TotalOrders, TotalRevenue, AvgDiscount | Half width |
| Top International Customers | Horizontal Bar | Y: CustomerName, X: [Order Revenue], filtered to non-US | Full width bottom |

### Slicers:
- Continent (dropdown)
- Country (multi-select)
- Customer Type (dropdown)

### Drill-through:
- Click any country on map → Filtered view showing all customers and orders for that country

---

## Page 7: Operations - Inventory & Supply Chain

### Layout: Inventory health dashboard

| Visual | Type | Measures/Fields | Size |
|---|---|---|---|
| Total Inventory Value | Card | [Current Inventory Value] | 1/4 width |
| Low Stock Alerts | Card | [Low Stock Items] with red conditional | 1/4 width |
| Avg Days of Supply | Card | [Avg Days of Supply] | 1/4 width |
| Inventory Turnover | Card | [Inventory Turnover] | 1/4 width |
| Inventory Status Distribution | Donut | Values: Count of BookID, Legend: Status | 1/3 width |
| Days of Supply by Book | Horizontal Bar | Y: Title, X: DaysOfSupply, Color: conditional (red < 30, yellow < 60, green) | 1/3 width |
| Inventory Value by Genre | Stacked Bar | Y: Genre, X: TotalInventoryValue | 1/3 width |
| Inventory Trend | Line Chart | X: SnapshotDate, Y: QuantityOnHand for selected books | Full width |
| Return Analysis | Matrix | Rows: BookTitle > Reason, Values: ReturnQty, RefundAmt, ReturnRate | Full width |

---

## Page 8: HR - Workforce Overview

### Layout: People analytics dashboard

| Visual | Type | Measures/Fields | Size |
|---|---|---|---|
| Total Headcount | Card | [Total Headcount] | 1/4 width |
| Avg Tenure | Card | [Avg Tenure Years] formatted X.X yrs | 1/4 width |
| Open Positions | Card | [Open Positions] | 1/4 width |
| Revenue per Employee | Card | [Revenue per Employee] | 1/4 width |
| Headcount by Department | Horizontal Bar | Y: DepartmentName, X: HeadCount | Half width |
| Employment Type | Donut | Values: Count, Legend: EmploymentType | Half width |
| Headcount by Location | Pie | Values: Count, Legend: Location | 1/3 width |
| Org Structure | Decomp Tree | Levels: Department > Manager > Employee | 2/3 width full |

---

## Page 9: HR - Compensation & Performance

### Layout: Pay analysis + performance

| Visual | Type | Measures/Fields | Size |
|---|---|---|---|
| Total Payroll Card | Card | [Total Payroll Cost] | 1/4 width |
| Avg Salary Card | Card | [Avg Salary per Employee] | 1/4 width |
| Avg Perf Score Card | KPI | [Avg Performance Score] target 4.0 | 1/4 width |
| Top Performer Rate | Card | [Top Performer Rate] | 1/4 width |
| Salary Distribution by Dept | Box & Whisker or Bar | X: Department, Y: BaseSalary | Half width |
| Bonus vs Base by Dept | Stacked Column | X: Department, Y: Base + Bonus | Half width |
| Performance Score Distribution | Histogram / Column | X: Rating Category, Y: Count | Half width |
| Performance Detail | Table | Employee, Department, Rating, GoalsMet, Score, Strengths | Half width |
| Payroll Cost per Revenue | Line | X: Month, Y: [Payroll Cost per Revenue Dollar] | Full width |

---

## Page 10: HR - Recruitment Pipeline

### Layout: Hiring funnel + metrics

| Visual | Type | Measures/Fields | Size |
|---|---|---|---|
| Open Reqs | Card | [Open Positions] | 1/3 width |
| Avg Time to Fill | Card | [Avg Time to Fill] formatted ## days | 1/3 width |
| Offer Accept Rate | Card | [Offer Acceptance Rate] formatted % | 1/3 width |
| Recruitment Funnel | Funnel | Stages: Applications → Interviewed → Offers → Accepted | Half width |
| Hiring by Department | Bar | X: Department, Y: Filled count | Half width |
| Salary Range Analysis | Range Column | X: JobTitle, Y: Min-Max salary range | Full width |
| Open Positions Table | Table | Requisition, Dept, Title, OpenDate, Apps, Status, DaysOpen | Full width |

---

## Bookmarks & Navigation

### Navigation Bar (left side or top tabs):
1. Executive Dashboard
2. Finance - P&L
3. Finance - Budget
4. Book Performance
5. Customers & Distribution
6. Geographic Analysis
7. Inventory & Supply
8. Workforce
9. Compensation & Performance
10. Recruitment

### Bookmarks:
- "Current Quarter View" - Pre-filtered to latest quarter
- "Holiday Season Analysis" - Filtered to Q4
- "Bestseller Deep Dive" - Filtered to Winter's Promise (BK-017)
- "Critical Inventory Alert" - Shows only Low Stock/Critical items
- "International Markets" - Filtered to non-US geographies

---

## Tooltips (Custom):

### Book Tooltip:
When hovering on any book reference: Title, Author, Genre, ISBN, Publish Date, List Price, Total Units Sold, Revenue

### Customer Tooltip:
When hovering on any customer: Name, Type, Region, First Order Date, Total Orders, Total Revenue, Avg Discount

### Employee Tooltip:
When hovering on any employee: Name, Title, Department, Hire Date, Tenure, Last Performance Score
