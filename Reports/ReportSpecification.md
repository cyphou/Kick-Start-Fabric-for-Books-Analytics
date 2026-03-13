<p align="center">
  <img src="../assets/workspace-logo.png" alt="Horizon Books" width="80"/>
</p>

<h1 align="center">Power BI Report Specification</h1>

<p align="center">
  <strong>10-page Analytics Report + 5-page Forecasting Report — visuals, slicers, bookmarks, tooltips</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Power%20BI-F2C811?style=flat-square&logo=powerbi&logoColor=black" alt="Power BI"/>
  <img src="https://img.shields.io/badge/analytics%20pages-10-blue?style=flat-square" alt="Analytics Pages"/>
  <img src="https://img.shields.io/badge/forecast%20pages-5-purple?style=flat-square" alt="Forecast Pages"/>
  <img src="https://img.shields.io/badge/format-PBIR%20v4.0-green?style=flat-square" alt="PBIR"/>
</p>

<p align="center">
  <a href="#-theme--branding">Theme</a> •
  <a href="#-page-1-executive-dashboard">Pages</a> •
  <a href="#-bookmarks--navigation">Navigation</a> •
  <a href="#-tooltips">Tooltips</a>
</p>

---

## 🎨 Theme & Branding

| Property | Value |
|----------|-------|
| **Primary Color** | `#1B3A5C` (Dark Navy Blue) |
| **Secondary Color** | `#E8A838` (Golden Amber) |
| **Accent 1** | `#3A8FBF` (Sky Blue) |
| **Accent 2** | `#6B4C9A` (Royal Purple) |
| **Accent 3** | `#2ECC71` (Emerald Green) |
| **Background** | `#F5F6FA` (Light Gray) |
| **Font** | Segoe UI |
| **Logo** | Horizon Books Publishing (top-left corner) |

---

## 📊 Analytics Report — Horizon Books Publishing Analytics

### 📄 Page 1: Executive Dashboard

> **Layout:** 4 KPI cards + 2 charts + 1 table

| Visual | Type | Data | Size |
|--------|------|------|------|
| Total Revenue | Card | `[Total Revenue]` formatted $X.XM | 1/4 width top |
| Gross Margin | Card | `[Gross Margin %]` formatted XX.X% | 1/4 width top |
| Total Orders | Card | `[Total Orders]` formatted #,### | 1/4 width top |
| Active Headcount | Card | `[Total Headcount]` | 1/4 width top |
| Revenue by Quarter | Clustered Column | X: FiscalQuarter, Y: [Total Revenue], Legend: FiscalYear | Half width middle |
| Revenue by Channel | Donut | Values: [Order Revenue], Legend: Channel | Half width middle |
| Top 10 Books | Table | Title, Author, Revenue, Units, ReturnRate | Full width bottom |

**Slicers:** Fiscal Year, Genre, Imprint

---

### 📄 Page 2: Finance — P&L Analysis

> **Layout:** 5 KPI cards + waterfall + line + matrix

| Visual | Type | Data |
|--------|------|------|
| Revenue / COGS / Gross Profit / OpEx / Operating Margin | Cards (5) | Respective measures |
| P&L Waterfall | Waterfall | Revenue → COGS → Gross Profit → OpEx → Operating Profit |
| Monthly Trend | Line | Revenue & OpEx over time |
| Account Breakdown | Matrix | AccountCategory > AccountName by Quarter |

**Slicers:** Fiscal Year, Fiscal Quarter

---

### 📄 Page 3: Finance — Budget vs Actual

> **Layout:** Gauge + cards + grouped bar + variance table

| Visual | Type | Data |
|--------|------|------|
| Budget Attainment | Gauge | `[Budget Attainment]` target 100% |
| Variance Cards | Cards (2) | `[Budget Variance]`, `[Budget Variance %]` |
| Budget vs Actual by Account | Grouped Bar | Budget & Actual amounts |
| Variance by Quarter | Clustered Column | Conditional color (green/red) |
| Detailed Variance Table | Table | Account, Budget, Actual, Variance, Variance% |

---

### 📄 Page 4: Operations — Book Performance

> **Layout:** Cards + treemap + bar + area + detail table

| Visual | Type | Data |
|--------|------|------|
| Best Seller / Titles / Avg Price | Cards (3) | Dynamic top book, count, ASP |
| Sales by Genre | Treemap | Genre → [Order Revenue] |
| Top 10 Books | Horizontal Bar | Title → [Order Revenue] desc |
| Monthly Sales Trend | Area | Top 5 books over time |
| Book Detail Table | Table | Title, Author, Genre, Imprint, Price, Units, Revenue, Return%, Margin |

**Slicers:** Genre, Imprint, Publish Date Range

---

### 📄 Page 5: Operations — Customer & Distribution

> **Layout:** 4 cards + bar + table + pie + combo chart

| Visual | Type | Data |
|--------|------|------|
| Active Customers / Revenue per Customer / AOV / On-Time | Cards (4) | |
| Revenue by Customer Type | Stacked Bar | CustomerType → [Order Revenue] |
| Top Customers | Table | Name, Type, Region, Orders, Revenue, Discount |
| Revenue by Region | Pie/Map | [Order Revenue] by Region |
| Order Volume Trend | Line + Column | Orders count + AOV over time |

**Drill-through:** Click customer → Customer Detail Page

---

### 📄 Page 6: Geographic Analysis

> **Layout:** 5 cards + map + column + table + bar

| Visual | Type | Data |
|--------|------|------|
| Countries / Intl Revenue / Intl % / Intl Customers / Locations | Cards (5) | |
| Global Sales Map | Azure Map | Lat/Long bubbles sized by [Order Revenue] |
| Revenue by Continent | Stacked Column | Continent → Revenue by CustomerType |
| Revenue by Country | Table | Country, Continent, Customers, Orders, Revenue |
| Top International Customers | Horizontal Bar | Non-US customers by revenue |

**Slicers:** Continent, Country, Customer Type
**Drill-through:** Click country → filtered customer/order view

---

### 📄 Page 7: Operations — Inventory & Supply Chain

> **Layout:** 4 cards + donut + 2 bars + line + matrix

| Visual | Type | Data |
|--------|------|------|
| Inventory Value / Low Stock / Days of Supply / Turnover | Cards (4) | |
| Inventory Status | Donut | Status distribution |
| Days of Supply by Book | Horizontal Bar | Conditional color (red < 30, yellow < 60, green) |
| Inventory by Genre | Stacked Bar | Genre → Value |
| Inventory Trend | Line | QuantityOnHand over time |
| Return Analysis | Matrix | Book > Reason → Return Qty, Refund, Rate |

---

### 📄 Page 8: HR — Workforce Overview

> **Layout:** 4 cards + bar + donut + pie + decomp tree

| Visual | Type | Data |
|--------|------|------|
| Headcount / Tenure / Open Positions / Rev per Employee | Cards (4) | |
| Headcount by Department | Horizontal Bar | Department → Count |
| Employment Type | Donut | Full-Time / Part-Time / Contract |
| Headcount by Location | Pie | Location → Count |
| Org Structure | Decomposition Tree | Department > Manager > Employee |

---

### 📄 Page 9: HR — Compensation & Performance

> **Layout:** 4 cards + distributions + trend + detail

| Visual | Type | Data |
|--------|------|------|
| Total Payroll / Avg Salary / Perf Score / Top Performer Rate | Cards (4) | |
| Salary Distribution | Box & Whisker | Department → BaseSalary |
| Bonus vs Base | Stacked Column | Department → Base + Bonus |
| Performance Distribution | Histogram | Rating Category → Count |
| Performance Detail | Table | Employee, Dept, Rating, Goals, Score, Strengths |
| Payroll per Revenue | Line | Monthly trend |

---

### 📄 Page 10: HR — Recruitment Pipeline

> **Layout:** 3 cards + funnel + bar + range + table

| Visual | Type | Data |
|--------|------|------|
| Open Reqs / Time to Fill / Offer Accept Rate | Cards (3) | |
| Recruitment Funnel | Funnel | Applications → Interviewed → Offers → Accepted |
| Hiring by Department | Bar | Department → Filled count |
| Salary Range Analysis | Range Column | JobTitle → Min-Max salary |
| Open Positions Table | Table | Requisition, Dept, Title, Date, Apps, Status, Days |

---

## 🔖 Bookmarks & Navigation

### Navigation Bar
1. Executive Dashboard → 2. Finance P&L → 3. Finance Budget → 4. Book Performance → 5. Customers → 6. Geographic → 7. Inventory → 8. Workforce → 9. Compensation → 10. Recruitment

### Bookmarks

| Bookmark | Filter |
|----------|--------|
| Current Quarter View | Pre-filtered to latest quarter |
| Holiday Season Analysis | Filtered to Q4 |
| Bestseller Deep Dive | Filtered to Winter's Promise (BK-017) |
| Critical Inventory Alert | Shows only Low Stock/Critical items |
| International Markets | Filtered to non-US geographies |

---

## 💬 Tooltips

| Tooltip | Shown On | Fields |
|---------|----------|--------|
| **Book** | Any book reference | Title, Author, Genre, ISBN, PublishDate, ListPrice, Units, Revenue |
| **Customer** | Any customer ref | Name, Type, Region, First Order, Orders, Revenue, Discount |
| **Employee** | Any employee ref | Name, Title, Department, HireDate, Tenure, Performance Score |

---

## 🔮 Forecasting Report — Horizon Books Forecasting

5 pages with Holt-Winters projections and 95% confidence bands:

| Page | Title | Key Visuals |
|------|-------|-------------|
| 1 | Sales Revenue Forecast | Channel revenue projection, confidence bands, actual vs forecast |
| 2 | Genre Demand Forecast | Genre-level unit demand and revenue projections |
| 3 | Financial P&L Forecast | Revenue, COGS, and operating expense projections |
| 4 | Inventory Demand Forecast | Book/warehouse demand, stock coverage analysis |
| 5 | Workforce Planning Forecast | Headcount, payroll, and open position projections |

---

<p align="center">
  <sub>Report format: PBIR v4.0 — Theme: HorizonBooksTheme.json</sub>
</p>
