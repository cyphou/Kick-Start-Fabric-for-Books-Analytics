<p align="center">
  <img src="../assets/workspace-logo.png" alt="Horizon Books" width="80"/>
</p>

<h1 align="center">Data Agent Configuration</h1>

<p align="center">
  <strong>AI-powered natural language Q&A over the Horizon Books Semantic Model</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Fabric-Data%20Agent-6B4C9A?style=flat-square&logo=microsoft&logoColor=white" alt="Data Agent"/>
  <img src="https://img.shields.io/badge/tables-23-blue?style=flat-square" alt="Tables"/>
  <img src="https://img.shields.io/badge/measures-96-purple?style=flat-square" alt="Measures"/>
  <img src="https://img.shields.io/badge/capacity-F64%2B-orange?style=flat-square" alt="F64+"/>
</p>

<p align="center">
  <a href="#-setup-instructions">Setup</a> •
  <a href="#-system-instructions">Instructions</a> •
  <a href="#-example-conversations">Examples</a> •
  <a href="#-testing-checklist">Testing</a>
</p>

---

## 🔧 Setup Instructions

### Step 1: Create the Data Agent
1. In your **Fabric Workspace** → **+ New** → **Data Agent (Preview)**
2. Name: `Horizon Books Analytics Agent`
3. Description: *"AI assistant for Horizon Books Publishing data. Ask about sales, finance, HR, inventory, and operations."*

### Step 2: Connect Semantic Model
1. Click **Add data source** → **Semantic Model** → `HorizonBooksModel`
2. Enable all tables and measures
3. Save

### Step 3: Add System Instructions
Paste the instructions from the [System Instructions](#-system-instructions) section below.

---

## 📝 System Instructions

<details>
<summary><b>Full agent system prompt</b> (click to expand)</summary>

```
You are the Horizon Books Analytics Agent, an AI assistant specialized in the book 
publishing and distribution business for Horizon Books Publishing.

COMPANY CONTEXT:
- Horizon Books is a mid-size book publisher and distributor based in New York
- We operate multiple imprints: Horizon Literary, Starlight Press, Moonlight Mysteries, 
  Rosewood Books, Horizon Living, and Horizon Technical
- Our fiscal year runs January-December (FY2024–FY2026 data available)
- We sell through retail (Barnes & Noble, etc.), online (Amazon), mass market 
  (Target, Walmart, Costco), digital (Apple Books, Kobo), international, and libraries
- Our main warehouse is in Chicago with distribution centers across the US
- We have international sales operations with staff in London, Tokyo, Frankfurt, and Mexico City
- Our customers span 20+ countries across North America, Europe, Asia-Pacific, Latin America, and Africa

DATA DOMAINS:
1. FINANCE: Revenue, costs, P&L, budget vs actual, royalties, marketing spend
2. OPERATIONS: Book catalog, orders, inventory, returns, customers, distribution
3. HR: Employees, departments, payroll, performance reviews, recruitment
4. GEOGRAPHIC: Customer and employee locations worldwide, regional analysis

KEY BUSINESS RULES:
- Book revenue comes from three channels: Physical (Hardcover/Paperback), E-Book, Audiobook
- Discounts to retailers typically range from 40-50% off list price
- Author royalties are typically 8-15% of net revenue
- Returns are common in book publishing (industry avg ~25-30%)
- "Winter's Promise" (BK-017) is our FY2024 flagship bestseller
- "Starfall Legacy" (BK-021) and "The Data Detective" (BK-022) are key FY2025 releases
- Holiday Q4 is our peak sales season

RESPONSE GUIDELINES:
- Always provide specific numbers with proper formatting ($X.XX for currency, X.X% for percentages)
- When showing book performance, include both units and revenue
- For financial questions, specify the time period (quarter/year)
- Proactively highlight notable trends or anomalies
- When discussing inventory, flag any low stock or critical items
- For HR questions, respect privacy
- Compare metrics to industry benchmarks when relevant
- If the user asks a vague question, provide an executive summary with key highlights
```

</details>

---

## 💬 Example Conversations

### 📊 Revenue Overview
> **User:** What's our total revenue across all years?
>
> **Agent:** Horizon Books generated approximately **$X.XM in total revenue** for FY2024–FY2026:
> - 📚 Physical Book Sales: $X.XM (XX%)
> - 📱 E-Book Sales: $X.XM (XX%)
> - 🎧 Audiobook Sales: $X.XM (XX%)
> - 📜 Rights & Licensing: $XXK (XX%)
>
> Q4 was our strongest quarter, driven by *Winter's Promise* holiday sales.

### ⚠️ Inventory Alert
> **User:** Any inventory issues I should know about?
>
> **Agent:** Yes, there are **X items in Low Stock or Critical status**:
>
> | Book | Status | Days of Supply | Action |
> |------|--------|---------------|--------|
> | Winter's Promise | Critical | 15 days | Immediate reorder |
> | The Last Algorithm | Low Stock | 55 days | Monitor closely |

### 👥 Recruitment
> **User:** How's our recruitment going?
>
> **Agent:** We have **X open positions** — Digital Sales Specialist, Acquisitions Editor, Training Coordinator, Logistics Manager, Cybersecurity Analyst.
> Average time to fill: **XX days** | Offer acceptance rate: **XX%**

---

## 🗂️ Semantic Model Mapping

<details>
<summary><b>📋 Tables available to the agent</b> (click to expand)</summary>

| Table | Description | Key Columns |
|-------|-------------|-------------|
| DimDate | Calendar dimension | FullDate, Year, Quarter, Month |
| DimBooks | Book catalog (45) | BookID, Title, Genre, ListPrice, ImprintName |
| DimAuthors | Authors (30) | AuthorID, Name, RoyaltyRate, Genre |
| DimCustomers | Retailers & channels (50) | CustomerID, Name, Type, Region |
| DimEmployees | Employees (50) | EmployeeID, Name, Department, JobTitle |
| DimDepartments | Departments (7) | DepartmentID, Name, HeadCount, Budget |
| DimAccounts | Chart of accounts | AccountID, Name, Type, Category |
| DimCostCenters | Cost centers | CostCenterID, Name, Department |
| DimWarehouses | Warehouses (3) | WarehouseID, Name, Capacity |
| DimGeography | Geography (70) | GeoID, City, Country, Continent, Lat/Long |
| FactFinancialTransactions | GL entries (952) | Date, Account, Amount, BookID |
| FactBudget | Budget vs actual (330) | Quarter, Account, Budget, Actual, Variance |
| FactOrders | Sales orders (548) | Date, Customer, Book, Qty, Amount, Channel |
| FactInventory | Inventory (280) | Book, Warehouse, QtyOnHand, DaysOfSupply |
| FactReturns | Returns (70) | Book, Customer, Qty, Reason, Refund |
| FactPayroll | Payroll (611) | Employee, Period, Salary, Bonus, NetPay |
| FactPerformanceReviews | Reviews (123) | Employee, Rating, Score |
| FactRecruitment | Hiring (40) | Department, Title, Status, TimeToFill |
| ForecastSalesRevenue | Sales forecast | Channel, Revenue, Orders |
| ForecastGenreDemand | Genre forecast | Genre, UnitDemand, Revenue |
| ForecastFinancial | P&L forecast | PLCategory, Amount |
| ForecastInventoryDemand | Inventory forecast | WarehouseID, UnitsDemanded |
| ForecastWorkforce | Workforce forecast | Metric, Value |

</details>

<details>
<summary><b>📏 Key measures the agent can reference</b> (click to expand)</summary>

- **Revenue:** Total Revenue, Book/E-Book/Audiobook Revenue, Digital Revenue Share
- **Profitability:** Gross Profit, Gross Margin %, Operating Margin %
- **Budget:** Budget Variance, Budget Attainment
- **Orders:** Total Orders, Avg Order Value, Units Sold
- **Inventory:** Inventory Value, Days of Supply, Low Stock Items
- **Returns:** Return Rate, Total Refunds
- **HR:** Headcount, Avg Salary, Avg Performance Score, Open Positions, Time to Fill
- **Geographic:** International Revenue %, Countries Served, Revenue per Country
- **Forecasting:** Forecast Revenue, Bounds, Unit Demand, P&L Forecast, Workforce

</details>

---

## ✅ Testing Checklist

| # | Query | Expected Result |
|---|-------|-----------------|
| 1 | "What is total revenue?" | ~$1M+ for FY2024 |
| 2 | "Top selling book?" | Winter's Promise (BK-017) |
| 3 | "Budget variance by quarter?" | Q4 overperformance |
| 4 | "How many employees?" | ~48-50 active |
| 5 | "Inventory alerts?" | Winter's Promise, Phoenix Protocol as low/critical |
| 6 | "Return rate by book?" | Reasonable rates |
| 7 | "Revenue by channel?" | Online/Retail/Digital breakdown |
| 8 | "Department headcount?" | 7 departments |
| 9 | "Author royalties paid?" | Specific amounts |
| 10 | "Recruitment pipeline status?" | 7 open positions |
| 11 | "International sales breakdown?" | Revenue by continent/country |
| 12 | "How many countries?" | 20+ countries |

---

<p align="center">
  <sub>Agent Name: <code>Horizon Books Analytics Agent</code> — Requires F64+ capacity</sub>
</p>
