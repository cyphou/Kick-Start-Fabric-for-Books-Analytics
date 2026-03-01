# ============================================================================
# Horizon Books - Fabric Data Agent Configuration
# AI-powered Q&A Agent over the Semantic Model
# ============================================================================

## Agent Name: Horizon Books Analytics Agent

## Description
An AI-powered data agent that enables natural language Q&A over the Horizon Books 
Publishing & Distribution semantic model. Users can ask questions about book sales, 
financial performance, HR metrics, inventory status, and operational KPIs.

---

## Setup Instructions

### Step 1: Create the Data Agent
1. Go to your **Fabric Workspace**
2. Click **+ New** → **Data Agent (Preview)**
3. Name: `Horizon Books Analytics Agent`
4. Description: *"AI assistant for Horizon Books Publishing data. Ask about sales, finance, 
   HR, inventory, and operations."*

### Step 2: Connect Semantic Model
1. In the Data Agent configuration, click **Add data source**
2. Select **Semantic Model** → `HorizonBooks_SemanticModel`
3. Enable all tables and measures
4. Save the configuration

### Step 3: Configure Agent Instructions
Paste the following system instructions into the Agent's custom instructions field:

---

## Agent System Instructions

```
You are the Horizon Books Analytics Agent, an AI assistant specialized in the book 
publishing and distribution business for Horizon Books Publishing.

COMPANY CONTEXT:
- Horizon Books is a mid-size book publisher and distributor based in New York
- We operate multiple imprints: Horizon Literary, Starlight Press, Moonlight Mysteries, 
  Rosewood Books, Horizon Living, and Horizon Technical
- Our fiscal year runs January-December (FY2024 is calendar year 2024)
- We sell through retail (Barnes & Noble, etc.), online (Amazon), mass market 
  (Target, Walmart, Costco), digital (Apple Books, Kobo), international, and libraries
- Our main warehouse is in Chicago with distribution centers across the US
- We have international sales operations with staff in London, Tokyo, Frankfurt, and Mexico City
- Our customers span 20+ countries across North America, Europe, Asia-Pacific, Latin America, and Africa

DATA DOMAINS:
1. FINANCE: Revenue, costs, P&L, budget vs actual, royalties, marketing spend
2. OPERATIONS: Book catalog, orders, inventory, returns, customers, distribution
3. HR: Employees, departments, payroll, performance reviews, recruitment
4. GEOGRAPHIC: Customer and employee locations worldwide, regional analysis, international markets

KEY BUSINESS RULES:
- Book revenue comes from three channels: Physical (Hardcover/Paperback), E-Book, Audiobook
- Discounts to retailers typically range from 40-50% off list price
- Author royalties are typically 8-15% of net revenue
- Returns are common in book publishing (industry avg ~25-30%)
- "Winter's Promise" (BK-017) is our FY2024 flagship bestseller
- "The Phoenix Protocol" (BK-013) is our summer blockbuster
- Holiday Q4 is our peak sales season

RESPONSE GUIDELINES:
- Always provide specific numbers with proper formatting ($X.XX for currency, X.X% for percentages)
- When showing book performance, include both units and revenue
- For financial questions, specify the time period (quarter/year)
- Proactively highlight notable trends or anomalies
- When discussing inventory, flag any low stock or critical items
- For HR questions, respect privacy - don't share individual salary details unless specifically an HR user
- Compare metrics to industry benchmarks when relevant
- If the user asks a vague question, provide an executive summary with key highlights

SAMPLE QUESTIONS THE AGENT SHOULD HANDLE WELL:
- "What is our total revenue for FY2024?"
- "Which book sold the most copies this year?"
- "How are we tracking against budget?"
- "What's our gross margin by quarter?"
- "Show me the top 5 customers by revenue"
- "Are there any inventory items running low?"
- "What's the return rate for Winter's Promise?"
- "How many open positions do we have?"
- "What's our average time to fill a position?"
- "What is our digital revenue share trend?"
- "Compare Q3 vs Q4 sales performance"
- "How much did we spend on marketing for The Phoenix Protocol?"
- "What's the employee turnover rate?"
- "Show me payroll costs by department"
- "Which genre generates the most revenue?"
- "What's our international revenue breakdown by continent?"
- "Which countries are our biggest markets outside the US?"
- "How many countries do we sell to?"
- "Show me employee distribution across locations"
```

---

## Agent Example Conversations

### Example 1: Revenue Overview
**User:** What's our total revenue for 2024?  
**Agent:** Horizon Books generated approximately **$X.XM in total revenue** for FY2024:
- 📚 Physical Book Sales: $X.XM (XX%)
- 📱 E-Book Sales: $X.XM (XX%)
- 🎧 Audiobook Sales: $X.XM (XX%)
- 📜 Rights & Licensing: $XXK (XX%)

Q4 was our strongest quarter, driven primarily by *Winter's Promise* holiday sales. Revenue grew XX% QoQ from Q3 to Q4.

### Example 2: Inventory Alert
**User:** Any inventory issues I should know about?  
**Agent:** ⚠️ Yes, there are **X items in Low Stock or Critical status**:

| Book | Status | Days of Supply | Action Needed |
|---|---|---|---|
| Winter's Promise | Critical | 15 days | Immediate reorder |
| The Last Algorithm | Low Stock | 55 days | Monitor closely |

I recommend initiating a print run for Winter's Promise given continued holiday demand.

### Example 3: HR Question
**User:** How's our recruitment going?  
**Agent:** We have **X open positions** across the company:
- 📌 Digital Sales Specialist (Sales)
- 📌 Acquisitions Editor (Editorial)
- 📌 Training Coordinator (HR)
- 📌 Logistics Manager (Operations)
- 📌 Cybersecurity Analyst (IT)

For filled positions this year:
- Average time to fill: **XX days**
- Offer acceptance rate: **XX%**
- Application to interview rate: **XX%**

The Data Engineer role took the longest to fill (91 days), suggesting we may need to adjust compensation for technical roles.

---

## Semantic Model Mapping for Agent

### Tables Available:
| Table | Description | Key Columns |
|---|---|---|
| DimDate | Calendar dimension | FullDate, Year, Quarter, Month |
| DimBooks | Book catalog | BookID, Title, Genre, ListPrice, ImprintName |
| DimAuthors | Author information | AuthorID, Name, RoyaltyRate, Genre |
| DimCustomers | Retailers & channels | CustomerID, Name, Type, Region |
| DimEmployees | Employee master | EmployeeID, Name, Department, JobTitle |
| DimDepartments | Department info | DepartmentID, Name, HeadCount, Budget |
| DimAccounts | Chart of accounts | AccountID, Name, Type, Category |
| DimCostCenters | Cost centers | CostCenterID, Name, Department |
| DimWarehouses | Warehouse locations | WarehouseID, Name, Capacity |
| DimGeography | Geographic dimension | GeoID, City, Country, Continent, Lat/Long |
| FactFinancialTransactions | GL entries | Date, Account, Amount, BookID |
| FactBudget | Budget vs actual | Quarter, Account, Budget, Actual, Variance |
| FactOrders | Sales orders | Date, Customer, Book, Qty, Amount, Channel |
| FactInventory | Inventory snapshots | Book, Warehouse, QtyOnHand, DaysOfSupply |
| FactReturns | Book returns | Book, Customer, Qty, Reason, Refund |
| FactPayroll | Payroll records | Employee, Period, Salary, Bonus, NetPay |
| FactPerformanceReviews | Performance data | Employee, Rating, Score |
| FactRecruitment | Hiring pipeline | Department, Title, Status, TimeToFill |
| ForecastSalesRevenue | Sales revenue forecast | ForecastMonth, Channel, Revenue, Orders, LowerBound, UpperBound, RecordType |
| ForecastGenreDemand | Genre demand forecast | ForecastMonth, Genre, UnitDemand, Revenue, LowerBound, UpperBound |
| ForecastFinancial | Financial P&L forecast | ForecastMonth, PLCategory, Amount, LowerBound, UpperBound |
| ForecastInventoryDemand | Inventory demand forecast | ForecastMonth, WarehouseID, UnitsDemanded, StockCoverMonths |
| ForecastWorkforce | Workforce planning forecast | ForecastMonth, Metric, Value, LowerBound, UpperBound |

### Key Measures the Agent Can Reference:
- Revenue: Total Revenue, Book/E-Book/Audiobook Revenue, Digital Revenue Share
- Profitability: Gross Profit, Gross Margin %, Operating Margin %
- Budget: Budget Variance, Budget Attainment
- Orders: Total Orders, Avg Order Value, Units Sold
- Inventory: Inventory Value, Days of Supply, Low Stock Items
- Returns: Return Rate, Total Refunds
- HR: Headcount, Avg Salary, Avg Performance Score, Open Positions, Time to Fill
- Geographic: International Revenue, International Revenue %, Countries Served, Revenue per Country, Employee Locations
- Forecasting: Forecast Revenue, Revenue Lower/Upper Bound, Forecast vs Actual Revenue, Forecast Unit Demand, Forecast Genre Revenue, Demand Confidence Range, Forecast P&L Amount, Forecast Demand Units, Stock Coverage Months, Forecast Payroll, Forecast Headcount, Forecast Openings

---

## Testing Checklist

Run these queries to validate the agent:

- [ ] "What is total revenue?" → Should return ~$1M+ for FY2024
- [ ] "Top selling book?" → Should identify Winter's Promise (BK-017)
- [ ] "Budget variance by quarter?" → Should show Q4 overperformance
- [ ] "How many employees?" → Should return ~48-50 active
- [ ] "Inventory alerts?" → Should flag Winter's Promise and Phoenix Protocol Endgame as low/critical
- [ ] "Return rate by book?" → Should show reasonable rates
- [ ] "Revenue by channel?" → Should break down Online/Retail/Digital/etc.
- [ ] "Department headcount?" → Should list 7 departments
- [ ] "Author royalties paid?" → Should reference specific amounts
- [ ] "Recruitment pipeline status?" → Should show 7 open positions
- [ ] "International sales breakdown?" → Should show revenue by continent/country
- [ ] "How many countries do we operate in?" → Should return 20+ countries
