# ============================================================================
# 🏢 Horizon Books Publishing & Distribution
# Microsoft Fabric End-to-End Demo
# Master Setup Guide
# ============================================================================

## 📋 Demo Overview

**Company:** Horizon Books Publishing & Distribution  
**Industry:** Book Publishing, Distribution, and Retail  
**Data Years:** FY2024–FY2026 (January 2024 – June 2026)  
**Fabric Components:** 3 Lakehouses (Medallion with schemas), Spark Environment, 4 Spark Notebooks, Semantic Model, Power BI Report, Data Agent

### Business Domains Covered

| Domain | Description | Key Data |
|---|---|---|
| **Finance** | P&L, Budget vs Actual, GL Transactions | Revenue, COGS, Royalties, Marketing, OpEx |
| **HR** | Workforce, Compensation, Recruitment | 50 employees, 7 departments, payroll, reviews |
| **Operations** | Books, Orders, Inventory, Returns, Geography | 45 titles, 30 authors, 50 customers, 548 orders |

### The Story
Horizon Books is a mid-size publisher with 6 imprints, operating from New York (HQ) 
and Chicago (warehouse), with international sales operations in London, Tokyo, Frankfurt, 
and Mexico City. The company serves 50 customers across 20+ countries globally. 
FY2024’s flagship title "Winter's Promise" by Catherine Harper 
drove record Q4 sales, and the momentum continued into FY2025-FY2026 with 
new releases like "Starfall Legacy" and "The Data Detective". The company 
publishes across Fiction (Literary, Sci-Fi, Fantasy, Mystery, Thriller, 
Romance) and Non-Fiction (Tech, Lifestyle, Health, Education).

---

## 🏗️ Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    Microsoft Fabric Workspace                            │
│                   "Horizon Books Analytics"                              │
│                    (logo: workspace-logo.png)                            │
│                                                                          │
│  ┌─────────────────┐   ┌──────────────────┐   ┌──────────────────┐      │
│  │    BronzeLH       │   │     SilverLH      │   │     GoldLH        │      │
│  │  (schema-enabled)│   │  (schema-enabled) │   │  (schema-enabled) │      │
│  │                  │   │                    │   │                    │      │
│  │  Files/          │   │  finance.*         │   │  dim.*             │      │
│  │  ├── Finance/    │──▶│  hr.*              │──▶│  fact.*            │      │
│  │  ├── HR/         │   │  operations.*      │   │  analytics.*       │      │
│  │  └── Operations/ │   │  web.*             │   │                    │      │
│  │                  │   │                    │   │                    │      │
│  │  dbo.* (raw)     │   │  17 Silver tables  │   │  10 Dims + 9 Facts│      │
│  │  (via Dataflows) │   │  + 4 Web tables    │   │  + 4 Analytics     │      │
│  └─────────────────┘   └──────────────────┘   └────────┬─────────┘      │
│         ▲                       ▲                       │                │
│         │                       │                       │                │
│  ┌──────┴──────────────────────┴────────────────────────┘                │
│  │  PL_HorizonBooks_Orchestration (Data Pipeline)                        │
│  │                                                                       │
│  │  Phase 1 (parallel):                                                  │
│  │  ┌──────────┐┌──────┐┌────────────┐ ┌────────────────────────┐       │
│  │  │DF_Finance││DF_HR ││DF_Operations│ │ NB01 BronzeToSilver    │       │
│  │  └──────────┘└──────┘└────────────┘ │ (CSV → SilverLH)       │       │
│  │  (CSV → BronzeLH Delta tables       └────────────┬───────────┘       │
│  │   via auto-configured destinations)                                   │
│  │                                                   │                   │
│  │  Phase 2:   ┌─────────────────────────────────────▼──────────┐       │
│  │             │ NB02 WebEnrichment (4 APIs → SilverLH.web.*)   │       │
│  │             └─────────────────────────────────────┬──────────┘       │
│  │  Phase 3:   ┌─────────────────────────────────────▼──────────┐       │
│  │             │ NB03 SilverToGold (SilverLH → GoldLH)          │       │
│  │             │ dim.*, fact.*, analytics.*                       │       │
│  │             └────────────────────────────────────────────────┘       │
│  └──────────────────────────────────────────────────────────────────────┘│
│                                                                          │
│         ┌──────────────────┐              ┌───────────────────────┐      │
│         │  PBIP Project     │              │   Semantic Model       │      │
│         │  (local / Git)    │─── Deploy ──▶│   (Direct Lake)        │      │
│         │                  │   via API    │   → GoldLH SQL EP      │      │
│         │ • TMDL Model     │              │                        │      │
│         │ • PBIR Report    │              │ 27 Relationships       │      │
│         └──────────────────┘              │ 96 DAX Measures        │      │
│                                           │ schemaName: dim|fact   │      │
│                                           └──┬──────────┬─────────┘      │
│                                              │          │                 │
│                                    ┌─────────▼──┐  ┌───▼──────┐         │
│                                    │  Power BI   │  │  Data    │         │
│                                    │  Report     │  │  Agent   │         │
│                                    │  (10 pages) │  │  (AI Q&A)│         │
│                                    └─────────────┘  └──────────┘         │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐     │
│  │  deploy/                  PowerShell Automation                  │     │
│  │  Deploy-Full.ps1               → ONE-COMMAND full deployment     │     │
│  │  New-HorizonBooksWorkspace.ps1 → Workspace + capacity + logo    │     │
│  │  HorizonBooks.psm1             → Shared module (tokens/API/log) │     │
│  │  Deploy-HorizonBooks.ps1       → Legacy step-by-step setup      │     │
│  │  Deploy-Pipeline.ps1           → Dataflows (with destinations)   │     │
│  │  Update-DataflowDestinations.ps1→ Re-apply destinations only     │     │
│  │  Deploy-DataAgent.ps1          → Data Agent creation             │     │
│  │  Deploy-PowerBI.ps1            → Semantic Model + 2 Reports      │     │
│  │  Validate-Deployment.ps1       → Post-deploy validation          │     │
│  │  Upload-SampleData.ps1         → Standalone CSV upload to Bronze │     │
│  │  Redeploy-Notebooks.ps1        → Re-deploy notebooks only        │     │
│  │  Deploy-Diagnostic.ps1         → Diagnostic notebook deploy      │     │
│  │  HorizonBooks_TaskFlow.json    → Fabric workspace task flow      │     │
│  │                                                                  │     │
│  │  tests/Deploy-HorizonBooks.Tests.ps1 → Pester test suite         │     │
│  └─────────────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────────┘
```

### Medallion Lakehouse Layout

| Lakehouse | Layer | Schemas | Contents |
|-----------|-------|---------|----------|
| **BronzeLH** | Bronze | `dbo` (default) | 17 CSV files in `Files/`, 17 Delta tables ingested by Dataflow Gen2 (auto-destination) |
| **SilverLH** | Silver | `finance`, `hr`, `operations`, `web` | Cleaned/typed/deduped Delta tables, web API enrichment data |
| **GoldLH** | Gold | `dim`, `fact`, `analytics` | Star schema dimensions & facts, advanced analytics tables |

All 3 Lakehouses are created with the **enableSchemas** feature enabled, allowing
schema-based organization (`lakehouse.schema.table` naming in Spark SQL).
The Semantic Model uses `schemaName: dim` / `schemaName: fact` in TMDL partition blocks
to bind Direct Lake to the correct GoldLH schemas.
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Deployment Options

### Option A: One-Command Deployment (Recommended — ~15 min)

Deploy everything with a single command. The script provisions all Fabric items,
runs the data pipeline, deploys the semantic model, and validates the result.

**Prerequisites:**
- Microsoft Fabric Capacity (F64 or Trial)
- Fabric Workspace already created (note the Workspace ID from the URL)
- PowerShell 5.1 or later
- `Az` module installed (`Install-Module Az -Scope CurrentUser`)
- Logged in via `Connect-AzAccount`

**One-liner:**

```powershell
Connect-AzAccount
.\deploy\Deploy-Full.ps1 -WorkspaceId "<your-workspace-guid>"
```

**What it does (11 automated steps):**

| Step | Action | Time |
|------|--------|------|
| 1 | Create 3 Lakehouses (BronzeLH, SilverLH, GoldLH) with schemas + wait for GoldLH SQL endpoint | ~3 min |
| 2 | Upload 17 CSV files to BronzeLH via OneLake DFS API | ~1 min |
| 3 | Deploy Spark Environment + 4 PySpark notebooks (each bound to its default Lakehouse) | ~1 min |
| 4 | Run NB01 Bronze→Silver (schema, quality, dedup → SilverLH) | ~3 min |
| 5 | Deploy 3 Dataflow Gen2 items + orchestration pipeline | ~1 min |
| 6 | **Run pipeline**: Dataflows + NB01 (parallel) → NB02 WebEnrichment → NB03 SilverToGold → NB04 Forecasting | ~7 min |
| 7 | Execute Lakehouse SQL scripts (CreateTables.sql + GenerateDateDimension.sql) | ~1 min |
| 8 | Deploy Semantic Model (Direct Lake on GoldLH, 96 measures, schemaName bindings) | ~1 min |
| 9 | Deploy 2 Power BI Reports: Analytics (10 pages) + Forecasting (5 pages), both PBIR bound to Semantic Model | <1 min |
| 10 | Deploy Data Agent (F64+ only) | <1 min |
| 11 | Validate all deployed items | <1 min |

**Optional flags:**

```powershell
# Deploy without auto-running the pipeline (trigger manually later)
.\deploy\Deploy-Full.ps1 -WorkspaceId "<guid>" -SkipPipelineRun

# Skip Data Agent (e.g., on Trial capacity)
.\deploy\Deploy-Full.ps1 -WorkspaceId "<guid>" -SkipDataAgent

# Skip post-deploy validation
.\deploy\Deploy-Full.ps1 -WorkspaceId "<guid>" -SkipValidation
```

**Output:** A timing summary table showing each step's duration and pass/fail status,
plus deployed resource IDs and a direct link to the Fabric portal.

### Option B: Step-by-Step Automated Deployment (~15 min)

Run each deployment script individually for finer control.

```powershell
# 1. Log in to Azure
Connect-AzAccount

# 2. (Optional) Create workspace with logo and capacity assignment
.\deploy\New-HorizonBooksWorkspace.ps1 -CapacityId "<capacity-guid>"

# 3. Run the full deployment pipeline
.\deploy\Deploy-Full.ps1 -WorkspaceId "<your-workspace-guid>" -SkipPipelineRun

# 4. (Or deploy individual components)
.\deploy\Deploy-Pipeline.ps1 -WorkspaceId "<your-workspace-guid>"   # Dataflows + Pipeline
.\deploy\Deploy-PowerBI.ps1  -WorkspaceId "<your-workspace-guid>"   # Semantic Model + 2 Reports
.\deploy\Deploy-DataAgent.ps1 -WorkspaceId "<your-workspace-guid>"  # Data Agent

# 5. Upload sample data (if not using Deploy-Full)
.\deploy\Upload-SampleData.ps1 -WorkspaceId "<guid>" -BronzeLakehouseId "<bronze-lh-guid>"

# 6. Validate everything was created
.\deploy\Validate-Deployment.ps1 -WorkspaceId "<your-workspace-guid>"

# 7. (Optional) Run Pester tests
Invoke-Pester .\tests\Deploy-HorizonBooks.Tests.ps1 -Tag "Unit"
```

The individual scripts:
1. **New-HorizonBooksWorkspace.ps1** — Creates workspace, assigns to Fabric capacity, uploads branded logo
2. **Deploy-Full.ps1** (with `-SkipPipelineRun`) provisions all items without executing the pipeline
3. **Deploy-Pipeline.ps1** deploys Dataflow Gen2 items with auto-configured destinations + orchestration pipeline
4. **Deploy-PowerBI.ps1** deploys the Semantic Model + 2 Reports (Analytics & Forecasting) from PBIP/TMDL definitions. **Idempotent:** re-running updates existing items instead of creating duplicates
5. **Upload-SampleData.ps1** uploads 17 CSVs to BronzeLH (requires `-WorkspaceId` and `-BronzeLakehouseId`)
6. **Deploy-DataAgent.ps1** creates the AI Data Agent (F64+ only)

### Notebook Pipeline Details

The 4-notebook pipeline implements a medallion architecture with web data enrichment and forecasting:

| Notebook | Default LH | Source | Target | Key Operations |
|---|---|---|---|---|
| **01_BronzeToSilver** | BronzeLH | 17 CSV files (BronzeLH/Files/) | SilverLH (finance/hr/operations schemas) | Schema enforcement, data quality checks, deduplication, audit columns, dimension/fact-specific transforms |
| **02_WebEnrichment** | SilverLH | 4 public APIs | SilverLH.web.* + enriched Silver tables | Exchange rates (frankfurter.app), holidays (date.nager.at), country indicators (restcountries.com), book metadata (openlibrary.org). **Static fallbacks** for all APIs ensure pipeline completes even when external services are unavailable. |
| **03_SilverToGold** | GoldLH | SilverLH tables (cross-LH reads) | GoldLH (dim/fact/analytics schemas) | DimDate with holidays, RFM segmentation, customer cohort analysis, revenue anomaly detection (Z-score), book co-purchasing patterns (market basket), revenue forecasting (EMA). **Safety net** ensures all 18 semantic-model-required tables exist in Gold (creates empty stubs if upstream data is unavailable). |
| **04_Forecasting** | GoldLH | GoldLH fact tables | GoldLH (analytics schema) | Holt-Winters time-series forecasting: sales revenue by channel, genre demand, financial P&L, inventory demand, workforce planning (6-month horizon, 95% confidence). **MLflow experiment tracking** with parent/child runs per forecast table. |
| **05_DiagnosticCheck** | GoldLH | All 3 Lakehouses | — (read-only) | Post-deployment diagnostic: verifies all 23 semantic-model tables exist in Gold, 17 Silver source tables, and Bronze CSV files. Reports missing/empty tables for troubleshooting. |

**Web APIs used** (all free, no authentication required):
- **frankfurter.app** — Monthly exchange rates (16 currencies, FY2024–FY2026)
- **date.nager.at** — Public holidays for 29 countries (2024–2026)
- **restcountries.com** — Country indicators (population, area, Gini index, languages)
- **openlibrary.org** — Book metadata by ISBN (subjects, cover URLs, publisher)

**Advanced Analytics tables created in Gold:**
- `GoldCohortAnalysis` — Customer retention matrix by first-purchase cohort
- `GoldRevenueAnomalies` — Daily revenue anomaly flags (30-day rolling Z-score)
- `GoldBookCoPurchase` — Book pair affinities with Support, Confidence, and Lift metrics
- `GoldRevenueForecast` — Channel revenue projection using weighted moving averages

### Pipeline Resilience & Observability

The pipeline is designed to be resilient to partial failures and observable via built-in tracking:

**API Fallbacks (NB02):**
- **frankfurter.app** — Falls back to static exchange rates if the API is unavailable or returns no data
- **restcountries.com** — Falls back to a built-in dataset covering all 28 Horizon Books countries
- **date.nager.at** — Continues with empty holiday data if unavailable
- **openlibrary.org** — Skips book enrichment gracefully on API failure

**Gold Layer Safety Net (NB03):**
- After all Silver→Gold transforms, NB03 verifies that all 18 semantic-model-required tables (10 dim + 8 fact) exist in Gold
- Any missing table is created as an empty stub with the correct schema, ensuring the Power BI report always loads without `Invalid object name` errors
- Stub tables are logged with warnings; re-running the full pipeline populates them with real data

**MLflow Experiment Tracking (NB04):**
- Fabric's automatic `mlflow.autolog()` is disabled to prevent spurious failed experiment runs
- A dedicated experiment `HorizonBooks_Forecasting` tracks all forecast runs
- **Parent run** logs global parameters (horizon, confidence level, min history)
- **Nested child runs** (one per forecast table) log model-specific parameters, metrics (row count, MAPE, elapsed time), and tags
- Aggregate metrics are logged to the parent run upon completion

**Diagnostic Notebook (NB05):**
- Quick post-deployment check that verifies all Gold, Silver, and Bronze tables/files exist
- Run standalone from the Fabric portal to troubleshoot data pipeline issues
- Deploy with `deploy/Deploy-Diagnostic.ps1`

### Option C: PBIP in Power BI Desktop

Open the PBIP projects locally for editing and development.

1. Open `HorizonBooksAnalytics/HorizonBooksAnalytics.pbip` in Power BI Desktop (Analytics report, 10 pages)
2. Open `HorizonBooksForecasting/HorizonBooksForecasting.pbip` for the Forecasting report (5 pages)
3. The semantic model uses placeholder tokens — search and replace in [expressions.tmdl](HorizonBooksAnalytics/HorizonBooksAnalytics.SemanticModel/definition/expressions.tmdl):
   - `{{SQL_ENDPOINT}}` → your GoldLH SQL endpoint (e.g. `xxxxx.datawarehouse.fabric.microsoft.com`)
   - `{{LAKEHOUSE_NAME}}` → your Gold Lakehouse name (e.g. `GoldLH`)
4. Connect and refresh to validate the model
5. Publish from Desktop to your Fabric workspace

### Option D: Manual Setup (~90 min)

---

### Step 1: Create the Lakehouse (5 min)

1. Go to your Fabric Workspace
2. Click **+ New** → **Lakehouse**
3. Name: `HorizonBooks_Lakehouse`
4. Click **Create**

### Step 2: Upload Sample Data (5 min)

1. In the Lakehouse, click **Get Data** → **Upload files**
2. Upload all 17 CSVs directly into **Files/** (flat, no subfolders):
   ```
   Files/
   ├── DimAccounts.csv
   ├── DimAuthors.csv
   ├── DimBooks.csv
   ├── DimCostCenters.csv
   ├── DimCustomers.csv
   ├── DimDepartments.csv
   ├── DimEmployees.csv
   ├── DimGeography.csv
   ├── DimWarehouses.csv
   ├── FactBudget.csv
   ├── FactFinancialTransactions.csv
   ├── FactInventory.csv
   ├── FactOrders.csv
   ├── FactPayroll.csv
   ├── FactPerformanceReviews.csv
   ├── FactRecruitment.csv
   └── FactReturns.csv
   ```
3. Upload all CSV files from the `SampleData/` folder in this repo (the deploy scripts flatten the subfolder structure automatically)

### Step 3: Create Dataflows Gen2 (15 min)

Follow the detailed guide in `Dataflows/DataflowConfiguration.md`

**Quick version:**
1. **+ New** → **Dataflow Gen2** → Name: `DF_Finance`
2. **Get Data** → **Lakehouse** → select `BronzeLH` → Files/
3. Add each CSV as a query, apply type transformations
4. Set **Data Destination** → Lakehouse `BronzeLH` → target table
5. **Publish** and wait for refresh to complete
6. Repeat for `DF_HR` and `DF_Operations`

> **Note:** When using the automated deployment (`Deploy-Pipeline.ps1`), Lakehouse destinations are **auto-configured** via `_DataDestination` queries embedded in the mashup.pq — no manual portal step is needed.

### Step 4: Run SQL Scripts (5 min)

1. Open the Lakehouse **SQL Endpoint**
2. Run `Lakehouse/GenerateDateDimension.sql` to create the DimDate table
3. Run `Lakehouse/CreateTables.sql` to create additional tables
4. Run `Lakehouse/CreateViews.sql` to create 8 analytics views:
   - `vw_BookSalesPerformance` — Book-level sales metrics
   - `vw_MonthlyFinancialSummary` — P&L by account/period
   - `vw_EmployeeCostAnalysis` — Payroll cost by employee/department
   - `vw_InventoryHealth` — Inventory status with supply alerts
   - `vw_CustomerOrderSummary` — Customer purchase summary
   - `vw_ReturnAnalysis` — Return rate by book/customer
   - `vw_GeographicSalesAnalysis` — Sales by geography
   - `vw_InternationalSalesByRegion` — Regional international sales
5. Verify all tables have data:
   ```sql
   SELECT 'DimAccounts' AS TableName, COUNT(*) AS RowCount FROM DimAccounts
   UNION ALL
   SELECT 'DimBooks', COUNT(*) FROM DimBooks
   UNION ALL
   SELECT 'DimEmployees', COUNT(*) FROM DimEmployees
   UNION ALL
   SELECT 'FactOrders', COUNT(*) FROM FactOrders
   UNION ALL
   SELECT 'FactFinancialTransactions', COUNT(*) FROM FactFinancialTransactions
   UNION ALL
   SELECT 'FactPayroll', COUNT(*) FROM FactPayroll;
   ```

### Step 5: Create the Semantic Model (20 min)

1. In the Lakehouse SQL Endpoint, click **New Semantic Model**
5. Name: `HorizonBooksModel`
3. Select ALL tables (Dim* and Fact*)
4. Open the model in the **Web Modeling** view or **Power BI Desktop**
5. **Create Relationships** as defined in `SemanticModel/SemanticModelDefinition.md`
6. **Create all DAX Measures** organized in display folders
7. Save and publish

### Step 6: Build the Power BI Reports (40 min)

**Analytics Report (10 pages):**
1. From the Semantic Model, click **Create Report**
2. Follow the layout in `Reports/ReportSpecification.md`
3. Build 10 report pages:
   - Page 1: Executive Dashboard
   - Page 2: Finance - P&L Analysis
   - Page 3: Finance - Budget vs Actual
   - Page 4: Operations - Book Performance
   - Page 5: Operations - Customer & Distribution
   - Page 6: Geographic Analysis (Map visuals)
   - Page 7: Operations - Inventory & Supply Chain
   - Page 8: HR - Workforce Overview
   - Page 9: HR - Compensation & Performance
   - Page 10: HR - Recruitment Pipeline
4. Add navigation, bookmarks, and tooltips
5. Save the report: `Horizon Books Analytics`

**Forecasting Report (5 pages):**
1. Create a second report from the same Semantic Model
2. Build 5 forecast pages (Holt-Winters projections with confidence bands):
   - Page 1: Sales Revenue Forecast (by channel)
   - Page 2: Genre Demand Forecast
   - Page 3: Financial P&L Forecast
   - Page 4: Inventory Demand Forecast
   - Page 5: Workforce Planning Forecast
3. Save the report: `Horizon Books Forecasting`

### Step 7: Create the Data Agent (10 min)

1. **+ New** → **Data Agent (Preview)**
2. Name: `Horizon Books Analytics Agent`
3. Connect to `HorizonBooksModel`
4. Add custom instructions from `DataAgent/DataAgentConfiguration.md`
5. Test with sample queries:
   - "What is our total revenue for 2024?"
   - "Which book is our bestseller?"
   - "Are there any inventory alerts?"
   - "How many open positions?"
6. Share the agent with your team

---

## 📁 Project File Structure

```
FullDemoFabricBookUseCase/
│
├── README.md                          ← This file (Master Guide)
│
├── assets/                            ← Branding & Assets
│   ├── workspace-logo.svg             ← Workspace logo (SVG source)
│   └── workspace-logo.png             ← Workspace logo (PNG, uploaded to Fabric)
│
├── deploy/                            ← PowerShell Deployment Scripts
│   ├── HorizonBooks.psm1              ← Shared helper module (tokens, API, logging)
│   ├── Deploy-Full.ps1                ← ONE-COMMAND full deployment (recommended)
│   ├── New-HorizonBooksWorkspace.ps1  ← Workspace creation + capacity + logo
│   ├── Deploy-HorizonBooks.ps1        ← Legacy step-by-step setup
│   ├── Deploy-Pipeline.ps1            ← Dataflows Gen2 (with destinations) + Pipeline
│   ├── Deploy-PowerBI.ps1             ← Semantic Model + 2 Reports (idempotent)
│   ├── Update-DataflowDestinations.ps1← Re-apply Lakehouse destinations to dataflows
│   ├── Deploy-DataAgent.ps1           ← Data Agent creation helper
│   ├── Validate-Deployment.ps1        ← Post-deploy validation checker
│   ├── Upload-SampleData.ps1          ← Standalone CSV upload to BronzeLH (OneLake DFS)
│   ├── Redeploy-Notebooks.ps1         ← Re-deploy notebooks without full redeploy
│   ├── Deploy-Diagnostic.ps1          ← Deploy diagnostic notebook (NB05)
│   └── HorizonBooks_TaskFlow.json     ← Importable Fabric Task Flow definition
│
├── notebooks/                         ← PySpark Transformation Notebooks
│   ├── 01_BronzeToSilver.py           ← Bronze→Silver (schema, quality, dedup)
│   ├── 02_WebEnrichment.py            ← Web data from 4 public APIs (with static fallbacks)
│   ├── 03_SilverToGold.py             ← Silver→Gold (RFM, cohort, anomaly, forecast + safety net)
│   ├── 04_Forecasting.py              ← Holt-Winters forecasting + MLflow experiment tracking
│   └── 05_DiagnosticCheck.py          ← Post-deploy diagnostic (Gold/Silver/Bronze validation)
│
├── tests/                             ← Pester Test Suite
│   ├── Deploy-HorizonBooks.Tests.ps1  ← Unit + Integration tests
│   └── Run-Tests.ps1                  ← Test runner script
│
├── HorizonBooksAnalytics/             ← Power BI Project (PBIP)
│   ├── HorizonBooksAnalytics.pbip     ← Project entry point
│   │
│   ├── HorizonBooksAnalytics.SemanticModel/
│   │   ├── .platform                  ← Fabric item metadata
│   │   ├── definition.pbism           ← Semantic model config (v4.2)
│   │   └── definition/
│   │       ├── database.tmdl          ← Compatibility level 1604
│   │       ├── model.tmdl             ← Table/expression refs, culture
│   │       ├── expressions.tmdl       ← Direct Lake shared expression
│   │       ├── relationships.tmdl     ← 27 relationships (2 inactive for role-playing)
│   │       └── tables/
│   │           ├── DimDate.tmdl       ← Date dimension (14 cols)
│   │           ├── DimAccounts.tmdl   ← Chart of Accounts (6 cols)
│   │           ├── DimCostCenters.tmdl
│   │           ├── DimBooks.tmdl      ← Book catalog (13 cols)
│   │           ├── DimAuthors.tmdl    ← Authors (13 cols)
│   │           ├── DimGeography.tmdl  ← Geography (13 cols, dataCategory)
│   │           ├── DimCustomers.tmdl  ← Customers (13 cols, dataCategory)
│   │           ├── DimEmployees.tmdl  ← Employees (12 cols + 5 measures)
│   │           ├── DimDepartments.tmdl
│   │           ├── DimWarehouses.tmdl ← Warehouses (12 cols, dataCategory)
│   │           ├── FactFinancialTransactions.tmdl  ← (18 measures)
│   │           ├── FactBudget.tmdl                 ← (5 measures)
│   │           ├── FactOrders.tmdl                 ← (26 measures)
│   │           ├── FactInventory.tmdl              ← (5 measures)
│   │           ├── FactReturns.tmdl                ← (5 measures)
│   │           ├── FactPayroll.tmdl                ← (7 measures)
│   │           ├── FactPerformanceReviews.tmdl     ← (3 measures)
│   │           └── FactRecruitment.tmdl            ← (5 measures)│           ├── ForecastSalesRevenue.tmdl       ← (4 measures)
│           ├── ForecastGenreDemand.tmdl        ← (3 measures)
│           ├── ForecastFinancial.tmdl          ← (3 measures)
│           ├── ForecastInventoryDemand.tmdl    ← (3 measures)
│           └── ForecastWorkforce.tmdl          ← (4 measures)│   │
│   └── HorizonBooksAnalytics.Report/
│       ├── .platform                  ← Fabric item metadata
│       ├── definition.pbir            ← Report config (PBIR v4.0)
│       └── definition/
│           ├── version.json
│           ├── report.json            ← Report settings & theme ref
│           ├── pages/
│           │   ├── pages.json         ← 10-page ordering
│           │   ├── ReportSection/     ← Executive Dashboard (10 visuals)
│           │   ├── ReportSection01/   ← Finance P&L (10 visuals)
│           │   ├── ReportSection02/   ← Budget vs Actual (6 visuals)
│           │   ├── ReportSection03/   ← Book Performance (9 visuals)
│           │   ├── ReportSection04/   ← Customers & Distribution (8 visuals)
│           │   ├── ReportSection05/   ← Geographic Analysis (11 visuals)
│           │   ├── ReportSection06/   ← Inventory & Supply Chain (9 visuals)
│           │   ├── ReportSection07/   ← Workforce Overview (8 visuals)
│           │   ├── ReportSection08/   ← Compensation & Performance (9 visuals)
│           │   └── ReportSection09/   ← Recruitment Pipeline (7 visuals)
│           └── RegisteredResources/
│               └── HorizonBooksTheme.json  ← Custom color theme
│├── HorizonBooksForecasting/               ← Forecasting Report (PBIP)
│   ├── HorizonBooksForecasting.pbip       ← Project entry point
│   └── HorizonBooksForecasting.Report/
│       ├── definition.pbir                ← Report config (PBIR v4.0)
│       └── definition/
│           ├── report.json                ← Report settings & theme ref
│           └── pages/
│               ├── pages.json             ← 5-page ordering
│               ├── ForecastPage01/        ← Sales Revenue Forecast
│               ├── ForecastPage02/        ← Genre Demand Forecast
│               ├── ForecastPage03/        ← Financial P&L Forecast
│               ├── ForecastPage04/        ← Inventory Demand Forecast
│               └── ForecastPage05/        ← Workforce Forecast
│├── SampleData/
│   ├── Finance/
│   │   ├── DimAccounts.csv            ← Chart of Accounts (28 rows)
│   │   ├── DimCostCenters.csv         ← Cost Centers (7 rows)
│   │   ├── FactFinancialTransactions.csv  ← GL Transactions (952 rows, FY2024–FY2026)
│   │   └── FactBudget.csv             ← Budget vs Actual (330 rows, monthly)
│   ├── HR/
│   │   ├── DimEmployees.csv           ← Employees (50 rows, incl. international)
│   │   ├── DimDepartments.csv         ← Departments (7 rows)
│   │   ├── FactPayroll.csv            ← Payroll Records (611 rows)
│   │   ├── FactPerformanceReviews.csv ← Reviews (123 rows, mid-year + year-end)
│   │   └── FactRecruitment.csv        ← Recruitment (40 rows)
│   └── Operations/
│       ├── DimBooks.csv               ← Book Catalog (45 rows)
│       ├── DimAuthors.csv             ← Authors (30 rows, international)
│       ├── DimCustomers.csv           ← Customers (50 rows, global)
│       ├── DimGeography.csv           ← Geography (70 rows, 29 countries)
│       ├── DimWarehouses.csv          ← Warehouses (3 rows)
│       ├── FactOrders.csv             ← Sales Orders (548 rows)
│       ├── FactInventory.csv          ← Inventory Snapshots (280 rows)
│       └── FactReturns.csv            ← Returns (70 rows)
│
├── Lakehouse/
│   ├── CreateTables.sql               ← DDL for tables
│   ├── CreateViews.sql                ← 8 analytics views (sales, finance, inventory, etc.)
│   └── GenerateDateDimension.sql      ← Date Dimension Generator
│
├── SemanticModel/
│   └── SemanticModelDefinition.md     ← Model, Relationships, DAX Measures
│
├── Reports/
│   └── ReportSpecification.md         ← 10-Page Report Layout & Specs
│
├── Forecasting/                       ← Forecasting Configuration
│   ├── README.md                      ← Forecast model documentation
│   └── forecast-config.json           ← Holt-Winters model config (5 models)
│
├── definitions/                       ← CI/CD Item Definitions
│   ├── environment/                   ← Spark Environment config
│   │   ├── environment-definition.json ← Runtime 1.3, adaptive, delta optimization
│   │   ├── public-libraries.json      ← PyPI dependencies
│   │   └── requirements.txt           ← pip-compatible format
│   └── items-manifest.json            ← Full item catalog (15 items)
│
├── .github/workflows/                 ← CI/CD Pipeline
│   └── ci-tests.yml                   ← GitHub Actions Pester tests (on push/PR to main)
│
└── DataAgent/
    └── DataAgentConfiguration.md      ← AI Agent Instructions & Config
```

---

## 📊 Key Demo Talking Points

### Finance Story
- **Total Revenue (FY2024–FY2026):** ~$3M+ across all channels (growing ~8–10% YoY)
- **Holiday Impact:** Q4 revenue is 2–3x higher than Q1 due to "Winter’s Promise" and seasonal demand
- **Budget Overperformance:** Revenue exceeded budget by ~40–80% in Q4 FY2024
- **Cost Control:** COGS stayed under budget; marketing spend increased for bestsellers
- **Rights Revenue:** Growing foreign rights deals ($90K+ in FY2024) diversify revenue
- **Multi-Year Trends:** FY2025 shows sustained growth with new title launches; FY2026 H1 data available

### Operations Story
- **Bestseller:** "Winter's Promise" dominated Q4 FY2024 with 50,000+ print run
- **New Releases:** FY2025 launches include "Starfall Legacy" (Fantasy) and "The Data Detective" (Tech)
- **Channel Mix:** Amazon/online is the largest channel (~40%), growing digital share
- **Order Volume:** 548 orders across FY2024–FY2026 (growing ~8% YoY)
- **Returns:** Industry-typical ~5–8% return rate, mainly overstock (not quality issues)
- **Fulfillment:** 93%+ on-time delivery rate, avg 3–4 day fulfillment
- **International:** 30+ international customers across Europe, Asia-Pacific, LATAM, and Africa
- **Geographic Reach:** Customers in 20+ countries, growing EMEA and APAC presence

### HR Story
- **Growing Team:** 50 employees across 7 departments, including international staff
- **Global Presence:** Staff in London, Tokyo, Frankfurt, Mexico City, and remote
- **Growth Mode:** Active recruitment pipeline (40 requisitions, 8 filled in FY2025)
- **Strong Performance:** 60%+ employees rated “Exceeds” or “Outstanding”
- **Competitive Pay:** Average tenure 4+ years suggests good retention
- **Multi-Year Payroll:** 611 payroll records spanning FY2024–FY2026

---

## 🎯 Demo Scenarios

### Scenario 1: Executive Briefing (5 min)
Start on Executive Dashboard → highlight Q4 surge → drill into Winter's Promise → 
show budget overperformance → mention headcount growth → compare FY2024 vs FY2025 trends

### Scenario 2: Finance Deep Dive (10 min)
P&L waterfall → Budget vs Actual by quarter → Cost analysis by category → 
Revenue by channel trend → Royalties impact

### Scenario 3: Operations Review (10 min)
Book performance ranking → Customer segmentation → Channel analysis → 
Inventory health check → Returns analysis → Fulfillment metrics

### Scenario 4: HR Analytics (10 min)
Workforce overview → Department distribution → Compensation analysis → 
Performance dashboard → Recruitment pipeline → Revenue per employee

### Scenario 5: AI-Powered Insights (5 min)
Open Data Agent → Ask "What's our total revenue across FY2024–FY2026?" → Ask "Any inventory alerts?" → 
Ask "Compare FY2024 vs FY2025" → Show how business users can self-serve analytics

---

## ⏱️ Setup Time Comparison

| Approach | Time | What You Get |
|---|---|---|
| **One-Command (Option A)** | **~15 min** | Everything: provision + run pipeline + model + validate |
| Step-by-Step (Option B) | ~15 min | Individual scripts for finer control |
| PBIP Desktop (Option C) | ~10 min | Local model editing + publish |
| Manual (Option D) | ~90 min | Full hands-on walkthrough |

### Automated Deployment Steps

| Step | Time | Component |
|---|---|---|
| Create Workspace + Logo | 1 min | Workspace, capacity, branding |
| Create 3 Lakehouses (schemas) | 3 min | BronzeLH, SilverLH, GoldLH + SQL endpoints |
| Upload 17 CSV files | 1 min | OneLake DFS API → BronzeLH |
| Notebook 1: Bronze→Silver | 3 min | CSV → SilverLH schemas (finance, hr, operations) |
| Dataflows + Pipeline | 1 min | 3 Dataflow Gen2 + orchestration pipeline |
| Pipeline run: DataFlows | 2 min | DF_Finance, DF_HR, DF_Operations (parallel) |
| Pipeline run: Web Enrichment | 2 min | 4 public APIs → SilverLH.web.* |
| Pipeline run: Silver→Gold | 3 min | SilverLH → GoldLH (dim, fact, analytics) |
| Deploy Semantic Model | 1 min | Direct Lake on GoldLH (96 measures, schemaName) |
| Deploy 2 Reports | <1 min | Analytics (10 pages) + Forecasting (5 pages) |
| Deploy Data Agent | <1 min | AI Q&A |
| **Total** | **~17 min** | |

---

## �️ Workspace Organization

Once deployed, the workspace is organized into **folders** and a **visual task flow** for clear navigation:

### Workspace Folders

| Folder | Contents |
|--------|----------|
| **01 - Data Storage** | BronzeLH, SilverLH, GoldLH (+ SQL Endpoints), StagingLH, StagingWH |
| **02 - Data Ingestion** | (Reserved for future connectors) |
| **03 - Data Transformation** | NB01_BronzeToSilver, NB02_WebEnrichment, NB03_SilverToGold, NB04_Forecasting, NB05_DiagnosticCheck, HorizonBooks_SparkEnv |
| **04 - Orchestration** | HorizonBooks Data Pipeline |
| **05 - Analytics** | HorizonBooksModel (Semantic Model), HorizonBooksAnalytics (Report, 10 pages), HorizonBooksForecasting (Report, 5 pages) |
| **Root** | 3 Dataflow Gen2 items (cannot be placed in folders — Fabric limitation) |

### Task Flow

A visual task flow at the top of the workspace list view shows the end-to-end pipeline:

```
[Orchestrate] → [Ingest CSV Data] → [Stage Raw Data] → [Bronze→Silver]
                                                            ↓
                [Visualize & Analyze] ← [Store Gold] ← [Silver→Gold] ← [Enrich Web Data]
```

**To set up the task flow:**
1. In the Fabric workspace, look for the task flow area at the top of the item list
2. Click **Import a task flow** and select `deploy/HorizonBooks_TaskFlow.json`
3. After import, assign workspace items to each task (item associations are not preserved in JSON exports)
4. Alternatively, select a predesigned task flow and customize it with the 8 tasks above

See details in [`deploy/HorizonBooks_TaskFlow.json`](deploy/HorizonBooks_TaskFlow.json).

---

## �📝 Notes

- All data is **fictional** and created for demo purposes
- Data spans **FY2024 (full year), FY2025 (full year), and FY2026 (H1 through June)**
- Financial figures are representative of a mid-size publisher
- Author names and book titles are entirely fictional
- Customer names reference real retailers for realism but all data is synthetic
- The demo is designed to run on a **Fabric Trial** capacity
- For larger demos, multiply the data using the patterns established here

### Data Type Notes

Some CSV columns have non-obvious formats that affect type casting in Dataflows:

| Column | File(s) | Format | M Type | Notes |
|--------|---------|--------|--------|-------|
| `FiscalYear` | FactFinancialTransactions, FactBudget | `FY2024`–`FY2026` | `type text` | Has "FY" prefix — not a pure integer |
| `FiscalMonth` | FactFinancialTransactions, FactBudget | numeric | `Int64.Type` | Integer month number (1–12) |
| `PerformanceRating` | FactPerformanceReviews | `Exceeds Expectations` | `type text` | Categorical label, not numeric |
| `GoalsMet` | FactPerformanceReviews | text | `type text` | Text descriptor |
| `TimeToFillDays` | FactRecruitment | integer or blank | `Int64.Type` | Nullable for open requisitions |
| `AccountID` | DimAccounts, FactFinancialTransactions, FactBudget | integer | `Int64.Type` | Numeric ID, not text |
| `ParentAccountID` | DimAccounts | integer or blank | `Int64.Type` | Nullable for root accounts |

### Dataflow Target Architecture

Dataflow Gen2 destinations are configured programmatically via the Fabric REST API.
Each `mashup.pq` file uses **global target parameters** for centralised connection management:

1. Two shared parameters (`TargetWorkspaceId`, `TargetLakehouseId`) are declared at the top of each mashup file with `meta [IsParameterQuery=true]`. Deploy scripts substitute `{{WORKSPACE_ID}}` / `{{BRONZE_LH_ID}}` placeholders with actual GUIDs.
2. Each table gets a `_Target` navigation query using `Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false])` pointing to the BronzeLH table. These reference the global parameters instead of inline IDs.
3. Each source query gets a `[DataDestinations = {...}]` attribute record linking it to the corresponding `_Target` query.
4. The `queryMetadata.json` marks `_Target` and parameter queries as `isHidden = true` and `loadEnabled = false`.

### Dataflow Data Quality Transformations

Beyond type enforcement, each dataflow applies domain-specific data quality rules:

- **Text standardisation**: `Text.Trim` + `Text.Proper` for names, locations, categories
- **Email normalisation**: `Text.Lower` for email addresses
- **Currency normalisation**: `Text.Upper` for currency codes, default `"USD"` for nulls
- **Numeric validation**: Non-negative clamping for monetary amounts, counts, and quantities
- **Range normalisation**: RoyaltyRate/Discount/CurrentUtilization to 0–1 (values > 1 ÷ 100)
- **Score clamping**: OverallScore clamped to 0–100
- **Coordinate validation**: Latitude [-90, 90], Longitude [-180, 180]
- **Null handling**: Defaults for missing budget amounts, empty descriptions, uncategorized items
- **Refund correction**: `Number.Abs` for RefundAmount to ensure positive values

See [`Dataflows/DataflowConfiguration.md`](Dataflows/DataflowConfiguration.md) for per-column transformation details.

To re-apply destinations after modifying dataflows in the portal:
```powershell
.\deploy\Update-DataflowDestinations.ps1 -WorkspaceId "<your-workspace-guid>"
```
