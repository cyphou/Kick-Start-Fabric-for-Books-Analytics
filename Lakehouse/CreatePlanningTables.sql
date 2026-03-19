-- ============================================================================
-- Horizon Books — Planning in Fabric IQ: Table Definitions
-- Microsoft Fabric Lakehouse SQL Endpoint
-- ============================================================================
-- Planning tables live in the GoldLH under the `planning` schema.
-- These are writeback-ready structures designed for use with Planning in
-- Fabric IQ — budget targets, scenario models, variance analysis, and
-- executive scenario summaries.
--
-- Reference: https://blog.fabric.microsoft.com/en-us/blog/introducing-planning-in-microsoft-fabric-iq-from-historical-data-to-forecasting-the-future
-- ============================================================================

-- ============================================================================
-- PLANNING SCHEMA
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS planning;

-- ============================================================================
-- 1. PlanRevenueTargets — Channel revenue targets with scenario modeling
-- ============================================================================
CREATE TABLE IF NOT EXISTS planning.PlanRevenueTargets (
    PlanMonth            DATE         NOT NULL,
    Channel              VARCHAR(50)  NOT NULL,
    Scenario             VARCHAR(20)  NOT NULL,    -- Base | Optimistic | Conservative
    TargetRevenue        DECIMAL(15,2) NOT NULL,
    FiscalYear           VARCHAR(10),
    FiscalQuarter        VARCHAR(5),
    PlanHorizon          INT,                       -- 1-12 (months ahead)
    RecordType           VARCHAR(20)  DEFAULT 'Plan',
    ApprovalStatus       VARCHAR(20)  DEFAULT 'Draft',  -- Draft | Submitted | Approved | Rejected
    LastModifiedBy       VARCHAR(100),
    LastModifiedAt       TIMESTAMP,
    _generated_at        TIMESTAMP
);

-- ============================================================================
-- 2. PlanFinancialTargets — P&L account targets with scenario modeling
-- ============================================================================
CREATE TABLE IF NOT EXISTS planning.PlanFinancialTargets (
    PlanMonth            DATE         NOT NULL,
    AccountType          VARCHAR(50)  NOT NULL,
    CostCenterID         VARCHAR(10),
    Scenario             VARCHAR(20)  NOT NULL,    -- Base | Optimistic | Conservative
    PlannedAmount        DECIMAL(15,2) NOT NULL,
    FiscalYear           VARCHAR(10),
    FiscalQuarter        VARCHAR(5),
    PlanHorizon          INT,
    RecordType           VARCHAR(20)  DEFAULT 'Plan',
    ApprovalStatus       VARCHAR(20)  DEFAULT 'Draft',
    LastModifiedBy       VARCHAR(100),
    LastModifiedAt       TIMESTAMP,
    _generated_at        TIMESTAMP
);

-- ============================================================================
-- 3. PlanWorkforceTargets — Headcount and payroll targets by department
-- ============================================================================
CREATE TABLE IF NOT EXISTS planning.PlanWorkforceTargets (
    PlanMonth            DATE         NOT NULL,
    Department           VARCHAR(100) NOT NULL,
    Scenario             VARCHAR(20)  NOT NULL,    -- Base | Optimistic | Conservative
    PlannedHeadcount     INT          NOT NULL,
    PlannedPayroll       DECIMAL(15,2) NOT NULL,
    FiscalYear           VARCHAR(10),
    FiscalQuarter        VARCHAR(5),
    PlanHorizon          INT,
    RecordType           VARCHAR(20)  DEFAULT 'Plan',
    ApprovalStatus       VARCHAR(20)  DEFAULT 'Draft',
    LastModifiedBy       VARCHAR(100),
    LastModifiedAt       TIMESTAMP,
    _generated_at        TIMESTAMP
);

-- ============================================================================
-- 4. PlanVarianceAnalysis — Consolidated plan-vs-actual variance
-- ============================================================================
CREATE TABLE IF NOT EXISTS planning.PlanVarianceAnalysis (
    Domain               VARCHAR(50)  NOT NULL,    -- Revenue | Finance | Workforce
    Category             VARCHAR(100) NOT NULL,
    FiscalYear           VARCHAR(10)  NOT NULL,
    FiscalQuarter        VARCHAR(5),
    FiscalMonth          INT,
    PlannedAmount        DECIMAL(15,2),
    ActualAmount         DECIMAL(15,2),
    Variance             DECIMAL(15,2),
    VariancePct          DECIMAL(8,4),
    Status               VARCHAR(20),              -- Favorable | On Track | Unfavorable | Critical
    _generated_at        TIMESTAMP
);

-- ============================================================================
-- 5. PlanScenarioSummary — Executive scenario comparison
-- ============================================================================
CREATE TABLE IF NOT EXISTS planning.PlanScenarioSummary (
    Domain               VARCHAR(50)  NOT NULL,    -- Revenue | Finance | Workforce
    Scenario             VARCHAR(20)  NOT NULL,    -- Base | Optimistic | Conservative
    FiscalQuarter        VARCHAR(5)   NOT NULL,
    PlannedTotal         DECIMAL(15,2) NOT NULL,
    RecordType           VARCHAR(20)  DEFAULT 'ScenarioSummary',
    _generated_at        TIMESTAMP
);

-- ============================================================================
-- PLANNING VIEWS — Analytics overlays for Power BI / Fabric IQ dashboards
-- ============================================================================

-- View: Revenue Plan vs Forecast comparison
CREATE OR REPLACE VIEW planning.vw_RevenuePlanVsForecast AS
SELECT
    p.PlanMonth,
    p.Channel,
    p.Scenario,
    p.TargetRevenue       AS PlannedRevenue,
    f.Revenue             AS ForecastRevenue,
    p.TargetRevenue - COALESCE(f.Revenue, 0) AS PlanForecastGap,
    p.ApprovalStatus,
    p.FiscalYear,
    p.FiscalQuarter
FROM planning.PlanRevenueTargets p
LEFT JOIN analytics.ForecastSalesRevenue f
    ON p.PlanMonth = f.ForecastMonth
    AND p.Channel = f.Channel
    AND f.RecordType = 'Forecast'
WHERE p.RecordType = 'Plan';

-- View: Financial Plan vs Budget alignment
CREATE OR REPLACE VIEW planning.vw_FinancialPlanVsBudget AS
SELECT
    p.PlanMonth,
    p.AccountType,
    p.Scenario,
    p.PlannedAmount,
    b.BudgetAmount        AS OriginalBudget,
    b.ActualAmount,
    p.PlannedAmount - COALESCE(b.BudgetAmount, 0) AS PlanBudgetDelta,
    b.Variance            AS BudgetVariance,
    p.ApprovalStatus,
    p.FiscalYear,
    p.FiscalQuarter
FROM planning.PlanFinancialTargets p
LEFT JOIN fact.FactBudget b
    ON p.FiscalYear = b.FiscalYear
    AND p.AccountType = b.AccountType
WHERE p.RecordType = 'Plan';

-- View: Workforce planning summary
CREATE OR REPLACE VIEW planning.vw_WorkforcePlanSummary AS
SELECT
    p.Department,
    p.Scenario,
    p.FiscalQuarter,
    SUM(p.PlannedHeadcount) AS TotalPlannedHC,
    SUM(p.PlannedPayroll)   AS TotalPlannedPayroll,
    AVG(p.PlannedPayroll / NULLIF(p.PlannedHeadcount, 0)) AS AvgCostPerHead,
    p.FiscalYear
FROM planning.PlanWorkforceTargets p
WHERE p.RecordType = 'Plan'
GROUP BY p.Department, p.Scenario, p.FiscalQuarter, p.FiscalYear;
