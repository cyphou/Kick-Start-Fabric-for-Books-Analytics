-- ============================================================================
-- Horizon Books Publishing & Distribution - Lakehouse Table Definitions
-- Microsoft Fabric Lakehouse SQL Endpoint
-- ============================================================================
-- Run these scripts in the Lakehouse SQL Endpoint after loading CSV data
-- via Dataflows Gen2 into the Lakehouse Tables.
-- ============================================================================

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- DimDate: Standard date dimension (auto-generated or loaded separately)
CREATE TABLE IF NOT EXISTS DimDate (
    DateKey              INT          NOT NULL,
    FullDate             DATE         NOT NULL,
    DayOfMonth           INT,
    DayOfWeek            INT,
    DayName              VARCHAR(10),
    MonthNumber           INT,
    MonthName            VARCHAR(10),
    Quarter              INT,
    QuarterName          VARCHAR(5),
    Year                 INT,
    FiscalYear           VARCHAR(10),
    FiscalQuarter        VARCHAR(5),
    IsWeekend            BOOLEAN,
    IsHoliday            BOOLEAN
);

-- DimAccounts: Chart of Accounts for Finance
CREATE TABLE IF NOT EXISTS DimAccounts (
    AccountID            INT          NOT NULL,
    AccountName          VARCHAR(100) NOT NULL,
    AccountType          VARCHAR(20)  NOT NULL,
    AccountCategory      VARCHAR(50)  NOT NULL,
    ParentAccountID      INT,
    IsActive             BOOLEAN      DEFAULT TRUE
);

-- DimCostCenters: Cost Centers / Departments for Finance
CREATE TABLE IF NOT EXISTS DimCostCenters (
    CostCenterID         VARCHAR(10)  NOT NULL,
    CostCenterName       VARCHAR(100) NOT NULL,
    Department           VARCHAR(50)  NOT NULL,
    DivisionHead         VARCHAR(100)
);

-- DimBooks: Book Catalog
CREATE TABLE IF NOT EXISTS DimBooks (
    BookID               VARCHAR(10)  NOT NULL,
    Title                VARCHAR(200) NOT NULL,
    AuthorID             VARCHAR(10)  NOT NULL,
    Genre                VARCHAR(50),
    SubGenre             VARCHAR(50),
    ISBN                 VARCHAR(20),
    PublishDate          DATE,
    ListPrice            DECIMAL(10,2),
    Format               VARCHAR(20),
    PageCount            INT,
    PrintRunSize         INT,
    ImprintName          VARCHAR(50),
    Status               VARCHAR(20)
);

-- DimAuthors: Author Information
CREATE TABLE IF NOT EXISTS DimAuthors (
    AuthorID             VARCHAR(10)  NOT NULL,
    FirstName            VARCHAR(50)  NOT NULL,
    LastName             VARCHAR(50)  NOT NULL,
    PenName              VARCHAR(100),
    AgentName            VARCHAR(100),
    AgentCompany         VARCHAR(100),
    ContractStartDate    DATE,
    ContractEndDate      DATE,
    RoyaltyRate          DECIMAL(5,2),
    AdvanceAmount        DECIMAL(12,2),
    Genre                VARCHAR(50),
    Nationality          VARCHAR(50),
    BookCount            INT
);

-- DimGeography: Geographic Dimension for Map Visuals and Regional Analysis
CREATE TABLE IF NOT EXISTS DimGeography (
    GeoID                VARCHAR(10)  NOT NULL,
    City                 VARCHAR(100) NOT NULL,
    StateProvince        VARCHAR(100),
    Country              VARCHAR(100) NOT NULL,
    Continent            VARCHAR(50),
    Region               VARCHAR(50),
    SubRegion            VARCHAR(50),
    Latitude             DECIMAL(10,6),
    Longitude            DECIMAL(10,6),
    TimeZone             VARCHAR(50),
    Currency             VARCHAR(10),
    Population           INT,
    IsCapital            BOOLEAN      DEFAULT FALSE
);

-- DimCustomers: Retailers, Distributors, Digital Channels
CREATE TABLE IF NOT EXISTS DimCustomers (
    CustomerID           VARCHAR(10)  NOT NULL,
    CustomerName         VARCHAR(100) NOT NULL,
    CustomerType         VARCHAR(30),
    ContactEmail         VARCHAR(100),
    City                 VARCHAR(50),
    State                VARCHAR(10),
    Country              VARCHAR(50),
    Region               VARCHAR(30),
    GeoID                VARCHAR(10),
    CreditLimit          DECIMAL(12,2),
    PaymentTerms         VARCHAR(20),
    IsActive             BOOLEAN      DEFAULT TRUE,
    AccountOpenDate      DATE
);

-- DimEmployees: Employee Master
CREATE TABLE IF NOT EXISTS DimEmployees (
    EmployeeID           VARCHAR(10)  NOT NULL,
    FirstName            VARCHAR(50)  NOT NULL,
    LastName             VARCHAR(50)  NOT NULL,
    Email                VARCHAR(100),
    HireDate             DATE,
    DepartmentID         VARCHAR(10),
    JobTitle             VARCHAR(100),
    ManagerID            VARCHAR(10),
    EmploymentType       VARCHAR(20),
    Location             VARCHAR(50),
    GeoID                VARCHAR(10),
    IsActive             BOOLEAN      DEFAULT TRUE
);

-- DimDepartments: Department Master
CREATE TABLE IF NOT EXISTS DimDepartments (
    DepartmentID         VARCHAR(10)  NOT NULL,
    DepartmentName       VARCHAR(100) NOT NULL,
    DepartmentHead       VARCHAR(100),
    HeadCount            INT,
    AnnualBudget         DECIMAL(12,2),
    Location             VARCHAR(50)
);

-- DimWarehouses: Warehouse / Distribution Center
CREATE TABLE IF NOT EXISTS DimWarehouses (
    WarehouseID          VARCHAR(10)  NOT NULL,
    WarehouseName        VARCHAR(100) NOT NULL,
    Address              VARCHAR(200),
    City                 VARCHAR(50),
    State                VARCHAR(10),
    Country              VARCHAR(50),
    SquareFootage        INT,
    MaxCapacityUnits     INT,
    CurrentUtilization   INT,
    ManagerID            VARCHAR(10),
    MonthlyRent          DECIMAL(10,2),
    IsActive             BOOLEAN      DEFAULT TRUE
);

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- FactFinancialTransactions: GL Transactions
CREATE TABLE IF NOT EXISTS FactFinancialTransactions (
    TransactionID        VARCHAR(20)  NOT NULL,
    TransactionDate      DATE         NOT NULL,
    AccountID            INT          NOT NULL,
    BookID               VARCHAR(10),
    Amount               DECIMAL(12,2) NOT NULL,
    Currency             VARCHAR(5)   DEFAULT 'USD',
    TransactionType      VARCHAR(10),
    FiscalYear           VARCHAR(10),
    FiscalQuarter        VARCHAR(5),
    FiscalMonth          INT,
    CostCenterID         VARCHAR(10),
    Description          VARCHAR(500)
);

-- FactBudget: Budget vs Actual (Monthly granularity)
CREATE TABLE IF NOT EXISTS FactBudget (
    BudgetID             VARCHAR(20)  NOT NULL,
    FiscalYear           VARCHAR(10)  NOT NULL,
    FiscalQuarter        VARCHAR(5)   NOT NULL,
    FiscalMonth          INT,
    AccountID            INT          NOT NULL,
    CostCenterID         VARCHAR(10),
    BudgetAmount         DECIMAL(12,2),
    ActualAmount         DECIMAL(12,2),
    Variance             DECIMAL(12,2),
    VariancePct          DECIMAL(8,2)
);

-- FactOrders: Sales Orders
CREATE TABLE IF NOT EXISTS FactOrders (
    OrderID              VARCHAR(20)  NOT NULL,
    OrderDate            DATE         NOT NULL,
    CustomerID           VARCHAR(10)  NOT NULL,
    BookID               VARCHAR(10)  NOT NULL,
    Quantity             INT,
    UnitPrice            DECIMAL(10,2),
    Discount             DECIMAL(5,2),
    TotalAmount          DECIMAL(12,2),
    OrderStatus          VARCHAR(20),
    ShipDate             DATE,
    DeliveryDate         DATE,
    WarehouseID          VARCHAR(10),
    SalesRepID           VARCHAR(10),
    Channel              VARCHAR(30)
);

-- FactInventory: Inventory Snapshots
CREATE TABLE IF NOT EXISTS FactInventory (
    InventoryID          VARCHAR(20)  NOT NULL,
    BookID               VARCHAR(10)  NOT NULL,
    WarehouseID          VARCHAR(10)  NOT NULL,
    SnapshotDate         DATE         NOT NULL,
    QuantityOnHand       INT,
    QuantityReserved     INT,
    QuantityAvailable    INT,
    ReorderPoint         INT,
    ReorderQuantity      INT,
    UnitCost             DECIMAL(10,2),
    TotalInventoryValue  DECIMAL(12,2),
    DaysOfSupply         INT,
    Status               VARCHAR(20)
);

-- FactReturns: Book Returns
CREATE TABLE IF NOT EXISTS FactReturns (
    ReturnID             VARCHAR(20)  NOT NULL,
    OrderID              VARCHAR(20)  NOT NULL,
    BookID               VARCHAR(10)  NOT NULL,
    CustomerID           VARCHAR(10)  NOT NULL,
    ReturnDate           DATE         NOT NULL,
    Quantity             INT,
    Reason               VARCHAR(100),
    ReturnStatus         VARCHAR(20),
    RefundAmount         DECIMAL(10,2),
    Condition            VARCHAR(20),
    RestockStatus        VARCHAR(20)
);

-- FactPayroll: Payroll Records
CREATE TABLE IF NOT EXISTS FactPayroll (
    PayrollID            VARCHAR(20)  NOT NULL,
    EmployeeID           VARCHAR(10)  NOT NULL,
    PayPeriodStart       DATE         NOT NULL,
    PayPeriodEnd         DATE         NOT NULL,
    BaseSalary           DECIMAL(10,2),
    Bonus                DECIMAL(10,2),
    Overtime             DECIMAL(10,2),
    Deductions           DECIMAL(10,2),
    NetPay               DECIMAL(10,2),
    PayDate              DATE
);

-- FactPerformanceReviews: Employee Reviews
CREATE TABLE IF NOT EXISTS FactPerformanceReviews (
    ReviewID             VARCHAR(20)  NOT NULL,
    EmployeeID           VARCHAR(10)  NOT NULL,
    ReviewDate           DATE         NOT NULL,
    ReviewerID           VARCHAR(10),
    PerformanceRating    VARCHAR(30),
    GoalsMet             VARCHAR(10),
    Strengths            VARCHAR(500),
    AreasForImprovement  VARCHAR(500),
    OverallScore         DECIMAL(3,1)
);

-- FactRecruitment: Hiring Pipeline
CREATE TABLE IF NOT EXISTS FactRecruitment (
    RequisitionID        VARCHAR(20)  NOT NULL,
    DepartmentID         VARCHAR(10)  NOT NULL,
    JobTitle             VARCHAR(100) NOT NULL,
    OpenDate             DATE         NOT NULL,
    CloseDate            DATE,
    Status               VARCHAR(20),
    ApplicationsReceived INT,
    Interviewed          INT,
    OffersExtended       INT,
    OfferAccepted        INT,
    HiringManagerID      VARCHAR(10),
    SalaryRangeMin       DECIMAL(10,2),
    SalaryRangeMax       DECIMAL(10,2),
    TimeToFillDays       INT
);

-- ============================================================================
-- VIEWS FOR COMMON ANALYTICS QUERIES
-- ============================================================================

-- View: Book Sales Performance Summary
CREATE OR REPLACE VIEW vw_BookSalesPerformance AS
SELECT 
    b.BookID,
    b.Title,
    CONCAT(a.FirstName, ' ', a.LastName) AS AuthorName,
    b.Genre,
    b.ImprintName,
    b.ListPrice,
    b.PrintRunSize,
    SUM(o.Quantity) AS TotalUnitsSold,
    SUM(o.TotalAmount) AS TotalRevenue,
    AVG(o.Discount) AS AvgDiscount,
    COUNT(DISTINCT o.CustomerID) AS UniqueCustomers,
    SUM(o.TotalAmount) / NULLIF(SUM(o.Quantity), 0) AS AvgRevenuePerUnit
FROM DimBooks b
    LEFT JOIN DimAuthors a ON b.AuthorID = a.AuthorID
    LEFT JOIN FactOrders o ON b.BookID = o.BookID
WHERE o.OrderStatus = 'Delivered'
GROUP BY b.BookID, b.Title, a.FirstName, a.LastName, b.Genre, b.ImprintName, b.ListPrice, b.PrintRunSize;

-- View: Monthly Financial Summary
CREATE OR REPLACE VIEW vw_MonthlyFinancialSummary AS
SELECT 
    ft.FiscalYear,
    ft.FiscalQuarter,
    ft.FiscalMonth,
    ac.AccountType,
    ac.AccountCategory,
    ac.AccountName,
    SUM(ft.Amount) AS TotalAmount,
    COUNT(*) AS TransactionCount
FROM FactFinancialTransactions ft
    JOIN DimAccounts ac ON ft.AccountID = ac.AccountID
GROUP BY ft.FiscalYear, ft.FiscalQuarter, ft.FiscalMonth, ac.AccountType, ac.AccountCategory, ac.AccountName;

-- View: Employee Cost Analysis
CREATE OR REPLACE VIEW vw_EmployeeCostAnalysis AS
SELECT 
    d.DepartmentName,
    e.EmployeeID,
    CONCAT(e.FirstName, ' ', e.LastName) AS EmployeeName,
    e.JobTitle,
    e.HireDate,
    SUM(p.BaseSalary) AS TotalBaseSalary,
    SUM(p.Bonus) AS TotalBonus,
    SUM(p.Overtime) AS TotalOvertime,
    SUM(p.NetPay) AS TotalNetPay,
    COUNT(*) AS PayPeriods
FROM DimEmployees e
    JOIN DimDepartments d ON e.DepartmentID = d.DepartmentID
    LEFT JOIN FactPayroll p ON e.EmployeeID = p.EmployeeID
GROUP BY d.DepartmentName, e.EmployeeID, e.FirstName, e.LastName, e.JobTitle, e.HireDate;

-- View: Inventory Health Dashboard
CREATE OR REPLACE VIEW vw_InventoryHealth AS
SELECT 
    b.BookID,
    b.Title,
    b.Genre,
    i.WarehouseID,
    w.WarehouseName,
    i.SnapshotDate,
    i.QuantityOnHand,
    i.QuantityAvailable,
    i.ReorderPoint,
    i.DaysOfSupply,
    i.TotalInventoryValue,
    i.Status,
    CASE 
        WHEN i.DaysOfSupply < 20 THEN 'Critical'
        WHEN i.DaysOfSupply < 50 THEN 'Low'
        WHEN i.DaysOfSupply < 100 THEN 'Adequate'
        ELSE 'Surplus'
    END AS SupplyStatus
FROM FactInventory i
    JOIN DimBooks b ON i.BookID = b.BookID
    JOIN DimWarehouses w ON i.WarehouseID = w.WarehouseID;

-- View: Customer Order Summary
CREATE OR REPLACE VIEW vw_CustomerOrderSummary AS
SELECT 
    c.CustomerID,
    c.CustomerName,
    c.CustomerType,
    c.Region,
    COUNT(DISTINCT o.OrderID) AS TotalOrders,
    SUM(o.Quantity) AS TotalUnitsSold,
    SUM(o.TotalAmount) AS TotalRevenue,
    AVG(o.Discount) AS AvgDiscount,
    MIN(o.OrderDate) AS FirstOrderDate,
    MAX(o.OrderDate) AS LastOrderDate,
    COUNT(DISTINCT o.BookID) AS UniqueTitlesOrdered
FROM DimCustomers c
    LEFT JOIN FactOrders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID, c.CustomerName, c.CustomerType, c.Region;

-- View: Return Rate Analysis
CREATE OR REPLACE VIEW vw_ReturnAnalysis AS
SELECT 
    b.BookID,
    b.Title,
    b.Genre,
    c.CustomerName,
    c.CustomerType,
    SUM(r.Quantity) AS TotalReturned,
    SUM(r.RefundAmount) AS TotalRefunded,
    r.Reason,
    r.Condition
FROM FactReturns r
    JOIN DimBooks b ON r.BookID = b.BookID
    JOIN DimCustomers c ON r.CustomerID = c.CustomerID
GROUP BY b.BookID, b.Title, b.Genre, c.CustomerName, c.CustomerType, r.Reason, r.Condition;

-- View: Geographic Sales Analysis
CREATE OR REPLACE VIEW vw_GeographicSalesAnalysis AS
SELECT 
    g.Country,
    g.Continent,
    g.Region,
    g.City,
    g.Latitude,
    g.Longitude,
    c.CustomerName,
    c.CustomerType,
    COUNT(DISTINCT o.OrderID) AS TotalOrders,
    SUM(o.Quantity) AS TotalUnitsSold,
    SUM(o.TotalAmount) AS TotalRevenue
FROM DimGeography g
    JOIN DimCustomers c ON g.GeoID = c.GeoID
    LEFT JOIN FactOrders o ON c.CustomerID = o.CustomerID
GROUP BY g.Country, g.Continent, g.Region, g.City, g.Latitude, g.Longitude, c.CustomerName, c.CustomerType;

-- View: International Sales by Region
CREATE OR REPLACE VIEW vw_InternationalSalesByRegion AS
SELECT 
    g.Continent,
    g.Region,
    g.Country,
    COUNT(DISTINCT c.CustomerID) AS CustomerCount,
    COUNT(DISTINCT o.OrderID) AS OrderCount,
    SUM(o.TotalAmount) AS TotalRevenue,
    SUM(o.Quantity) AS TotalUnits,
    AVG(o.Discount) AS AvgDiscount
FROM DimGeography g
    JOIN DimCustomers c ON g.GeoID = c.GeoID
    LEFT JOIN FactOrders o ON c.CustomerID = o.CustomerID
WHERE o.OrderStatus IN ('Delivered', 'Shipped')
GROUP BY g.Continent, g.Region, g.Country;
