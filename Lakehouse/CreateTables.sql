-- ============================================================================
-- Horizon Books Publishing & Distribution - Lakehouse Table Definitions
-- Microsoft Fabric Lakehouse SQL Endpoint
-- ============================================================================
-- REFERENCE ONLY — In the automated deployment, NB03 (SilverToGold)
-- creates all dimension and fact tables programmatically with schema
-- prefixes (dim.DimAccounts, fact.FactOrders, etc.).
--
-- These DDL statements are provided for manual setup (Option D) or as
-- documentation of the expected table structures. Table names here use
-- the default (dbo) schema for SQL Endpoint compatibility.
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
-- ANALYTICS VIEWS
-- ============================================================================
-- Views have been moved to CreateViews.sql for separation of concerns.
-- See: Lakehouse/CreateViews.sql
-- ============================================================================
