-- ============================================================================
-- Horizon Books Publishing & Distribution - Analytics Views
-- Microsoft Fabric Lakehouse SQL Endpoint
-- ============================================================================
-- These views provide pre-built analytics queries against the Gold layer
-- star schema. Run against the Lakehouse SQL Endpoint after Gold tables
-- have been populated by NB03 and NB04.
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
