-- ============================================================================
-- Horizon Books - Date Dimension Generator
-- Run this AFTER creating tables to populate DimDate with 2023-2025 range
-- ============================================================================

-- Generate dates from 2023-01-01 to 2025-12-31
INSERT INTO DimDate
SELECT
    CAST(DATE_FORMAT(d.date_val, 'yyyyMMdd') AS INT) AS DateKey,
    d.date_val AS FullDate,
    DAY(d.date_val) AS DayOfMonth,
    DAYOFWEEK(d.date_val) AS DayOfWeek,
    DATE_FORMAT(d.date_val, 'EEEE') AS DayName,
    MONTH(d.date_val) AS MonthNumber,
    DATE_FORMAT(d.date_val, 'MMMM') AS MonthName,
    QUARTER(d.date_val) AS Quarter,
    CONCAT('Q', QUARTER(d.date_val)) AS QuarterName,
    YEAR(d.date_val) AS Year,
    CONCAT('FY', YEAR(d.date_val)) AS FiscalYear,
    CONCAT('Q', QUARTER(d.date_val)) AS FiscalQuarter,
    CASE WHEN DAYOFWEEK(d.date_val) IN (1, 7) THEN TRUE ELSE FALSE END AS IsWeekend,
    FALSE AS IsHoliday
FROM (
    SELECT EXPLODE(SEQUENCE(
        TO_DATE('2023-01-01'), 
        TO_DATE('2025-12-31'), 
        INTERVAL 1 DAY
    )) AS date_val
) d;
