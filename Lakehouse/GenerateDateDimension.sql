-- ============================================================================
-- Horizon Books - Date Dimension Generator
-- ============================================================================
-- REFERENCE ONLY — In the automated deployment, NB03 (SilverToGold)
-- generates a richer DimDate with holiday integration, fiscal periods,
-- and a wider date range. This script is kept for manual setup (Option D).
-- ============================================================================
--
-- Generate dates from 2023-01-01 to 2027-12-31
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
        TO_DATE('2027-12-31'), 
        INTERVAL 1 DAY
    )) AS date_val
) d;
