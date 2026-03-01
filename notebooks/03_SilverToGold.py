# Synapse Analytics notebook source
# ============================================================================
# Horizon Books - Notebook 3: Silver to Gold
# Applies business logic, builds derived tables, generates DimDate,
# integrates web data, RFM segmentation, cohort analysis, anomaly
# detection, book co-purchasing, and simple forecasting
# ============================================================================

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "70d8979b-18bb-4af6-9e7b-44e7ad96393d",
# META       "default_lakehouse_name": "GoldLH",
# META       "default_lakehouse_workspace_id": "91b2dca3-5729-4e7d-a473-bfeb85c16aa1",
# META       "known_lakehouses": [
# META         {
# META           "id": "bc992d2e-8d01-451b-b96d-e7435fcf4c62"
# META         },
# META         {
# META           "id": "f4f99f30-f239-44e9-8f37-0ec19d1458fe"
# META         },
# META         {
# META           "id": "70d8979b-18bb-4af6-9e7b-44e7ad96393d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 1: Setup and read Silver tables
# -----------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, concat, concat_ws, upper, lower, initcap,
    year, month, quarter, dayofmonth, dayofweek, weekofyear, date_format,
    to_date, datediff, months_between, current_date, current_timestamp,
    sum as spark_sum, count, countDistinct, avg, min as spark_min,
    max as spark_max, first, last, lag, lead, dense_rank,
    row_number, percent_rank, ntile, round as spark_round,
    expr, explode, sequence, array, struct, broadcast,
    stddev, variance, abs as spark_abs, log as spark_log,
    collect_list, collect_set, size, array_intersect,
    date_add, date_sub, add_months, trunc,
    monotonically_increasing_id, lpad
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    IntegerType, DoubleType, StringType, BooleanType, DateType,
    ArrayType, StructType, StructField
)

spark = SparkSession.builder.getOrCreate()

# ── Medallion Architecture Configuration ──
# Default lakehouse = GoldLH (schema-enabled)
# Read from SilverLH using cross-lakehouse naming
# Write to GoldLH schemas: dim, fact, analytics
SILVER_LH = "SilverLH"
GOLD_SCHEMAS = ["dim", "fact", "analytics"]

# Silver table schema mapping (for cross-LH reads)
SILVER_TABLE_SCHEMAS = {
    "DimAccounts": "finance", "DimCostCenters": "finance",
    "FactFinancialTransactions": "finance", "FactBudget": "finance",
    "DimEmployees": "hr", "DimDepartments": "hr",
    "FactPayroll": "hr", "FactPerformanceReviews": "hr", "FactRecruitment": "hr",
    "DimBooks": "operations", "DimAuthors": "operations",
    "DimGeography": "operations", "DimCustomers": "operations",
    "DimWarehouses": "operations",
    "FactOrders": "operations", "FactInventory": "operations", "FactReturns": "operations",
    "WebExchangeRates": "web", "WebPublicHolidays": "web",
    "WebCountryIndicators": "web", "WebBookMetadata": "web",
}

def read_silver(name):
    """Read a table from SilverLH using cross-lakehouse naming."""
    schema = SILVER_TABLE_SCHEMAS.get(name, "dbo")
    return spark.table(f"{SILVER_LH}.{schema}.{name}")

def gold_table(name, gold_schema):
    """Return schema-qualified Gold table name."""
    return f"{gold_schema}.{name}"

def fill_null_columns(df, columns, default=0):
    """Fill nulls in the given columns with a default value."""
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, coalesce(col(c), lit(default)))
    return df

# Create Gold schemas
for schema in GOLD_SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"  ✓ Gold schema '{schema}' ready")

print("\n=== Horizon Books: Silver → Gold Transformations ===")
print(f"Spark version: {spark.version}")
print(f"Source: {SILVER_LH} | Target: GoldLH (schemas: {', '.join(GOLD_SCHEMAS)})")

# Read existing Silver tables from SilverLH
tables_to_read = [
    "DimAccounts", "DimCostCenters", "DimBooks", "DimAuthors",
    "DimGeography", "DimCustomers", "DimEmployees", "DimDepartments",
    "DimWarehouses",
    "FactFinancialTransactions", "FactBudget", "FactOrders",
    "FactInventory", "FactReturns", "FactPayroll",
    "FactPerformanceReviews", "FactRecruitment"
]

dfs = {}
for t in tables_to_read:
    try:
        dfs[t] = read_silver(t)
        print(f"  ✓ Read {SILVER_LH}.{SILVER_TABLE_SCHEMAS[t]}.{t}: {dfs[t].count()} rows")
    except Exception as e:
        print(f"  ⚠ Could not read {t}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 2: Generate DimDate (Calendar Dimension)
# -----------------------------------------------------------
from datetime import date

print("\n" + "="*60)
print("  Generating DimDate (2022-01-01 to 2027-12-31)")
print("="*60)

start_date = date(2022, 1, 1)
end_date = date(2027, 12, 31)

# Generate date sequence
date_df = spark.sql(
    f"SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as dates"
).select(explode("dates").alias("FullDate"))

# Build comprehensive date dimension
dim_date = date_df.select(
    # Key
    (year("FullDate") * 10000 + month("FullDate") * 100 + dayofmonth("FullDate"))
        .cast(IntegerType()).alias("DateKey"),

    col("FullDate"),

    # Day attributes
    dayofmonth("FullDate").alias("DayOfMonth"),
    dayofweek("FullDate").alias("DayOfWeek"),
    date_format("FullDate", "EEEE").alias("DayName"),
    date_format("FullDate", "E").alias("DayNameShort"),
    when(dayofweek("FullDate").isin(1, 7), lit(True)).otherwise(lit(False)).alias("IsWeekend"),
    lit(False).alias("IsHoliday"),

    # Week
    weekofyear("FullDate").alias("WeekOfYear"),
    concat(lit("W"), lpad(weekofyear("FullDate").cast("string"), 2, "0")).alias("WeekLabel"),

    # Month
    month("FullDate").alias("MonthNumber"),
    date_format("FullDate", "MMMM").alias("MonthName"),
    date_format("FullDate", "MMM").alias("MonthNameShort"),
    concat(date_format("FullDate", "yyyy"), lit("-"), date_format("FullDate", "MM")).alias("YearMonth"),

    # Quarter
    quarter("FullDate").alias("Quarter"),
    concat(lit("Q"), quarter("FullDate")).alias("QuarterName"),
    concat(year("FullDate").cast(StringType()), lit("-Q"), quarter("FullDate").cast(StringType())).alias("YearQuarter"),

    # Year
    year("FullDate").alias("Year"),

    # Fiscal calendar (July start)
    when(month("FullDate") >= 7, year("FullDate") + 1).otherwise(year("FullDate")).alias("FiscalYear"),
    concat(
        lit("FY"),
        when(month("FullDate") >= 7, year("FullDate") + 1).otherwise(year("FullDate"))
    ).alias("FiscalYearLabel"),
    when(month("FullDate") >= 7,
         ((month("FullDate") - 7) / 3 + 1).cast(IntegerType())
    ).otherwise(
         ((month("FullDate") + 5) / 3 + 1).cast(IntegerType())
    ).alias("FiscalQuarter"),
    concat(
        lit("FQ"),
        when(month("FullDate") >= 7,
             ((month("FullDate") - 7) / 3 + 1).cast(IntegerType())
        ).otherwise(
             ((month("FullDate") + 5) / 3 + 1).cast(IntegerType())
        )
    ).alias("FiscalQuarterLabel"),

    # Relative flags — dynamic at runtime via current_date()
    when(
        (year("FullDate") == year(current_date())) & (month("FullDate") == month(current_date())),
        lit(True)
    ).otherwise(lit(False)).alias("IsCurrentMonth"),

    when(year("FullDate") == year(current_date()), lit(True)).otherwise(lit(False)).alias("IsCurrentYear"),
)

dim_date.write.mode("overwrite").format("delta").saveAsTable(gold_table("DimDate", "dim"))
date_count = dim_date.count()
print(f"  ✓ DimDate generated: {date_count} rows")
print(f"    Date range: {start_date} to {end_date}")

# Enrich with public holidays from WebPublicHolidays (fetched by Notebook 2)
try:
    df_holidays = spark.table(f"{SILVER_LH}.web.WebPublicHolidays")
    # US holidays as the primary calendar (Horizon Books HQ)
    us_holidays = df_holidays.filter(col("CountryCode") == "US").select(
        col("HolidayDate"),
        col("HolidayNameEnglish").alias("HolidayName")
    ).distinct()

    dim_date_enriched = dim_date.join(
        broadcast(us_holidays),
        dim_date["FullDate"] == us_holidays["HolidayDate"],
        "left"
    ).withColumn("IsHoliday",
        when(col("HolidayDate").isNotNull(), lit(True)).otherwise(lit(False))
    ).withColumn("HolidayName",
        coalesce(col("HolidayName"), lit(None).cast(StringType()))
    ).withColumn("IsBusinessDay",
        when(col("IsWeekend") | col("IsHoliday"), lit(False)).otherwise(lit(True))
    ).drop("HolidayDate")

    dim_date_enriched.write.mode("overwrite").format("delta").saveAsTable(gold_table("DimDate", "dim"))
    holiday_count = dim_date_enriched.filter(col("IsHoliday")).count()
    print(f"  ✓ DimDate enriched with {holiday_count} US public holidays")
    print(f"    Added: HolidayName, IsBusinessDay")
except Exception as e:
    print(f"  ⚠ WebPublicHolidays not available, IsHoliday remains FALSE: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 3: Enrich DimBooks with sales aggregates
# -----------------------------------------------------------

print("\n" + "="*60)
print("  Enriching DimBooks with sales metrics")
print("="*60)

df_books = dfs.get("DimBooks")
df_orders = dfs.get("FactOrders")

if df_books is not None and df_orders is not None:
    # Calculate sales metrics per book
    book_sales = (df_orders
        .groupBy("BookID")
        .agg(
            spark_sum("Quantity").alias("TotalUnitsSold"),
            spark_sum("TotalAmount").alias("TotalRevenue"),
            countDistinct("CustomerID").alias("UniqueCustomers"),
            countDistinct("OrderID").alias("TotalOrders"),
            avg("Discount").alias("AvgDiscount"),
            spark_min("OrderDate").alias("FirstOrderDate"),
            spark_max("OrderDate").alias("LastOrderDate"),
            avg("Quantity").alias("AvgOrderQuantity")
        )
    )

    # Calculate return metrics per book
    df_returns = dfs.get("FactReturns")
    if df_returns is not None:
        book_returns = (df_returns
            .groupBy("BookID")
            .agg(
                spark_sum("Quantity").alias("TotalUnitsReturned"),
                spark_sum("RefundAmount").alias("TotalRefunds"),
                count("ReturnID").alias("ReturnCount")
            )
        )
    else:
        book_returns = None

    # Join enrichments back to DimBooks
    df_books_enriched = df_books.join(book_sales, "BookID", "left")

    if book_returns is not None:
        df_books_enriched = df_books_enriched.join(book_returns, "BookID", "left")
        df_books_enriched = df_books_enriched.withColumn(
            "ReturnRate",
            when(col("TotalUnitsSold") > 0,
                 spark_round(coalesce(col("TotalUnitsReturned"), lit(0)) / col("TotalUnitsSold") * 100, 2)
            ).otherwise(lit(0.0))
        )
    else:
        df_books_enriched = (df_books_enriched
            .withColumn("TotalUnitsReturned", lit(0))
            .withColumn("TotalRefunds", lit(0.0))
            .withColumn("ReturnCount", lit(0))
            .withColumn("ReturnRate", lit(0.0))
        )

    # Compute revenue per unit and book rank
    df_books_enriched = (df_books_enriched
        .withColumn("RevenuePerUnit",
            when(col("TotalUnitsSold") > 0,
                 spark_round(col("TotalRevenue") / col("TotalUnitsSold"), 2)
            ).otherwise(lit(0.0))
        )
        .withColumn("SalesRank",
            dense_rank().over(Window.orderBy(col("TotalRevenue").desc()))
        )
        .withColumn("PerformanceTier",
            when(col("SalesRank") <= 5, lit("Top 5"))
            .when(col("SalesRank") <= 15, lit("Top 15"))
            .when(col("SalesRank") <= 30, lit("Mid-tier"))
            .otherwise(lit("Long Tail"))
        )
    )

    # Fill nulls for books with no sales
    df_books_enriched = fill_null_columns(df_books_enriched, [
        "TotalUnitsSold", "TotalRevenue", "UniqueCustomers", "TotalOrders",
        "AvgOrderQuantity", "TotalUnitsReturned", "TotalRefunds", "ReturnCount"
    ])

    df_books_enriched.write.mode("overwrite").format("delta").saveAsTable(gold_table("DimBooks", "dim"))
    print(f"  ✓ DimBooks enriched: {df_books_enriched.count()} rows")
    print(f"    Added: TotalUnitsSold, TotalRevenue, UniqueCustomers, ReturnRate, SalesRank, PerformanceTier")
else:
    print("  ⚠ DimBooks or FactOrders not available, skipping enrichment")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 4: Enrich DimCustomers with lifetime metrics
# -----------------------------------------------------------

print("\n" + "="*60)
print("  Enriching DimCustomers with lifetime value metrics")
print("="*60)

df_customers = dfs.get("DimCustomers")
df_orders = dfs.get("FactOrders")

if df_customers is not None and df_orders is not None:
    # Customer purchase metrics
    customer_metrics = (df_orders
        .groupBy("CustomerID")
        .agg(
            spark_sum("TotalAmount").alias("LifetimeRevenue"),
            count("OrderID").alias("TotalOrders"),
            spark_sum("Quantity").alias("TotalUnitsPurchased"),
            countDistinct("BookID").alias("UniqueTitles"),
            avg("TotalAmount").alias("AvgOrderValue"),
            spark_min("OrderDate").alias("FirstOrderDate"),
            spark_max("OrderDate").alias("LastOrderDate"),
            countDistinct("Channel").alias("ChannelsUsed")
        )
    )

    # Add return metrics
    df_returns = dfs.get("FactReturns")
    if df_returns is not None:
        customer_returns = (df_returns
            .groupBy("CustomerID")
            .agg(
                spark_sum("RefundAmount").alias("TotalRefunds"),
                count("ReturnID").alias("ReturnCount")
            )
        )
        customer_metrics = customer_metrics.join(customer_returns, "CustomerID", "left")
    else:
        customer_metrics = (customer_metrics
            .withColumn("TotalRefunds", lit(0.0))
            .withColumn("ReturnCount", lit(0))
        )

    df_customers_enriched = df_customers.join(customer_metrics, "CustomerID", "left")

    # Compute derived metrics
    df_customers_enriched = (df_customers_enriched
        # Net revenue (revenue minus refunds)
        .withColumn("NetRevenue",
            coalesce(col("LifetimeRevenue"), lit(0.0)) - coalesce(col("TotalRefunds"), lit(0.0))
        )
        # Return rate
        .withColumn("ReturnRate",
            when(col("TotalOrders") > 0,
                 spark_round(coalesce(col("ReturnCount"), lit(0)) / col("TotalOrders") * 100, 2)
            ).otherwise(lit(0.0))
        )
        # Customer tenure in days
        .withColumn("CustomerTenureDays",
            when(col("FirstOrderDate").isNotNull(),
                 datediff(coalesce(col("LastOrderDate"), current_date()), col("FirstOrderDate"))
            ).otherwise(lit(0))
        )
        # Recency (days since last order vs. current date)
        .withColumn("DaysSinceLastOrder",
            when(col("LastOrderDate").isNotNull(),
                 datediff(current_date(), col("LastOrderDate"))
            ).otherwise(lit(999))
        )
        # Customer segment by value
        .withColumn("ValueSegment",
            when(col("NetRevenue") >= 50000, lit("Platinum"))
            .when(col("NetRevenue") >= 20000, lit("Gold"))
            .when(col("NetRevenue") >= 5000, lit("Silver"))
            .otherwise(lit("Bronze"))
        )
        # Activity flag
        .withColumn("ActivityStatus",
            when(col("DaysSinceLastOrder") <= 90, lit("Active"))
            .when(col("DaysSinceLastOrder") <= 180, lit("At Risk"))
            .when(col("DaysSinceLastOrder") <= 365, lit("Dormant"))
            .otherwise(lit("Churned"))
        )
    )

    # ── RFM Segmentation (Recency, Frequency, Monetary) ──
    # Score each dimension 1-5 using ntile quantile bucketing
    df_customers_enriched = (df_customers_enriched
        .withColumn("R_Score",
            when(col("DaysSinceLastOrder") == 999, lit(1))
            .otherwise(
                ntile(5).over(
                    Window.orderBy(col("DaysSinceLastOrder").desc())  # Lower recency = higher score
                )
            )
        )
        .withColumn("F_Score",
            ntile(5).over(Window.orderBy(col("TotalOrders")))
        )
        .withColumn("M_Score",
            ntile(5).over(Window.orderBy(coalesce(col("NetRevenue"), lit(0))))
        )
        .withColumn("RFM_Score",
            col("R_Score") * 100 + col("F_Score") * 10 + col("M_Score")
        )
        .withColumn("RFM_Segment",
            when((col("R_Score") >= 4) & (col("F_Score") >= 4) & (col("M_Score") >= 4), lit("Champion"))
            .when((col("R_Score") >= 4) & (col("F_Score") >= 3), lit("Loyal"))
            .when((col("R_Score") >= 4) & (col("F_Score") <= 2), lit("New Customer"))
            .when((col("R_Score") >= 3) & (col("M_Score") >= 4), lit("Big Spender"))
            .when((col("R_Score") <= 2) & (col("F_Score") >= 3), lit("At Risk Loyal"))
            .when((col("R_Score") <= 2) & (col("M_Score") >= 3), lit("Cant Lose"))
            .when((col("R_Score") <= 2) & (col("F_Score") <= 2), lit("Lost"))
            .otherwise(lit("Potential"))
        )
    )

    # Fill nulls
    df_customers_enriched = fill_null_columns(df_customers_enriched, [
        "LifetimeRevenue", "TotalOrders", "TotalUnitsPurchased",
        "UniqueTitles", "AvgOrderValue", "TotalRefunds", "ReturnCount"
    ])

    df_customers_enriched.write.mode("overwrite").format("delta").saveAsTable(gold_table("DimCustomers", "dim"))
    print(f"  ✓ DimCustomers enriched: {df_customers_enriched.count()} rows")
    print(f"    Added: LifetimeRevenue, NetRevenue, ValueSegment, ActivityStatus, ReturnRate")
    print(f"    Added: R_Score, F_Score, M_Score, RFM_Score, RFM_Segment")
else:
    print("  ⚠ DimCustomers or FactOrders not available, skipping enrichment")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 5: Enrich DimEmployees with performance & payroll metrics
# -----------------------------------------------------------

print("\n" + "="*60)
print("  Enriching DimEmployees with HR metrics")
print("="*60)

df_employees = dfs.get("DimEmployees")
df_payroll = dfs.get("FactPayroll")
df_reviews = dfs.get("FactPerformanceReviews")

if df_employees is not None:
    # Payroll aggregates
    if df_payroll is not None:
        emp_payroll = (df_payroll
            .groupBy("EmployeeID")
            .agg(
                spark_sum("BaseSalary").alias("TotalBasePay"),
                spark_sum("Bonus").alias("TotalBonuses"),
                spark_sum("Overtime").alias("TotalOvertime"),
                spark_sum("NetPay").alias("TotalNetPay"),
                count("PayrollID").alias("PayPeriods"),
                avg("BaseSalary").alias("AvgMonthlyBase"),
                avg("Deductions").alias("AvgDeductions")
            )
        )
        df_emp_enriched = df_employees.join(emp_payroll, "EmployeeID", "left")
        # Bonus-to-base ratio
        df_emp_enriched = df_emp_enriched.withColumn(
            "BonusRatio",
            when(col("TotalBasePay") > 0,
                 spark_round(coalesce(col("TotalBonuses"), lit(0.0)) / col("TotalBasePay") * 100, 2)
            ).otherwise(lit(0.0))
        )
    else:
        df_emp_enriched = df_employees

    # Performance aggregates
    if df_reviews is not None:
        emp_reviews = (df_reviews
            .groupBy("EmployeeID")
            .agg(
                avg("OverallScore").alias("AvgOverallScore"),
                spark_max("OverallScore").alias("HighestScore"),
                spark_min("OverallScore").alias("LowestScore"),
                count("ReviewID").alias("ReviewCount"),
                last("PerformanceRating").alias("LatestPerformanceRating")
            )
        )
        df_emp_enriched = df_emp_enriched.join(emp_reviews, "EmployeeID", "left")

        # Performance tier (based on OverallScore 0-100 scale)
        df_emp_enriched = df_emp_enriched.withColumn(
            "PerformanceTier",
            when(col("AvgOverallScore") >= 90, lit("Star Performer"))
            .when(col("AvgOverallScore") >= 70, lit("Strong"))
            .when(col("AvgOverallScore") >= 50, lit("Developing"))
            .otherwise(lit("Needs Improvement"))
        )
    else:
        df_emp_enriched = (df_emp_enriched
            .withColumn("AvgOverallScore", lit(None).cast(DoubleType()))
            .withColumn("PerformanceTier", lit("Unknown"))
        )

    # Tenure calculation
    df_emp_enriched = (df_emp_enriched
        .withColumn("TenureYears",
            when(col("HireDate").isNotNull(),
                 spark_round(months_between(current_date(), col("HireDate")) / 12, 1)
            ).otherwise(lit(0.0))
        )
        .withColumn("TenureBand",
            when(col("TenureYears") >= 10, lit("10+ years"))
            .when(col("TenureYears") >= 5, lit("5-10 years"))
            .when(col("TenureYears") >= 2, lit("2-5 years"))
            .when(col("TenureYears") >= 1, lit("1-2 years"))
            .otherwise(lit("< 1 year"))
        )
    )

    df_emp_enriched.write.mode("overwrite").format("delta").saveAsTable(gold_table("DimEmployees", "dim"))
    print(f"  ✓ DimEmployees enriched: {df_emp_enriched.count()} rows")
    print(f"    Added: TotalBasePay, BonusRatio, AvgOverallScore, PerformanceTier, TenureYears, TenureBand")
else:
    print("  ⚠ DimEmployees not available, skipping enrichment")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 6: Create FactMonthlySalesSummary (aggregated Gold table)
# -----------------------------------------------------------

print("\n" + "="*60)
print("  Creating FactMonthlySalesSummary (Gold aggregate)")
print("="*60)

df_orders = dfs.get("FactOrders")

if df_orders is not None:
    monthly_sales = (df_orders
        .withColumn("OrderMonth", date_format(col("OrderDate"), "yyyy-MM-01"))
        .withColumn("OrderMonthDate", to_date(col("OrderMonth")))
        .groupBy("OrderMonthDate", "Channel", "BookID")
        .agg(
            count("OrderID").alias("OrderCount"),
            spark_sum("Quantity").alias("TotalQuantity"),
            spark_sum("TotalAmount").alias("TotalRevenue"),
            avg("Discount").alias("AvgDiscount"),
            countDistinct("CustomerID").alias("UniqueCustomers"),
            avg("TotalAmount").alias("AvgOrderValue"),
            spark_sum(when(col("OrderStatus") == "Delivered", 1).otherwise(0)).alias("DeliveredOrders"),
            spark_sum(when(col("OrderStatus") == "Cancelled", 1).otherwise(0)).alias("CancelledOrders"),
            avg("FulfillmentDays").alias("AvgFulfillmentDays"),
            avg("DeliveryDays").alias("AvgDeliveryDays")
        )
        .withColumn("DateKey",
            (year("OrderMonthDate") * 10000 +
             month("OrderMonthDate") * 100 + 1).cast(IntegerType())
        )
        .withColumn("FulfillmentRate",
            when(col("OrderCount") > 0,
                 spark_round(col("DeliveredOrders") / col("OrderCount") * 100, 2)
            ).otherwise(lit(0.0))
        )
        .withColumn("CancellationRate",
            when(col("OrderCount") > 0,
                 spark_round(col("CancelledOrders") / col("OrderCount") * 100, 2)
            ).otherwise(lit(0.0))
        )
    )

    # Add month-over-month growth
    window_mom = Window.partitionBy("Channel", "BookID").orderBy("OrderMonthDate")
    monthly_sales = (monthly_sales
        .withColumn("PrevMonthRevenue", lag("TotalRevenue", 1).over(window_mom))
        .withColumn("MoM_GrowthPct",
            when(col("PrevMonthRevenue") > 0,
                 spark_round((col("TotalRevenue") - col("PrevMonthRevenue")) / col("PrevMonthRevenue") * 100, 2)
            ).otherwise(lit(None))
        )
        .drop("PrevMonthRevenue")
    )

    monthly_sales.write.mode("overwrite").format("delta").saveAsTable(gold_table("FactMonthlySalesSummary", "fact"))
    print(f"  ✓ FactMonthlySalesSummary: {monthly_sales.count()} rows")
    print(f"    Grain: Month × Channel × Book")
    print(f"    Includes: MoM growth, fulfillment rate, cancellation rate")
else:
    print("  ⚠ FactOrders not available, skipping aggregation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 7: Create FactInventorySnapshots (SCD-like tracking)
# -----------------------------------------------------------

print("\n" + "="*60)
print("  Creating FactInventorySnapshots with trend analysis")
print("="*60)

df_inventory = dfs.get("FactInventory")

if df_inventory is not None:
    # Add trend analysis window functions
    inv_window = Window.partitionBy("BookID", "WarehouseID").orderBy("SnapshotDate")

    df_inv_enriched = (df_inventory
        # Previous snapshot values for trend
        .withColumn("PrevQuantityOnHand", lag("QuantityOnHand", 1).over(inv_window))
        .withColumn("PrevDaysOfSupply", lag("DaysOfSupply", 1).over(inv_window))

        # Change metrics
        .withColumn("QuantityChange",
            when(col("PrevQuantityOnHand").isNotNull(),
                 col("QuantityOnHand") - col("PrevQuantityOnHand")
            ).otherwise(lit(0))
        )
        .withColumn("SupplyTrend",
            when(col("PrevDaysOfSupply").isNotNull(),
                when(col("DaysOfSupply") > col("PrevDaysOfSupply"), lit("Improving"))
                .when(col("DaysOfSupply") < col("PrevDaysOfSupply"), lit("Declining"))
                .otherwise(lit("Stable"))
            ).otherwise(lit("Initial"))
        )

        # Risk classification
        .withColumn("StockRisk",
            when(col("DaysOfSupply") <= 15, lit("Critical"))
            .when(col("DaysOfSupply") <= 30, lit("Low"))
            .when(col("DaysOfSupply") <= 60, lit("Adequate"))
            .otherwise(lit("Surplus"))
        )

        # Turnover rate proxy
        .withColumn("TurnoverIndicator",
            when(col("QuantityChange") < -1000, lit("Fast Moving"))
            .when(col("QuantityChange") < 0, lit("Normal"))
            .when(col("QuantityChange") == 0, lit("Slow Moving"))
            .otherwise(lit("Restocked"))
        )

        .drop("PrevQuantityOnHand", "PrevDaysOfSupply")
    )

    df_inv_enriched.write.mode("overwrite").format("delta").saveAsTable(gold_table("FactInventory", "fact"))
    print(f"  ✓ FactInventory enriched: {df_inv_enriched.count()} rows")
    print(f"    Added: QuantityChange, SupplyTrend, StockRisk, TurnoverIndicator")
else:
    print("  ⚠ FactInventory not available, skipping")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 8: Enrich FactOrders with customer & book context
# -----------------------------------------------------------

print("\n" + "="*60)
print("  Enriching FactOrders with denormalized context")
print("="*60)

df_orders = dfs.get("FactOrders")
df_books = spark.table(gold_table("DimBooks", "dim"))  # Read enriched version
df_customers = spark.table(gold_table("DimCustomers", "dim"))  # Read enriched version

if df_orders is not None:
    # Add book genre and imprint for filtering
    book_context = df_books.select(
        col("BookID"),
        col("Genre").alias("BookGenre"),
        col("SubGenre").alias("BookSubGenre"),
        col("ImprintName").alias("BookImprint"),
        col("Format").alias("BookFormat")
    )

    # Add customer segment for analysis
    customer_context = df_customers.select(
        col("CustomerID"),
        col("CustomerType"),
        col("Region").alias("CustomerRegion"),
        col("Country").alias("CustomerCountry")
    )
    # Only add enrichments if those columns exist
    try:
        customer_context = customer_context.withColumn(
            "ValueSegment",
            df_customers.select("ValueSegment").columns  # Check existence
        )
    except:
        pass

    if "ValueSegment" in df_customers.columns:
        customer_context = df_customers.select(
            col("CustomerID"),
            col("CustomerType"),
            col("Region").alias("CustomerRegion"),
            col("Country").alias("CustomerCountry"),
            col("ValueSegment").alias("CustomerValueSegment")
        )

    df_orders_enriched = (df_orders
        .join(book_context, "BookID", "left")
        .join(customer_context, "CustomerID", "left")
    )

    # Running total per customer (Customer Lifetime progression)
    cust_window = Window.partitionBy("CustomerID").orderBy("OrderDate").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    df_orders_enriched = df_orders_enriched.withColumn(
        "CustomerRunningTotal", spark_sum("TotalAmount").over(cust_window)
    )

    # Order sequence number per customer
    df_orders_enriched = df_orders_enriched.withColumn(
        "CustomerOrderSeq", row_number().over(
            Window.partitionBy("CustomerID").orderBy("OrderDate")
        )
    )

    # Is repeat customer flag
    df_orders_enriched = df_orders_enriched.withColumn(
        "IsRepeatOrder", when(col("CustomerOrderSeq") > 1, lit(True)).otherwise(lit(False))
    )

    df_orders_enriched.write.mode("overwrite").format("delta").saveAsTable(gold_table("FactOrders", "fact"))
    print(f"  ✓ FactOrders enriched: {df_orders_enriched.count()} rows")
    print(f"    Added: BookGenre, BookImprint, CustomerRegion, CustomerRunningTotal, IsRepeatOrder")
else:
    print("  ⚠ FactOrders not available, skipping")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 9: Enrich FactFinancialTransactions with P&L hierarchy
# -----------------------------------------------------------

print("\n" + "="*60)
print("  Enriching FactFinancialTransactions with P&L context")
print("="*60)

df_fin = dfs.get("FactFinancialTransactions")
df_accounts = dfs.get("DimAccounts")

if df_fin is not None and df_accounts is not None:
    # Add account hierarchy for P&L roll-ups
    account_context = df_accounts.select(
        col("AccountID"),
        col("AccountName"),
        col("AccountType").alias("PLCategory"),
        col("AccountCategory").alias("PLSubCategory")
    )

    df_fin_enriched = df_fin.join(account_context, "AccountID", "left")

    # Running total by account  (YTD accumulation)
    ytd_window = Window.partitionBy("AccountID", "FiscalYear").orderBy("TransactionDate").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    df_fin_enriched = df_fin_enriched.withColumn(
        "YTD_Amount", spark_sum("Amount").over(ytd_window)
    )

    # Quarterly running total
    qtd_window = Window.partitionBy("AccountID", "FiscalYear", "FiscalQuarter").orderBy("TransactionDate").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    df_fin_enriched = df_fin_enriched.withColumn(
        "QTD_Amount", spark_sum("Amount").over(qtd_window)
    )

    # Absolute amount for easier summing (since debits are negative)
    df_fin_enriched = df_fin_enriched.withColumn(
        "AbsAmount",
        when(col("Amount") < 0, col("Amount") * -1).otherwise(col("Amount"))
    )

    df_fin_enriched.write.mode("overwrite").format("delta").saveAsTable(gold_table("FactFinancialTransactions", "fact"))
    print(f"  ✓ FactFinancialTransactions enriched: {df_fin_enriched.count()} rows")
    print(f"    Added: PLCategory, PLSubCategory, YTD_Amount, QTD_Amount, AbsAmount")
else:
    print("  ⚠ FactFinancialTransactions or DimAccounts not available, skipping")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 9b: Write pass-through tables from Silver to Gold
# -----------------------------------------------------------
# Tables that are not enriched still need to be copied to Gold
# so the Gold layer is self-contained for downstream consumers.

print("\n" + "="*60)
print("  Writing pass-through tables to Gold")
print("="*60)

# --- Pass-through Dimensions (Silver → Gold dim schema) ---
passthrough_dims = [
    "DimAccounts", "DimCostCenters", "DimAuthors",
    "DimGeography", "DimDepartments", "DimWarehouses"
]
for tbl in passthrough_dims:
    df = dfs.get(tbl)
    if df is not None:
        df.write.mode("overwrite").format("delta").saveAsTable(gold_table(tbl, "dim"))
        print(f"  ✓ {gold_table(tbl, 'dim'):40s} {df.count():>8,} rows  (pass-through)")
    else:
        print(f"  ⚠ {tbl} not available in Silver, skipping")

# --- Pass-through Facts (Silver → Gold fact schema) ---
passthrough_facts = [
    "FactBudget", "FactReturns", "FactPayroll",
    "FactPerformanceReviews", "FactRecruitment"
]
for tbl in passthrough_facts:
    df = dfs.get(tbl)
    if df is not None:
        df.write.mode("overwrite").format("delta").saveAsTable(gold_table(tbl, "fact"))
        print(f"  ✓ {gold_table(tbl, 'fact'):40s} {df.count():>8,} rows  (pass-through)")
    else:
        print(f"  ⚠ {tbl} not available in Silver, skipping")

# --- Web Enrichment tables (Silver web schema → Gold dim schema) ---
web_tables = [
    "WebExchangeRates", "WebPublicHolidays",
    "WebCountryIndicators", "WebBookMetadata"
]
for tbl in web_tables:
    try:
        df = read_silver(tbl)  # reads from SilverLH.web.<tbl>
        df.write.mode("overwrite").format("delta").saveAsTable(gold_table(tbl, "dim"))
        print(f"  ✓ {gold_table(tbl, 'dim'):40s} {df.count():>8,} rows  (web enrichment)")
    except Exception as e:
        print(f"  ⚠ {tbl} not available in Silver web schema: {e}")

print(f"\n  Pass-through complete: {len(passthrough_dims)} dims, "
      f"{len(passthrough_facts)} facts, {len(web_tables)} web tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 10: Customer Cohort Analysis (Gold aggregate)
# -----------------------------------------------------------
# Cohort = month of first purchase. Tracks retention over time.

print("\n" + "="*60)
print("  Building Customer Cohort Analysis")
print("="*60)

try:
    df_orders = spark.table(gold_table("FactOrders", "fact"))

    # Determine cohort month (first purchase month) for each customer
    cohort_base = df_orders.groupBy("CustomerID").agg(
        trunc(spark_min("OrderDate"), "month").alias("CohortMonth")
    )

    # Add cohort to each order
    orders_with_cohort = df_orders.join(broadcast(cohort_base), "CustomerID", "left")
    orders_with_cohort = orders_with_cohort.withColumn(
        "OrderMonth", trunc(col("OrderDate"), "month")
    ).withColumn(
        "CohortIndex",
        spark_round(months_between(col("OrderMonth"), col("CohortMonth"))).cast(IntegerType())
    )

    # Aggregate: for each cohort × period, count distinct customers and revenue
    cohort_analysis = (orders_with_cohort
        .groupBy("CohortMonth", "CohortIndex")
        .agg(
            countDistinct("CustomerID").alias("ActiveCustomers"),
            spark_sum("TotalAmount").alias("CohortRevenue"),
            count("OrderID").alias("CohortOrders"),
            avg("TotalAmount").alias("AvgOrderValue")
        )
    )

    # Add cohort size (period 0 customers) for retention rate calculation
    cohort_sizes = cohort_analysis.filter(col("CohortIndex") == 0).select(
        col("CohortMonth"),
        col("ActiveCustomers").alias("CohortSize")
    )
    cohort_analysis = cohort_analysis.join(broadcast(cohort_sizes), "CohortMonth", "left")

    cohort_analysis = cohort_analysis.withColumn(
        "RetentionRate",
        when(col("CohortSize") > 0,
             spark_round(col("ActiveCustomers") / col("CohortSize") * 100, 2)
        ).otherwise(lit(0.0))
    ).withColumn(
        "DateKey",
        (year("CohortMonth") * 10000 + month("CohortMonth") * 100 + 1).cast(IntegerType())
    )

    cohort_analysis.write.mode("overwrite").format("delta").saveAsTable(gold_table("GoldCohortAnalysis", "analytics"))
    print(f"  ✓ GoldCohortAnalysis: {cohort_analysis.count()} rows")
    print(f"    Grain: CohortMonth × CohortIndex")
    print(f"    Includes: RetentionRate, CohortRevenue, AvgOrderValue")
except Exception as e:
    print(f"  ⚠ Cohort analysis failed: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 11: Anomaly Detection on Revenue (Z-score method)
# -----------------------------------------------------------
# Flags daily revenue anomalies using rolling 30-day statistics

print("\n" + "="*60)
print("  Running Revenue Anomaly Detection")
print("="*60)

try:
    df_orders = spark.table(gold_table("FactOrders", "fact"))

    # Daily revenue aggregation
    daily_revenue = (df_orders
        .groupBy("OrderDate")
        .agg(
            spark_sum("TotalAmount").alias("DailyRevenue"),
            count("OrderID").alias("DailyOrderCount"),
            countDistinct("CustomerID").alias("DailyUniqueCustomers"),
            countDistinct("BookID").alias("DailyUniqueTitles")
        )
        .orderBy("OrderDate")
    )

    # Rolling 30-day mean and stddev for Z-score calculation
    rolling_window = Window.orderBy("OrderDate").rowsBetween(-29, 0)

    daily_anomalies = (daily_revenue
        .withColumn("RollingMean30d", avg("DailyRevenue").over(rolling_window))
        .withColumn("RollingStddev30d", stddev("DailyRevenue").over(rolling_window))

        # Z-Score = (value - mean) / stddev
        .withColumn("ZScore",
            when(col("RollingStddev30d") > 0,
                 spark_round((col("DailyRevenue") - col("RollingMean30d")) / col("RollingStddev30d"), 3)
            ).otherwise(lit(0.0))
        )

        # Flag anomalies (|Z| > 2 = unusual, |Z| > 3 = extreme)
        .withColumn("AnomalyFlag",
            when(spark_abs(col("ZScore")) > 3, lit("Extreme"))
            .when(spark_abs(col("ZScore")) > 2, lit("Unusual"))
            .otherwise(lit("Normal"))
        )
        .withColumn("AnomalyDirection",
            when(col("ZScore") > 2, lit("Spike"))
            .when(col("ZScore") < -2, lit("Drop"))
            .otherwise(lit("Normal"))
        )

        # Deviation from rolling mean (%)
        .withColumn("DeviationPct",
            when(col("RollingMean30d") > 0,
                 spark_round((col("DailyRevenue") - col("RollingMean30d")) / col("RollingMean30d") * 100, 2)
            ).otherwise(lit(0.0))
        )

        # DateKey for DimDate join
        .withColumn("DateKey",
            (year("OrderDate") * 10000 + month("OrderDate") * 100 + dayofmonth("OrderDate"))
                .cast(IntegerType())
        )
    )

    daily_anomalies.write.mode("overwrite").format("delta").saveAsTable(gold_table("GoldRevenueAnomalies", "analytics"))
    anomaly_count = daily_anomalies.filter(col("AnomalyFlag") != "Normal").count()
    print(f"  ✓ GoldRevenueAnomalies: {daily_anomalies.count()} rows")
    print(f"    Anomalies detected: {anomaly_count}")
    print(f"    Method: Rolling 30-day Z-Score (|Z|>2 = unusual, |Z|>3 = extreme)")
except Exception as e:
    print(f"  ⚠ Anomaly detection failed: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 12: Book Co-Purchasing Analysis (Market Basket)
# -----------------------------------------------------------
# Finds pairs of books frequently purchased by the same customer

print("\n" + "="*60)
print("  Building Book Co-Purchasing Patterns")
print("="*60)

try:
    df_orders = spark.table(gold_table("FactOrders", "fact"))
    df_books = spark.table(gold_table("DimBooks", "dim"))

    # Get list of books purchased by each customer
    customer_books = (df_orders
        .select("CustomerID", "BookID")
        .distinct()
    )

    # Self-join to find book pairs purchased by the same customer
    pairs = (customer_books.alias("a")
        .join(customer_books.alias("b"),
              (col("a.CustomerID") == col("b.CustomerID")) &
              (col("a.BookID") < col("b.BookID")))
        .select(
            col("a.BookID").alias("BookA"),
            col("b.BookID").alias("BookB"),
            col("a.CustomerID")
        )
    )

    # Count co-purchases per pair
    co_purchase = (pairs
        .groupBy("BookA", "BookB")
        .agg(
            countDistinct("CustomerID").alias("SharedCustomers")
        )
        .filter(col("SharedCustomers") >= 2)  # At least 2 shared customers
    )

    # Add book titles for readability
    book_names = df_books.select(
        col("BookID"), col("Title"), col("Genre"), col("ImprintName")
    )

    co_purchase_enriched = (co_purchase
        .join(book_names.alias("ba"),
              col("BookA") == col("ba.BookID"), "left")
        .withColumnRenamed("Title", "BookA_Title")
        .withColumnRenamed("Genre", "BookA_Genre")
        .withColumnRenamed("ImprintName", "BookA_Imprint")
        .drop(col("ba.BookID"))
        .join(book_names.alias("bb"),
              col("BookB") == col("bb.BookID"), "left")
        .withColumnRenamed("Title", "BookB_Title")
        .withColumnRenamed("Genre", "BookB_Genre")
        .withColumnRenamed("ImprintName", "BookB_Imprint")
        .drop(col("bb.BookID"))
    )

    # Calculate similarity metrics
    # Total buyers per book for support/confidence
    book_buyer_counts = customer_books.groupBy("BookID").agg(
        countDistinct("CustomerID").alias("BuyerCount")
    )
    total_customers = customer_books.select("CustomerID").distinct().count()

    co_purchase_enriched = (co_purchase_enriched
        .join(book_buyer_counts.withColumnRenamed("BookID", "BookA")
              .withColumnRenamed("BuyerCount", "BookA_Buyers"), "BookA", "left")
        .join(book_buyer_counts.withColumnRenamed("BookID", "BookB")
              .withColumnRenamed("BuyerCount", "BookB_Buyers"), "BookB", "left")
        .withColumn("Support",
            spark_round(col("SharedCustomers") / lit(total_customers) * 100, 2)
        )
        .withColumn("ConfidenceAtoB",
            when(col("BookA_Buyers") > 0,
                 spark_round(col("SharedCustomers") / col("BookA_Buyers") * 100, 2)
            ).otherwise(lit(0.0))
        )
        .withColumn("ConfidenceBtoA",
            when(col("BookB_Buyers") > 0,
                 spark_round(col("SharedCustomers") / col("BookB_Buyers") * 100, 2)
            ).otherwise(lit(0.0))
        )
        .withColumn("Lift",
            when((col("BookA_Buyers") > 0) & (col("BookB_Buyers") > 0),
                 spark_round(
                     (col("SharedCustomers") * lit(total_customers)) /
                     (col("BookA_Buyers") * col("BookB_Buyers")), 3
                 )
            ).otherwise(lit(0.0))
        )
        .withColumn("IsCrossGenre",
            when(col("BookA_Genre") != col("BookB_Genre"), lit(True)).otherwise(lit(False))
        )
        .orderBy(col("SharedCustomers").desc())
    )

    co_purchase_enriched.write.mode("overwrite").format("delta").saveAsTable(gold_table("GoldBookCoPurchase", "analytics"))
    print(f"  ✓ GoldBookCoPurchase: {co_purchase_enriched.count()} pairs")
    cross_genre = co_purchase_enriched.filter(col("IsCrossGenre")).count()
    print(f"    Cross-genre pairs: {cross_genre}")
    print(f"    Metrics: Support, Confidence (A→B, B→A), Lift")
except Exception as e:
    print(f"  ⚠ Co-purchasing analysis failed: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 13: Revenue Forecast (Exponential Moving Average)
# -----------------------------------------------------------
# Simple EMA-based projection for next 3 months per channel

print("\n" + "="*60)
print("  Building Revenue Forecast (EMA)")
print("="*60)

try:
    df_orders = spark.table(gold_table("FactOrders", "fact"))

    # Monthly revenue by channel
    monthly_channel = (df_orders
        .withColumn("OrderMonth", trunc(col("OrderDate"), "month"))
        .groupBy("OrderMonth", "Channel")
        .agg(
            spark_sum("TotalAmount").alias("MonthlyRevenue"),
            count("OrderID").alias("MonthlyOrders"),
            countDistinct("CustomerID").alias("MonthlyCustomers")
        )
        .orderBy("Channel", "OrderMonth")
    )

    # Calculate EMA components using window functions
    # EMA approximation using weighted moving averages
    ema_window = Window.partitionBy("Channel").orderBy("OrderMonth")

    forecast_base = (monthly_channel
        # Month index for weighting
        .withColumn("MonthIdx", row_number().over(ema_window))
        .withColumn("TotalMonths",
            count("*").over(Window.partitionBy("Channel"))
        )

        # Lag values for trend calculation
        .withColumn("PrevRevenue", lag("MonthlyRevenue", 1).over(ema_window))
        .withColumn("Prev2Revenue", lag("MonthlyRevenue", 2).over(ema_window))
        .withColumn("Prev3Revenue", lag("MonthlyRevenue", 3).over(ema_window))

        # 3-month Simple Moving Average
        .withColumn("SMA_3m",
            avg("MonthlyRevenue").over(
                Window.partitionBy("Channel").orderBy("OrderMonth").rowsBetween(-2, 0)
            )
        )

        # Weighted Moving Average (recent months weighted higher)
        .withColumn("WMA_3m",
            when(col("Prev2Revenue").isNotNull(),
                 spark_round(
                     (col("MonthlyRevenue") * 3 +
                      coalesce(col("PrevRevenue"), lit(0)) * 2 +
                      coalesce(col("Prev2Revenue"), lit(0)) * 1) / 6, 2
                 )
            ).otherwise(col("MonthlyRevenue"))
        )

        # Month-over-month growth rate
        .withColumn("MoM_Growth",
            when(col("PrevRevenue") > 0,
                 (col("MonthlyRevenue") - col("PrevRevenue")) / col("PrevRevenue")
            ).otherwise(lit(0.0))
        )

        # Average growth rate (last 3 months)
        .withColumn("AvgGrowth3m",
            avg("MoM_Growth").over(
                Window.partitionBy("Channel").orderBy("OrderMonth").rowsBetween(-2, 0)
            )
        )

        # Seasonality index (month's share of annual total)
        .withColumn("AnnualTotal",
            spark_sum("MonthlyRevenue").over(
                Window.partitionBy("Channel", year("OrderMonth"))
            )
        )
        .withColumn("SeasonalityIndex",
            when(col("AnnualTotal") > 0,
                 spark_round(col("MonthlyRevenue") / col("AnnualTotal") * 12, 3)
            ).otherwise(lit(1.0))
        )

        .drop("Prev2Revenue", "Prev3Revenue")
    )

    # Generate forecast rows for next 3 months (Jan-Mar 2025)
    # Use last channel data as base, apply trend
    last_month_data = forecast_base.filter(
        col("MonthIdx") == col("TotalMonths")
    ).select(
        col("Channel"),
        col("WMA_3m").alias("BaseForecast"),
        col("AvgGrowth3m"),
        col("MonthlyRevenue").alias("LastActualRevenue"),
        col("OrderMonth").alias("LastActualMonth")
    )

    # Create forecast months
    from pyspark.sql.functions import lit as spark_lit
    forecast_rows = []
    channels_data = last_month_data.collect()
    for ch in channels_data:
        base = float(ch["BaseForecast"]) if ch["BaseForecast"] else 0
        growth = float(ch["AvgGrowth3m"]) if ch["AvgGrowth3m"] else 0
        for offset in range(1, 4):
            projected = base * ((1 + growth) ** offset)
            forecast_rows.append((
                ch["Channel"],
                f"2025-{offset:02d}-01",
                round(projected, 2),
                round(base, 2),
                round(growth, 4),
                offset,
                "Forecast"
            ))

    if forecast_rows:
        schema_fc = StructType([
            StructField("Channel", StringType()),
            StructField("ForecastMonth", StringType()),
            StructField("ForecastRevenue", DoubleType()),
            StructField("BaseForecast", DoubleType()),
            StructField("GrowthRate", DoubleType()),
            StructField("ForecastHorizon", IntegerType()),
            StructField("RecordType", StringType())
        ])
        df_forecast = spark.createDataFrame(forecast_rows, schema=schema_fc)
        df_forecast = df_forecast.withColumn("ForecastMonth", to_date(col("ForecastMonth")))
        df_forecast = df_forecast.withColumn("_generated_at", current_timestamp())

        # Combine actuals and forecasts
        actuals_for_union = (forecast_base
            .select(
                col("Channel"),
                col("OrderMonth").alias("ForecastMonth"),
                col("MonthlyRevenue").alias("ForecastRevenue"),
                col("WMA_3m").alias("BaseForecast"),
                col("MoM_Growth").alias("GrowthRate"),
                lit(0).alias("ForecastHorizon"),
                lit("Actual").alias("RecordType")
            )
            .withColumn("_generated_at", current_timestamp())
        )

        df_combined = actuals_for_union.unionByName(df_forecast)
        df_combined.write.mode("overwrite").format("delta").saveAsTable(gold_table("GoldRevenueForecast", "analytics"))
        fc_count = df_forecast.count()
        print(f"  ✓ GoldRevenueForecast: {df_combined.count()} rows "
              f"({df_combined.count() - fc_count} actuals + {fc_count} forecasts)")
        print(f"    Method: Weighted Moving Average + avg 3-month growth trend")
        print(f"    Forecast horizon: Jan-Mar 2025")
    else:
        print("  ⚠ No data available for forecasting")
except Exception as e:
    print(f"  ⚠ Revenue forecast failed: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 14: Data validation and row count summary
# -----------------------------------------------------------

print("\n" + "="*60)
print("  GOLD LAYER VALIDATION")
print("="*60)

gold_tables = [
    # Dimensions (dim schema)
    gold_table("DimDate", "dim"), gold_table("DimAccounts", "dim"),
    gold_table("DimCostCenters", "dim"), gold_table("DimBooks", "dim"),
    gold_table("DimAuthors", "dim"), gold_table("DimGeography", "dim"),
    gold_table("DimCustomers", "dim"), gold_table("DimEmployees", "dim"),
    gold_table("DimDepartments", "dim"), gold_table("DimWarehouses", "dim"),
    # Facts (fact schema)
    gold_table("FactFinancialTransactions", "fact"), gold_table("FactBudget", "fact"),
    gold_table("FactOrders", "fact"), gold_table("FactInventory", "fact"),
    gold_table("FactReturns", "fact"), gold_table("FactPayroll", "fact"),
    gold_table("FactPerformanceReviews", "fact"), gold_table("FactRecruitment", "fact"),
    gold_table("FactMonthlySalesSummary", "fact"),
    # Web enrichment tables (dim schema — reference data)
    gold_table("WebExchangeRates", "dim"), gold_table("WebPublicHolidays", "dim"),
    gold_table("WebCountryIndicators", "dim"), gold_table("WebBookMetadata", "dim"),
    # Advanced analytics (analytics schema)
    gold_table("GoldCohortAnalysis", "analytics"), gold_table("GoldRevenueAnomalies", "analytics"),
    gold_table("GoldBookCoPurchase", "analytics"), gold_table("GoldRevenueForecast", "analytics")
]

total_rows = 0
table_counts = {}

for t in gold_tables:
    try:
        df = spark.table(t)
        row_count = df.count()
        col_count = len(df.columns)
        table_counts[t] = row_count
        total_rows += row_count
        print(f"  ✓ {t:40s} {row_count:>8,} rows  {col_count:>3} cols")
    except Exception as e:
        print(f"  ✗ {t:40s} ERROR: {e}")

print(f"\n  TOTAL: {total_rows:,} rows across {len(table_counts)} tables")

# Cross-table referential integrity checks
print("\n  Referential Integrity Checks:")
try:
    orders = spark.table(gold_table("FactOrders", "fact"))
    books = spark.table(gold_table("DimBooks", "dim"))
    customers = spark.table(gold_table("DimCustomers", "dim"))

    # Orders → Books
    orphan_books = orders.join(books, "BookID", "left_anti").count()
    print(f"  {'✓' if orphan_books == 0 else '⚠'} FactOrders → DimBooks: {orphan_books} orphans")

    # Orders → Customers
    orphan_custs = orders.join(customers, "CustomerID", "left_anti").count()
    print(f"  {'✓' if orphan_custs == 0 else '⚠'} FactOrders → DimCustomers: {orphan_custs} orphans")

    # Orders → DimDate
    dimdate = spark.table(gold_table("DimDate", "dim"))
    order_dates = orders.select(col("DateKey")).distinct()
    orphan_dates = order_dates.join(dimdate, "DateKey", "left_anti").count()
    print(f"  {'✓' if orphan_dates == 0 else '⚠'} FactOrders → DimDate: {orphan_dates} orphan DateKeys")
except Exception as e:
    print(f"  ⚠ Could not validate referential integrity: {e}")

print("\n=== Silver → Gold Complete ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
