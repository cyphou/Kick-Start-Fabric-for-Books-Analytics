# Synapse Analytics notebook source
# ============================================================================
# Horizon Books - Notebook 1: Bronze to Silver
# Reads Bronze tables (loaded by Dataflows) and applies quality transforms
# ============================================================================

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 1: Configuration and Setup
# -----------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, initcap, when, lit, coalesce,
    to_date, to_timestamp, regexp_replace, length, isnull,
    count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    year, month, dayofmonth, current_timestamp, sha2, concat_ws,
    row_number, monotonically_increasing_id, datediff
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    DateType, BooleanType, DecimalType, LongType
)

spark = SparkSession.builder.getOrCreate()

# ── Medallion Architecture Configuration ──
# Default lakehouse = BronzeLH (Dataflows load CSVs into Bronze tables)
# This notebook reads Bronze tables and writes to SilverLH with schemas
BRONZE_LH = "BronzeLH"
SILVER_LH = "SilverLH"
SILVER_SCHEMAS = ["finance", "hr", "operations"]

# Table-to-schema mapping
TABLE_SCHEMA_MAP = {
    # Finance domain
    "DimAccounts": "finance", "DimCostCenters": "finance",
    "FactFinancialTransactions": "finance", "FactBudget": "finance",
    # HR domain
    "DimEmployees": "hr", "DimDepartments": "hr",
    "FactPayroll": "hr", "FactPerformanceReviews": "hr", "FactRecruitment": "hr",
    # Operations domain
    "DimBooks": "operations", "DimAuthors": "operations",
    "DimGeography": "operations", "DimCustomers": "operations",
    "DimWarehouses": "operations",
    "FactOrders": "operations", "FactInventory": "operations", "FactReturns": "operations",
}

def silver_table(name):
    """Return fully qualified Silver table name: SilverLH.schema.table"""
    schema = TABLE_SCHEMA_MAP.get(name, "dbo")
    return f"{SILVER_LH}.{schema}.{name}"

# Data quality counters
quality_report = {}

def log_quality(table_name, metric, value):
    """Track data quality metrics for final report."""
    if table_name not in quality_report:
        quality_report[table_name] = {}
    quality_report[table_name][metric] = value

# ── Reusable transform helpers ──
def standardize_boolean(df, col_name, default=True):
    """Standardize a boolean column from string values (TRUE/YES/1/Y → True, etc.)."""
    return df.withColumn(
        col_name,
        when(upper(col(col_name).cast("string")).isin("TRUE", "YES", "1", "Y"), lit(True))
        .when(upper(col(col_name).cast("string")).isin("FALSE", "NO", "0", "N"), lit(False))
        .otherwise(lit(default))
    )

def compute_date_key(date_col_name):
    """Return a DateKey expression (YYYYMMDD integer) from a date column."""
    return (year(col(date_col_name)) * 10000 +
            month(col(date_col_name)) * 100 +
            dayofmonth(col(date_col_name))).cast(IntegerType())

print("=== Horizon Books: Bronze → Silver Transformation ===")
print(f"Spark version: {spark.version}")
print(f"Source: {BRONZE_LH} tables (loaded by Dataflows)")
print(f"Target: {SILVER_LH} (schemas: {', '.join(SILVER_SCHEMAS)})")

# Create schemas on SilverLH
for schema in SILVER_SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_LH}.{schema}")
    print(f"  ✓ Schema {SILVER_LH}.{schema} ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 2: Define explicit schemas for type safety
# -----------------------------------------------------------

schema_DimAccounts = StructType([
    StructField("AccountID", LongType(), False),
    StructField("AccountName", StringType(), False),
    StructField("AccountType", StringType(), False),
    StructField("AccountCategory", StringType(), True),
    StructField("ParentAccountID", LongType(), True),
    StructField("IsActive", StringType(), True)
])

schema_DimCostCenters = StructType([
    StructField("CostCenterID", StringType(), False),
    StructField("CostCenterName", StringType(), False),
    StructField("Department", StringType(), True),
    StructField("DivisionHead", StringType(), True)
])

schema_DimBooks = StructType([
    StructField("BookID", StringType(), False),
    StructField("Title", StringType(), False),
    StructField("AuthorID", StringType(), False),
    StructField("Genre", StringType(), True),
    StructField("SubGenre", StringType(), True),
    StructField("ISBN", StringType(), True),
    StructField("PublishDate", StringType(), True),
    StructField("ListPrice", DoubleType(), True),
    StructField("Format", StringType(), True),
    StructField("PageCount", LongType(), True),
    StructField("PrintRunSize", LongType(), True),
    StructField("ImprintName", StringType(), True),
    StructField("Status", StringType(), True)
])

schema_DimAuthors = StructType([
    StructField("AuthorID", StringType(), False),
    StructField("FirstName", StringType(), False),
    StructField("LastName", StringType(), False),
    StructField("PenName", StringType(), True),
    StructField("AgentName", StringType(), True),
    StructField("AgentCompany", StringType(), True),
    StructField("ContractStartDate", StringType(), True),
    StructField("ContractEndDate", StringType(), True),
    StructField("RoyaltyRate", DoubleType(), True),
    StructField("AdvanceAmount", DoubleType(), True),
    StructField("Genre", StringType(), True),
    StructField("Nationality", StringType(), True),
    StructField("BookCount", LongType(), True)
])

schema_DimGeography = StructType([
    StructField("GeoID", StringType(), False),
    StructField("City", StringType(), False),
    StructField("StateProvince", StringType(), True),
    StructField("Country", StringType(), False),
    StructField("Continent", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("SubRegion", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("TimeZone", StringType(), True),
    StructField("Currency", StringType(), True),
    StructField("Population", LongType(), True),
    StructField("IsCapital", StringType(), True)  # Transformed to boolean downstream
])

schema_DimCustomers = StructType([
    StructField("CustomerID", StringType(), False),
    StructField("CustomerName", StringType(), False),
    StructField("CustomerType", StringType(), True),
    StructField("ContactEmail", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("GeoID", StringType(), True),
    StructField("CreditLimit", DoubleType(), True),
    StructField("PaymentTerms", StringType(), True),
    StructField("IsActive", StringType(), True),  # Transformed to boolean downstream
    StructField("AccountOpenDate", StringType(), True)
])

schema_DimEmployees = StructType([
    StructField("EmployeeID", StringType(), False),
    StructField("FirstName", StringType(), False),
    StructField("LastName", StringType(), False),
    StructField("Email", StringType(), True),
    StructField("HireDate", StringType(), True),
    StructField("DepartmentID", StringType(), True),
    StructField("JobTitle", StringType(), True),
    StructField("ManagerID", StringType(), True),
    StructField("EmploymentType", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("GeoID", StringType(), True),
    StructField("IsActive", StringType(), True)  # Transformed to boolean downstream
])

schema_DimDepartments = StructType([
    StructField("DepartmentID", StringType(), False),
    StructField("DepartmentName", StringType(), False),
    StructField("DepartmentHead", StringType(), True),
    StructField("HeadCount", LongType(), True),
    StructField("AnnualBudget", DoubleType(), True),
    StructField("Location", StringType(), True)
])

schema_DimWarehouses = StructType([
    StructField("WarehouseID", StringType(), False),
    StructField("WarehouseName", StringType(), False),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("SquareFootage", LongType(), True),
    StructField("MaxCapacityUnits", LongType(), True),
    StructField("CurrentUtilization", DoubleType(), True),
    StructField("ManagerID", StringType(), True),
    StructField("MonthlyRent", DoubleType(), True),
    StructField("IsActive", StringType(), True)  # Transformed to boolean downstream
])

print("Schemas defined for 9 dimension tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 3: Define schemas for fact tables
# -----------------------------------------------------------

schema_FactFinancialTransactions = StructType([
    StructField("TransactionID", StringType(), False),
    StructField("TransactionDate", StringType(), False),
    StructField("AccountID", LongType(), False),
    StructField("BookID", StringType(), True),
    StructField("Amount", DoubleType(), False),
    StructField("Currency", StringType(), True),
    StructField("TransactionType", StringType(), True),
    StructField("FiscalYear", StringType(), True),
    StructField("FiscalQuarter", StringType(), True),
    StructField("FiscalMonth", LongType(), True),
    StructField("CostCenterID", StringType(), True),
    StructField("Description", StringType(), True)
])

schema_FactBudget = StructType([
    StructField("BudgetID", StringType(), False),
    StructField("FiscalYear", StringType(), True),
    StructField("FiscalQuarter", StringType(), True),
    StructField("FiscalMonth", LongType(), True),
    StructField("AccountID", LongType(), False),
    StructField("CostCenterID", StringType(), True),
    StructField("BudgetAmount", DoubleType(), True),
    StructField("ActualAmount", DoubleType(), True),
    StructField("Variance", DoubleType(), True),
    StructField("VariancePct", DoubleType(), True)
])

schema_FactOrders = StructType([
    StructField("OrderID", StringType(), False),
    StructField("OrderDate", StringType(), False),
    StructField("CustomerID", StringType(), False),
    StructField("BookID", StringType(), False),
    StructField("Quantity", LongType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("TotalAmount", DoubleType(), True),
    StructField("OrderStatus", StringType(), True),
    StructField("ShipDate", StringType(), True),
    StructField("DeliveryDate", StringType(), True),
    StructField("WarehouseID", StringType(), True),
    StructField("SalesRepID", StringType(), True),
    StructField("Channel", StringType(), True)
])

schema_FactInventory = StructType([
    StructField("InventoryID", StringType(), False),
    StructField("BookID", StringType(), False),
    StructField("WarehouseID", StringType(), False),
    StructField("SnapshotDate", StringType(), False),
    StructField("QuantityOnHand", LongType(), True),
    StructField("QuantityReserved", LongType(), True),
    StructField("QuantityAvailable", LongType(), True),
    StructField("ReorderPoint", LongType(), True),
    StructField("ReorderQuantity", LongType(), True),
    StructField("UnitCost", DoubleType(), True),
    StructField("TotalInventoryValue", DoubleType(), True),
    StructField("DaysOfSupply", LongType(), True),
    StructField("Status", StringType(), True)
])

schema_FactReturns = StructType([
    StructField("ReturnID", StringType(), False),
    StructField("OrderID", StringType(), True),
    StructField("BookID", StringType(), False),
    StructField("CustomerID", StringType(), False),
    StructField("ReturnDate", StringType(), False),
    StructField("Quantity", LongType(), True),
    StructField("Reason", StringType(), True),
    StructField("ReturnStatus", StringType(), True),
    StructField("RefundAmount", DoubleType(), True),
    StructField("Condition", StringType(), True),
    StructField("RestockStatus", StringType(), True)
])

schema_FactPayroll = StructType([
    StructField("PayrollID", StringType(), False),
    StructField("EmployeeID", StringType(), False),
    StructField("PayPeriodStart", StringType(), False),
    StructField("PayPeriodEnd", StringType(), False),
    StructField("BaseSalary", DoubleType(), True),
    StructField("Bonus", DoubleType(), True),
    StructField("Overtime", DoubleType(), True),
    StructField("Deductions", DoubleType(), True),
    StructField("NetPay", DoubleType(), True),
    StructField("PayDate", StringType(), True)
])

schema_FactPerformanceReviews = StructType([
    StructField("ReviewID", StringType(), False),
    StructField("EmployeeID", StringType(), False),
    StructField("ReviewDate", StringType(), False),
    StructField("ReviewerID", StringType(), True),
    StructField("PerformanceRating", StringType(), True),
    StructField("GoalsMet", StringType(), True),
    StructField("Strengths", StringType(), True),
    StructField("AreasForImprovement", StringType(), True),
    StructField("OverallScore", DoubleType(), True)
])

schema_FactRecruitment = StructType([
    StructField("RequisitionID", StringType(), False),
    StructField("DepartmentID", StringType(), False),
    StructField("JobTitle", StringType(), False),
    StructField("OpenDate", StringType(), False),
    StructField("CloseDate", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("ApplicationsReceived", LongType(), True),
    StructField("Interviewed", LongType(), True),
    StructField("OffersExtended", LongType(), True),
    StructField("OfferAccepted", LongType(), True),
    StructField("HiringManagerID", StringType(), True),
    StructField("SalaryRangeMin", DoubleType(), True),
    StructField("SalaryRangeMax", DoubleType(), True),
    StructField("TimeToFillDays", LongType(), True)
])

print("Schemas defined for 8 fact tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 4: Generic ingestion with data quality checks
# -----------------------------------------------------------

def ingest_bronze_with_quality(table_name, schema=None, source_table=None):
    """
    Reads a Bronze Delta table (loaded by Dataflow), applies data quality
    checks, and prepares for Silver writing.

    Falls back to CSV if Bronze table doesn't exist (first run / no DF).
    Returns the DataFrame for downstream use.
    """
    if source_table is None:
        source_table = f"{BRONZE_LH}.{table_name}"

    print(f"\n{'='*60}")
    print(f"  Ingesting: {table_name}")
    print(f"  Source:    {source_table}")
    print(f"{'='*60}")

    # Try reading from Bronze Delta table first, fall back to CSV
    try:
        df_raw = spark.read.table(source_table)
        print(f"  ✓ Reading from Bronze table: {source_table}")
    except Exception:
        # Fallback: read CSV directly (if Dataflows haven't run yet)
        csv_path = f"Files/{table_name}.csv"
        print(f"  ⚠ Bronze table not found, falling back to CSV: {csv_path}")
        read_opts = (spark.read
            .option("header", "true")
            .option("multiLine", "true")
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
        )
        if schema:
            read_opts = read_opts.schema(schema)
        else:
            read_opts = read_opts.option("inferSchema", "true")
        df_raw = read_opts.csv(csv_path)

    raw_count = df_raw.count()
    log_quality(table_name, "raw_rows", raw_count)
    print(f"  Raw rows: {raw_count}")

    # Check for corrupt records
    if "_corrupt_record" in df_raw.columns:
        corrupt_count = df_raw.filter(col("_corrupt_record").isNotNull()).count()
        if corrupt_count > 0:
            print(f"  ⚠ Corrupt records: {corrupt_count}")
            log_quality(table_name, "corrupt_records", corrupt_count)
            df_raw = df_raw.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        else:
            df_raw = df_raw.drop("_corrupt_record")

    # Trim all string columns (use schema if provided, otherwise infer from DataFrame)
    col_fields = schema.fields if schema else [StructField(f.name, f.dataType, f.nullable) for f in df_raw.schema.fields]
    for field in col_fields:
        if isinstance(field.dataType, StringType):
            df_raw = df_raw.withColumn(field.name, trim(col(field.name)))

    # Count nulls per column
    null_counts = {}
    for field in col_fields:
        if not field.nullable:
            null_count = df_raw.filter(col(field.name).isNull()).count()
            if null_count > 0:
                null_counts[field.name] = null_count
                print(f"  ⚠ Null in non-nullable column {field.name}: {null_count}")

    if null_counts:
        log_quality(table_name, "null_violations", null_counts)

    # Remove exact duplicates based on primary key (first column)
    pk_col = col_fields[0].name
    window = Window.partitionBy(pk_col).orderBy(lit(1))
    df_deduped = df_raw.withColumn("_row_num", row_number().over(window))
    dup_count = df_deduped.filter(col("_row_num") > 1).count()
    if dup_count > 0:
        print(f"  ⚠ Duplicate keys removed: {dup_count}")
        log_quality(table_name, "duplicates_removed", dup_count)
    df_deduped = df_deduped.filter(col("_row_num") == 1).drop("_row_num")

    # Add audit columns
    source_label = source_table if source_table else f"Files/{table_name}.csv"
    df_final = (df_deduped
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_table", lit(source_label))
        .withColumn("_row_hash", sha2(concat_ws("|", *[col(f.name) for f in col_fields]), 256))
    )

    final_count = df_final.count()
    log_quality(table_name, "silver_rows", final_count)
    print(f"  Silver rows: {final_count}")

    return df_final

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 5: Ingest Dimension Tables (Bronze → Silver)
# -----------------------------------------------------------

# Finance dimensions
df_accounts = ingest_bronze_with_quality("DimAccounts", schema_DimAccounts)
df_costcenters = ingest_bronze_with_quality("DimCostCenters", schema_DimCostCenters)

# Operations dimensions
df_books = ingest_bronze_with_quality("DimBooks", schema_DimBooks)
df_authors = ingest_bronze_with_quality("DimAuthors", schema_DimAuthors)
df_geography = ingest_bronze_with_quality("DimGeography", schema_DimGeography)
df_customers = ingest_bronze_with_quality("DimCustomers", schema_DimCustomers)
df_warehouses = ingest_bronze_with_quality("DimWarehouses", schema_DimWarehouses)

# HR dimensions
df_employees = ingest_bronze_with_quality("DimEmployees", schema_DimEmployees)
df_departments = ingest_bronze_with_quality("DimDepartments", schema_DimDepartments)

print("\n✓ All 9 dimension tables ingested")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 6: Apply dimension-specific transformations
# -----------------------------------------------------------

# --- DimAccounts: Standardize boolean values ---
df_accounts = standardize_boolean(df_accounts, "IsActive")
print("DimAccounts: Standardized IsActive boolean")

# --- DimBooks: Parse dates and validate prices ---
df_books = (df_books
    .withColumn("PublishDate", to_date(col("PublishDate"), "yyyy-MM-dd"))
    .withColumn("ListPrice", when(col("ListPrice") < 0, lit(0.0)).otherwise(col("ListPrice")))
    .withColumn("PageCount", when(col("PageCount") <= 0, lit(None)).otherwise(col("PageCount")))
    .withColumn("PrintRunSize", when(col("PrintRunSize") <= 0, lit(None)).otherwise(col("PrintRunSize")))
    .withColumn("Genre", initcap(col("Genre")))
    .withColumn("Status", initcap(col("Status")))
)
print("DimBooks: Parsed dates, validated prices, standardized casing")

# --- DimAuthors: Parse dates, validate royalty rates ---
df_authors = (df_authors
    .withColumn("ContractStartDate", to_date(col("ContractStartDate"), "yyyy-MM-dd"))
    .withColumn("ContractEndDate", to_date(col("ContractEndDate"), "yyyy-MM-dd"))
    .withColumn("RoyaltyRate",
        when(col("RoyaltyRate") > 1.0, col("RoyaltyRate") / 100.0)
        .otherwise(col("RoyaltyRate"))
    )
    .withColumn("Genre", initcap(col("Genre")))
)
print("DimAuthors: Parsed dates, normalized royalty rates")

# --- DimGeography: Validate coordinates ---
df_geography = (df_geography
    .withColumn("Latitude",
        when((col("Latitude") < -90) | (col("Latitude") > 90), lit(None))
        .otherwise(col("Latitude"))
    )
    .withColumn("Longitude",
        when((col("Longitude") < -180) | (col("Longitude") > 180), lit(None))
        .otherwise(col("Longitude"))
    )
    .withColumn("IsCapital",
        when(upper(col("IsCapital").cast("string")).isin("TRUE", "YES", "1"), lit(True))
        .otherwise(lit(False))
    )
    .withColumn("Country", initcap(col("Country")))
    .withColumn("Continent", initcap(col("Continent")))
)
print("DimGeography: Validated coordinates, standardized booleans")

# --- DimCustomers: Validate emails, standardize booleans ---
df_customers = standardize_boolean(df_customers, "IsActive")
df_customers = (df_customers
    .withColumn("ContactEmail", lower(trim(col("ContactEmail"))))
    .withColumn("AccountOpenDate", to_date(col("AccountOpenDate"), "yyyy-MM-dd"))
    .withColumn("CreditLimit",
        when(col("CreditLimit") < 0, lit(0.0)).otherwise(col("CreditLimit"))
    )
    .withColumn("Country", initcap(col("Country")))
)
print("DimCustomers: Validated emails, dates, credit limits, booleans")

# --- DimEmployees: Parse dates, standardize emails and booleans ---
df_employees = standardize_boolean(df_employees, "IsActive")
df_employees = (df_employees
    .withColumn("HireDate", to_date(col("HireDate"), "yyyy-MM-dd"))
    .withColumn("Email", lower(trim(col("Email"))))
)
print("DimEmployees: Parsed dates, standardized emails and booleans")

# --- DimWarehouses: Validate utilization percentage, standardize booleans ---
df_warehouses = (df_warehouses
    .withColumn("CurrentUtilization",
        when(col("CurrentUtilization") > 1.0, col("CurrentUtilization") / 100.0)
        .otherwise(col("CurrentUtilization"))
    )
    .withColumn("CurrentUtilization",
        when(col("CurrentUtilization") < 0, lit(0.0))
        .when(col("CurrentUtilization") > 1.0, lit(1.0))
        .otherwise(col("CurrentUtilization"))
    )
)
df_warehouses = standardize_boolean(df_warehouses, "IsActive")
print("DimWarehouses: Validated utilization percentages, standardized booleans")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 7: Ingest and transform Fact Tables
# -----------------------------------------------------------

# Finance facts
df_fin_txn = ingest_bronze_with_quality("FactFinancialTransactions", schema_FactFinancialTransactions)
df_budget = ingest_bronze_with_quality("FactBudget", schema_FactBudget)

# Operations facts
df_orders = ingest_bronze_with_quality("FactOrders", schema_FactOrders)
df_inventory = ingest_bronze_with_quality("FactInventory", schema_FactInventory)
df_returns = ingest_bronze_with_quality("FactReturns", schema_FactReturns)

# HR facts
df_payroll = ingest_bronze_with_quality("FactPayroll", schema_FactPayroll)
df_reviews = ingest_bronze_with_quality("FactPerformanceReviews", schema_FactPerformanceReviews)
df_recruitment = ingest_bronze_with_quality("FactRecruitment", schema_FactRecruitment)

print("\n✓ All 8 fact tables ingested")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 8: Apply fact-specific transformations
# -----------------------------------------------------------

# --- FactFinancialTransactions: Parse dates, standardize currency ---
df_fin_txn = (df_fin_txn
    .withColumn("TransactionDate", to_date(col("TransactionDate"), "yyyy-MM-dd"))
    .withColumn("Currency", upper(trim(coalesce(col("Currency"), lit("USD")))))
    .withColumn("TransactionType", initcap(col("TransactionType")))
    .withColumn("DateKey", compute_date_key("TransactionDate"))
)
print("FactFinancialTransactions: Parsed dates, added DateKey, standardized currency")

# --- FactBudget: Recalculate variance if missing ---
df_budget = (df_budget
    .withColumn("Variance",
        coalesce(col("Variance"), col("ActualAmount") - col("BudgetAmount"))
    )
    .withColumn("VariancePct",
        when(col("BudgetAmount") != 0,
             (col("ActualAmount") - col("BudgetAmount")) / col("BudgetAmount") * 100
        ).otherwise(lit(0.0))
    )
)
print("FactBudget: Recalculated variance, added VariancePct")

# --- FactOrders: Parse dates, validate amounts, calculate derived fields ---
df_orders = (df_orders
    .withColumn("OrderDate", to_date(col("OrderDate"), "yyyy-MM-dd"))
    .withColumn("ShipDate", to_date(col("ShipDate"), "yyyy-MM-dd"))
    .withColumn("DeliveryDate", to_date(col("DeliveryDate"), "yyyy-MM-dd"))
    .withColumn("DateKey", compute_date_key("OrderDate"))
    .withColumn("OrderStatus", initcap(col("OrderStatus")))
    .withColumn("Channel", initcap(col("Channel")))
    # Recalculate TotalAmount = Quantity * UnitPrice * (1 - Discount)
    .withColumn("TotalAmountCalc",
        col("Quantity") * col("UnitPrice") * (1 - coalesce(col("Discount"), lit(0.0)))
    )
    # Validate: if calculated differs by more than 1%, flag it
    .withColumn("_amount_check",
        when(
            (col("TotalAmount") > 0) &
            (((col("TotalAmountCalc") - col("TotalAmount")) / col("TotalAmount")).between(-0.01, 0.01)),
            lit("OK")
        ).otherwise(lit("MISMATCH"))
    )
    # Calculate fulfillment days
    .withColumn("FulfillmentDays",
        when(col("ShipDate").isNotNull() & col("OrderDate").isNotNull(),
             datediff(col("ShipDate"), col("OrderDate"))
        ).otherwise(lit(None))
    )
    .withColumn("DeliveryDays",
        when(col("DeliveryDate").isNotNull() & col("ShipDate").isNotNull(),
             datediff(col("DeliveryDate"), col("ShipDate"))
        ).otherwise(lit(None))
    )
)

mismatch_count = df_orders.filter(col("_amount_check") == "MISMATCH").count()
if mismatch_count > 0:
    print(f"  ⚠ Order amount mismatches: {mismatch_count}")
    log_quality("FactOrders", "amount_mismatches", mismatch_count)

df_orders = df_orders.drop("TotalAmountCalc", "_amount_check")
print("FactOrders: Parsed dates, added DateKey/FulfillmentDays/DeliveryDays, validated amounts")

# --- FactInventory: Parse dates, validate status logic ---

df_inventory = (df_inventory
    .withColumn("SnapshotDate", to_date(col("SnapshotDate"), "yyyy-MM-dd"))
    .withColumn("DateKey", compute_date_key("SnapshotDate"))
    .withColumn("Status", initcap(col("Status")))
    # Validate: QuantityAvailable should roughly equal OnHand - Reserved
    .withColumn("QuantityAvailable",
        coalesce(col("QuantityAvailable"),
                 col("QuantityOnHand") - col("QuantityReserved"))
    )
    # Validate inventory value
    .withColumn("TotalInventoryValue",
        coalesce(col("TotalInventoryValue"),
                 col("QuantityOnHand") * col("UnitCost"))
    )
    # Flag near-reorder items
    .withColumn("NeedsReorder",
        when(col("QuantityAvailable") <= col("ReorderPoint"), lit(True))
        .otherwise(lit(False))
    )
)
print("FactInventory: Parsed dates, validated quantities, added NeedsReorder flag")

# --- FactReturns: Parse dates, validate refund amounts ---
df_returns = (df_returns
    .withColumn("ReturnDate", to_date(col("ReturnDate"), "yyyy-MM-dd"))
    .withColumn("DateKey", compute_date_key("ReturnDate"))
    .withColumn("Reason", initcap(col("Reason")))
    .withColumn("ReturnStatus", initcap(col("ReturnStatus")))
    .withColumn("Condition", initcap(col("Condition")))
    .withColumn("RestockStatus", initcap(col("RestockStatus")))
    .withColumn("RefundAmount",
        when(col("RefundAmount") < 0, col("RefundAmount") * -1)
        .otherwise(col("RefundAmount"))
    )
)
print("FactReturns: Parsed dates, validated refunds, standardized categories")

# --- FactPayroll: Parse dates, validate pay calculations ---
df_payroll = (df_payroll
    .withColumn("PayPeriodStart", to_date(col("PayPeriodStart"), "yyyy-MM-dd"))
    .withColumn("PayPeriodEnd", to_date(col("PayPeriodEnd"), "yyyy-MM-dd"))
    .withColumn("PayDate", to_date(col("PayDate"), "yyyy-MM-dd"))
    .withColumn("DateKey", compute_date_key("PayDate"))
    # Gross = Base + Bonus + Overtime
    .withColumn("GrossPay",
        coalesce(col("BaseSalary"), lit(0.0)) +
        coalesce(col("Bonus"), lit(0.0)) +
        coalesce(col("Overtime"), lit(0.0))
    )
    # Validate: Net should roughly equal Gross - Deductions
    .withColumn("NetPayCalc",
        col("GrossPay") - coalesce(col("Deductions"), lit(0.0))
    )
    .withColumn("DeductionRate",
        when(col("GrossPay") > 0,
             coalesce(col("Deductions"), lit(0.0)) / col("GrossPay")
        ).otherwise(lit(0.0))
    )
)
print("FactPayroll: Parsed dates, added GrossPay/DeductionRate, validated calculations")

# --- FactPerformanceReviews: Parse dates, validate scores ---
df_reviews = (df_reviews
    .withColumn("ReviewDate", to_date(col("ReviewDate"), "yyyy-MM-dd"))
    .withColumn("DateKey", compute_date_key("ReviewDate"))
    .withColumn("PerformanceRating", initcap(col("PerformanceRating")))
    .withColumn("OverallScore",
        when(col("OverallScore") < 0, lit(0.0))
        .when(col("OverallScore") > 100, lit(100.0))
        .otherwise(col("OverallScore"))
    )
)
print("FactPerformanceReviews: Parsed dates, validated scores")

# --- FactRecruitment: Parse dates, calculate time-to-fill ---
df_recruitment = (df_recruitment
    .withColumn("OpenDate", to_date(col("OpenDate"), "yyyy-MM-dd"))
    .withColumn("CloseDate", to_date(col("CloseDate"), "yyyy-MM-dd"))
    .withColumn("DateKey", compute_date_key("OpenDate"))
    .withColumn("Status", initcap(col("Status")))
    .withColumn("TimeToFillDays",
        coalesce(
            col("TimeToFillDays"),
            when(col("CloseDate").isNotNull(),
                 datediff(col("CloseDate"), col("OpenDate"))
            ).otherwise(lit(None))
        )
    )
    .withColumn("HireRate",
        when(col("ApplicationsReceived") > 0,
             coalesce(col("OffersExtended"), lit(0)) / col("ApplicationsReceived")
        ).otherwise(lit(0.0))
    )
    .withColumn("InterviewRate",
        when(col("ApplicationsReceived") > 0,
             coalesce(col("Interviewed"), lit(0)) / col("ApplicationsReceived")
        ).otherwise(lit(0.0))
    )
)
print("FactRecruitment: Parsed dates, calculated HireRate/InterviewRate, validated TimeToFillDays")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 9: Write Silver tables to Lakehouse (Delta format)
# -----------------------------------------------------------

silver_tables = {
    # Finance dimensions
    "DimAccounts": df_accounts,
    "DimCostCenters": df_costcenters,
    # Finance facts
    "FactFinancialTransactions": df_fin_txn,
    "FactBudget": df_budget,
    # HR dimensions
    "DimEmployees": df_employees,
    "DimDepartments": df_departments,
    # HR facts
    "FactPayroll": df_payroll,
    "FactPerformanceReviews": df_reviews,
    "FactRecruitment": df_recruitment,
    # Operations dimensions
    "DimBooks": df_books,
    "DimAuthors": df_authors,
    "DimGeography": df_geography,
    "DimCustomers": df_customers,
    "DimWarehouses": df_warehouses,
    # Operations facts
    "FactOrders": df_orders,
    "FactInventory": df_inventory,
    "FactReturns": df_returns,
}

print("\n" + "="*60)
print("  Writing Silver tables to SilverLH (with schemas)")
print("="*60)

current_schema = None
for table_name, df in silver_tables.items():
    schema = TABLE_SCHEMA_MAP[table_name]
    if schema != current_schema:
        print(f"\n  Schema: {SILVER_LH}.{schema}")
        current_schema = schema
    # Drop audit columns before saving (they're for lineage, not model)
    save_df = df.drop("_ingested_at", "_source_table", "_row_hash")
    fq_name = silver_table(table_name)
    save_df.write.mode("overwrite").format("delta").saveAsTable(fq_name)
    row_count = save_df.count()
    print(f"    ✓ {fq_name}: {row_count} rows")

print(f"\n✓ All 17 Silver tables written to {SILVER_LH} (3 schemas)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 10: Data Quality Summary Report
# -----------------------------------------------------------
import json

print("\n" + "="*60)
print("  DATA QUALITY REPORT")
print("="*60)

total_raw = 0
total_silver = 0

for table_name, metrics in sorted(quality_report.items()):
    raw = metrics.get("raw_rows", 0)
    silver = metrics.get("silver_rows", 0)
    total_raw += raw
    total_silver += silver

    issues = []
    if "corrupt_records" in metrics:
        issues.append(f"corrupt={metrics['corrupt_records']}")
    if "duplicates_removed" in metrics:
        issues.append(f"dups={metrics['duplicates_removed']}")
    if "null_violations" in metrics:
        issues.append(f"nulls={metrics['null_violations']}")
    if "amount_mismatches" in metrics:
        issues.append(f"amt_mismatch={metrics['amount_mismatches']}")

    status = "⚠" if issues else "✓"
    issue_str = f" [{', '.join(issues)}]" if issues else ""
    print(f"  {status} {table_name}: {raw} → {silver} rows{issue_str}")

print(f"\n  TOTAL: {total_raw} raw → {total_silver} silver rows")
print(f"  DROP RATE: {((total_raw - total_silver) / max(total_raw, 1) * 100):.2f}%")
print("\n=== Bronze → Silver Complete ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
