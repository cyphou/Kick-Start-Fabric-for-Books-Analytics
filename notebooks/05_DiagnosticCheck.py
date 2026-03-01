# Synapse Analytics notebook source
# ============================================================================
# Horizon Books - Diagnostic: Check Gold Lakehouse Tables
# Quick check to verify all semantic-model-required tables exist in Gold.
# Run this after the full pipeline to validate before refreshing the report.
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
# Cell 1: Check all schemas and tables in Gold Lakehouse
# -----------------------------------------------------------
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("=" * 70)
print("  GOLD LAKEHOUSE DIAGNOSTIC CHECK")
print("=" * 70)

# --- 1. List all schemas ---
print("\n1. SCHEMAS IN GOLD LAKEHOUSE:")
schemas = spark.sql("SHOW SCHEMAS").collect()
for s in schemas:
    print(f"   {s[0]}")

# --- 2. Check all tables the semantic model needs ---
print("\n2. SEMANTIC MODEL REQUIRED TABLES:")

required_tables = {
    "dim": [
        "DimDate", "DimAccounts", "DimCostCenters", "DimBooks",
        "DimAuthors", "DimGeography", "DimCustomers", "DimEmployees",
        "DimDepartments", "DimWarehouses"
    ],
    "fact": [
        "FactFinancialTransactions", "FactBudget", "FactOrders",
        "FactInventory", "FactReturns", "FactPayroll",
        "FactPerformanceReviews", "FactRecruitment"
    ],
    "analytics": [
        "ForecastSalesRevenue", "ForecastGenreDemand",
        "ForecastFinancial", "ForecastInventoryDemand",
        "ForecastWorkforce"
    ],
}

missing = []
empty = []
ok = []

for schema, tables in required_tables.items():
    print(f"\n   Schema: {schema}")
    for tbl in tables:
        full_name = f"{schema}.{tbl}"
        try:
            df = spark.table(full_name)
            count = df.count()
            cols = len(df.columns)
            if count == 0:
                status = "⚠ EMPTY"
                empty.append(full_name)
            else:
                status = "✓ OK   "
                ok.append(full_name)
            print(f"     {status}  {full_name:45s} {count:>8,} rows  {cols:>2} cols")
        except Exception as e:
            status = "✗ MISSING"
            missing.append(full_name)
            err_msg = str(e).split('\n')[0][:60]
            print(f"     {status} {full_name:45s} → {err_msg}")

# --- 3. Summary ---
print("\n" + "=" * 70)
print("  SUMMARY")
print("=" * 70)
print(f"  OK     : {len(ok)}")
print(f"  Empty  : {len(empty)}")
print(f"  Missing: {len(missing)}")

if missing:
    print(f"\n  ⚠ MISSING TABLES ({len(missing)}):")
    for t in missing:
        print(f"    ✗ {t}")
    print("\n  → These will cause 'Invalid object name' errors in the report.")
    print("  → Run the full pipeline (Dataflows → NB01 → NB02 → NB03 → NB04)")
    print("    and check output for errors.")

if empty:
    print(f"\n  ⚠ EMPTY TABLES ({len(empty)}):")
    for t in empty:
        print(f"    ⚠ {t}")

# --- 4. Check Silver source tables ---
print("\n\n3. SILVER LAKEHOUSE SOURCE CHECK:")
silver_tables = {
    "finance": ["DimAccounts", "DimCostCenters", "FactFinancialTransactions", "FactBudget"],
    "hr": ["DimEmployees", "DimDepartments", "FactPayroll", "FactPerformanceReviews", "FactRecruitment"],
    "operations": ["DimBooks", "DimAuthors", "DimGeography", "DimCustomers", "DimWarehouses",
                    "FactOrders", "FactInventory", "FactReturns"],
}

silver_missing = []
for schema, tables in silver_tables.items():
    print(f"\n   Schema: SilverLH.{schema}")
    for tbl in tables:
        full_name = f"SilverLH.{schema}.{tbl}"
        try:
            df = spark.table(full_name)
            count = df.count()
            print(f"     ✓ {full_name:50s} {count:>8,} rows")
        except Exception:
            silver_missing.append(full_name)
            print(f"     ✗ {full_name:50s} MISSING")

if silver_missing:
    print(f"\n  ⚠ {len(silver_missing)} Silver table(s) missing!")
    print("  → Run the Dataflows (DF_Finance, DF_HR, DF_Operations) first,")
    print("    then NB01 (BronzeToSilver) to populate Silver.")

# --- 5. Check Bronze (Files) ---
print("\n\n4. BRONZE LAKEHOUSE FILES CHECK:")
try:
    import os
    bronze_path = "/lakehouse/default/Files/"
    if os.path.exists(bronze_path):
        files = [f for f in os.listdir(bronze_path) if f.endswith('.csv')]
        if files:
            print(f"   Found {len(files)} CSV files in Bronze:")
            for f in sorted(files):
                print(f"     {f}")
        else:
            print("   ⚠ No CSV files found in Bronze/Files/")
    else:
        print("   ⚠ Bronze Files path not accessible (notebook attached to different lakehouse)")
        print("   → This notebook should be attached to BronzeLH to check CSV files")
except Exception as e:
    print(f"   ⚠ Cannot check Bronze files: {e}")

print("\n" + "=" * 70)
print("  DIAGNOSTIC COMPLETE")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
