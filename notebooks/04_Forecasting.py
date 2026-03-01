# Synapse Analytics notebook source
# ============================================================================
# Horizon Books - Notebook 4: Forecasting Models
# Builds time-series forecasts on Gold Lakehouse data using
# Holt-Winters (statsmodels) for revenue, genre demand,
# financial P&L, and inventory/workforce planning.
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
# Cell 1: Setup — imports, helpers, configuration
# -----------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, concat, year, month, quarter,
    dayofmonth, date_format, to_date, trunc, current_timestamp,
    sum as spark_sum, count, countDistinct, avg, round as spark_round,
    min as spark_min, max as spark_max, row_number, lag,
    monotonically_increasing_id, explode
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    DateType, TimestampType
)
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import warnings
warnings.filterwarnings("ignore")

spark = SparkSession.builder.getOrCreate()

# ── Configuration ──
FORECAST_HORIZON = 6        # months ahead
CONFIDENCE_LEVEL = 0.95     # 95 % confidence interval
MIN_HISTORY_MONTHS = 12     # minimum data points for Holt-Winters
GOLD_SCHEMAS = ["dim", "fact", "analytics"]
FORECAST_GENERATED_AT = datetime.utcnow()

# ── Execution tracking ──
_cell_results = {}          # track success/failure per forecast model
_notebook_start = time.time()

def gold_table(name, schema="analytics"):
    """Schema-qualified Gold table name."""
    return f"{schema}.{name}"

def _write_forecast_table(pdf_result, schema, table_name):
    """Idempotent write: overwrite Delta table with schema validation."""
    df_result = spark.createDataFrame(pdf_result, schema=schema)
    full_name = gold_table(table_name, "analytics")
    df_result.write.mode("overwrite").format("delta").option(
        "overwriteSchema", "true"
    ).saveAsTable(full_name)
    return df_result.count()

# Ensure analytics schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS analytics")

# ── Forecasting helpers ──
def holt_winters_forecast(ts_series, horizon, seasonal_periods=12):
    """
    Apply Holt-Winters Exponential Smoothing to a pandas Series.
    Falls back to simple linear trend if insufficient data.
    Returns (forecast_values, lower_bound, upper_bound, model_name).
    """
    from statsmodels.tsa.holtwinters import ExponentialSmoothing

    values = ts_series.dropna().values.astype(float)
    n = len(values)

    if n < seasonal_periods * 2:
        # Not enough data for full seasonal model — use additive trend only
        try:
            model = ExponentialSmoothing(
                values, trend="add", seasonal=None
            ).fit(optimized=True)
            fcast = model.forecast(horizon)
            residuals = values - model.fittedvalues
            sigma = np.std(residuals)
            z = 1.96  # 95% CI
            lower = fcast - z * sigma * np.sqrt(np.arange(1, horizon + 1))
            upper = fcast + z * sigma * np.sqrt(np.arange(1, horizon + 1))
            return fcast, lower, upper, "HoltLinearTrend"
        except Exception:
            pass

    if n >= seasonal_periods * 2:
        # Full Holt-Winters with additive seasonality
        for seasonal in ["add", "mul"]:
            for trend in ["add", "mul"]:
                try:
                    model = ExponentialSmoothing(
                        values,
                        trend=trend,
                        seasonal=seasonal,
                        seasonal_periods=seasonal_periods,
                        initialization_method="estimated"
                    ).fit(optimized=True)
                    fcast = model.forecast(horizon)
                    residuals = values - model.fittedvalues
                    sigma = np.std(residuals)
                    z = 1.96
                    lower = fcast - z * sigma * np.sqrt(np.arange(1, horizon + 1))
                    upper = fcast + z * sigma * np.sqrt(np.arange(1, horizon + 1))
                    model_label = f"HoltWinters_{trend}_{seasonal}"
                    return fcast, lower, upper, model_label
                except Exception:
                    continue

    # Ultimate fallback: weighted moving average + linear trend
    if n >= 3:
        weights = np.arange(1, n + 1, dtype=float)
        weights /= weights.sum()
        wma = np.average(values, weights=weights)
        # Simple linear regression for trend
        x = np.arange(n)
        slope = np.polyfit(x, values, 1)[0]
        fcast = np.array([wma + slope * (i + 1) for i in range(horizon)])
        sigma = np.std(values[-min(n, 6):])
        z = 1.96
        lower = fcast - z * sigma
        upper = fcast + z * sigma
        return fcast, lower, upper, "WMA_LinearTrend"

    # Not enough data
    last_val = values[-1] if n > 0 else 0.0
    fcast = np.full(horizon, last_val)
    return fcast, fcast * 0.8, fcast * 1.2, "Naive"


def compute_mape(actual, predicted):
    """Mean Absolute Percentage Error (safe for zeros)."""
    mask = actual != 0
    if mask.sum() == 0:
        return None
    return float(np.mean(np.abs((actual[mask] - predicted[mask]) / actual[mask])) * 100)


print("=" * 60)
print("  Horizon Books — Forecasting Models")
print("=" * 60)
print(f"  Forecast horizon : {FORECAST_HORIZON} months")
print(f"  Confidence level : {CONFIDENCE_LEVEL * 100:.0f}%")
print(f"  Min history      : {MIN_HISTORY_MONTHS} months")
print(f"  Generated at     : {FORECAST_GENERATED_AT:%Y-%m-%d %H:%M:%S} UTC")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 2: Sales Revenue Forecast by Channel
# -----------------------------------------------------------
# Holt-Winters per Channel → analytics.ForecastSalesRevenue
# Replaces the simple EMA from NB03 with proper seasonal model.

print("\n" + "=" * 60)
print("  📈 Sales Revenue Forecast by Channel")
print("=" * 60)

_cell2_start = time.time()
try:
    df_orders = spark.table(gold_table("FactOrders", "fact"))

    # Monthly revenue by channel
    monthly_channel = (df_orders
        .withColumn("OrderMonth", trunc(col("OrderDate"), "month"))
        .groupBy("OrderMonth", "Channel")
        .agg(
            spark_sum("TotalAmount").alias("Revenue"),
            count("OrderID").alias("Orders"),
            countDistinct("CustomerID").alias("Customers")
        )
        .orderBy("Channel", "OrderMonth")
    )

    pdf_monthly = monthly_channel.toPandas()
    pdf_monthly["OrderMonth"] = pd.to_datetime(pdf_monthly["OrderMonth"])

    channels = pdf_monthly["Channel"].unique()
    last_date = pdf_monthly["OrderMonth"].max()
    all_results = []
    model_info = []

    for ch in channels:
        ch_data = pdf_monthly[pdf_monthly["Channel"] == ch].sort_values("OrderMonth")
        ts = ch_data.set_index("OrderMonth")["Revenue"]

        if len(ts) < 3:
            print(f"  ⚠ {ch}: only {len(ts)} months — skipping")
            continue

        fcast, lower, upper, model_name = holt_winters_forecast(ts, FORECAST_HORIZON)

        # Back-test MAPE on last 3 months (in-sample)
        if len(ts) >= 6:
            train = ts.iloc[:-3]
            test  = ts.iloc[-3:]
            try:
                bt_fcast, _, _, _ = holt_winters_forecast(train, 3)
                mape = compute_mape(test.values, bt_fcast)
            except Exception:
                mape = None
        else:
            mape = None

        model_info.append({"Channel": ch, "Model": model_name, "Months": len(ts),
                           "MAPE_3m": round(mape, 2) if mape else None})

        # Actuals
        for _, row in ch_data.iterrows():
            all_results.append({
                "ForecastMonth": row["OrderMonth"],
                "Channel": ch,
                "Revenue": float(row["Revenue"]),
                "Orders": int(row["Orders"]),
                "Customers": int(row["Customers"]),
                "LowerBound": float(row["Revenue"]),
                "UpperBound": float(row["Revenue"]),
                "ForecastHorizon": 0,
                "RecordType": "Actual",
                "ForecastModel": model_name
            })

        # Forecasts
        for i in range(FORECAST_HORIZON):
            fc_date = last_date + pd.DateOffset(months=i + 1)
            all_results.append({
                "ForecastMonth": fc_date,
                "Channel": ch,
                "Revenue": float(max(fcast[i], 0)),
                "Orders": 0,
                "Customers": 0,
                "LowerBound": float(max(lower[i], 0)),
                "UpperBound": float(max(upper[i], 0)),
                "ForecastHorizon": i + 1,
                "RecordType": "Forecast",
                "ForecastModel": model_name
            })

    if all_results:
        pdf_result = pd.DataFrame(all_results)
        pdf_result["ForecastMonth"] = pd.to_datetime(pdf_result["ForecastMonth"]).dt.date
        pdf_result["_generated_at"] = FORECAST_GENERATED_AT

        schema = StructType([
            StructField("ForecastMonth", DateType()),
            StructField("Channel", StringType()),
            StructField("Revenue", DoubleType()),
            StructField("Orders", IntegerType()),
            StructField("Customers", IntegerType()),
            StructField("LowerBound", DoubleType()),
            StructField("UpperBound", DoubleType()),
            StructField("ForecastHorizon", IntegerType()),
            StructField("RecordType", StringType()),
            StructField("ForecastModel", StringType()),
            StructField("_generated_at", TimestampType())
        ])
        df_result = spark.createDataFrame(pdf_result, schema=schema)
        df_result.write.mode("overwrite").format("delta").option(
            "overwriteSchema", "true"
        ).saveAsTable(
            gold_table("ForecastSalesRevenue", "analytics")
        )

        n_actual = len([r for r in all_results if r["RecordType"] == "Actual"])
        n_fcast  = len([r for r in all_results if r["RecordType"] == "Forecast"])
        elapsed = time.time() - _cell2_start
        print(f"  ✓ ForecastSalesRevenue: {len(all_results)} rows "
              f"({n_actual} actuals + {n_fcast} forecasts) [{elapsed:.1f}s]")
        for mi in model_info:
            mape_str = f"MAPE={mi['MAPE_3m']}%" if mi['MAPE_3m'] else "MAPE=N/A"
            print(f"    {mi['Channel']:20s} {mi['Model']:25s} ({mi['Months']}m, {mape_str})")
        _cell_results["ForecastSalesRevenue"] = {"status": "OK", "rows": len(all_results), "elapsed": elapsed}
    else:
        print("  ⚠ No data available for sales forecast")
        _cell_results["ForecastSalesRevenue"] = {"status": "SKIP", "rows": 0}

except Exception as e:
    print(f"  ⚠ Sales Revenue Forecast failed: {e}")
    import traceback; traceback.print_exc()
    _cell_results["ForecastSalesRevenue"] = {"status": "FAIL", "error": str(e)}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 3: Genre Demand Forecast
# -----------------------------------------------------------
# Predict monthly book unit demand by Genre using Holt-Winters.
# Uses the enriched BookGenre column from FactOrders.

print("\n" + "=" * 60)
print("  📚 Genre Demand Forecast")
print("=" * 60)

_cell3_start = time.time()
try:
    df_orders = spark.table(gold_table("FactOrders", "fact"))

    # Check if BookGenre column exists (enriched in NB03 Cell 8)
    genre_col = "BookGenre" if "BookGenre" in df_orders.columns else None
    if genre_col is None:
        # Fallback: join with DimBooks for Genre
        df_books = spark.table(gold_table("DimBooks", "dim"))
        df_orders = df_orders.join(
            df_books.select("BookID", col("Genre").alias("BookGenre")),
            "BookID", "left"
        )
        genre_col = "BookGenre"

    # Monthly demand by genre
    monthly_genre = (df_orders
        .filter(col(genre_col).isNotNull())
        .withColumn("OrderMonth", trunc(col("OrderDate"), "month"))
        .groupBy("OrderMonth", col(genre_col).alias("Genre"))
        .agg(
            spark_sum("Quantity").alias("UnitDemand"),
            spark_sum("TotalAmount").alias("Revenue"),
            count("OrderID").alias("OrderCount")
        )
        .orderBy("Genre", "OrderMonth")
    )

    pdf_genre = monthly_genre.toPandas()
    pdf_genre["OrderMonth"] = pd.to_datetime(pdf_genre["OrderMonth"])
    genres = pdf_genre["Genre"].unique()
    last_date = pdf_genre["OrderMonth"].max()
    all_results = []
    model_info = []

    for genre in genres:
        g_data = pdf_genre[pdf_genre["Genre"] == genre].sort_values("OrderMonth")
        ts_units = g_data.set_index("OrderMonth")["UnitDemand"]
        ts_rev   = g_data.set_index("OrderMonth")["Revenue"]

        if len(ts_units) < 3:
            continue

        fcast_u, lower_u, upper_u, model_u = holt_winters_forecast(ts_units, FORECAST_HORIZON)
        fcast_r, lower_r, upper_r, _       = holt_winters_forecast(ts_rev, FORECAST_HORIZON)

        # Back-test
        if len(ts_units) >= 6:
            try:
                bt_f, _, _, _ = holt_winters_forecast(ts_units.iloc[:-3], 3)
                mape = compute_mape(ts_units.iloc[-3:].values, bt_f)
            except Exception:
                mape = None
        else:
            mape = None

        model_info.append({"Genre": genre, "Model": model_u, "Months": len(ts_units),
                           "MAPE_3m": round(mape, 2) if mape else None})

        # Actuals
        for _, row in g_data.iterrows():
            all_results.append({
                "ForecastMonth": row["OrderMonth"],
                "Genre": genre,
                "UnitDemand": float(row["UnitDemand"]),
                "Revenue": float(row["Revenue"]),
                "LowerBound": float(row["UnitDemand"]),
                "UpperBound": float(row["UnitDemand"]),
                "ForecastHorizon": 0,
                "RecordType": "Actual",
                "ForecastModel": model_u
            })

        # Forecasts
        for i in range(FORECAST_HORIZON):
            fc_date = last_date + pd.DateOffset(months=i + 1)
            all_results.append({
                "ForecastMonth": fc_date,
                "Genre": genre,
                "UnitDemand": float(max(fcast_u[i], 0)),
                "Revenue": float(max(fcast_r[i], 0)),
                "LowerBound": float(max(lower_u[i], 0)),
                "UpperBound": float(max(upper_u[i], 0)),
                "ForecastHorizon": i + 1,
                "RecordType": "Forecast",
                "ForecastModel": model_u
            })

    if all_results:
        pdf_result = pd.DataFrame(all_results)
        pdf_result["ForecastMonth"] = pd.to_datetime(pdf_result["ForecastMonth"]).dt.date
        pdf_result["_generated_at"] = FORECAST_GENERATED_AT

        schema = StructType([
            StructField("ForecastMonth", DateType()),
            StructField("Genre", StringType()),
            StructField("UnitDemand", DoubleType()),
            StructField("Revenue", DoubleType()),
            StructField("LowerBound", DoubleType()),
            StructField("UpperBound", DoubleType()),
            StructField("ForecastHorizon", IntegerType()),
            StructField("RecordType", StringType()),
            StructField("ForecastModel", StringType()),
            StructField("_generated_at", TimestampType())
        ])
        df_result = spark.createDataFrame(pdf_result, schema=schema)
        df_result.write.mode("overwrite").format("delta").option(
            "overwriteSchema", "true"
        ).saveAsTable(
            gold_table("ForecastGenreDemand", "analytics")
        )

        n_actual = len([r for r in all_results if r["RecordType"] == "Actual"])
        n_fcast  = len([r for r in all_results if r["RecordType"] == "Forecast"])
        elapsed = time.time() - _cell3_start
        print(f"  ✓ ForecastGenreDemand: {len(all_results)} rows "
              f"({n_actual} actuals + {n_fcast} forecasts) [{elapsed:.1f}s]")
        for mi in model_info:
            mape_str = f"MAPE={mi['MAPE_3m']}%" if mi['MAPE_3m'] else "MAPE=N/A"
            print(f"    {mi['Genre']:20s} {mi['Model']:25s} ({mi['Months']}m, {mape_str})")
        _cell_results["ForecastGenreDemand"] = {"status": "OK", "rows": len(all_results), "elapsed": elapsed}
    else:
        print("  ⚠ No genre data available for forecasting")
        _cell_results["ForecastGenreDemand"] = {"status": "SKIP", "rows": 0}

except Exception as e:
    print(f"  ⚠ Genre Demand Forecast failed: {e}")
    import traceback; traceback.print_exc()
    _cell_results["ForecastGenreDemand"] = {"status": "FAIL", "error": str(e)}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 4: Financial P&L Forecast
# -----------------------------------------------------------
# Forecast monthly P&L by AccountType (Revenue, COGS, OpEx…)
# using FactFinancialTransactions with PLCategory enrichment.

print("\n" + "=" * 60)
print("  💰 Financial P&L Forecast")
print("=" * 60)

_cell4_start = time.time()
try:
    df_fin = spark.table(gold_table("FactFinancialTransactions", "fact"))

    # Use PLCategory if enriched by NB03, otherwise fall back to AccountType via join
    cat_col = "PLCategory"
    if cat_col not in df_fin.columns:
        df_accounts = spark.table(gold_table("DimAccounts", "dim"))
        df_fin = df_fin.join(
            df_accounts.select("AccountID", col("AccountType").alias("PLCategory")),
            "AccountID", "left"
        )

    # Monthly aggregation by P&L category
    monthly_pl = (df_fin
        .filter(col("PLCategory").isNotNull())
        .withColumn("TxnMonth", trunc(col("TransactionDate"), "month"))
        .groupBy("TxnMonth", "PLCategory")
        .agg(
            spark_sum("Amount").alias("Amount"),
            count("*").alias("TransactionCount")
        )
        .orderBy("PLCategory", "TxnMonth")
    )

    pdf_pl = monthly_pl.toPandas()
    pdf_pl["TxnMonth"] = pd.to_datetime(pdf_pl["TxnMonth"])
    categories = pdf_pl["PLCategory"].unique()
    last_date = pdf_pl["TxnMonth"].max()
    all_results = []
    model_info = []

    for cat in categories:
        c_data = pdf_pl[pdf_pl["PLCategory"] == cat].sort_values("TxnMonth")
        ts = c_data.set_index("TxnMonth")["Amount"]

        if len(ts) < 3:
            continue

        # Financial data can be negative (expenses) — use absolute for modeling,
        # then restore sign
        sign = 1 if ts.mean() >= 0 else -1
        ts_abs = ts.abs() if sign < 0 else ts

        fcast, lower, upper, model_name = holt_winters_forecast(ts_abs, FORECAST_HORIZON)

        if sign < 0:
            fcast = -fcast
            lower_orig = lower
            lower = -upper  # flip bounds for negative
            upper = -lower_orig

        # Back-test
        if len(ts) >= 6:
            try:
                ts_abs_bt = ts_abs.iloc[:-3] if sign < 0 else ts.iloc[:-3]
                bt_f, _, _, _ = holt_winters_forecast(ts_abs_bt, 3)
                if sign < 0:
                    bt_f = -bt_f
                mape = compute_mape(ts.iloc[-3:].values, bt_f)
            except Exception:
                mape = None
        else:
            mape = None

        model_info.append({"Category": cat, "Model": model_name, "Months": len(ts),
                           "MAPE_3m": round(mape, 2) if mape else None})

        # Actuals
        for _, row in c_data.iterrows():
            all_results.append({
                "ForecastMonth": row["TxnMonth"],
                "PLCategory": cat,
                "Amount": float(row["Amount"]),
                "TransactionCount": int(row["TransactionCount"]),
                "LowerBound": float(row["Amount"]),
                "UpperBound": float(row["Amount"]),
                "ForecastHorizon": 0,
                "RecordType": "Actual",
                "ForecastModel": model_name
            })

        # Forecasts
        for i in range(FORECAST_HORIZON):
            fc_date = last_date + pd.DateOffset(months=i + 1)
            all_results.append({
                "ForecastMonth": fc_date,
                "PLCategory": cat,
                "Amount": float(fcast[i]),
                "TransactionCount": 0,
                "LowerBound": float(lower[i]),
                "UpperBound": float(upper[i]),
                "ForecastHorizon": i + 1,
                "RecordType": "Forecast",
                "ForecastModel": model_name
            })

    if all_results:
        pdf_result = pd.DataFrame(all_results)
        pdf_result["ForecastMonth"] = pd.to_datetime(pdf_result["ForecastMonth"]).dt.date
        pdf_result["_generated_at"] = FORECAST_GENERATED_AT

        schema = StructType([
            StructField("ForecastMonth", DateType()),
            StructField("PLCategory", StringType()),
            StructField("Amount", DoubleType()),
            StructField("TransactionCount", IntegerType()),
            StructField("LowerBound", DoubleType()),
            StructField("UpperBound", DoubleType()),
            StructField("ForecastHorizon", IntegerType()),
            StructField("RecordType", StringType()),
            StructField("ForecastModel", StringType()),
            StructField("_generated_at", TimestampType())
        ])
        df_result = spark.createDataFrame(pdf_result, schema=schema)
        df_result.write.mode("overwrite").format("delta").option(
            "overwriteSchema", "true"
        ).saveAsTable(
            gold_table("ForecastFinancial", "analytics")
        )

        n_actual = len([r for r in all_results if r["RecordType"] == "Actual"])
        n_fcast  = len([r for r in all_results if r["RecordType"] == "Forecast"])
        elapsed = time.time() - _cell4_start
        print(f"  ✓ ForecastFinancial: {len(all_results)} rows "
              f"({n_actual} actuals + {n_fcast} forecasts) [{elapsed:.1f}s]")
        for mi in model_info:
            mape_str = f"MAPE={mi['MAPE_3m']}%" if mi['MAPE_3m'] else "MAPE=N/A"
            print(f"    {mi['Category']:20s} {mi['Model']:25s} ({mi['Months']}m, {mape_str})")
        _cell_results["ForecastFinancial"] = {"status": "OK", "rows": len(all_results), "elapsed": elapsed}
    else:
        print("  ⚠ No financial data available for forecasting")
        _cell_results["ForecastFinancial"] = {"status": "SKIP", "rows": 0}

except Exception as e:
    print(f"  ⚠ Financial P&L Forecast failed: {e}")
    import traceback; traceback.print_exc()
    _cell_results["ForecastFinancial"] = {"status": "FAIL", "error": str(e)}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 5: Inventory Demand Forecast by Warehouse
# -----------------------------------------------------------
# Predict future order volume per warehouse for supply planning.
# Combines with current inventory to flag reorder needs.

print("\n" + "=" * 60)
print("  📦 Inventory Demand Forecast by Warehouse")
print("=" * 60)

_cell5_start = time.time()
try:
    df_orders = spark.table(gold_table("FactOrders", "fact"))

    # Monthly order volume by warehouse
    monthly_wh = (df_orders
        .filter(col("WarehouseID").isNotNull())
        .withColumn("OrderMonth", trunc(col("OrderDate"), "month"))
        .groupBy("OrderMonth", "WarehouseID")
        .agg(
            spark_sum("Quantity").alias("UnitsDemanded"),
            spark_sum("TotalAmount").alias("Revenue"),
            count("OrderID").alias("OrderCount")
        )
        .orderBy("WarehouseID", "OrderMonth")
    )

    pdf_wh = monthly_wh.toPandas()
    pdf_wh["OrderMonth"] = pd.to_datetime(pdf_wh["OrderMonth"])
    warehouses = pdf_wh["WarehouseID"].unique()
    last_date = pdf_wh["OrderMonth"].max()
    all_results = []
    model_info = []

    # Get current inventory levels per warehouse (latest snapshot)
    try:
        df_inv = spark.table(gold_table("FactInventory", "fact"))
        latest_inv = (df_inv
            .groupBy("WarehouseID")
            .agg(spark_sum("QuantityOnHand").alias("CurrentStock"))
        ).toPandas()
        stock_map = dict(zip(latest_inv["WarehouseID"], latest_inv["CurrentStock"]))
    except Exception:
        stock_map = {}

    for wh in warehouses:
        wh_data = pdf_wh[pdf_wh["WarehouseID"] == wh].sort_values("OrderMonth")
        ts = wh_data.set_index("OrderMonth")["UnitsDemanded"]

        if len(ts) < 3:
            continue

        fcast, lower, upper, model_name = holt_winters_forecast(ts, FORECAST_HORIZON)

        # Back-test
        if len(ts) >= 6:
            try:
                bt_f, _, _, _ = holt_winters_forecast(ts.iloc[:-3], 3)
                mape = compute_mape(ts.iloc[-3:].values, bt_f)
            except Exception:
                mape = None
        else:
            mape = None

        current_stock = stock_map.get(int(wh), 0)

        model_info.append({
            "WarehouseID": int(wh), "Model": model_name,
            "Months": len(ts), "MAPE_3m": round(mape, 2) if mape else None,
            "CurrentStock": current_stock
        })

        # Actuals
        for _, row in wh_data.iterrows():
            all_results.append({
                "ForecastMonth": row["OrderMonth"],
                "WarehouseID": int(row["WarehouseID"]),
                "UnitsDemanded": float(row["UnitsDemanded"]),
                "Revenue": float(row["Revenue"]),
                "LowerBound": float(row["UnitsDemanded"]),
                "UpperBound": float(row["UnitsDemanded"]),
                "ForecastHorizon": 0,
                "RecordType": "Actual",
                "ForecastModel": model_name,
                "CurrentStock": float(current_stock),
                "CumulativeDemand": 0.0,
                "StockCoverMonths": 0.0
            })

        # Forecasts with cumulative demand and stock coverage
        cumulative = 0.0
        for i in range(FORECAST_HORIZON):
            fc_date = last_date + pd.DateOffset(months=i + 1)
            demand = float(max(fcast[i], 0))
            cumulative += demand

            # How many months of stock remain?
            avg_monthly_demand = cumulative / (i + 1) if cumulative > 0 else 1
            stock_cover = current_stock / avg_monthly_demand if avg_monthly_demand > 0 else 99

            all_results.append({
                "ForecastMonth": fc_date,
                "WarehouseID": int(wh),
                "UnitsDemanded": demand,
                "Revenue": 0.0,
                "LowerBound": float(max(lower[i], 0)),
                "UpperBound": float(max(upper[i], 0)),
                "ForecastHorizon": i + 1,
                "RecordType": "Forecast",
                "ForecastModel": model_name,
                "CurrentStock": float(current_stock),
                "CumulativeDemand": round(cumulative, 2),
                "StockCoverMonths": round(stock_cover, 1)
            })

    if all_results:
        pdf_result = pd.DataFrame(all_results)
        pdf_result["ForecastMonth"] = pd.to_datetime(pdf_result["ForecastMonth"]).dt.date
        pdf_result["_generated_at"] = FORECAST_GENERATED_AT

        schema = StructType([
            StructField("ForecastMonth", DateType()),
            StructField("WarehouseID", IntegerType()),
            StructField("UnitsDemanded", DoubleType()),
            StructField("Revenue", DoubleType()),
            StructField("LowerBound", DoubleType()),
            StructField("UpperBound", DoubleType()),
            StructField("ForecastHorizon", IntegerType()),
            StructField("RecordType", StringType()),
            StructField("ForecastModel", StringType()),
            StructField("CurrentStock", DoubleType()),
            StructField("CumulativeDemand", DoubleType()),
            StructField("StockCoverMonths", DoubleType()),
            StructField("_generated_at", TimestampType())
        ])
        df_result = spark.createDataFrame(pdf_result, schema=schema)
        df_result.write.mode("overwrite").format("delta").option(
            "overwriteSchema", "true"
        ).saveAsTable(
            gold_table("ForecastInventoryDemand", "analytics")
        )

        n_actual = len([r for r in all_results if r["RecordType"] == "Actual"])
        n_fcast  = len([r for r in all_results if r["RecordType"] == "Forecast"])
        elapsed = time.time() - _cell5_start
        print(f"  ✓ ForecastInventoryDemand: {len(all_results)} rows "
              f"({n_actual} actuals + {n_fcast} forecasts) [{elapsed:.1f}s]")
        for mi in model_info:
            mape_str = f"MAPE={mi['MAPE_3m']}%" if mi['MAPE_3m'] else "MAPE=N/A"
            stock_str = f"stock={mi['CurrentStock']:,.0f}"
            print(f"    WH-{mi['WarehouseID']:<17d} {mi['Model']:25s} ({mi['Months']}m, {mape_str}, {stock_str})")
        _cell_results["ForecastInventoryDemand"] = {"status": "OK", "rows": len(all_results), "elapsed": elapsed}
    else:
        print("  ⚠ No warehouse data available for forecasting")
        _cell_results["ForecastInventoryDemand"] = {"status": "SKIP", "rows": 0}

except Exception as e:
    print(f"  ⚠ Inventory Demand Forecast failed: {e}")
    import traceback; traceback.print_exc()
    _cell_results["ForecastInventoryDemand"] = {"status": "FAIL", "error": str(e)}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 6: Workforce Planning Forecast
# -----------------------------------------------------------
# Forecast monthly hiring volume and payroll costs using
# FactRecruitment (hiring pipeline) and FactPayroll.

print("\n" + "=" * 60)
print("  👥 Workforce Planning Forecast")
print("=" * 60)

_cell6_start = time.time()
try:
    # ── Hiring Volume Forecast ──
    df_recruit = spark.table(gold_table("FactRecruitment", "fact"))

    monthly_hiring = (df_recruit
        .withColumn("HireMonth", trunc(col("OpenDate"), "month"))
        .groupBy("HireMonth")
        .agg(
            count("*").alias("Openings"),
            spark_sum(when(col("Status") == "Filled", 1).otherwise(0)).alias("Hires")
        )
        .orderBy("HireMonth")
    )

    pdf_hiring = monthly_hiring.toPandas()
    pdf_hiring["HireMonth"] = pd.to_datetime(pdf_hiring["HireMonth"])
    last_date_h = pdf_hiring["HireMonth"].max()

    # ── Payroll Cost Forecast ──
    df_payroll = spark.table(gold_table("FactPayroll", "fact"))

    monthly_payroll = (df_payroll
        .withColumn("PayMonth", trunc(col("PayDate"), "month"))
        .groupBy("PayMonth")
        .agg(
            spark_sum("GrossPay").alias("TotalPayroll"),
            countDistinct("EmployeeID").alias("Headcount")
        )
        .orderBy("PayMonth")
    )

    pdf_payroll = monthly_payroll.toPandas()
    pdf_payroll["PayMonth"] = pd.to_datetime(pdf_payroll["PayMonth"])
    last_date_p = pdf_payroll["PayMonth"].max()

    all_results = []
    model_summary = []

    # Forecast hiring
    ts_openings = pdf_hiring.set_index("HireMonth")["Openings"]
    ts_hires    = pdf_hiring.set_index("HireMonth")["Hires"]

    if len(ts_openings) >= 3:
        fcast_o, low_o, up_o, model_o = holt_winters_forecast(ts_openings, FORECAST_HORIZON)
        fcast_h, low_h, up_h, _       = holt_winters_forecast(ts_hires, FORECAST_HORIZON)
        model_summary.append(f"Openings: {model_o} ({len(ts_openings)}m)")

        for _, row in pdf_hiring.iterrows():
            all_results.append({
                "ForecastMonth": row["HireMonth"],
                "Metric": "Openings",
                "Value": float(row["Openings"]),
                "LowerBound": float(row["Openings"]),
                "UpperBound": float(row["Openings"]),
                "ForecastHorizon": 0,
                "RecordType": "Actual",
                "ForecastModel": model_o
            })
            all_results.append({
                "ForecastMonth": row["HireMonth"],
                "Metric": "Hires",
                "Value": float(row["Hires"]),
                "LowerBound": float(row["Hires"]),
                "UpperBound": float(row["Hires"]),
                "ForecastHorizon": 0,
                "RecordType": "Actual",
                "ForecastModel": model_o
            })

        for i in range(FORECAST_HORIZON):
            fc_date = last_date_h + pd.DateOffset(months=i + 1)
            all_results.append({
                "ForecastMonth": fc_date,
                "Metric": "Openings",
                "Value": float(max(fcast_o[i], 0)),
                "LowerBound": float(max(low_o[i], 0)),
                "UpperBound": float(max(up_o[i], 0)),
                "ForecastHorizon": i + 1,
                "RecordType": "Forecast",
                "ForecastModel": model_o
            })
            all_results.append({
                "ForecastMonth": fc_date,
                "Metric": "Hires",
                "Value": float(max(fcast_h[i], 0)),
                "LowerBound": float(max(low_h[i], 0)),
                "UpperBound": float(max(up_h[i], 0)),
                "ForecastHorizon": i + 1,
                "RecordType": "Forecast",
                "ForecastModel": model_o
            })

    # Forecast payroll
    ts_payroll = pdf_payroll.set_index("PayMonth")["TotalPayroll"]
    ts_headcount = pdf_payroll.set_index("PayMonth")["Headcount"]

    if len(ts_payroll) >= 3:
        fcast_p, low_p, up_p, model_p = holt_winters_forecast(ts_payroll, FORECAST_HORIZON)
        fcast_hc, low_hc, up_hc, _    = holt_winters_forecast(ts_headcount, FORECAST_HORIZON)
        model_summary.append(f"Payroll: {model_p} ({len(ts_payroll)}m)")

        for _, row in pdf_payroll.iterrows():
            all_results.append({
                "ForecastMonth": row["PayMonth"],
                "Metric": "TotalPayroll",
                "Value": float(row["TotalPayroll"]),
                "LowerBound": float(row["TotalPayroll"]),
                "UpperBound": float(row["TotalPayroll"]),
                "ForecastHorizon": 0,
                "RecordType": "Actual",
                "ForecastModel": model_p
            })
            all_results.append({
                "ForecastMonth": row["PayMonth"],
                "Metric": "Headcount",
                "Value": float(row["Headcount"]),
                "LowerBound": float(row["Headcount"]),
                "UpperBound": float(row["Headcount"]),
                "ForecastHorizon": 0,
                "RecordType": "Actual",
                "ForecastModel": model_p
            })

        for i in range(FORECAST_HORIZON):
            fc_date = last_date_p + pd.DateOffset(months=i + 1)
            all_results.append({
                "ForecastMonth": fc_date,
                "Metric": "TotalPayroll",
                "Value": float(max(fcast_p[i], 0)),
                "LowerBound": float(max(low_p[i], 0)),
                "UpperBound": float(max(up_p[i], 0)),
                "ForecastHorizon": i + 1,
                "RecordType": "Forecast",
                "ForecastModel": model_p
            })
            all_results.append({
                "ForecastMonth": fc_date,
                "Metric": "Headcount",
                "Value": float(max(fcast_hc[i], 0)),
                "LowerBound": float(max(low_hc[i], 0)),
                "UpperBound": float(max(up_hc[i], 0)),
                "ForecastHorizon": i + 1,
                "RecordType": "Forecast",
                "ForecastModel": model_p
            })

    if all_results:
        pdf_result = pd.DataFrame(all_results)
        pdf_result["ForecastMonth"] = pd.to_datetime(pdf_result["ForecastMonth"]).dt.date
        pdf_result["_generated_at"] = FORECAST_GENERATED_AT

        schema = StructType([
            StructField("ForecastMonth", DateType()),
            StructField("Metric", StringType()),
            StructField("Value", DoubleType()),
            StructField("LowerBound", DoubleType()),
            StructField("UpperBound", DoubleType()),
            StructField("ForecastHorizon", IntegerType()),
            StructField("RecordType", StringType()),
            StructField("ForecastModel", StringType()),
            StructField("_generated_at", TimestampType())
        ])
        df_result = spark.createDataFrame(pdf_result, schema=schema)
        df_result.write.mode("overwrite").format("delta").option(
            "overwriteSchema", "true"
        ).saveAsTable(
            gold_table("ForecastWorkforce", "analytics")
        )

        n_actual = len([r for r in all_results if r["RecordType"] == "Actual"])
        n_fcast  = len([r for r in all_results if r["RecordType"] == "Forecast"])
        elapsed = time.time() - _cell6_start
        print(f"  ✓ ForecastWorkforce: {len(all_results)} rows "
              f"({n_actual} actuals + {n_fcast} forecasts) [{elapsed:.1f}s]")
        for ms in model_summary:
            print(f"    {ms}")
        _cell_results["ForecastWorkforce"] = {"status": "OK", "rows": len(all_results), "elapsed": elapsed}
    else:
        print("  ⚠ No workforce data available for forecasting")
        _cell_results["ForecastWorkforce"] = {"status": "SKIP", "rows": 0}

except Exception as e:
    print(f"  ⚠ Workforce Planning Forecast failed: {e}")
    import traceback; traceback.print_exc()
    _cell_results["ForecastWorkforce"] = {"status": "FAIL", "error": str(e)}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 7: Forecast Validation & Summary
# -----------------------------------------------------------

print("\n" + "=" * 60)
print("  FORECAST LAYER VALIDATION")
print("=" * 60)

forecast_tables = [
    "analytics.ForecastSalesRevenue",
    "analytics.ForecastGenreDemand",
    "analytics.ForecastFinancial",
    "analytics.ForecastInventoryDemand",
    "analytics.ForecastWorkforce"
]

total_rows = 0
total_forecast_rows = 0
validation_errors = []

for t in forecast_tables:
    try:
        df = spark.table(t)
        row_count = df.count()
        col_count = len(df.columns)
        fc_count  = df.filter(col("RecordType") == "Forecast").count()
        total_rows += row_count
        total_forecast_rows += fc_count

        # Forecast date range
        fc_min = df.filter(col("RecordType") == "Forecast").agg(
            spark_min("ForecastMonth")
        ).collect()[0][0]
        fc_max = df.filter(col("RecordType") == "Forecast").agg(
            spark_max("ForecastMonth")
        ).collect()[0][0]

        fc_range = f"{fc_min} → {fc_max}" if fc_min else "N/A"
        print(f"  ✓ {t:42s} {row_count:>6,} rows  {col_count:>2} cols  "
              f"({fc_count} forecasts, {fc_range})")

        # Data quality checks
        if row_count == 0:
            validation_errors.append(f"{t}: table is empty")
        if fc_count == 0:
            validation_errors.append(f"{t}: no forecast rows generated")
    except Exception as e:
        print(f"  ✗ {t:42s} ERROR: {e}")
        validation_errors.append(f"{t}: {e}")

print(f"\n  TOTAL: {total_rows:,} rows across {len(forecast_tables)} tables")
print(f"  FORECAST ROWS: {total_forecast_rows:,}")

# Model summary
print("\n  Model Usage:")
for t in forecast_tables:
    try:
        df = spark.table(t)
        models = df.select("ForecastModel").distinct().collect()
        model_names = [m["ForecastModel"] for m in models]
        tbl_short = t.split(".")[-1]
        print(f"    {tbl_short:35s} → {', '.join(model_names)}")
    except Exception:
        pass

# Execution summary
total_elapsed = time.time() - _notebook_start
print(f"\n  {'─' * 50}")
print(f"  Execution Summary:")
print(f"  {'─' * 50}")
for tbl_name, info in _cell_results.items():
    status = info["status"]
    if status == "OK":
        print(f"    ✓ {tbl_name:35s} {info['rows']:>6,} rows  {info['elapsed']:.1f}s")
    elif status == "SKIP":
        print(f"    ⚠ {tbl_name:35s} SKIPPED (no data)")
    else:
        print(f"    ✗ {tbl_name:35s} FAILED: {info.get('error', 'unknown')}")

n_ok   = sum(1 for v in _cell_results.values() if v["status"] == "OK")
n_fail = sum(1 for v in _cell_results.values() if v["status"] == "FAIL")
n_skip = sum(1 for v in _cell_results.values() if v["status"] == "SKIP")

print(f"\n  Total: {n_ok} OK, {n_skip} skipped, {n_fail} failed")
print(f"  Total elapsed: {total_elapsed:.1f}s")
print(f"  Generated at: {FORECAST_GENERATED_AT:%Y-%m-%d %H:%M:%S} UTC")

if validation_errors:
    print(f"\n  ⚠ VALIDATION WARNINGS ({len(validation_errors)}):")
    for ve in validation_errors:
        print(f"    - {ve}")

if n_fail > 0:
    raise RuntimeError(
        f"Forecasting notebook completed with {n_fail} failure(s). "
        f"Check logs above for details."
    )

print("\n=== Forecasting Complete ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
