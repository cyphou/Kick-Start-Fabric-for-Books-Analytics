# Forecasting Module

## Overview
This folder contains the configuration, documentation, and supporting artifacts for the
Horizon Books time-series forecasting pipeline (Notebook 04).

## Forecast Models

| Model | Table | Method | Horizon |
|-------|-------|--------|---------|
| Sales Revenue | `ForecastSalesRevenue` | Holt-Winters (additive) | 6 months |
| Genre Demand | `ForecastGenreDemand` | Holt-Winters per genre | 6 months |
| Financial P&L | `ForecastFinancial` | Holt-Winters per account type | 6 months |
| Inventory Demand | `ForecastInventoryDemand` | Holt-Winters per book | 6 months |
| Workforce | `ForecastWorkforce` | Holt-Winters on headcount/payroll | 6 months |

## Dependencies
- **statsmodels** ≥ 0.14 — `ExponentialSmoothing` (Holt-Winters)
- **pandas** ≥ 1.5 — Time-series data manipulation
- **numpy** ≥ 1.24 — Numerical operations
- **scipy** ≥ 1.10 — Statistical functions (statsmodels dependency)
- **mlflow** — Experiment tracking (built into Fabric runtime)

These are installed via the **HorizonBooks_SparkEnv** Fabric Spark environment
(see `definitions/environment/`).

## MLflow Experiment Tracking

All forecast runs are tracked in the `HorizonBooks_Forecasting` MLflow experiment:

- **Autolog disabled** — Fabric's automatic `mlflow.autolog()` is turned off to prevent
  spurious failed experiment runs from statsmodels `.fit()` calls in the try/except fallback loop.
- **Parent run** — One parent run per notebook execution, named `NB04_Forecasting_<timestamp>`,
  logs global parameters (horizon, confidence level, min history months).
- **Child runs** — Each forecast table (SalesRevenue, GenreDemand, Financial, InventoryDemand,
  Workforce) creates a nested child run logging:
  - **Parameters**: table name, model name(s), dimensions
  - **Metrics**: row count, forecast MAPE, elapsed time (seconds)
  - **Tags**: model types, dimension breakdowns
- **Aggregate metrics** — The parent run logs total rows written, tables created, and
  overall elapsed time upon completion.

View experiment results in the Fabric portal under **Experiments** → `HorizonBooks_Forecasting`.

## Configuration

Default parameters (defined in `04_Forecasting.py`, Cell 1):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `FORECAST_HORIZON` | 6 | Months to forecast ahead |
| `CONFIDENCE_LEVEL` | 0.95 | Confidence interval (95%) |
| `MIN_HISTORY_MONTHS` | 12 | Minimum data points required |
| Seasonal periods | 12 | Monthly seasonality cycle (Holt-Winters `seasonal_periods`) |

> **Note:** `forecast-config.json` documents the same defaults but is not consumed at runtime.
> The notebook uses Python constants directly for Spark compatibility.

## Output Schema
All forecast tables are written to **GoldLH** under the `analytics` schema.
Each table has a model-specific schema; common columns across all tables:

| Column | Type | Description |
|--------|------|-------------|
| `ForecastMonth` | Date | Projected month (1st of month) |
| `LowerBound` | Double | Lower confidence interval bound |
| `UpperBound` | Double | Upper confidence interval bound |
| `ForecastHorizon` | Integer | Period index (0 = actual, 1–6 = forecast) |
| `RecordType` | String | `Actual` or `Forecast` |
| `ForecastModel` | String | Algorithm used (e.g. `HoltWinters_add_add`) |
| `_generated_at` | Timestamp | Run timestamp |

### Model-specific columns

| Table | Extra Columns |
|-------|---------------|
| `ForecastSalesRevenue` | `Channel`, `Revenue`, `Orders`, `Customers` |
| `ForecastGenreDemand` | `Genre`, `UnitDemand`, `Revenue` |
| `ForecastFinancial` | `PLCategory`, `Amount`, `TransactionCount` |
| `ForecastInventoryDemand` | `BookID`, `WarehouseID`, `QuantityOnHand` (see notebook) |
| `ForecastWorkforce` | `Department`, `Headcount`, `NetPay` (see notebook) |

## How to Run
The forecasting notebook runs as part of the orchestration pipeline
(`PL_HorizonBooks_Orchestration`) after the Silver→Gold transformation,
or can be run standalone from the Fabric workspace.
