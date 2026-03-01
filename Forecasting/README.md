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

These are installed via the **HorizonBooks_SparkEnv** Fabric Spark environment
(see `definitions/environment/`).

## Configuration

Default parameters (defined in `forecast-config.json`):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `forecastHorizon` | 6 | Months to forecast ahead |
| `confidenceLevel` | 0.95 | Confidence interval (95%) |
| `minHistoryMonths` | 12 | Minimum data points required |
| `seasonalPeriods` | 12 | Monthly seasonality cycle |

## Output Schema
All forecast tables are written to **GoldLH** under the `analytics` schema and include:
- `ForecastDate` — Projected date
- `ForecastValue` — Point forecast
- `LowerBound` / `UpperBound` — Confidence interval
- `ModelName` — Algorithm used
- `GeneratedAt` — Timestamp of forecast run

## How to Run
The forecasting notebook runs as part of the orchestration pipeline
(`PL_HorizonBooks_Orchestration`) after the Silver→Gold transformation,
or can be run standalone from the Fabric workspace.
