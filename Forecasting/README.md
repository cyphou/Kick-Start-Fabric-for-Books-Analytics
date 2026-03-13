<p align="center">
  <img src="../assets/workspace-logo.png" alt="Horizon Books" width="80"/>
</p>

<h1 align="center">Forecasting Module</h1>

<p align="center">
  <strong>Holt-Winters time-series forecasting with MLflow experiment tracking</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/models-5-blue?style=flat-square" alt="Models"/>
  <img src="https://img.shields.io/badge/horizon-6%20months-green?style=flat-square" alt="Horizon"/>
  <img src="https://img.shields.io/badge/confidence-95%25-purple?style=flat-square" alt="Confidence"/>
  <img src="https://img.shields.io/badge/MLflow-tracked-orange?style=flat-square&logo=mlflow&logoColor=white" alt="MLflow"/>
</p>

<p align="center">
  <a href="#-forecast-models">Models</a> •
  <a href="#-mlflow-experiment-tracking">MLflow</a> •
  <a href="#-configuration">Config</a> •
  <a href="#-output-schema">Schema</a> •
  <a href="#-dependencies">Dependencies</a>
</p>

---

## 📊 Forecast Models

| # | Model | Target Table | Method | Dimensions |
|---|-------|-------------|--------|------------|
| 1 | Sales Revenue | `ForecastSalesRevenue` | Holt-Winters (additive) | By channel |
| 2 | Genre Demand | `ForecastGenreDemand` | Holt-Winters per genre | By genre |
| 3 | Financial P&L | `ForecastFinancial` | Holt-Winters per account type | By P&L category |
| 4 | Inventory Demand | `ForecastInventoryDemand` | Holt-Winters per book | By book |
| 5 | Workforce | `ForecastWorkforce` | Holt-Winters on headcount/payroll | By metric |

All models project **6 months ahead** with **95% confidence intervals** and require at least **12 months** of history.

---

## 📈 MLflow Experiment Tracking

All forecast runs are tracked in the `HorizonBooks_Forecasting` MLflow experiment:

```mermaid
flowchart TD
    P["🧪 Parent Run\nNB04_Forecasting_<timestamp>"] --> C1["📊 SalesRevenue\nparams, metrics, tags"]
    P --> C2["📊 GenreDemand"]
    P --> C3["📊 Financial"]
    P --> C4["📊 InventoryDemand"]
    P --> C5["📊 Workforce"]

    style P fill:#1B3A5C,color:#fff
    style C1 fill:#3A8FBF,color:#fff
    style C2 fill:#3A8FBF,color:#fff
    style C3 fill:#3A8FBF,color:#fff
    style C4 fill:#3A8FBF,color:#fff
    style C5 fill:#3A8FBF,color:#fff
```

| Level | Logs |
|-------|------|
| **Parent run** | Global parameters (horizon, confidence, min history) |
| **Child runs** (×5) | Table name, model name, dimensions, row count, MAPE, elapsed time |
| **Aggregation** | Total rows written, tables created, overall elapsed time |

> [!NOTE]
> Fabric's automatic `mlflow.autolog()` is **disabled** to prevent spurious failed experiment runs from statsmodels `.fit()` calls in the try/except fallback loop.

View results: Fabric Portal → **Experiments** → `HorizonBooks_Forecasting`

---

## ⚙️ Configuration

Default parameters (defined in `04_Forecasting.py`, Cell 1):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `FORECAST_HORIZON` | 6 | Months to forecast ahead |
| `CONFIDENCE_LEVEL` | 0.95 | Confidence interval (95%) |
| `MIN_HISTORY_MONTHS` | 12 | Minimum data points required |
| Seasonal periods | 12 | Monthly seasonality cycle |

> `forecast-config.json` documents the same defaults but is not consumed at runtime. The notebook uses Python constants directly for Spark compatibility.

---

## 📋 Output Schema

All tables are written to **GoldLH** under the `analytics` schema.

### Common Columns (all tables)

| Column | Type | Description |
|--------|------|-------------|
| `ForecastMonth` | Date | Projected month (1st of month) |
| `LowerBound` | Double | Lower confidence interval |
| `UpperBound` | Double | Upper confidence interval |
| `ForecastHorizon` | Integer | 0 = actual, 1–6 = forecast |
| `RecordType` | String | `Actual` or `Forecast` |
| `ForecastModel` | String | Algorithm (e.g. `HoltWinters_add_add`) |
| `_generated_at` | Timestamp | Run timestamp |

### Model-Specific Columns

<details>
<summary><b>ForecastSalesRevenue</b></summary>

| Column | Type |
|--------|------|
| Channel | String |
| Revenue | Double |
| Orders | Integer |
| Customers | Integer |

**Measures:** Forecast Revenue, Revenue Lower/Upper Bound, Forecast vs Actual Revenue

</details>

<details>
<summary><b>ForecastGenreDemand</b></summary>

| Column | Type |
|--------|------|
| Genre | String |
| UnitDemand | Double |
| Revenue | Double |

**Measures:** Forecast Unit Demand, Forecast Genre Revenue, Demand Confidence Range

</details>

<details>
<summary><b>ForecastFinancial</b></summary>

| Column | Type |
|--------|------|
| PLCategory | String |
| Amount | Double |
| TransactionCount | Integer |

**Measures:** Forecast P&L Amount, Forecast P&L Lower, Forecast P&L Upper

</details>

<details>
<summary><b>ForecastInventoryDemand</b></summary>

| Column | Type |
|--------|------|
| BookID / WarehouseID | String/Integer |
| UnitsDemanded | Double |
| CumulativeDemand | Double |
| StockCoverMonths | Double |

**Measures:** Forecast Demand Units, Stock Coverage Months, Cumulative Forecast Demand

</details>

<details>
<summary><b>ForecastWorkforce</b></summary>

| Column | Type |
|--------|------|
| Department | String |
| Metric | String |
| Headcount / NetPay | Double |

**Measures:** Forecast Workforce Value, Forecast Payroll, Forecast Headcount, Forecast Openings

</details>

---

## 📦 Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| **statsmodels** | ≥ 0.14 | `ExponentialSmoothing` (Holt-Winters) |
| **pandas** | ≥ 1.5 | Time-series data manipulation |
| **numpy** | ≥ 1.24 | Numerical operations |
| **scipy** | ≥ 1.10 | Statistical functions |
| **mlflow** | built-in | Experiment tracking (Fabric runtime) |

Installed via the **HorizonBooks_SparkEnv** Fabric Spark environment (see `definitions/environment/`).

---

## ▶️ How to Run

The forecast notebook runs as part of the orchestration pipeline (`PL_HorizonBooks_Orchestration`) after Silver→Gold, or can be run **standalone** from the Fabric workspace.

For local exploration, use `ForecastingExploration.ipynb` (standard Jupyter notebook with Holt-Winters visualizations).

---

<p align="center">
  <sub>Notebook: <code>04_Forecasting.py</code> — Target: <code>GoldLH.analytics.*</code></sub>
</p>
