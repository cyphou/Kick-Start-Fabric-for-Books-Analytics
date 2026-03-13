<p align="center">
  <img src="../assets/workspace-logo.png" alt="Horizon Books" width="80"/>
</p>

<h1 align="center">CI/CD Item Definitions</h1>

<p align="center">
  <strong>Static reference copies of all Fabric item definitions for version control & CI/CD</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/items-15-blue?style=flat-square" alt="Items"/>
  <img src="https://img.shields.io/badge/tokens-20-orange?style=flat-square" alt="Tokens"/>
  <img src="https://img.shields.io/badge/format-JSON%20%2B%20PQ-green?style=flat-square" alt="Formats"/>
</p>

<p align="center">
  <a href="#-token-placeholders">Tokens</a> •
  <a href="#-folder-structure">Structure</a> •
  <a href="#-usage-in-cicd">CI/CD</a> •
  <a href="#-dataflow-destination-pattern">Destinations</a>
</p>

---

## 🔑 Token Placeholders

Deploy scripts substitute these tokens with actual GUIDs at runtime:

| Token | Description | Resolved By |
|-------|-------------|-------------|
| `{{WORKSPACE_ID}}` | Fabric workspace GUID | Deploy scripts |
| `{{BRONZE_LH_ID}}` | BronzeLH Lakehouse GUID | Deploy-Full.ps1 |
| `{{SILVER_LH_ID}}` | SilverLH Lakehouse GUID | Deploy-Full.ps1 |
| `{{GOLD_LH_ID}}` | GoldLH Lakehouse GUID | Deploy-Full.ps1 |
| `{{BRONZE_LH_NAME}}` | BronzeLH display name | Deploy-Full.ps1 |
| `{{SILVER_LH_NAME}}` | SilverLH display name | Deploy-Full.ps1 |
| `{{GOLD_LH_NAME}}` | GoldLH display name | Deploy-Full.ps1 |
| `{{SQL_ENDPOINT}}` | GoldLH SQL analytics endpoint | Deploy-Full.ps1 |
| `{{DF_FINANCE_ID}}` | Dataflow Gen2 Finance GUID | Deploy-Pipeline.ps1 |
| `{{DF_HR_ID}}` | Dataflow Gen2 HR GUID | Deploy-Pipeline.ps1 |
| `{{DF_OPERATIONS_ID}}` | Dataflow Gen2 Operations GUID | Deploy-Pipeline.ps1 |
| `{{NB01_ID}}` – `{{NB04_ID}}` | Notebook GUIDs | Deploy-Pipeline.ps1 |
| `{{SPARK_ENV_ID}}` | Spark Environment GUID | Deploy-Full.ps1 |
| `{{PIPELINE_ID}}` | Data Pipeline GUID | Deploy-Pipeline.ps1 |
| `{{SEMANTIC_MODEL_ID}}` | Semantic Model GUID | Deploy-Full.ps1 |
| `{{REPORT_ID}}` | Power BI Report GUID | Deploy script |
| `{{DATA_AGENT_ID}}` | Data Agent GUID | Deploy-DataAgent.ps1 |

---

## 📁 Folder Structure

```
definitions/
├── items-manifest.json              # Full item catalog (15 items)
├── README.md                        # This file
│
├── dataflows/
│   ├── queryMetadata.json           # Shared compute settings
│   ├── DF_Finance/
│   │   ├── mashup.pq                # Power Query M (4 tables + 4 destinations)
│   │   └── output-config.json       # Lakehouse mapping (legacy)
│   ├── DF_HR/
│   │   ├── mashup.pq                # 5 tables + 5 destinations
│   │   └── output-config.json
│   └── DF_Operations/
│       ├── mashup.pq                # 8 tables + 8 destinations
│       └── output-config.json
│
├── pipeline/
│   └── pipeline-content.json        # Orchestration pipeline (6 activities)
│
├── notebooks/
│   ├── nb01-lakehouse-metadata.json  # NB01 lakehouse binding
│   ├── nb02-lakehouse-metadata.json
│   ├── nb03-lakehouse-metadata.json
│   └── nb04-lakehouse-metadata.json
│
├── environment/
│   ├── environment-definition.json   # Spark 1.3, adaptive query, delta opt
│   ├── public-libraries.json         # PyPI dependencies
│   └── requirements.txt              # pip-compatible format
│
├── lakehouses/
│   ├── BronzeLH.json                # Bronze config (schemas, files, tables)
│   ├── SilverLH.json                # Silver config (4 schemas)
│   └── GoldLH.json                  # Gold config (dim/fact/analytics)
│
├── report/
│   └── report-definition.json       # Report spec (10 pages, visuals, bookmarks)
│
└── dataagent/
    └── dataagent-definition.json    # Data Agent config
```

---

## 🔄 Dataflow Destination Pattern

Each `mashup.pq` contains paired queries for every table:

```
┌──────────────────────┐       ┌──────────────────────────────┐
│  TableName           │──────▶│  TableName_DataDestination   │
│  (source + transform)│       │  (Lakehouse.Contents → table)│
│  [DataDestinations]  │       │  Hidden, loadEnabled = false  │
└──────────────────────┘       └──────────────────────────────┘
```

- **`TableName_DataDestination`** — Hidden navigation query using `Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false])`
- **`TableName`** — Source query with `[DataDestinations = {...}]` attribute linking to the destination query

This enables **fully automated** Lakehouse destinations without manual portal configuration.

---

## 🔧 Usage in CI/CD

These files serve as:

| Purpose | Description |
|---------|-------------|
| **Version control** | Every Fabric item definition tracked in Git |
| **Code review** | Diff-friendly reference for M queries, pipeline activities |
| **CI/CD templates** | Consumable by alternative deployment tooling |
| **Onboarding** | New team members can inspect definitions without deploying |

> The primary deployment path remains `Deploy-Full.ps1` and `Deploy-Pipeline.ps1`, which generate definitions dynamically and deploy via the Fabric REST API.

### Item Coverage

| Item Type | Count | Location |
|-----------|-------|----------|
| Lakehouse | 3 | `lakehouses/` |
| Environment | 1 | `environment/` |
| Notebook | 4 | `notebooks/` (metadata only — code in `notebooks/*.py`) |
| Dataflow Gen2 | 3 | `dataflows/` |
| Pipeline | 1 | `pipeline/` |
| Semantic Model | 1 | `HorizonBooksAnalytics/` (PBIP/TMDL format) |
| Report | 2 | `HorizonBooksAnalytics/` + `HorizonBooksForecasting/` |

---

<p align="center">
  <sub>Manifest: <code>items-manifest.json</code> — 15 Fabric items</sub>
</p>
