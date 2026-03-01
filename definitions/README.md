# Fabric Item Definitions (CI/CD Reference Copies)

This folder contains **static reference copies** of all Fabric item definitions for CI/CD
visibility, code review, and version control. The deploy scripts (`Deploy-Full.ps1`,
`Deploy-Pipeline.ps1`) generate these definitions dynamically at runtime — these files
are the canonical snapshots of what gets deployed.

Token placeholders (`{{...}}`) show where runtime values (workspace/lakehouse GUIDs) are injected.

## Token Placeholders

| Token | Description | Resolved By |
|---|---|---|
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
| `{{NB01_ID}}` | Notebook 01 BronzeToSilver GUID | Deploy-Pipeline.ps1 |
| `{{NB02_ID}}` | Notebook 02 WebEnrichment GUID | Deploy-Pipeline.ps1 |
| `{{NB03_ID}}` | Notebook 03 SilverToGold GUID | Deploy-Pipeline.ps1 |
| `{{SEMANTIC_MODEL_ID}}` | Semantic Model GUID | Deploy-Full.ps1 |
| `{{REPORT_ID}}` | Power BI Report GUID | Manual / Deploy script |
| `{{DATA_AGENT_ID}}` | Data Agent GUID | Deploy-DataAgent.ps1 |

## Folder Structure

```
definitions/
├── dataflows/
│   ├── queryMetadata.json          # Shared Dataflow Gen2 compute settings
│   ├── DF_Finance/
│   │   ├── mashup.pq               # Power Query M (4 Finance tables + 4 _DataDestination queries)
│   │   └── output-config.json      # Lakehouse destination mapping (legacy reference)
│   ├── DF_HR/
│   │   ├── mashup.pq               # Power Query M (5 HR tables + 5 _DataDestination queries)
│   │   └── output-config.json
│   └── DF_Operations/
│       ├── mashup.pq               # Power Query M (8 Operations tables + 8 _DataDestination queries)
│       └── output-config.json
├── pipeline/
│   └── pipeline-content.json       # Orchestration pipeline (6 activities)
├── notebooks/
│   ├── nb01-lakehouse-metadata.json  # NB01 lakehouse binding template
│   ├── nb02-lakehouse-metadata.json  # NB02 lakehouse binding template
│   └── nb03-lakehouse-metadata.json  # NB03 lakehouse binding template
├── lakehouses/
│   ├── BronzeLH.json               # Bronze Lakehouse creation config (schemas, files, tables)
│   ├── SilverLH.json               # Silver Lakehouse creation config (4 schemas)
│   └── GoldLH.json                 # Gold Lakehouse creation config (dim/fact/analytics)
├── report/
│   └── report-definition.json      # Power BI report spec (10 pages, visuals, bookmarks)
├── dataagent/
│   └── dataagent-definition.json   # Data Agent config (instructions, table mapping, starter Qs)
├── items-manifest.json             # Full item catalog for CI/CD (13 items)
└── README.md                       # This file
```

## Usage in CI/CD

These files serve as:
1. **Version-controlled documentation** of every Fabric item definition
2. **Diff-friendly reference** for code reviews (changes to M queries, pipeline activities, etc.)
3. **CI/CD templates** — they can be consumed by alternative deployment tooling that reads files + replaces tokens
4. **Onboarding aid** — new team members can inspect definitions without deploying

The primary deployment path remains `Deploy-Full.ps1` and `Deploy-Pipeline.ps1`, which
generate definitions dynamically and deploy via the Fabric REST API.

### Dataflow Destination Pattern

Each `mashup.pq` contains paired queries for every table:
- **`TableName_DataDestination`** — A hidden navigation query using `Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false])` that points to the target BronzeLH Delta table
- **`TableName`** — The source query with a `[DataDestinations = {...}]` attribute linking to the destination query

This pattern enables fully automated Lakehouse destinations without manual portal configuration.
Use `deploy/Update-DataflowDestinations.ps1` to re-apply destinations after editing dataflows in the portal.

Semantic Model TMDL files are already in `HorizonBooksAnalytics/` (PBIP format).

### Item Coverage

| Item Type | Count | Definition Location |
|---|---|---|
| Lakehouse | 3 | `definitions/lakehouses/` |
| Notebook | 3 | `definitions/notebooks/` (metadata) + `notebooks/` (source) |
| DataflowGen2 | 3 | `definitions/dataflows/` |
| DataPipeline | 1 | `definitions/pipeline/` |
| SemanticModel | 1 | `HorizonBooksAnalytics/` (PBIP/TMDL) |
| Report | 1 | `definitions/report/` |
| DataAgent | 1 | `definitions/dataagent/` |
| **Total** | **13** | |
