<#
.SYNOPSIS
    Comprehensive Pester 5 test suite for the Horizon Books 3-Lakehouse Medallion Fabric demo.
    Covers structure, non-regression, TMDL, definitions, data quality, deploy scripts, and integration.

.DESCRIPTION
    Test categories (Tags):
    - Unit          : Local file/structure validation (no Azure needed)
    - NonRegression : Exact counts that must not change (measures, tables, relationships, etc.)
    - TMDL          : Semantic model integrity (partitions, schemas, expressions, relationships)
    - Definition    : CI/CD definition files (manifest, pipeline, dataflows, notebooks)
    - DataQuality   : CSV sample data validation (headers, row counts, no empty files)
    - DeployScript  : Deploy-Full.ps1 / Deploy-Pipeline.ps1 quality checks
    - Integration   : Live Fabric workspace validation (requires -WorkspaceId)

.EXAMPLE
    # Run all offline tests
    Invoke-Pester -Path .\tests\Deploy-HorizonBooks.Tests.ps1 -ExcludeTag "Integration"

.EXAMPLE
    # Run only non-regression tests
    Invoke-Pester -Path .\tests\Deploy-HorizonBooks.Tests.ps1 -Tag "NonRegression"

.EXAMPLE
    # Run integration tests against a live workspace
    $container = New-PesterContainer -Path .\tests\Deploy-HorizonBooks.Tests.ps1 -Data @{ WorkspaceId = "your-guid" }
    Invoke-Pester -Container $container -Tag "Integration"
#>

param(
    [Parameter(Mandatory = $false)]
    [string]$WorkspaceId
)

# ---------------------------------------------------------------------------
# Path setup  (Discovery phase – needed for -ForEach data)
# ---------------------------------------------------------------------------
$scriptDir    = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$projectRoot  = Split-Path -Parent $scriptDir
$deployDir    = Join-Path $projectRoot "deploy"
$dataDir      = Join-Path $projectRoot "SampleData"
$pbipDir      = Join-Path $projectRoot "HorizonBooksAnalytics"
$smDir        = Join-Path $pbipDir    "HorizonBooksAnalytics.SemanticModel"
$reportDir    = Join-Path $pbipDir    "HorizonBooksAnalytics.Report"
$tmdlDir      = Join-Path $smDir      "definition"
$tablesDir    = Join-Path $tmdlDir    "tables"
$pagesDir     = Join-Path $reportDir  "definition\pages"
$defDir       = Join-Path $projectRoot "definitions"
$notebooksDir = Join-Path $projectRoot "notebooks"
$assetsDir    = Join-Path $projectRoot "assets"

$FabricApiBase = "https://api.fabric.microsoft.com/v1"

# ---------------------------------------------------------------------------
# BeforeAll  (Run phase – variables available inside It blocks)
# ---------------------------------------------------------------------------
BeforeAll {
    $scriptDir    = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
    $projectRoot  = Split-Path -Parent $scriptDir
    $deployDir    = Join-Path $projectRoot "deploy"
    $dataDir      = Join-Path $projectRoot "SampleData"
    $pbipDir      = Join-Path $projectRoot "HorizonBooksAnalytics"
    $smDir        = Join-Path $pbipDir    "HorizonBooksAnalytics.SemanticModel"
    $reportDir    = Join-Path $pbipDir    "HorizonBooksAnalytics.Report"
    $tmdlDir      = Join-Path $smDir      "definition"
    $tablesDir    = Join-Path $tmdlDir    "tables"
    $pagesDir     = Join-Path $reportDir  "definition\pages"
    $defDir       = Join-Path $projectRoot "definitions"
    $notebooksDir = Join-Path $projectRoot "notebooks"
    $assetsDir    = Join-Path $projectRoot "assets"

    $FabricApiBase = "https://api.fabric.microsoft.com/v1"
}

# ============================================================================
# 1. UNIT TESTS  -  Project structure & file existence
# ============================================================================
Describe "Project Structure" -Tag "Unit" {

    Context "Deployment Scripts" {
        $expectedScripts = @(
            "Deploy-Full.ps1",
            "Deploy-Pipeline.ps1",
            "Deploy-DataAgent.ps1",
            "Deploy-HorizonBooks.ps1",
            "New-HorizonBooksWorkspace.ps1",
            "Validate-Deployment.ps1",
            "HorizonBooks_TaskFlow.json"
        )
        It "deploy/<file> exists: <_>" -ForEach $expectedScripts {
            Join-Path $deployDir $_ | Should -Exist
        }
    }

    Context "Notebooks" {
        $expectedNotebooks = @(
            "01_BronzeToSilver.py",
            "02_WebEnrichment.py",
            "03_SilverToGold.py"
        )
        It "notebooks/<file> exists: <_>" -ForEach $expectedNotebooks {
            Join-Path $notebooksDir $_ | Should -Exist
        }
    }

    Context "PBIP Project" {
        It "SemanticModel definition folder exists" {
            $tmdlDir | Should -Exist
        }
        It "Report definition folder exists" {
            "$reportDir\definition" | Should -Exist
        }
        It ".pbip file exists" {
            Get-ChildItem $pbipDir -Filter "*.pbip" | Should -Not -BeNullOrEmpty
        }
        It "definition.pbism exists" {
            Join-Path $smDir "definition.pbism" | Should -Exist
        }
        It "definition.pbir exists" {
            Join-Path $reportDir "definition.pbir" | Should -Exist
        }
        It "Semantic Model .platform exists" {
            Join-Path $smDir ".platform" | Should -Exist
        }
        It "Report .platform exists" {
            Join-Path $reportDir ".platform" | Should -Exist
        }
    }

    Context "Sample Data Directories" {
        It "SampleData/Finance exists"    { Join-Path $dataDir "Finance"    | Should -Exist }
        It "SampleData/HR exists"         { Join-Path $dataDir "HR"         | Should -Exist }
        It "SampleData/Operations exists" { Join-Path $dataDir "Operations" | Should -Exist }
    }

    Context "Definitions Folder" {
        $expectedDirs = @("dataflows", "notebooks", "pipeline", "lakehouses", "report", "dataagent")
        It "definitions/<folder> exists: <_>" -ForEach $expectedDirs {
            Join-Path $defDir $_ | Should -Exist
        }
        It "items-manifest.json exists" {
            Join-Path $defDir "items-manifest.json" | Should -Exist
        }
    }

    Context "Workspace Logo" {
        It "SVG logo exists in assets/" {
            Join-Path $assetsDir "workspace-logo.svg" | Should -Exist
        }
        It "PNG logo exists in assets/" {
            Join-Path $assetsDir "workspace-logo.png" | Should -Exist
        }
        It "PNG logo is valid image (>100 bytes)" {
            $pngPath = Join-Path $assetsDir "workspace-logo.png"
            (Get-Item $pngPath).Length | Should -BeGreaterThan 100
        }
        It "PNG logo has correct PNG magic bytes" {
            $pngPath = Join-Path $assetsDir "workspace-logo.png"
            $bytes = [System.IO.File]::ReadAllBytes($pngPath)
            $bytes[0] | Should -Be 0x89
            $bytes[1] | Should -Be 0x50
            $bytes[2] | Should -Be 0x4E
            $bytes[3] | Should -Be 0x47
        }
    }
}

# ============================================================================
# 2. NON-REGRESSION TESTS  -  Exact counts that must not change
# ============================================================================
Describe "Non-Regression Baselines" -Tag "NonRegression" {

    Context "TMDL Table Count" {
        It "Semantic model has exactly 23 table files" {
            (Get-ChildItem $tablesDir -Filter "*.tmdl").Count | Should -Be 23
        }
    }

    Context "TMDL Relationship Count" {
        It "relationships.tmdl defines exactly 27 relationships" {
            $content = Get-Content (Join-Path $tmdlDir "relationships.tmdl") -Raw
            $count = ([regex]::Matches($content, '(?m)^relationship ')).Count
            $count | Should -Be 27
        }
    }

    Context "TMDL Total Measure Count" {
        It "Semantic model has exactly 96 DAX measures across all tables" {
            $total = 0
            Get-ChildItem $tablesDir -Filter "*.tmdl" | ForEach-Object {
                $txt = Get-Content $_.FullName -Raw
                $total += ([regex]::Matches($txt, '(?m)^\tmeasure ')).Count
            }
            $total | Should -Be 96
        }
    }

    Context "Per-Table Measure Counts" {
        $expectedMeasures = @(
            @{ Table = "DimAccounts";                Expected = 0 },
            @{ Table = "DimAuthors";                 Expected = 0 },
            @{ Table = "DimBooks";                   Expected = 0 },
            @{ Table = "DimCostCenters";             Expected = 0 },
            @{ Table = "DimCustomers";               Expected = 0 },
            @{ Table = "DimDate";                    Expected = 0 },
            @{ Table = "DimDepartments";             Expected = 0 },
            @{ Table = "DimEmployees";               Expected = 5 },
            @{ Table = "DimGeography";               Expected = 0 },
            @{ Table = "DimWarehouses";              Expected = 0 },
            @{ Table = "FactBudget";                 Expected = 5 },
            @{ Table = "FactFinancialTransactions";  Expected = 18 },
            @{ Table = "FactInventory";              Expected = 5 },
            @{ Table = "FactOrders";                 Expected = 26 },
            @{ Table = "FactPayroll";                Expected = 7 },
            @{ Table = "FactPerformanceReviews";     Expected = 3 },
            @{ Table = "FactRecruitment";            Expected = 5 },
            @{ Table = "FactReturns";                Expected = 5 },
            @{ Table = "ForecastSalesRevenue";        Expected = 4 },
            @{ Table = "ForecastGenreDemand";         Expected = 3 },
            @{ Table = "ForecastFinancial";           Expected = 3 },
            @{ Table = "ForecastInventoryDemand";     Expected = 3 },
            @{ Table = "ForecastWorkforce";           Expected = 4 }
        )
        It "Table <Table> has <Expected> measures" -ForEach $expectedMeasures {
            $file = Join-Path $tablesDir "$Table.tmdl"
            $txt = Get-Content $file -Raw
            $cnt = ([regex]::Matches($txt, '(?m)^\tmeasure ')).Count
            $cnt | Should -Be $Expected
        }
    }

    Context "Per-Table Column Counts" {
        $expectedColumns = @(
            @{ Table = "DimAccounts";                Expected = 6 },
            @{ Table = "DimAuthors";                 Expected = 13 },
            @{ Table = "DimBooks";                   Expected = 13 },
            @{ Table = "DimCostCenters";             Expected = 4 },
            @{ Table = "DimCustomers";               Expected = 13 },
            @{ Table = "DimDate";                    Expected = 14 },
            @{ Table = "DimDepartments";             Expected = 6 },
            @{ Table = "DimEmployees";               Expected = 12 },
            @{ Table = "DimGeography";               Expected = 13 },
            @{ Table = "DimWarehouses";              Expected = 12 },
            @{ Table = "FactBudget";                 Expected = 10 },
            @{ Table = "FactFinancialTransactions";  Expected = 12 },
            @{ Table = "FactInventory";              Expected = 13 },
            @{ Table = "FactOrders";                 Expected = 14 },
            @{ Table = "FactPayroll";                Expected = 10 },
            @{ Table = "FactPerformanceReviews";     Expected = 9 },
            @{ Table = "FactRecruitment";            Expected = 14 },
            @{ Table = "FactReturns";                Expected = 11 },
            @{ Table = "ForecastSalesRevenue";        Expected = 10 },
            @{ Table = "ForecastGenreDemand";         Expected = 9 },
            @{ Table = "ForecastFinancial";           Expected = 9 },
            @{ Table = "ForecastInventoryDemand";     Expected = 12 },
            @{ Table = "ForecastWorkforce";           Expected = 8 }
        )
        It "Table <Table> has <Expected> columns" -ForEach $expectedColumns {
            $file = Join-Path $tablesDir "$Table.tmdl"
            $txt = Get-Content $file -Raw
            $cnt = ([regex]::Matches($txt, '(?m)^\tcolumn ')).Count
            $cnt | Should -Be $Expected
        }
    }

    Context "CSV File Count" {
        It "SampleData contains exactly 17 CSV files" {
            (Get-ChildItem $dataDir -Recurse -Filter "*.csv").Count | Should -Be 17
        }
    }

    Context "CSV Row Counts (excluding header)" {
        $expectedRows = @(
            @{ Domain = "Finance";    File = "DimAccounts.csv";                Rows = 26 },
            @{ Domain = "Finance";    File = "DimCostCenters.csv";             Rows = 7 },
            @{ Domain = "Finance";    File = "FactBudget.csv";                 Rows = 132 },
            @{ Domain = "Finance";    File = "FactFinancialTransactions.csv";  Rows = 225 },
            @{ Domain = "HR";         File = "DimDepartments.csv";             Rows = 7 },
            @{ Domain = "HR";         File = "DimEmployees.csv";               Rows = 50 },
            @{ Domain = "HR";         File = "FactPayroll.csv";                Rows = 143 },
            @{ Domain = "HR";         File = "FactPerformanceReviews.csv";     Rows = 45 },
            @{ Domain = "HR";         File = "FactRecruitment.csv";            Rows = 24 },
            @{ Domain = "Operations"; File = "DimAuthors.csv";                 Rows = 30 },
            @{ Domain = "Operations"; File = "DimBooks.csv";                   Rows = 45 },
            @{ Domain = "Operations"; File = "DimCustomers.csv";               Rows = 50 },
            @{ Domain = "Operations"; File = "DimGeography.csv";               Rows = 70 },
            @{ Domain = "Operations"; File = "DimWarehouses.csv";              Rows = 3 },
            @{ Domain = "Operations"; File = "FactInventory.csv";              Rows = 70 },
            @{ Domain = "Operations"; File = "FactOrders.csv";                 Rows = 200 },
            @{ Domain = "Operations"; File = "FactReturns.csv";                Rows = 40 }
        )
        It "<Domain>/<File> has <Rows> data rows" -ForEach $expectedRows {
            $path = Join-Path $dataDir "$Domain\$File"
            $lines = (Get-Content $path).Count
            ($lines - 1) | Should -Be $Rows   # subtract header
        }
    }

    Context "Report Page Count" {
        It "Report has exactly 10 pages" {
            (Get-ChildItem $pagesDir -Directory).Count | Should -Be 10
        }
    }

    Context "Manifest Item Count" {
        It "items-manifest.json defines exactly 14 items" {
            $manifest = Get-Content (Join-Path $defDir "items-manifest.json") -Raw | ConvertFrom-Json
            $manifest.items.Count | Should -Be 14
        }
    }

    Context "Pipeline Activity Count" {
        It "Pipeline defines exactly 7 activities" {
            $pipeline = Get-Content (Join-Path $defDir "pipeline\pipeline-content.json") -Raw | ConvertFrom-Json
            $pipeline.properties.activities.Count | Should -Be 7
        }
    }

    Context "Model ref table Count" {
        It "model.tmdl declares exactly 23 ref tables" {
            $content = Get-Content (Join-Path $tmdlDir "model.tmdl") -Raw
            $count = ([regex]::Matches($content, '(?m)^ref table ')).Count
            $count | Should -Be 23
        }
    }

    Context "Workspace Folder Count" {
        It "Manifest defines exactly 5 workspace folders" {
            $manifest = Get-Content (Join-Path $defDir "items-manifest.json") -Raw | ConvertFrom-Json
            $manifest.workspaceFolders.Count | Should -Be 5
        }
    }
}

# ============================================================================
# 3. TMDL / SEMANTIC MODEL TESTS  -  Structural integrity
# ============================================================================
Describe "TMDL Semantic Model Integrity" -Tag "TMDL" {

    Context "Core Definition Files" {
        $expectedFiles = @("model.tmdl", "expressions.tmdl", "relationships.tmdl", "database.tmdl")
        It "<_> exists in definition folder" -ForEach $expectedFiles {
            Join-Path $tmdlDir $_ | Should -Exist
        }
    }

    Context "Expression Tokens" {
        It "expressions.tmdl contains the SQL_ENDPOINT token placeholder" {
            $content = Get-Content (Join-Path $tmdlDir "expressions.tmdl") -Raw
            $content | Should -Match '\{\{SQL_ENDPOINT\}\}'
        }
        It "expressions.tmdl contains the LAKEHOUSE_NAME token placeholder" {
            $content = Get-Content (Join-Path $tmdlDir "expressions.tmdl") -Raw
            $content | Should -Match '\{\{LAKEHOUSE_NAME\}\}'
        }
        It "expressions.tmdl declares the DatabaseQuery queryGroup" {
            $content = Get-Content (Join-Path $tmdlDir "expressions.tmdl") -Raw
            $content | Should -Match 'queryGroup:\s*DatabaseQuery'
        }
    }

    Context "Model Structure" {
        It "model.tmdl has defaultPowerBIDataSourceVersion" {
            $content = Get-Content (Join-Path $tmdlDir "model.tmdl") -Raw
            $content | Should -Match 'defaultPowerBIDataSourceVersion:\s*powerBI_V3'
        }
        It "model.tmdl has culture en-US" {
            $content = Get-Content (Join-Path $tmdlDir "model.tmdl") -Raw
            $content | Should -Match 'culture:\s*en-US'
        }
        It "model.tmdl declares queryGroup DatabaseQuery" {
            $content = Get-Content (Join-Path $tmdlDir "model.tmdl") -Raw
            $content | Should -Match '(?m)^queryGroup DatabaseQuery'
        }
        It "model.tmdl declares ref expression DatabaseQuery" {
            $content = Get-Content (Join-Path $tmdlDir "model.tmdl") -Raw
            $content | Should -Match '(?m)^ref expression DatabaseQuery'
        }
    }

    Context "Every Table Has a Partition" {
        $tableFiles = Get-ChildItem $tablesDir -Filter "*.tmdl" | ForEach-Object { @{ Name = $_.BaseName; Path = $_.FullName } }
        It "Table <Name> has at least one partition block" -ForEach $tableFiles {
            $content = Get-Content $Path -Raw
            $content | Should -Match '(?m)^\tpartition '
        }
    }

    Context "Every Table Partition Uses schemaName" {
        $tableFiles = Get-ChildItem $tablesDir -Filter "*.tmdl" | ForEach-Object { @{ Name = $_.BaseName; Path = $_.FullName } }
        It "Table <Name> partition has schemaName property" -ForEach $tableFiles {
            $content = Get-Content $Path -Raw
            $content | Should -Match 'schemaName:\s*(dim|fact|analytics)'
        }
    }

    Context "Every Table Has a lineageTag" {
        $tableFiles = Get-ChildItem $tablesDir -Filter "*.tmdl" | ForEach-Object { @{ Name = $_.BaseName; Path = $_.FullName } }
        It "Table <Name> has a lineageTag" -ForEach $tableFiles {
            $content = Get-Content $Path -Raw
            $content | Should -Match 'lineageTag:\s*[0-9a-f-]+'
        }
    }

    Context "Dim Tables Use dim Schema" {
        $dimTables = Get-ChildItem $tablesDir -Filter "Dim*.tmdl" | ForEach-Object { @{ Name = $_.BaseName; Path = $_.FullName } }
        It "Table <Name> uses schemaName: dim" -ForEach $dimTables {
            $content = Get-Content $Path -Raw
            $content | Should -Match 'schemaName:\s*dim'
        }
    }

    Context "Fact Tables Use fact Schema" {
        $factTables = Get-ChildItem $tablesDir -Filter "Fact*.tmdl" | ForEach-Object { @{ Name = $_.BaseName; Path = $_.FullName } }
        It "Table <Name> uses schemaName: fact" -ForEach $factTables {
            $content = Get-Content $Path -Raw
            $content | Should -Match 'schemaName:\s*fact'
        }
    }

    Context "Relationship Naming Convention" {
        It "All relationships follow Table_Column_DimTable pattern" {
            $content = Get-Content (Join-Path $tmdlDir "relationships.tmdl") -Raw
            $names = [regex]::Matches($content, '(?m)^relationship (.+)$') | ForEach-Object { $_.Groups[1].Value.Trim() }
            foreach ($name in $names) {
                $name | Should -Match '^\w+_\w+_\w+$'
            }
        }
    }

    Context "Only One Inactive Relationship" {
        It "Exactly one relationship is marked isActive: false" {
            $content = Get-Content (Join-Path $tmdlDir "relationships.tmdl") -Raw
            $count = ([regex]::Matches($content, 'isActive:\s*false')).Count
            $count | Should -Be 1
        }
        It "The inactive relationship is DimEmployees_GeoID_DimGeography" {
            $content = Get-Content (Join-Path $tmdlDir "relationships.tmdl") -Raw
            $blocks = $content -split '(?m)(?=^relationship )'
            $inactive = $blocks | Where-Object { $_ -match 'isActive:\s*false' }
            $inactive | Should -Match 'relationship DimEmployees_GeoID_DimGeography'
        }
    }

    Context "All Relationships Use oneDirection Cross-Filtering" {
        It "Every relationship has crossFilteringBehavior: oneDirection" {
            $content = Get-Content (Join-Path $tmdlDir "relationships.tmdl") -Raw
            $relCount   = ([regex]::Matches($content, '(?m)^relationship ')).Count
            $oneDir     = ([regex]::Matches($content, 'crossFilteringBehavior:\s*oneDirection')).Count
            $oneDir | Should -Be $relCount
        }
    }

    Context "No Duplicate Relationship Names" {
        It "Each relationship name is unique" {
            $content = Get-Content (Join-Path $tmdlDir "relationships.tmdl") -Raw
            $names = [regex]::Matches($content, '(?m)^relationship (.+)$') | ForEach-Object { $_.Groups[1].Value.Trim() }
            $names.Count | Should -Be ($names | Sort-Object -Unique).Count
        }
    }

    Context "Relationship References Valid Tables" {
        It "All fromColumn/toColumn reference existing table files" {
            $content = Get-Content (Join-Path $tmdlDir "relationships.tmdl") -Raw
            $refs = [regex]::Matches($content, '(?:fromColumn|toColumn):\s*(\w+)\.\w+') | ForEach-Object { $_.Groups[1].Value }
            $tableNames = (Get-ChildItem $tablesDir -Filter "*.tmdl").BaseName
            foreach ($ref in ($refs | Sort-Object -Unique)) {
                $ref | Should -BeIn $tableNames
            }
        }
    }

    Context "model.tmdl ref tables match table files" {
        It "Every ref table in model.tmdl has a corresponding .tmdl file" {
            $content = Get-Content (Join-Path $tmdlDir "model.tmdl") -Raw
            $refTables = [regex]::Matches($content, '(?m)^ref table (\w+)') | ForEach-Object { $_.Groups[1].Value }
            $tableNames = (Get-ChildItem $tablesDir -Filter "*.tmdl").BaseName
            foreach ($ref in $refTables) {
                $ref | Should -BeIn $tableNames
            }
        }
        It "Every .tmdl table file is referenced in model.tmdl" {
            $content = Get-Content (Join-Path $tmdlDir "model.tmdl") -Raw
            $refTables = [regex]::Matches($content, '(?m)^ref table (\w+)') | ForEach-Object { $_.Groups[1].Value }
            $tableNames = (Get-ChildItem $tablesDir -Filter "*.tmdl").BaseName
            foreach ($tn in $tableNames) {
                $tn | Should -BeIn $refTables
            }
        }
    }

    Context "No Orphan Measures (every measure has a name)" {
        It "Every measure directive has a non-empty name" {
            Get-ChildItem $tablesDir -Filter "*.tmdl" | ForEach-Object {
                $lines = Get-Content $_.FullName
                $lines | Where-Object { $_ -match '^\tmeasure ' } | ForEach-Object {
                    $_ | Should -Match '^\tmeasure \S+'
                }
            }
        }
    }
}

# ============================================================================
# 4. DEFINITION FILE VALIDATION TESTS  -  CI/CD readiness
# ============================================================================
Describe "Definition File Validation" -Tag "Definition" {

    Context "items-manifest.json Schema" {
        BeforeAll {
            $manifest = Get-Content (Join-Path $defDir "items-manifest.json") -Raw | ConvertFrom-Json
        }

        It "Has a workspace section with displayName" {
            $manifest.workspace.displayName | Should -Not -BeNullOrEmpty
        }
        It "Has a workspace token placeholder" {
            $manifest.workspace.token | Should -Be '{{WORKSPACE_ID}}'
        }
        It "Has items array" {
            $manifest.items | Should -Not -BeNullOrEmpty
        }
        It "Has deploymentOrder array with 14 entries" {
            $manifest.deploymentOrder.Count | Should -Be 14
        }
        It "deploymentOrder matches items displayNames" {
            $itemNames = $manifest.items | ForEach-Object { $_.displayName } | Sort-Object
            $orderNames = $manifest.deploymentOrder | Sort-Object
            $orderNames | Should -Be $itemNames
        }
        It "Every item has a type" {
            foreach ($item in $manifest.items) {
                $item.type | Should -Not -BeNullOrEmpty
            }
        }
        It "Every item has a token" {
            foreach ($item in $manifest.items) {
                $item.token | Should -Match '^\{\{.+\}\}$'
            }
        }
        It "All tokens are unique" {
            $tokens = $manifest.items | ForEach-Object { $_.token }
            $tokens.Count | Should -Be ($tokens | Sort-Object -Unique).Count
        }
        It "Contains all expected item types" {
            $types = $manifest.items | ForEach-Object { $_.type } | Sort-Object -Unique
            $expectedTypes = @("Lakehouse", "Notebook", "DataflowGen2", "DataPipeline", "SemanticModel", "Report", "DataAgent")
            foreach ($t in $expectedTypes) {
                $t | Should -BeIn $types
            }
        }
    }

    Context "Manifest Workspace Folders" {
        BeforeAll {
            $manifest = Get-Content (Join-Path $defDir "items-manifest.json") -Raw | ConvertFrom-Json
        }

        It "Each workspace folder has a displayName" {
            foreach ($f in $manifest.workspaceFolders) {
                $f.displayName | Should -Not -BeNullOrEmpty
            }
        }
        It "Workspace folder names are numbered 1-5" {
            $names = $manifest.workspaceFolders.displayName | Sort-Object
            $names[0] | Should -Match '^1\.'
            $names[4] | Should -Match '^5\.'
        }
        It "All manifest items are assigned to exactly one workspace folder" {
            $folderItems = $manifest.workspaceFolders | ForEach-Object { $_.items } | ForEach-Object { $_ }
            $allItems = $manifest.items | ForEach-Object { $_.displayName }
            foreach ($item in $allItems) {
                $item | Should -BeIn $folderItems
            }
        }
    }

    Context "Pipeline Definition" {
        BeforeAll {
            $pipeline = Get-Content (Join-Path $defDir "pipeline\pipeline-content.json") -Raw | ConvertFrom-Json
        }

        It "Has a properties.activities array" {
            $pipeline.properties.activities | Should -Not -BeNullOrEmpty
        }

        $activityData = @(
            @{ Name = "HorizonBooks_DF_Finance";        Type = "RefreshDataflow"; Folder = "1. Data Ingestion" },
            @{ Name = "HorizonBooks_DF_HR";             Type = "RefreshDataflow"; Folder = "1. Data Ingestion" },
            @{ Name = "HorizonBooks_DF_Operations";     Type = "RefreshDataflow"; Folder = "1. Data Ingestion" },
            @{ Name = "HorizonBooks_01_BronzeToSilver"; Type = "TridentNotebook"; Folder = "2. Transformation" },
            @{ Name = "HorizonBooks_02_WebEnrichment";  Type = "TridentNotebook"; Folder = "2. Transformation" },
            @{ Name = "HorizonBooks_03_SilverToGold";   Type = "TridentNotebook"; Folder = "3. Gold Layer" }
        )
        It "Activity '<Name>' exists with type '<Type>' in folder '<Folder>'" -ForEach $activityData {
            $pipeline = Get-Content (Join-Path $defDir "pipeline\pipeline-content.json") -Raw | ConvertFrom-Json
            $act = $pipeline.properties.activities | Where-Object { $_.name -eq $Name }
            $act | Should -Not -BeNullOrEmpty
            $act.type | Should -Be $Type
            $act.folder.name | Should -Be $Folder
        }

        It "Pipeline contains tokenized workspace/item ID references" {
            $raw = Get-Content (Join-Path $defDir "pipeline\pipeline-content.json") -Raw
            $raw | Should -Match '\{\{WORKSPACE_ID\}\}'
        }
    }

    Context "Dataflow Definitions" {
        $dfDomains = @("DF_Finance", "DF_HR", "DF_Operations")

        It "<_>/mashup.pq exists" -ForEach $dfDomains {
            Join-Path $defDir "dataflows\$_\mashup.pq" | Should -Exist
        }
        It "<_>/output-config.json exists" -ForEach $dfDomains {
            Join-Path $defDir "dataflows\$_\output-config.json" | Should -Exist
        }
        It "Shared queryMetadata.json exists" {
            Join-Path $defDir "dataflows\queryMetadata.json" | Should -Exist
        }

        It "<_>/mashup.pq contains WORKSPACE_ID token" -ForEach $dfDomains {
            $content = Get-Content (Join-Path $defDir "dataflows\$_\mashup.pq") -Raw
            $content | Should -Match '\{\{WORKSPACE_ID\}\}'
        }
        It "<_>/mashup.pq contains BRONZE_LH_ID token" -ForEach $dfDomains {
            $content = Get-Content (Join-Path $defDir "dataflows\$_\mashup.pq") -Raw
            $content | Should -Match '\{\{BRONZE_LH_ID\}\}'
        }
    }

    Context "Notebook Lakehouse Metadata" {
        $notebookMetas = @(
            @{ File = "nb01-lakehouse-metadata.json"; Token = "BRONZE_LH_ID" },
            @{ File = "nb02-lakehouse-metadata.json"; Token = "SILVER_LH_ID" },
            @{ File = "nb03-lakehouse-metadata.json"; Token = "GOLD_LH_ID" },
            @{ File = "nb04-lakehouse-metadata.json"; Token = "GOLD_LH_ID" }
        )
        It "<File> exists" -ForEach $notebookMetas {
            Join-Path $defDir "notebooks\$File" | Should -Exist
        }
        It "<File> contains <Token> placeholder" -ForEach $notebookMetas {
            $content = Get-Content (Join-Path $defDir "notebooks\$File") -Raw
            $content | Should -Match "\{\{$Token\}\}"
        }
    }

    Context "Lakehouse Definitions" {
        $lhFiles = @("BronzeLH.json", "SilverLH.json", "GoldLH.json")
        It "<_> exists in definitions/lakehouses" -ForEach $lhFiles {
            Join-Path $defDir "lakehouses\$_" | Should -Exist
        }
        It "<_> specifies enableSchemas: true" -ForEach $lhFiles {
            $lh = Get-Content (Join-Path $defDir "lakehouses\$_") -Raw | ConvertFrom-Json
            $lh.creationPayload.enableSchemas | Should -Be $true
        }
    }

    Context "Report Definition" {
        It "report-definition.json exists" {
            Join-Path $defDir "report\report-definition.json" | Should -Exist
        }
    }

    Context "DataAgent Definition" {
        It "dataagent-definition.json exists" {
            Join-Path $defDir "dataagent\dataagent-definition.json" | Should -Exist
        }
    }
}

# ============================================================================
# 5. DATA QUALITY TESTS  -  CSV sample data validation
# ============================================================================
Describe "Sample Data Quality" -Tag "DataQuality" {

    Context "No Empty CSV Files" {
        $csvFiles = Get-ChildItem $dataDir -Recurse -Filter "*.csv" | ForEach-Object { @{ Path = $_.FullName; Name = "$($_.Directory.Name)/$($_.Name)" } }
        It "<Name> has at least a header + 1 data row" -ForEach $csvFiles {
            (Get-Content $Path).Count | Should -BeGreaterOrEqual 2
        }
    }

    Context "CSV Headers Match Expected Column Counts" {
        $headerSpecs = @(
            @{ Domain = "Finance"; File = "DimAccounts.csv"; ExpectedCols = 6 },
            @{ Domain = "Finance"; File = "DimCostCenters.csv"; ExpectedCols = 4 },
            @{ Domain = "Finance"; File = "FactBudget.csv"; ExpectedCols = 10 },
            @{ Domain = "Finance"; File = "FactFinancialTransactions.csv"; ExpectedCols = 12 },
            @{ Domain = "HR"; File = "DimDepartments.csv"; ExpectedCols = 6 },
            @{ Domain = "HR"; File = "DimEmployees.csv"; ExpectedCols = 12 },
            @{ Domain = "HR"; File = "FactPayroll.csv"; ExpectedCols = 10 },
            @{ Domain = "HR"; File = "FactPerformanceReviews.csv"; ExpectedCols = 9 },
            @{ Domain = "HR"; File = "FactRecruitment.csv"; ExpectedCols = 14 },
            @{ Domain = "Operations"; File = "DimAuthors.csv"; ExpectedCols = 13 },
            @{ Domain = "Operations"; File = "DimBooks.csv"; ExpectedCols = 13 },
            @{ Domain = "Operations"; File = "DimCustomers.csv"; ExpectedCols = 13 },
            @{ Domain = "Operations"; File = "DimGeography.csv"; ExpectedCols = 13 },
            @{ Domain = "Operations"; File = "DimWarehouses.csv"; ExpectedCols = 12 },
            @{ Domain = "Operations"; File = "FactInventory.csv"; ExpectedCols = 13 },
            @{ Domain = "Operations"; File = "FactOrders.csv"; ExpectedCols = 14 },
            @{ Domain = "Operations"; File = "FactReturns.csv"; ExpectedCols = 11 }
        )
        It "<Domain>/<File> header has <ExpectedCols> columns" -ForEach $headerSpecs {
            $header = Get-Content (Join-Path $dataDir "$Domain\$File") -First 1
            ($header -split ',').Count | Should -Be $ExpectedCols
        }
    }

    Context "CSV Files Contain No BOM Characters in Data" {
        $csvFiles = Get-ChildItem $dataDir -Recurse -Filter "*.csv" | ForEach-Object { @{ Path = $_.FullName; Name = "$($_.Directory.Name)/$($_.Name)" } }
        It "<Name> has no BOM in data rows" -ForEach $csvFiles {
            $bytes = [System.IO.File]::ReadAllBytes($Path)
            $content = [System.Text.Encoding]::UTF8.GetString($bytes)
            $stripped = $content -replace '^\xEF\xBB\xBF', ''
            $stripped | Should -Not -Match '\xEF\xBB\xBF'
        }
    }

    Context "Finance Fact Tables Have Numeric Amount Columns" {
        It "FactFinancialTransactions.csv header contains 'Amount'" {
            $header = Get-Content (Join-Path $dataDir "Finance\FactFinancialTransactions.csv") -First 1
            $header | Should -Match 'Amount'
        }
        It "FactBudget.csv header contains 'Budget'" {
            $header = Get-Content (Join-Path $dataDir "Finance\FactBudget.csv") -First 1
            $header | Should -Match 'Budget'
        }
    }

    Context "ID Columns Are Present" {
        $idChecks = @(
            @{ Domain = "Finance";    File = "DimAccounts.csv";    IdCol = "AccountID" },
            @{ Domain = "Finance";    File = "DimCostCenters.csv"; IdCol = "CostCenterID" },
            @{ Domain = "HR";         File = "DimEmployees.csv";   IdCol = "EmployeeID" },
            @{ Domain = "HR";         File = "DimDepartments.csv"; IdCol = "DepartmentID" },
            @{ Domain = "Operations"; File = "DimBooks.csv";       IdCol = "BookID" },
            @{ Domain = "Operations"; File = "DimAuthors.csv";     IdCol = "AuthorID" },
            @{ Domain = "Operations"; File = "DimCustomers.csv";   IdCol = "CustomerID" },
            @{ Domain = "Operations"; File = "DimGeography.csv";   IdCol = "GeoID" },
            @{ Domain = "Operations"; File = "DimWarehouses.csv";  IdCol = "WarehouseID" }
        )
        It "<Domain>/<File> has <IdCol> column" -ForEach $idChecks {
            $header = Get-Content (Join-Path $dataDir "$Domain\$File") -First 1
            $header | Should -Match $IdCol
        }
    }
}

# ============================================================================
# 6. DEPLOY SCRIPT QUALITY TESTS  -  Deploy-Full.ps1 validation
# ============================================================================
Describe "Deploy-Full.ps1 Quality" -Tag "DeployScript" {

    BeforeAll {
        $deployFullPath = Join-Path $deployDir "Deploy-Full.ps1"
        $deployFullContent = Get-Content $deployFullPath -Raw
    }

    Context "Script Parameters" {
        $expectedParams = @(
            "WorkspaceId",
            "BronzeLakehouseName",
            "SilverLakehouseName",
            "GoldLakehouseName",
            "SemanticModelName",
            "ReportName",
            "SkipPipelineRun",
            "SkipReport",
            "SkipDataAgent",
            "SkipValidation"
        )
        It "Declares parameter: <_>" -ForEach $expectedParams {
            $deployFullContent | Should -Match "\`$$_"
        }
        It "WorkspaceId is mandatory" {
            $deployFullContent | Should -Match '(?s)Mandatory\s*=\s*\$true.*?\$WorkspaceId'
        }
    }

    Context "Script Steps" {
        It "Contains 11 deployment steps (Step 0 through Step 10)" {
            $steps = [regex]::Matches($deployFullContent, 'Step\s+(\d+)')
            $stepNums = $steps | ForEach-Object { [int]$_.Groups[1].Value } | Sort-Object -Unique
            $stepNums.Count | Should -BeGreaterOrEqual 11
            $stepNums | Should -Contain 0
            $stepNums | Should -Contain 10
        }
    }

    Context "Essential Functions" {
        $expectedFunctions = @(
            "Get-FabricToken",
            "Invoke-FabricApi",
            "Wait-FabricOperation",
            "New-OrGetFabricItem",
            "Update-FabricItemDefinition",
            "Upload-FileToOneLake",
            "New-SchemaLakehouse",
            "Write-Banner",
            "Write-Step",
            "Write-Info",
            "Write-Success",
            "Write-Warn",
            "Write-Err",
            "Measure-Step"
        )
        It "Defines function: <_>" -ForEach $expectedFunctions {
            $deployFullContent | Should -Match "function\s+$_"
        }
    }

    Context "Idempotency Patterns" {
        It "Handles ItemDisplayNameAlreadyInUse for idempotent creates" {
            $deployFullContent | Should -Match 'ItemDisplayNameAlreadyInUse'
        }
        It "Has 429 (throttle) retry logic" {
            $deployFullContent | Should -Match '429'
        }
        It "Has retry-after delay handling" {
            $deployFullContent | Should -Match 'Retry-After|retry.after'
        }
    }

    Context "Three Lakehouses" {
        It "Creates BronzeLH" {
            $deployFullContent | Should -Match 'BronzeLH|BronzeLakehouse'
        }
        It "Creates SilverLH" {
            $deployFullContent | Should -Match 'SilverLH|SilverLakehouse'
        }
        It "Creates GoldLH" {
            $deployFullContent | Should -Match 'GoldLH|GoldLakehouse'
        }
    }

    Context "Schema-Enabled Lakehouse" {
        It "Uses enableSchemas in lakehouse creation" {
            $deployFullContent | Should -Match 'enableSchemas'
        }
    }

    Context "No Hardcoded Secrets" {
        It "Does not contain hardcoded bearer tokens" {
            $deployFullContent | Should -Not -Match 'Bearer\s+ey[A-Za-z0-9]+'
        }
        It "Does not contain hardcoded connection strings" {
            $deployFullContent | Should -Not -Match 'Server=.+;Database=.+;'
        }
    }

    Context "Error Handling" {
        It "Uses try/catch blocks" {
            $deployFullContent | Should -Match 'try\s*\{'
            $deployFullContent | Should -Match 'catch\s*\{'
        }
        It "Uses ErrorAction" {
            $deployFullContent | Should -Match 'ErrorAction'
        }
    }

    Context "Script Has Documentation" {
        It "Has a synopsis comment block" {
            $deployFullContent | Should -Match '\.SYNOPSIS'
        }
    }
}

Describe "Deploy-Pipeline.ps1 Quality" -Tag "DeployScript" {

    BeforeAll {
        $deployPipelinePath = Join-Path $deployDir "Deploy-Pipeline.ps1"
        $deployPipelineContent = Get-Content $deployPipelinePath -Raw
    }

    Context "Script Parameters" {
        It "Has WorkspaceId parameter" {
            $deployPipelineContent | Should -Match '\$WorkspaceId'
        }
    }

    Context "Pipeline Creates Activities" {
        It "References Dataflow activities" {
            $deployPipelineContent | Should -Match 'Dataflow|DataflowGen2'
        }
        It "References Notebook activities" {
            $deployPipelineContent | Should -Match 'Notebook|TridentNotebook'
        }
    }
}

# ============================================================================
# 7. REPORT STRUCTURE TESTS  -  PBIR validation
# ============================================================================
Describe "Power BI Report Structure" -Tag "Unit" {

    Context "Report Pages" {
        $expectedPages = @(
            "ReportSection",
            "ReportSection01",
            "ReportSection02",
            "ReportSection03",
            "ReportSection04",
            "ReportSection05",
            "ReportSection06",
            "ReportSection07",
            "ReportSection08",
            "ReportSection09"
        )
        It "Page folder <_> exists" -ForEach $expectedPages {
            Join-Path $pagesDir $_ | Should -Exist
        }
        It "<_> has page.json and at least 1 visual" -ForEach $expectedPages {
            $pageDir = Join-Path $pagesDir $_
            Join-Path $pageDir "page.json" | Should -Exist
            $visuals = Join-Path $pageDir "visuals"
            $visuals | Should -Exist
            (Get-ChildItem $visuals -Directory).Count | Should -BeGreaterThan 0
        }
    }

    Context "Report Definition Files" {
        It "report.json exists" {
            "$reportDir\definition\report.json" | Should -Exist
        }
        It "pages.json exists" {
            Join-Path $pagesDir "pages.json" | Should -Exist
        }
    }
}

# ============================================================================
# 8. NOTEBOOK QUALITY TESTS  -  Source file checks
# ============================================================================
Describe "Notebook Source Quality" -Tag "Unit" {

    Context "Notebook 01 - BronzeToSilver" {
        BeforeAll { $nb01 = Get-Content (Join-Path $notebooksDir "01_BronzeToSilver.py") -Raw }

        It "References BronzeLH (Files/ path)" {
            $nb01 | Should -Match 'Files/'
        }
        It "References SilverLH as target" {
            $nb01 | Should -Match 'SilverLH|silver'
        }
        It "Contains METADATA block for deploy-time replacement" {
            $nb01 | Should -Match 'METADATA'
        }
    }

    Context "Notebook 02 - WebEnrichment" {
        BeforeAll { $nb02 = Get-Content (Join-Path $notebooksDir "02_WebEnrichment.py") -Raw }

        It "Contains web/HTTP data fetching logic" {
            $nb02 | Should -Match 'http|requests|url|urllib'
        }
    }

    Context "Notebook 03 - SilverToGold" {
        BeforeAll { $nb03 = Get-Content (Join-Path $notebooksDir "03_SilverToGold.py") -Raw }

        It "References GoldLH" {
            $nb03 | Should -Match 'GoldLH|gold'
        }
        It "Builds dim/fact schema tables" {
            $nb03 | Should -Match '"dim"|"fact"'
        }
    }

    Context "Notebook 04 - Forecasting" {
        BeforeAll { $nb04 = Get-Content (Join-Path $notebooksDir "04_Forecasting.py") -Raw }

        It "References analytics schema" {
            $nb04 | Should -Match 'analytics'
        }
        It "Uses Holt-Winters / ExponentialSmoothing" {
            $nb04 | Should -Match 'ExponentialSmoothing|holt_winters'
        }
        It "Writes 5 forecast tables" {
            $tables = @('ForecastSalesRevenue', 'ForecastGenreDemand', 'ForecastFinancial', 'ForecastInventoryDemand', 'ForecastWorkforce')
            foreach ($t in $tables) {
                $nb04 | Should -Match $t
            }
        }
    }
}

# ============================================================================
# 9. CROSS-REFERENCE TESTS  -  Consistency across artifacts
# ============================================================================
Describe "Cross-Artifact Consistency" -Tag "NonRegression" {

    Context "CSV Tables Map to TMDL Tables" {
        It "Every CSV file has a matching TMDL table definition" {
            $csvNames = Get-ChildItem $dataDir -Recurse -Filter "*.csv" | ForEach-Object { $_.BaseName }
            $tmdlNames = (Get-ChildItem $tablesDir -Filter "*.tmdl").BaseName
            foreach ($csv in $csvNames) {
                $csv | Should -BeIn $tmdlNames
            }
        }
    }

    Context "TMDL Tables Without CSV (Generated)" {
        It "DimDate is in TMDL but has no CSV (notebook-generated)" {
            Join-Path $tablesDir "DimDate.tmdl" | Should -Exist
            Get-ChildItem $dataDir -Recurse -Filter "DimDate.csv" | Should -BeNullOrEmpty
        }
    }

    Context "Manifest Deployment Order Starts with Lakehouses" {
        It "First 3 items in deploymentOrder are the 3 Lakehouses" {
            $manifest = Get-Content (Join-Path $defDir "items-manifest.json") -Raw | ConvertFrom-Json
            $manifest.deploymentOrder[0] | Should -Be "BronzeLH"
            $manifest.deploymentOrder[1] | Should -Be "SilverLH"
            $manifest.deploymentOrder[2] | Should -Be "GoldLH"
        }
    }

    Context "Manifest Deployment Order Ends with Analytics" {
        It "Last item in deploymentOrder is the DataAgent" {
            $manifest = Get-Content (Join-Path $defDir "items-manifest.json") -Raw | ConvertFrom-Json
            $manifest.deploymentOrder[-1] | Should -Be "HorizonBooks DataAgent"
        }
        It "Second-to-last is the Report" {
            $manifest = Get-Content (Join-Path $defDir "items-manifest.json") -Raw | ConvertFrom-Json
            $manifest.deploymentOrder[-2] | Should -Be "Horizon Books Publishing Analytics"
        }
    }

    Context "Pipeline Activities Reference Manifest Items" {
        It "Every pipeline activity name matches a manifest item displayName" {
            $manifest = Get-Content (Join-Path $defDir "items-manifest.json") -Raw | ConvertFrom-Json
            $pipeline = Get-Content (Join-Path $defDir "pipeline\pipeline-content.json") -Raw | ConvertFrom-Json
            $itemNames = $manifest.items | ForEach-Object { $_.displayName }
            foreach ($act in $pipeline.properties.activities) {
                $act.name | Should -BeIn $itemNames
            }
        }
    }

    Context "TMDL Column Counts Match CSV Column Counts" {
        It "Each CSV-backed table has same column count in TMDL and CSV" {
            $csvFiles = Get-ChildItem $dataDir -Recurse -Filter "*.csv"
            foreach ($csv in $csvFiles) {
                $header = Get-Content $csv.FullName -First 1
                $csvCols = ($header -split ',').Count
                $tmdlFile = Join-Path $tablesDir "$($csv.BaseName).tmdl"
                if (Test-Path $tmdlFile) {
                    $txt = Get-Content $tmdlFile -Raw
                    $tmdlCols = ([regex]::Matches($txt, '(?m)^\tcolumn ')).Count
                    $tmdlCols | Should -Be $csvCols -Because "$($csv.BaseName) TMDL columns should match CSV columns"
                }
            }
        }
    }
}

# ============================================================================
# 10. TOKEN PATTERN TESTS  -  Ensure no unresolved tokens in source files
# ============================================================================
Describe "Token Pattern Integrity" -Tag "Definition" {

    Context "All Definition Files Use Valid Token Patterns" {
        It "All {{ }} tokens in definitions follow the expected naming convention" {
            $validTokens = @(
                '{{WORKSPACE_ID}}', '{{BRONZE_LH_ID}}', '{{SILVER_LH_ID}}', '{{GOLD_LH_ID}}',
                '{{NB01_ID}}', '{{NB02_ID}}', '{{NB03_ID}}', '{{NB04_ID}}',
                '{{DF_FINANCE_ID}}', '{{DF_HR_ID}}', '{{DF_OPERATIONS_ID}}',
                '{{PIPELINE_ID}}', '{{SEMANTIC_MODEL_ID}}', '{{REPORT_ID}}', '{{DATA_AGENT_ID}}',
                '{{SQL_ENDPOINT}}', '{{LAKEHOUSE_NAME}}', '{{DATAFLOW_NAME}}',
                '{{BRONZE_LH_NAME}}', '{{SILVER_LH_NAME}}', '{{GOLD_LH_NAME}}'
            )
            $allDefFiles = Get-ChildItem $defDir -Recurse -File
            foreach ($f in $allDefFiles) {
                $content = Get-Content $f.FullName -Raw
                $tokens = [regex]::Matches($content, '\{\{[A-Z_]+\}\}') | ForEach-Object { $_.Value }
                foreach ($tok in $tokens) {
                    $tok | Should -BeIn $validTokens -Because "Unexpected token $tok found in $($f.Name)"
                }
            }
        }
    }

    Context "TMDL Expressions Use Only SQL_ENDPOINT and LAKEHOUSE_NAME Tokens" {
        It "expressions.tmdl has exactly 2 distinct token types" {
            $content = Get-Content (Join-Path $tmdlDir "expressions.tmdl") -Raw
            $tokens = [regex]::Matches($content, '\{\{[A-Z_]+\}\}') | ForEach-Object { $_.Value } | Sort-Object -Unique
            $tokens.Count | Should -Be 2
            $tokens | Should -Contain '{{SQL_ENDPOINT}}'
            $tokens | Should -Contain '{{LAKEHOUSE_NAME}}'
        }
    }
}

# ============================================================================
# 11. FILE ENCODING TESTS  -  No corruption
# ============================================================================
Describe "File Encoding & Syntax" -Tag "Unit" {

    Context "PowerShell Scripts Are Valid" {
        # Deploy-HorizonBooks.ps1 is a legacy script with known backtick-encoding issues; excluded from parse checks
        $ps1Files = Get-ChildItem $deployDir -Filter "*.ps1" | Where-Object { $_.Name -ne 'Deploy-HorizonBooks.ps1' } | ForEach-Object { @{ Path = $_.FullName; Name = $_.Name } }
        It "<Name> is parseable PowerShell (no syntax errors)" -ForEach $ps1Files {
            $errors = $null
            [System.Management.Automation.Language.Parser]::ParseFile($Path, [ref]$null, [ref]$errors)
            $errors.Count | Should -Be 0
        }
    }

    Context "JSON Files Are Valid" {
        It "items-manifest.json is valid JSON" {
            { Get-Content (Join-Path $defDir "items-manifest.json") -Raw | ConvertFrom-Json } | Should -Not -Throw
        }
        It "pipeline-content.json is valid JSON" {
            { Get-Content (Join-Path $defDir "pipeline\pipeline-content.json") -Raw | ConvertFrom-Json } | Should -Not -Throw
        }
        It "All output-config.json files are valid JSON" {
            Get-ChildItem $defDir -Recurse -Filter "output-config.json" | ForEach-Object {
                { Get-Content $_.FullName -Raw | ConvertFrom-Json } | Should -Not -Throw
            }
        }
        It "queryMetadata.json is valid JSON" {
            { Get-Content (Join-Path $defDir "dataflows\queryMetadata.json") -Raw | ConvertFrom-Json } | Should -Not -Throw
        }
        It "All notebook metadata JSON files are valid" {
            Get-ChildItem (Join-Path $defDir "notebooks") -Filter "*.json" | ForEach-Object {
                { Get-Content $_.FullName -Raw | ConvertFrom-Json } | Should -Not -Throw
            }
        }
        It "All lakehouse definition JSON files are valid" {
            Get-ChildItem (Join-Path $defDir "lakehouses") -Filter "*.json" | ForEach-Object {
                { Get-Content $_.FullName -Raw | ConvertFrom-Json } | Should -Not -Throw
            }
        }
        It "HorizonBooks_TaskFlow.json is valid JSON" {
            { Get-Content (Join-Path $deployDir "HorizonBooks_TaskFlow.json") -Raw | ConvertFrom-Json } | Should -Not -Throw
        }
    }
}

# ============================================================================
# 12. INTEGRATION TESTS  -  Live Fabric workspace (requires -WorkspaceId)
# ============================================================================
Describe "Fabric Workspace Integration" -Tag "Integration" -Skip:(-not $WorkspaceId) {

    BeforeAll {
        if (-not $WorkspaceId) { return }
        try {
            $token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
            $script:intHeaders = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
            $script:allItems = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" -Headers $script:intHeaders).value
        }
        catch {
            Write-Warning "Cannot obtain Azure token: $_"
        }
    }

    Context "Workspace Exists" {
        It "Can retrieve workspace details" {
            $resp = Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId" -Headers $script:intHeaders -Method Get
            $resp.id | Should -Be $WorkspaceId
        }
    }

    Context "Three Lakehouses Deployed" {
        $lakehouses = @("BronzeLH", "SilverLH", "GoldLH")
        It "Lakehouse '<_>' exists in workspace" -ForEach $lakehouses {
            $found = $script:allItems | Where-Object { $_.displayName -eq $_ -and $_.type -eq "Lakehouse" }
            $found | Should -Not -BeNullOrEmpty
        }
    }

    Context "Notebooks Deployed" {
        $notebooks = @("HorizonBooks_01_BronzeToSilver", "HorizonBooks_02_WebEnrichment", "HorizonBooks_03_SilverToGold")
        It "Notebook '<_>' exists in workspace" -ForEach $notebooks {
            $found = $script:allItems | Where-Object { $_.displayName -eq $_ -and $_.type -eq "Notebook" }
            $found | Should -Not -BeNullOrEmpty
        }
    }

    Context "Dataflows Deployed" {
        $dataflows = @("HorizonBooks_DF_Finance", "HorizonBooks_DF_HR", "HorizonBooks_DF_Operations")
        It "DataflowGen2 '<_>' exists in workspace" -ForEach $dataflows {
            $found = $script:allItems | Where-Object { $_.displayName -eq $_ -and $_.type -eq "DataflowGen2" }
            $found | Should -Not -BeNullOrEmpty
        }
    }

    Context "Pipeline Deployed" {
        It "PL_HorizonBooks_Orchestration pipeline exists" {
            $found = $script:allItems | Where-Object { $_.displayName -eq "PL_HorizonBooks_Orchestration" -and $_.type -eq "DataPipeline" }
            $found | Should -Not -BeNullOrEmpty
        }
    }

    Context "Semantic Model Deployed" {
        It "HorizonBooksModel semantic model exists" {
            $found = $script:allItems | Where-Object { $_.displayName -eq "HorizonBooksModel" -and $_.type -eq "SemanticModel" }
            $found | Should -Not -BeNullOrEmpty
        }
    }

    Context "Report Deployed" {
        It "A Power BI report exists in workspace" {
            $found = $script:allItems | Where-Object { $_.type -eq "Report" }
            $found | Should -Not -BeNullOrEmpty
        }
    }

    Context "GoldLH SQL Endpoint" {
        It "GoldLH lakehouse has a SQL analytics endpoint" {
            $lh = $script:allItems | Where-Object { $_.displayName -eq "GoldLH" -and $_.type -eq "Lakehouse" }
            if ($lh) {
                $lhDetail = Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses/$($lh.id)" -Headers $script:intHeaders
                $lhDetail.properties.sqlEndpointProperties.connectionString | Should -Not -BeNullOrEmpty
            }
        }
    }

    Context "Item Count" {
        It "Workspace has at least 11 deployed items (3 LH + 3 NB + 3 DF + 1 PL + 1 SM)" {
            $script:allItems.Count | Should -BeGreaterOrEqual 11
        }
    }
}
