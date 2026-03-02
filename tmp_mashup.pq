section Section1;

// -- Global Target Parameters --
shared TargetWorkspaceId = "91b2dca3-5729-4e7d-a473-bfeb85c16aa1" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];
shared TargetLakehouseId = "58e660a3-1a6c-4ffa-af2e-907fd848614b" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];

shared DimAccounts_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "DimAccounts", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DimAccounts_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared DimAccounts = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="DimAccounts.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"AccountID", type text},
        {"AccountName", type text},
        {"AccountType", type text},
        {"AccountCategory", type text},
        {"ParentAccountID", type text},
        {"IsActive", type logical}
    }),
    RemovedDuplicates = Table.Distinct(ChangedType),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;
shared DimCostCenters_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "DimCostCenters", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DimCostCenters_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared DimCostCenters = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="DimCostCenters.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"CostCenterID", type text},
        {"CostCenterName", type text},
        {"Department", type text},
        {"DivisionHead", type text}
    }),
    RemovedDuplicates = Table.Distinct(ChangedType),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;
shared FactFinancialTransactions_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "FactFinancialTransactions", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FactFinancialTransactions_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared FactFinancialTransactions = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="FactFinancialTransactions.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"TransactionID", type text},
        {"TransactionDate", type date},
        {"AccountID", type text},
        {"BookID", type text},
        {"Amount", type number},
        {"Currency", type text},
        {"TransactionType", type text},
        {"FiscalYear", type text},
        {"FiscalQuarter", type text},
        {"FiscalMonth", type text},
        {"CostCenterID", type text},
        {"Description", type text}
    }),
    RemovedDuplicates = Table.Distinct(ChangedType),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;
shared FactBudget_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "FactBudget", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FactBudget_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared FactBudget = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="FactBudget.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"BudgetID", type text},
        {"FiscalYear", type text},
        {"FiscalQuarter", type text},
        {"FiscalMonth", type text},
        {"AccountID", type text},
        {"CostCenterID", type text},
        {"BudgetAmount", type number},
        {"ActualAmount", type number},
        {"Variance", type number},
        {"VariancePct", type number}
    }),
    RemovedDuplicates = Table.Distinct(ChangedType),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;
