section Section1;

// -- Global Target Parameters --
// These are replaced at deployment time by Deploy-Pipeline.ps1 or Update-DataflowDestinations.ps1
shared TargetWorkspaceId = "{{WORKSPACE_ID}}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];
shared TargetLakehouseId = "{{BRONZE_LH_ID}}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];

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
        {"AccountID", Int64.Type},
        {"AccountName", type text},
        {"AccountType", type text},
        {"AccountCategory", type text},
        {"ParentAccountID", Int64.Type},
        {"IsActive", type logical}
    }),
    // Trim and standardize text fields
    TrimmedText = Table.TransformColumns(ChangedType, {
        {"AccountName", Text.Trim, type text},
        {"AccountType", each Text.Proper(Text.Trim(_)), type text},
        {"AccountCategory", each if _ = null then "Uncategorized" else Text.Proper(Text.Trim(_)), type text}
    }),
    RemovedDuplicates = Table.Distinct(TrimmedText),
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
    // Standardize text casing
    StandardizedText = Table.TransformColumns(ChangedType, {
        {"CostCenterName", Text.Trim, type text},
        {"Department", each Text.Proper(Text.Trim(_)), type text},
        {"DivisionHead", each Text.Proper(Text.Trim(_)), type text}
    }),
    RemovedDuplicates = Table.Distinct(StandardizedText),
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
        {"AccountID", Int64.Type},
        {"BookID", type text},
        {"Amount", type number},
        {"Currency", type text},
        {"TransactionType", type text},
        {"FiscalYear", type text},
        {"FiscalQuarter", type text},
        {"FiscalMonth", Int64.Type},
        {"CostCenterID", type text},
        {"Description", type text}
    }),
    // Standardize Currency (uppercase, default USD), proper case TransactionType
    StandardizedText = Table.TransformColumns(ChangedType, {
        {"Currency", each if _ = null then "USD" else Text.Upper(Text.Trim(_)), type text},
        {"TransactionType", each Text.Proper(Text.Trim(_)), type text},
        {"Description", each if _ = null then "" else Text.Trim(_), type text},
        {"FiscalYear", Text.Trim, type text},
        {"FiscalQuarter", Text.Trim, type text}
    }),
    // Fill null amounts with 0
    ValidatedAmounts = Table.ReplaceValue(StandardizedText, null, 0, Replacer.ReplaceValue, {"Amount"}),
    RemovedDuplicates = Table.Distinct(ValidatedAmounts),
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
        {"FiscalMonth", Int64.Type},
        {"AccountID", Int64.Type},
        {"CostCenterID", type text},
        {"BudgetAmount", type number},
        {"ActualAmount", type number},
        {"Variance", type number},
        {"VariancePct", type number}
    }),
    // Trim fiscal period text
    TrimmedText = Table.TransformColumns(ChangedType, {
        {"FiscalYear", Text.Trim, type text},
        {"FiscalQuarter", Text.Trim, type text}
    }),
    // Fill null monetary amounts with 0
    FilledBudget = Table.ReplaceValue(TrimmedText, null, 0, Replacer.ReplaceValue, {"BudgetAmount"}),
    FilledActual = Table.ReplaceValue(FilledBudget, null, 0, Replacer.ReplaceValue, {"ActualAmount"}),
    FilledVariance = Table.ReplaceValue(FilledActual, null, 0, Replacer.ReplaceValue, {"Variance"}),
    FilledVariancePct = Table.ReplaceValue(FilledVariance, null, 0, Replacer.ReplaceValue, {"VariancePct"}),
    RemovedDuplicates = Table.Distinct(FilledVariancePct),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;
