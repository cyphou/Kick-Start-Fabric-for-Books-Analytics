section Section1;

// -- Global Target Parameters --
// These are replaced at deployment time by Deploy-Pipeline.ps1 or Update-DataflowDestinations.ps1
shared TargetWorkspaceId = "{{WORKSPACE_ID}}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];
shared TargetLakehouseId = "{{BRONZE_LH_ID}}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];

shared DimEmployees_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "DimEmployees", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DimEmployees_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared DimEmployees = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="DimEmployees.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"EmployeeID", type text},
        {"FirstName", type text},
        {"LastName", type text},
        {"Email", type text},
        {"HireDate", type date},
        {"DepartmentID", type text},
        {"JobTitle", type text},
        {"ManagerID", type text},
        {"EmploymentType", type text},
        {"Location", type text},
        {"GeoID", type text},
        {"IsActive", type logical}
    }),
    // Normalize email to lowercase, proper case names and titles
    StandardizedText = Table.TransformColumns(ChangedType, {
        {"FirstName", each Text.Proper(Text.Trim(_)), type text},
        {"LastName", each Text.Proper(Text.Trim(_)), type text},
        {"Email", each if _ = null then null else Text.Lower(Text.Trim(_)), type text},
        {"JobTitle", each Text.Proper(Text.Trim(_)), type text},
        {"EmploymentType", each Text.Proper(Text.Trim(_)), type text},
        {"Location", each Text.Proper(Text.Trim(_)), type text}
    }),
    RemovedDuplicates = Table.Distinct(StandardizedText),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;

shared DimDepartments_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "DimDepartments", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DimDepartments_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared DimDepartments = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="DimDepartments.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"DepartmentID", type text},
        {"DepartmentName", type text},
        {"DepartmentHead", type text},
        {"HeadCount", Int64.Type},
        {"AnnualBudget", type number},
        {"Location", type text}
    }),
    // Proper case department names, validate non-negative numbers
    StandardizedText = Table.TransformColumns(ChangedType, {
        {"DepartmentName", each Text.Proper(Text.Trim(_)), type text},
        {"DepartmentHead", each Text.Proper(Text.Trim(_)), type text},
        {"Location", each Text.Proper(Text.Trim(_)), type text}
    }),
    ValidatedNumbers = Table.TransformColumns(StandardizedText, {
        {"HeadCount", each if _ = null or _ < 0 then 0 else _, Int64.Type},
        {"AnnualBudget", each if _ = null or _ < 0 then 0 else _, type number}
    }),
    RemovedDuplicates = Table.Distinct(ValidatedNumbers),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;

shared FactPayroll_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "FactPayroll", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FactPayroll_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared FactPayroll = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="FactPayroll.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"PayrollID", type text},
        {"EmployeeID", type text},
        {"PayPeriodStart", type date},
        {"PayPeriodEnd", type date},
        {"BaseSalary", type number},
        {"Bonus", type number},
        {"Overtime", type number},
        {"Deductions", type number},
        {"NetPay", type number},
        {"PayDate", type date}
    }),
    // Validate monetary amounts: fill nulls with 0, ensure non-negative
    ValidatedAmounts = Table.TransformColumns(ChangedType, {
        {"BaseSalary", each if _ = null then 0 else Number.Abs(_), type number},
        {"Bonus", each if _ = null then 0 else Number.Abs(_), type number},
        {"Overtime", each if _ = null then 0 else Number.Abs(_), type number},
        {"Deductions", each if _ = null then 0 else Number.Abs(_), type number},
        {"NetPay", each if _ = null then 0 else _, type number}
    }),
    RemovedDuplicates = Table.Distinct(ValidatedAmounts),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;

shared FactPerformanceReviews_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "FactPerformanceReviews", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FactPerformanceReviews_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared FactPerformanceReviews = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="FactPerformanceReviews.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"ReviewID", type text},
        {"EmployeeID", type text},
        {"ReviewDate", type date},
        {"ReviewerID", type text},
        {"PerformanceRating", type text},
        {"GoalsMet", type text},
        {"Strengths", type text},
        {"AreasForImprovement", type text},
        {"OverallScore", type number}
    }),
    // Standardize text casing, trim feedback fields
    StandardizedText = Table.TransformColumns(ChangedType, {
        {"PerformanceRating", each if _ = null then null else Text.Proper(Text.Trim(_)), type text},
        {"GoalsMet", each if _ = null then null else Text.Trim(_), type text},
        {"Strengths", each if _ = null then "" else Text.Trim(_), type text},
        {"AreasForImprovement", each if _ = null then "" else Text.Trim(_), type text}
    }),
    // Clamp OverallScore to valid range (0-100)
    ClampedScores = Table.TransformColumns(StandardizedText, {
        {"OverallScore", each if _ = null then null
            else if _ < 0 then 0
            else if _ > 100 then 100
            else _, type number}
    }),
    RemovedDuplicates = Table.Distinct(ClampedScores),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;

shared FactRecruitment_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "FactRecruitment", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FactRecruitment_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared FactRecruitment = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="FactRecruitment.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"RequisitionID", type text},
        {"DepartmentID", type text},
        {"JobTitle", type text},
        {"OpenDate", type date},
        {"CloseDate", type date},
        {"Status", type text},
        {"ApplicationsReceived", Int64.Type},
        {"Interviewed", Int64.Type},
        {"OffersExtended", Int64.Type},
        {"OfferAccepted", Int64.Type},
        {"HiringManagerID", type text},
        {"SalaryRangeMin", type number},
        {"SalaryRangeMax", type number},
        {"TimeToFillDays", Int64.Type}
    }),
    // Standardize text casing
    StandardizedText = Table.TransformColumns(ChangedType, {
        {"JobTitle", each Text.Proper(Text.Trim(_)), type text},
        {"Status", each Text.Proper(Text.Trim(_)), type text}
    }),
    // Validate non-negative counts
    ValidatedCounts = Table.TransformColumns(StandardizedText, {
        {"ApplicationsReceived", each if _ = null or _ < 0 then 0 else _, Int64.Type},
        {"Interviewed", each if _ = null or _ < 0 then 0 else _, Int64.Type},
        {"OffersExtended", each if _ = null or _ < 0 then 0 else _, Int64.Type},
        {"OfferAccepted", each if _ = null or _ < 0 then 0 else _, Int64.Type}
    }),
    // Validate salary ranges are non-negative
    ValidatedSalaries = Table.TransformColumns(ValidatedCounts, {
        {"SalaryRangeMin", each if _ = null or _ < 0 then 0 else _, type number},
        {"SalaryRangeMax", each if _ = null or _ < 0 then 0 else _, type number}
    }),
    RemovedDuplicates = Table.Distinct(ValidatedSalaries),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;
