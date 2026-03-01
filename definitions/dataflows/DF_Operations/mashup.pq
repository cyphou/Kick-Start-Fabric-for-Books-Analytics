section Section1;

// -- Global Target Parameters --
// These are replaced at deployment time by Deploy-Pipeline.ps1 or Update-DataflowDestinations.ps1
shared TargetWorkspaceId = "{{WORKSPACE_ID}}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];
shared TargetLakehouseId = "{{BRONZE_LH_ID}}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];

shared DimBooks_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "DimBooks", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DimBooks_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared DimBooks = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="DimBooks.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"BookID", type text},
        {"Title", type text},
        {"AuthorID", type text},
        {"Genre", type text},
        {"SubGenre", type text},
        {"ISBN", type text},
        {"PublishDate", type date},
        {"ListPrice", type number},
        {"Format", type text},
        {"PageCount", Int64.Type},
        {"PrintRunSize", Int64.Type},
        {"ImprintName", type text},
        {"Status", type text}
    }),
    // Standardize text casing
    StandardizedText = Table.TransformColumns(ChangedType, {
        {"Title", Text.Trim, type text},
        {"Genre", each Text.Proper(Text.Trim(_)), type text},
        {"SubGenre", each if _ = null then null else Text.Proper(Text.Trim(_)), type text},
        {"ISBN", Text.Trim, type text},
        {"Format", each Text.Proper(Text.Trim(_)), type text},
        {"ImprintName", each if _ = null then null else Text.Trim(_), type text},
        {"Status", each Text.Proper(Text.Trim(_)), type text}
    }),
    // Validate numeric ranges: price >= 0, page count > 0
    ValidatedNumbers = Table.TransformColumns(StandardizedText, {
        {"ListPrice", each if _ = null or _ < 0 then 0 else _, type number},
        {"PageCount", each if _ = null or _ <= 0 then null else _, Int64.Type},
        {"PrintRunSize", each if _ = null or _ <= 0 then null else _, Int64.Type}
    }),
    RemovedDuplicates = Table.Distinct(ValidatedNumbers),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;

shared DimAuthors_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "DimAuthors", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DimAuthors_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared DimAuthors = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="DimAuthors.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"AuthorID", type text},
        {"FirstName", type text},
        {"LastName", type text},
        {"PenName", type text},
        {"AgentName", type text},
        {"AgentCompany", type text},
        {"ContractStartDate", type date},
        {"ContractEndDate", type date},
        {"RoyaltyRate", type number},
        {"AdvanceAmount", type number},
        {"Genre", type text},
        {"Nationality", type text},
        {"BookCount", Int64.Type}
    }),
    // Standardize text casing
    StandardizedText = Table.TransformColumns(ChangedType, {
        {"FirstName", each Text.Proper(Text.Trim(_)), type text},
        {"LastName", each Text.Proper(Text.Trim(_)), type text},
        {"PenName", each if _ = null then null else Text.Trim(_), type text},
        {"AgentName", each if _ = null then null else Text.Proper(Text.Trim(_)), type text},
        {"AgentCompany", each if _ = null then null else Text.Trim(_), type text},
        {"Genre", each Text.Proper(Text.Trim(_)), type text},
        {"Nationality", each Text.Proper(Text.Trim(_)), type text}
    }),
    // Normalize RoyaltyRate to 0-1 range (convert from percentage if > 1)
    NormalizedRoyalty = Table.TransformColumns(StandardizedText, {
        {"RoyaltyRate", each if _ = null then null else if _ > 1 then _ / 100 else _, type number},
        {"AdvanceAmount", each if _ = null or _ < 0 then 0 else _, type number}
    }),
    RemovedDuplicates = Table.Distinct(NormalizedRoyalty),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;

shared DimCustomers_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "DimCustomers", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DimCustomers_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared DimCustomers = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="DimCustomers.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"CustomerID", type text},
        {"CustomerName", type text},
        {"CustomerType", type text},
        {"ContactEmail", type text},
        {"City", type text},
        {"State", type text},
        {"Country", type text},
        {"Region", type text},
        {"GeoID", type text},
        {"CreditLimit", type number},
        {"PaymentTerms", type text},
        {"IsActive", type logical},
        {"AccountOpenDate", type date}
    }),
    // Normalize email to lowercase, proper case location fields
    StandardizedText = Table.TransformColumns(ChangedType, {
        {"CustomerName", Text.Trim, type text},
        {"CustomerType", each Text.Proper(Text.Trim(_)), type text},
        {"ContactEmail", each if _ = null then null else Text.Lower(Text.Trim(_)), type text},
        {"City", each Text.Proper(Text.Trim(_)), type text},
        {"State", each Text.Proper(Text.Trim(_)), type text},
        {"Country", each Text.Proper(Text.Trim(_)), type text},
        {"Region", each Text.Proper(Text.Trim(_)), type text},
        {"PaymentTerms", each if _ = null then null else Text.Trim(_), type text}
    }),
    // Validate CreditLimit is non-negative
    ValidatedAmounts = Table.TransformColumns(StandardizedText, {
        {"CreditLimit", each if _ = null or _ < 0 then 0 else _, type number}
    }),
    RemovedDuplicates = Table.Distinct(ValidatedAmounts),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;

shared DimGeography_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "DimGeography", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DimGeography_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared DimGeography = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="DimGeography.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"GeoID", type text},
        {"City", type text},
        {"StateProvince", type text},
        {"Country", type text},
        {"Continent", type text},
        {"Region", type text},
        {"SubRegion", type text},
        {"Latitude", type number},
        {"Longitude", type number},
        {"TimeZone", type text},
        {"Currency", type text},
        {"Population", Int64.Type},
        {"IsCapital", type logical}
    }),
    // Standardize casing: proper case geo names, uppercase currency
    StandardizedText = Table.TransformColumns(ChangedType, {
        {"City", each Text.Proper(Text.Trim(_)), type text},
        {"StateProvince", each if _ = null then null else Text.Proper(Text.Trim(_)), type text},
        {"Country", each Text.Proper(Text.Trim(_)), type text},
        {"Continent", each Text.Proper(Text.Trim(_)), type text},
        {"Region", each Text.Proper(Text.Trim(_)), type text},
        {"SubRegion", each if _ = null then null else Text.Proper(Text.Trim(_)), type text},
        {"TimeZone", Text.Trim, type text},
        {"Currency", each Text.Upper(Text.Trim(_)), type text}
    }),
    // Validate coordinate ranges: Latitude [-90, 90], Longitude [-180, 180]
    ValidatedCoords = Table.TransformColumns(StandardizedText, {
        {"Latitude", each if _ = null then null else if _ < -90 or _ > 90 then null else _, type number},
        {"Longitude", each if _ = null then null else if _ < -180 or _ > 180 then null else _, type number}
    }),
    RemovedDuplicates = Table.Distinct(ValidatedCoords),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;

shared DimWarehouses_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "DimWarehouses", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DimWarehouses_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared DimWarehouses = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="DimWarehouses.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"WarehouseID", type text},
        {"WarehouseName", type text},
        {"Address", type text},
        {"City", type text},
        {"State", type text},
        {"Country", type text},
        {"SquareFootage", Int64.Type},
        {"MaxCapacityUnits", Int64.Type},
        {"CurrentUtilization", type number},
        {"ManagerID", type text},
        {"MonthlyRent", type number},
        {"IsActive", type logical}
    }),
    // Standardize text casing
    StandardizedText = Table.TransformColumns(ChangedType, {
        {"WarehouseName", Text.Trim, type text},
        {"Address", each if _ = null then null else Text.Trim(_), type text},
        {"City", each Text.Proper(Text.Trim(_)), type text},
        {"State", each Text.Proper(Text.Trim(_)), type text},
        {"Country", each Text.Proper(Text.Trim(_)), type text}
    }),
    // Normalize CurrentUtilization to 0-1 range, validate numeric ranges
    ValidatedNumbers = Table.TransformColumns(StandardizedText, {
        {"CurrentUtilization", each if _ = null then 0
            else if _ > 1 then (if _ / 100 > 1 then 1 else _ / 100)
            else if _ < 0 then 0
            else _, type number},
        {"MonthlyRent", each if _ = null or _ < 0 then 0 else _, type number},
        {"SquareFootage", each if _ = null or _ < 0 then 0 else _, Int64.Type},
        {"MaxCapacityUnits", each if _ = null or _ < 0 then 0 else _, Int64.Type}
    }),
    RemovedDuplicates = Table.Distinct(ValidatedNumbers),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;

shared FactOrders_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "FactOrders", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FactOrders_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared FactOrders = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="FactOrders.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"OrderID", type text},
        {"OrderDate", type date},
        {"CustomerID", type text},
        {"BookID", type text},
        {"Quantity", Int64.Type},
        {"UnitPrice", type number},
        {"Discount", type number},
        {"TotalAmount", type number},
        {"OrderStatus", type text},
        {"ShipDate", type date},
        {"DeliveryDate", type date},
        {"WarehouseID", type text},
        {"SalesRepID", type text},
        {"Channel", type text}
    }),
    // Standardize text casing for categorical fields
    StandardizedText = Table.TransformColumns(ChangedType, {
        {"OrderStatus", each Text.Proper(Text.Trim(_)), type text},
        {"Channel", each Text.Proper(Text.Trim(_)), type text}
    }),
    // Validate amounts: non-negative prices, discount in 0-1 range
    ValidatedAmounts = Table.TransformColumns(StandardizedText, {
        {"Quantity", each if _ = null or _ < 0 then 0 else _, Int64.Type},
        {"UnitPrice", each if _ = null or _ < 0 then 0 else _, type number},
        {"Discount", each if _ = null then 0 else if _ < 0 then 0 else if _ > 1 then _ / 100 else _, type number},
        {"TotalAmount", each if _ = null or _ < 0 then 0 else _, type number}
    }),
    RemovedDuplicates = Table.Distinct(ValidatedAmounts),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;

shared FactInventory_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "FactInventory", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FactInventory_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared FactInventory = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="FactInventory.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"InventoryID", type text},
        {"BookID", type text},
        {"WarehouseID", type text},
        {"SnapshotDate", type date},
        {"QuantityOnHand", Int64.Type},
        {"QuantityReserved", Int64.Type},
        {"QuantityAvailable", Int64.Type},
        {"ReorderPoint", Int64.Type},
        {"ReorderQuantity", Int64.Type},
        {"UnitCost", type number},
        {"TotalInventoryValue", type number},
        {"DaysOfSupply", Int64.Type},
        {"Status", type text}
    }),
    // Standardize Status casing
    StandardizedText = Table.TransformColumns(ChangedType, {
        {"Status", each Text.Proper(Text.Trim(_)), type text}
    }),
    // Validate non-negative quantities and costs
    ValidatedQuantities = Table.TransformColumns(StandardizedText, {
        {"QuantityOnHand", each if _ = null or _ < 0 then 0 else _, Int64.Type},
        {"QuantityReserved", each if _ = null or _ < 0 then 0 else _, Int64.Type},
        {"QuantityAvailable", each if _ = null or _ < 0 then 0 else _, Int64.Type},
        {"ReorderPoint", each if _ = null or _ < 0 then 0 else _, Int64.Type},
        {"ReorderQuantity", each if _ = null or _ < 0 then 0 else _, Int64.Type},
        {"DaysOfSupply", each if _ = null or _ < 0 then 0 else _, Int64.Type},
        {"UnitCost", each if _ = null or _ < 0 then 0 else _, type number},
        {"TotalInventoryValue", each if _ = null or _ < 0 then 0 else _, type number}
    }),
    RemovedDuplicates = Table.Distinct(ValidatedQuantities),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;

shared FactReturns_Target = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = TargetWorkspaceId]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = TargetLakehouseId]}[Data],
    TableNavigation = Navigation_2{[Id = "FactReturns", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FactReturns_Target", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared FactReturns = let
    Source = Lakehouse.Contents(null){[workspaceId=TargetWorkspaceId]}[Data],
    Navigate = Source{[lakehouseId=TargetLakehouseId]}[Data],
    FilesFolder = Navigate{[Name="Files"]}[Data],
    FileContent = FilesFolder{[Name="FactReturns.csv"]}[Content],
    ImportedCSV = Csv.Document(FileContent, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders, {
        {"ReturnID", type text},
        {"OrderID", type text},
        {"BookID", type text},
        {"CustomerID", type text},
        {"ReturnDate", type date},
        {"Quantity", Int64.Type},
        {"Reason", type text},
        {"ReturnStatus", type text},
        {"RefundAmount", type number},
        {"Condition", type text},
        {"RestockStatus", type text}
    }),
    // Standardize categorical text casing
    StandardizedText = Table.TransformColumns(ChangedType, {
        {"Reason", each Text.Proper(Text.Trim(_)), type text},
        {"ReturnStatus", each Text.Proper(Text.Trim(_)), type text},
        {"Condition", each Text.Proper(Text.Trim(_)), type text},
        {"RestockStatus", each Text.Proper(Text.Trim(_)), type text}
    }),
    // Validate amounts: quantity non-negative, refund absolute value
    ValidatedAmounts = Table.TransformColumns(StandardizedText, {
        {"Quantity", each if _ = null or _ < 0 then 0 else _, Int64.Type},
        {"RefundAmount", each if _ = null then 0 else Number.Abs(_), type number}
    }),
    RemovedDuplicates = Table.Distinct(ValidatedAmounts),
    FilteredNulls = Table.SelectRows(RemovedDuplicates, each Record.FieldValues(_) <> null)
in
    FilteredNulls;
