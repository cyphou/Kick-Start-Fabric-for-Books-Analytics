# Synapse Analytics notebook source
# ============================================================================
# Horizon Books - Notebook 2: Web Data Enrichment
# Fetches external data from public APIs and enriches Silver tables:
#   - Exchange rates (frankfurter.app) for multi-currency normalization
#   - Public holidays (date.nager.at) for DimDate enrichment
#   - Country economic indicators (restcountries.com) for DimGeography
#   - Book metadata (Open Library API) for DimBooks
# ============================================================================

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 1: Setup and configuration
# -----------------------------------------------------------
import requests
import json
import time
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, concat, concat_ws, upper, lower, trim,
    to_date, year, month, dayofmonth, current_timestamp,
    udf, explode, struct, array, broadcast, round as spark_round,
    first as spark_first
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    DateType, BooleanType, ArrayType, MapType
)

spark = SparkSession.builder.getOrCreate()

# ── Medallion Architecture Configuration ──
# Default lakehouse = SilverLH (schema-enabled)
# Silver tables are organized by schema: finance, hr, operations
# Web enrichment tables go to the 'web' schema
WEB_SCHEMA = "web"
SILVER_SCHEMAS = {
    "DimAccounts": "finance", "DimCostCenters": "finance",
    "FactFinancialTransactions": "finance", "FactBudget": "finance",
    "DimEmployees": "hr", "DimDepartments": "hr",
    "FactPayroll": "hr", "FactPerformanceReviews": "hr", "FactRecruitment": "hr",
    "DimBooks": "operations", "DimAuthors": "operations",
    "DimGeography": "operations", "DimCustomers": "operations",
    "DimWarehouses": "operations",
    "FactOrders": "operations", "FactInventory": "operations", "FactReturns": "operations",
}

def silver_table(name):
    """Return schema-qualified Silver table name."""
    return f"{SILVER_SCHEMAS.get(name, 'dbo')}.{name}"

def web_table(name):
    """Return schema-qualified web enrichment table name."""
    return f"{WEB_SCHEMA}.{name}"

# Create web schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {WEB_SCHEMA}")

# ── Reusable transform helpers ──
def get_monthly_rates(df_rates):
    """Aggregate exchange rates to one rate per currency per month."""
    return df_rates.groupBy("TargetCurrency", "RateMonth", "RateYear").agg(
        spark_first("ExchangeRate").alias("ExchangeRate")
    )

def convert_to_usd(amount_col_name, currency_col_name, rate_col_name="ExchangeRate"):
    """Return a Column expression that converts an amount to USD using the exchange rate."""
    return (
        when(col(currency_col_name) == "USD", col(amount_col_name))
        .otherwise(
            when(col(rate_col_name).isNotNull(),
                 spark_round(col(amount_col_name) / col(rate_col_name), 2))
            .otherwise(col(amount_col_name))
        )
    )

def exchange_rate_to_usd(currency_col_name, rate_col_name="ExchangeRate"):
    """Return a Column expression for the ExchangeRateToUSD column."""
    return (
        when(col(currency_col_name) == "USD", lit(1.0))
        .otherwise(coalesce(col(rate_col_name), lit(1.0)))
    )

print("=== Horizon Books: Web Data Enrichment ===")
print(f"Spark version: {spark.version}")
print(f"Run timestamp: {datetime.now().isoformat()}")
print(f"Web schema: {WEB_SCHEMA}")

# Rate limiting helper
def safe_request(url, retries=3, delay=1.0, timeout=15):
    """Make HTTP GET request with retries and rate limiting."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = delay * (2 ** attempt)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  HTTP {resp.status_code} for {url}")
                return None
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                print(f"  Request failed after {retries} attempts: {e}")
                return None
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 2: Fetch exchange rates from Frankfurter API
# -----------------------------------------------------------
# https://frankfurter.app — free, no API key required
# Fetches monthly average rates for FY2024-FY2026 for currencies used
# by Horizon Books' international customers

print("\n" + "="*60)
print("  Fetching Exchange Rates (Frankfurter API)")
print("="*60)

# Currencies used by Horizon Books (from DimGeography/FactFinancialTransactions)
target_currencies = ["EUR", "GBP", "JPY", "MXN", "CAD", "AUD", "BRL", "INR",
                     "ZAR", "KES", "NGN", "EGP", "CNY", "KRW", "SGD", "AED"]

# Fetch monthly rates for 2024-2026
# NOTE: Frankfurter API only returns rates for business days.
# Requesting the 1st of a month that falls on a weekend/holiday returns 404.
# Use the time-series endpoint (date range) so the API returns the nearest
# available business day rate for each month.
exchange_rows = []
rate_fetch_success = True

from datetime import date as _date, timedelta
import calendar
_today = _date.today()

for yr in [2024, 2025, 2026]:
    end_month = 12 if yr < _today.year else min(12, _today.month)
    for m in range(1, end_month + 1):
        # Build a short date range (1st–7th) so we always hit a business day
        start_str = f"{yr}-{m:02d}-01"
        end_str = f"{yr}-{m:02d}-07"
        url = (f"https://frankfurter.app/{start_str}..{end_str}"
               f"?from=USD&to={','.join(target_currencies)}")
        data = safe_request(url)
        if data and "rates" in data:
            # Take the first available date in the range
            first_date = sorted(data["rates"].keys())[0]
            day_rates = data["rates"][first_date]
            for currency, rate in day_rates.items():
                exchange_rows.append({
                    "RateDate": start_str,
                    "BaseCurrency": "USD",
                    "TargetCurrency": currency,
                    "ExchangeRate": float(rate),
                    "RateMonth": m,
                    "RateYear": yr
                })
            print(f"  ✓ {start_str} (via {first_date}): {len(day_rates)} currencies")
        else:
            print(f"  ⚠ Could not fetch rates for {start_str}")
            rate_fetch_success = False
        time.sleep(0.3)  # Rate limit courtesy

if exchange_rows:
    schema_rates = StructType([
        StructField("RateDate", StringType(), False),
        StructField("BaseCurrency", StringType(), False),
        StructField("TargetCurrency", StringType(), False),
        StructField("ExchangeRate", DoubleType(), False),
        StructField("RateMonth", IntegerType(), False),
        StructField("RateYear", IntegerType(), False)
    ])
    df_rates = spark.createDataFrame(exchange_rows, schema=schema_rates)
    df_rates = df_rates.withColumn("RateDate", to_date(col("RateDate")))
    df_rates = df_rates.withColumn("_fetched_at", current_timestamp())
    df_rates = df_rates.withColumn("_source", lit("frankfurter.app"))

    df_rates.write.mode("overwrite").format("delta").saveAsTable(web_table("WebExchangeRates"))
    print(f"  ✓ {WEB_SCHEMA}.WebExchangeRates saved: {df_rates.count()} rows")
    print(f"    Currencies: {len(set([r['TargetCurrency'] for r in exchange_rows]))}")
else:
    print("  ⚠ No exchange rate data fetched — will use fallback static rates")
    # Fallback: create static rates table with approximate values
    fallback_rates = [
        ("USD", "EUR", 0.92), ("USD", "GBP", 0.79), ("USD", "JPY", 149.5),
        ("USD", "MXN", 17.1), ("USD", "CAD", 1.36), ("USD", "AUD", 1.53),
        ("USD", "BRL", 4.97), ("USD", "INR", 83.1), ("USD", "ZAR", 18.6),
        ("USD", "KES", 155.0), ("USD", "NGN", 1400.0), ("USD", "EGP", 30.9),
        ("USD", "CNY", 7.2), ("USD", "KRW", 1320.0), ("USD", "SGD", 1.34),
        ("USD", "AED", 3.67)
    ]
    fallback_rows = []
    for yr in [2024, 2025, 2026]:
        for m in range(1, 13):
            for base, target, rate in fallback_rates:
                fallback_rows.append((f"{yr}-{m:02d}-01", base, target, rate, m, yr))
    schema_fb = StructType([
        StructField("RateDate", StringType()), StructField("BaseCurrency", StringType()),
        StructField("TargetCurrency", StringType()), StructField("ExchangeRate", DoubleType()),
        StructField("RateMonth", IntegerType()), StructField("RateYear", IntegerType())
    ])
    df_rates = spark.createDataFrame(fallback_rows, schema=schema_fb)
    df_rates = df_rates.withColumn("RateDate", to_date(col("RateDate")))
    df_rates = df_rates.withColumn("_fetched_at", current_timestamp())
    df_rates = df_rates.withColumn("_source", lit("fallback_static"))
    df_rates.write.mode("overwrite").format("delta").saveAsTable(web_table("WebExchangeRates"))
    print(f"  ✓ {WEB_SCHEMA}.WebExchangeRates (fallback): {df_rates.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 3: Fetch public holidays from Nager.Date API
# -----------------------------------------------------------
# https://date.nager.at — free, no API key required
# Fetches public holidays for countries where Horizon Books operates

print("\n" + "="*60)
print("  Fetching Public Holidays (Nager.Date API)")
print("="*60)

# Map Horizon Books countries to ISO 3166-1 alpha-2 codes
country_codes = {
    "United States": "US", "Canada": "CA", "United Kingdom": "GB",
    "Germany": "DE", "France": "FR", "Japan": "JP", "Mexico": "MX",
    "Australia": "AU", "Brazil": "BR", "India": "IN", "South Africa": "ZA",
    "China": "CN", "South Korea": "KR", "Nigeria": "NG", "Kenya": "KE",
    "Egypt": "EG", "Singapore": "SG", "United Arab Emirates": "AE",
    "Spain": "ES", "Italy": "IT", "Netherlands": "NL", "Sweden": "SE",
    "Argentina": "AR", "Chile": "CL", "Colombia": "CO", "Peru": "PE",
    "Poland": "PL", "Czech Republic": "CZ", "Austria": "AT"
}

holiday_rows = []
for country_name, code in country_codes.items():
    for yr in [2024, 2025, 2026]:
        url = f"https://date.nager.at/api/v3/PublicHolidays/{yr}/{code}"
        data = safe_request(url, delay=0.5)
        if data and isinstance(data, list):
            for h in data:
                holiday_rows.append({
                    "HolidayDate": h.get("date", ""),
                    "HolidayName": h.get("localName", h.get("name", "")),
                    "HolidayNameEnglish": h.get("name", ""),
                    "CountryCode": code,
                    "CountryName": country_name,
                    "IsFixed": h.get("fixed", False),
                    "IsGlobal": h.get("global", True),
                    "HolidayType": ",".join(h.get("types", ["Public"])),
                    "Year": yr
                })
            print(f"  ✓ {country_name} ({code}) {yr}: {len([h for h in data])} holidays")
        else:
            print(f"  ⚠ No holidays for {country_name} ({code}) {yr}")
        time.sleep(0.2)

if holiday_rows:
    schema_holidays = StructType([
        StructField("HolidayDate", StringType(), False),
        StructField("HolidayName", StringType(), True),
        StructField("HolidayNameEnglish", StringType(), True),
        StructField("CountryCode", StringType(), False),
        StructField("CountryName", StringType(), True),
        StructField("IsFixed", BooleanType(), True),
        StructField("IsGlobal", BooleanType(), True),
        StructField("HolidayType", StringType(), True),
        StructField("Year", IntegerType(), False)
    ])
    df_holidays = spark.createDataFrame(holiday_rows, schema=schema_holidays)
    df_holidays = df_holidays.withColumn("HolidayDate", to_date(col("HolidayDate")))
    df_holidays = df_holidays.withColumn("_fetched_at", current_timestamp())
    df_holidays = df_holidays.withColumn("_source", lit("date.nager.at"))

    df_holidays.write.mode("overwrite").format("delta").saveAsTable(web_table("WebPublicHolidays"))
    print(f"\n  ✓ WebPublicHolidays saved: {df_holidays.count()} rows")
    print(f"    Countries: {df_holidays.select('CountryCode').distinct().count()}")
else:
    print("  ⚠ No holiday data fetched — DimDate IsHoliday will remain FALSE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 4: Fetch country indicators from REST Countries API
# -----------------------------------------------------------
# https://restcountries.com — free, no API key required
# Fetches population, GDP, languages, area, etc.

print("\n" + "="*60)
print("  Fetching Country Indicators (REST Countries API)")
print("="*60)

# Fetch all countries in one call (efficient)
url = "https://restcountries.com/v3.1/all?fields=name,cca2,cca3,capital,population,area,region,subregion,languages,currencies,timezones,latlng,gini,flag,continents"
all_countries = safe_request(url, timeout=30)

country_rows = []
if all_countries and isinstance(all_countries, list):
    for c in all_countries:
        try:
            # Extract primary language
            langs = c.get("languages", {})
            primary_lang = list(langs.values())[0] if langs else None
            all_langs = ", ".join(langs.values()) if langs else None

            # Extract primary currency
            currs = c.get("currencies", {})
            primary_curr_code = list(currs.keys())[0] if currs else None
            primary_curr_name = currs[primary_curr_code].get("name") if primary_curr_code and primary_curr_code in currs else None

            # Extract capital
            capitals = c.get("capital", [])
            capital = capitals[0] if capitals else None

            # Extract Gini coefficient (inequality index)
            gini = c.get("gini", {})
            latest_gini = list(gini.values())[-1] if gini else None
            gini_year = list(gini.keys())[-1] if gini else None

            # Coordinates
            latlng = c.get("latlng", [None, None])

            country_rows.append({
                "CountryCode2": c.get("cca2"),
                "CountryCode3": c.get("cca3"),
                "CountryNameOfficial": c.get("name", {}).get("official"),
                "CountryNameCommon": c.get("name", {}).get("common"),
                "Capital": capital,
                "Population": c.get("population"),
                "AreaSqKm": c.get("area"),
                "RegionWorld": c.get("region"),
                "SubRegionWorld": c.get("subregion"),
                "PrimaryLanguage": primary_lang,
                "AllLanguages": all_langs,
                "PrimaryCurrencyCode": primary_curr_code,
                "PrimaryCurrencyName": primary_curr_name,
                "Latitude": latlng[0] if latlng and len(latlng) > 0 else None,
                "Longitude": latlng[1] if latlng and len(latlng) > 1 else None,
                "Continent": c.get("continents", [None])[0],
                "GiniIndex": latest_gini,
                "GiniYear": int(gini_year) if gini_year else None,
                "FlagEmoji": c.get("flag")
            })
        except Exception as e:
            print(f"  ⚠ Error parsing {c.get('name', {}).get('common', 'unknown')}: {e}")

    print(f"  ✓ Parsed {len(country_rows)} countries")
else:
    print("  ⚠ REST Countries API unavailable")

if country_rows:
    schema_countries = StructType([
        StructField("CountryCode2", StringType(), True),
        StructField("CountryCode3", StringType(), True),
        StructField("CountryNameOfficial", StringType(), True),
        StructField("CountryNameCommon", StringType(), True),
        StructField("Capital", StringType(), True),
        StructField("Population", IntegerType(), True),
        StructField("AreaSqKm", DoubleType(), True),
        StructField("RegionWorld", StringType(), True),
        StructField("SubRegionWorld", StringType(), True),
        StructField("PrimaryLanguage", StringType(), True),
        StructField("AllLanguages", StringType(), True),
        StructField("PrimaryCurrencyCode", StringType(), True),
        StructField("PrimaryCurrencyName", StringType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("Continent", StringType(), True),
        StructField("GiniIndex", DoubleType(), True),
        StructField("GiniYear", IntegerType(), True),
        StructField("FlagEmoji", StringType(), True)
    ])
    df_countries = spark.createDataFrame(country_rows, schema=schema_countries)
    df_countries = df_countries.withColumn("_fetched_at", current_timestamp())
    df_countries = df_countries.withColumn("_source", lit("restcountries.com"))

    # Population density
    df_countries = df_countries.withColumn(
        "PopulationDensity",
        when(col("AreaSqKm") > 0,
             spark_round(col("Population") / col("AreaSqKm"), 2)
        ).otherwise(lit(None))
    )

    # Economic size category
    df_countries = df_countries.withColumn(
        "EconomicSizeCategory",
        when(col("Population") >= 100_000_000, lit("Large"))
        .when(col("Population") >= 20_000_000, lit("Medium"))
        .when(col("Population") >= 1_000_000, lit("Small"))
        .otherwise(lit("Micro"))
    )

    df_countries.write.mode("overwrite").format("delta").saveAsTable(web_table("WebCountryIndicators"))
    print(f"  ✓ WebCountryIndicators saved: {df_countries.count()} rows")
else:
    print("  ⚠ REST Countries API unavailable — using static fallback for Horizon Books countries")
    # Static fallback covering the 28 countries in DimGeography
    # Columns: CountryCode2,CountryCode3,CountryNameOfficial,CountryNameCommon,Capital,
    #          Population,AreaSqKm,RegionWorld,SubRegionWorld,PrimaryLanguage,AllLanguages,
    #          PrimaryCurrencyCode,PrimaryCurrencyName,Latitude,Longitude,Continent,
    #          GiniIndex,GiniYear,FlagEmoji
    fallback_countries = [
        ("AR","ARG","Argentine Republic","Argentina","Buenos Aires",45810000,2780400.0,"Americas","South America","Spanish","Spanish","ARS","Argentine Peso",-34.0,-64.0,"South America",42.9,2020,"🇦🇷"),
        ("AU","AUS","Commonwealth of Australia","Australia","Canberra",26000000,7692024.0,"Oceania","Australia and New Zealand","English","English","AUD","Australian Dollar",-27.0,133.0,"Oceania",34.4,2018,"🇦🇺"),
        ("BR","BRA","Federative Republic of Brazil","Brazil","Brasília",214000000,8515767.0,"Americas","South America","Portuguese","Portuguese","BRL","Brazilian Real",-10.0,-55.0,"South America",48.9,2020,"🇧🇷"),
        ("CA","CAN","Canada","Canada","Ottawa",38000000,9984670.0,"Americas","Northern America","English","English, French","CAD","Canadian Dollar",60.0,-95.0,"North America",33.3,2017,"🇨🇦"),
        ("CN","CHN","People's Republic of China","China","Beijing",1412000000,9596961.0,"Asia","Eastern Asia","Chinese","Chinese","CNY","Chinese Yuan",35.0,105.0,"Asia",38.2,2019,"🇨🇳"),
        ("DK","DNK","Kingdom of Denmark","Denmark","Copenhagen",5857000,43094.0,"Europe","Northern Europe","Danish","Danish","DKK","Danish Krone",56.0,10.0,"Europe",28.2,2019,"🇩🇰"),
        ("FR","FRA","French Republic","France","Paris",67750000,551695.0,"Europe","Western Europe","French","French","EUR","Euro",46.0,2.0,"Europe",32.4,2019,"🇫🇷"),
        ("DE","DEU","Federal Republic of Germany","Germany","Berlin",83200000,357114.0,"Europe","Western Europe","German","German","EUR","Euro",51.0,9.0,"Europe",31.9,2019,"🇩🇪"),
        ("IN","IND","Republic of India","India","New Delhi",1408000000,3287263.0,"Asia","Southern Asia","Hindi","Hindi, English","INR","Indian Rupee",20.0,77.0,"Asia",35.7,2019,"🇮🇳"),
        ("IE","IRL","Republic of Ireland","Ireland","Dublin",5030000,70273.0,"Europe","Northern Europe","Irish","Irish, English","EUR","Euro",53.0,-8.0,"Europe",31.4,2018,"🇮🇪"),
        ("IL","ISR","State of Israel","Israel","Jerusalem",9364000,20770.0,"Asia","Western Asia","Hebrew","Hebrew, Arabic","ILS","Israeli New Shekel",31.5,34.75,"Asia",39.0,2018,"🇮🇱"),
        ("IT","ITA","Italian Republic","Italy","Rome",59110000,301336.0,"Europe","Southern Europe","Italian","Italian","EUR","Euro",42.83,12.83,"Europe",35.9,2019,"🇮🇹"),
        ("JP","JPN","Japan","Japan","Tokyo",125700000,377975.0,"Asia","Eastern Asia","Japanese","Japanese","JPY","Japanese Yen",36.0,138.0,"Asia",32.9,2018,"🇯🇵"),
        ("KE","KEN","Republic of Kenya","Kenya","Nairobi",54000000,580367.0,"Africa","Eastern Africa","Swahili","Swahili, English","KES","Kenyan Shilling",1.0,38.0,"Africa",40.8,2019,"🇰🇪"),
        ("MX","MEX","United Mexican States","Mexico","Mexico City",128900000,1964375.0,"Americas","Central America","Spanish","Spanish","MXN","Mexican Peso",23.0,-102.0,"North America",45.4,2020,"🇲🇽"),
        ("NL","NLD","Kingdom of the Netherlands","Netherlands","Amsterdam",17590000,41850.0,"Europe","Western Europe","Dutch","Dutch","EUR","Euro",52.5,5.75,"Europe",28.1,2019,"🇳🇱"),
        ("NG","NGA","Federal Republic of Nigeria","Nigeria","Abuja",218000000,923768.0,"Africa","Western Africa","English","English","NGN","Nigerian Naira",10.0,8.0,"Africa",35.1,2018,"🇳🇬"),
        ("NO","NOR","Kingdom of Norway","Norway","Oslo",5408000,323802.0,"Europe","Northern Europe","Norwegian","Norwegian","NOK","Norwegian Krone",62.0,10.0,"Europe",27.6,2019,"🇳🇴"),
        ("PL","POL","Republic of Poland","Poland","Warsaw",37750000,312679.0,"Europe","Central Europe","Polish","Polish","PLN","Polish Zloty",52.0,20.0,"Europe",30.2,2019,"🇵🇱"),
        ("PT","PRT","Portuguese Republic","Portugal","Lisbon",10310000,92090.0,"Europe","Southern Europe","Portuguese","Portuguese","EUR","Euro",39.5,-8.0,"Europe",33.5,2019,"🇵🇹"),
        ("SG","SGP","Republic of Singapore","Singapore","Singapore",5454000,710.0,"Asia","South-Eastern Asia","English","English, Malay, Mandarin, Tamil","SGD","Singapore Dollar",1.37,103.8,"Asia",None,None,"🇸🇬"),
        ("ZA","ZAF","Republic of South Africa","South Africa","Pretoria",60000000,1221037.0,"Africa","Southern Africa","Zulu","Zulu, Xhosa, Afrikaans, English","ZAR","South African Rand",-29.0,24.0,"Africa",63.0,2014,"🇿🇦"),
        ("KR","KOR","Republic of Korea","South Korea","Seoul",51780000,100210.0,"Asia","Eastern Asia","Korean","Korean","KRW","South Korean Won",37.0,127.5,"Asia",31.4,2019,"🇰🇷"),
        ("ES","ESP","Kingdom of Spain","Spain","Madrid",47350000,505992.0,"Europe","Southern Europe","Spanish","Spanish","EUR","Euro",40.0,-4.0,"Europe",34.7,2019,"🇪🇸"),
        ("SE","SWE","Kingdom of Sweden","Sweden","Stockholm",10350000,450295.0,"Europe","Northern Europe","Swedish","Swedish","SEK","Swedish Krona",62.0,15.0,"Europe",30.0,2019,"🇸🇪"),
        ("AE","ARE","United Arab Emirates","United Arab Emirates","Abu Dhabi",9890000,83600.0,"Asia","Western Asia","Arabic","Arabic","AED","UAE Dirham",24.0,54.0,"Asia",26.0,2018,"🇦🇪"),
        ("GB","GBR","United Kingdom of Great Britain and Northern Ireland","United Kingdom","London",67330000,242495.0,"Europe","Northern Europe","English","English","GBP","Pound Sterling",54.0,-2.0,"Europe",35.1,2019,"🇬🇧"),
        ("US","USA","United States of America","United States","Washington, D.C.",331900000,9833520.0,"Americas","Northern America","English","English","USD","United States Dollar",38.0,-97.0,"North America",41.4,2020,"🇺🇸"),
    ]
    fallback_rows = []
    for row in fallback_countries:
        fallback_rows.append({
            "CountryCode2": row[0], "CountryCode3": row[1],
            "CountryNameOfficial": row[2], "CountryNameCommon": row[3],
            "Capital": row[4], "Population": row[5], "AreaSqKm": row[6],
            "RegionWorld": row[7], "SubRegionWorld": row[8],
            "PrimaryLanguage": row[9], "AllLanguages": row[10],
            "PrimaryCurrencyCode": row[11], "PrimaryCurrencyName": row[12],
            "Latitude": row[13], "Longitude": row[14], "Continent": row[15],
            "GiniIndex": row[16], "GiniYear": row[17], "FlagEmoji": row[18]
        })
    schema_countries = StructType([
        StructField("CountryCode2", StringType(), True),
        StructField("CountryCode3", StringType(), True),
        StructField("CountryNameOfficial", StringType(), True),
        StructField("CountryNameCommon", StringType(), True),
        StructField("Capital", StringType(), True),
        StructField("Population", IntegerType(), True),
        StructField("AreaSqKm", DoubleType(), True),
        StructField("RegionWorld", StringType(), True),
        StructField("SubRegionWorld", StringType(), True),
        StructField("PrimaryLanguage", StringType(), True),
        StructField("AllLanguages", StringType(), True),
        StructField("PrimaryCurrencyCode", StringType(), True),
        StructField("PrimaryCurrencyName", StringType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("Continent", StringType(), True),
        StructField("GiniIndex", DoubleType(), True),
        StructField("GiniYear", IntegerType(), True),
        StructField("FlagEmoji", StringType(), True)
    ])
    df_countries = spark.createDataFrame(fallback_rows, schema=schema_countries)
    df_countries = df_countries.withColumn("_fetched_at", current_timestamp())
    df_countries = df_countries.withColumn("_source", lit("fallback_static"))
    df_countries = df_countries.withColumn(
        "PopulationDensity",
        when(col("AreaSqKm") > 0,
             spark_round(col("Population") / col("AreaSqKm"), 2)
        ).otherwise(lit(None))
    )
    df_countries = df_countries.withColumn(
        "EconomicSizeCategory",
        when(col("Population") >= 100_000_000, lit("Large"))
        .when(col("Population") >= 20_000_000, lit("Medium"))
        .when(col("Population") >= 1_000_000, lit("Small"))
        .otherwise(lit("Micro"))
    )
    df_countries.write.mode("overwrite").format("delta").saveAsTable(web_table("WebCountryIndicators"))
    print(f"  ✓ WebCountryIndicators (fallback): {df_countries.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 5: Fetch book metadata from Open Library API
# -----------------------------------------------------------
# https://openlibrary.org — free, no API key required
# Enriches DimBooks with real-world metadata (subjects, ratings, etc.)

print("\n" + "="*60)
print("  Fetching Book Metadata (Open Library API)")
print("="*60)

# Read current DimBooks from Silver layer for ISBNs
try:
    df_books = spark.table(silver_table("DimBooks"))
    isbn_list = [row.ISBN for row in df_books.select("ISBN").distinct().collect() if row.ISBN]
    print(f"  Found {len(isbn_list)} ISBNs to look up")
except Exception as e:
    isbn_list = []
    print(f"  ⚠ Could not read DimBooks: {e}")

book_metadata_rows = []
for isbn in isbn_list:
    # Clean ISBN (remove hyphens)
    isbn_clean = isbn.replace("-", "")
    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn_clean}&format=json&jscmd=data"
    data = safe_request(url, delay=0.5)

    if data:
        key = f"ISBN:{isbn_clean}"
        if key in data:
            book = data[key]
            subjects = [s.get("name", "") for s in book.get("subjects", [])[:5]]
            publishers = [p.get("name", "") for p in book.get("publishers", [])[:1]]

            book_metadata_rows.append({
                "ISBN": isbn,
                "ISBNClean": isbn_clean,
                "OL_Title": book.get("title"),
                "OL_Subtitle": book.get("subtitle"),
                "OL_NumberOfPages": book.get("number_of_pages"),
                "OL_Publishers": publishers[0] if publishers else None,
                "OL_PublishDate": book.get("publish_date"),
                "OL_Subjects": "; ".join(subjects) if subjects else None,
                "OL_SubjectCount": len(book.get("subjects", [])),
                "OL_CoverURL": book.get("cover", {}).get("medium"),
                "OL_URL": book.get("url"),
                "OL_Weight": book.get("weight"),
                "HasOpenLibraryEntry": True
            })
            print(f"  ✓ {isbn}: \"{book.get('title', 'N/A')}\"")
        else:
            book_metadata_rows.append({
                "ISBN": isbn, "ISBNClean": isbn_clean,
                "OL_Title": None, "OL_Subtitle": None, "OL_NumberOfPages": None,
                "OL_Publishers": None, "OL_PublishDate": None, "OL_Subjects": None,
                "OL_SubjectCount": 0, "OL_CoverURL": None, "OL_URL": None,
                "OL_Weight": None, "HasOpenLibraryEntry": False
            })
            print(f"  ○ {isbn}: not found in Open Library")
    else:
        book_metadata_rows.append({
            "ISBN": isbn, "ISBNClean": isbn_clean,
            "OL_Title": None, "OL_Subtitle": None, "OL_NumberOfPages": None,
            "OL_Publishers": None, "OL_PublishDate": None, "OL_Subjects": None,
            "OL_SubjectCount": 0, "OL_CoverURL": None, "OL_URL": None,
            "OL_Weight": None, "HasOpenLibraryEntry": False
        })
    time.sleep(0.3)

if book_metadata_rows:
    schema_books_web = StructType([
        StructField("ISBN", StringType(), False),
        StructField("ISBNClean", StringType(), True),
        StructField("OL_Title", StringType(), True),
        StructField("OL_Subtitle", StringType(), True),
        StructField("OL_NumberOfPages", IntegerType(), True),
        StructField("OL_Publishers", StringType(), True),
        StructField("OL_PublishDate", StringType(), True),
        StructField("OL_Subjects", StringType(), True),
        StructField("OL_SubjectCount", IntegerType(), True),
        StructField("OL_CoverURL", StringType(), True),
        StructField("OL_URL", StringType(), True),
        StructField("OL_Weight", StringType(), True),
        StructField("HasOpenLibraryEntry", BooleanType(), True)
    ])
    df_books_web = spark.createDataFrame(book_metadata_rows, schema=schema_books_web)
    df_books_web = df_books_web.withColumn("_fetched_at", current_timestamp())
    df_books_web = df_books_web.withColumn("_source", lit("openlibrary.org"))

    df_books_web.write.mode("overwrite").format("delta").saveAsTable(web_table("WebBookMetadata"))
    found_count = len([r for r in book_metadata_rows if r["HasOpenLibraryEntry"]])
    print(f"\n  ✓ WebBookMetadata saved: {df_books_web.count()} rows "
          f"({found_count} found, {len(book_metadata_rows) - found_count} not found)")
else:
    print("  ⚠ No book metadata fetched")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 6: Enrich DimGeography with country indicators
# -----------------------------------------------------------

print("\n" + "="*60)
print("  Enriching DimGeography with Country Indicators")
print("="*60)

try:
    df_geo = spark.table(silver_table("DimGeography"))
    df_indicators = spark.table(web_table("WebCountryIndicators"))

    # Map Horizon Books country names to match REST Countries common names
    # (most should match directly via fuzzy join on country name)
    geo_enriched = df_geo.join(
        broadcast(df_indicators.select(
            col("CountryNameCommon").alias("_match_country"),
            col("Population").alias("CountryPopulation"),
            col("AreaSqKm"),
            col("PopulationDensity"),
            col("PrimaryLanguage"),
            col("AllLanguages"),
            col("GiniIndex"),
            col("EconomicSizeCategory"),
            col("CountryCode2"),
            col("CountryCode3"),
            col("Capital").alias("CountryCapital"),
            col("FlagEmoji")
        )),
        col("Country") == col("_match_country"),
        "left"
    ).drop("_match_country")

    geo_enriched.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(silver_table("DimGeography"))
    enriched_count = geo_enriched.filter(col("CountryPopulation").isNotNull()).count()
    print(f"  ✓ DimGeography enriched: {geo_enriched.count()} rows "
          f"({enriched_count} matched with indicators)")
    print(f"    Added: CountryPopulation, AreaSqKm, PopulationDensity, PrimaryLanguage, "
          f"GiniIndex, EconomicSizeCategory, CountryCode2/3, FlagEmoji")
except Exception as e:
    print(f"  ⚠ Could not enrich DimGeography: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 7: Normalize FactFinancialTransactions to USD
# -----------------------------------------------------------

print("\n" + "="*60)
print("  Normalizing Financial Transactions to USD")
print("="*60)

try:
    df_fin = spark.table(silver_table("FactFinancialTransactions"))
    df_rates = spark.table(web_table("WebExchangeRates"))

    # Join transactions to exchange rates on currency and month
    df_fin_with_month = df_fin.withColumn(
        "_txn_month", month(col("TransactionDate"))
    ).withColumn(
        "_txn_year", year(col("TransactionDate"))
    )

    # Get one rate per currency per month
    monthly_rates = get_monthly_rates(df_rates)

    # Join: transactions currency → rate for that month
    df_fin_normalized = df_fin_with_month.join(
        broadcast(monthly_rates),
        (df_fin_with_month["Currency"] == monthly_rates["TargetCurrency"]) &
        (df_fin_with_month["_txn_month"] == monthly_rates["RateMonth"]) &
        (df_fin_with_month["_txn_year"] == monthly_rates["RateYear"]),
        "left"
    )

    # Calculate USD-normalized amounts
    df_fin_normalized = (df_fin_normalized
        .withColumn("ExchangeRateToUSD", exchange_rate_to_usd("Currency"))
        .withColumn("AmountUSD", convert_to_usd("Amount", "Currency"))
        .drop("TargetCurrency", "RateMonth", "RateYear", "ExchangeRate",
               "_txn_month", "_txn_year")
    )

    df_fin_normalized.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(silver_table("FactFinancialTransactions"))
    multi_curr = df_fin_normalized.filter(col("Currency") != "USD").count()
    print(f"  ✓ FactFinancialTransactions: {df_fin_normalized.count()} rows")
    print(f"    Added: ExchangeRateToUSD, AmountUSD")
    print(f"    Non-USD transactions normalized: {multi_curr}")
except Exception as e:
    print(f"  ⚠ Could not normalize transactions: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 8: Normalize FactOrders to USD
# -----------------------------------------------------------

print("\n" + "="*60)
print("  Normalizing FactOrders to USD")
print("="*60)

try:
    df_orders = spark.table(silver_table("FactOrders"))
    df_rates = spark.table(web_table("WebExchangeRates"))
    df_customers = spark.table(silver_table("DimCustomers"))
    df_geo = spark.table(silver_table("DimGeography"))

    # We need to determine each order's currency from the customer's geography
    # Join: Order → Customer → Geography → Currency
    cust_currency = df_customers.join(
        df_geo.select("GeoID", "Currency"),
        "GeoID", "left"
    ).select(
        col("CustomerID"),
        col("Currency").alias("CustomerCurrency")
    )

    df_orders_curr = df_orders.join(broadcast(cust_currency), "CustomerID", "left")
    df_orders_curr = df_orders_curr.withColumn(
        "OrderCurrency", coalesce(col("CustomerCurrency"), lit("USD"))
    )

    # Get monthly rates
    monthly_rates = get_monthly_rates(df_rates)

    # Add month/year for join
    df_orders_curr = df_orders_curr.withColumn(
        "_ord_month", month(col("OrderDate"))
    ).withColumn(
        "_ord_year", year(col("OrderDate"))
    )

    # Join to rates
    df_orders_norm = df_orders_curr.join(
        broadcast(monthly_rates),
        (df_orders_curr["OrderCurrency"] == monthly_rates["TargetCurrency"]) &
        (df_orders_curr["_ord_month"] == monthly_rates["RateMonth"]) &
        (df_orders_curr["_ord_year"] == monthly_rates["RateYear"]),
        "left"
    )

    # Calculate USD amounts
    df_orders_norm = (df_orders_norm
        .withColumn("ExchangeRateToUSD", exchange_rate_to_usd("OrderCurrency"))
        .withColumn("TotalAmountUSD", convert_to_usd("TotalAmount", "OrderCurrency"))
        .withColumn("UnitPriceUSD", convert_to_usd("UnitPrice", "OrderCurrency"))
        .drop("TargetCurrency", "RateMonth", "RateYear", "ExchangeRate",
               "_ord_month", "_ord_year", "CustomerCurrency")
    )

    df_orders_norm.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(silver_table("FactOrders"))
    intl = df_orders_norm.filter(col("OrderCurrency") != "USD").count()
    print(f"  ✓ FactOrders: {df_orders_norm.count()} rows")
    print(f"    Added: OrderCurrency, ExchangeRateToUSD, TotalAmountUSD, UnitPriceUSD")
    print(f"    International orders normalized: {intl}")
except Exception as e:
    print(f"  ⚠ Could not normalize orders: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------
# Cell 9: Summary of web enrichment
# -----------------------------------------------------------

print("\n" + "="*60)
print("  WEB ENRICHMENT SUMMARY")
print("="*60)

web_tables = [
    web_table("WebExchangeRates"), web_table("WebPublicHolidays"),
    web_table("WebCountryIndicators"), web_table("WebBookMetadata")
]

for t in web_tables:
    try:
        df = spark.table(t)
        print(f"  ✓ {t:30s} {df.count():>6,} rows  {len(df.columns):>3} cols")
    except Exception as e:
        print(f"  ✗ {t:30s} NOT CREATED")

print("\n  Enriched Silver tables:")
enriched_tables = [silver_table("DimGeography"), silver_table("FactFinancialTransactions"), silver_table("FactOrders")]
for t in enriched_tables:
    try:
        df = spark.table(t)
        print(f"  ✓ {t:30s} {df.count():>6,} rows  {len(df.columns):>3} cols")
    except Exception as e:
        print(f"  ✗ {t:30s} ERROR: {e}")

print("\n=== Web Enrichment Complete ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
