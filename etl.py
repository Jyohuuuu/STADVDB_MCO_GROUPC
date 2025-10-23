import pandas as pd
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, text

# --- Country name normalization map ---
name_map = {
    "bahamas": "bahamas, the",
    "brunei": "brunei darussalam",
    "cape verde": "cabo verde",
    "democratic republic of the congo": "congo, dem. rep.",
    "republic of the congo": "congo, rep.",
    "ivory coast": "cote d'ivoire",
    "czech republic": "czechia",
    "egypt": "egypt, arab rep.",
    "gambia": "gambia, the",
    "hong kong": "hong kong sar, china",
    "hong kong (china)": "hong kong sar, china",
    "iran": "iran, islamic rep.",
    "south korea": "korea, rep.",
    "kosovo (disputed territory)": "kosovo",
    "kyrgyzstan": "kyrgyz republic",
    "laos": "lao pdr",
    "macau": "macao sar, china",
    "macao": "macao sar, china",
    "macao (china)": "macao sar, china",
    "micronesia": "micronesia, fed. sts.",
    "federated states of micronesia": "micronesia, fed. sts.",
    "puerto rico": "puerto rico (us)",
    "russia": "russian federation",
    "são tomé and príncipe": "sao tome and principe",
    "slovakia": "slovak republic",
    "saint kitts and nevis": "st. kitts and nevis",
    "saint lucia": "st. lucia",
    "saint vincent and the grenadines": "st. vincent and the grenadines",
    "syria": "syrian arab republic",
    "turkey": "turkiye",
    "venezuela": "venezuela, rb",
    "vietnam": "viet nam",
    "yemen": "yemen, rep."
}

def normalize_country(name):
    """Standardize and map country names to a common lowercase form."""
    if not isinstance(name, str):
        return name
    n = name.strip().lower()
    return name_map.get(n, n)

# --- Read CSV and Excel ---
quality_data = pd.read_csv('Quality_of_Life-2.csv')
gdp_data = pd.read_excel('2020-2025-2.xlsx')

# --- Melt GDP data if wide format ---
if 'Country' in gdp_data.columns:
    gdp_data = pd.melt(gdp_data, id_vars=['Country'], var_name='Year', value_name='Value')
    gdp_data.rename(columns={'Country': 'Country Name'}, inplace=True)
else:
    if 'Country Name' not in gdp_data.columns and 'Country' in gdp_data.columns:
        gdp_data.rename(columns={'Country': 'Country Name'}, inplace=True)

# --- Parse XML population file ---
tree = ET.parse('API_SP.POP.TOTL_DS2_en_xml_v2_1021474-2.xml')
root = tree.getroot()

data = []
for record in root.findall('./data/record'):
    record_data = {}
    for field in record.findall('field'):
        name = field.attrib.get('name')
        if name == 'Item':
            continue
        if name == 'Country or Area':
            record_data['Country or Area'] = field.text
            record_data['Country Code'] = field.attrib.get('key')
        else:
            record_data[name] = field.text
    data.append(record_data)

pop_data = pd.DataFrame(data)
if 'Value' in pop_data.columns:
    pop_data.rename(columns={'Value': 'Population'}, inplace=True)

# --- Normalize country names across datasets ---
quality_data['country_norm'] = quality_data['country'].apply(normalize_country)
gdp_data['country_norm'] = gdp_data['Country Name'].apply(normalize_country)
pop_data['country_norm'] = pop_data['Country or Area'].apply(normalize_country)

# --- Transfer to Local SQL (staging) ---
username = "root"
password = "password"
host = "localhost"
database = "source_database"
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{database}')

quality_data.to_sql('quality_of_life', con=engine, if_exists='replace', index=False)
gdp_data.to_sql('gdp', con=engine, if_exists='replace', index=False)
pop_data.to_sql('population', con=engine, if_exists='replace', index=False)

# --- Read back from staging database ---
quality_df = pd.read_sql('quality_of_life', con=engine)
gdp_df = pd.read_sql('gdp', con=engine)
pop_df = pd.read_sql('population', con=engine)

if 'country_norm' not in quality_df.columns:
    quality_df['country_norm'] = quality_df['country'].apply(normalize_country)
if 'country_norm' not in gdp_df.columns:
    gdp_df['country_norm'] = gdp_df['Country Name'].apply(normalize_country)
if 'country_norm' not in pop_df.columns:
    pop_df['country_norm'] = pop_df['Country or Area'].apply(normalize_country)

print("--- Preview ---")
print("Unique countries → Quality:", quality_df['country_norm'].nunique(),
      " | GDP:", gdp_df['country_norm'].nunique(),
      " | Population:", pop_df['country_norm'].nunique())

# --- dim_country ---
all_countries = gdp_df[['country_norm']].drop_duplicates().reset_index(drop=True).rename(columns={'country_norm': 'country_name'})
all_countries['country_key'] = all_countries.index + 1

country_codes = pop_df[['country_norm', 'Country Code']].drop_duplicates().rename(columns={'country_norm': 'country_name'})
all_countries = pd.merge(all_countries, country_codes, how='left', on='country_name')
all_countries.rename(columns={'Country Code': 'country_code'}, inplace=True)

# --- Add Kosovo manually if missing ---
if 'kosovo' not in all_countries['country_name'].values:
    new_key = all_countries['country_key'].max() + 1
    all_countries = pd.concat([
        all_countries,
        pd.DataFrame([{'country_key': new_key, 'country_name': 'kosovo', 'country_code': 'XKX'}])
    ], ignore_index=True)

dim_country = all_countries[['country_key', 'country_name', 'country_code']].copy()

# --- dim_time ---
gdp_df['Year'] = pd.to_numeric(gdp_df['Year'], errors='coerce')
pop_df['Year'] = pd.to_numeric(pop_df['Year'], errors='coerce')

# 🔍 DEBUG: Check what years are in each dataset
print("GDP years:", sorted(gdp_df['Year'].dropna().unique()))
print("POP years:", sorted(pop_df['Year'].dropna().unique()))

all_years = sorted(set(gdp_df['Year'].dropna().unique()) | set(pop_df['Year'].dropna().unique()) | {2025})
dim_time = pd.DataFrame({'time_key': all_years, 'year_value': all_years})
dim_time['is_historical'] = dim_time['year_value'] < 2025
dim_time['period_type'] = 'Annual'

# --- dim_quality_of_life ---
print("\n--- DEBUG: Raw Quality of Life Values ---")
print(quality_df[['country', 'Quality of Life Value']].head(15))

# Quality of Life Value has a weird format with colons that requires special handling
if 'Quality of Life Value' in quality_df.columns:
    def clean_quality_value(x):
        if pd.isna(x):
            return 0.0
        x_str = str(x).strip()
        
        if x_str.startswith(':') or ':' in x_str:
            cleaned = x_str.replace(':', '').strip()
            cleaned = cleaned.replace("'", "").strip()
            try:
                result = float(cleaned) if cleaned else 0.0
                return result
            except ValueError:
                return 0.0
        else:
            try:
                result = float(x_str) if x_str else 0.0
                return result
            except ValueError:
                return 0.0
    
    print("\n--- Cleaning Quality of Life Values ---")
    quality_df['Quality of Life Value'] = quality_df['Quality of Life Value'].apply(clean_quality_value)

other_numeric_cols = [
    'Purchasing Power Value', 'Safety Value', 'Health Care Value', 'Climate Value',
    'Cost of Living Value', 'Property Price to Income Value', 'Traffic Commute Time Value',
    'Pollution Value'
]

for col in other_numeric_cols:
    if col in quality_df.columns:
        quality_df[col] = (
            quality_df[col]
            .astype(str)
            .str.replace(',', '')
            .str.strip()
        )
        quality_df[col] = pd.to_numeric(quality_df[col], errors='coerce').fillna(0)

print("\n--- AFTER CLEANING Quality of Life Values ---")
print(quality_df[['country', 'Quality of Life Value']].head(15))
print(f"Non-zero values: {(quality_df['Quality of Life Value'] > 0).sum()}")
category_cols = [
    'Purchasing Power Category', 'Safety Category', 'Health Care Category', 
    'Climate Category', 'Cost of Living Category', 'Property Price to Income Category',
    'Traffic Commute Time Category', 'Pollution Category', 'Quality of Life Category'
]

for col in category_cols:
    if col in quality_df.columns:
        quality_df[col] = quality_df[col].replace('None', 'None')
        quality_df[col] = quality_df[col].fillna('None')

dim_quality_of_life = pd.merge(
    quality_df,
    dim_country.rename(columns={'country_name': 'country_name_norm'}),
    how='left',
    left_on='country_norm',
    right_on='country_name_norm'
)

dim_quality_of_life = dim_quality_of_life[dim_quality_of_life['country_key'].notna()].copy()
dim_quality_of_life = dim_quality_of_life.drop(columns=[c for c in ['country', 'country_norm', 'country_name_norm', 'country_code'] if c in dim_quality_of_life.columns])
dim_quality_of_life.rename(columns={
    'Purchasing Power Value': 'purchasing_power_value',
    'Safety Value': 'safety_value',
    'Health Care Value': 'health_care_value',
    'Climate Value': 'climate_value',
    'Cost of Living Value': 'cost_of_living_value',
    'Property Price to Income Value': 'property_price_income_value',
    'Traffic Commute Time Value': 'traffic_commute_value',
    'Pollution Value': 'pollution_value',
    'Quality of Life Value': 'quality_of_life_value',
    'Purchasing Power Category': 'purchasing_power_category',
    'Safety Category': 'safety_category',
    'Health Care Category': 'health_care_category',
    'Climate Category': 'climate_category',
    'Cost of Living Category': 'cost_of_living_category',
    'Property Price to Income Category': 'property_price_income_category',
    'Traffic Commute Time Category': 'traffic_commute_category',
    'Pollution Category': 'pollution_category',
    'Quality of Life Category': 'quality_of_life_category'
}, inplace=True)

# --- fact_country_metrics ---
pop_df['country_norm'] = pop_df['country_norm'].astype(str)
gdp_df['country_norm'] = gdp_df['country_norm'].astype(str)

fact_df = pd.merge(
    pop_df,
    gdp_df,
    how='right', 
    left_on=['country_norm', 'Year'],
    right_on=['country_norm', 'Year'],
    suffixes=('_pop', '_gdp')
)

fact_df = pd.merge(
    fact_df,
    dim_country.rename(columns={'country_name': 'country_norm'}),
    how='left',
    left_on='country_norm',
    right_on='country_norm'
)
fact_df = pd.merge(fact_df, dim_time, how='left', left_on='Year', right_on='year_value')

fact_df.rename(columns={'Population': 'population', 'Value': 'gdp_usd'}, inplace=True)
fact_df['population'] = pd.to_numeric(fact_df['population'], errors='coerce').fillna(0)
fact_df['gdp_usd'] = pd.to_numeric(fact_df['gdp_usd'], errors='coerce').fillna(0)
fact_df['gdp_per_capita'] = fact_df.apply(
    lambda r: (r['gdp_usd'] * 1_000_000) / r['population'] if (r.get('population', 0) and r['population'] > 0) else 0,
    axis=1
)
fact_country_metrics = fact_df[['country_key', 'time_key', 'gdp_usd', 'population', 'gdp_per_capita']].copy()

missing_fk = fact_country_metrics[fact_country_metrics['country_key'].isna()]
if not missing_fk.empty:
    print("\n--- WARNING: Missing country_key in fact_country_metrics ---")
    print(missing_fk.head())

# --- Final cleanup and load ---
dim_country['country_name'] = dim_country['country_name'].str.title()

dw_username = "root"
dw_password = "password"
dw_host = "localhost"
dw_database = "country_data_warehouse"
dw_engine = create_engine(f'mysql+pymysql://{dw_username}:{dw_password}@{dw_host}/{dw_database}')

with dw_engine.connect() as connection:
    with connection.begin():
        connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        connection.execute(text("TRUNCATE TABLE fact_country_metrics;"))
        connection.execute(text("TRUNCATE TABLE dim_quality_of_life;"))
        connection.execute(text("TRUNCATE TABLE dim_country;"))
        connection.execute(text("TRUNCATE TABLE dim_time;"))
        connection.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

dim_country.to_sql('dim_country', con=dw_engine, if_exists='append', index=False)
dim_time.to_sql('dim_time', con=dw_engine, if_exists='append', index=False)
dim_quality_of_life.to_sql('dim_quality_of_life', con=dw_engine, if_exists='append', index=False)
fact_country_metrics.to_sql('fact_country_metrics', con=dw_engine, if_exists='append', index=False)

print("\n--- Data loaded into data warehouse successfully! ---")



