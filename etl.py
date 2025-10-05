import pandas as pd
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, text

# read csv
quality_data = pd.read_csv('Quality_of_Life-2.csv')
# read xlsx
gdp_data = pd.read_excel('2020-2025-2.xlsx')
gdp_data = pd.melt(gdp_data, id_vars=['Country'], var_name='Year', value_name='Value')
gdp_data.rename(columns={'Country': 'Country Name'}, inplace=True)


# --- XML parsing logic ---
tree = ET.parse('API_SP.POP.TOTL_DS2_en_xml_v2_1021474-2.xml')
root = tree.getroot()

data = []
for record in root.findall('./data/record'):
    record_data = {}
    for field in record.findall('field'):
        if field.attrib['name'] == 'Item':
            continue
        if field.attrib['name'] == 'Country or Area':
            record_data['Country or Area'] = field.text
            record_data['Country Code'] = field.attrib['key']
        else:
            record_data[field.attrib['name']] = field.text
    data.append(record_data)
    

pop_data = pd.DataFrame(data)
pop_data.rename(columns={'Value': 'Population'}, inplace=True)


print("--- Quality of Life Data Columns ---")
print(quality_data.columns)
print("\nData Preview:")
print(quality_data.head())
print("\n--- GDP Data Columns ---")
print(gdp_data.columns)
print("\nData Preview:")
print(gdp_data.head())
print("\n--- Population Data from XML ---")
print(f"Shape: {pop_data.shape}")
print("Columns:", pop_data.columns)
print("Data Preview:")
print(pop_data.head())

# Data extraction complete

# transfer data in local sql environment called source_database
# Replace with your actual MySQL credentials
username = "root"         # your MySQL username
password = "password" # your MySQL password
host = "localhost"
database = "source_database" 
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{database}')

quality_data.to_sql('quality_of_life', con=engine, if_exists='replace', index=False)
gdp_data.to_sql('gdp', con=engine, if_exists='replace', index=False)
pop_data.to_sql('population', con=engine, if_exists='replace', index=False)


# then transform and load to target database warehouse

# --- Transformation and Loading ---

# Credentials for the data warehouse
dw_username = "root"
dw_password = "Chem123!!"
dw_host = "localhost"
dw_database = "country_data_warehouse"
dw_engine = create_engine(f'mysql+pymysql://{dw_username}:{dw_password}@{dw_host}/{dw_database}')

# Read data from the staging database
print("\n--- Reading data from staging database ---")
quality_df = pd.read_sql('quality_of_life', con=engine)
gdp_df = pd.read_sql('gdp', con=engine)
pop_df = pd.read_sql('population', con=engine)
print("--- Staging data read successfully ---")

# --- Transformations ---
# create dim_country
print("Creating dim_country...")
country_names_gdp = gdp_df[['Country Name']].rename(columns={'Country Name': 'country_name'}).drop_duplicates()
country_names_qol = quality_df[['country']].rename(columns={'country': 'country_name'}).drop_duplicates()
country_names_pop = pop_df[['Country or Area']].rename(columns={'Country or Area': 'country_name'}).drop_duplicates()
all_countries = pd.concat([country_names_qol, country_names_pop, country_names_gdp]).drop_duplicates().reset_index(drop=True)
all_countries['country_key'] = all_countries.index + 1

country_codes = pop_df[['Country or Area', 'Country Code']].drop_duplicates()
all_countries = pd.merge(all_countries, country_codes, how='left', left_on='country_name', right_on='Country or Area')
all_countries.drop(columns=['Country or Area'], inplace=True)
all_countries.rename(columns={'Country Code': 'country_code'}, inplace=True)

dim_country = all_countries[['country_key', 'country_name', 'country_code']]
dim_country['region'] = None
dim_country['continent'] = None
print("dim_country created.")

# create dim_time
print("Creating dim_time...")
gdp_df['Year'] = pd.to_numeric(gdp_df['Year'], errors='coerce').dropna()
pop_df['Year'] = pd.to_numeric(pop_df['Year'], errors='coerce').dropna()

years_gdp = gdp_df['Year'].unique()
years_pop = pop_df['Year'].unique()
all_years = pd.Series(list(set(years_gdp) | set(years_pop))).unique()
dim_time = pd.DataFrame({'time_key': all_years, 'year_value': all_years})
dim_time['is_historical'] = dim_time['year_value'] < 2025 # Assuming current year is 2025
dim_time['period_type'] = 'Annual'
print("dim_time created.")

# create dim_quality_of_life
print("Creating dim_quality_of_life...")
# clean data
numeric_cols = [
    'Purchasing Power Value', 'Safety Value', 'Health Care Value', 'Climate Value',
    'Cost of Living Value', 'Property Price to Income Value', 'Traffic Commute Time Value',
    'Pollution Value', 'Quality of Life Value'
]

for col in numeric_cols:
    quality_df[col] = quality_df[col].astype(str).str.replace(',', '').str.replace(r'^\': \s*', '', regex=True)
    quality_df[col] = pd.to_numeric(quality_df[col], errors='coerce').fillna(0.0)

dim_quality_of_life = pd.merge(quality_df, dim_country, how='inner', left_on='country', right_on='country_name')
dim_quality_of_life = dim_quality_of_life.drop(columns=['country', 'country_name', 'country_code', 'region', 'continent'])
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
    'Quality of Life Category': 'quality_of_life_category',
}, inplace=True)
dim_quality_of_life['quality_tier'] = None # Placeholder
dim_quality_of_life['development_status'] = None # Placeholder
dim_quality_of_life = dim_quality_of_life[['country_key', 'purchasing_power_value', 'safety_value', 'health_care_value', 'climate_value', 'cost_of_living_value', 'property_price_income_value', 'traffic_commute_value', 'pollution_value', 'quality_of_life_value', 'purchasing_power_category', 'safety_category', 'health_care_category', 'climate_category', 'cost_of_living_category', 'property_price_income_category', 'traffic_commute_category', 'pollution_category', 'quality_of_life_category', 'quality_tier', 'development_status']]
print("dim_quality_of_life created.")

# Create fact_country_metrics
print("Creating fact_country_metrics...")
pop_df['Year'] = pop_df['Year'].astype(int)
gdp_df['Year'] = gdp_df['Year'].astype(int)
fact_country_metrics = pd.merge(pop_df, gdp_df, how='inner', left_on=['Country or Area', 'Year'], right_on=['Country Name', 'Year'])
fact_country_metrics = pd.merge(fact_country_metrics, dim_country, how='inner', left_on='Country or Area', right_on='country_name')
fact_country_metrics = pd.merge(fact_country_metrics, dim_time, how='inner', left_on='Year', right_on='year_value')

fact_country_metrics.rename(columns={'Population': 'population', 'Value': 'gdp_usd'}, inplace=True)
fact_country_metrics['population'] = pd.to_numeric(fact_country_metrics['population'], errors='coerce').fillna(0)
fact_country_metrics['gdp_usd'] = pd.to_numeric(fact_country_metrics['gdp_usd'], errors='coerce').fillna(0)
fact_country_metrics['gdp_per_capita'] = fact_country_metrics.apply(lambda row: (row['gdp_usd'] * 1000000) / (row['population']) if row['population'] > 0 else 0, axis=1)
fact_country_metrics = fact_country_metrics[['country_key', 'time_key', 'gdp_usd', 'population', 'gdp_per_capita']]
print("fact_country_metrics created.")

print("--- Data transformation complete ---")

# load data into data warehouse
print("\n--- Loading data into data warehouse ---")
with dw_engine.connect() as connection:
    with connection.begin():
        connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        connection.execute(text("TRUNCATE TABLE fact_country_metrics;"))
        connection.execute(text("TRUNCATE TABLE dim_quality_of_life;"))
        connection.execute(text("TRUNCATE TABLE dim_country;"))
        connection.execute(text("TRUNCATE TABLE dim_time;"))
        connection.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

dim_country.to_sql('dim_country', con=dw_engine, if_exists='append', index=False)
print("dim_country loaded.")
dim_time.to_sql('dim_time', con=dw_engine, if_exists='append', index=False)
print("dim_time loaded.")
dim_quality_of_life.to_sql('dim_quality_of_life', con=dw_engine, if_exists='append', index=False)
print("dim_quality_of_life loaded.")
fact_country_metrics.to_sql('fact_country_metrics', con=dw_engine, if_exists='append', index=False)
print("fact_country_metrics loaded.")

print("--- Data loaded into data warehouse successfully! ---")