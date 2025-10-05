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
password = "Chem123!!" # your MySQL password
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
print("\n--- Transforming data ---")
print("for dim_country")
country_names_qol = quality_df[['country']].rename(columns={'country': 'country_name'}).drop_duplicates()
print(country_names_qol.head(5)) # testing purposes
country_names_pop = pop_df[['Country or Area']].rename(columns={'Country or Area': 'country_name'}).drop_duplicates()
print(country_names_pop.head(5)) # testing purposes
country_names_gdp = gdp_df[['Country Name']].rename(columns={'Country Name': 'country_name'}).drop_duplicates()
print(country_names_gdp.head(5)) # testing purposes
all_countries = pd.concat([country_names_qol, country_names_pop, country_names_gdp]).drop_duplicates().reset_index(drop=True)
all_countries['country_key'] = all_countries.index + 1
# im not sure what to do with the region and continent columns in the dim_country table yet

country_codes = pop_df[['Country or Area', 'Country Code']].drop_duplicates() 
all_countries = pd.merge(all_countries, country_codes, how='left', left_on='country_name', right_on='Country or Area')
print(all_countries.head(5)) # testing purposes
all_countries.drop(columns=['Country or Area'], inplace=True)
all_countries.rename(columns={'Country Code': 'country_code'}, inplace=True)

dim_country = all_countries[['country_key', 'country_name', 'country_code']]
dim_country['region'] = None
dim_country['continent'] = None


print("dim_country created.")

dim_country.to_sql('dim_country', con=dw_engine, if_exists='append', index=False)

