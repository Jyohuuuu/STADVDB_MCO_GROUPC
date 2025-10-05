import pandas as pd
import xml.etree.ElementTree as ET

# read csv
quality_data = pd.read_csv('Quality_of_Life-2.csv')
# read xlsx
gdp_data = pd.read_excel('2020-2025-2.xlsx')

# --- XML parsing logic ---
tree = ET.parse('API_SP.POP.TOTL_DS2_en_xml_v2_1021474-2.xml')
root = tree.getroot()

data = []
for record in root.findall('./data/record'):
    record_data = {}
    for field in record.findall('field'):
        if field.attrib['name'] == 'Item':
            continue
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
