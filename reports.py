import pandas as pd
from sqlalchemy import create_engine, text

dw_username = "root"
dw_password = "Chem123!!"
dw_host = "localhost"
dw_database = "country_data_warehouse"
dw_engine = create_engine(f'mysql+pymysql://{dw_username}:{dw_password}@{dw_host}/{dw_database}')


def gdp_population_correlation_report():
    query = text("""
        SELECT c.country_name, f.population,  f.gdp_usd, f.time_key
        FROM fact_country_metrics f
         JOIN dim_country c ON f.country_key = c.country_key
        WHERE f.time_key = (SELECT MAX(time_key) FROM fact_country_metrics)
         order by f.time_key
                 
        
    """)
    df = pd.read_sql(query, dw_engine)
    
    return df

if __name__ == "__main__":
    df = gdp_population_correlation_report()
    print(df.head(5))