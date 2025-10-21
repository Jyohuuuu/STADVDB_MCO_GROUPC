import pandas as pd
from sqlalchemy import create_engine, text

dw_username = "root"
dw_password = "admin"
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

def cost_of_living_vs_purchasing_power_report():
    query = text("""
    SELECT 
        t.year_value,
        c.country_name,
        AVG(q.cost_of_living_value) AS avg_cost_of_living,
        AVG(q.purchasing_power_value) AS avg_purchasing_power,
        ROUND(AVG(q.cost_of_living_value) / NULLIF(AVG(q.purchasing_power_value), 0), 2) AS avg_inflation_pressure_ratio
        FROM fact_country_metrics f
    JOIN dim_country c ON f.country_key = c.country_key
    JOIN dim_time t ON f.time_key = t.time_key
    JOIN dim_quality_of_life q ON f.country_key = q.country_key
    GROUP BY ROLLUP (t.year_value, c.country_name)
    ORDER BY t.year_value, c.country_name;   
    """)
    df = pd.read_sql(query, dw_engine)
    
    return df

def climate_quality_vs_economic_development_report():
    #OLAP USED: SLICE
    query = text("""
    SELECT 
        c.country_name,
        t.year_value,
        AVG(q.climate_value) AS climate_quality_2025,
        SUM(f.gdp_usd) AS total_gdp_usd,
        ROUND(SUM(f.gdp_usd) / NULLIF(AVG(q.climate_value), 0), 2) AS development_efficiency_ratio
    FROM fact_country_metrics f
    JOIN dim_country c ON f.country_key = c.country_key
    JOIN dim_time t ON f.time_key = t.time_key
    JOIN dim_quality_of_life q ON f.country_key = q.country_key
    WHERE t.year_value BETWEEN 2020 AND 2025
    GROUP BY c.country_name, t.year_value
    ORDER BY t.year_value, c.country_name; 
    """)
    df = pd.read_sql(query, dw_engine)
    
    return df

if __name__ == "__main__":
    df = gdp_population_correlation_report()
    print(df.head(5))