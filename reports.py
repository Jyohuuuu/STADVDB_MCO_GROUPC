import pandas as pd
from sqlalchemy import create_engine, text

dw_username = "root"
dw_password = "password"
dw_host = "localhost"
dw_database = "country_data_warehouse"
dw_engine = create_engine(f'mysql+pymysql://{dw_username}:{dw_password}@{dw_host}/{dw_database}')


def gdp_population_correlation_report():
    query = text("""
        SELECT 
            c.country_name, 
            f.population,  
            f.gdp_usd, 
            f.time_key
        FROM fact_country_metrics f
        JOIN dim_country c ON f.country_key = c.country_key
        WHERE f.time_key = (
            SELECT MAX(f2.time_key)
            FROM fact_country_metrics f2
            WHERE f2.gdp_usd > 0 AND f2.population > 0
        )
        ORDER BY f.time_key;
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
    GROUP BY t.year_value, c.country_name WITH ROLLUP
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

def traffic_commute_category_report():
    query = text("""
    SELECT q.traffic_commute_category,
       AVG(f.gdp_per_capita) AS avg_gdp_per_capita,
       SUM(f.population) AS total_population,
       CASE 
           WHEN q.traffic_commute_category LIKE '%Very High%' THEN 1
           WHEN q.traffic_commute_category LIKE '%High%' THEN 2
           WHEN q.traffic_commute_category LIKE '%Moderate%' THEN 3
           WHEN q.traffic_commute_category LIKE '%Low%' AND q.traffic_commute_category NOT LIKE '%Very%' THEN 4
           WHEN q.traffic_commute_category LIKE '%Very Low%' THEN 5
           ELSE 6
       END AS sort_order
FROM fact_country_metrics f
JOIN dim_quality_of_life q ON f.country_key = q.country_key
GROUP BY q.traffic_commute_category
ORDER BY sort_order;

    """)
    df = pd.read_sql(query, dw_engine)
    
    return df

def quality_of_life_by_region_report():
    query = text("""
        SELECT 
            CASE
                -- 🌍 Africa
                WHEN c.country_name IN (
                    'Algeria','Angola','Benin','Botswana','Burkina Faso','Burundi',
                    'Cabo Verde','Cameroon','Central African Republic','Chad','Comoros',
                    'Congo, Dem. Rep.','Congo, Rep.','Cote D''Ivoire','Djibouti','Egypt, Arab Rep.',
                    'Equatorial Guinea','Eritrea','Eswatini','Ethiopia','Gabon','Gambia, The',
                    'Ghana','Guinea','Guinea-Bissau','Kenya','Lesotho','Liberia','Libya',
                    'Madagascar','Malawi','Mali','Mauritania','Mauritius','Morocco','Mozambique',
                    'Namibia','Niger','Nigeria','Rwanda','Sao Tome And Principe','Senegal',
                    'Seychelles','Sierra Leone','Somalia','South Africa','South Sudan','Sudan',
                    'Tanzania','Togo','Tunisia','Uganda','Zambia','Zimbabwe'
                ) THEN 'Africa'

                -- 🌏 Asia
                WHEN c.country_name IN (
                    'Afghanistan','Armenia','Azerbaijan','Bahrain','Bangladesh','Bhutan',
                    'Brunei Darussalam','Cambodia','China','Georgia','Hong Kong Sar, China',
                    'India','Indonesia','Iran, Islamic Rep.','Iraq','Israel','Japan','Jordan',
                    'Kazakhstan','Korea, Rep.','Kuwait','Kyrgyz Republic','Lao Pdr','Lebanon',
                    'Malaysia','Maldives','Macao Sar, China','Mongolia','Myanmar','Nepal','Oman',
                    'Pakistan','Palestine','Philippines','Qatar','Saudi Arabia','Singapore',
                    'Sri Lanka','Syrian Arab Republic','Tajikistan','Taiwan','Thailand',
                    'Timor-Leste','Turkmenistan','United Arab Emirates','Uzbekistan','Viet Nam',
                    'Yemen, Rep.'
                ) THEN 'Asia'

                -- 🌏 Oceania
                WHEN c.country_name IN (
                    'Australia','Fiji','Kiribati','Marshall Islands','Micronesia, Fed. Sts.',
                    'Nauru','New Zealand','Palau','Papua New Guinea','Samoa','Solomon Islands',
                    'Tonga','Tuvalu','Vanuatu'
                ) THEN 'Oceania'

                -- 🌍 Europe
                WHEN c.country_name IN (
                    'Albania','Andorra','Austria','Belarus','Belgium','Bosnia And Herzegovina',
                    'Bulgaria','Croatia','Cyprus','Czechia','Denmark','Estonia','Finland','France',
                    'Germany','Greece','Hungary','Iceland','Ireland','Italy','Kosovo','Latvia',
                    'Lithuania','Luxembourg','Malta','Moldova','Monaco','Montenegro','Netherlands',
                    'North Macedonia','Norway','Poland','Portugal','Romania','Russian Federation',
                    'San Marino','Serbia','Slovak Republic','Slovenia','Spain','Sweden',
                    'Switzerland','Turkiye','Ukraine','United Kingdom'
                ) THEN 'Europe'

                -- 🌎 North America
                WHEN c.country_name IN (
                    'Antigua And Barbuda','Aruba','Bahamas, The','Barbados','Belize','Canada',
                    'Costa Rica','Cuba','Dominica','Dominican Republic','El Salvador','Grenada',
                    'Guatemala','Haiti','Honduras','Jamaica','Mexico','Nicaragua','Panama',
                    'Puerto Rico (Us)','St. Kitts And Nevis','St. Lucia',
                    'St. Vincent And The Grenadines','Trinidad And Tobago','United States'
                ) THEN 'North America'

                -- 🌎 South America
                WHEN c.country_name IN (
                    'Argentina','Bolivia','Brazil','Chile','Colombia','Ecuador','Guyana',
                    'Paraguay','Peru','Suriname','Uruguay','Venezuela, Rb'
                ) THEN 'South America'

                ELSE 'Antarctica'
            END AS region,

            ROUND(AVG(q.quality_of_life_value), 2) AS avg_quality_of_life_index
        FROM dim_quality_of_life q
        JOIN dim_country c ON q.country_key = c.country_key
        GROUP BY region
        ORDER BY avg_quality_of_life_index DESC;
    """)
    df = pd.read_sql(query, dw_engine)
    return df






if __name__ == "__main__":
    df = traffic_commute_category_report()
    print(df.head(5))