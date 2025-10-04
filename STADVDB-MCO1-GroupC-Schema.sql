CREATE DATABASE country_data_warehouse;
USE country_data_warehouse;
CREATE TABLE dim_country (
    country_key INT AUTO_INCREMENT PRIMARY KEY,
    country_name VARCHAR(100),
    country_code VARCHAR(3),
    region VARCHAR(50),
    continent VARCHAR(50)
);
CREATE TABLE dim_time (
    time_key INT PRIMARY KEY,
    year_value INT,
    is_historical BOOLEAN,
    period_type VARCHAR(20)
);
CREATE TABLE dim_quality_of_life (
    country_key INT PRIMARY KEY,
    
    purchasing_power_value DECIMAL(5,2),
    safety_value DECIMAL(5,2),
    health_care_value DECIMAL(5,2),
    climate_value DECIMAL(5,2),
    cost_of_living_value DECIMAL(5,2),
    property_price_income_value DECIMAL(5,2),
    traffic_commute_value DECIMAL(5,2),
    pollution_value DECIMAL(5,2),
    quality_of_life_value DECIMAL(5,2),
    
    purchasing_power_category VARCHAR(20),
    safety_category VARCHAR(20),
    health_care_category VARCHAR(20),
    climate_category VARCHAR(20),
    cost_of_living_category VARCHAR(20),
    property_price_income_category VARCHAR(20),
    traffic_commute_category VARCHAR(20),
    pollution_category VARCHAR(20),
    quality_of_life_category VARCHAR(20),
    
    quality_tier VARCHAR(20),
    development_status VARCHAR(20),
    
    FOREIGN KEY (country_key) REFERENCES dim_country(country_key)
);
CREATE TABLE fact_country_metrics (
    country_key INT,
    time_key INT,
    
    gdp_usd DECIMAL(15,2),
    population BIGINT,
    gdp_per_capita DECIMAL(10,2),
    
    PRIMARY KEY (country_key, time_key),
    FOREIGN KEY (country_key) REFERENCES dim_country(country_key),
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key)
);
