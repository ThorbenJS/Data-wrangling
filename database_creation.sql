
CREATE DATABASE historical_weather_data
WITH OWNER = thorsundell;

CREATE TABLE daily_weather (
    id SERIAL PRIMARY KEY,
    formatted_date DATE NOT NULL,
    precip_type VARCHAR(255) NOT NULL,
    temperature_c DECIMAL NOT NULL,
    apparent_temp_c DECIMAL NOT NULL,
    humidity DECIMAL NOT NULL,
    wind_speed_kmh DECIMAL NOT NULL,
    wind_strength VARCHAR(255) NOT NULL,
    visibility_km DECIMAL NOT NULL,
    pressure_millibars DECIMAL NOT NULL,
    avg_temperature_c DECIMAL NOT NULL,
    avg_humidity DECIMAL NOT NULL,
    avg_wind_speed_mkh DECIMAL NOT NULL
);


CREATE TABLE monthly_weather (
    id SERIAL PRIMARY KEY,
    month DATE NOT NULL,
    avg_temperature_c DECIMAL NOT NULL,
    avg_apparent_temp_c DECIMAL NOT NULL,
    avg_humidity DECIMAL NOT NULL,
    avg_visibility_km DECIMAL NOT NULL,
    avg_pressure_millibars DECIMAL NOT NULL,
    mode_precip_type VARCHAR(255)
);
