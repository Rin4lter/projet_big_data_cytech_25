-- dim_datetime
CREATE TABLE IF NOT EXISTS dim_datetime (
    datetime_id VARCHAR(20) PRIMARY KEY,
    timestamp_value TIMESTAMP,
    year INT,
    month INT,
    day INT,
    hour INT,
    weekday INT,
    is_weekend BOOLEAN
);

-- dim_location
CREATE TABLE IF NOT EXISTS dim_location (
    location_id INT PRIMARY KEY,
    borough VARCHAR(50),
    zone VARCHAR(100),
    service_zone VARCHAR(50)
);

-- dim_rate_code
CREATE TABLE IF NOT EXISTS dim_rate_code (
    rate_code_id INT PRIMARY KEY,
    description VARCHAR(50)
);

-- dim_payment_type
CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_id INT PRIMARY KEY,
    description VARCHAR(50)
);


-- fact_trips
CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id SERIAL PRIMARY KEY,
    vendor_id INT,
    
    pickup_datetime_id VARCHAR(20) REFERENCES dim_datetime(datetime_id),
    dropoff_datetime_id VARCHAR(20) REFERENCES dim_datetime(datetime_id),
    
    pickup_location_id INT REFERENCES dim_location(location_id),
    dropoff_location_id INT REFERENCES dim_location(location_id),
    rate_code_id INT REFERENCES dim_rate_code(rate_code_id),
    payment_type_id INT REFERENCES dim_payment_type(payment_type_id),
    
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT
);