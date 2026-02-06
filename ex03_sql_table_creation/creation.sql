-- 维度表保持不变：只建这一张
CREATE TABLE IF NOT EXISTS dim_datetime (
    datetime_id VARCHAR(20) PRIMARY KEY, -- 如 '2023010112'
    timestamp_value TIMESTAMP,           -- 具体时间戳
    year INT,
    month INT,
    day INT,
    hour INT,
    weekday INT,
    is_weekend BOOLEAN
);

-- 2. 维度表：地点 (Borough, Zone)
CREATE TABLE IF NOT EXISTS dim_location (
    location_id INT PRIMARY KEY,
    borough VARCHAR(50),
    zone VARCHAR(100),
    service_zone VARCHAR(50)
);

-- 3. 维度表：费率代码
CREATE TABLE IF NOT EXISTS dim_rate_code (
    rate_code_id INT PRIMARY KEY,
    description VARCHAR(50)
);

-- 4. 维度表：支付方式
CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_id INT PRIMARY KEY,
    description VARCHAR(50)
);

-- (其他维度表 dim_location, dim_rate_code 等保持不变...)

-- 事实表：同时引用两次 dim_datetime
CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id SERIAL PRIMARY KEY,
    vendor_id INT,
    
    -- 【关键修改在这里】建立了两个字段，都指向同一张 dim_datetime 表
    pickup_datetime_id VARCHAR(20) REFERENCES dim_datetime(datetime_id),
    dropoff_datetime_id VARCHAR(20) REFERENCES dim_datetime(datetime_id),
    
    pickup_location_id INT REFERENCES dim_location(location_id),
    dropoff_location_id INT REFERENCES dim_location(location_id),
    rate_code_id INT REFERENCES dim_rate_code(rate_code_id),
    payment_type_id INT REFERENCES dim_payment_type(payment_type_id),
    
    -- 各种指标
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