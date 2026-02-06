-- 插入参考数据：费率代码 (RateCodeID)
-- 1=Standard rate, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated fare, 6=Group ride
INSERT INTO dim_rate_code (rate_code_id, description) VALUES
(1, 'Standard rate'),
(2, 'JFK'),
(3, 'Newark'),
(4, 'Nassau or Westchester'),
(5, 'Negotiated fare'),
(6, 'Group ride'),
(99, 'Unknown')
ON CONFLICT (rate_code_id) DO NOTHING;

-- 插入参考数据：支付方式 (Payment_type)
-- 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip
INSERT INTO dim_payment_type (payment_type_id, description) VALUES
(1, 'Credit card'),
(2, 'Cash'),
(3, 'No charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided trip')
ON CONFLICT (payment_type_id) DO NOTHING;

-- 注意：dim_location 和 dim_datetime 的数据量较大
-- dim_location 通常来自 'taxi_zone_lookup.csv'
-- dim_datetime 和 fact_trips 将在 Exercise 2 Branch 2 (Spark ETL) 中动态生成并插入

TRUNCATE TABLE dim_location CASCADE;

-- 2. 导入 CSV 数据
-- 前提：你必须把下载好的 taxi+_zone_lookup.csv 放在
-- 你的 spark_data_integration_exercice3 文件夹里
COPY dim_location (location_id, borough, zone, service_zone)
FROM '/docker-entrypoint-initdb.d/taxi_zone_lookup.csv'
DELIMITER ','
CSV HEADER;