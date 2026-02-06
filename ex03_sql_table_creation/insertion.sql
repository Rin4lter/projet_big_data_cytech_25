-- RateCodeID
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

-- Payment_type
-- 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip
INSERT INTO dim_payment_type (payment_type_id, description) VALUES
(1, 'Credit card'),
(2, 'Cash'),
(3, 'No charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided trip')
ON CONFLICT (payment_type_id) DO NOTHING;


TRUNCATE TABLE dim_location CASCADE;

COPY dim_location (location_id, borough, zone, service_zone)
FROM '/docker-entrypoint-initdb.d/taxi_zone_lookup.csv'
DELIMITER ','
CSV HEADER;