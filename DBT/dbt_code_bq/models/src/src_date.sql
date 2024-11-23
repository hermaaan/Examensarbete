WITH stg_data_crypto AS (SELECT * FROM {{ source('crypto_api', 'stg_data_crypto') }})

SELECT
    EXTRACT(DATE FROM TIMESTAMP) AS date,
    FORMAT_TIMESTAMP('%A', timestamp) AS day_name,
    FORMAT_TIMESTAMP('%B', timestamp) AS month_name,
    EXTRACT(DAY FROM timestamp) AS day_of_month,
    EXTRACT(WEEK FROM timestamp) AS week_number,
    EXTRACT(MONTH FROM timestamp) AS month_number, 
    EXTRACT(YEAR FROM timestamp) AS year, 
    EXTRACT(HOUR FROM timestamp) AS hour, 
    EXTRACT(MINUTE FROM timestamp) AS minute_value,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM timestamp) IN (1, 7) THEN 1
        ELSE 0
    END AS is_weekend  -- 1 for weekend, 0 for weekday
FROM stg_data_crypto
