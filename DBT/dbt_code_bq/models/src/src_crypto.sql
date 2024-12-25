WITH stg_data_crypto AS (select * from {{ source('crypto_api', 'stg_data_crypto') }})

SELECT distinct
    id, 
    EXTRACT(DATE FROM TIMESTAMP) AS date,
    timestamp,
    rank, 
    priceUsd AS price_usd,
    volumeUsd24Hr AS volume_usd_24Hr,
    supply, 
    marketCapUsd AS market_cap_usd,
    changePercent24Hr AS change_percent_24Hr,
    vwap24Hr AS vwap_24Hr
FROM stg_data_crypto