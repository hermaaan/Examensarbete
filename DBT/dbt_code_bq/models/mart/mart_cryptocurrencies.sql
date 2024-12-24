WITH fct_crypto AS (SELECT * FROM {{ ref('fct_crypto') }}),

asset AS (SELECT * FROM {{ ref('dim_asset') }}),

date AS (SELECT * FROM {{ ref('dim_date') }})

SELECT DISTINCT
    currency_name,
    currency_symbol,
    rank,
    price_usd,
    change_percent_24Hr,
    market_cap_usd,
    vwap_24Hr,
    supply,
    volume_usd_24Hr,
    original_date
FROM fct_crypto AS f
LEFT JOIN asset AS ca
    ON f.asset_key = ca.asset_id
LEFT JOIN date AS da
    ON f.date_key = da.date