WITH cc AS (SELECT * FROM {{ ref('src_crypto') }}),

ca AS (select * from {{ ref('src_asset') }}),

da AS (select * from {{ ref('src_date') }})

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['ca.id', 'ca.currency_name']) }} as asset_key,
    {{ dbt_utils.generate_surrogate_key(['da.date', 'da.day_name']) }} as date_key,
    timestamp,
    price_usd,
    rank,
    volume_usd_24hr,
    supply,
    market_cap_usd,
    change_percent_24hr,
    vwap_24hr
FROM cc
LEFT JOIN ca ON cc.id = ca.id
LEFT JOIN da ON cc.date = da.date