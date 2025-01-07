WITH cc AS (SELECT * FROM {{ ref('src_crypto') }}),

ca AS (select * from {{ ref('src_asset') }}),

da AS (select * from {{ ref('src_date') }})

SELECT
    {{dbt_utils.generate_surrogate_key(['cc.id','cc.timestamp'])}} as fct_id,
    {{ dbt_utils.generate_surrogate_key(['ca.id', 'ca.currency_name']) }} as asset_key,
    {{ dbt_utils.generate_surrogate_key(['da.date', 'da.day_name']) }} as date_key,
    timestamp,
    price_usd,
    rank,
    volume_usd_24hr,
    supply,
    coalesce(max_supply, 0) AS max_supply, --0 means unlimited supply
    market_cap_usd,
    change_percent_24hr,
    vwap_24hr
FROM cc
LEFT JOIN ca ON cc.id = ca.id
LEFT JOIN da ON cc.date = da.date