WITH src_asset as (select * from {{ ref('src_asset') }})

SELECT 
    {{dbt_utils.generate_surrogate_key(['id', 'currency_name'])}} AS asset_id,
    currency_name, 
    currency_symbol, 
    rank, 
    supply, 
    coalesce(max_supply, 0) AS max_supply, --0 means unlimited
    web_url
FROM src_asset