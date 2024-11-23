WITH fct_crypto AS (SELECT * FROM {{ ref('fct_crypto') }}),

asset AS (SELECT * FROM {{ ref('dim_asset') }}),

date AS (SELECT * FROM {{ ref('dim_date') }})

SELECT 
    *
    
FROM fct_crypto AS f
LEFT JOIN asset AS ca
    ON f.asset_key = ca.asset_id
LEFT JOIN date AS da
    ON f.date_key = da.date