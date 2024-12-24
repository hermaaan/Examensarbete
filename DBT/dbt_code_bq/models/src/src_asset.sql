WITH stg_data_crypto AS (select * from {{ source('crypto_api', 'stg_data_crypto') }})

SELECT 
    id, 
    name AS currency_name, 
    symbol AS currency_symbol, 
    maxSupply AS max_supply, 
    explorer AS web_url
FROM stg_data_crypto