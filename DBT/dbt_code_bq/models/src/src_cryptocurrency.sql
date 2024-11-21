with stg_data_crypto AS (select * from {{ source('crypto_api', 'stg_data_crypto') }})

select *
from stg_data_crypto