WITH src_market as (select * from {{ ref('src_market') }})

SELECT *

FROM src_market