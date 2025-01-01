WITH src_date as (select * from {{ ref('src_date') }})

SELECT DISTINCT
    {{dbt_utils.generate_surrogate_key(['date', 'day_name'])}} AS date_key,
    date,
    day_name,
    month_name,
    day_of_month,
    week_number,
    month_number, 
    year, 
    is_weekend 
FROM src_date