WITH src_date as (select * from {{ ref('src_date') }})

SELECT 
    {{dbt_utils.generate_surrogate_key(['date', 'day_name'])}} AS date, --update later
    date AS original_date, -- -ll-
    day_name,
    month_name,
    day_of_month,
    week_number,
    month_number, 
    year, 
    hour, 
    minute_value,
    is_weekend 
FROM src_date