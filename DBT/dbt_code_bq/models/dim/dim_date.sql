WITH src_date AS (select * from {{ ref('src_date') }})

SELECT 
    {{dbt_utils.generate_surrogate_key(['date', 'day_name'])}} AS date "###update later"
    date,
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