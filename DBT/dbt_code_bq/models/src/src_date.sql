SELECT
    d AS date,
    FORMAT_DATE('%A', d) AS day_name,
    FORMAT_DATE('%B', d) AS month_name,
    EXTRACT(DAY FROM d) AS day_of_month,
    CASE 
        WHEN EXTRACT(WEEK FROM d) = 0 THEN 1 -- treat week 0 as week 1
        ELSE EXTRACT(WEEK FROM d)
    END AS week_number,
    EXTRACT(MONTH FROM d) AS month_number,
    EXTRACT(YEAR FROM d) AS year,
    CASE WHEN EXTRACT(DAYOFWEEK FROM d) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
FROM 
    UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2029-12-31', INTERVAL 1 DAY)) AS d

--Anledningen till vrf jag it bara tog de datum som finns utifrån timestamp är bara för att detta är ett sätt som Ica använder datum på 
--ksk inte det bästa alternativet men lite roligare