WITH nps AS (
SELECT 
Response_ID as id, 
Response_Timestamp AS timestamp,
date(Response_Timestamp	) AS date,
score,
CASE WHEN score <= 6 THEN 'Detractor'
     WHEN score >= 7 AND score <=8  THEN 'Netural'
     WHEN score >= 9  THEN 'Promoter'
END AS nps_catagory
FROM {{ source('fy', 'dim_delighted_survey_responses') }}
),

case_for_cat AS (
SELECT 
id,
timestamp,
date,
score,
nps_catagory,
CASE WHEN nps_catagory = 'Detractor' THEN 1 ELSE 0 END AS number_of_detractors,
CASE WHEN nps_catagory = 'Netural' THEN 1 ELSE 0 END AS number_of_netural,
CASE WHEN nps_catagory = 'Promoter' THEN 1 ELSE 0 END AS number_of_promoters,
COUNT(id) over (partition by date) AS responces_on_day
FROM nps
),

window_for_cat AS (
SELECT 
*,
SUM(number_of_detractors) OVER (partition by date) AS total_number_of_detractors_on_date,
SUM(number_of_netural) OVER (partition by date) AS total_number_of_netural_on_date,
SUM(number_of_promoters) OVER (partition by date) AS total_number_of_promoters_on_date,
FROM 
case_for_cat
),

averages AS (
SELECT DISTINCT
date,
responces_on_day,	
total_number_of_detractors_on_date,	
total_number_of_netural_on_date,	
total_number_of_promoters_on_date,
((total_number_of_promoters_on_date - total_number_of_detractors_on_date) / responces_on_day) AS nps_on_date,
AVG((total_number_of_promoters_on_date - total_number_of_detractors_on_date) / responces_on_day) OVER (PARTITION BY EXTRACT(WEEK FROM date))   AS nps_on_week,
AVG((total_number_of_promoters_on_date - total_number_of_detractors_on_date) / responces_on_day) OVER (PARTITION BY EXTRACT(MONTH FROM date))  AS nps_on_month,
FROM window_for_cat
)

SELECT 
date,
responces_on_day,	
total_number_of_detractors_on_date,	
total_number_of_netural_on_date,	
total_number_of_promoters_on_date,
(nps_on_date * 100) AS nps_on_date,
(nps_on_week * 100) AS nps_on_week,
(nps_on_month * 100) AS nps_on_month,
FROM averages