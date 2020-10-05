SELECT 
Response_ID,
user_id,
Response_Timestamp AS response_timestamp,
lag (Response_Timestamp) over (partition by (cast(user_id as STRING)) order by Response_Timestamp) AS last_response_timestamp,
lead (Response_Timestamp) over (partition by (cast(user_id as STRING)) order by Response_Timestamp) AS next_response_timestamp,
score
FROM {{ source('fy', 'dim_delighted_survey_responses') }}
WHERE user_id is not null 