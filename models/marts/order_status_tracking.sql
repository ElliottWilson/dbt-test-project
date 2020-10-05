WITH orders AS (
SELECT  order_id,
        user_id,
        order_created_at AS created_timpstamp
FROM {{ source('fy', 'fct_orders') }}
),

-- Need to do this do get the most recent delighted survey after an order
delighted AS (
SELECT  Response_ID AS response_id,
        user_id,
        Response_Timestamp AS response_timestamp,
        lag (Response_Timestamp) over (partition by (cast(user_id as STRING)) order by Response_Timestamp) AS last_response_timestamp,
        lead (Response_Timestamp) over (partition by (cast(user_id as STRING)) order by Response_Timestamp) AS next_response_timestamp,
        score
FROM {{ source('fy', 'dim_delighted_survey_responses') }}
WHERE user_id is not null),

status_time_pivot AS (
SELECT  order_id,
        CASE WHEN order_status = 'processing' THEN status_changed_timestamp ELSE NULL END  AS processing_timestamp,
        CASE WHEN order_status = 'cancelled' THEN status_changed_timestamp ELSE NULL END   AS cancelled_timestamp,
        CASE WHEN order_status = 'printing' THEN status_changed_timestamp ELSE NULL END    AS printing_timestamp,
        CASE WHEN order_status = 'shipped' THEN status_changed_timestamp ELSE NULL END     AS shipped_timestamp
FROM {{ source('fy', 'fct_order_status') }}
),

status_time AS (
SELECT 
order_id,
MIN(processing_timestamp) AS processing_timestamp,
MIN(cancelled_timestamp)  AS cancelled_timestamp,
MIN(printing_timestamp)   AS printing_timestamp,
MIN(shipped_timestamp)    AS shipped_timestamp
FROM status_time_pivot
GROUP BY 1)

SELECT orders.*,
       processing_timestamp,
       cancelled_timestamp,
       printing_timestamp,
       shipped_timestamp,
       TIMESTAMP_DIFF(shipped_timestamp, created_timpstamp, MINUTE) AS mintues_from_ordered_to_shipped,
       TIMESTAMP_DIFF(shipped_timestamp, created_timpstamp, HOUR)   AS hour_from_ordered_to_shipped,
       TIMESTAMP_DIFF(shipped_timestamp, created_timpstamp, DAY)    AS days_from_ordered_to_shipped,
       response_timestamp,
       score
FROM orders 
INNER JOIN status_time ON orders.order_id = status_time.order_id
-- Need to do this do get the most recent delighted survey after an order
LEFT JOIN delighted ON orders.user_id = delighted.user_id
AND (response_timestamp > created_timpstamp AND last_response_timestamp IS NULL)

