
WITH raw_reviews as (

SELECT
listing_id
,date as review_date
,reviewer_name
,comments as review_text
,sentiment as review_sentiment
FROM
`dbt-analytics-engineer-403622.dbt_stage.raw_reviews`

)


SELECT * FROM raw_reviews