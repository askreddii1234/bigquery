WITH raw_listings as (

SELECT
id as listing_id
,listing_url
,name as listing_name
,room_type
,minimum_nights
,host_id
,price as price_str
,created_at
,updated_at
FROM
`dbt-analytics-engineer-403622.dbt_stage.raw_listings`

)

SELECT * FROM raw_listings
