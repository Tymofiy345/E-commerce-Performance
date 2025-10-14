with CTE_for_session_start as(
SELECT
event_name,
(select value.int_value from e.event_params where key = 'ga_session_id') as session_id,
user_pseudo_id || cast((select value.int_value from e.event_params where key = 'ga_session_id') as string) as user_session_id,
traffic_source.source,
traffic_source.medium,
traffic_source.name as campaign,
regexp_extract((select value.string_value from e.event_params where key = 'page_location'), r'(?:https:\/\/)?[^\/]+\/(.*)') as landing_page_location,
geo.country,
device.category,
device.language,
device.operating_system
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
WHERE event_name = 'session_start'),



CTE_for_event_timestamp as(
SELECT
event_name,
(select value.int_value from e.event_params where key = 'ga_session_id') as session_id,
user_pseudo_id || cast((select value.int_value from e.event_params where key = 'ga_session_id') as string) as user_session_id,
timestamp_micros(event_timestamp) as event_timestamp,
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
where event_name in ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'add_shipping_info', 'add_payment_info', 'purchase')
)
--SELECT * FROM CTE_for_session_start
--SELECT * FROM CTE_for_event_timestamp

SELECT t1.*, t2.event_name, t2.event_timestamp
FROM CTE_for_session_start t1
LEFT JOIN CTE_for_event_timestamp t2
ON t1.user_session_id = t2.user_session_id