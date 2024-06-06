CREATE OR REPLACE TABLE `icezy-423617.Ecommerce.fact_sales` AS
SELECT
  event_time_utc AS event_time,
  event_type,
  product_id,
  user_id,
  user_session,
  price,
  total_sales_per_user,
  avg_price_per_category,
  event_type_encoded,
  category_code_encoded,
  brand_encoded,
  FORMAT_TIMESTAMP('%Y-%m', event_time_utc) AS year_month
FROM `icezy-423617.Ecommerce.transformed_data`;