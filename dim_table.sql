-- Time Dimension Table
CREATE OR REPLACE TABLE `icezy-423617.Ecommerce.dim_time` AS
SELECT
  event_time_utc AS event_time,
  EXTRACT(DAYOFWEEK FROM event_time_utc) AS day_of_week,
  EXTRACT(HOUR FROM event_time_utc) AS hour_of_day,
  EXTRACT(YEAR FROM event_time_utc) AS year,
  EXTRACT(MONTH FROM event_time_utc) AS month,
  FORMAT_TIMESTAMP('%Y-%m', event_time_utc) AS year_month
FROM `icezy-423617.Ecommerce.transformed_data`
GROUP BY event_time_utc;

-- Product Dimension Table
CREATE OR REPLACE TABLE `icezy-423617.Ecommerce.dim_product` AS
SELECT
  product_id,
  category_code,
  price,
  category_code_encoded
FROM `icezy-423617.Ecommerce.transformed_data`
GROUP BY product_id, category_code, price, category_code_encoded;

-- User Dimension Table
CREATE OR REPLACE TABLE `icezy-423617.Ecommerce.dim_user` AS
SELECT
  user_id,
  user_session
FROM `icezy-423617.Ecommerce.transformed_data`
GROUP BY user_id, user_session;

-- Category Dimension Table
CREATE OR REPLACE TABLE `icezy-423617.Ecommerce.dim_category` AS
SELECT
  category_code,
  category_code_filled
FROM `icezy-423617.Ecommerce.transformed_data`
GROUP BY category_code, category_code_filled;

-- Brand Dimension Table
CREATE OR REPLACE TABLE `icezy-423617.Ecommerce.dim_brand` AS
SELECT
  brand,
  brand_encoded
FROM `icezy-423617.Ecommerce.transformed_data`
GROUP BY brand, brand_encoded;