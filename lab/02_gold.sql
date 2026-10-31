-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 Gold: tables ready for questions
-- MAGIC Point dashboards and Genie at these.

-- COMMAND ----------

USE workspace.shop;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_customer_preferences
COMMENT 'One row per Mart 233 customer profile: city, age band, their favourite category (most items bought in the last 30 days, or ever if they have not shopped recently), total spend in Ghana cedis (GHS) and preferred payment method. Synthetic lab data.'
AS
WITH by_category AS (
  SELECT
    customer_id,
    category,
    sum(CASE WHEN ordered_at >= current_timestamp() - INTERVAL 30 DAYS THEN quantity ELSE 0 END) AS items_30d,
    sum(quantity) AS items,
    sum(revenue_ghs) AS spend
  FROM silver_orders
  GROUP BY ALL
),
by_payment AS (
  SELECT customer_id, payment, count(*) AS n
  FROM silver_orders
  GROUP BY ALL
)
SELECT
  c.customer_id,
  c.name,
  c.city,
  c.age_band,
  c.joined_at,
  -- What they are into now: most items in the last 30 days, then all-time items, then spend
  max_by(bc.category, bc.items_30d * 1e12 + bc.items * 1e8 + bc.spend) AS favourite_category,
  round(sum(bc.spend), 2)                  AS total_spend_ghs,
  first(pm.payment)                        AS preferred_payment
FROM customers c
JOIN by_category bc USING (customer_id)
JOIN (SELECT customer_id, max_by(payment, n) AS payment FROM by_payment GROUP BY customer_id) pm
  USING (customer_id)
GROUP BY c.customer_id, c.name, c.city, c.age_band, c.joined_at;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_category_city
COMMENT 'Orders, revenue in Ghana cedis (GHS) and average rating per category, city and age band. Synthetic lab data.'
AS
SELECT
  city,
  age_band,
  category,
  count(*)                    AS orders,
  round(sum(revenue_ghs), 2)  AS revenue_ghs,
  round(avg(rating), 2)       AS avg_rating,
  round(avg(CASE WHEN payment = 'Mobile money' THEN 1 ELSE 0 END), 3) AS mobile_money_share
FROM silver_orders
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_products
COMMENT 'Each product with its category, price, orders, revenue in Ghana cedis (GHS) and average rating. Synthetic lab data.'
AS
SELECT
  p.product_id,
  p.product_name,
  p.category,
  p.price_ghs,
  count(o.order_id)              AS orders,
  round(sum(o.revenue_ghs), 2)   AS revenue_ghs,
  round(avg(o.rating), 2)        AS avg_rating
FROM products p
LEFT JOIN silver_orders o USING (product_id)
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_daily
COMMENT 'Orders and revenue in Ghana cedis (GHS) per day and category. Synthetic lab data.'
AS
SELECT order_date, category, count(*) AS orders, round(sum(revenue_ghs), 2) AS revenue_ghs
FROM silver_orders
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_personas
COMMENT 'One row per Mart 233 customer with their shopper persona, worked out from their favourite category, plus orders, spend in Ghana cedis (GHS) and a spend tier within the persona. Synthetic lab data.'
AS
WITH orders_per_customer AS (
  SELECT customer_id, count(*) AS orders FROM silver_orders GROUP BY ALL
),
base AS (
  SELECT
    p.customer_id, p.name, p.city, p.age_band, p.favourite_category,
    p.total_spend_ghs, p.preferred_payment, o.orders,
    CASE p.favourite_category
      WHEN 'Phones'      THEN 'Mobile first'
      WHEN 'Fashion'     THEN 'Trend setter'
      WHEN 'Electronics' THEN 'Gadget lover'
      WHEN 'Home'        THEN 'Home maker'
      WHEN 'Groceries'   THEN 'Family stocker'
      WHEN 'Beauty'      THEN 'Self-care regular'
    END AS persona
  FROM gold_customer_preferences p
  JOIN orders_per_customer o USING (customer_id)
)
SELECT
  *,
  CASE WHEN percent_rank() OVER (PARTITION BY persona ORDER BY total_spend_ghs) >= 0.8
       THEN 'Top spender' ELSE 'Regular' END AS spend_tier
FROM base;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_persona_summary
COMMENT 'One row per shopper persona: customers, average spend in Ghana cedis (GHS) and orders, most common age band and city, mobile money share and most bought product. Synthetic lab data.'
AS
WITH top_product AS (
  SELECT g.persona, mode(s.product_name) AS top_product
  FROM silver_orders s JOIN gold_personas g USING (customer_id)
  WHERE s.category = g.favourite_category
  GROUP BY g.persona
)
SELECT
  persona,
  count(*)                                   AS customers,
  round(avg(total_spend_ghs), 2)             AS avg_spend_ghs,
  round(avg(orders), 1)                      AS avg_orders,
  mode(age_band)                             AS main_age_band,
  mode(city)                                 AS top_city,
  round(avg(CASE WHEN preferred_payment = 'Mobile money' THEN 1 ELSE 0 END), 3) AS mobile_money_share,
  first(t.top_product)                       AS top_product
FROM gold_personas
JOIN top_product t USING (persona)
GROUP BY persona;

-- COMMAND ----------

-- The six personas side by side
SELECT * FROM gold_persona_summary ORDER BY customers DESC;

-- COMMAND ----------

-- What do shoppers of each age band prefer?
SELECT age_band, favourite_category, count(*) AS customers
FROM gold_customer_preferences
GROUP BY ALL
ORDER BY age_band, customers DESC;

-- COMMAND ----------

-- Top category by orders in each city
SELECT city, max_by(category, orders) AS top_category, max(orders) AS orders
FROM (SELECT city, category, sum(orders) AS orders FROM gold_category_city GROUP BY ALL)
GROUP BY city
ORDER BY city;
