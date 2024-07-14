-- Order count, sales, and AOV for Macbooks sold in North America for each quarter across all years?
SELECT DATE_TRUNC(purchase_ts, quarter) AS quarter,
  COUNT(*) AS order_count,
  SUM(usd_price) AS sales,
  AVG(usd_price) AS AOV
FROM core.orders
LEFT JOIN core.customers
  ON orders.customer_id = customers.id
LEFT JOIN core.geo_lookup
  ON customers.country_code = geo_lookup.country
WHERE LOWER(product_name) LIKE '%macbook%'
  AND region = 'NA'
GROUP BY 1
ORDER BY 1;

-- 
WITH avg_delivery_time AS (
    SELECT EXTRACT(YEAR FROM orders.purchase_ts) AS year,
      region,
      AVG(DATE_DIFF(delivery_ts, orders.purchase_ts, day)) AS avg_delivery_days
    FROM core.orders
    LEFT JOIN core.customers
      ON orders.customer_id = customers.id
    LEFT JOIN core.geo_lookup
      ON customers.country_code = geo_lookup.country
    LEFT JOIN core.order_status
      ON orders.id = order_status.order_id
    WHERE region IS NOT NULL
    GROUP BY 1,2
),

  delivery_ranking AS (
    SELECT year,
      region,
      avg_delivery_days,
      ROW_NUMBER() OVER (PARTITION BY year ORDER BY avg_delivery_days DESC) AS ranking
    FROM avg_delivery_time
    ORDER BY year
)

SELECT year,
  region,
  avg_delivery_days
FROM delivery_ranking
WHERE ranking = 1
;
