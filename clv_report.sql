-- =============================================================================
-- clv_report.sql  --  Part 4: Customer Lifetime Value (CLV)
--
-- Runs against the CLEANED analytics database produced by pipeline.py
-- (dim_customers + fct_orders), NOT the raw shopdata.db.
--
-- One row per customer, ranked by lifetime value (highest first):
--   customer_id          - customer key
--   full_name            - customer name
--   total_orders_placed  - number of valid orders
--   lifetime_value_usd   - sum of valid USD order amounts
--   customer_cohort      - signup month, e.g. '2023-01'
--
-- fct_orders already contains only cleaned/valid orders (amount > 0, converted
-- to USD), so SUM(usd_amount) is the lifetime value of valid orders.
--
-- LEFT JOIN keeps every customer (those with no orders show 0). Orphan orders
-- whose customer_id is not in dim_customers are excluded by design.
--
-- Run:  sqlite3 analytics.db < clv_report.sql
-- =============================================================================

SELECT
    c.customer_id,
    c.full_name,
    COUNT(o.order_id)                          AS total_orders_placed,
    ROUND(COALESCE(SUM(o.usd_amount), 0), 2)   AS lifetime_value_usd,
    strftime('%Y-%m', c.signup_date)           AS customer_cohort
FROM dim_customers c
LEFT JOIN fct_orders o
       ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.full_name, customer_cohort
ORDER BY lifetime_value_usd DESC;
