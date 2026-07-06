-- =============================================================================
-- exploration.sql  --  Part 1: Data Exploration & Understanding
--
-- Surfaces data-quality anomalies in vw_raw_customers and vw_raw_orders (plus
-- the vw_exchange_rates view they depend on) BEFORE building the cleaning
-- pipeline. Each query is a standalone check; the header comment states what it
-- finds and why it matters for the ETL.
--
-- Run:  sqlite3 shopdata.db < exploration.sql
-- =============================================================================


-- ############################################################################
-- CUSTOMERS  (vw_raw_customers)
-- ############################################################################

-- [C1] Duplicate customer_id ------------------------------------------------
-- The same customer appears on multiple rows with different signup_date /
-- email / phone. Cleaning rule: keep the row with the most recent signup_date.
SELECT
    customer_id,
    COUNT(*)              AS row_count,
    COUNT(DISTINCT email) AS distinct_emails,
    MIN(signup_date)      AS earliest_signup,
    MAX(signup_date)      AS latest_signup
FROM vw_raw_customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY customer_id;


-- [C2] Missing contact info -------------------------------------------------
-- NULL / empty email or phone. Cleaning rule: default missing email to
-- 'unknown@domain.com'. Missing phones become empty once digits are stripped.
SELECT
    customer_id,
    full_name,
    email,
    phone,
    signup_date
FROM vw_raw_customers
WHERE email IS NULL OR TRIM(email) = ''
   OR phone IS NULL OR TRIM(phone) = ''
ORDER BY customer_id;


-- [C3] Inconsistent / non-numeric phone formatting --------------------------
-- Phones arrive in many shapes: "+1 (555) 123-4567", "Ext 444",
-- "1-800-555-DINO" (contains letters!), "+44 20 7123 1234".
-- A naive "strip punctuation" leaves letters behind, so flag them separately.
-- GLOB '*[^0-9]*' = contains at least one non-digit character.
SELECT
    customer_id,
    phone,
    CASE WHEN phone GLOB '*[A-Za-z]*' THEN 'YES' ELSE 'no' END AS has_letters
FROM vw_raw_customers
WHERE phone IS NOT NULL
  AND phone GLOB '*[^0-9]*'          -- not already all-digits
ORDER BY has_letters DESC, customer_id;


-- ############################################################################
-- ORDERS  (vw_raw_orders)
-- ############################################################################

-- [O1] Non-positive order amounts -------------------------------------------
-- Negative or zero totals are system errors. Cleaning rule: filter them out.
SELECT order_id, customer_id, order_date, total_amount, currency, status
FROM vw_raw_orders
WHERE total_amount IS NULL OR total_amount <= 0
ORDER BY total_amount;


-- [O2] Missing / unknown currency -------------------------------------------
-- NULL currency. Cleaning rule: treat as already 'USD' (rate = 1).
SELECT order_id, customer_id, order_date, total_amount, currency, status
FROM vw_raw_orders
WHERE currency IS NULL OR TRIM(currency) = ''
ORDER BY order_id;


-- [O3] Non-USD orders with no matching exchange rate ------------------------
-- No rate exists for the (currency, order_date) pair. A naive INNER JOIN on
-- the rates view would SILENTLY DROP these orders. USD is excluded (implicitly
-- 1:1 and absent from the rates view). Fallback strategy must be chosen.
SELECT
    o.order_id,
    o.order_date,
    o.currency,
    o.total_amount
FROM vw_raw_orders o
LEFT JOIN vw_exchange_rates r
       ON r.currency = o.currency
      AND r.date     = o.order_date
WHERE o.currency IS NOT NULL
  AND o.currency <> 'USD'
  AND r.rate_to_usd IS NULL
ORDER BY o.currency, o.order_date;


-- [O4] Orphan orders --------------------------------------------------------
-- Orders whose customer_id has no matching row in the customers view.
SELECT o.order_id, o.customer_id, o.order_date
FROM vw_raw_orders o
LEFT JOIN vw_raw_customers c ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL
ORDER BY o.order_id;
