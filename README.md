
ETL pipeline that extracts raw order-management data from `shopdata.db`, cleans it, and loads a clean
analytical model for Customer Lifetime Value (CLV) reporting.


## Part 1 

`exploration.sql` contains standalone diagnostic queries (tagged `[C1]`–`[C3]` for customers, `[O1]`–`[O4]`
for orders). Each query's header comment states what it finds and why it matters for the cleaning stage.

Run it against the source database:

```bash
sqlite3 shopdata.db < exploration.sql
```

### Data quality findings

Source is 12 customer rows (10 distinct customers), 20 orders, 15 daily exchange rates (EUR/GBP/JPY only).

**Customers (`vw_raw_customers`)**

1. **Duplicate customers** `[C1]` — `customer_id` 1 (Alice) and 2 (Bob) each appear twice with different
   emails/phones and signup dates. → Dedup on `customer_id`, keeping the **most recent `signup_date`**.
2. **Missing contact info** `[C2]` — NULL `email` for customers 2 (older row) and 8; NULL `phone` for
   customers 5 and 8. → Default missing email to `unknown@domain.com`.
3. **Inconsistent phone formatting** `[C3]` — 8 rows use mixed formats: `+1 (555) 123-4567`,
   `(555) 333 4444`, `+44 20 7123 1234`, and even alphabetic values like `Ext 444` and `1-800-555-DINO`.
   → Strip to digits; note a plain punctuation-strip leaves the letters in `DINO`, so **non-digits (incl.
   letters) must be removed**.

**Orders (`vw_raw_orders`)**

4. **Non-positive amounts** `[O1]` — orders 103 (−50) and 113 (−100) are `SYSTEM_ERROR`, and order 114 is
   `0.0` despite status `COMPLETED`. → Filter out `total_amount <= 0`.
5. **Missing currency** `[O2]` — orders 107 and 116 have NULL `currency`. → Treat NULL currency as `USD`.
6. **Missing exchange rates** `[O3]` — 6 non-USD orders (110, 111, 113, 115, 118, 120) have **no rate for
   their exact `(currency, order_date)`**. A naïve inner join to the rates view would silently drop them.
   → Needs a fallback (nearest prior rate, or treat as USD per the assignment's default).
7. **Orphan orders** `[O4]` — orders 106 and 118 reference `customer_id = 99`, which does not exist in the
   customers view. → Flag for the CLV join (these will have no matching customer dimension row).

## Part 2 — Pipeline

`pipeline.py` is a Prefect flow that runs the ETL: it extracts the three views, applies the cleaning
rules, and loads `dim_customers` + `fct_orders` into a new `analytics.db` (falling back to
`clean_customers.csv` / `clean_orders.csv` if the database cannot be written).

Install dependencies and run the flow:

```bash
pip install -r requirements.txt
python pipeline.py
```

Cleaning rules applied:

- **Customers** — deduplicate on `customer_id` keeping the most recent `signup_date`; strip `phone` to
  digits only; default missing `email` to `unknown@domain.com`.
- **Orders** — drop rows with `total_amount <= 0`; add `usd_amount` by joining `vw_exchange_rates` on
  `(currency, order_date)`, treating a missing currency or missing rate as USD (rate 1).

On the provided data this yields **10 customers** (from 12 rows) and **17 orders** (from 20; 3 dropped).
Transformation logic lives in plain, DB-free functions so it can be unit-tested with dummy DataFrames
(Part 3).

## Part 3 — Tests

`test_pipeline.py` unit-tests the pure transformation functions with dummy DataFrames / scalars only —
no database and no Prefect run context — so the business rules are validated in isolation. It covers the
phone standardizer, currency conversion (matching rate, right-date selection, USD/missing-rate/null-currency
fallback, rounding, and no-rows-dropped), invalid-order filtering, and the customer dedup / email-fill rules.

```bash
pip install -r requirements.txt
pytest
```

## Part 4 — CLV report

`clv_report.sql` runs against the cleaned `analytics.db` and returns one row per customer — `customer_id`,
`full_name`, `total_orders_placed`, `lifetime_value_usd`, and `customer_cohort` (signup month) — ranked by
lifetime value descending. Run it once the pipeline has produced `analytics.db`:

```bash
sqlite3 analytics.db < clv_report.sql
```


## Repository layout

| File | Purpose |
|------|---------|
| `exploration.sql` | Part 1 — data-quality diagnostic queries |
| `pipeline.py` | Part 2 — Prefect ETL flow (extract / transform / load) |
| `test_pipeline.py` | Part 3 — unit tests for the transformation logic |
| `clv_report.sql` | Part 4 — CLV query against `analytics.db` |
| `requirements.txt` | Python dependencies |
| `shopdata.db` | Provided source database (read-only views) |
| `analytics.db` | Generated output (`dim_customers`, `fct_orders`) |